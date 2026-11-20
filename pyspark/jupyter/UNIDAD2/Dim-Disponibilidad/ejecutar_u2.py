#!/usr/bin/env python3
"""Disponibilidad U2: descubrir, tópico, producir y verificar; no entrena modelos."""
from pathlib import Path
import argparse
import json
import signal
import sys
import threading
import time
import platform
import hashlib
from importlib.metadata import version
from uuid import uuid4

ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'src'))
from disponibilidad_u2.configuracion import cargar
from disponibilidad_u2.gbfs import ClienteGBFS
from disponibilidad_u2.contrato import construir_captura
from disponibilidad_u2.productor import Ingesta,bloqueo
from disponibilidad_u2.kafka_io import comprobar_topico,consumidor_verificacion
from disponibilidad_u2.verificacion import verificar_captura,continuidad
from disponibilidad_u2.referencia_u1 import comprobar_referencia
from disponibilidad_u2.utilidades import ahora,guardar,log
from disponibilidad_u2 import VERSION


def argumentos(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('accion',choices=('descubrir','topico','producir','verificar','prueba-real'))
    p.add_argument('--config',type=Path)
    p.add_argument('--bootstrap-servers')
    p.add_argument('--topic')
    p.add_argument('--index-url')
    p.add_argument('--intervalo',type=float,help='Segundos; siempre se respeta al menos el TTL del status')
    p.add_argument('--capturas',type=int,help='producir: 0=continuo; prueba-real: por defecto 2')
    p.add_argument('--crear',action='store_true',help='Solo topico: crear si no existe; nunca alterar uno existente')
    p.add_argument('--resumen',type=Path,help='Solo verificar: resumen local de una captura')
    p.add_argument('--output',type=Path,help='Informe público acotado, sin estados masivos')
    return p.parse_args(argv)


def main(argv=None):
    args=argumentos(argv);stop=threading.Event()
    for sig in (signal.SIGINT,signal.SIGTERM):signal.signal(sig,lambda *_:stop.set())
    engine=None;consumer=None;run=None
    started=time.monotonic()
    manifest={'version':VERSION,'inicio_utc':ahora(),'estado':'en_curso','accion':args.accion,'errores':[]}
    try:
        cfg=cargar(args.config,bootstrap_servers=args.bootstrap_servers,topic=args.topic,
                   gbfs_index_url=args.index_url,poll_seconds=args.intervalo)
        if args.crear and args.accion!='topico':raise ValueError('--crear solo se permite con topico')
        n=args.capturas if args.capturas is not None else (2 if args.accion=='prueba-real' else 0)
        if n<0 or (args.accion=='prueba-real' and n<2):raise ValueError('Número de capturas inválido')
        run=ROOT/'data/ejecuciones'/(ahora().replace(':','-')+'-'+str(uuid4())[:8])
        manifest['id']=run.name
        manifest['configuracion']=cfg
        manifest['software']={'python':platform.python_version(),'kafka_python':version('kafka-python')}
        manifest['codigo_sha256']={p.relative_to(ROOT).as_posix():hashlib.sha256(p.read_bytes()).hexdigest()
            for p in [ROOT/'ejecutar_u2.py',*sorted((ROOT/'src/disponibilidad_u2').glob('*.py'))]}
        manifest['u1']=comprobar_referencia(cfg['u1_reference'])
        guardar(run/'manifiesto.json',manifest)
        engine=Ingesta(cfg,stop,ROOT/'data')
        if args.accion=='descubrir':
            raw=engine.client.capturar();capture=construir_captura(raw)
            manifest['fuente']={'index_url':cfg['gbfs_index_url'],'index_version':engine.client.indice['version'],
                'index_ttl':engine.client.indice['ttl'],'feeds':engine.client.urls,
                'summary':capture['summary'],
                'station_fields':sorted({k for r in raw['feed']['data']['stations'] for k in r}),
                'sample_event':capture['events'][0]}
        elif args.accion=='topico':manifest['topic']=comprobar_topico(cfg,args.crear)
        elif args.accion=='verificar':
            if args.resumen is None:raise ValueError('verificar requiere --resumen')
            manifest['topic']=comprobar_topico(cfg)
            summary=json.loads(args.resumen.read_text())
            if summary['kafka_topic']!=cfg['topic']:raise ValueError('Resumen pertenece a otro tópico')
            consumer,group=consumidor_verificacion(cfg)
            report,_=verificar_captura(consumer,summary)
            manifest.update(grupo_verificacion=group,verificacion=report)
        else:
            manifest['topic']=comprobar_topico(cfg)
            with bloqueo(ROOT/'data'):
                if args.accion=='prueba-real':
                    if engine.pending.exists():raise RuntimeError('Hay una captura pendiente; recuperarla con producir antes de probar')
                    consumer,group=consumidor_verificacion(cfg,desde_final=True)
                    manifest['grupo_verificacion']=group
                previous=None;manifest['capturas']=[];manifest['verificaciones']=[]
                while not stop.is_set() and (n==0 or len(manifest['capturas'])<n):
                    failures_before=engine.counters['fallos_captura']
                    try:
                        summary=engine.una_captura()
                        if not summary['complete']:raise RuntimeError('Captura incompleta; outbox preservado para recuperación')
                        manifest['capturas'].append(summary)
                        if consumer is not None:
                            proof,current=verificar_captura(consumer,summary)
                            manifest['verificaciones'].append(proof)
                            if previous is not None:manifest.setdefault('continuidad',[]).append(continuidad(previous,current))
                            previous=current
                        guardar(run/'manifiesto.json',manifest)
                    except InterruptedError:break
                    except Exception as exc:
                        manifest['errores'].append({'fase':'captura','tipo':type(exc).__name__})
                        if engine.counters['fallos_captura']==failures_before:engine.counters['fallos_captura']+=1
                        log('captura_fallida',error_tipo=type(exc).__name__,contadores=dict(engine.counters))
                        if n or engine.pending.exists():raise
                        summary={'ttl':0}
                    if n and len(manifest['capturas'])>=n:break
                    wait=engine.espera(summary)
                    # Mantener heartbeats/poll del grupo de verificación durante la espera.
                    while wait>0 and not stop.is_set():
                        tick=min(wait,1.0);stop.wait(tick);wait-=tick
                        if consumer is not None:consumer.poll(timeout_ms=0)
                if n and len(manifest['capturas'])!=n:
                    raise InterruptedError('Prueba acotada interrumpida antes de completarse')
        manifest['estado']='completada' if not stop.is_set() else 'detenida'
        return_code=0
    except Exception as exc:
        manifest['estado']='fallida'
        manifest['errores'].append({'fase':'ejecucion','tipo':type(exc).__name__})
        log('ejecucion_fallida',error_tipo=type(exc).__name__)
        # Errores propios no contienen URL/credenciales; no imprimir excepciones de dependencias.
        if type(exc) in (ValueError,RuntimeError,AssertionError):print(str(exc),file=sys.stderr)
        return_code=1
    finally:
        try:
            if consumer is not None:consumer.close()
            if engine is not None:engine.close()
        except Exception as exc:
            manifest['estado']='fallida';manifest['errores'].append({'fase':'cierre','tipo':type(exc).__name__});return_code=1
        manifest['fin_utc']=ahora()
        manifest['duracion_s']=time.monotonic()-started
        if engine is not None:manifest['contadores']=dict(engine.counters)
        if run is not None:guardar(run/'manifiesto.json',manifest)
        if args.output:guardar(args.output,manifest)
        log('ejecucion_finalizada',estado=manifest['estado'],manifiesto=str(run/'manifiesto.json') if run else None,
            contadores=manifest.get('contadores',{}))
    return return_code


if __name__=='__main__':raise SystemExit(main())
