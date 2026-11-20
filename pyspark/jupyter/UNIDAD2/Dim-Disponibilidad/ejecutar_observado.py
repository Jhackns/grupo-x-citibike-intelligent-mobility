#!/usr/bin/env python3
"""Lanza los ejecutables existentes, registra su telemetría y los cierra sin borrar estado."""
import argparse
from datetime import datetime,timezone
import json
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
from uuid import uuid4
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'src'))
from disponibilidad_u2.config_streaming import cargar_streaming
from disponibilidad_u2.postgres import cargar_sql
from disponibilidad_u2.observabilidad_sql import consultar
from disponibilidad_u2.utilidades import ahora,guardar
from disponibilidad_u2.verificacion_cohorte import estado_cohorte,completa,registrar_ejecucion


def containers():
    r=subprocess.run(['docker','ps','--filter','label=com.docker.compose.project=citibike-disponibilidad-streaming','--format','{{.Names}}'],text=True,capture_output=True,check=True)
    return set(r.stdout.split())


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--capturas',type=int,default=0);p.add_argument('--intervalo',type=float,default=300)
    p.add_argument('--spark-master');p.add_argument('--duracion-segundos',type=int,default=0)
    p.add_argument('--trigger',help='Propaga el trigger existente a Spark y su configuración')
    p.add_argument('--cohorte-inicial',action='store_true',help='Captura hasta madurar y resolver los seis grupos de la primera captura SQL')
    p.add_argument('--pausa-verificacion-segundos',type=int,default=0)
    p.add_argument('--config',type=Path,default=ROOT/'config/streaming-etapa4.json')
    p.add_argument('--config-sql',type=Path,default=ROOT/'config/postgres-etapa4.json')
    p.add_argument('--informe',type=Path,default=ROOT/'reports/observabilidad/ejecucion_viva.json')
    a=p.parse_args();cfg=cargar_streaming(a.config,spark_master=a.spark_master,trigger=a.trigger);sql=cargar_sql(a.config_sql)
    if a.cohorte_inicial and a.capturas:p.error('--cohorte-inicial requiere capturas continuas, --capturas 0')
    public=json.loads((ROOT/'config/observabilidad.json').read_text());registry_path=ROOT/public['registro']
    stop=threading.Event()
    for s in (signal.SIGINT,signal.SIGTERM):signal.signal(s,lambda *_:stop.set())
    run=ROOT/'data/observabilidad/ejecuciones'/str(uuid4());run.mkdir(parents=True)
    registry={'inicio_utc':ahora(),'fase':'preparando','flujo_activo':sql['flow_id'],'controlado':True,
       'intervalo_configurado_s':a.intervalo,
       'flujos_detenidos':[public['flow_historico']] if not containers() else [],'procesos':{}}
    report={'inicio_utc':ahora(),'estado':'en_curso','flujo':sql['flow_id'],'registro':str(registry_path.relative_to(ROOT)),
        'configuracion_stream':cfg,'configuracion_sql':sql,'errores':[]}
    procs={};handles=[];owned={};observer=None
    def save():guardar(registry_path,registry)
    def launch(name,command):
        path=run/(name+'.jsonl');f=path.open('w');handles.append(f)
        proc=subprocess.Popen(command,stdout=f,stderr=subprocess.STDOUT,cwd=ROOT.parents[3])
        procs[name]=proc
        ticks=Path(f'/proc/{proc.pid}/stat').read_text().split(') ',1)[1].split()[19]
        registry['procesos'][name]={'pid':proc.pid,'inicio_ticks':ticks,'log':str(path)};save()
        return proc
    def wait_runtime(name,before_files,before_containers):
        directory=ROOT/('data/sql/ejecuciones' if name=='sql' else 'data/streaming/ejecuciones')
        deadline=time.monotonic()+150
        while time.monotonic()<deadline:
            if procs[name].poll() is not None:raise RuntimeError('El proceso '+name+' terminó durante inicio')
            for file in set(directory.glob('*.json'))-before_files:
                try:m=json.loads(file.read_text())
                except ValueError:continue
                match=m.get('configuracion_sql',{}).get('flow_id')==sql['flow_id'] if name=='sql' else m.get('configuracion',{}).get('output_dir')==cfg['output_dir']
                if match and (m.get('estado')=='activa' or m.get('query_id')):
                    new=containers()-before_containers
                    if len(new)!=1:raise RuntimeError('No se identificó un contenedor propio único')
                    owned[name]=new.pop();registry['procesos'][name].update(runtime=str(file),contenedor=owned[name]);save();return
            if stop.wait(1):raise InterruptedError('Inicio interrumpido')
        raise TimeoutError('Tiempo de inicio agotado')
    try:
        registrar_ejecucion(sql['flow_id'],report['inicio_utc'],evidencia=str(a.informe))
        save();observer=subprocess.Popen([sys.executable,str(ROOT/'observar.py')],cwd=ROOT.parents[3])
        for name in ('spark','sql'):
            d=ROOT/('data/sql/ejecuciones' if name=='sql' else 'data/streaming/ejecuciones');before=set(d.glob('*.json'));before_c=containers()
            cmd=[sys.executable,str(ROOT/'lanzar_streaming.py'),'--config',str(a.config)]
            if a.spark_master:cmd+=['--spark-master',a.spark_master]
            if a.trigger:cmd+=['--trigger',a.trigger]
            if name=='sql':cmd+=['--sql','--config-sql',str(a.config_sql)]
            launch(name,cmd);wait_runtime(name,before,before_c)
        producer_report=run/'productor.json'
        producer=launch('productor',[sys.executable,str(ROOT/'ejecutar_u2.py'),'producir','--capturas',str(a.capturas),
                    '--intervalo',str(a.intervalo),'--output',str(producer_report)])
        registry['fase']='ejecutando';save();started=time.monotonic();next_check=0
        while not stop.is_set() and producer.poll() is None:
            if any(procs[n].poll() is not None for n in ('spark','sql')):raise RuntimeError('Terminó un consumidor propio')
            if a.cohorte_inicial and time.monotonic()>=next_check:
                c=estado_cohorte(sql['flow_id'],sql['tolerance_seconds'],sql['late_seconds'])
                report['cohorte']=c;guardar(a.informe,report);next_check=time.monotonic()+15
                if completa(c):break
            if a.duracion_segundos and time.monotonic()-started>=a.duracion_segundos:
                if a.cohorte_inicial:raise TimeoutError('La cohorte no cerró sus seis grupos dentro del límite; no se declara completada')
                break
            stop.wait(1)
        if producer.poll() is None:producer.send_signal(signal.SIGTERM)
        producer.wait(timeout=90)
        if producer.returncode:raise RuntimeError('Productor terminó sin completar la prueba')
        production=json.loads(producer_report.read_text());report['produccion']=production
        if a.cohorte_inicial and not completa(report.get('cohorte')):
            raise RuntimeError('Productor terminó antes del cierre de la cohorte definida')
        expected=sum(c['confirmed_events'] for c in production.get('capturas',[]))
        capture_ids=[c['capture_id'] for c in production.get('capturas',[])]
        scope=" AND capture_id IN ("+','.join("'"+x.replace("'","''")+"'" for x in capture_ids)+")" if capture_ids else ' AND false'
        deadline=time.monotonic()+150
        while time.monotonic()<deadline:
            n=consultar("SELECT to_json(count(*)) FROM disponibilidad.observaciones WHERE flujo='"+sql['flow_id'].replace("'","''")+"'"+scope)
            if n>=expected:break
            if any(procs[k].poll() is not None for k in ('spark','sql')):raise RuntimeError('Consumidor detenido antes de confirmar SQL')
            time.sleep(2)
        else:raise TimeoutError('SQL no alcanzó las capturas confirmadas')
        report['observaciones_sql']=n;registry['fase']='drenado';save()
        stop.wait(a.pausa_verificacion_segundos)
        report['estado']='completada'
    except Exception as exc:
        report['estado']='fallida';report['errores'].append({'tipo':type(exc).__name__,'detalle':str(exc)[:300]})
    finally:
        for name in ('productor','sql','spark'):
            proc=procs.get(name)
            if proc is None:continue
            if proc.poll() is None:
                if name in owned:
                    subprocess.run([sys.executable,str(ROOT/'detener_streaming.py'),'--contenedor',owned[name]],check=False)
                else:proc.send_signal(signal.SIGTERM)
            try:proc.wait(timeout=100)
            except subprocess.TimeoutExpired:
                report['estado']='fallida';report['errores'].append({'tipo':'TimeoutError','detalle':'Proceso propio sin cierre confirmado: '+name})
        registry['fase']='detenido';registry['fin_utc']=ahora();save()
        time.sleep(20) # Cuatro scrapes de 5 s para verificar el estado detenido.
        if observer is not None:
            observer.send_signal(signal.SIGTERM);observer.wait(timeout=30)
        for f in handles:f.close()
        report['fin_utc']=ahora();report['procesos']=registry['procesos'];report['fase_final']=registry['fase']
        registrar_ejecucion(sql['flow_id'],report['inicio_utc'],report['fin_utc'],report['estado'],str(a.informe))
        guardar(a.informe,report)
    print(json.dumps({'estado':report['estado'],'flujo':sql['flow_id'],'informe':str(a.informe)}))
    return 0 if report['estado']=='completada' else 1


if __name__=='__main__':raise SystemExit(main())
