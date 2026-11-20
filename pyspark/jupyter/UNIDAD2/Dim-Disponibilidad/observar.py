#!/usr/bin/env python3
"""Colector local: comprueba procesos propios, publica estado SQL y un textfile Prometheus."""
import argparse
from pathlib import Path
import json
import os
import signal
import sys
import threading
import time
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'src'))
from disponibilidad_u2.telemetria import construir,leer_json,eventos,proceso_activo
from disponibilidad_u2.observabilidad_sql import ejecutar
from disponibilidad_u2.utilidades import ahora,guardar


def main():
    p=argparse.ArgumentParser();p.add_argument('--duracion-segundos',type=int,default=0);a=p.parse_args()
    cfg=json.loads((ROOT/'config/observabilidad.json').read_text());stop=threading.Event()
    for sig in (signal.SIGINT,signal.SIGTERM):signal.signal(sig,lambda *_:stop.set())
    start=time.monotonic();report={'inicio_utc':ahora(),'pid':os.getpid(),'sondeos':0,'errores_sql':0}
    while not stop.is_set():
        registry=leer_json(ROOT/cfg['registro']);now=time.time()
        text,states,flow=construir(registry,now,cfg['prometheus_url'])
        rows=[];umbral_actual=None
        selected=leer_json(ROOT/'config/ejecucion_u2_seleccionada.json')
        selected_flow=selected.get('flujo')
        archived=leer_json(ROOT/selected['informe_ejecucion']) if selected.get('informe_ejecucion') else {}
        archived_known=archived.get('estado')=='completada' and archived.get('fase_final')=='detenido'
        def literal(v):return "'"+str(v).replace("'","''")+"'"
        for f in {cfg['flow_historico'],flow,selected_flow}-{None}:
            intervalo=registry.get('intervalo_configurado_s',300) if f==flow else 60
            if f==flow:
                records=eventos(registry.get('procesos',{}).get('productor',{}).get('log'))
                capture=next((e.get('capture_id') for e in reversed(records) if e.get('evento')=='captura_confirmada'),None)
                ttl=leer_json(ROOT/'data/resumenes'/(str(capture)+'.json')).get('ttl') if capture else None
                if isinstance(ttl,(int,float)):intervalo=max(intervalo,ttl)
            umbral=2*intervalo+cfg['margen_antiguedad_s']
            if f==flow:umbral_actual=umbral
            for component,state in states.items():
                value=state if f==flow or component in ('postgres','kafka') else (False if f in registry.get('flujos_detenidos',[]) else None)
                if f==selected_flow and f!=flow and component in ('productor','spark','sql') and archived_known:
                    value=proceso_activo(archived.get('procesos',{}).get(component),True)
                flag='NULL' if value is None else ('true' if value else 'false')
                detail='no_configurado' if value is None else ('ejecutando' if value else 'detenido')
                rows.append('('+','.join([literal(f),literal(component),flag,literal(ahora()),literal(detail),str(int(intervalo)),str(int(umbral))])+')')
        try:
            ejecutar('INSERT INTO observabilidad_disponibilidad.componentes(flujo,componente,activo,comprobado_utc,detalle,intervalo_consulta_s,umbral_datos_s) VALUES '+','.join(rows)+
              ' ON CONFLICT(flujo,componente) DO UPDATE SET activo=excluded.activo,comprobado_utc=excluded.comprobado_utc,detalle=excluded.detalle,intervalo_consulta_s=excluded.intervalo_consulta_s,umbral_datos_s=excluded.umbral_datos_s;')
            sql_ok=1
        except Exception:report['errores_sql']+=1;sql_ok=0
        text+='\n# TYPE disponibilidad_observador_sql_up gauge\ndisponibilidad_observador_sql_up '+str(sql_ok)+'\n'
        text+='\n# TYPE disponibilidad_captura_umbral_antiguedad_seconds gauge\ndisponibilidad_captura_umbral_antiguedad_seconds '+('NaN' if umbral_actual is None else str(umbral_actual))+'\n'
        path=ROOT/cfg['metricas'];path.parent.mkdir(parents=True,exist_ok=True)
        tmp=path.with_suffix('.tmp');tmp.write_text(text);os.replace(tmp,path)
        report['sondeos']+=1;report['ultimo_sondeo_utc']=ahora()
        guardar(ROOT/'data/observabilidad/observador.json',report)
        if a.duracion_segundos and time.monotonic()-start>=a.duracion_segundos:break
        stop.wait(cfg['periodo_sondeo_s'])
    report['fin_utc']=ahora();guardar(ROOT/'data/observabilidad/observador.json',report)


if __name__=='__main__':main()
