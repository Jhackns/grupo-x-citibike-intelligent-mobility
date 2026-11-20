#!/usr/bin/env python3
"""Observa dos conteos crecientes desde Grafana y la detención real del flujo propio."""
from datetime import datetime,timedelta,timezone
from pathlib import Path
import json
import sys
import time
import urllib.request
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'src'))
from disponibilidad_u2.grafana_api import consultar_grafana
from disponibilidad_u2.utilidades import guardar,ahora

q=json.loads((ROOT/'observabilidad/consultas.json').read_text())
cfg=json.loads((ROOT/'config/observabilidad.json').read_text())
v={'flujo':cfg['flow_vivo'],'estacion':'__all','tipo':'convencionales','horizonte':'5'}
now=datetime.now(timezone.utc);start=(now-timedelta(hours=1)).isoformat();end=(now+timedelta(hours=1)).isoformat()
samples=[];deadline=time.monotonic()+240
while time.monotonic()<deadline:
    rows=consultar_grafana(q['cobertura'],v,start,end)
    state=consultar_grafana(q['estado'],v,start,end)
    sample={'fecha_utc':ahora(),'cobertura':rows,'estado':state}
    if not samples or rows!=samples[-1]['cobertura'] or state[0]['estado_flujo']!=samples[-1]['estado'][0]['estado_flujo']:
        samples.append(sample);guardar(ROOT/'reports/observabilidad/actualizacion_viva.json',{'estado':'en_curso','muestras':samples})
    counts={r['cobertura'][0]['predicciones'] for r in samples if r['cobertura']}
    if len(counts)>=2:break
    time.sleep(5)
assert len({r['cobertura'][0]['predicciones'] for r in samples if r['cobertura']})>=2,'No se observaron dos conteos distintos'
assert any(s['estado'][0]['estado_flujo']=='ACTIVO' for s in samples)
deadline=time.monotonic()+180
while time.monotonic()<deadline:
    state=consultar_grafana(q['estado'],v,start,end)
    if state and state[0]['estado_flujo']=='DETENIDO':break
    time.sleep(3)
assert state[0]['estado_flujo']=='DETENIDO'
metric=consultar_grafana(q['metricas'],v,start,end)
assert metric and metric[0]['n']==0 and all(metric[0][k] is None for k in ('mae','rmse','sesgo'))
empty=consultar_grafana(q['metricas'],{**v,'estacion':'estacion-inexistente'},start,end)
assert empty==[]
pred=consultar_grafana(q['predicciones'],v,start,end)
assert pred and all(r['vigencia']=='VIGENTE' for r in pred)
report={'estado':'verificada','muestras':samples,'detenido':state,'metricas_sin_muestras':metric,
        'estacion_inexistente':empty,'predicciones_vigentes':len(pred),'nota':'API real → Kafka → Spark → SQL → consulta Grafana. No se esperaron etiquetas a 15 minutos de esta nueva prueba.'}
guardar(ROOT/'reports/observabilidad/actualizacion_viva.json',report)
print({'estado':'verificada','conteos_observados':sorted({r['cobertura'][0]['predicciones'] for r in samples if r['cobertura']})})
