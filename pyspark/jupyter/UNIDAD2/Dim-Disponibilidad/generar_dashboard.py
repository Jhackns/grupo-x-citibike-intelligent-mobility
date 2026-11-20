#!/usr/bin/env python3
"""Genera el dashboard y catálogo SQL versionables, sin configuración secreta."""
from pathlib import Path
import json
import sys
sys.path.insert(0,str(Path(__file__).resolve().parent/"src"))
from disponibilidad_u2.consultas_evaluacion import resumen
ROOT=Path(__file__).resolve().parent
PG={'type':'postgres','uid':'disponibilidad-pg'}
PROM={'type':'prometheus','uid':'disponibilidad-prom'}
SCOPE="flujo IN (${flujo:sqlstring}) AND ('__all' IN (${estacion:sqlstring}) OR station_id IN (${estacion:sqlstring}))"
PRED=SCOPE+" AND tipo IN (${tipo:sqlstring}) AND horizonte_min::text IN (${horizonte:sqlstring}) AND $__timeFilter(origen_utc)"
TARGET_PRED=PRED.replace('$__timeFilter(origen_utc)','$__timeFilter(objetivo_utc)')
queries={
'estado':'''WITH f AS MATERIALIZED(SELECT * FROM grafana_disponibilidad.flujos WHERE flujo IN (${flujo:sqlstring})),
 s AS(SELECT flujo,count(*) n,count(activo) conocidos,bool_and(activo) activos,max(telemetria_antigua::int) antigua
 FROM grafana_disponibilidad.componentes WHERE componente IN ('productor','spark','sql') GROUP BY flujo)
 SELECT f.flujo,f.ultima_captura_utc,f.ultima_actualizacion_utc,
 extract(epoch FROM clock_timestamp()-f.ultima_captura_utc)::double precision edad_ultima_captura_s,
 extract(epoch FROM clock_timestamp()-f.ultima_actualizacion_utc)::double precision edad_fuente_ahora_s,
 CASE WHEN f.ultima_captura_utc IS NULL THEN 'SIN CAPTURAS'
 WHEN s.n IS NULL OR s.conocidos<3 THEN 'SIN TELEMETRIA'
 WHEN s.antigua=1 THEN 'TELEMETRIA ANTIGUA'
 WHEN NOT s.activos THEN 'DETENIDO'
 WHEN clock_timestamp()-f.ultima_captura_utc>make_interval(secs=>f.umbral_datos_s) THEN 'SIN CAPTURAS RECIENTES'
 ELSE 'ACTIVO' END estado_flujo,f.ejecucion_u1 FROM f LEFT JOIN s USING(flujo)''',
'actual':f'''SELECT DISTINCT ON(station_id) station_id,captura_utc,last_updated_utc,last_reported_utc,
 CASE WHEN ${{tipo:sqlstring}}='convencionales' THEN convencionales ELSE electricas END conteo_reportado,
 CASE WHEN ${{tipo:sqlstring}}='convencionales' THEN elegible_convencionales ELSE elegible_electricas END elegible,
 discrepancia,CASE WHEN clock_timestamp()-captura_utc>make_interval(secs=>f.umbral_datos_s) THEN 'ANTIGUA' ELSE 'RECIENTE' END vigencia
 FROM grafana_disponibilidad.observaciones JOIN grafana_disponibilidad.flujos f USING(flujo) WHERE {SCOPE} AND $__timeFilter(captura_utc)
 ORDER BY station_id,captura_utc DESC LIMIT 100''',
'predicciones':f'''SELECT station_id,tipo,horizonte_min,origen_utc,objetivo_utc,valor_predicho,disponible_sql_utc,
 CASE WHEN objetivo_utc<clock_timestamp() THEN 'CADUCADA' ELSE 'VIGENTE' END vigencia,version_predictor
 FROM(SELECT DISTINCT ON(station_id,tipo,horizonte_min) * FROM grafana_disponibilidad.predicciones
 WHERE {PRED} ORDER BY station_id,tipo,horizonte_min,origen_utc DESC) p LIMIT 100''',
'observado_predicho':f'''SELECT objetivo_utc AS time,avg(CASE WHEN estado='evaluada' THEN valor_predicho END) AS "Predicho emparejado (media estaciones)",
 avg(CASE WHEN estado='evaluada' THEN valor_observado END) AS "Observado elegible (media estaciones)"
 FROM grafana_disponibilidad.predicciones WHERE {TARGET_PRED} GROUP BY objetivo_utc ORDER BY objetivo_utc''',
'metricas':f'''SELECT count(*) FILTER(WHERE estado='evaluada') AS n,
 avg(abs(error)) FILTER(WHERE estado='evaluada') AS mae,
 sqrt(avg(error*error) FILTER(WHERE estado='evaluada')) AS rmse,
 avg(error) FILTER(WHERE estado='evaluada') AS sesgo
 FROM grafana_disponibilidad.predicciones WHERE {PRED} HAVING count(*)>0''',
'estratos':f'''SELECT CASE WHEN cambio THEN 'con_cambios' ELSE 'sin_cambios' END periodo,
 estrato_discrepancia,count(*) AS n,avg(abs(error)) mae,sqrt(avg(error*error)) rmse,avg(error) sesgo
 FROM grafana_disponibilidad.predicciones WHERE {PRED} AND estado='evaluada'
 GROUP BY cambio,estrato_discrepancia ORDER BY periodo,estrato_discrepancia''',
'cobertura':f'''WITH f AS MATERIALIZED(SELECT * FROM grafana_disponibilidad.flujos WHERE flujo IN (${{flujo:sqlstring}})),
 p AS(SELECT p.*,CASE WHEN estado<>'pendiente' THEN estado
 WHEN cierre_no_antes_utc>clock_timestamp() THEN 'pendiente_inmadura'
 WHEN objetivo_utc+make_interval(secs=>f.tolerancia_s)>f.ultima_captura_utc-make_interval(secs=>f.tardios_s)
 OR f.ultima_captura_utc IS NULL THEN 'pendiente_sin_avance' ELSE 'pendiente_cerrable' END estado_detallado
 FROM grafana_disponibilidad.predicciones p JOIN f USING(flujo) WHERE {PRED})
 SELECT count(*) predicciones,count(*) FILTER(WHERE estado='evaluada') evaluadas,
 count(*) FILTER(WHERE estado='vencida') vencidas,count(*) FILTER(WHERE estado='pendiente') pendientes,
 count(*) FILTER(WHERE estado_detallado='pendiente_inmadura') pendientes_inmaduras,
 count(*) FILTER(WHERE estado_detallado='pendiente_sin_avance') pendientes_sin_avance,
 count(*) FILTER(WHERE estado_detallado='pendiente_cerrable') pendientes_cerrables,
 100.0*count(*) FILTER(WHERE estado='evaluada')/count(*) cobertura_pct FROM p HAVING count(*)>0''',
'vencidas':f'''SELECT motivo,count(*) n FROM grafana_disponibilidad.predicciones
 WHERE {PRED} AND estado='vencida' GROUP BY motivo ORDER BY n DESC''',
'calidad':f'''SELECT count(*) observaciones,count(*) FILTER(WHERE rechazado_total_o_parcial) rechazos_totales_o_parciales,
 count(*) FILTER(WHERE NOT CASE WHEN ${{tipo:sqlstring}}='convencionales' THEN elegible_convencionales ELSE elegible_electricas END) no_elegibles_tipo,
 count(*) FILTER(WHERE discrepancia) discrepantes,count(*) FILTER(WHERE discrepancia=false) sin_discrepancias,
 count(*) FILTER(WHERE discrepancia IS NULL) calidad_desconocida
 FROM grafana_disponibilidad.observaciones WHERE {SCOPE} AND $__timeFilter(captura_utc) HAVING count(*)>0''',
'latencia':f'''SELECT to_timestamp(floor(extract(epoch FROM origen_utc)/300)*300) AS time,
 avg(latencia_sql_s) AS "Media captura a SQL (s)",
 percentile_cont(0.95) WITHIN GROUP(ORDER BY latencia_sql_s) AS "P95 captura a SQL (s)"
 FROM grafana_disponibilidad.predicciones WHERE {PRED} GROUP BY time ORDER BY time''',
'emparejamientos':f'''SELECT station_id,tipo,horizonte_min,origen_utc,objetivo_utc,captura_objetivo_utc,
 horizonte_efectivo_s,error_emparejamiento_s,valor_predicho,valor_observado,estado,motivo
 FROM grafana_disponibilidad.predicciones WHERE {PRED} AND estado<>'pendiente' ORDER BY objetivo_utc DESC LIMIT 100'''
}
queries['metricas']='WITH resumen AS ('+resumen(PRED,filtro_flujo="flujo IN (${flujo:sqlstring})")+''') SELECT n,mae,rmse,sesgo,numerador,denominador,cobertura_pct,
 evaluadas_origen_inicio_utc,evaluadas_origen_fin_utc,evaluadas_objetivo_inicio_utc,evaluadas_objetivo_fin_utc,
 tipo,horizonte_min,alcance,flujo,momento,instante_corte_utc FROM resumen'''
queries['cobertura']='WITH resumen AS ('+resumen(PRED,comparar_cortes=True,filtro_flujo="flujo IN (${flujo:sqlstring})")+''') SELECT momento,numerador,denominador,cobertura_pct,
 predicciones,evaluadas,vencidas,pendientes,pendientes_inmaduras,pendientes_flujo_finalizado,
 pendientes_sin_avance_no_atribuido,pendientes_cerrables,pendientes_sin_avance,
 vencidas_sin_captura,vencidas_objetivo_no_elegible,vencidas_trayecto,vencidas_ambiguas,vencidas_otras,
 origen_inicio_utc,origen_fin_utc,evaluadas_origen_inicio_utc,evaluadas_origen_fin_utc,
 evaluadas_objetivo_inicio_utc,evaluadas_objetivo_fin_utc,instante_corte_utc,tipo,horizonte_min,flujo FROM resumen'''
panels=[]
def sql_panel(key,title,pos,kind='table',description=''):
    panel={'id':len(panels)+1,'title':title,'type':kind,'datasource':PG,'gridPos':dict(zip(('x','y','w','h'),pos)),
      'description':description,'fieldConfig':{'defaults':{'noValue':'Sin datos / sin muestras'},'overrides':[]},
      'targets':[{'refId':'A','datasource':PG,'rawSql':queries[key],'format':'time_series' if kind=='timeseries' else 'table','editorMode':'code','rawQuery':True}],
      'options':{'showHeader':True}}
    if kind=='timeseries':
        panel['fieldConfig']['defaults']['custom']={'drawStyle':'line','spanNulls':False,'insertNulls':False,'lineInterpolation':'linear','showPoints':'auto'}
        panel['options']={'legend':{'displayMode':'list','placement':'bottom'},'tooltip':{'mode':'multi'}}
    panel['fieldConfig']['overrides'].extend([
      {'matcher':{'id':'byRegexp','options':'.*_utc$'},'properties':[{'id':'unit','value':'dateTimeAsIso'}]},
      {'matcher':{'id':'byName','options':'cobertura_pct'},'properties':[{'id':'unit','value':'percent'},{'id':'decimals','value':2}]},
      {'matcher':{'id':'byRegexp','options':'^(mae|rmse|sesgo)$'},'properties':[{'id':'decimals','value':6}]}
    ])
    panels.append(panel)

sql_panel('estado','Estado actual del flujo y relojes UTC',(0,0,24,5),description='Estado operativo actual, independiente del periodo histórico. DETENIDO no significa conteo cero. Antigüedad usa reloj actual.')
sql_panel('actual','Última disponibilidad reportada — hasta 100 estaciones',(0,5,12,7),description='Último dato dentro del periodo seleccionado. ANTIGUA conserva el valor y su fecha; no representa disponibilidad física actual confirmada. Horizonte no aplica.')
sql_panel('predicciones','Últimas predicciones — vigencia explícita',(12,5,12,7),description='Fecha objetivo = captura + horizonte. CADUCADA no es un pronóstico actual. Hasta 100 estaciones.')
sql_panel('observado_predicho','Observado frente a predicho — fecha objetivo ideal',(0,12,16,8),'timeseries','Ambas series se alinean por fecha objetivo; no por last_reported. Observado ausente permanece NULL. Si selecciona todas las estaciones se muestran medias, no sumas.')
sql_panel('metricas','Errores del subconjunto evaluado — n y periodo real',(16,12,8,8),description='MAE/RMSE/sesgo describen exclusivamente los n pares evaluados; el resto no tiene error conocido. Se muestran intervalos efectivos de origen/objetivo UTC y cobertura madura.')
sql_panel('cobertura','Cobertura madura — fin de prueba y auditoría actual',(0,20,16,6),description='Numerador: evaluadas maduras; denominador: predicciones maduras; sin maduras = NULL. Desglose de finalización del flujo, falta de captura, calidad futura y otras causas. No son errores globales.')
sql_panel('vencidas','Vencidas — motivos conservados',(16,20,8,6),description='No se modificó ±30 s. El informe auditó las 19.572 vencidas de etapa 3 sin encontrar falsos faltantes.')
sql_panel('calidad','Calidad: observaciones, rechazos y discrepancias',(0,26,12,6),description='Rechazos están incluidos en observaciones. Discrepancias no invalidan automáticamente. Nulos se cuentan como calidad desconocida; horizonte no aplica.')
sql_panel('latencia','Captura → predicción confirmada en PostgreSQL',(12,26,12,6),'timeseries','Confirmación post-commit es una cota superior de visibilidad. Incluye espera/backlog; no incluye el margen deliberado de evaluación ni la duración total de prueba.')
sql_panel('estratos','Errores por cambios y discrepancias',(0,32,12,8),description='Estratos de origen/objetivo disponibles solo después de evaluar. No se usan como predictores ni se presentan como exactitud física.')
sql_panel('emparejamientos','Trazabilidad del emparejamiento — 100 resueltos',(12,32,12,8),description='Muestra la captura realmente observada y el horizonte efectivo. Vencidas sin resultado conservan NULL.')

def prom_panel(title,expressions,pos,description):
    panels.append({'id':len(panels)+1,'type':'timeseries','title':title,'gridPos':dict(zip(('x','y','w','h'),pos)),
      'datasource':PROM,'description':description,'fieldConfig':{'defaults':{'noValue':'Sin telemetría','custom':{'spanNulls':False}},'overrides':[]},
      'targets':[{'refId':chr(65+i),'expr':e,'datasource':PROM,'legendFormat':name} for i,(e,name) in enumerate(expressions)]})

prom_panel('Técnica: componentes de la ejecución observada',[
 ('disponibilidad_componente_up','{{componente}}'),('up{job="disponibilidad-aplicaciones"}','scrape colector')],(0,40,12,7),'Telemetría de la ejecución registrada, no del modelo histórico seleccionado. 0 = proceso/servicio detenido; serie ausente = sin telemetría.')
prom_panel('Productor: confirmaciones, consultas y fallos',[
 ('rate((disponibilidad_productor_eventos_confirmados_total == disponibilidad_productor_eventos_confirmados_total)[2m:5s])','ACK/s'),('rate((disponibilidad_productor_consultas_total == disponibilidad_productor_consultas_total)[2m:5s])','consultas/s'),
 ('disponibilidad_productor_fallos_publicacion_total','fallos publicación'),('disponibilidad_productor_reintentos_http_total','reintentos HTTP')],(12,40,12,7),'Counters del productor observado. ACK confirmado, no mero envío. Sin etiquetas de estación, evento o ejecución.')
prom_panel('Kafka: offsets y backlog estimado por partición',[
 ('kafka_topic_partition_current_offset{topic="citibike-disponibilidad-estaciones-v1"}','offset {{partition}}'),
 ('disponibilidad_kafka_backlog_estimado_eventos','backlog estimado {{partition}}'),('kafka_brokers','brokers')],(0,47,12,7),'Backlog estimado con offsets de Spark y última muestra Kafka; no es consumer-group lag ni exactly-once. Si faltan datos o no se alinean temporalmente, NaN.')
prom_panel('Spark: microbatches y estado',[
 ('disponibilidad_spark_duracion_lote_seconds','lote s {{consulta}}'),('disponibilidad_spark_filas_estado','estado filas {{consulta}}'),
 ('disponibilidad_spark_descartes_watermark_ultimo_lote','tardíos último lote {{consulta}}')],(12,47,12,7),'Progreso Structured Streaming. Métricas de ejecución no son conteos de eventos de negocio únicos. Son gauges de último lote, no counters acumulados.')
panels.append({'id':len(panels)+1,'type':'text','title':'Contrato y alcance',( 'gridPos'):{'x':0,'y':54,'w':24,'h':4},'options':{'mode':'markdown','content':'Conteos **reportados** por tipo: 1 bicycle/human, 2 bicycle/electric_assist. Persistencia seleccionada U1 **20261117T171917344468Z**; no necesita entrenamiento. Exactitud física no confirmada. Filtros estación/tipo/horizonte/periodo aplican a negocio; técnica muestra la ejecución observada; el snapshot batch solo responde a tipo y declara su fecha fija. UTC; datos antiguos y nulos se indican explícitamente. Paneles no acreditan generalización semanal o estacional.'}})

variables=[{'name':'flujo','label':'Ejecución SQL','type':'query','datasource':PG,'query':'SELECT flujo AS __text,flujo AS __value FROM grafana_disponibilidad.flujos ORDER BY flujo',
 'refresh':1,'current':{'text':'disponibilidad-correcciones-20261119-v1','value':'disponibilidad-correcciones-20261119-v1'}},
 {'name':'estacion','label':'Estación','type':'query','datasource':PG,'query':"SELECT 'Todas las estaciones' AS __text,'__all' AS __value UNION ALL SELECT station_id AS __text,station_id AS __value FROM grafana_disponibilidad.estaciones WHERE flujo IN (${flujo:sqlstring}) ORDER BY __text",
 'refresh':1,'includeAll':False,'current':{'text':'Todas las estaciones','value':'__all'}},
 {'name':'tipo','label':'Tipo reportado','type':'custom','query':'convencionales,electricas','current':{'text':'convencionales','value':'convencionales'},
 'options':[{'text':x,'value':x,'selected':x=='convencionales'} for x in ('convencionales','electricas')]},
 {'name':'horizonte','label':'Horizonte (min)','type':'custom','query':'5,10,15','current':{'text':'5','value':'5'},
 'options':[{'text':x,'value':x,'selected':x=='5'} for x in ('5','10','15')]}]
panels.append({'id':17,'type':'stat','title':'KPIs batch — snapshot con fecha y n explícitos',
 'gridPos':{'x':0,'y':65,'w':24,'h':6},'datasource':PROM,
 'description':'Dos KPIs batch de la captura 8ff5bace-0ecf-499d-aa8a-bf43d57a3a0a del 19/11/2026. Fuente exacta: reports/correcciones/kpis_batch.json. Solo el filtro tipo aplica; estación, flujo y horizonte no cambian este snapshot. La edad indica que no es inventario actual.',
 'fieldConfig':{'defaults':{'noValue':'Sin snapshot batch'},'overrides':[{'matcher':{'id':'byFrameRefID','options':'E'},'properties':[{'id':'unit','value':'dateTimeAsIso'}]}]},
 'options':{'reduceOptions':{'calcs':['lastNotNull'],'fields':'','values':False},'textMode':'value_and_name','colorMode':'none','graphMode':'none'},
 'targets':[{'refId':chr(65+i),'datasource':PROM,'expr':expr,'instant':True,'legendFormat':name}
  for i,(expr,name) in enumerate([
   ('disponibilidad_batch_bicicletas_media{job="disponibilidad_batch",tipo=~"${tipo:regex}"}','Media en estaciones elegibles — batch'),
   ('disponibilidad_batch_fraccion_sin_bicicletas{job="disponibilidad_batch",tipo=~"${tipo:regex}"}','Fracción elegibles sin bicicletas (0–1) — batch'),
   ('time()-disponibilidad_batch_datos_timestamp_seconds{job="disponibilidad_batch",tipo=~"${tipo:regex}"}','Edad snapshot batch (s)'),
   ('disponibilidad_batch_estaciones_elegibles{job="disponibilidad_batch",tipo=~"${tipo:regex}"}','n estaciones elegibles — batch'),
   ('disponibilidad_batch_datos_timestamp_seconds{job="disponibilidad_batch",tipo=~"${tipo:regex}"}*1000','Fecha snapshot UTC')])]})

dashboard={'id':None,'uid':'citibike-disponibilidad-u2','title':'Citi Bike — Disponibilidad reportada y observabilidad',
 'schemaVersion':40,'version':2,'editable':False,'timezone':'utc','refresh':'30s','time':{'from':'now-2d','to':'now'},
 'tags':['citibike','disponibilidad','U2'],'templating':{'list':variables},'panels':panels}
# La tabla de errores muestra periodos y denominadores: ancho completo, mismos 16 paneles.
for panel in panels:
    if panel['id']==4:panel['gridPos']['w']=24
    elif panel['id']==5:
        panel['gridPos'].update(x=0,y=20,w=24,h=7)
        for field,width in [('n',60),('mae',90),('rmse',90),('sesgo',90),('numerador',75),('denominador',75),('cobertura_pct',80)]:
            panel['fieldConfig']['overrides'].append({'matcher':{'id':'byName','options':field},'properties':[{'id':'custom.width','value':width}]})
        panel['fieldConfig']['overrides'].append({'matcher':{'id':'byRegexp','options':'^evaluadas_.*_utc$'},'properties':[{'id':'custom.width','value':180}]})
    elif panel['id']>=6:panel['gridPos']['y']+=7

if __name__=='__main__':
    p=ROOT/'observabilidad/dashboards/disponibilidad.json';p.parent.mkdir(parents=True,exist_ok=True)
    p.write_text(json.dumps(dashboard,ensure_ascii=False,indent=2)+'\n')
    (ROOT/'observabilidad/consultas.json').write_text(json.dumps(queries,ensure_ascii=False,indent=2)+'\n')
    print('Dashboard y catálogo SQL generados sin secretos:',len(panels),'paneles')
