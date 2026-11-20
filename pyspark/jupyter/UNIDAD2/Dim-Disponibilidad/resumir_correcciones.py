#!/usr/bin/env python3
"""Informe reproducible del ensayo explícito, cohortes separadas y espacio medido."""
from pathlib import Path
import json,sys
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT/'src'))
from disponibilidad_u2.observabilidad_sql import consultar
from disponibilidad_u2.consultas_evaluacion import resumen
from disponibilidad_u2.grafana_api import literal
from disponibilidad_u2.utilidades import guardar,ahora


def table(headers,rows):
    return '| '+' | '.join(headers)+' |\n| '+' | '.join(['---']*len(headers))+' |\n'+''.join('| '+' | '.join(map(str,row))+' |\n' for row in rows)

def fmt(x):return 'NULL — sin muestra' if x is None else (f'{x:.6f}' if isinstance(x,float) else str(x))

def main():
    run=json.loads((ROOT/'reports/correcciones/ejecucion_viva.json').read_text());assert run['estado']=='completada',run.get('errores')
    f=run['flujo'];cap=run['cohorte']['capture_id'];last=run['produccion']['capturas'][-1]['capture_id'];flow='flujo='+literal(f)
    result={'fecha_auditoria_utc':ahora(),'estado':'verificada','flujo':f,'cohorte_capture_id':cap,'ultima_capture_id':last,'ultima_captura_emitida_utc':run['produccion']['capturas'][-1]['captured_at_utc'],
      'inicio_ejecucion_utc':run['inicio_utc'],'fin_ejecucion_utc':run['fin_utc'],'cohorte':run['cohorte'],
      'capturas':len(run['produccion']['capturas']),'observaciones_sql':run['observaciones_sql'],'poblaciones':{}}
    for name,where in [('cohorte_inicial',flow+' AND capture_id='+literal(cap)),('ultima_captura',flow+' AND capture_id='+literal(last)),('posteriores_a_cohorte',flow+' AND capture_id<>'+literal(cap)),('flujo_completo',flow)]:
        q=resumen(where,comparar_cortes=True,filtro_flujo=flow,corte_actual=literal(result['fecha_auditoria_utc'])+'::timestamptz')
        result['poblaciones'][name]=consultar('SELECT jsonb_agg(to_jsonb(r)) FROM ('+q+') r')
    origin=literal(run['cohorte']['origen_utc']);result['capturas_cercanas']=consultar('''WITH c AS(SELECT DISTINCT captura_utc FROM disponibilidad.observaciones WHERE flujo='''+literal(f)+'''),r AS(
      SELECT h,''' +origin+'''::timestamptz+make_interval(mins=>h) objetivo_utc FROM unnest(ARRAY[5,10,15])h)
      SELECT jsonb_agg(jsonb_build_object('horizonte_min',h,'objetivo_utc',objetivo_utc,
       'capturas_en_banda',(SELECT count(*) FROM c WHERE abs(extract(epoch FROM captura_utc-objetivo_utc))<=30),
       'cercanas',(SELECT jsonb_agg(to_jsonb(x)) FROM(SELECT captura_utc,extract(epoch FROM captura_utc-objetivo_utc) desfase_s FROM c ORDER BY abs(extract(epoch FROM captura_utc-objetivo_utc)),captura_utc LIMIT 2)x))) FROM r''')
    result['motivos_cohorte']=consultar('''SELECT jsonb_agg(to_jsonb(r)) FROM(SELECT p.tipo,p.horizonte_min,e.motivo,count(*) n
      FROM disponibilidad.predicciones p JOIN disponibilidad.evaluaciones e USING(flujo,prediccion_id)
      JOIN disponibilidad.observaciones o ON o.flujo=p.flujo AND o.observacion_id=p.event_id
      WHERE p.flujo='''+literal(f)+' AND o.capture_id='+literal(cap)+''' AND e.estado='vencida'
      GROUP BY p.tipo,p.horizonte_min,e.motivo ORDER BY p.tipo,p.horizonte_min,e.motivo)r''')
    result['estratos_cohorte']=consultar('''SELECT jsonb_agg(to_jsonb(r)) FROM(SELECT p.tipo,p.horizonte_min,e.cambio,e.estrato_discrepancia,
      count(*) n,avg(abs(e.error)) mae,sqrt(avg(e.error*e.error)) rmse,avg(e.error) sesgo
      FROM disponibilidad.predicciones p JOIN disponibilidad.evaluaciones e USING(flujo,prediccion_id)
      JOIN disponibilidad.observaciones o ON o.flujo=p.flujo AND o.observacion_id=p.event_id
      WHERE p.flujo='''+literal(f)+' AND o.capture_id='+literal(cap)+''' AND e.estado='evaluada'
      GROUP BY p.tipo,p.horizonte_min,e.cambio,e.estrato_discrepancia ORDER BY p.tipo,p.horizonte_min,e.cambio,e.estrato_discrepancia)r''')
    result['tablas_bytes']=consultar("SELECT jsonb_object_agg(relname,pg_total_relation_size(c.oid)) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='disponibilidad' AND relname IN ('observaciones','predicciones','evaluaciones','rechazos')")
    baseline=json.loads((ROOT/'reports/observabilidad/resumen_sql.json').read_text())
    delta=sum(result['tablas_bytes'].values())-sum(baseline['tamano_tablas_bytes'].values())
    result['almacenamiento']={'baseline_fecha':baseline['fecha_utc'],'baseline_bytes':sum(baseline['tamano_tablas_bytes'].values()),'actual_bytes':sum(result['tablas_bytes'].values()),'incremento_bytes':delta,
       'incremento_mib_por_captura':delta/result['capturas']/1024**2,'nota':'Diferencia física aproximada de cuatro tablas compartidas; incluye índices/bloat, influida por vacuum. No incluye WAL, ni es tamaño lógico exclusivo por flow.'}
    frozen=ROOT/'reports/correcciones/medicion_antes_promql.json'
    if frozen.exists():
        measured=json.loads(frozen.read_text())
        result['tablas_bytes_actuales_incluyen_otros_ensayos']=result['tablas_bytes']
        result['tablas_bytes']=measured['tablas_bytes']
        result['almacenamiento']=measured['almacenamiento']
        result['almacenamiento']['medicion_congelada']='reports/correcciones/medicion_antes_promql.json'
    sql=(ROOT/'reports/cierre/consulta_latencias.sql').read_text();pair="'disponibilidad-etapa3-vivo-20261119-v1','disponibilidad-etapa4-vivo-20261119-v1'";assert pair in sql
    sql=sql.replace(pair,literal(f));(ROOT/'reports/correcciones/consulta_latencias.sql').write_text(sql)
    result['latencias']=consultar(sql)['latencias']
    for rows in result['poblaciones'].values():
        for r in rows:
            assert r['predicciones']==r['evaluadas']+r['vencidas']+r['pendientes']
            assert r['denominador']==r['predicciones']-r['pendientes_inmaduras']
    initial=[r for r in result['poblaciones']['cohorte_inicial'] if r['momento']=='fin_prueba'];assert len(initial)==6 and all(r['pendientes']==0 for r in initial)
    guardar(ROOT/'reports/correcciones/resultados.json',result)
    text='# Correcciones U2: resultados de la prueba continua\n\n'
    text+='Fuente explícita: `'+f+'`; '+str(result['capturas'])+' capturas, '+str(result['observaciones_sql'])+' observaciones SQL.\n\n'
    text+='Cohorte primaria congelada: `'+cap+'`, origen '+run['cohorte']['origen_utc']+'. Plazo mínimo '+run['cohorte']['plazo_minimo_utc']+'. Frontier observado al cerrar cohorte '+run['cohorte']['ultima_captura_utc']+'. Captura final emitida '+result['ultima_captura_emitida_utc']+'. Cierre de procesos '+run['fin_utc']+'.\n\n'
    text+='La captura siguió hasta superar 15 min + 30 s + 600 s de margen y cerrar los seis grupos. No se alteraron cantidades, tolerancias ni calidad. Los errores siguientes describen **solo el subconjunto evaluado**, no todos los pronósticos; las ausencias no tienen error conocido.\n\n'
    headers=['Tipo','Min','n evaluada','MAE del subconjunto','RMSE del subconjunto','Sesgo','Numerador','Denominador maduro','Cobertura %','Vencidas']
    text+=table(headers,[[r['tipo'],r['horizonte_min'],r['n'],fmt(r['mae']),fmt(r['rmse']),fmt(r['sesgo']),r['numerador'],r['denominador'],fmt(r['cobertura_pct']),r['vencidas']] for r in initial])
    text+='\nA 10 minutos no hubo captura en la banda de ±30 s: el resultado es vencimiento sin error medible, no MAE cero. No se cambió de cohorte después de conocer ese resultado. Los desfases y motivos exactos están en resultados.json; otras cohortes maduras del flujo se reportan por separado.\n\n'
    text+=table(['Tipo','Min','Motivo comprobado','n'],[[r['tipo'],r['horizonte_min'],r['motivo'],r['n']] for r in result['motivos_cohorte']])
    for name in ['ultima_captura','posteriores_a_cohorte','flujo_completo']:
        text+='\n## '+name+'\n\n'
        text+=table(['Tipo','Min','Corte','Evaluadas','Vencidas','Inmaduras','Sin avance con flujo finalizado','Cerrables','Numerador','Denominador','Cobertura %'],[[r['tipo'],r['horizonte_min'],r['momento'],r['evaluadas'],r['vencidas'],r['pendientes_inmaduras'],r['pendientes_flujo_finalizado'],r['pendientes_cerrables'],r['numerador'],r['denominador'],fmt(r['cobertura_pct'])] for r in result['poblaciones'][name]])
    text+='\n`fin_prueba` usa metadatos y confirmaciones disponibles al cierre; `auditoria_actual` aplica el reloj de auditoría. Un plazo todavía inmaduro no entra al denominador. Una pendiente madura sin avance después de una finalización demostrada se identifica como tal, sin convertirla en vencida.\n\n## Latencias y periodos\n\n'
    text+=table(['Medida','n','Mediana s','p95 s','Desde UTC','Hasta UTC'],[[r['metrica'],r['n'],fmt(r['mediana_s']),fmt(r['p95_s']),r['periodo_inicio_utc'],r['periodo_fin_utc']] for r in result['latencias']])
    text+='\nAntigüedad del feed/reporte no es procesamiento. Captura→marca Spark no mide el fin de escritura; captura→SQL confirmado incluye espera y persistencia. Demora de evaluación incluye margen deliberado y colas. Los n por predicción comparten capturas y no son muestras temporales independientes.\n\n'
    text+='Ver [verificación de cobertura y tasas](verificacion.json), [reinicio y alerta HTTP](reinicio_exportador.json), [KPIs batch](kpis_batch.json) y [auditoría actualizada](auditoria_actualizada.md).\n'
    (ROOT/'reports/correcciones/informe.md').write_text(text)
    print({'estado':'verificada','cohorte':cap,'capturas':result['capturas'],'mib_por_captura':result['almacenamiento']['incremento_mib_por_captura']})

if __name__=='__main__':main()
