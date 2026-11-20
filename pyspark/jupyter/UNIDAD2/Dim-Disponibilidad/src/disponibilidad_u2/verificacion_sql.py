"""Reconciliación distribuida/SQL; solo recopila agregados pequeños."""
import json
from .utilidades import ahora


def verificar(spark,db,source,cfg,u1):
    from pyspark.sql import functions as F
    from .predictor_streaming import predecir
    flow=cfg['flow_id']
    obs=spark.read.option('recursiveFileLookup','true').parquet(str(source/'observaciones')).cache()
    try:
        n=obs.count();expected=predecir(obs,u1).count()
        rejected=obs.where('NOT elegible_pred_convencionales OR NOT elegible_pred_electricas').count()
        counts=db.json('''SELECT jsonb_build_object(
          'observaciones',(SELECT count(*) FROM disponibilidad.observaciones WHERE flujo=?),
          'rechazos',(SELECT count(*) FROM disponibilidad.rechazos WHERE flujo=?),
          'predicciones',(SELECT count(*) FROM disponibilidad.predicciones WHERE flujo=?),
          'evaluaciones',(SELECT count(*) FROM disponibilidad.evaluaciones WHERE flujo=?),
          'lotes',(SELECT count(*) FROM disponibilidad.lotes WHERE flujo=?),
          'lotes_sin_confirmacion',(SELECT count(*) FROM disponibilidad.lotes WHERE flujo=? AND confirmado_utc IS NULL))::text''',[flow]*6)
        assert (counts['observaciones'],counts['rechazos'],counts['predicciones'],counts['evaluaciones'])==(n,rejected,expected,expected),counts
        assert counts['lotes_sin_confirmacion']==0
        pending_ready=db.json('''SELECT to_json(count(*))::text FROM disponibilidad.evaluaciones e
           JOIN disponibilidad.predicciones p USING(flujo,prediccion_id)
           WHERE flujo=? AND e.estado='pendiente' AND e.cierre_no_antes_utc<=clock_timestamp()
           AND p.objetivo_utc+make_interval(secs=>?::int)<=(SELECT max(captura_utc)-make_interval(secs=>?::int)
             FROM disponibilidad.observaciones WHERE flujo=? AND contrato_valido)''',
           (flow,cfg['tolerance_seconds'],cfg['late_seconds'],flow))
        assert pending_ready==0,'Quedan evaluaciones maduras sin cerrar; drenar antes de verificar'
        native=(spark.read.parquet(str(source/'predicciones')) if next((source/'predicciones').rglob('*.parquet'),None)
                else predecir(obs.limit(0),u1))
        assert native.count()==expected
        assert native.select('prediccion_id').distinct().count()==expected
        checked=0;block=[]
        def check_block(block):
            matched=db.json('''SELECT to_json(count(*))::text FROM jsonb_array_elements(?::jsonb) x
              JOIN disponibilidad.predicciones p ON p.flujo=? AND p.prediccion_id=x->>'prediccion_id'
              WHERE p.event_id=x->>'event_id' AND p.tipo=x->>'tipo'
              AND p.horizonte_min=(x->>'horizonte_min')::int AND p.valor_predicho=(x->>'valor_predicho')::double precision
              AND p.origen_utc=(x->>'origen_utc')::timestamptz AND p.objetivo_utc=(x->>'objetivo_utc')::timestamptz
              AND p.version_predictor=x->>'version_predictor' AND p.contrato_sha256=x->>'contrato_sha256' ''',
              ('['+','.join(block)+']',flow))
            assert matched==len(block),'Predicciones Parquet y SQL diferentes'
        for raw in native.select('prediccion_id','event_id','tipo','horizonte_min','valor_predicho',
                     'origen_utc','objetivo_utc','version_predictor','contrato_sha256').toJSON().toLocalIterator():
            block.append(raw)
            if len(block)>=cfg['batch_rows']:check_block(block);checked+=len(block);block=[]
        if block:check_block(block);checked+=len(block)
        bad=db.json('''SELECT to_json(count(*))::text FROM disponibilidad.evaluaciones e
          JOIN disponibilidad.predicciones p USING(flujo,prediccion_id)
          LEFT JOIN disponibilidad.observaciones o ON o.flujo=e.flujo AND o.observacion_id=e.observacion_objetivo_id
          WHERE e.flujo=? AND (e.estado='evaluada' AND (
             e.captura_objetivo_utc<=p.origen_utc OR abs(e.error_emparejamiento_s)>?::int OR
             o.station_id IS DISTINCT FROM p.station_id OR NOT o.catalogo_compatible OR
             e.horizonte_efectivo_s IS DISTINCT FROM extract(epoch FROM e.captura_objetivo_utc-p.origen_utc)::double precision OR
             e.error IS DISTINCT FROM p.valor_predicho-e.valor_observado OR
             e.valor_observado IS DISTINCT FROM CASE WHEN p.tipo='convencionales' THEN o.convencionales ELSE o.electricas END OR
             NOT CASE WHEN p.tipo='convencionales' THEN o.elegible_convencionales ELSE o.elegible_electricas END OR
             e.cerrado_utc<e.cierre_no_antes_utc))''',(flow,cfg['tolerance_seconds']))
        assert bad==0
        # Materializar evita descomprimir el payload de origen por cada bandera
        # y por cada uno de sus seis pronósticos. Se comparan objetos completos.
        changed=db.json('''WITH origen AS MATERIALIZED (
          SELECT observacion_id,captura_utc,station_id,convencionales,electricas,
            (SELECT coalesce(jsonb_object_agg(key,value),'{}'::jsonb) FROM jsonb_each(o.payload)
               WHERE left(key,2)='q_') AS calidad
          FROM disponibilidad.observaciones o WHERE flujo=?)
          SELECT to_json(count(*))::text FROM disponibilidad.predicciones p
          JOIN origen o ON o.observacion_id=p.event_id
          WHERE p.flujo=? AND (p.origen_utc IS DISTINCT FROM o.captura_utc OR p.station_id IS DISTINCT FROM o.station_id OR
          p.valor_predicho IS DISTINCT FROM CASE WHEN p.tipo='convencionales' THEN o.convencionales ELSE o.electricas END
          OR (SELECT coalesce(jsonb_object_agg(key,value),'{}'::jsonb) FROM jsonb_each(p.payload)
             WHERE left(key,2)='q_') IS DISTINCT FROM o.calidad)''',(flow,flow))
        assert changed==0
        for c in u1['contratos']:
            assert db.json('''SELECT to_json(count(*))::text FROM disponibilidad.predicciones
              WHERE flujo=? AND tipo=? AND horizonte_min=?::int AND
              (contrato_sha256<>? OR version_predictor<>? OR ejecucion_u1<>?)''',
              (flow,c['tipo'],c['horizonte_min'],c['sha256'],c['version'],u1['ref']['ejecucion_id']))==0
        def rows(query,params=(flow,)):
            return db.json('SELECT coalesce(jsonb_agg(to_jsonb(x)),\'[]\'::jsonb)::text FROM ('+query+') x',params)
        states=rows('SELECT estado,motivo,count(*) n FROM disponibilidad.evaluaciones WHERE flujo=? GROUP BY estado,motivo ORDER BY estado,motivo')
        assert sum(x['n'] for x in states)==expected
        periods=rows('''SELECT tipo,horizonte_min,count(*) predicciones,
           count(*) FILTER(WHERE e.estado='evaluada') evaluadas,
           count(*) FILTER(WHERE e.estado='pendiente') pendientes,
           count(*) FILTER(WHERE e.estado='vencida') vencidas
           FROM disponibilidad.predicciones p JOIN disponibilidad.evaluaciones e USING(flujo,prediccion_id)
           WHERE flujo=? GROUP BY tipo,horizonte_min ORDER BY tipo,horizonte_min''')
        first=rows('''SELECT tipo,horizonte_min,e.estado,e.motivo,count(*) n FROM disponibilidad.predicciones p
           JOIN disponibilidad.evaluaciones e USING(flujo,prediccion_id)
           WHERE p.flujo=? AND p.origen_utc=(SELECT min(origen_utc) FROM disponibilidad.predicciones WHERE flujo=?)
           GROUP BY tipo,horizonte_min,e.estado,e.motivo ORDER BY tipo,horizonte_min,e.estado''',(flow,flow))
        latency=db.json('''SELECT row_to_json(x)::text FROM (SELECT
           min(captura_a_sql_confirmado_s) captura_a_sql_min_s,
           avg(captura_a_sql_confirmado_s) captura_a_sql_media_s,
           percentile_cont(0.95) WITHIN GROUP(ORDER BY captura_a_sql_confirmado_s) captura_a_sql_p95_s,
           max(captura_a_sql_confirmado_s) captura_a_sql_max_s,
           count(*) FILTER(WHERE captura_a_sql_confirmado_s>horizonte_min*60) predicciones_confirmadas_despues_objetivo,
           min(antiguedad_feed_s) antiguedad_feed_min_s,avg(antiguedad_feed_s) antiguedad_feed_media_s,
           max(antiguedad_feed_s) antiguedad_feed_max_s,
           avg(antiguedad_reporte_s) antiguedad_reporte_media_s,
           min(objetivo_a_evaluacion_confirmada_s) objetivo_a_evaluacion_min_s,
           avg(objetivo_a_evaluacion_confirmada_s) objetivo_a_evaluacion_media_s,
           max(objetivo_a_evaluacion_confirmada_s) objetivo_a_evaluacion_max_s
           FROM disponibilidad.latencias WHERE flujo=?) x''',(flow,))
        cadence=rows('''SELECT capture_id,captura_utc,last_updated_utc,
             extract(epoch FROM captura_utc-lag(captura_utc) OVER(ORDER BY captura_utc)) intervalo_consulta_observado_s,
             extract(epoch FROM last_updated_utc-lag(last_updated_utc) OVER(ORDER BY captura_utc)) intervalo_actualizacion_fuente_s
           FROM (SELECT DISTINCT capture_id,captura_utc,last_updated_utc FROM disponibilidad.observaciones WHERE flujo=?) t
           ORDER BY captura_utc''')
        metrics=rows('SELECT * FROM disponibilidad.metricas WHERE flujo=? ORDER BY tipo,horizonte_min,ventana_utc,periodo,discrepancias')
        global_metrics=rows('''SELECT tipo,horizonte_min,periodo,discrepancias,sum(n) n,
            sum(n*mae)/sum(n) mae,sqrt(sum(n*rmse*rmse)/sum(n)) rmse,sum(n*sesgo)/sum(n) sesgo
            FROM disponibilidad.metricas WHERE flujo=? GROUP BY tipo,horizonte_min,periodo,discrepancias
            ORDER BY tipo,horizonte_min,periodo,discrepancias''')
        coverage=rows('SELECT * FROM disponibilidad.cobertura WHERE flujo=? ORDER BY tipo,horizonte_min,ventana_utc')
        maxeval=db.json('''SELECT row_to_json(x)::text FROM(SELECT min(captura_utc) inicio_utc,max(captura_utc) fin_utc,
          extract(epoch FROM max(captura_utc)-min(captura_utc)) periodo_observado_s,
          count(DISTINCT capture_id) capturas FROM disponibilidad.observaciones WHERE flujo=?) x''',(flow,))
        sample=rows('''SELECT p.prediccion_id,p.station_id,p.tipo,p.horizonte_min,p.origen_utc,p.objetivo_utc,p.valor_predicho,
          e.observacion_objetivo_id,e.captura_objetivo_utc,e.horizonte_efectivo_s,e.error_emparejamiento_s,
          e.valor_observado,e.error,e.estado,e.estrato_discrepancia FROM disponibilidad.predicciones p
          JOIN disponibilidad.evaluaciones e USING(flujo,prediccion_id)
          WHERE p.flujo=? AND e.estado='evaluada' ORDER BY p.horizonte_min DESC,p.prediccion_id LIMIT 6''')
        return {'estado':'verificada','fecha_utc':ahora(),'flujo':flow,'version_u1':u1['ref']['ejecucion_id'],
          'conteos':counts,'periodo':maxeval,'errores_integridad':bad,'diferencias_persistencia_calidad':changed,
          'predicciones_parquet_sql_comparadas_por_id_y_valores':checked,
          'pendientes_cerrables_sin_resolver':pending_ready,
          'estados':states,'por_tipo_horizonte':periods,'primera_cohorte':first,'latencias':latency,
          'cadencia':cadence,'metricas':metrics,'metricas_globales':global_metrics,'cobertura':coverage,'muestra_evaluaciones':sample,
          'nota':'Agregados de datos reales del flujo indicado; las pruebas sintéticas usan otros flow_id.'}
    finally:obs.unpersist()
