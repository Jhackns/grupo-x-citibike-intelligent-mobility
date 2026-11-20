-- Cada fila de muestras identifica su granularidad; no confundir relojes.
BEGIN READ ONLY;
SET LOCAL TIME ZONE 'UTC';
WITH obs AS MATERIALIZED (
 SELECT * FROM disponibilidad.observaciones WHERE flujo IN
 ('disponibilidad-correcciones-20261119-v1')
), cap AS MATERIALIZED (
 SELECT flujo,capture_id,min(captura_utc) captura_utc,min(last_updated_utc) fuente_utc
 FROM obs WHERE contrato_valido GROUP BY flujo,capture_id
), intcap AS (
 SELECT *,extract(epoch FROM captura_utc-lag(captura_utc) OVER(PARTITION BY flujo ORDER BY captura_utc)) s FROM cap
), pred AS MATERIALIZED (
 SELECT p.*,b.confirmado_utc FROM disponibilidad.predicciones p
 JOIN disponibilidad.lotes b ON b.flujo=p.flujo AND b.batch_id=p.batch_id
 WHERE p.flujo IN ('disponibilidad-correcciones-20261119-v1')
), muestras AS (
 SELECT flujo,'intervalo_consulta_incluye_reinicio' metrica,'intervalos entre capturas' granularidad,
 captura_utc t,extract(epoch FROM captura_utc) fin_epoch,s FROM intcap
 UNION ALL SELECT flujo,'antiguedad_fuente_en_captura','capturas',captura_utc,extract(epoch FROM captura_utc),
 extract(epoch FROM captura_utc-fuente_utc) FROM cap
 UNION ALL SELECT flujo,'antiguedad_reporte_en_captura','observaciones',captura_utc,extract(epoch FROM captura_utc),
 extract(epoch FROM captura_utc-last_reported_utc) FROM obs
 UNION ALL SELECT flujo,'captura_a_calidad_spark','observaciones',captura_utc,
 extract(epoch FROM (payload->>'procesado_utc')::timestamptz),(payload->>'latencia_procesamiento_s')::double precision FROM obs
 UNION ALL SELECT flujo,'captura_a_prediccion_sql_confirmada','predicciones',origen_utc,
 extract(epoch FROM confirmado_utc),extract(epoch FROM confirmado_utc-origen_utc) FROM pred
 UNION ALL SELECT p.flujo,'objetivo_ideal_a_evaluacion_confirmada','predicciones evaluadas',p.objetivo_utc,
 extract(epoch FROM r.confirmado_utc),extract(epoch FROM r.confirmado_utc-p.objetivo_utc)
 FROM pred p JOIN disponibilidad.evaluaciones e USING(flujo,prediccion_id)
 JOIN disponibilidad.rondas_evaluacion r ON r.id=e.ronda_id WHERE e.estado='evaluada'
 UNION ALL SELECT p.flujo,'captura_futura_a_evaluacion_confirmada','predicciones evaluadas',e.captura_objetivo_utc,
 extract(epoch FROM r.confirmado_utc),extract(epoch FROM r.confirmado_utc-e.captura_objetivo_utc)
 FROM pred p JOIN disponibilidad.evaluaciones e USING(flujo,prediccion_id)
 JOIN disponibilidad.rondas_evaluacion r ON r.id=e.ronda_id WHERE e.estado='evaluada'
), resumen AS (
 SELECT flujo,metrica,granularidad,count(s) n,count(*) FILTER(WHERE s IS NULL) ausentes,
 count(*) FILTER(WHERE s<0) negativos,min(t) periodo_inicio_utc,max(t) periodo_fin_utc,
 to_timestamp(max(fin_epoch)) confirmacion_o_fin_max_utc,
 percentile_cont(0.5) WITHIN GROUP(ORDER BY s) mediana_s,
 percentile_cont(0.95) WITHIN GROUP(ORDER BY s) p95_s,min(s) minimo_s,max(s) maximo_s
 FROM muestras GROUP BY flujo,metrica,granularidad ORDER BY flujo,metrica
)
SELECT jsonb_build_object('fecha_auditoria_utc',transaction_timestamp(),'latencias',jsonb_agg(to_jsonb(r))) FROM resumen r;
COMMIT;
