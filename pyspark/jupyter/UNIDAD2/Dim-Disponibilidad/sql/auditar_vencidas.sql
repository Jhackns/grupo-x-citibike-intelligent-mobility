SET TIME ZONE 'UTC';
WITH vencidas AS MATERIALIZED (
 SELECT p.*,e.motivo,e.observacion_objetivo_id,e.calidad_objetivo FROM disponibilidad.predicciones p
 JOIN disponibilidad.evaluaciones e USING(flujo,prediccion_id)
 WHERE p.flujo='disponibilidad-etapa3-vivo-20261119-v1' AND e.estado='vencida'
), faltantes AS MATERIALIZED (
 SELECT v.*,c.captura_utc cercana_utc,extract(epoch FROM c.captura_utc-v.objetivo_utc) desfase_s
 FROM vencidas v LEFT JOIN LATERAL (
  SELECT o.captura_utc FROM disponibilidad.observaciones o
  WHERE o.flujo=v.flujo AND o.station_id=v.station_id
  AND o.captura_utc BETWEEN v.objetivo_utc-interval '450 seconds' AND v.objetivo_utc+interval '450 seconds'
  ORDER BY abs(extract(epoch FROM o.captura_utc-v.objetivo_utc)),o.captura_utc,o.observacion_id LIMIT 1
 ) c ON true WHERE motivo='sin_captura_objetivo_en_tolerancia'
), grupos AS (
 SELECT origen_utc,objetivo_utc,horizonte_min,count(*) n,
 min(abs(desfase_s)) minimo_desfase_s,max(abs(desfase_s)) maximo_desfase_s,
 count(*) FILTER(WHERE abs(desfase_s)<=30) candidatos_en_banda
 FROM faltantes GROUP BY origen_utc,objetivo_utc,horizonte_min
), invalidas AS (
 SELECT v.tipo,v.horizonte_min,v.station_id,v.origen_utc,o.captura_utc,
 o.contrato_valido,o.elegible_convencionales,o.elegible_electricas,
 o.payload->'motivos_pred_convencionales' motivos_convencionales,
 o.payload->'motivos_pred_electricas' motivos_electricas,
 o.payload->'edad_reporte_s' edad_reporte_s,
 o.payload->'is_installed' is_installed,o.payload->'is_renting' is_renting,o.payload->'is_returning' is_returning
 FROM vencidas v JOIN disponibilidad.observaciones o
 ON o.flujo=v.flujo AND o.observacion_id=v.observacion_objetivo_id
 WHERE v.motivo='objetivo_no_elegible'
)
SELECT jsonb_build_object(
 'motivos',(SELECT jsonb_agg(to_jsonb(x)) FROM(SELECT motivo,count(*) n FROM vencidas GROUP BY motivo) x),
 'faltantes_por_cohorte',(SELECT jsonb_agg(to_jsonb(g)) FROM grupos g),
 'invalidas',(SELECT jsonb_agg(to_jsonb(i)) FROM invalidas i),
 'falsos_faltantes',(SELECT count(*) FROM faltantes WHERE abs(desfase_s)<=30),
 'sin_cercana_450s',(SELECT count(*) FROM faltantes WHERE cercana_utc IS NULL),
 'invalidas_elegibles_por_error',(SELECT count(*) FROM vencidas v JOIN disponibilidad.observaciones o
 ON o.flujo=v.flujo AND o.observacion_id=v.observacion_objetivo_id
 WHERE v.motivo='objetivo_no_elegible' AND CASE WHEN v.tipo='convencionales' THEN o.elegible_convencionales ELSE o.elegible_electricas END),
 'fechas_captura',(SELECT jsonb_agg(to_jsonb(c)) FROM(SELECT captura_utc,count(*) estaciones
   FROM disponibilidad.observaciones WHERE flujo='disponibilidad-etapa3-vivo-20261119-v1'
   GROUP BY captura_utc ORDER BY captura_utc) c)
);
