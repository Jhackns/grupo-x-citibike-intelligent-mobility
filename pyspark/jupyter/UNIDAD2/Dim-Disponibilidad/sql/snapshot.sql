SET TIME ZONE 'UTC';
SELECT jsonb_object_agg(tabla,datos) FROM (
 SELECT 'observaciones' tabla,jsonb_build_object('n',count(*),'hash',md5(coalesce(string_agg(md5(to_jsonb(o)::text),'' ORDER BY observacion_id),''))) datos
 FROM disponibilidad.observaciones o WHERE flujo=:'flujo'
 UNION ALL SELECT 'rechazos',jsonb_build_object('n',count(*),'hash',md5(coalesce(string_agg(md5(to_jsonb(r)::text),'' ORDER BY observacion_id),'')))
 FROM disponibilidad.rechazos r WHERE flujo=:'flujo'
 UNION ALL SELECT 'predicciones',jsonb_build_object('n',count(*),'hash',md5(coalesce(string_agg(md5(to_jsonb(p)::text),'' ORDER BY prediccion_id),'')))
 FROM disponibilidad.predicciones p WHERE flujo=:'flujo'
 UNION ALL SELECT 'evaluaciones',jsonb_build_object('n',count(*),'hash',md5(coalesce(string_agg(md5(to_jsonb(e)::text),'' ORDER BY prediccion_id),'')))
 FROM disponibilidad.evaluaciones e WHERE flujo=:'flujo'
 UNION ALL SELECT 'lotes',jsonb_build_object('n',count(*),'hash',md5(coalesce(string_agg(md5(to_jsonb(l)::text),'' ORDER BY batch_id),'')))
 FROM disponibilidad.lotes l WHERE flujo=:'flujo'
 UNION ALL SELECT 'rondas_evaluacion',jsonb_build_object('n',count(*),'hash',md5(coalesce(string_agg(md5(to_jsonb(r)::text),'' ORDER BY id),'')))
 FROM disponibilidad.rondas_evaluacion r WHERE flujo=:'flujo'
) s;
