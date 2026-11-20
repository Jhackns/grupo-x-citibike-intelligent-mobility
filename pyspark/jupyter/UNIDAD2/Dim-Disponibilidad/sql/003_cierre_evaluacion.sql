-- Metadatos de pruebas y trazabilidad, sin modificar 001 ni resultados históricos.
BEGIN;
CREATE TABLE IF NOT EXISTS observabilidad_disponibilidad.ejecuciones(
 flujo text PRIMARY KEY,inicio_utc timestamptz NOT NULL,fin_utc timestamptz,
 estado text NOT NULL,evidencia text NOT NULL,
 CHECK(fin_utc IS NULL OR fin_utc>=inicio_utc)
);
CREATE OR REPLACE VIEW grafana_disponibilidad.ejecuciones AS
 SELECT * FROM observabilidad_disponibilidad.ejecuciones;
-- Solo columnas nativas para el frontier; no descomprime payload/JSON por estación.
CREATE OR REPLACE VIEW grafana_disponibilidad.instantes_observaciones AS
 SELECT o.flujo,o.captura_utc,o.contrato_valido,b.confirmado_utc confirmado_sql_utc
 FROM disponibilidad.observaciones o JOIN disponibilidad.lotes b
 ON b.flujo=o.flujo AND b.batch_id=o.batch_id;
CREATE OR REPLACE VIEW grafana_disponibilidad.observaciones AS
 SELECT o.flujo,o.observacion_id,o.event_id,o.capture_id,o.station_id,o.captura_utc,
 o.last_updated_utc,o.last_reported_utc,o.convencionales,o.electricas,
 o.elegible_convencionales,o.elegible_electricas,
 NOT o.elegible_convencionales OR NOT o.elegible_electricas rechazado_total_o_parcial,
 CASE WHEN q.q_discrepancia_total OR q.q_discrepancia_convencionales OR q.q_discrepancia_electricas THEN true
      WHEN q.q_discrepancia_total=false AND q.q_discrepancia_convencionales=false AND q.q_discrepancia_electricas=false THEN false END discrepancia,
 q.q_discrepancia_total,q.q_discrepancia_convencionales,q.q_discrepancia_electricas,
 o.contrato_valido,b.confirmado_utc confirmado_sql_utc
 FROM disponibilidad.observaciones o JOIN disponibilidad.lotes b ON b.flujo=o.flujo AND b.batch_id=o.batch_id
 CROSS JOIN LATERAL jsonb_to_record(o.payload) AS q(q_discrepancia_total boolean,
 q_discrepancia_convencionales boolean,q_discrepancia_electricas boolean);

CREATE OR REPLACE VIEW grafana_disponibilidad.predicciones AS
 SELECT p.flujo,p.prediccion_id,p.event_id,p.station_id,p.origen_utc,p.objetivo_utc,
 p.tipo,p.horizonte_min,p.valor_predicho,p.version_predictor,p.ejecucion_u1,p.contrato_sha256,
 b.confirmado_utc disponible_sql_utc,
 extract(epoch FROM b.confirmado_utc-p.origen_utc) latencia_sql_s,
 e.estado,e.motivo,e.cierre_no_antes_utc,e.captura_objetivo_utc,e.horizonte_efectivo_s,
 e.error_emparejamiento_s,e.valor_observado,e.error,e.cambio,e.estrato_discrepancia,
 o.capture_id,e.cerrado_utc,r.confirmado_utc evaluacion_confirmada_utc
 FROM disponibilidad.predicciones p JOIN disponibilidad.evaluaciones e USING(flujo,prediccion_id)
 JOIN disponibilidad.lotes b ON b.flujo=p.flujo AND b.batch_id=p.batch_id
 JOIN disponibilidad.observaciones o ON o.flujo=p.flujo AND o.observacion_id=p.event_id
 LEFT JOIN disponibilidad.rondas_evaluacion r ON r.id=e.ronda_id;
GRANT SELECT ON grafana_disponibilidad.ejecuciones TO disponibilidad_grafana;
GRANT SELECT ON grafana_disponibilidad.instantes_observaciones TO disponibilidad_grafana;
COMMIT;
