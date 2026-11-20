-- Extensión aditiva: no cambia 001, reglas, predicciones ni cierres anteriores.
BEGIN;
DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='disponibilidad_grafana') THEN
   CREATE ROLE disponibilidad_grafana LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION;
   COMMENT ON ROLE disponibilidad_grafana IS 'Disponibilidad U2 Grafana solo lectura v1';
 ELSIF (SELECT shobj_description(oid,'pg_authid') FROM pg_roles WHERE rolname='disponibilidad_grafana')
      IS DISTINCT FROM 'Disponibilidad U2 Grafana solo lectura v1' THEN
   RAISE EXCEPTION 'El rol existente no pertenece a esta entrega';
 END IF;
END $$;
ALTER ROLE disponibilidad_grafana SET default_transaction_read_only=on;
ALTER ROLE disponibilidad_grafana SET statement_timeout='30s';
ALTER ROLE disponibilidad_grafana SET search_path='grafana_disponibilidad,pg_catalog';
GRANT CONNECT ON DATABASE disponibilidad_u2 TO disponibilidad_grafana;
REVOKE TEMPORARY ON DATABASE disponibilidad_u2 FROM PUBLIC;
CREATE SCHEMA IF NOT EXISTS observabilidad_disponibilidad;
CREATE TABLE IF NOT EXISTS observabilidad_disponibilidad.componentes (
 flujo text NOT NULL,componente text NOT NULL,activo boolean,
 comprobado_utc timestamptz NOT NULL,detalle text NOT NULL,
 PRIMARY KEY(flujo,componente)
);
ALTER TABLE observabilidad_disponibilidad.componentes ADD COLUMN IF NOT EXISTS intervalo_consulta_s integer;
ALTER TABLE observabilidad_disponibilidad.componentes ADD COLUMN IF NOT EXISTS umbral_datos_s integer;
CREATE SCHEMA IF NOT EXISTS grafana_disponibilidad;
REVOKE ALL ON SCHEMA grafana_disponibilidad FROM PUBLIC;
CREATE INDEX IF NOT EXISTS predicciones_grafana_estacion
 ON disponibilidad.predicciones(flujo,station_id,tipo,horizonte_min,objetivo_utc);
CREATE INDEX IF NOT EXISTS predicciones_grafana_periodo
 ON disponibilidad.predicciones(flujo,objetivo_utc);

CREATE OR REPLACE VIEW grafana_disponibilidad.flujos AS
 SELECT ids.flujo,coalesce((f.contrato->>'late_seconds')::int,600) tardios_s,
 coalesce((f.contrato->>'tolerance_seconds')::int,30) tolerancia_s,
 f.contrato->>'u1_id' ejecucion_u1,
 (SELECT max(captura_utc) FROM disponibilidad.observaciones o WHERE o.flujo=ids.flujo AND contrato_valido) ultima_captura_utc,
 (SELECT last_updated_utc FROM disponibilidad.observaciones o WHERE o.flujo=ids.flujo AND contrato_valido
    ORDER BY captura_utc DESC,observacion_id LIMIT 1) ultima_actualizacion_utc,
 coalesce((SELECT umbral_datos_s FROM observabilidad_disponibilidad.componentes c
    WHERE c.flujo=ids.flujo AND c.componente='productor'),900) umbral_datos_s
 FROM (SELECT flujo FROM disponibilidad.flujos WHERE flujo NOT LIKE 'fixture-%'
       UNION SELECT flujo FROM observabilidad_disponibilidad.componentes) ids
 LEFT JOIN disponibilidad.flujos f USING(flujo);

CREATE OR REPLACE VIEW grafana_disponibilidad.componentes AS
 SELECT flujo,componente,activo,comprobado_utc,detalle,
 clock_timestamp()-comprobado_utc>interval '30 seconds' telemetria_antigua,intervalo_consulta_s,umbral_datos_s
 FROM observabilidad_disponibilidad.componentes;

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

CREATE OR REPLACE VIEW grafana_disponibilidad.estaciones AS
 SELECT DISTINCT flujo,station_id FROM disponibilidad.observaciones WHERE station_id IS NOT NULL;
GRANT USAGE ON SCHEMA grafana_disponibilidad TO disponibilidad_grafana;
GRANT SELECT ON ALL TABLES IN SCHEMA grafana_disponibilidad TO disponibilidad_grafana;
COMMIT;
