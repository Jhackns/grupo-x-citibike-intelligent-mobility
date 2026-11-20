-- Solo la base propia disponibilidad_u2. No contiene claves ni usa tablas ajenas.
CREATE SCHEMA IF NOT EXISTS disponibilidad;
CREATE TABLE IF NOT EXISTS disponibilidad.esquema(version int PRIMARY KEY, sha256 text NOT NULL);
CREATE TABLE IF NOT EXISTS disponibilidad.flujos (
  flujo text PRIMARY KEY,
  contrato jsonb NOT NULL,
  creado_utc timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS disponibilidad.lotes (
  flujo text NOT NULL REFERENCES disponibilidad.flujos,
  batch_id bigint NOT NULL,
  observaciones bigint NOT NULL,
  rechazos bigint NOT NULL,
  predicciones bigint NOT NULL,
  confirmado_utc timestamptz,
  PRIMARY KEY(flujo,batch_id)
);
CREATE TABLE IF NOT EXISTS disponibilidad.observaciones (
  flujo text NOT NULL REFERENCES disponibilidad.flujos,
  observacion_id text NOT NULL,
  event_id text,
  capture_id text,
  station_id text,
  captura_utc timestamptz,
  last_updated_utc timestamptz,
  last_reported_utc timestamptz,
  contrato_valido boolean NOT NULL,
  convencionales bigint,
  electricas bigint,
  elegible_convencionales boolean NOT NULL,
  elegible_electricas boolean NOT NULL,
  regla_version text,
  catalogo_compatible boolean NOT NULL,
  payload jsonb NOT NULL,
  batch_id bigint NOT NULL,
  PRIMARY KEY(flujo,observacion_id),
  FOREIGN KEY(flujo,batch_id) REFERENCES disponibilidad.lotes DEFERRABLE INITIALLY DEFERRED
);
CREATE INDEX IF NOT EXISTS observaciones_estacion_tiempo
  ON disponibilidad.observaciones(flujo,station_id,captura_utc,observacion_id);
CREATE INDEX IF NOT EXISTS observaciones_frontier
  ON disponibilidad.observaciones(flujo,captura_utc DESC) WHERE contrato_valido;
CREATE TABLE IF NOT EXISTS disponibilidad.rechazos (
  flujo text NOT NULL,
  observacion_id text NOT NULL,
  motivos_convencionales jsonb NOT NULL,
  motivos_electricas jsonb NOT NULL,
  PRIMARY KEY(flujo,observacion_id),
  FOREIGN KEY(flujo,observacion_id) REFERENCES disponibilidad.observaciones
);
CREATE TABLE IF NOT EXISTS disponibilidad.predicciones (
  flujo text NOT NULL,
  prediccion_id text NOT NULL,
  event_id text NOT NULL,
  station_id text NOT NULL,
  origen_utc timestamptz NOT NULL,
  objetivo_utc timestamptz NOT NULL,
  tipo text NOT NULL CHECK(tipo IN ('convencionales','electricas')),
  horizonte_min int NOT NULL CHECK(horizonte_min IN (5,10,15)),
  valor_predicho double precision NOT NULL CHECK(valor_predicho>=0),
  version_predictor text NOT NULL,
  contrato_sha256 text NOT NULL,
  ejecucion_u1 text NOT NULL,
  payload jsonb NOT NULL,
  batch_id bigint NOT NULL,
  PRIMARY KEY(flujo,prediccion_id),
  UNIQUE(flujo,event_id,tipo,horizonte_min,version_predictor,contrato_sha256),
  FOREIGN KEY(flujo,event_id) REFERENCES disponibilidad.observaciones(flujo,observacion_id),
  FOREIGN KEY(flujo,batch_id) REFERENCES disponibilidad.lotes DEFERRABLE INITIALLY DEFERRED,
  CHECK(objetivo_utc=origen_utc+make_interval(mins=>horizonte_min))
);
CREATE TABLE IF NOT EXISTS disponibilidad.rondas_evaluacion (
  id text PRIMARY KEY,
  flujo text NOT NULL REFERENCES disponibilidad.flujos,
  iniciado_utc timestamptz NOT NULL DEFAULT clock_timestamp(),
  confirmado_utc timestamptz
);
CREATE TABLE IF NOT EXISTS disponibilidad.evaluaciones (
  flujo text NOT NULL,
  prediccion_id text NOT NULL,
  estado text NOT NULL DEFAULT 'pendiente' CHECK(estado IN ('pendiente','evaluada','vencida')),
  motivo text,
  cierre_no_antes_utc timestamptz NOT NULL,
  observacion_objetivo_id text,
  captura_objetivo_utc timestamptz,
  horizonte_efectivo_s double precision,
  error_emparejamiento_s double precision,
  valor_actual double precision,
  valor_observado double precision,
  error double precision,
  cambio boolean,
  estrato_discrepancia text,
  calidad_objetivo jsonb,
  cerrado_utc timestamptz,
  ronda_id text REFERENCES disponibilidad.rondas_evaluacion,
  PRIMARY KEY(flujo,prediccion_id),
  FOREIGN KEY(flujo,prediccion_id) REFERENCES disponibilidad.predicciones,
  FOREIGN KEY(flujo,observacion_objetivo_id) REFERENCES disponibilidad.observaciones(flujo,observacion_id),
  CHECK((estado='evaluada' AND valor_observado IS NOT NULL AND error IS NOT NULL AND observacion_objetivo_id IS NOT NULL)
     OR (estado IN ('pendiente','vencida') AND error IS NULL))
);
CREATE INDEX IF NOT EXISTS evaluaciones_pendientes
  ON disponibilidad.evaluaciones(flujo,cierre_no_antes_utc,prediccion_id) WHERE estado='pendiente';

CREATE OR REPLACE FUNCTION disponibilidad.indicadores(p jsonb) RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $$
 SELECT coalesce(jsonb_object_agg(key,value),'{}'::jsonb) FROM jsonb_each(p)
 WHERE key LIKE 'q\_%' ESCAPE '\' OR key LIKE 'elegible\_%' ESCAPE '\'
 OR key LIKE 'motivos\_%' ESCAPE '\' OR key IN ('regla_version','catalogo_modelo_compatible')
$$;

CREATE OR REPLACE FUNCTION disponibilidad.estrato(a jsonb,b jsonb) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
 SELECT CASE WHEN EXISTS(SELECT 1 FROM unnest(ARRAY['q_discrepancia_total','q_discrepancia_convencionales','q_discrepancia_electricas']) k
     WHERE (a->>k)::boolean OR (b->>k)::boolean) THEN 'con_discrepancias'
   WHEN NOT EXISTS(SELECT 1 FROM unnest(ARRAY['q_discrepancia_total','q_discrepancia_convencionales','q_discrepancia_electricas']) k
     WHERE a->>k IS NULL OR b->>k IS NULL) THEN 'sin_discrepancias' ELSE 'desconocido' END
$$;

-- Primero elegir la captura, después verificar calidad, igual que Gold U1.
-- El frontier exige progreso de datos, no solo el reloj de pared.
CREATE OR REPLACE FUNCTION disponibilidad.evaluar(f text, limite int, ahora timestamptz, ronda text)
RETURNS int LANGUAGE plpgsql AS $$
DECLARE p record; t record; a record; cfg jsonb; toler int; hueco int; tardios int;
  frontier timestamptz; v_motivo text; actual double precision; observado double precision;
  mal_trayecto boolean; n int:=0;
BEGIN
 PERFORM pg_advisory_xact_lock(hashtextextended(f,0));
 SELECT contrato INTO STRICT cfg FROM disponibilidad.flujos WHERE flujo=f;
 toler:=(cfg->>'tolerance_seconds')::int;
 hueco:=(cfg->>'gap_seconds')::int;
 tardios:=(cfg->>'late_seconds')::int;
 SELECT max(captura_utc)-make_interval(secs=>tardios) INTO frontier
 FROM disponibilidad.observaciones WHERE flujo=f AND contrato_valido;
 FOR p IN SELECT pr.*,e.cierre_no_antes_utc FROM disponibilidad.evaluaciones e
   JOIN disponibilidad.predicciones pr USING(flujo,prediccion_id)
   WHERE e.flujo=f AND e.estado='pendiente' AND e.cierre_no_antes_utc<=ahora
   AND pr.objetivo_utc+make_interval(secs=>toler)<=frontier
   ORDER BY e.cierre_no_antes_utc,pr.prediccion_id LIMIT limite
 LOOP
   SELECT * INTO STRICT a FROM disponibilidad.observaciones
     WHERE flujo=f AND observacion_id=p.event_id;
   actual:=CASE WHEN p.tipo='convencionales' THEN a.convencionales ELSE a.electricas END;
   SELECT o.* INTO t FROM disponibilidad.observaciones o
     WHERE o.flujo=f AND o.station_id=p.station_id
     AND o.captura_utc BETWEEN p.objetivo_utc-make_interval(secs=>toler) AND p.objetivo_utc+make_interval(secs=>toler)
     AND o.captura_utc>p.origen_utc
     ORDER BY abs(extract(epoch FROM o.captura_utc-p.objetivo_utc)),o.captura_utc,o.observacion_id LIMIT 1;
   v_motivo:=NULL; observado:=NULL;
   IF NOT FOUND THEN v_motivo:='sin_captura_objetivo_en_tolerancia';
   ELSE
     observado:=CASE WHEN p.tipo='convencionales' THEN t.convencionales ELSE t.electricas END;
     IF (SELECT count(*) FROM disponibilidad.observaciones WHERE flujo=f AND station_id=p.station_id AND captura_utc=t.captura_utc)>1 THEN
       v_motivo:='captura_objetivo_ambigua';
     ELSIF NOT (CASE WHEN p.tipo='convencionales' THEN t.elegible_convencionales ELSE t.elegible_electricas END)
       OR NOT t.catalogo_compatible OR t.regla_version<>a.regla_version OR observado IS NULL THEN
       v_motivo:='objetivo_no_elegible';
     ELSE
       SELECT coalesce(bool_or(NOT elegible OR delta>hueco OR repetidos>1),false) INTO mal_trayecto FROM (
         SELECT CASE WHEN p.tipo='convencionales' THEN elegible_convencionales ELSE elegible_electricas END AS elegible,
           extract(epoch FROM captura_utc-lag(captura_utc) OVER(ORDER BY captura_utc,observacion_id)) AS delta,
           count(*) OVER(PARTITION BY captura_utc) AS repetidos
         FROM disponibilidad.observaciones WHERE flujo=f AND station_id=p.station_id
         AND captura_utc BETWEEN p.origen_utc AND t.captura_utc
       ) pasos;
       IF mal_trayecto THEN v_motivo:='hueco_o_inactividad_en_trayecto'; END IF;
     END IF;
   END IF;
   UPDATE disponibilidad.evaluaciones SET
     estado=CASE WHEN v_motivo IS NULL THEN 'evaluada' ELSE 'vencida' END, motivo=v_motivo,
     observacion_objetivo_id=t.observacion_id,captura_objetivo_utc=t.captura_utc,
     horizonte_efectivo_s=extract(epoch FROM t.captura_utc-p.origen_utc),
     error_emparejamiento_s=extract(epoch FROM t.captura_utc-p.objetivo_utc),
     valor_actual=actual,valor_observado=observado,
     error=CASE WHEN v_motivo IS NULL THEN p.valor_predicho-observado END,
     cambio=CASE WHEN v_motivo IS NULL THEN observado<>actual END,
     estrato_discrepancia=CASE WHEN t.observacion_id IS NOT NULL THEN disponibilidad.estrato(a.payload,t.payload) END,
     calidad_objetivo=CASE WHEN t.observacion_id IS NOT NULL THEN disponibilidad.indicadores(t.payload) END,
     cerrado_utc=ahora,ronda_id=ronda
   WHERE flujo=f AND prediccion_id=p.prediccion_id AND estado='pendiente';
   n:=n+1;
 END LOOP;
 RETURN n;
END $$;

CREATE OR REPLACE VIEW disponibilidad.cobertura AS
 SELECT p.flujo,p.tipo,p.horizonte_min,date_bin(interval '5 minutes',p.origen_utc,timestamptz '2000-01-01 UTC') AS ventana_utc,
 count(*) AS predicciones,count(*) FILTER(WHERE e.estado='pendiente') AS pendientes,
 count(*) FILTER(WHERE e.estado='evaluada') AS evaluadas,count(*) FILTER(WHERE e.estado='vencida') AS vencidas,
 count(*) FILTER(WHERE e.estado='evaluada')::double precision/nullif(count(*),0) AS fraccion_evaluada
 FROM disponibilidad.predicciones p JOIN disponibilidad.evaluaciones e USING(flujo,prediccion_id)
 GROUP BY p.flujo,p.tipo,p.horizonte_min,ventana_utc;

CREATE OR REPLACE VIEW disponibilidad.metricas AS
 SELECT p.flujo,p.tipo,p.horizonte_min,date_bin(interval '5 minutes',p.origen_utc,timestamptz '2000-01-01 UTC') AS ventana_utc,
 CASE WHEN grouping(e.cambio)=1 THEN 'todos' WHEN e.cambio THEN 'con_cambios' ELSE 'sin_cambios' END AS periodo,
 CASE WHEN grouping(e.estrato_discrepancia)=1 THEN 'todos' ELSE e.estrato_discrepancia END AS discrepancias,
 count(*) AS n,avg(abs(e.error)) AS mae,sqrt(avg(e.error*e.error)) AS rmse,avg(e.error) AS sesgo
 FROM disponibilidad.predicciones p JOIN disponibilidad.evaluaciones e USING(flujo,prediccion_id)
 WHERE e.estado='evaluada'
 GROUP BY p.flujo,p.tipo,p.horizonte_min,ventana_utc,
 GROUPING SETS((),(e.cambio),(e.estrato_discrepancia),(e.cambio,e.estrato_discrepancia));

CREATE OR REPLACE VIEW disponibilidad.latencias AS
 SELECT p.flujo,p.prediccion_id,p.tipo,p.horizonte_min,p.origen_utc,
 extract(epoch FROM p.origen_utc-o.last_updated_utc) AS antiguedad_feed_s,
 extract(epoch FROM p.origen_utc-o.last_reported_utc) AS antiguedad_reporte_s,
 extract(epoch FROM b.confirmado_utc-p.origen_utc) AS captura_a_sql_confirmado_s,
 extract(epoch FROM r.confirmado_utc-p.objetivo_utc) AS objetivo_a_evaluacion_confirmada_s,
 extract(epoch FROM r.confirmado_utc-e.captura_objetivo_utc) AS captura_objetivo_a_evaluacion_s,
 e.estado FROM disponibilidad.predicciones p
 JOIN disponibilidad.observaciones o ON o.flujo=p.flujo AND o.observacion_id=p.event_id
 JOIN disponibilidad.lotes b ON b.flujo=p.flujo AND b.batch_id=p.batch_id
 JOIN disponibilidad.evaluaciones e ON e.flujo=p.flujo AND e.prediccion_id=p.prediccion_id
 LEFT JOIN disponibilidad.rondas_evaluacion r ON r.id=e.ronda_id;
