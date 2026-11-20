-- Auditoría de solo lectura. Dos flujos explícitos; nunca incluye fixtures.
BEGIN READ ONLY;
SET LOCAL TIME ZONE 'UTC';
WITH seleccion AS MATERIALIZED (
 SELECT p.*,e.estado,e.cierre_no_antes_utc,e.error,e.cerrado_utc,e.captura_objetivo_utc,
 e.cambio,e.estrato_discrepancia,f.ultima_captura_utc,f.tardios_s,f.tolerancia_s,
 CASE WHEN p.flujo='disponibilidad-etapa3-vivo-20261119-v1' THEN timestamptz '2026-11-19T04:34:55.662Z' ELSE timestamptz '2026-11-19T15:24:30.593Z' END AS auditoria_utc
 FROM disponibilidad.predicciones p JOIN disponibilidad.evaluaciones e USING(flujo,prediccion_id)
 JOIN grafana_disponibilidad.flujos f USING(flujo)
 WHERE p.flujo IN ('disponibilidad-etapa3-vivo-20261119-v1','disponibilidad-etapa4-vivo-20261119-v1')
), resumen AS (
 SELECT flujo,tipo,horizonte_min,max(auditoria_utc) auditoria_utc,
 min(origen_utc) origen_inicio_utc,max(origen_utc) origen_fin_utc,
 min(origen_utc) FILTER(WHERE estado='evaluada') evaluadas_origen_inicio_utc,
 max(origen_utc) FILTER(WHERE estado='evaluada') evaluadas_origen_fin_utc,
 min(objetivo_utc) FILTER(WHERE estado='evaluada') evaluadas_objetivo_inicio_utc,
 max(objetivo_utc) FILTER(WHERE estado='evaluada') evaluadas_objetivo_fin_utc,
 min(captura_objetivo_utc) FILTER(WHERE estado='evaluada') observadas_inicio_utc,
 max(captura_objetivo_utc) FILTER(WHERE estado='evaluada') observadas_fin_utc,
 count(*) predicciones,count(*) FILTER(WHERE cierre_no_antes_utc<=auditoria_utc) plazo_terminado,
 count(*) FILTER(WHERE estado='evaluada') evaluadas,count(*) FILTER(WHERE estado='vencida') vencidas,
 count(*) FILTER(WHERE estado='pendiente' AND cierre_no_antes_utc>auditoria_utc) pendientes_inmaduras,
 count(*) FILTER(WHERE estado='pendiente' AND cierre_no_antes_utc<=auditoria_utc
  AND (ultima_captura_utc IS NULL OR objetivo_utc+make_interval(secs=>tolerancia_s)>ultima_captura_utc-make_interval(secs=>tardios_s))) pendientes_maduras_sin_avance,
 count(*) FILTER(WHERE estado='pendiente' AND cierre_no_antes_utc<=auditoria_utc
  AND objetivo_utc+make_interval(secs=>tolerancia_s)<=ultima_captura_utc-make_interval(secs=>tardios_s)) pendientes_cerrables,
 count(*) FILTER(WHERE estado<>'pendiente' AND cierre_no_antes_utc>auditoria_utc) cierres_antes_de_plazo,
 avg(abs(error)) FILTER(WHERE estado='evaluada') mae,
 sqrt(avg(error*error) FILTER(WHERE estado='evaluada')) rmse,
 avg(error) FILTER(WHERE estado='evaluada') sesgo,
 100.0*count(*) FILTER(WHERE estado='evaluada' AND cierre_no_antes_utc<=auditoria_utc)
  /nullif(count(*) FILTER(WHERE cierre_no_antes_utc<=auditoria_utc),0) cobertura_plazo_terminado_pct,
 100.0*count(*) FILTER(WHERE estado='evaluada')/nullif(count(*) FILTER(WHERE estado<>'pendiente'),0) cobertura_solo_resueltas_pct
 FROM seleccion GROUP BY flujo,tipo,horizonte_min ORDER BY flujo,tipo,horizonte_min
), estratos AS (
 SELECT flujo,tipo,horizonte_min,cambio,estrato_discrepancia,count(*) n,avg(abs(error)) mae,
 sqrt(avg(error*error)) rmse,avg(error) sesgo FROM seleccion WHERE estado='evaluada'
 GROUP BY flujo,tipo,horizonte_min,cambio,estrato_discrepancia ORDER BY flujo,tipo,horizonte_min,cambio,estrato_discrepancia
)
SELECT jsonb_build_object('fecha_auditoria_utc',transaction_timestamp(),
 'regla_denominador','cierre_no_antes_utc <= fecha_auditoria_utc; incluye pendientes maduras bloqueadas; excluye inmaduras',
 'resultados',(SELECT jsonb_agg(to_jsonb(r)) FROM resumen r),
 'estratos',(SELECT jsonb_agg(to_jsonb(s)) FROM estratos s));
COMMIT;
