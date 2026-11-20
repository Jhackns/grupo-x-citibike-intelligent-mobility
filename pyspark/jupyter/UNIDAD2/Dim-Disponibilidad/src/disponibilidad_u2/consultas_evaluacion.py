"""Una definición de cobertura/as-of para dashboard e informes de verificación."""


def resumen(filtro,comparar_cortes=False,filtro_flujo='true',corte_actual='statement_timestamp()'):
    """filtro es SQL interno construido por la aplicación, nunca texto recibido de la API.

    Excluye inmaduras del denominador y reconstruye solo datos confirmados al corte.
    La perspectiva fin_prueba existe únicamente cuando hay evidencia de finalización.
    """
    # La frontera se calcula una vez por corte: un filtro estrecho puede inducir
    # nested loops que la recalculen por predicción si PostgreSQL integra el CTE.
    final="UNION ALL SELECT flujo,'fin_prueba',fin_utc,fin_utc,estado FROM grafana_disponibilidad.ejecuciones WHERE fin_utc IS NOT NULL AND "+filtro_flujo if comparar_cortes else ''
    return f'''WITH cortes AS (
      SELECT f.flujo,'auditoria_actual'::text momento,{corte_actual} instante_corte_utc,
       r.fin_utc,r.estado estado_ejecucion FROM grafana_disponibilidad.flujos f
       LEFT JOIN grafana_disponibilidad.ejecuciones r USING(flujo) WHERE {filtro_flujo}
      {final}
    ), frontera AS MATERIALIZED (
      SELECT c.*,max(o.captura_utc) FILTER(WHERE o.contrato_valido) frontera_utc
      FROM cortes c LEFT JOIN grafana_disponibilidad.instantes_observaciones o
       ON o.flujo=c.flujo AND o.captura_utc<=c.instante_corte_utc AND o.confirmado_sql_utc<=c.instante_corte_utc
      GROUP BY c.flujo,c.momento,c.instante_corte_utc,c.fin_utc,c.estado_ejecucion
    ), datos AS MATERIALIZED (
      SELECT p.*,f.momento,f.instante_corte_utc,f.fin_utc,f.estado_ejecucion,f.frontera_utc,
       g.tardios_s,g.tolerancia_s,
       CASE WHEN p.evaluacion_confirmada_utc<=f.instante_corte_utc THEN p.estado ELSE 'pendiente' END estado_corte
      FROM frontera f JOIN grafana_disponibilidad.predicciones p USING(flujo)
      JOIN grafana_disponibilidad.flujos g USING(flujo)
      WHERE {filtro} AND p.disponible_sql_utc<=f.instante_corte_utc AND p.origen_utc<=f.instante_corte_utc
    ), clasificada AS (
      SELECT *,CASE WHEN estado_corte<>'pendiente' THEN estado_corte
       WHEN cierre_no_antes_utc>instante_corte_utc THEN 'inmadura'
       WHEN frontera_utc IS NULL OR objetivo_utc+make_interval(secs=>tolerancia_s)>frontera_utc-make_interval(secs=>tardios_s)
        THEN CASE WHEN fin_utc<=instante_corte_utc THEN 'flujo_finalizado_sin_avance' ELSE 'sin_avance_no_atribuido' END
       ELSE 'cerrable' END causa FROM datos
    ) SELECT flujo,tipo,horizonte_min,momento,instante_corte_utc,
      min(origen_utc) origen_inicio_utc,max(origen_utc) origen_fin_utc,
      min(origen_utc) FILTER(WHERE estado_corte='evaluada') evaluadas_origen_inicio_utc,
      max(origen_utc) FILTER(WHERE estado_corte='evaluada') evaluadas_origen_fin_utc,
      min(objetivo_utc) FILTER(WHERE estado_corte='evaluada') evaluadas_objetivo_inicio_utc,
      max(objetivo_utc) FILTER(WHERE estado_corte='evaluada') evaluadas_objetivo_fin_utc,
      min(captura_objetivo_utc) FILTER(WHERE estado_corte='evaluada') observadas_inicio_utc,
      max(captura_objetivo_utc) FILTER(WHERE estado_corte='evaluada') observadas_fin_utc,
      count(*) predicciones,count(*) FILTER(WHERE estado_corte='evaluada') n,
      count(*) FILTER(WHERE estado_corte='evaluada') evaluadas,
      count(*) FILTER(WHERE estado_corte='vencida') vencidas,
      count(*) FILTER(WHERE estado_corte='pendiente') pendientes,
      count(*) FILTER(WHERE causa='inmadura') pendientes_inmaduras,
      count(*) FILTER(WHERE causa IN ('flujo_finalizado_sin_avance','sin_avance_no_atribuido')) pendientes_sin_avance,
      count(*) FILTER(WHERE causa='flujo_finalizado_sin_avance') pendientes_flujo_finalizado,
      count(*) FILTER(WHERE causa='sin_avance_no_atribuido') pendientes_sin_avance_no_atribuido,
      count(*) FILTER(WHERE causa='cerrable') pendientes_cerrables,
      count(*) FILTER(WHERE estado_corte='vencida' AND motivo='sin_captura_objetivo_en_tolerancia') vencidas_sin_captura,
      count(*) FILTER(WHERE estado_corte='vencida' AND motivo='objetivo_no_elegible') vencidas_objetivo_no_elegible,
      count(*) FILTER(WHERE estado_corte='vencida' AND motivo='hueco_o_inactividad_en_trayecto') vencidas_trayecto,
      count(*) FILTER(WHERE estado_corte='vencida' AND motivo='captura_objetivo_ambigua') vencidas_ambiguas,
      count(*) FILTER(WHERE estado_corte='vencida' AND coalesce(motivo,'') NOT IN
       ('sin_captura_objetivo_en_tolerancia','objetivo_no_elegible','hueco_o_inactividad_en_trayecto','captura_objetivo_ambigua')) vencidas_otras,
      count(*) FILTER(WHERE estado_corte='evaluada' AND cierre_no_antes_utc<=instante_corte_utc) numerador,
      count(*) FILTER(WHERE cierre_no_antes_utc<=instante_corte_utc) denominador,
      100.0*count(*) FILTER(WHERE estado_corte='evaluada' AND cierre_no_antes_utc<=instante_corte_utc)
       /nullif(count(*) FILTER(WHERE cierre_no_antes_utc<=instante_corte_utc),0) cobertura_pct,
      avg(abs(error)) FILTER(WHERE estado_corte='evaluada') mae,
      sqrt(avg(error*error) FILTER(WHERE estado_corte='evaluada')) rmse,
      avg(error) FILTER(WHERE estado_corte='evaluada') sesgo,
      'Errores únicamente del subconjunto evaluado; consultar n y cobertura'::text alcance
     FROM clasificada GROUP BY flujo,tipo,horizonte_min,momento,instante_corte_utc
     ORDER BY flujo,tipo,horizonte_min,momento'''
