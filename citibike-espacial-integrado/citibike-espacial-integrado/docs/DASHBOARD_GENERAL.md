# Dashboard general · CitiBike Intelligent Mobility (U1 + U2 + U3)

**Cristhian Chuquitarqui Chura | Análisis espacial**. Un solo tablero que cuenta una historia:
**analizar el pasado (U1) → anticipar el futuro (U2) → comprobar el funcionamiento (U3)**.

- Grafana → Dashboards → **Proyecto Sello** → *CitiBike Intelligent Mobility — Cristhian Chuquitarqui · Análisis espacial (U1 · U2 · U3)*
- URL directa: http://localhost:43000/d/citibike-general-cristhian
- JSON exportable e importable: `infra/grafana/provisioning/dashboards/json/dashboard_general_cristhian.json`

Los tableros originales `U2 · Espacial` y `U3 · Integración` no se modificaron.

## Archivos nuevos

| Archivo | Para qué sirve |
|---|---|
| `scripts/generar_dashboard_general.py` | Genera el JSON a partir de los resultados reales de `salidas/` |
| `infra/grafana/provisioning/dashboards/json/dashboard_general_cristhian.json` | El dashboard (Grafana lo carga solo) |
| `infra/grafana/provisioning/datasources/u1_batch.yml` | Datasource `U1 Batch` (TestData, uid `u1-batch`) para los resultados históricos |

Para regenerarlo (por ejemplo, después de volver a correr el notebook U1):

```bash
docker compose exec spark python scripts/generar_dashboard_general.py
docker compose restart grafana
```

## Fuentes de datos

| Sección | Fuente | Por qué |
|---|---|---|
| U1 · batch | `salidas/u1/gold_ranking_zonas` (Parquet) + `salidas/u1/estaciones_zona.json`, incrustados como CSV y servidos por el datasource `U1 Batch` | Los resultados de U1 **no se publican en Prometheus**. Son históricos (julio 2026), así que no dependen del selector de tiempo |
| U2 · streaming | Prometheus ← Pushgateway ← notebook U2_02 (`lambda26_u2esp_*`) | Métricas publicadas en vivo por las consultas `u2z_actividad`, `u2z_estado_iot` y `u2z_calidad` |
| U3 · integración | Prometheus ← Pushgateway ← `U3_ejecucion_integrada.py` (`lambda26_u3_*`) | KPI de la última corrida integrada |
| Kafka | Prometheus ← kafka-exporter (`kafka_topic_partition_current_offset`) | Ingesta por topic |

Filtros: **Fuente de datos** (Prometheus) y **Zona (U2)** (`label_values(lambda26_u2esp_actividad_observada, zona)`, con All = todas). El filtro de zona se aplica a *Real vs. predicha* y a *Pronóstico vs. promedio histórico*. Por defecto muestra las 5 zonas top de `salidas/u2/zonas_top.json` para no saturar las gráficas. La actualización es cada 5 s.

## Paneles y consultas

### 01 · U1 — Análisis histórico
| # | Panel | Datos |
|---|---|---|
| 1 | Mapa de calor (Geomap: capa de calor + cuadrados) | 99 zonas. El punto es el **centro exacto de la celda** de 0,02°: `lat0 + (fila+0,5)·0,02`, `lon0 + (col+0,5)·0,02`, con la fórmula `zona_de` del notebook (`lat0=40.5`, `lon0=-74.3`). El tooltip muestra zona, estación de referencia, puesto, actividad, salidas, llegadas, participación, n.º de estaciones, centro de la celda, centro medio de las estaciones y periodo. Mapa base: Esri Dark Gray |
| 2 | Ranking top 10 (barras horizontales) | Las 10 zonas con más actividad de `gold_ranking_zonas` |

**Actividad = salidas + llegadas**, la misma definición del notebook U1: cada viaje cuenta una vez en su zona de origen y otra en la de destino. Por eso el total de actividad es 9.943.694, que es 2 × los viajes de Silver.

### 02 · U2 — Predicción en tiempo real
| # | Panel | PromQL |
|---|---|---|
| 3A | Actividad actual | `lambda26_u2esp_actividad_total_ventana * on(job, instance) group_left(ventana) lambda26_u2esp_ventana_actual_info` |
| 3B | Pronóstico siguiente ventana | `lambda26_u2esp_pronostico_total_siguiente_top` (15 zonas top) |
| 3C | Zonas en alerta | `lambda26_u2esp_zonas_en_alerta_siguiente` (criterio U2_02: pronóstico ≥ 1,5 × media histórica de la franja) |
| 3D | MAE producción vs. prueba | `lambda26_u2esp_mae_online` y `lambda26_u2esp_modelo_mae_test and on(modelo) lambda26_u2esp_modelo_ganador_info` |
| 4 | Real vs. predicha | `lambda26_u2esp_actividad_observada{zona=~"$zona"}` (línea continua) y `lambda26_u2esp_actividad_predicha_para_ventana{zona=~"$zona"}` (discontinua) |
| 5 | Pronóstico top 10 (⚠ alerta en rojo) | `lambda26_u2esp_actividad_pronostico_siguiente >= 1.5 * clamp_min(lambda26_u2esp_actividad_media_historica_siguiente, 1)` (alerta) y `topk(10, …) unless on(zona) (…)` |
| 6 | Pronóstico vs. promedio histórico | `lambda26_u2esp_actividad_pronostico_siguiente` y `lambda26_u2esp_actividad_media_historica_siguiente` |
| 7 | Ocupación por zona (IoT) | `sort_desc(lambda26_u2esp_iot_ocupacion_zona_pct)` = 100 × bicis / (bicis + docks libres) |
| 8 | Bicis disponibles (10 más y 10 menos) | `topk(10, lambda26_u2esp_iot_bicis_zona) or bottomk(10, lambda26_u2esp_iot_bicis_zona)` |
| 9 | Estaciones vacías por zona | `sort_desc(topk(15, lambda26_u2esp_iot_estaciones_vacias_zona > 0))` |

### 03 · U3 — Integración y observabilidad
| # | Panel | PromQL |
|---|---|---|
| 10 | Estado de la última ejecución | `(lambda26_u3_run_info{estado="OK"} * 1) or (lambda26_u3_run_info * 0)` (1 = OK verde, 0 = ERROR rojo); `lambda26_u3_run_timestamp_seconds * 1000`; `lambda26_u3_duracion_total_s / 60` |
| 11 | Compuertas | `lambda26_u3_compuertas_ok`, `lambda26_u3_compuertas_total`, la diferencia de ambas y `100 * ok / total` |
| 12 | Eventos y calidad | `lambda26_u3_eventos_leidos`, `lambda26_u3_ratio_validos`, `lambda26_u3_ventanas_cerradas`, `lambda26_u3_descartados_por_watermark`, `lambda26_u3_eventos_invalidos` |
| 13 | Latencia | `lambda26_u3_latencia_p50_ms` y `_p95_ms` (corrida U3); en vivo: `lambda26_u2esp_calidad_latencia_prom_ms` y `_p95_ms` |
| 14 | Micro-lote vs. trigger de 5 s | `quantile_over_time(0.5 / 0.95, lambda26_u2esp_spark_batch_duration_ms{query="u2z_actividad"}[5m])`, con umbral rojo en 5000 ms |
| 15 | Rendimiento del pipeline | `lambda26_u2esp_spark_processed_rows_per_s`, `_input_rows_per_s`, `sum by (topic)(rate(kafka_topic_partition_current_offset[1m]))`, y en el eje derecho `lambda26_u2esp_spark_offsets_behind_latest_max` |

## Limitaciones y lo que falta (sin inventar datos)

- **Compuertas:** `U3_ejecucion_integrada.py` evalúa sus 2 compuertas de BI después del primer envío y luego re-publica `compuertas_ok/total` (POST, sin borrar los demás KPI). Así Grafana muestra el mismo total que el manifiesto (21/21). Las corridas anteriores a este cambio mostraban 19.
- **Latencia p50 en vivo:** U2_02 publica el promedio y el p95, no el p50. El p50 solo existe para la corrida U3.
- **Duplicados en U3:** la corrida U3 no publica duplicados del sensor. Están en U2 (`lambda26_u2esp_spark_duplicados_descartados{query="u2z_estado_iot"}`), pero no se muestran para no mezclar fuentes.
- **Pushgateway no guarda las métricas en disco.** Si se reinician los contenedores, la sección U3 queda vacía hasta correr de nuevo `U3_ejecucion_integrada.py`, y la sección U2 hasta arrancar el streaming (scripts/streaming_en_segundo_plano.py). La sección U1 no se ve afectada.
- **Importar en otro Grafana:** hace falta un datasource TestData con uid `u1-batch` (lo crea `u1_batch.yml`) y un Prometheus, que se elige en el selector "Fuente de datos".
