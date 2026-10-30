# Producto U2 — Dimensión Estaciones (Grimaldo Arredondo Martinez)

| Campo | Valor |
| :--- | :--- |
| **Proyecto** | CitiBike Intelligent Mobility |
| **Equipo** | Data Geniuses |
| **Integrante** | Grimaldo Arredondo Martinez |
| **Dimensión** | Estaciones: utilización y demanda futura |
| **Producto** | Unidad 2: pipeline en tiempo real con ingesta empresarial e IoT, streaming con Spark, observabilidad/costos y BI/ML |
| **Código** | `lambda26/u2-estaciones/` (productores), `lambda26/pyspark/sesiones/u2-estaciones/` (notebooks), `lambda26/obs/grafana/.../u2_estaciones_grimaldo.json` (tablero) |

> Todos los valores de esta página salen de la corrida real del 29 de octubre de 2026 (notebooks 01 y 02, logs de los productores y el tablero). No se escribieron a mano: se copiaron de las salidas.

## 1. Pregunta y evolución desde U1

**Pregunta U2 (Brief S2):** *¿Qué estaciones podrían registrar mayor demanda durante la siguiente ventana temporal?*

| | U1 (batch) | U2 (streaming) |
| :--- | :--- | :--- |
| Pregunta | ¿Qué estaciones concentran la demanda y cuáles tienen alta probabilidad de alta utilización **en un día**? | ¿Cuántos viajes saldrán de cada estación **en los próximos 15 minutos**? |
| Granularidad | estación × día | estación × ventana de 15 min |
| Modelo | Clasificación `es_alta_demanda` (P75). Ganador: RandomForest, AUC 0.786 | Regresión sobre la serie (lags + media histórica por franja). Ganador: **LinearRegression** (MAE 1,9805 en test, mejor que la media histórica: 2,1231) |
| Salida | Notebook con ranking y métricas | Tablero Grafana en vivo: ranking actual, observado vs. predicho, pronóstico t+1 y estado de los docks |
| Lo que se reutiliza | — | Esquema explícito, reglas Silver, el mismo corte train/test (2026-07-24) y la idea de "alta demanda" como alerta |

## 2. Arquitectura de la dimensión (capa de velocidad de Lambda)

```
CSV Citi Bike (U1) ─ producer_rides_replay ─► Kafka citibike-rides ───────┐
                                                                          ├─► Spark Structured Streaming ─► Pushgateway ─► Prometheus ─► Grafana
Feed GBFS en vivo ── producer_gbfs_estaciones ► Kafka citibike-estaciones ┘     u2_demanda · u2_estado_iot · u2_calidad · monitor
```

**Tabla 1. De la sesión a la capacidad de mi dimensión**

| Sesión | Capacidad | Cómo la implementa mi dimensión |
| :--- | :--- | :--- |
| S6 | Eventos empresariales | `citibike-rides`: cada viaje del histórico real se publica como `viaje.salida` y `viaje.llegada`, en orden cronológico, con `stationId` como key |
| S7 | Eventos IoT/sensores | `citibike-estaciones`: estado de los docks (sensores de anclaje) desde el feed GBFS en vivo, enlazado con los IDs del histórico |
| S8 | Streaming con Spark | Ventanas de 15 min por estación, watermark de 10 min, checkpoint; deduplicación de reportes del sensor con `dropDuplicatesWithinWatermark` |
| S9 | Observabilidad y costos | Throughput, duración de micro-lote, retraso de offsets, latencia, válidos; costo mensual a partir del volumen medido |
| S10 | Series de tiempo e inferencia | Modelo global para las 20 estaciones top, comparado contra baselines e inferido en vivo ventana a ventana |
| S11 | BI/ML distribuido | Tablero con KPIs en vivo y la predicción en la misma vista, junto al MAE en producción |

## 3. Criterio 1 — Ingesta de eventos empresariales (Kafka)

- **Contrato:** `u2-estaciones/CONTRATO_U2.md` §1. Topic `citibike-rides`, 3 particiones, key `stationId`.
- **Fuente:** el día `2026-07-28` (período de prueba, posterior al corte de entrenamiento) reproducido con `SPEEDUP = 60`.
- **Volumen reproducido:** **266 435** eventos (salidas + llegadas) desde las 06:00, con 591 viajes descartados por calidad. Línea `loaded` de `rides.log`:

```json
{"service": "u2-estaciones", "component": "producer-rides", "status": "loaded", "archivos": 5, "replayDate": "2026-07-28", "desdeHora": 6, "eventos": 266435, "descartadosCalidad": 591}
```
- **Evidencia:** Kafka UI con los mensajes de `citibike-rides`. La key de cada mensaje es el `stationId` y los eventos se reparten entre las 3 particiones.

![Kafka UI: mensajes de citibike-rides con stationId como key](img/u2-estaciones/kafka-rides.png)

## 4. Criterio 2 — Ingesta IoT / sensores

- **Fuente:** feed GBFS `station_status` de Citi Bike. Los números de bicis y docks disponibles vienen de los sensores de anclaje de cada estación, y `last_reported` es la hora del reporte del dispositivo.
- **Esquema ajustado:** el `station_id` UUID del feed se enlaza con el `short_name` del histórico, para que ambos topics usen la misma key. Resultado en `gbfs.log`: `"enlazadas": 20, "enlazadasPor": {"short_name": 20}, "sinEnlace": []`.
- **Volumen ajustado:** solo las 20 estaciones del modelo, cada 30 s (≈ 0,67 ev/s).
- **Verificación:** reportes repetidos del sensor deduplicados en Spark: **54** duplicados descartados en los últimos micro-lotes (notebook 02 §8.2), y el reporte inyectado ×3 quedó en **1** fila. Evento con `bicisDisponibles = -3` contado como inválido (`citibike-estaciones`: 1 inválido; 99,92 % válidos en el tablero).
- **Hallazgo:** 2 de las 20 estaciones top (`6876.04` y `6912.01`, en Central Park South) aparecieron fuera de servicio en el feed real (0 bicis, 0 docks), así que el panel de ocupación muestra 18 barras.
- **Evidencia:**

![Fila IoT del tablero](img/u2-estaciones/iot.png)

## 5. Criterio 3 — Procesamiento streaming (ventanas, watermark, checkpoint)

| Elemento | Implementación | Evidencia en vivo |
| :--- | :--- | :--- |
| Ventanas | `window(eventTime, "15 minutes")` por `stationId`, sobre el tiempo del evento | Ventana `2026-07-28 09:45` NYC: 1.º W 21 St & 6 Ave (14 salidas), 2.º S 2 St & Kent Ave (12), 3.º Washington St & Gansevoort St (10) |
| Watermark | 10 min sobre `eventTime`; modo `append` | Viaje inyectado 2 h tarde → `numRowsDroppedByWatermark = 1` (§8.2) |
| Checkpoint | `artifacts/checkpoints/{demanda,estado_iot,calidad}` | `demanda`: 37 en `offsets/`, 36 en `commits/`; `offsets/36` → `{"citibike-rides":{"0":25617,"1":26349,"2":29335}}`; reinicio con `REINICIAR = False` retoma sin reprocesar |
| Deduplicación | `dropDuplicatesWithinWatermark(["stationId","timestamp"])` sobre `publishedAt` (15 min) | Mismo reporte ×3 → 1 fila en `stream/estado_iot` |

## 6. Criterio 4 — Observabilidad

Tablero **U2 · Estaciones — Grimaldo**, fila "Observabilidad":

- Throughput de entrada vs. procesado por consulta. Si procesado ≥ entrada, Spark mantiene el ritmo.
- Duración del micro-lote frente al trigger de 5 s.
- Retraso frente a Kafka (`maxOffsetsBehindLatest`) y tasa de ingesta por topic (kafka-exporter).
- Latencia de ingesta (promedio y p95), eventos válidos e inválidos, filas descartadas por watermark y estado de Spark.

**Correlación observada** (promedios en Prometheus; 3 min a 60x, últimos 90 s a 120x):

| Métrica (`u2_demanda`) | `SPEEDUP = 60` | `SPEEDUP = 120` |
| :--- | ---: | ---: |
| Ingesta en Kafka, `citibike-rides` (msg/s) | 371,3 | 472,9 |
| Entrada de Spark (filas/s) | 373,4 | 498,6 |
| Duración del micro-lote, promedio (ms) | 982 | 2 585 |
| Duración del micro-lote, máximo (ms) | 1 418 | 5 940 |
| Filas en el state store | 2 473 | 3 624 |
| Retraso frente a Kafka (offsets, máx.) | 0 | 0 |

Al subir `SPEEDUP` de 60 a 120, la entrada pasó de 373 a 499 filas/s y la duración del micro-lote de 982 a 2 585 ms en promedio, con picos de 5,9 s **por encima del trigger de 5 s**. El retraso de offsets se mantuvo en 0. La ingesta no se duplicó (×1,27 ya en Kafka) porque las dos mediciones cubren franjas históricas distintas (06-11 h y 11-17 h). Ver `U2_hallazgos.md` §3.

![Fila Observabilidad del tablero](img/u2-estaciones/observabilidad.png)

## 7. Criterio 5 — Costos y criterios de escalado

Calculado en el notebook 02 (§10) con el volumen medido por `u2_calidad`:

| Topic | Bytes/evento | ev/s del sistema real | GB/día | GB en Kafka (7 d) |
| :--- | ---: | ---: | ---: | ---: |
| `citibike-rides` | 278,0 | 6,42 | 0,143 | 1,004 |
| `citibike-estaciones` (todo el sistema, ~2.200 estaciones cada 30 s) | 383,5 | 73,33 | 2,263 | 15,842 |

Costo mensual estimado: **US$ 92,76/mes** = US$ 91,10 de cómputo (3 × t3.medium × 730 h) + US$ 1,66 de almacenamiento (72,19 GB/mes de Parquet crudo, cota superior) (precios de referencia de S9, AWS us-east-1).

**Escalado:** se agrega cómputo a Spark cuando la duración del micro-lote supera el trigger de forma sostenida. Se agregan particiones cuando el retraso de offsets crece. Pasar de 20 a 200 estaciones mantiene **un solo modelo global** y solo multiplica las filas de inferencia por micro-lote.

## 8. Criterio 6 — Salidas BI

Fila "Negocio" del tablero: salidas de la última ventana, ranking top 10 de todas las estaciones, real vs. predicho por estación, pronóstico t+1 y **estaciones en alerta** (pronóstico ≥ 1,5 × la media histórica de la franja). Este último KPI conecta con la "alta demanda" de U1.

![Fila Negocio del tablero en vivo](img/u2-estaciones/negocio.png)

## 9. Criterio 7 — Series de tiempo validadas

**Validación offline (notebook 01, test desde 2026-07-24):**

| Modelo | RMSE | MAE | R² |
| :--- | ---: | ---: | ---: |
| Baseline persistencia | 3,7468 | 2,4977 | 0,4503 |
| Baseline media histórica | 3,1973 | 2,1231 | 0,5997 |
| **LinearRegression (ganador)** | **2,9492** | **1,9805** | **0,6594** |
| GBTRegressor (60 árboles) | 2,9576 | 1,9845 | 0,6575 |

LinearRegression le gana a los dos baselines: un RMSE 21,3 % menor que la persistencia y un MAE 6,7 % menor que la media histórica. GBT queda prácticamente empatado con LinearRegression, así que se eligió el modelo más simple.

**Validación en producción (notebook 02 §9.2):** **200** predicciones comparadas con la ventana real (10 ventanas × 20 estaciones). MAE en producción = **2,784** (RMSE 3,727) frente a MAE en test = **1,980**. El error en vivo es 41 % mayor y se concentra en las estaciones de más demanda de la punta de la mañana (`5712.03`: MAE 4,63). Con más ventanas, el MAE en vivo del tablero bajó a 2,43 (captura de la fila Negocio). Ver `U2_hallazgos.md` §4.

Integrado al tablero BI: el MAE en producción y el MAE en test se ven lado a lado en la fila "Negocio".

## 10. Hallazgos y decisiones

- **Ceros explícitos:** sin la malla estación × ventana completa, los lags saltan sobre las ventanas sin viajes y el modelo aprende una serie que no existe.
- **Watermark de la telemetría sobre `publishedAt`:** con `last_reported`, una estación cuyo sensor lleva rato sin reportar se descartaría como dato tardío.
- **Tiempo del evento histórico:** las ventanas usan la hora original del viaje. Los paneles de Grafana usan la hora de publicación. Por eso el tablero muestra la ventana reproducida como texto ("2026-07-28 09:00 NYC").
- **Limitación:** `citibike-rides` es un día histórico reproducido y `citibike-estaciones` es el estado actual. Se muestran juntos, pero no se cruzan en un mismo KPI porque no son del mismo momento.
- **Estaciones top fuera de servicio:** `6876.04` y `6912.01` reportan 0 bicis y 0 docks en el feed real. La ocupación queda indefinida y no se publica (18 barras de 20).
- **Recrear el productor reinicia el replay detrás del watermark:** para la prueba a 120x, el replay arrancó en la hora 11 (`REPLAY_START_HOUR`), ya que desde las 06:00 todo habría sido dato tardío.
- **MAE en producción > MAE en test** (2,784 frente a 1,980): arranque en frío de los lags y una sola franja del día evaluada.
- **`StackOverflowError` al detener `u2_calidad` (§11):** las consultas se detuvieron bien, pero el hilo dejó una traza de `java.util.regex` cuyo origen no se ve. Causa no identificada; los resultados no se ven afectados.
- Detalle completo (síntoma, diagnóstico, solución y lección) en `lambda26/u2-estaciones/U2_hallazgos.md`.
