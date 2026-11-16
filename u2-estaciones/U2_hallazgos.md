# U2 Estaciones — Errores y hallazgos reales

Registro de lo que realmente falló o sorprendió al integrar y ejecutar la dimensión **Estaciones** contra Kafka, el feed GBFS real y Grafana
(29 de octubre de 2026, ~21:45-22:20 hora de Lima). Sirve para la sección "Error o hallazgo" de la sustentación.

## 1. Dos estaciones top estaban fuera de servicio: el panel de ocupación mostraba 18 barras, no 20

- **Síntoma:** "Estaciones reportando" marcaba 20, pero "Ocupación por estación" solo tenía 18 barras (`count(lambda26_u2est_iot_ocupacion_pct)` = 18).
- **Diagnóstico:** las estaciones `6876.04` (Central Park S & 6 Ave) y `6912.01` (7 Ave & Central Park South) reportaban en el feed GBFS real `bicisDisponibles = 0`, `docksDisponibles = 0` y `enServicio = false`. La ocupación `bicis / (bicis + docks)` es 0/0, así que `procesar_estado` la deja en `None` y no la publica. Las dos están entre las 20 de mayor demanda en train (puestos 7 y 10).
- **Solución:** no hay nada que corregir: el comportamiento es el correcto. El sensor sí reporta (cuentan en "Estaciones reportando"), pero la estación no está operando y no tiene sentido hablar de ocupación. Tampoco cuentan como "vacías", porque ese KPI exige `enServicio`.
- **Lección:** una estación con mucha demanda histórica puede estar cerrada hoy. El tablero IoT lo deja ver, y es justo el cruce que un operador necesita antes de mandar un camión de rebalanceo.

## 2. Precaución de diseño: recrear el productor reinicia el replay detrás del watermark

- **Situación:** la prueba de correlación del criterio 4 pide cambiar `SPEEDUP` a 120 y recrear el contenedor. Al recrearlo, `producer_rides_replay.py` vuelve a empezar en `REPLAY_START_HOUR` (06:00 históricas). A esa altura el watermark de `u2_demanda` ya iba por ~10:50 del 2026-07-28.
- **Diagnóstico:** con el watermark de 10 min, todo viaje con hora de evento anterior a ~10:40 es tardío y `u2_demanda` lo descartaría. Habría throughput, pero ninguna ventana nueva, y la medición del micro-lote no sería comparable. Es el mismo efecto del viaje inyectado en §8.2, solo que con miles de eventos. No lo dejé ocurrir: lo anticipé antes de recrear el contenedor.
- **Solución:** para la medición a 120x usé `REPLAY_START_HOUR = 11`, la hora siguiente a la última publicada (`10:58`). Al terminar, `compose.yml` volvió a `SPEEDUP = 60` y `REPLAY_START_HOUR = 6`, y el replay de cierre se lanzó con `exec -e REPLAY_START_HOUR=18` para seguir por delante del watermark (~17:45).
- **Lección:** en una fuente reproducida, el tiempo del evento lo controla el productor. Reiniciarlo es como recibir datos de hace horas, y el watermark los descarta, que es justo para lo que existe.

## 3. Correlación SPEEDUP 60 → 120: la ingesta no se duplicó, el micro-lote sí se alargó

- **Síntoma:** se esperaba el doble de entrada al duplicar `SPEEDUP`. En Kafka, la ingesta de `citibike-rides` pasó de 371,3 a 472,9 mensajes/s (×1,27), y la entrada de `u2_demanda` pasó de 373,4 a 498,6 filas/s (×1,34).
- **Diagnóstico:** el crecimiento se ve ya en Kafka (kafka-exporter), antes de Spark, así que Spark no es el freno: su retraso de offsets siguió en 0. Las dos mediciones cubren franjas históricas distintas (06:00-11:00 a 60x y 11:00-17:45 a 120x) con distinta demanda. No medí por separado si el productor alcanza el ritmo nominal, así que la causa exacta queda abierta.
- **Lo que sí cambió en Spark:** la duración del micro-lote de `u2_demanda` pasó de 982 ms a 2 585 ms en promedio, con un máximo de 5 940 ms, **por encima del trigger de 5 s**. Las filas en el state store pasaron de 2 473 a 3 624.
- **Lección:** la duración del micro-lote es la primera señal de saturación, antes que el retraso de offsets. Un máximo por encima del trigger es el umbral de escalado que propone §10.

## 4. MAE en producción mayor que en test

- **Síntoma:** §9.2: MAE en producción = 2,784 (200 predicciones, 10 ventanas × 20 estaciones), frente a 1,980 en test (+41 %).
- **Diagnóstico:** el error se concentra en las estaciones de más demanda de esa mañana (`5712.03` MAE 4,63 con 12,6 salidas medias, y `6233.04` MAE 4,06 con 14,1). Las primeras 4 ventanas de cada estación arrancan en frío: los lags se rellenan con la media histórica de la franja, no con datos observados. El test offline promedia 7 días completos, y la validación en vivo cubrió solo la punta de la mañana del 28 de julio. Con el stream corriendo más tiempo, el MAE en vivo bajó a 2,61 (valor del tablero tras la corrida a 120x).
- **Lección:** el número del test no es una promesa para cualquier tramo del día. La métrica en producción tiene que estar en el mismo tablero que la del test, para notar cuándo se separan.

## 5. Datos de U1: los CSV no están en el repo del equipo

- **Síntoma:** `pyspark/jupyter/UNIDAD1/data/` no existe en `grupo-x-citibike-intelligent-mobility`: su `.gitignore` excluye `*.csv` y `**/data/`.
- **Solución:** copié los 5 `202607-citibike-tripdata_*.csv` de `D:\descargas\202607-citibike-tripdata\`. La verificación es la salida del notebook 01: `Rango: 2026-06-30 -> 2026-07-31 (32 días) | corte train/test: 2026-07-24`, igual que en U1.

## 6. Remotos de `lambda26`: el fork no es `origin`

- **Síntoma:** en el clon local, `origin` apunta a `262bigdata/lambda26`, donde no hay permiso de escritura, y el fork `grimaldoupeu/lambda26` está como `fork`.
- **Solución:** la rama `u2-estaciones-grimaldo` se sube con `git push fork ...`, no a `origin`.

## 7. El notebook 02 se ejecutó por fases con `nbclient`, no desde la interfaz

- La guía pide ejecutarlo en Jupyter para que las consultas sigan vivas mientras se mira Grafana. Desde Claude Code no hay interfaz, así que un script (`artifacts/driver/run02.py`, no versionado) abrió **un solo kernel** y ejecutó §1-7, dejó las consultas corriendo, luego §8-10 y, al final, §11. Guardó el `.ipynb` con las salidas después de cada celda. El efecto es el mismo que en la interfaz: el kernel y las consultas siguen vivos entre fases.

## 8. `StackOverflowError` en el hilo de `u2_calidad` al detener las consultas (§11)

- **Síntoma:** §11 imprimió `Consultas detenidas: ninguna activa` (las tres se detuvieron), seguido de `Exception in thread "stream execution thread for u2_calidad ..." java.lang.StackOverflowError` y una traza de ~74 KB.
- **Diagnóstico:** la traza tiene 1 024 frames, todos de `java.util.regex.Pattern` (recursión de una expresión regular). La JVM la corta en ese límite, así que no se ve qué parte de Spark la invocó. El notebook no usa expresiones regulares en `u2_calidad`. Ocurrió durante el `stop()`, cuando ya estaban escritos los Parquet de `stream/calidad` y los checkpoints, y ninguna celda quedó con error. La causa exacta **no quedó identificada**.
- **Solución:** ninguna por ahora. La salida se dejó tal cual en el notebook como evidencia, y no afecta los resultados de §8-10.
- **Lección:** detener una consulta a mitad de micro-lote interrumpe su hilo, y el error puede salir por stderr aunque `stop()` termine bien. Para confirmar el cierre hay que mirar `spark.streams.active`, no solo el ruido de la consola.

## Resultados de la corrida (29 de octubre de 2026)

| Paso | Resultado real |
|---|---|
| Notebook 01 §2 | Bronze 4 993 137 → Silver 4 971 847 (21 290 descartados por calidad) |
| Notebook 01 §3 | `2026-06-30 -> 2026-07-31`, corte `2026-07-24` |
| Notebook 01 §8 | Ganador LinearRegression: RMSE 2,9492, MAE 1,9805, R² 0,6594 (media histórica: 3,1973 / 2,1231 / 0,5997) |
| Notebook 01 §11 | `Pushgateway: 200` |
| `rides.log` | `loaded`: 266 435 eventos del 2026-07-28 desde las 06:00 (591 descartados por calidad) |
| `gbfs.log` | `enlazadas: 20`, `enlazadasPor: {"short_name": 20}`, `sinEnlace: []` |
| Notebook 02 §8.2 | 1 fila descartada por watermark, 54 duplicados descartados, `prueba-duplicado` → 1 fila, 1 inválido en `citibike-estaciones` |
| Notebook 02 §8.3 | `demanda`: 37 en `offsets/`, 36 en `commits/`; último `offsets/36` → `{"citibike-rides":{"0":25617,"1":26349,"2":29335}}` |
| Notebook 02 §9.2 | 200 predicciones, MAE 2,784, RMSE 3,727 (test: 1,980) |
| Notebook 02 §10 | US$ 91,10 cómputo + US$ 1,66 almacenamiento = **US$ 92,76/mes** |
