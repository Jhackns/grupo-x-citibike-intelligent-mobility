# u2-estaciones — Producto U2 · Dimensión Estaciones (Grimaldo Arredondo)

Pipeline en tiempo real de la dimensión **Estaciones** del Proyecto Sello *CitiBike Intelligent Mobility*:

```
CSV históricos (U1) ── producer_rides_replay.py ──► Kafka: citibike-rides ───────┐
                                                                                 ├─► Spark Structured Streaming ──► Pushgateway ──► Prometheus ──► Grafana
Feed GBFS en vivo   ── producer_gbfs_estaciones.py ─► Kafka: citibike-estaciones ┘    (02_streaming...ipynb)          (+ kafka-exporter)      "U2 · Estaciones — Grimaldo"
                                                                                       modelo del 01_modelo...ipynb
```

| Pieza | Dónde |
|---|---|
| Productores y contrato | `u2-estaciones/` (esta carpeta), `CONTRATO_U2.md` |
| Notebooks | `pyspark/jupyter/UNIDAD2/Dim-Estaciones/01_modelo_series_tiempo_estaciones.ipynb`, `02_streaming_estaciones_inferencia.ipynb` |
| Datos de entrada | `pyspark/jupyter/UNIDAD2/Dim-Estaciones/data/*.csv` (los **mismos 5 CSV de U1**; `.gitignore` los excluye) |
| Tablero | `obs/grafana/provisioning/dashboards/json/u2_estaciones_grimaldo.json` (se carga solo al arrancar Grafana) |

Todos los comandos son para **PowerShell**, desde la raíz del repo del equipo (`grupo-x-citibike-intelligent-mobility`).

## 0. Datos

Copia los 5 CSV de Citi Bike que usaste en U1 a `pyspark\jupyter\UNIDAD2\Dim-Estaciones\data\`:

```powershell
New-Item -ItemType Directory -Force pyspark\jupyter\UNIDAD2\Dim-Estaciones\data | Out-Null
Copy-Item <ruta-de-tus-CSV-de-U1>\*.csv pyspark\jupyter\UNIDAD2\Dim-Estaciones\data\
```

## 1. Infraestructura (Kafka, observabilidad, PySpark)

```powershell
if (-not (Test-Path pyspark\.env)) { Copy-Item pyspark\.env.example pyspark\.env }
docker compose -f kafka/compose.yml up -d
docker compose -f obs/compose.yml up -d --force-recreate grafana   # recarga el provisioning del tablero
docker compose -f pyspark/compose.yml -f pyspark/compose.kafka.yml up -d
```

Topics con 3 particiones. Créalos **antes** de lanzar los productores: si no, Kafka los auto-crea con 1 sola partición.

```powershell
foreach ($t in 'citibike-rides','citibike-estaciones') {
  docker compose -f kafka/compose.yml exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic $t --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
}
docker compose -f kafka/compose.yml exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic citibike-rides --bootstrap-server kafka:9092
```

## 2. Notebook 01 — entrenar el modelo (una sola vez)

Jupyter en `http://localhost:4487/?token=sintoken` (token de `pyspark/.env`, copia de `.env.example`) → `UNIDAD2/Dim-Estaciones/01_modelo_series_tiempo_estaciones.ipynb` → *Run All*. Genera en `artifacts/`:
`estaciones_top.json` (lo necesita el productor GBFS), `models/modelo_estaciones_ts`, `media_slot/` y `metricas_modelo.json`.

## 3. Productores

```powershell
docker compose -f u2-estaciones/compose.yml up -d --build
New-Item -ItemType Directory -Force u2-estaciones\app\logs | Out-Null

docker compose -f u2-estaciones/compose.yml exec -d u2-estaciones sh -c "python -u /app/producer_rides_replay.py > /app/logs/rides.log 2>&1"
docker compose -f u2-estaciones/compose.yml exec -d u2-estaciones sh -c "python -u /app/producer_gbfs_estaciones.py > /app/logs/gbfs.log 2>&1"
```

Comprobar:

```powershell
Get-Content u2-estaciones\app\logs\rides.log -Tail 5    # "loaded" (eventos del día), luego "published"
Get-Content u2-estaciones\app\logs\gbfs.log  -Tail 5    # "starting" con enlazadas=20 y sinEnlace=[], luego "published"
```

- `rides.log` tarda ~1 min en mostrar `loaded`: lee los 5 CSV y se queda con el día `REPLAY_DATE`.
- Si `gbfs.log` muestra `sinEnlace` con IDs, esas estaciones no se encontraron en el feed: ni por `short_name` ni por nombre. Las demás siguen funcionando.
- Kafka UI (`http://localhost:48085`) → topics `citibike-rides` y `citibike-estaciones` → *Messages*.

Parámetros (en `compose.yml`, se aplican al **recrear** el contenedor con `up -d`): `REPLAY_DATE`, `REPLAY_START_HOUR`, `SPEEDUP`, `REPLAY_LOOP`, `GBFS_INTERVAL_S`, `GBFS_NUM_ESTACIONES`.

## 4. Notebook 02 — streaming + inferencia en vivo

`UNIDAD2/Dim-Estaciones/02_streaming_estaciones_inferencia.ipynb` → ejecutar hasta la sección 7 (arranque). Deja las consultas corriendo y abre Grafana: `http://localhost:43000` (admin/admin) → *Dashboards* → carpeta **Proyecto Sello** → **U2 · Estaciones — Grimaldo**.

Las secciones 8-10 son la evidencia para la sustentación: dato tardío, duplicado e inválido inyectados, checkpoint, ranking y pronóstico, MAE en producción y costos. La sección 11 detiene todo.

## 5. Evidencias a capturar (por criterio)

| Criterio | Evidencia |
|---|---|
| 1. Eventos empresariales | `rides.log` publicando + Kafka UI con mensajes de `citibike-rides` + `CONTRATO_U2.md` §1 |
| 2. IoT/sensores | `gbfs.log` (`enlazadas=20`) + fila **IoT** del tablero + duplicados descartados (notebook 02, §8.2) |
| 3. Streaming | §8.1 (`lastProgress` con watermark y estado), §8.2 (`numRowsDroppedByWatermark` = 1 tras el viaje tardío), §8.3 (checkpoint) |
| 4. Observabilidad | Fila **Observabilidad** del tablero: throughput, duración de micro-lote, retraso de offsets, latencia y válidos |
| 5. Costos y escalado | Notebook 02, §10 (tabla con valores medidos + criterios de escalado) |
| 6. BI | Fila **Negocio**: ranking actual, observado y pronóstico por estación, estaciones en alerta |
| 7. Series de tiempo | Notebook 01 (§8 comparación contra baselines, §12 gráfico) + "MAE en producción" vs. "MAE en test" en el tablero |

## Problemas comunes

- **El tablero no aparece:** Grafana lee el provisioning solo al arrancar. Usa `docker compose -f obs/compose.yml up -d --force-recreate grafana`.
- **Paneles "No data" en la fila Negocio:** la primera ventana se emite cuando el watermark pasa su final (~25 s reales con `SPEEDUP=60`). Revisa que `rides.log` esté publicando.
- **`estaciones_top.json` no existe:** falta correr el notebook 01 antes del productor GBFS.
- **Las consultas no leen nada al relanzar:** un checkpoint reutilizado ignora `startingOffsets` (lección de S08). Usa `REINICIAR = True` en §7.
- **`gbfs_unreachable`:** sin internet o el feed caído. El productor reintenta cada `GBFS_INTERVAL_S`.
