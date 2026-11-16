# Contrato de eventos — U2 · Dimensión Estaciones (Grimaldo Arredondo)

Proyecto Sello **CitiBike Intelligent Mobility** · Equipo **Data Geniuses**

| Topic | Tipo de fuente | Productor | Key de Kafka | Particiones | Volumen (demo) |
|---|---|---|---|---|---|
| `citibike-rides` | **Eventos empresariales**: viajes del sistema (histórico real reproducido) | `app/producer_rides_replay.py` | `stationId` | 3 | ≈ 2 eventos por viaje; con `SPEEDUP=60`, un día real se reproduce en 24 min |
| `citibike-estaciones` | **IoT / telemetría**: estado de los docks (feed GBFS en vivo) | `app/producer_gbfs_estaciones.py` | `stationId` | 3 | `GBFS_NUM_ESTACIONES / GBFS_INTERVAL_S` = 20 / 30 s ≈ 0,67 ev/s |

Las dos fuentes comparten la **misma clave de estación** (`stationId` = el `start_station_id` / `end_station_id` de los CSV, por ejemplo `6140.05`). Por eso un mismo `stationId` cae siempre en la misma partición, en los dos topics.

## 1. `citibike-rides` — `viaje.salida` / `viaje.llegada`

Cada viaje del histórico genera dos eventos: la **salida**, en `started_at` con la estación de origen, y la **llegada**, en `ended_at` con la estación de destino.

```json
{
  "tipoEvento": "viaje.salida",
  "stationId": "6140.05",
  "stationName": "W 21 St & 6 Ave",
  "rideId": "A1B2C3D4E5F60718",
  "rideableType": "electric_bike",
  "memberCasual": "member",
  "origen": "citibike-tripdata-replay",
  "timestamp": 1785232800000,
  "publishedAt": 1793325089320
}
```

| Campo | Tipo | Descripción | Regla de validez (`isValid`) |
|---|---|---|---|
| `tipoEvento` | string | `viaje.salida` o `viaje.llegada` | uno de los dos valores |
| `stationId` | string | ID de estación del CSV (**key de Kafka**) | no nulo |
| `stationName` | string | Nombre de la estación | — |
| `rideId` | string | `ride_id` del viaje | — |
| `rideableType` | string | `classic_bike` / `electric_bike` | — |
| `memberCasual` | string | `member` / `casual` | — |
| `origen` | string | `citibike-tripdata-replay` | — |
| `timestamp` | long (ms epoch) | **Tiempo del evento**: hora original del viaje (zona `America/New_York`) | > 0 |
| `publishedAt` | long (ms epoch) | Hora real de publicación en Kafka (para medir la latencia de ingesta) | no nulo |

**Calidad en origen** (las mismas reglas de la capa Silver de U1): se deduplica por `ride_id`, se descartan los viajes sin estación de origen o destino y solo se aceptan duraciones entre 1 y 180 min.

**Por qué un día del período de prueba:** el modelo se entrenó con fechas anteriores a `2026-07-24`, el mismo corte de U1. El productor reproduce `REPLAY_DATE = 2026-07-28`, así que la inferencia en vivo corre sobre datos que el modelo no vio. En modo `REPLAY_LOOP=true`, cada vuelta se desplaza 24 h para que el tiempo del evento siga creciendo y el watermark no descarte la vuelta siguiente como dato tardío.

## 2. `citibike-estaciones` — `estacion.estado`

```json
{
  "tipoEvento": "estacion.estado",
  "stationId": "6140.05",
  "gbfsStationId": "66dc120f-0aca-11e7-82f6-3863bb44ef7c",
  "stationName": "W 21 St & 6 Ave",
  "capacidad": 51,
  "bicisDisponibles": 12,
  "ebikesDisponibles": 4,
  "docksDisponibles": 37,
  "bicisInhabilitadas": 1,
  "docksInhabilitados": 0,
  "enServicio": true,
  "origen": "gbfs",
  "timestamp": 1793325080000,
  "publishedAt": 1793325092518
}
```

| Campo | Tipo | Unidad | Descripción | Regla de validez |
|---|---|---|---|---|
| `tipoEvento` | string | — | Siempre `estacion.estado` | igual a `estacion.estado` |
| `stationId` | string | — | `short_name` GBFS = ID del CSV (**key de Kafka**) | no nulo |
| `gbfsStationId` | string | — | `station_id` (UUID) del feed | — |
| `capacidad` | int | anclajes | Capacidad declarada de la estación | — |
| `bicisDisponibles` | int | bicicletas | `num_bikes_available` | 0 a 150 |
| `ebikesDisponibles` | int | bicicletas | `num_ebikes_available` | — |
| `docksDisponibles` | int | anclajes | `num_docks_available` | 0 a 150 |
| `bicisInhabilitadas`, `docksInhabilitados` | int | — | Bicis y docks fuera de servicio | — |
| `enServicio` | bool | — | `is_installed = 1` y `is_renting = 1` | — |
| `timestamp` | long | ms epoch | **Hora del sensor** (`last_reported` × 1000) | > 0 |
| `publishedAt` | long | ms epoch | Hora en que se leyó el feed | no nulo |

**Ajustes de esquema y volumen:**
- **Enlace con el histórico:** el feed identifica las estaciones con un UUID, mientras que los CSV usan el `short_name`. El productor los enlaza con `station_information.json`: primero por `short_name` y, si no lo encuentra, por nombre exacto. El log de arranque (`enlazadasPor`) muestra cuántas se enlazaron por cada vía.
- **Solo las estaciones top-K** del notebook 01 (`artifacts/estaciones_top.json`): telemetría y modelo siguen el mismo conjunto. K e intervalo se controlan con `GBFS_NUM_ESTACIONES` y `GBFS_INTERVAL_S`.
- **Reportes repetidos:** una estación tranquila repite el mismo `last_reported` en varias lecturas. El productor publica todo lo que lee y **Spark deduplica** por (`stationId`, `timestamp`). El número de duplicados descartados se ve en Grafana.
- **Rango 0-150:** un conteo negativo es físicamente imposible y ninguna estación de NYC tiene más de 150 anclajes. Un valor fuera de rango cuenta como **inválido**. No se descarta en silencio: el panel "Eventos inválidos" lo muestra.

## 3. Grupos y consumidores

| Consumidor | Topics | Checkpoint |
|---|---|---|
| `u2_demanda` (Spark) | `citibike-rides` | `artifacts/checkpoints/demanda` |
| `u2_estado_iot` (Spark) | `citibike-estaciones` | `artifacts/checkpoints/estado_iot` |
| `u2_calidad` (Spark) | ambos | `artifacts/checkpoints/calidad` |

Spark Structured Streaming no usa un *consumer group* de Kafka para sus offsets: los guarda en el checkpoint. Por eso el retraso se mide con `maxOffsetsBehindLatest` del conector y no con `kafka_consumergroup_lag`.
