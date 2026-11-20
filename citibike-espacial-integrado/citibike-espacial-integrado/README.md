# CitiBike Intelligent Mobility · Proyecto integrado (U1 + U2 + U3)

**Cristhian Chuquitarqui Chura · Análisis espacial** · Big Data lambda26 · Equipo **Data Geniuses**

> **Pregunta central:** ¿dónde se concentra la actividad de Citi Bike NYC, hoy y en la siguiente ventana de 15 minutos?
> **Historia:** **ANALIZAR EL PASADO** (U1 · batch) → **ANTICIPAR EL FUTURO** (U2 · streaming) → **COMPROBAR EL FUNCIONAMIENTO** (U3 · integración)

Esta versión reúne todo en **un solo notebook** y **un solo dashboard de Grafana**:

- 📓 **`notebook/CitiBike_Integrado_U1_U2_U3.ipynb`**: el código completo de U1, U2 (modelo + streaming) y U3, en orden.
- 📊 **Dashboard general**: http://localhost:43000/d/citibike-general-cristhian, con tres filas U1, U2 y U3.

## Cómo correrlo

Requisitos: **Docker Desktop abierto** y unos **8 GB de RAM**. No hace falta instalar Python ni Java.

### Opción A · Un solo comando (Windows)

```powershell
powershell -ExecutionPolicy Bypass -File .\ejecutar.ps1
```

El script levanta todo, descarga los datos (~974 MB), ejecuta el notebook completo (~20 min) y deja el streaming en vivo para Grafana.

### Opción B · Paso a paso (Windows, Linux o Mac)

```bash
cp .env.example .env                                     # en Windows: copy .env.example .env
docker compose up -d --build --wait
docker compose exec spark python datos/descargar_datos.py
```

Después abre Jupyter (http://localhost:4488/?token=sintoken), entra a `notebook/CitiBike_Integrado_U1_U2_U3.ipynb` y pulsa **Run → Run All Cells**.
Mientras corre, abre el dashboard general en Grafana.

## Qué abrir

| Herramienta | URL |
|---|---|
| **Dashboard general** | http://localhost:43000/d/citibike-general-cristhian (admin / admin; si pide cambiar la contraseña, pulsa *Skip*) |
| Jupyter | http://localhost:4488/?token=sintoken |
| Kafka UI | http://localhost:48085 |
| Prometheus | http://localhost:49090 |

## Contenido

```
├── notebook/CitiBike_Integrado_U1_U2_U3.ipynb   ← TODO el proyecto (U1 + U2 + U3)
├── docker-compose.yml        ← Kafka, productores, Spark/Jupyter, Pushgateway, Prometheus y Grafana
├── ejecutar.ps1              ← un solo comando (Windows)
├── scripts/
│   ├── U3_ejecucion_integrada.py      ← compuertas + batch + streaming + KPI + manifiesto (lo llama el notebook)
│   ├── U3_comparar_manifiestos.py     ← no regresión entre corridas
│   ├── streaming_en_segundo_plano.py  ← mantiene el streaming U2 vivo para Grafana
│   └── generar_dashboard_general.py   ← reconstruye el dashboard con los resultados de U1
├── productores/              ← eventos de viajes (Kafka citibike-rides) y feed GBFS en vivo (citibike-estaciones)
├── infra/                    ← Spark (Dockerfile), Prometheus y Grafana (solo el dashboard general)
├── datos/                    ← descargar_datos.py; aquí quedan los 5 CSV (no se suben a git)
├── evidencias/               ← resultados reales de la entrega (métricas, manifiestos, no regresión, gráficos)
├── docs/                     ← contrato de eventos, hardening y riesgos, fuentes y consultas del dashboard
└── salidas/                  ← lo que genera el notebook (se crea al correr; no se sube a git)
```

## Resultados (corridas reales con datos de julio 2026)

| Unidad | Resultado |
|---|---|
| **U1 · Batch** | 4.993.137 viajes → Silver 99,57 %; 2.418 estaciones en 99 zonas; las 10 zonas top suman el **52,8 %** de la actividad; GBTRegressor con RMSE −9,7 % frente al baseline; alta actividad detectada con **F1 = 0,94** |
| **U2 · Streaming** | Modelo t+1 de las 15 zonas top (LinearRegression) con **RMSE 27,74**, un **9,9 % mejor** que la persistencia; inferencia en vivo con Kafka + Spark Structured Streaming (ventanas de 15 min, watermark, checkpoint, deduplicación del sensor GBFS) |
| **U3 · Integración** | Se levanta con un comando; la ejecución integrada pasa **21/21 compuertas**; latencia de ingesta p50 ≈ 2,5 s y p95 ≈ 4,8 s; prueba de fallo controlado y no regresión entre corridas |

## Comandos útiles

| Para… | Comando |
|---|---|
| Detener el streaming en segundo plano | `docker compose exec spark python scripts/streaming_en_segundo_plano.py --detener` |
| Volver a arrancarlo | `docker compose exec -d spark python scripts/streaming_en_segundo_plano.py` |
| Correr solo la U3 | `docker compose exec spark python scripts/U3_ejecucion_integrada.py` |
| Apagar todo | `docker compose down` |

## Problemas frecuentes

- **`port is already allocated`:** otro proyecto usa los mismos puertos (4488, 43000, 49092…). Detén sus contenedores (`docker stop <nombre>`) y vuelve a correr `docker compose up -d --wait`. Si Kafka queda sin red (en su log aparece `UnknownHostException: kafka`), recrea los contenedores con `docker compose up -d --force-recreate --wait`.
- **"No data" en la fila U2:** el streaming no está corriendo. Arráncalo con el comando de la tabla y espera 2–3 minutos.
- **"No data" en la fila U3:** Pushgateway no guarda las métricas al reiniciarse. Vuelve a correr la sección U3 del notebook.
- **Antes de volver a correr el notebook completo**, detén el streaming en segundo plano, para no tener dos consumidores a la vez.

Más detalle de cada panel y su consulta en `docs/DASHBOARD_GENERAL.md`; riesgos aceptados en `docs/U3_hardening_y_riesgos.md`.
