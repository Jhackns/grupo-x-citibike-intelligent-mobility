# U3 · Hardening, no regresión y riesgos aceptados (S14)

## 1. Lista de comprobación aplicada a `docker-compose.yml`

| # | Comprobación | Antes (entrega U2: 4 compose separados) | Después (U3: un solo `docker-compose.yml`) | Cómo verificarlo |
|---|---|---|---|---|
| 1 | Imágenes con versión exacta | Sí en Kafka/obs; las imágenes propias sin etiqueta | Todas fijas, incluidas las propias (`citibike-espacial/spark:1.0`, `productores:1.0`) | `docker compose config \| grep image` |
| 2 | Healthchecks | Ninguno | Kafka, Prometheus, Pushgateway, Grafana y Spark/Jupyter | `docker ps` muestra `(healthy)` |
| 3 | Orden de arranque | `depends_on` solo esperaba que el contenedor **arrancara** | `depends_on: condition: service_healthy`: espera a que esté **listo** | `docker compose up -d --wait` termina con código 0 |
| 4 | Credenciales | Grafana `admin/admin` escrito en el compose | Variables `GRAFANA_USER`/`GRAFANA_PASSWORD` en `.env` (con valor por defecto) | `.env.example`; `.env` en `.gitignore` |
| 5 | Arranque de productores | `exec -d` a mano; lanzar dos veces mezclaba fechas y el watermark descartaba todo | Un servicio por productor: Docker garantiza **una** instancia y la reinicia si cae | `docker compose ps` |
| 6 | Topics | Se creaban a mano con un bucle de PowerShell | `KAFKA_NUM_PARTITIONS=3`: nacen con 3 particiones al primer evento | Kafka UI → Topics |
| 7 | Confirmación en Prometheus | Validaba "hay un valor", aunque fuera de una corrida anterior | Se consulta por `run_id`: solo cuenta el valor de **esta** corrida | Compuerta "Prometheus tiene los KPI de ESTA corrida" |
| 8 | Fallo a mitad de la corrida | Una excepción en el streaming cortaba el script sin dejar rastro | Se captura, se registra como compuerta fallida y el manifiesto se escribe igual | Manifiesto con `estado: DETENIDA` |
| 9 | Datos y secretos en el repositorio | — | `.gitignore` excluye `datos/*.csv`, `salidas/` y `.env` | `git status` |

## 2. Prueba de no regresión

Se corre `U3_ejecucion_integrada.py` antes y después del cambio y se comparan los manifiestos con
`U3_comparar_manifiestos.py`. La tabla de la corrida real está en `evidencias/no_regresion.md`.

La compuerta que **detiene** se demuestra con `--forzar-fallo`: la corrida queda `DETENIDA`, no arranca Spark
y el manifiesto lo registra (`evidencias/manifiesto_forzar_fallo.json`).

## 3. Riesgos aceptados

| Riesgo | Por qué se acepta aquí | Qué cambiaría en producción |
|---|---|---|
| Kafka sin volumen persistente | Los datos se reproducen desde los CSV; reiniciar de cero es simple | Volumen persistente y factor de replicación 3 |
| Kafka en PLAINTEXT, sin autenticación | Red local de Docker; TLS complicaría cada ejemplo | SASL/TLS y ACL por topic |
| Grafana y Jupyter con credenciales por defecto | Deben arrancar sin configuración previa para la demo | Contraseña propia en `.env` o en un gestor de secretos |
| Feed GBFS público, sin contrato | Es la fuente real de los docks; si cae, solo el panel IoT queda sin datos (el productor registra `gbfs_unreachable` y reintenta) | Fuente con acuerdo de servicio |
| Puertos publicados al host | Se llega a cada herramienta desde el navegador | Solo el punto de entrada detrás de un proxy |
| Pushgateway guarda el último valor | Es el patrón del curso para jobs y notebooks | Exponer métricas con un exporter y alertar si un valor envejece |
| El stream reproduce un día histórico (28 jul) | No hay viajes en vivo públicos; el día es de **prueba** (posterior al corte), el modelo no lo vio | Fuente de viajes en tiempo real del operador |
