# Etapa 2: consumidor persistente y predicciones

La [etapa 3](postgres.md) añade persistencia PostgreSQL y evaluación diferida como
consumidor de esta salida durable. Este documento conserva el alcance/evidencia
de etapa 2; las rutas de su ensayo anterior permanecen intactas.

Estado y cifras de la ejecución comprobada: [informe](reports/streaming/verificacion.md).
Esta etapa termina en Parquet local durable. No implementa PostgreSQL, Grafana,
evaluación diferida ni entrenamiento. Los valores pronosticados son **conteos
reportados por tipo**; elegibilidad no garantiza exactitud física.

## Ejecución y dependencias

El productor sigue en la `.venv` del host. El consumidor es un proceso
`spark-submit`, no un notebook. [lanzar_streaming.py](lanzar_streaming.py) prepara
su configuración y ejecuta [ejecutar_streaming.py](ejecutar_streaming.py) mediante
[compose.streaming.yml](compose.streaming.yml). Reutiliza la imagen ya existente
`lambda26-pyspark-pyspark`; no reconstruye ni modifica Dockerfile, Compose del
equipo, redes de compañeros o el contenedor Jupyter.

Se comprobó el runtime real: **Spark 4.2.0, Scala 2.13.18, Python 3.10.22, Java 21**.
Se fija `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0`, coincidente con Spark y
el binario Scala 2.13. El cliente JVM resuelto es Kafka 3.9.2 y el broker propio
es Apache Kafka 3.9.1; publicación y lectura reales verifican la compatibilidad
utilizada. El productor Python conserva `kafka-python==3.0.11` de etapa 1.
[Lock de los 11 JAR](config/streaming-dependencias.json): versiones, tamaño y SHA256
del conjunto utilizado. No se instala PySpark ni otro paquete Python en el host.
Ivy descarga los JAR fijados a `data/dependencias/ivy/`; el primer arranque requiere
acceso a Maven. Las ejecuciones posteriores verificadas usaron esa caché.

La selección del artefacto sigue la [guía oficial del conector](https://spark.apache.org/docs/4.2.0/streaming/structured-streaming-kafka-integration.html).
La implementación usa `dropDuplicatesWithinWatermark`, `foreachBatch` y sinks
de archivos descritos en la [guía Structured Streaming](https://spark.apache.org/docs/4.2.0/streaming/apis-on-dataframes-and-datasets.html).

El nuevo contenedor pertenece al proyecto `citibike-disponibilidad-streaming`,
entra únicamente en `citibike-disponibilidad_default` y usa
`kafka-disponibilidad:9092`. El productor local conserva `127.0.0.1:19092`.
Montajes: `pyspark/jupyter` → `/opt:ro`, más esta dimensión →
`/opt/UNIDAD2/Dim-Disponibilidad:rw`. U1 se consulta en modo solo lectura.
El wrapper crea archivos de identidad propios en `data/streaming/sistema/`
para el UID/GID del host; evita errores de login Hadoop y archivos propiedad de
root, sin modificar usuarios de la imagen ni del host.

## Comandos exactos desde la raíz del repositorio

Preparar solo si hace falta; el entorno y la imagen ya existen en esta entrega:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/requirements.txt
docker compose -f pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/compose.kafka.yml up -d kafka-disponibilidad
docker inspect --format '{{.State.Health.Status}}' citibike-disponibilidad-kafka
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/ejecutar_u2.py topico --crear
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/lanzar_streaming.py --dependencias --spark-master 'local[2]'
```

No volver a crear la `.venv` si ya existe. No ejecutar `docker build` por esta
etapa; si falta la imagen existente, revisar primero el entorno del equipo.
`topico --crear` verifica sin alterar el tópico si ya existe. Esperar a `healthy`.

Consumidor continuo en primer plano (terminal 1):

```bash
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/lanzar_streaming.py
```

Con configuración inicial predeterminada usa `starting_offsets=latest`:
**iniciar el consumidor antes de publicar** y esperar a `streaming_iniciado`.
Los reinicios con checkpoint ignoran offsets iniciales y recuperan los guardados.
Para replay, elegir explícitamente offsets y nuevas rutas en un JSON; nunca
borrar checkpoints ni cambiar automáticamente a `earliest` ante pérdida de datos.

Publicar una captura (terminal 2):

```bash
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/ejecutar_u2.py producir --capturas 1 --output pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/reports/streaming/mi_captura.json
```

Para alimentar continuamente, usar `producir --capturas 0` en lugar del comando
anterior. El intervalo normal sigue en 300 s; el trigger de Spark no cambia esa
cadencia ni fuerza actualizaciones de la fuente.

Recursos y ejecución acotada:

```bash
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/lanzar_streaming.py --spark-master 'local[2]' --trigger '15 seconds' --duracion-segundos 60
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/lanzar_streaming.py --disponibles
```

El primero mantiene las cuatro consultas concurrentes durante el intervalo y
solicita su cierre limpio; puede tardar más por arranque/cierre del microbatch.
`--disponibles` procesa lo disponible y termina: ejecuta las cuatro consultas
secuencialmente para drenar cada salida derivada. Ambos conservan checkpoints.
No ejecutar dos consumidores con las mismas rutas: un lock lo impide.

Verificar por lectura una salida que contenga **exactamente** las capturas
indicadas en los manifiestos de productor (enumerar todos si son varios):

```bash
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/lanzar_streaming.py --verificar --produccion pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/reports/streaming/mi_captura.json --informe reports/streaming/mi_verificacion.json
```

El comando falla si las capturas/conteos no corresponden. No es un verificador
parcial de un flujo con más capturas: para la evidencia de esta entrega usar su
configuración y sus **dos manifiestos explícitos**, según el informe.

Pruebas sin API ni Kafka reales:

```bash
.venv/bin/python -m unittest discover -s pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/tests -v
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/lanzar_streaming.py --pruebas --spark-master 'local[2]'
```

El primer comando ejecuta las pruebas Python de ingesta y omite las Spark si no
hay PySpark local; el segundo ejecuta las cuatro pruebas Spark en Docker.

Detener: Ctrl+C en las terminales de productor y consumidor; SIGINT/SIGTERM pide
cierre limpio. Si la terminal no está disponible, localizar **solo** el contenedor
de esta dimensión y detenerlo por su nombre exacto:

```bash
docker ps --filter label=com.docker.compose.project=citibike-disponibilidad-streaming --format '{{.Names}}'
docker stop --time 90 NOMBRE_EXACTO_DEL_CONTENEDOR_MOSTRADO
docker compose -f pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/compose.kafka.yml stop kafka-disponibilidad
```

No usar `down -v`, borrar salidas/checkpoints ni detener servicios del equipo.
El wrapper usa `run --rm`: retira solo el contenedor transitorio al finalizar.
Para permitir el cierre Python y su informe antes del JVM, se recomienda ahora
`.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/detener_streaming.py --contenedor NOMBRE_EXACTO_DEL_CONTENEDOR_MOSTRADO`.
El stop directo de Docker puede interrumpir el informe de aplicación; los
checkpoints y salidas durables se recuperan por separado.

## Configuración y contrato de U1

[config/streaming.json](config/streaming.json) es pública y sin secretos.
`--config RUTA_JSON` admite overrides de sus claves; `--spark-master` y
`--trigger` tienen precedencia. Predeterminados: `local[4]`, driver 2 GiB, shuffle
4, trigger 10 s, máximo 10.000 offsets y 4 archivos por microbatch, watermark
10 min y ventana 5 min. Contenedor propio limitado a 4 CPU/3 GiB. La prueba usó
`local[2]`, shuffle 2, watermark/ventana 1 min, sin atribuir sus tiempos a local[4].
No se aumenta automáticamente memoria ni se modifica el servicio PySpark.

Salidas y checkpoints solo pueden estar dentro de `data/streaming/` de esta
dimensión, en raíces separadas. Las rutas relativas se resuelven contra esta
dimensión, independientemente del directorio de la terminal. `u1_reference` apunta
explícitamente a `../../UNIDAD1/Dim-Disponibilidad/config/ejecucion_seleccionada.json`.

Se exige la ejecución **20261117T171917344468Z** y se recargan sus seis originales:
`data/ejecuciones/20261117T171917344468Z/modelos/seleccionados/<tipo>_<5|10|15>min/contrato.json`
dentro de U1. Se comprueban SHA256, tipo/horizonte, candidato, necesidad de fit,
campos requeridos, selección congelada y procedencia del manifiesto. No se elige
la carpeta más reciente ni se cambia al modelo MLlib de referencia si falta algo.
Las reglas/lectura originales también deben coincidir con los hashes del
manifiesto U1; se cargan sin escribir `__pycache__` allí.

Persistencia requiere únicamente `<tipo>_actual`: se toma el conteo explícito por
tipo (`objetivo_convencionales` / `objetivo_electricas` en la salida de calidad).
No se reconstruyen las 30 variables, ni se cargan pesos MLlib, ni se entrena.
El pronóstico double es igual al conteo actual para cada horizonte. No se presenta
como una nueva mejora predictiva ni como evaluación con resultados futuros.

## Esquema, calidad y tiempos

[calidad_streaming.py](src/disponibilidad_u2/calidad_streaming.py) define el
esquema del sobre y reutiliza el esquema de estación U1. Se conserva JSON original,
event_id, capture_id, station_id, source, catálogos/hash, campos de estación,
representaciones originales y normalizadas UTC de captura/last_updated/last_reported,
clave Kafka, tópico, partición, offset y timestamp Kafka. No se infiere el esquema.
El validador ejecutable de etapa 1 comprueba sobre y coherencia de relojes.

La misma función `aplicar_reglas` U1, versión `silver-v2-tipos-gbfs`, se ejecuta
por microbatch y versión de catálogo. Umbral original de antigüedad: **450 s**, no
la tolerancia del watermark. No hay imputación a cero ni deduplicación por cantidad
o last_reported. Se conservan nulos y discrepancias entre campos; elegibilidad es
independiente para cada tipo. Cantidades negativas/ausentes del objetivo,
actividad operativa desconocida/inactiva, antigüedad y conflictos reciben los
motivos U1. Estación inactiva no equivale automáticamente a registro inválido.
Consultar [las reglas originales](../../UNIDAD1/Dim-Disponibilidad/reports/calidad/reglas_y_hallazgos.md).

Un guard adicional de servicio comprueba exactamente el catálogo histórico:
ID 1 bicycle/human, ID 2 bicycle/electric_assist. Un cambio de catálogo conserva
el evento y los indicadores U1, pero impide predecir con contratos incompatibles;
no redefine silenciosamente los objetivos. Se limita a 16 versiones por batch.
No se unen metadatos por estación ni se multiplican filas.

`q_cambio_mismo_reporte` conserva la lógica U1 **dentro del microbatch y versión
de catálogo**, declarada en `alcance_diagnostico_mismo_reporte`; no detecta cambios
entre microbatches. Es diagnóstico y no participa en la elegibilidad U1. No se
afirma paridad de ese diagnóstico con una comparación de todo el histórico a la vez.

Tres relojes distintos: captura (recepción), actualización del feed y reporte de
estación. Además se conserva timestamp Kafka y `procesado_utc` en calidad.
`latencia_procesamiento_s` mide **captura → calidad**, incluyendo red, espera en
Kafka, arranque y backlog; no mide solo CPU ni confirma entrega antes del horizonte.
El progreso Spark aporta tiempos propios del microbatch. Una prueba en replay
puede emitir después de la fecha objetivo: no demuestra un SLA online. La política
de vencimiento para servir predicciones será una decisión posterior explícita.

## Duplicados, retrasos y agregación

Para contratos válidos, el event time es `captura_utc` y la clave de deduplicación
es `event_id`. `dropDuplicatesWithinWatermark` mantiene estado acotado con
watermark **10 min por defecto**: margen configurable para reintentos, outbox y
desorden, no un SLA demostrado por dos capturas. Repetir cantidades o last_reported
en otra captura no elimina observaciones. Se asume que un mismo event_id identifica
un payload inmutable, como garantiza el outbox del productor.

Un sobre inválido usa timestamp Kafka y coordenadas tópico/partición/offset para
evitar que IDs ausentes o inventados colapsen distintos rechazos o adelanten el
watermark. Un reloj de captura más de 300 s en el futuro respecto a Kafka invalida
el sobre. Las fechas y cadenas originales permanecen en JSON para trazabilidad.

Eventos demasiado tardíos pueden descartarse **antes de calidad**, y por tanto no
aparecer en rechazados. `numRowsDroppedByWatermark` y el estado se registran en el
progreso. No se garantiza deduplicación perpetua: IDs salen del estado; el watermark
avanza con el máximo event time observado, no con el reloj de pared durante inactividad.
Para recuperar tardíos se necesita replay explícito de Kafka aún retenido a rutas
nuevas; esta etapa no crea automáticamente ese backfill ni cambia retención.

La cuarta consulta agrega ventanas de 5 min (1 min en la prueba): número de
observaciones, elegibles por tipo, contratos inválidos, rechazo total,
observaciones con cualquier discrepancia y latencia media captura→calidad.
Usa watermark y modo append: solo escribe ventanas finalizadas. Ventanas abiertas
quedan en estado; sus conteos no se deben comparar aún con el total de entrada.
No es evaluación del error futuro del predictor.

## Salidas y garantías

Predeterminadas bajo `data/streaming/principal/`; la prueba vigente tiene rutas
explícitas diferentes, indicadas en su informe. Cuatro consultas y checkpoints:

| Consulta / salida | Checkpoint | Contenido |
|---|---|---|
| observaciones | ingesta | Todos los eventos admitidos tras deduplicar, con campos y calidad; cada batch es un directorio Parquet atómico |
| rechazados | rechazados | Subconjunto total **o parcialmente** no elegible, con motivos por tipo |
| predicciones | predicciones | Hasta seis filas por evento, solo para objetivos elegibles |
| metricas | metricas | Ventanas finalizadas de volumen y calidad |

Observaciones incluye también rechazados: **no son conjuntos disjuntos**.
Un rechazo parcial puede producir tres predicciones del tipo válido.
`prediccion_id` es SHA256 del event_id/tipo/horizonte/versión/SHA del contrato.
La fila incluye origen, fecha objetivo UTC exacta a +300/+600/+900 s, valor double,
versión/procedencia del predictor, metadatos y solo calidad disponible al predecir.

Q1 usa `foreachBatch` con batch_id: escribe a staging fuera del directorio leído,
completa Parquet y metadatos, y publica por rename atómico en el mismo filesystem.
Si el batch ya está publicado con el fingerprint correcto, el reintento no lo
reescribe. Q2/Q3/Q4 leen observaciones mediante file source con esquema explícito,
y utilizan sus propios checkpoints y el log transaccional del sink Parquet nativo.
Leer estas últimas salidas con `spark.read.parquet(ruta)` para respetar
`_spark_metadata`, no concatenar indiscriminadamente archivos tras un fallo.

Esto evita duplicar resultados ante reintentos/reinicios **con checkpoints,
metadatos y filesystem local POSIX intactos y un único escritor**. No es una
transacción única entre Kafka y cuatro salidas ni exactly-once extremo a extremo.
Las salidas derivadas pueden ir retrasadas y se recuperan al reiniciar. Un fallo
puede dejar staging huérfano fuera de la entrada; no se borra automáticamente.
Pérdida de disco, edición manual, restauraciones parciales o pérdida de checkpoints
requieren recuperación explícita, no están cubiertas por la prueba.

`_pipeline.json` vincula configuración semántica, código, reglas y referencia U1.
Cambios incompatibles se rechazan: elegir rutas nuevas explícitas. Se permiten
recursos, trigger y límites por batch distintos sin reiniciar la semántica; los
offsets iniciales no reemplazan un checkpoint existente. La aplicación rechaza
una salida Parquet cuyo checkpoint falta. `failOnDataLoss=true` evita ocultar
offsets eliminados por la retención Kafka de 24 h/1 GiB por partición.

Cada proceso registra configuración, software, contratos y hashes, esquemas,
estado/error y progreso con offsets, watermark, estado y tiempos en
`data/streaming/ejecuciones/<UUID>.json`. La evidencia pequeña versionable se
conserva en `reports/streaming/`; datos, staging, checkpoints y JAR quedan fuera
de Git por las reglas existentes de `data/`. También siguen excluidos `.venv`,
credenciales y entornos locales. No se crea infraestructura para las etapas futuras.
