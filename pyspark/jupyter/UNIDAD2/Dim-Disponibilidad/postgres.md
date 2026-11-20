# Etapa 3: PostgreSQL y evaluación diferida

Se añade persistencia SQL al flujo existente, sin modificar el código/fingerprint
de las cuatro consultas de etapa 2. No hay entrenamiento nuevo. Este documento conserva la evidencia de etapa 3;
Grafana se incorpora después en [observabilidad.md](observabilidad.md), sin alterar
sus reglas ni los resultados históricos.
La pregunta sigue referida a **conteos reportados explícitamente por tipo**,
no a cantidades físicas verificadas. Elegibilidad no garantiza exactitud.

Resultados ejecutados: [reports/postgres/verificacion.md](reports/postgres/verificacion.md).
La evidencia de etapa 2 se conserva en sus rutas anteriores; el ensayo de etapa 3
tiene configuración, flujo SQL y checkpoints explícitos diferentes.

```text
Productor API → Kafka propio → Spark etapa 2 → observaciones Parquet con calidad
                                              ↓ Spark, file source + checkpoint SQL
                     misma función predecir de U1 seleccionada (persistencia)
                                              ↓ transacción por microbatch
                  PostgreSQL: observaciones + rechazos + predicciones + pendientes
                                              ↓ barrera temporal y de progreso
                    evaluadas / vencidas → vistas de métricas y cobertura
```

El consumidor SQL lee **observaciones durables** de etapa 2 y reutiliza su misma
función `predecir`. Así publica observación, rechazo y seis pronósticos posibles
de forma transaccional sin depender del orden de llegada de tres sinks Parquet
independientes. Los IDs y valores deben coincidir con los de etapa 2. No se
reconstruyen variables Gold ni se cargan pesos MLlib.

## Infraestructura y dependencias

Se revisaron los servicios existentes. El PostgreSQL activo pertenece a Cloudshield;
el de otro proyecto está detenido. Ambos se dejaron intactos. Se reutilizó la
**imagen local PostgreSQL 17.11**, fijada por digest en
[compose.postgres.yml](compose.postgres.yml), para un servicio propio:

| Elemento | Configuración |
|---|---|
| Proyecto / contenedor | citibike-disponibilidad-postgres / citibike-disponibilidad-postgres |
| Base / esquema | disponibilidad_u2 / disponibilidad |
| Red existente propia | citibike-disponibilidad_default |
| Desde Docker / host | postgres-disponibilidad:5432 / 127.0.0.1:15432 |
| Volumen persistente | citibike-disponibilidad-postgres_postgres_data |
| Recursos PostgreSQL | 1 CPU / 768 MiB; no se cambia memoria de otros servicios |
| Spark / Scala | 4.2.0 / 2.13.18, imagen existente sin reconstrucción |
| Controlador fijado | org.postgresql:postgresql:42.7.14 |

[Versiones, digest y SHA de JAR](config/postgres-dependencias.json), incluido
`checker-qual:3.55.1`. Se usa JDBC dentro del JVM de Spark; **ningún paquete Python
nuevo**, ninguna instalación global. Se conserva `requirements.txt` de etapa 1.
[Documentación oficial del controlador](https://jdbc.postgresql.org/download/).

[preparar_postgres.py](preparar_postgres.py) crea una contraseña aleatoria local
solo si falta, con permisos 600, bajo `data/secretos/`. No imprime su valor ni la
regenera al reejecutar. Compose la pasa como secret; el cliente la lee de un
archivo montado mediante [compose.sql-cliente.yml](compose.sql-cliente.yml).
No se añade a URL JDBC, argumentos, SparkConf, informes o Git. No cambiar ese
archivo si ya hay un volumen inicializado: PostgreSQL no cambia la contraseña
del volumen existente mediante POSTGRES_PASSWORD_FILE.

La base es exclusiva de esta dimensión; el usuario de inicialización administra
solo este PostgreSQL aislado. No es un rol de lectura para un futuro dashboard.
La aplicación comprueba `current_database() = disponibilidad_u2` y usa únicamente
el esquema `disponibilidad`. No ejecuta DDL en tablas de compañeros.

## Esquema e idempotencia

DDL versionable: [sql/001_disponibilidad.sql](sql/001_disponibilidad.sql).
La tabla `esquema` fija versión/SHA; un DDL distinto requiere migración explícita,
no se aplica sobre la misma versión silenciosamente.

| Tabla / vista | Clave y propósito |
|---|---|
| flujos | flow_id explícito ligado a fingerprint de entrada, reglas, definición de conteo y referencia U1 |
| lotes | (flujo, batch_id), recibo transaccional de un microbatch y confirmación del commit |
| observaciones | (flujo, observacion_id): event_id válido o coordenada Kafka del rechazo sin identidad válida |
| rechazos | (flujo, observacion_id), FK a observación; motivos independientes por objetivo |
| predicciones | (flujo, prediccion_id), FK a observación; además unicidad de evento/tipo/horizonte/versión/contrato |
| evaluaciones | (flujo, prediccion_id), FK a predicción y a la captura futura seleccionada |
| rondas_evaluacion | UUID por ronda con cierres y confirmación del commit |
| cobertura | Estado y proporción evaluada por tipo/horizonte/ventana UTC de origen de 5 min |
| metricas | MAE, RMSE, sesgo y n para evaluadas; totales y estratos cruzados |
| latencias | Fuente, captura→SQL y fecha objetivo→evaluación, manteniendo sus relojes separados |

Observaciones conserva el payload completo, JSON original, campos/tiempos de
estación, capture_id/event_id, metadatos Kafka, catálogos y calidad. Predicciones
conserva origen/objetivo, tipo/horizonte, valor double, versión/SHA/procedencia U1
y calidad disponible al emitir. Evaluaciones añade calidad de la captura futura,
sin introducirla en los predictores.

Fechas SQL tipadas inválidas quedan NULL y el valor original permanece en payload;
la validación nativa [`pg_input_is_valid`](https://www.postgresql.org/docs/17/functions-info.html#FUNCTIONS-INFO-VALIDITY)
evita abortar todo el lote por una fecha inválida. Una captura con reloj válido
y contrato inválido conserva su fecha y participa en la selección de candidatos,
para no ocultarla escogiendo otra más conveniente. Nunca se rellenan cantidades
ausentes con cero. Los rechazos también son observaciones: no sumar ambos como
conjuntos disjuntos. Repetir cantidades en capturas diferentes conserva sus filas.

La escritura usa bloques JSON de 500 filas y prepared statements, iterados desde
Spark sin recopilar el histórico completo. Cada microbatch tiene **una sola
transacción** para observaciones/rechazos/predicciones/pendientes/recibo. Las FK
de lote se comprueban al commit. `ON CONFLICT` conserva el contenido existente y
rechaza identidades con payload distinto; no sobrescribe predicciones para mejorar
resultados. El recibo ya confirmado permite omitir un callback repetido.
[Semántica PostgreSQL de ON CONFLICT](https://www.postgresql.org/docs/17/sql-insert.html).

Si falla antes de commit, rollback; si el commit fue aceptado pero se perdió el
ACK, se recupera el recibo por flow_id/batch_id y no se insertan filas otra vez.
La confirmación posterior al commit se completa si falta, sin reemplazar una
marca existente. Hay tres intentos acotados con espera 1/2 s. Los errores necesarios
hacen terminar la consulta con error; no se declara éxito de un lote parcial.

Un lock de checkpoint, un lock de sesión SQL por flujo y locks transaccionales
evitan escritores simultáneos y serializan carga/cierre. Una salida SQL existente
sin su checkpoint se rechaza. Esto depende de conservar checkpoints, metadatos,
claves y volumen: no cubre restauraciones parciales o copias obsoletas de checkpoints.
Kafka, Parquet y SQL **no forman una transacción distribuida**, ni se afirma
exactly-once extremo a extremo. La retención Kafka y la política de tardíos de
etapa 2 continúan aplicándose.
El evaluador intenta adquirir el lock sin esperar: si un lote está cargando,
aplaza la ronda al siguiente poll. Cada ronda cierra como máximo 4.000 registros
para no bloquear la ingesta durante un barrido grande. La prueba de concurrencia
cubre el caso detectado durante la ejecución real.

## Emparejamiento temporal, equivalente a U1

Se recargan los seis contratos seleccionados de U1 **20261117T171917344468Z** con
SHA, manifiesto y selección congelada, sin escoger carpetas recientes. Persistencia
es el ganador de las seis combinaciones y no requiere fit. SQL valida que la salida
de etapa 2 procede de esa misma referencia. El código U1 no se modifica.

Los parámetros se contrastan con la configuración U1: **tolerancia 30 s, hueco
máximo 450 s**, versión `gold-v3-emparejamiento-estrecho`. La definición sigue siendo
vehicle_types_available, ID 1 bicycle/human e ID 2 bicycle/electric_assist.
Los catálogos incompatibles no se reinterpretan automáticamente.

Para cada predicción:

1. Fecha ideal = origen UTC + 5/10/15 min. Esperar a superar **ideal + 30 s +
   margen tardío**, 600 s por defecto, como mínimo el watermark **real** registrado
   en el manifiesto de etapa 2.
2. Exigir además `max(captura válida guardada) − margen >= ideal + 30 s`.
   Esta barrera de progreso evita cerrar por reloj cuando la ingesta está detenida.
   Sin avance suficiente se mantiene pendiente, aunque haya pasado mucho tiempo.
3. Misma estación: buscar capturas en [ideal−30 s, ideal+30 s], estrictamente
   posteriores al origen. Elegir la más cercana; empate por fecha anterior y
   luego ID estable. **Se elige antes de mirar calidad**, igual que Gold U1.
4. Verificar elegibilidad del tipo futuro, catálogo/definición/reglas compatibles,
   y que no haya observaciones no elegibles ni saltos >450 s entre origen y futuro.
   Dos eventos distintos de la misma estación con fecha idéntica son ambiguos;
   no se elige arbitrariamente un valor para evaluar.
5. Guardar el ID/fecha realmente observados, horizonte efectivo, error de
   emparejamiento, motivos, calidad futura y estado terminal. No se salta el
   candidato inválido para elegir otro favorable.

Estados: **pendiente** (no cerrable todavía), **evaluada** (resultado futuro
válido), **vencida** (cierre sin resultado válido: ausencia en banda, objetivo
no elegible, ambigüedad o hueco/inactividad). Un valor ausente no produce error cero.
No se usa la observación de origen como verdad futura. No hay cortes train/test
nuevos en este flujo: son evaluaciones online de los contratos ya congelados.

El cierre es inmutable. Llegadas admitidas antes del cierre pueden cambiar qué
candidato se selecciona; una llegada posterior al cierre no reabre métricas.
Eventos anteriores al watermark pueden haber sido descartados por Spark antes
de SQL. El margen es una política explícita, no garantía de que la API o la red
jamás se retrasen más; recuperaciones fuera de ese margen necesitan backfill y
una versión de evaluación separada. No se borra ni corrige el registro cerrado.

## Métricas y medición de latencia

Ventanas UTC de **origen** de 5 min. Se eligen cinco minutos por el menor horizonte y para resumir varias capturas
del ensayo; no se interpreta la ventana como frecuencia de actualización GBFS.
Error firmado = predicho − observado; sesgo positivo significa sobreestimación. MAE = media del absoluto; RMSE = raíz
de media del cuadrado. Solo entran evaluadas. Se informa n siempre, sin redondear
pronósticos a enteros para calcular errores.

Estratos de cambio: observado futuro ≠ conteo actual / iguales. Discrepancias:
OR de los tres indicadores U1 en origen y objetivo; si ninguno es verdadero y
los seis son conocidos, sin discrepancias; en otro caso, desconocido. Se publican
todos, cada estrato y sus cruces. Esas filas son vistas alternativas de las mismas
evaluaciones: **no sumar el n del total con el de los estratos**.

Cobertura usa todas las predicciones como denominador y cuenta pendientes,
evaluadas y vencidas. El rechazo de entrada tiene denominador separado de
observaciones. Se conservan los motivos de exclusión y la calidad a ambos tiempos.

Se diferencian:

| Medida | Relojes |
|---|---|
| Consulta API | Diferencia entre capturas consecutivas; además intervalo configurado en productor |
| Actualización de fuente | Diferencia entre last_updated de capturas consecutivas |
| Antigüedad | Captura − last_updated del feed y captura − last_reported de estación |
| Predicción disponible en SQL | Confirmación posterior al commit del lote − captura |
| Evaluación completada | Confirmación posterior al commit de ronda − fecha objetivo; también − captura futura real |

Las marcas de confirmación se toman del reloj PostgreSQL **después** del primer
commit: son una cota superior de primera visibilidad SQL, incluyendo el pequeño
viaje de confirmación. Si faltan tras una caída, se completan en recuperación y
reflejan ese retraso, no se inventa una hora anterior. Las marcas se conservan en
reintentos. El retraso de evaluación incluye deliberadamente tolerancia y tardíos;
no confundirlo con retraso de ingesta. Los relojes host/Docker deben estar
sincronizados. La duración total del ensayo no se usa como latencia online.

## Configuración y comandos

Desde la raíz del repositorio. Configuración pública: [postgres.json](config/postgres.json).
La configuración Spark es la existente; `--sql` añade únicamente el conector JDBC
y el montaje del secret al mismo lanzador. Los originales y dimensiones ajenas
siguen montados solo lectura. Predeterminado local[4], configurable local[2].

```bash
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/preparar_postgres.py
docker compose -f pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/compose.kafka.yml up -d kafka-disponibilidad
docker compose -f pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/compose.postgres.yml up -d postgres-disponibilidad
docker inspect --format '{{.State.Health.Status}}' citibike-disponibilidad-kafka citibike-disponibilidad-postgres
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/lanzar_streaming.py --sql --inicializar
```

Con servicios healthy, iniciar etapa 2 en terminal 1 y esperar `streaming_iniciado`:

```bash
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/lanzar_streaming.py
```

Iniciar SQL en terminal 2, esperar `sql_iniciado` y luego productor en terminal 3:

```bash
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/lanzar_streaming.py --sql
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/ejecutar_u2.py producir --capturas 0
```

Prueba acotada equivalente a la realizada, en tres terminales y usando las rutas
principales (si existen, las reanuda). Esperar los mensajes de inicio antes de
arrancar el productor. Para otro ensayo separado, pasar JSON explícitos con
otras rutas y flow_id; la evidencia seleccionada permanece en `verificacion_etapa3`.

```bash
# Terminal 1
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/lanzar_streaming.py --spark-master 'local[2]' --duracion-segundos 2400
# Terminal 2
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/lanzar_streaming.py --sql --spark-master 'local[2]' --duracion-segundos 2300
# Terminal 3
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/ejecutar_u2.py producir --capturas 28 --intervalo 60 --output pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/reports/postgres/produccion_nueva.json
```

El tiempo real depende de red y procesamiento; comprobar que el periodo cubra
al menos 1.530 s, que la primera cohorte de 15 min cierre y que no queden pendientes
cerrables. Tras terminar el productor, drenar/parar como se explica abajo. Los
límites de duración son topes de seguridad de los procesos, no latencias medidas.

Para otro periodo o ensayo, crear configuraciones **explícitas** de streaming y
SQL con otras rutas y flow_id. Usar siempre las dos juntas:

```bash
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/lanzar_streaming.py --sql --config pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/config/streaming-etapa3.json --config-sql pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/config/postgres-verificacion.json --disponibles
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/lanzar_streaming.py --sql --config pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/config/streaming-etapa3.json --config-sql pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/config/postgres-verificacion.json --evaluar
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/lanzar_streaming.py --sql --config pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/config/streaming-etapa3.json --config-sql pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/config/postgres-verificacion.json --verificar-sql --informe reports/postgres/relectura.json
```

`--disponibles` drena archivos nuevos y evaluaciones maduras; `--evaluar` drena
solo estas últimas, en rondas de hasta 4.000, sin nuevas capturas ni imputación. El consumidor continuo
evalúa cada 10 s aunque no lleguen archivos nuevos; la barrera de progreso sigue
vigente. `--duracion-segundos N` acota procesos sin notebook abierto.
La reconciliación `--verificar-sql` requiere un flujo detenido/drenado: comprueba
conteos y cada ID/valor/contrato de predicción contra Parquet, en bloques acotados,
sin recopilar la tabla completa. Con ingesta concurrente los denominadores pueden
avanzar durante la lectura; para seguimiento en vivo usar las vistas SQL.

Consultar métricas del flujo **seleccionado**, sin incluir fixtures:

```bash
docker exec citibike-disponibilidad-postgres psql -U disponibilidad_owner -d disponibilidad_u2 -c "SELECT * FROM disponibilidad.cobertura WHERE flujo='disponibilidad-etapa3-vivo-20261119-v1' ORDER BY ventana_utc,tipo,horizonte_min;"
docker exec citibike-disponibilidad-postgres psql -U disponibilidad_owner -d disponibilidad_u2 -c "SELECT * FROM disponibilidad.metricas WHERE flujo='disponibilidad-etapa3-vivo-20261119-v1' AND periodo='todos' AND discrepancias='todos' ORDER BY ventana_utc,tipo,horizonte_min;"
```

Pruebas sintéticas (flujos fixture-* independientes, **no datos API**):

```bash
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/lanzar_streaming.py --sql --pruebas-sql --config pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/config/streaming-sql-pruebas.json
```

Parada limpia: terminar primero el productor (Ctrl+C o su límite de capturas),
esperar que se confirmen las capturas y drenar. Identificar los contenedores propios:

```bash
docker ps --filter label=com.docker.compose.project=citibike-disponibilidad-streaming --format '{{.Names}}'
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/detener_streaming.py --contenedor NOMBRE_EXACTO_SQL
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/detener_streaming.py --contenedor NOMBRE_EXACTO_SPARK_ETAPA2
docker compose -f pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/compose.postgres.yml stop postgres-disponibilidad
docker compose -f pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/compose.kafka.yml stop kafka-disponibilidad
```

El helper comprueba el proyecto Compose y señala únicamente al Python propio.
`docker stop` directo señala al JVM de spark-submit y puede terminar Python antes
de guardar su informe; el helper permite su finally/cierre limpio. Un corte así
se recupera con recibos y checkpoints, pero no se presenta como cierre correcto.
No usar `down -v` ni eliminar checkpoints. Mantener secret y volumen juntos.

## Límites y continuación

La prueba acotada no demuestra operación sostenida ni precisión generalizable.
Su consulta API de 60 s es distinta de la cadencia histórica U1 cercana a 300 s;
se conserva el emparejamiento por tiempo, pero cambia la densidad de observaciones
y de comprobaciones de continuidad. Sus métricas no sustituyen la prueba histórica.
La evaluación mide conteos reportados y depende de cobertura, elegibilidad,
continuidad y margen tardío. El sesgo de selección por observaciones sin resultado
debe analizarse con la cobertura, no ocultarse descartando predicciones.

Un único driver escribe bloques al SQL de esta máquina: apropiado para esta
etapa acotada, no una prueba de escalabilidad. Para volúmenes mayores habría que
medir backpressure, particiones y staging JDBC con merge transaccional, manteniendo
las mismas claves/contratos. No se implementa esa infraestructura ahora.

Los manifests locales de aplicación quedan bajo `data/sql/ejecuciones/`; el código,
SQL, configuración sin secretos, informes agregados y evidencia pequeña son
versionables. Parquet, JAR, checkpoints, credenciales y `.venv` continúan ignorados.
Grafana, panel de negocio, operación prolongada y sustentación personal siguen
pendientes; no se presenta esta etapa como cumplimiento completo de U2.
