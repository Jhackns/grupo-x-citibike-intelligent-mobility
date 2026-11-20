# Disponibilidad Citi Bike — U2: ingesta, predicciones, evaluación y Grafana

[Guía operativa única y comandos desde la raíz](../../../../OPERACION-DISPONIBILIDAD.md)


Implementación de consulta directa de GBFS, productor ejecutable y consumidor
persistente **Kafka → Spark Structured Streaming → calidad U1 → persistencia
→ PostgreSQL → evaluación diferida**. No depende de Jupyter.
Grafana y Prometheus propios están implementados; la revisión vigente incluye consultas API y capturas de un navegador real, documentadas en `reports/correcciones/`.
No se reentrenó ni modificó U1.

## Correcciones vigentes y operación completa

Referencia explícita: `config/ejecucion_u2_seleccionada.json`. Ensayo principal
`disponibilidad-correcciones-20261119-v1`, 27 capturas y 68.013 observaciones,
cohorte inicial congelada. Las etapas anteriores y la auditoría de `reports/cierre/`
son antecedentes; no se borran ni se eligen carpetas automáticamente.

[Auditoría actualizada](reports/correcciones/auditoria_actualizada.md) ·
[Resultados por cohorte, periodos y cobertura](reports/correcciones/informe.md) ·
[Costos cuantificados](costos.md).

**USD 144/mes es únicamente una estimación documental. No se crearon, contrataron
ni desplegaron recursos cloud de pago. Todos los componentes y pruebas de esta
entrega se ejecutan localmente; el gasto cloud nuevo de esta intervención es cero.**

Cobertura madura con numerador/denominador y fin_prueba/auditoria_actual; errores
solo del subconjunto evaluado. Counters sin NaN, consultas históricas corregidas,
exportador SIGTERM exit0, tres reglas y dos KPIs batch propios Pushgateway.
El dashboard mantiene UID y ahora tiene 17 paneles, con periodos efectivos.
A 10 min la cohorte primaria tiene **0 % de cobertura** y errores NULL: no se amplió ±30s.
El ensayo complementario PromQL es independiente y no valida precisión futura.

### Iniciar todo el flujo propio

Desde la raíz del repositorio. Usar la `.venv` existente; si falta, crearla con
`python3 -m venv .venv` e instalar únicamente el requirements de esta dimensión.
Requiere Docker, la imagen Spark existente, red propia y artefactos locales de la
selección U1 explícita. No necesita credenciales Supabase ni datasets nuevos.
No regenerar secretos si se conserva un volumen SQL inicializado.

```bash
D=pyspark/jupyter/UNIDAD2/Dim-Disponibilidad
.venv/bin/python -m pip install -r "$D/requirements.txt"
.venv/bin/python "$D/preparar_postgres.py"
docker compose -f "$D/compose.kafka.yml" up -d kafka-disponibilidad
docker compose -f "$D/compose.postgres.yml" up -d postgres-disponibilidad
docker inspect --format '{{.State.Health.Status}}' citibike-disponibilidad-kafka
docker inspect --format '{{.State.Health.Status}}' citibike-disponibilidad-postgres
```

Continuar cuando ambos estén **healthy**. En esta máquina ya existen esquema y
tópico. En una instalación nueva, inicializar el esquema con el comando existente
(solo ese paso crea/verifica DDL; no entrena ni consulta API):

```bash
.venv/bin/python "$D/ejecutar_u2.py" topico --crear
.venv/bin/python "$D/lanzar_streaming.py" --sql --inicializar --config "$D/config/streaming-correcciones.json" --config-sql "$D/config/postgres-correcciones.json" --spark-master 'local[2]'
.venv/bin/python "$D/preparar_observabilidad.py"
docker compose -f "$D/compose.observabilidad.yml" up -d
```

Para reanudar ingesta, predicción, SQL y evaluación en primer plano:

```bash
.venv/bin/python "$D/ejecutar_observado.py" --config "$D/config/streaming-correcciones.json" --config-sql "$D/config/postgres-correcciones.json" --intervalo 300 --informe "$D/reports/correcciones/demo_manual.json"
```

Usa `local[4]` por defecto; añadir `--spark-master 'local[2]'` para menos recursos.
Las pruebas realizadas usaron local[2]. El lanzador registra y administra su
colector, productor y dos consumidores; no iniciar otro colector simultáneo.
Para un ensayo acotado añadir `--capturas 3 --intervalo 60`. Un ensayo tan corto
**no basta** para completar etiquetas a 15 min más tolerancia/margen tardío.
Se reutilizan explícitamente las salidas y checkpoints del flujo de correcciones; para otro flujo
independiente hay que indicar config/config-sql con flow_id y rutas nuevos.
No borrar checkpoints ni cambiar rutas para saltar una incompatibilidad/retención.

### Tests locales de Disponibilidad

Desde la raíz del repositorio o cualquier subdirectorio del checkout, ejecutar en Bash con la `.venv` del proyecto. Cada suite usa un proceso Python independiente.

```bash
(
  set -e
  repo="$(git rev-parse --show-toplevel)"
  for unidad in UNIDAD1 UNIDAD2; do
    dimension="$repo/pyspark/jupyter/$unidad/Dim-Disponibilidad"
    echo "Tests locales de $unidad"
    cd "$dimension"
    PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$dimension:$dimension/src" \
      "$repo/.venv/bin/python" -m unittest discover -s "$dimension/tests" -v
  done
)
```

En la `.venv` local del descargador, las pruebas que requieren PySpark o SQL en Docker figuran como omitidas.

### Verificar

En otra terminal, definir también `D` si no existe en esa shell:

```bash
D=pyspark/jupyter/UNIDAD2/Dim-Disponibilidad
curl --fail http://127.0.0.1:13000/api/health
curl --fail http://127.0.0.1:19090/api/v1/targets
.venv/bin/python "$D/verificar_correcciones.py"
```

El verificador requiere servicios/colector disponibles, conserva las referencias
SQL históricas y **actualiza** `reports/correcciones/verificacion.json`; guardar
una copia antes si se necesita preservar otra instantánea. Comprueba las correcciones y la integración; su éxito no acredita la sustentación
ni los requisitos físicos/empresariales pendientes. Releer salidas paradas no es
verificar llegada nueva de eventos.

Para recalcular cobertura madura y percentiles **sin producir eventos ni cerrar
evaluaciones**, con PostgreSQL propio activo:

```bash
docker exec -i citibike-disponibilidad-postgres psql -XqAt -v ON_ERROR_STOP=1 -U disponibilidad_owner -d disponibilidad_u2 < "$D/reports/cierre/consulta_resultados.sql"
docker exec -i citibike-disponibilidad-postgres psql -XqAt -v ON_ERROR_STOP=1 -U disponibilidad_owner -d disponibilidad_u2 < "$D/reports/cierre/consulta_latencias.sql"
```

Dashboard: `http://127.0.0.1:13000/d/citibike-disponibilidad-u2`.
Usuario `disponibilidad_admin`, clave solo en el archivo local
`data/secretos/grafana_admin_password`. No copiarla a comandos, informes o Git.
La [revisión visual vigente y guía personal](reports/correcciones/revision_visual.md) da URLs con
periodos fijos y pasos visuales. Para inspeccionar sin producir datos, omitir
`ejecutar_observado.py` y ejecutar solo `observar.py` en primer plano, cerrándolo
con Ctrl+C después. La prueba principal vigente tiene evaluaciones a 5 y 15 minutos; su cohorte inicial
no tiene objetivo válido a 10 minutos. El ensayo corto PromQL usa otro flujo y no
aporta evidencia de precisión. Seleccionar el flujo y periodo de cada informe.

### Detener conservando datos

Ctrl+C en `ejecutar_observado.py` y esperar su informe de cierre; administra sus
procesos propios. Si se inició `observar.py` aparte, cerrarlo también con Ctrl+C.
Si un lanzador desapareció sin cerrar sus contenedores, identificarlos con
`docker ps --filter label=com.docker.compose.project=citibike-disponibilidad-streaming --format '{{.Names}}'`
y usar `detener_streaming.py --contenedor NOMBRE_EXACTO_PROPIO` para cada uno;
no aplicar kill/stop a servicios del equipo.

```bash
docker compose -f "$D/compose.observabilidad.yml" stop
docker compose -f "$D/compose.kafka.yml" stop kafka-disponibilidad
docker compose -f "$D/compose.postgres.yml" stop postgres-disponibilidad
```

No usar `down -v`: conservar los cinco volúmenes propios (Kafka, PostgreSQL, Grafana, Prometheus y Pushgateway), Parquet y checkpoints.
El estado final de **este cierre** está en
[servicios_finales.json](reports/correcciones/servicios_finales.json); los informes de
etapas anteriores mantienen su fecha y contexto. No se hicieron commits ni push.

## Antecedentes de las etapas 1 a 4

Evidencia histórica de etapa 1: dos capturas reales, **5.038 eventos confirmados y consumidos**,
21 pruebas sintéticas OK y cadencia observada de 61,96 s en el ensayo de 60 s.
Los IDs y resultados están en [el informe ejecutado](reports/verificacion.md).
La verificación de etapa 2, sus comandos, salidas, checkpoints y estado
final de los procesos se documentan en [streaming.md](streaming.md) y
[reports/streaming/verificacion.md](reports/streaming/verificacion.md).
Los informes de etapa 1 se conservan como antecedentes.

La evidencia histórica de **PostgreSQL y evaluación diferida** está documentada en
[postgres.md](postgres.md), con [resultados y estado final](reports/postgres/verificacion.md).
Reutiliza el lanzador mediante `--sql` y la salida durable de etapa 2. La prueba
selecciona explícitamente `config/streaming-etapa3.json` y
`config/postgres-verificacion.json`, flujo `disponibilidad-etapa3-vivo-20261119-v1`.
No se mezclan las salidas anteriores ni se seleccionan carpetas automáticamente.

La evidencia histórica de **Grafana y observabilidad, etapa 4**, está en: [comandos y configuración](observabilidad.md),
[verificación real](reports/observabilidad/verificacion.md),
[auditoría de las 19.572 vencidas](reports/observabilidad/vencidas.md) y
[costos con supuestos explícitos](costos.md). Dashboard versionado:
`observabilidad/dashboards/disponibilidad.json`, UID `citibike-disponibilidad-u2`.
Ese ensayo histórico usa explícitamente `config/streaming-etapa4.json` y
`config/postgres-etapa4.json`, flujo `disponibilidad-etapa4-vivo-20261119-v1`.
No reemplaza la evidencia de etapa 3. Los procesos propios de prueba están detenidos
al cierre; se conservan volúmenes y checkpoints.

## Arquitectura y relación con U1

```text
Histórico U1, conservado:
API → capturador externo → Supabase Storage → descarga incremental
    → PySpark batch → Silver → Gold → evaluación y artefactos

Implementado (etapas 1–4):
API GBFS → descubrimiento y caché de catálogos → productor local
         → Kafka propio → Spark Structured Streaming → calidad U1
         → observaciones / rechazados / predicciones / métricas en Parquet
         → consumidor SQL con checkpoint → PostgreSQL propio
         → observaciones futuras → evaluación diferida / métricas / cobertura

PostgreSQL → vistas de solo lectura → Grafana
Productor / Kafka / Spark → métricas propias → Prometheus → Grafana
```

Supabase sigue siendo el archivo histórico; el productor no lo consulta ni
necesita sus credenciales. Esta entrega mide estados operativos reportados, no
viajes, demanda inferida ni verdad física de las cantidades.

La selección explícita de U1 se lee desde
`../../UNIDAD1/Dim-Disponibilidad/config/ejecucion_seleccionada.json`.
El ID es **20261117T171917344468Z**. Sus seis ganadores son persistencia a
5/10/15 minutos para convencionales/eléctricas. No necesitan fit. Los originales
están en `../../UNIDAD1/Dim-Disponibilidad/data/ejecuciones/20261117T171917344468Z/modelos/seleccionados/<tipo>_<horizonte>min/contrato.json`.
Los mejores candidatos MLlib son referencias separadas de esa misma ejecución,
en `modelos/referencias_mllib/`; no sustituyen a los ganadores. El preflight lee la
referencia versionada y comprueba los hashes de los contratos originales cuando
están presentes. No busca carpetas recientes ni carga pesos para inferir.

## Fuente comprobada

Se partió de [Citi Bike System Data](https://citibikenyc.com/system-data), cuyo
enlace de datos en tiempo real lleva al
[índice GBFS 2.3](https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json).
El índice inicial es configurable con `gbfs_index_url`, `DISP_GBFS_INDEX_URL` o
`--index-url`. Las URL de feeds se descubren del índice, no se construyen por nombre.

| Feed anunciado | URL descubierta y consultada |
|---|---|
| station_status | https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_status.json |
| station_information | https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json |
| vehicle_types | https://gbfs.lyft.com/gbfs/2.3/bkn/en/vehicle_types.json |

Las respuestas verificadas declaran **2.3**, `last_updated` Unix entero, `ttl=60`
y `data.stations` o `data.vehicle_types`. El catálogo observado contiene:

| vehicle_type_id | form_factor | propulsion_type |
|---|---|---|
| 1 | bicycle | human |
| 2 | bicycle | electric_assist |

Es una comprobación del catálogo consultado, no una equivalencia permanente de
IDs. Cada versión de catálogo vuelve a comprobarse; los eventos conservan su
referencia y semántica. Una versión GBFS distinta de 2.3 se rechaza hasta revisar
el esquema. Un catálogo sin IDs únicos o sin semántica explícita bloquea la captura.

Se conserva el objeto completo de estación en `station`, incluidas extensiones
desconocidas, `vehicle_types_available`, total, extensión eléctrica, disabled,
docks, estados operativos y `last_reported`. No se resta total menos eléctricas
ni se sustituye el array por `num_ebikes_available`. Los campos ausentes siguen
ausentes en el original; no se rellenan con ceros. El productor solo anota
problemas de ingesta; el consumidor streaming aplica las reglas originales de U1
y comprueba por separado el catálogo actual antes de declarar elegibilidad por objetivo.

## Frecuencias

| Consulta | Configuración normal |
|---|---:|
| station_status | 300 s |
| station_information | 3.600 s |
| vehicle_types | 21.600 s |
| Índice GBFS | 21.600 s |

Los 300 s se eligen por la cadencia observada de U1 y su escala de variables y
horizontes, no porque la API tarde obligatoriamente cinco minutos en actualizar.
La fuente anuncia TTL de 60 s. Se usa `max(intervalo_configurado, ttl)` y la espera
se mide desde la recepción del último status; el tiempo de obtener catálogos no
acorta la separación. Latencia de red y procesamiento pueden alargar el intervalo.

La prueba real usa **60 s explícitos**, diferenciados del valor normal. Su
cadencia realmente observada está en el informe de verificación. Dos capturas
no demuestran una frecuencia universal ni un SLA de actualización.

Los catálogos tienen frecuencias propias por su menor variación observada en el
histórico. Es una política de caché configurable, más larga que el TTL anunciado;
no garantiza que sus metadatos sigan siendo actuales durante toda la caché. Se
conservan fecha de consulta, `last_updated` y hash. Al expirar, un refresco fallido
bloquea la captura; no se usa silenciosamente el snapshot expirado. Los catálogos
se consultan completos y se guardan por hash bajo `data/catalogos/`, nunca una vez
por estación. Nuevas estaciones sin correspondencia se marcan, sin un join que
duplique o elimine estados.

## Contrato e identidad

[JSON Schema v1](contratos/evento.v1.schema.json),
[constructor y validador](src/disponibilidad_u2/contrato.py).
Un evento por estación/captura, con clave Kafka **station_id UTF-8**.

| Campo | Significado |
|---|---|
| schema_version | `citibike.disponibilidad.v1` |
| capture_id | UUID generado una sola vez para la captura |
| event_id | UUID5 determinista de capture_id y station_id |
| captured_at_utc | Momento de recepción, UTC con Z |
| last_updated_original / last_updated_utc | Reloj original del feed y representación UTC |
| last_reported_original / last_reported_utc | Reloj de estación; normalizado nullable si falta o es inválido |
| source | Sistema, índice inicial, URL de status y versión GBFS |
| station | Objeto original completo, sin limpiar ni reconstruir cantidades |
| catalogs | Versión/hash, fechas, URL y archivo de los dos catálogos |
| vehicle_types_catalog | Semántica explícita de los tipos, transportada con el evento |
| u1_catalog_compatible | Coincidencia de las semánticas históricas de los IDs 1/2; no es garantía de calidad física |
| ingestion_issues | Ausencias, tipos desconocidos o inconsistencias estructurales del estado |

El validador ejecutable comprueba identidad, campos del sobre, correspondencia
de relojes y referencias. El JSON Schema documenta además la forma para otros
consumidores; no se añadió una dependencia externa de JSON Schema.

Cambios dentro del objeto de estación son conservados; una estructura de feed
inválida, IDs ausentes/repetidos o una captura vacía se rechazan antes de enviar.
Un estado operativo inactivo o con cantidades discrepantes no se transforma en
cero ni se elimina en ingesta. Estados iguales en momentos distintos producen
eventos distintos, aunque `last_reported` no cambie.

Antes de enviar se guarda un outbox atómico en `data/estado/pendiente.json`.
Los reintentos reutilizan el mismo contenido e IDs; un reinicio recupera ese
outbox y solo intenta los eventos no confirmados. Se impide otro productor
simultáneo mediante lock local. Una captura incompleta termina con error y deja
el pendiente recuperable. El archivo se retira solo después de guardar el resumen
completo; eso no borra capturas históricas ni archivos remotos.

## Kafka y semántica de entrega

| Parámetro | Valor |
|---|---|
| Tópico propio/configurable | `citibike-disponibilidad-estaciones-v1` |
| Particiones / réplicas | 3 / 1 |
| Retención temporal configurada | 86.400.000 ms, 24 h |
| Retención por tamaño | 1.073.741.824 bytes por partición; puede reducir el tiempo conservado |
| Cleanup / min ISR | delete / 1 |
| Confirmación | acks=all, idempotencia del cliente y max in-flight=1 |
| Reintentos | HTTP 3 intentos; cliente Kafka 3 reintentos; aplicación 3 intentos |

`topico --crear` solo crea si falta; si existe comprueba particiones, réplicas y
propiedades. Ante diferencias termina con error: no altera tópicos existentes.
El productor tiene deshabilitada la creación automática de tópicos.
La purga opera por segmentos y comprobaciones periódicas; 24 h no representa
un vencimiento instantáneo garantizado para cada mensaje.

Los futures de Kafka deben responder para contar `eventos_confirmados`; llamar
send no equivale a confirmar. Se informa `unconfirmed_events`, pues un timeout
puede ser una entrega ambigua. HTTP y reenvíos de aplicación tienen espera
exponencial; los reintentos internos del cliente usan backoff de 500 ms y límite
de entrega de 30 s. Timeouts y números de intentos son configurables.
`consultas` cuenta intentos HTTP; `capturas`, entregas completas reconocidas;
`eventos_confirmados`, event_id distintos con ACK. Los fallos/reintentos cuentan
intentos, mientras `unconfirmed_events` cuenta los eventos finales sin confirmar.

La idempotencia del cliente no elimina reenvíos de aplicación ni hace
exactly-once de extremo a extremo. Si se pierde un ACK o hay un crash entre ACK y
checkpoint del outbox, pueden repetirse mensajes con el mismo event_id. Si una
captura ya completada se vuelve a consultar tras reiniciar, recibe otro capture_id
porque es otra observación. El consumidor streaming trata duplicados por
event_id dentro del watermark, sin deduplicar por cantidad ni station_id/last_reported. La prueba
acotada de consumo observa duplicados durante su ventana, no demuestra ausencia
de duplicados posteriores. Una sola réplica no tolera perder el disco/broker.

## Infraestructura revisada

Rama de trabajo: `u2-duracion-miguel`. No se encontró AGENTS.md en el repositorio
ni en sus directorios ascendentes revisados. `pyspark/Dockerfile` conserva Python
3.10, Java 21, PySpark 4.2.0 y Jupyter; `pyspark/compose.yml` conserva
`lambda26-pyspark`, montaje `./jupyter:/opt`, red `lambda26-pyspark_default` y
puertos host 4487→8888 y 4043→4040. No se modificaron.

Existía un Kafka detenido de `/home/miguel/citbike-bigdata`, red
`citibike-u2_default`, con listener anunciado `kafka:9092` y sin puerto al host.
Se dejó intacto: no es infraestructura de este checkout y su conectividad no
permite el productor local sin tocar ese proyecto. Se reutilizó la imagen
`apache/kafka:3.9.1` ya disponible para un broker aislado mediante
[compose.kafka.yml](compose.kafka.yml).

| Contexto | Dirección |
|---|---|
| Productor local en .venv | 127.0.0.1:19092 |
| Cliente en red propia Docker | kafka-disponibilidad:9092 |
| Red propia | citibike-disponibilidad_default |
| Contenedor / volumen propios | citibike-disponibilidad-kafka / citibike-disponibilidad_kafka_data |

Listeners INTERNAL y HOST son distintos y anuncian direcciones alcanzables desde
su contexto; CONTROLLER no se publica. PLAINTEXT se usa solo en este entorno
local: el puerto host se limita a loopback. `config/docker.json` sirve para clientes
que ya estén en esa red. El contenedor PySpark existente conserva su red y montaje.
El proceso de etapa 2 reutiliza su imagen en un contenedor propio conectado a la
red Kafka mediante [compose.streaming.yml](compose.streaming.yml), sin reconstruir
la imagen ni modificar los servicios del equipo. Spark 4.2.0 / Scala 2.13.18 y el
conector Kafka 4.2.0 están comprobados; véase [dependencias y ejecución](streaming.md).

Un servicio transitorio `preparar-volumen` da al usuario Kafka permisos sobre el
volumen nuevo propio. El broker usa heap 256–512 MiB y límite 1 GiB/1 CPU; no se
aumenta memoria de PySpark ni de otros servicios. El productor corre en la .venv
local, no dentro de la imagen PySpark ni como notebook.

## Comandos desde la raíz del repositorio

Si no existe `.venv`, crearla con `python3 -m venv .venv`. Reutilizarla e instalar
solo la dependencia fijada, sin paquetes globales:

```bash
.venv/bin/python -m pip install -r pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/requirements.txt
docker compose -f pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/compose.kafka.yml up -d kafka-disponibilidad
docker inspect --format '{{.State.Health.Status}}' citibike-disponibilidad-kafka
```

Con el broker healthy, crear/verificar el tópico y descubrir la fuente:

```bash
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/ejecutar_u2.py topico --crear --output pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/reports/topico.json
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/ejecutar_u2.py descubrir --output pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/reports/fuente_gbfs.json
```

Captura individual y consumidor posterior: usar el capture_id impreso para
seleccionar el resumen exacto; no escoger el más reciente por carpeta.

```bash
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/ejecutar_u2.py producir --capturas 1
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/ejecutar_u2.py verificar --resumen pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/data/resumenes/REEMPLAZAR_CAPTURE_ID.json
```

Prueba integrada acotada: abre antes el grupo propio, fija sus offsets al final,
publica y consume cada captura y comprueba continuidad. Termina automáticamente:

```bash
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/ejecutar_u2.py prueba-real --capturas 2 --intervalo 60 --output pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/reports/verificacion_real.json
```

Productor continuo, explícitamente ilimitado, en primer plano:

```bash
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/ejecutar_u2.py producir --capturas 0
```

Ctrl+C o SIGTERM solicita apagado: se dejan de crear capturas, se cierran cliente
y consumidor y se preserva el outbox si falta confirmar algo. Los timeouts
acotan el cierre. Para detener solo el broker propio, conservando sus mensajes:

```bash
docker compose -f pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/compose.kafka.yml stop kafka-disponibilidad
```

Opciones públicas: `--config`, `--index-url`, `--bootstrap-servers`, `--topic`,
`--intervalo`. Precedencia CLI → variables DISP_* → JSON → predeterminados.
`.env.example` muestra variables públicas opcionales; el programa no carga dotenv
automáticamente. Los logs muestran capture_id, confirmados, fallos, reintentos
y tipos de error, sin volcar entorno, cabeceras ni mensajes de excepciones externas.

## Pruebas y evidencias

```bash
.venv/bin/python -m unittest discover -s pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/tests -v
docker exec citibike-disponibilidad-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-disponibilidad:9092 --describe --topic citibike-disponibilidad-estaciones-v1
```

Las 21 pruebas sintéticas originales cubren formatos, relojes, ausencias/ceros, catálogos,
IDs, reintentos, outbox, estados iguales, caché diferenciada, lock, frecuencia,
tópico existente y resolución de offsets antes de publicar. No son datos reales. La etapa 4 añade seis pruebas de observabilidad; la suite
local actual tiene 27 OK y 17 pruebas Spark/SQL omitidas fuera de Docker,
registradas en [pruebas.json](reports/observabilidad/pruebas.json).

La prueba real está en [verificacion_real.json](reports/verificacion_real.json),
con grupo, capture_id, fuente, estaciones esperadas, ACKs, consumo único, claves,
particiones y continuidad. [Ejemplo real de evento](contratos/ejemplo_evento_real.json).
[Informe legible y estado al terminar](reports/verificacion.md).
Los manifiestos de intentos fallidos y de ejecuciones completas están en
`data/ejecuciones/`, con configuración, versiones, hashes de código, contadores
y errores. Los snapshots/outbox/resúmenes locales quedan bajo `data/`, ignorado
por Git. El esquema, código, configuración pública, informes pequeños y ejemplo
real son versionables; no se hace commit ni push.

## Estado académico tras las correcciones

Ver la [tabla requisito/evidencia/estado/pendiente actualizada](reports/correcciones/auditoria_actualizada.md).
Quedan la adaptación empresarial/IoT (no hay sensor propio ni transacciones),
coordinación del brief/tablero común y las evidencias personales. No se declara
cumplimiento total ni se publica documentación por cuenta propia.

### Documentación local publicable

```bash
D=pyspark/jupyter/UNIDAD2/Dim-Disponibilidad
.venv/bin/python -m pip install -r "$D/requirements-docs.txt"
.venv/bin/python "$D/preparar_documentacion.py"
.venv/bin/python -m mkdocs build --strict -f "$D/mkdocs-u2.yml"
```

Fuentes: `documentacion/`; artefacto ignorado: `data/publicacion/site`.
Para revisar localmente: `.venv/bin/python -m mkdocs serve -f "$D/mkdocs-u2.yml" -a 127.0.0.1:18080`
y abrir http://127.0.0.1:18080 ; cerrar con Ctrl+C. No ejecutar gh-deploy.
No se modificó `mkdocs.yml` ni dimensiones de compañeros.

### Verificación de las correcciones

Con PostgreSQL/Grafana/Prometheus/exportadores activos y un solo colector:

```bash
.venv/bin/python "$D/verificar_correcciones.py"
.venv/bin/python "$D/resumir_correcciones.py"
.venv/bin/python -m unittest discover -s "$D/tests" -p test_observabilidad.py -v
.venv/bin/python -m unittest discover -s "$D/tests" -p test_cohorte.py -v
```

Los dos primeros leen datos; actualizan sus informes. La medición física se congeló
antes del ensayo PromQL complementario para no atribuirlo al ensayo principal.
Para volver a publicar **solo localmente** los KPIs batch de la captura explícita:

```bash
.venv/bin/python "$D/publicar_kpis_batch.py" --flujo disponibilidad-correcciones-20261119-v1 --capture-id 8ff5bace-0ecf-499d-aa8a-bf43d57a3a0a
```

El panel batch responde solo a tipo, declara fecha/n y no es inventario actual.
No se usa Pushgateway para el productor continuo. Para repetir el replay S8
con los tres eventos reales ya guardados, sin API ni Kafka:

```bash
.venv/bin/python "$D/lanzar_streaming.py" --replay-real --muestra-real "$D/data/verificacion/replay_real.json" --informe reports/correcciones/replay_s8.json --spark-master 'local[2]'
```

No hace falta repetirlo para leer la evidencia:10s/30s dieron batchId4/1 a45s,
tres eventos únicos y un tardío descartado en cada modo. Es replay controlado,
no retraso espontáneo del proveedor ni prueba de carga. La ejecución continua
principal sí consumió API/Kafka reales durante más de25min.

No se entrenó, cambió la tolerancia ni alteró U1. Sin commits ni push.
