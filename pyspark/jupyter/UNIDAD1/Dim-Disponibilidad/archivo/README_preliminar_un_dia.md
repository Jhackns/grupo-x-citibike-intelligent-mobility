> Documento histórico de la prueba de un día. La guía vigente es [README.md](../README.md). El código exacto anterior está en `data/archivo_preliminar/20261117T142303450900Z/`; sus comandos y resultados no describen el flujo ampliado actual.

# Disponibilidad de bicicletas por estación — Unidad 1

**Responsable:** Jose Miguel Condo Huamani. **Estado:** fases 1–6: descarga, inspección, Silver, Gold, comparación/evaluación
experimental e integración reproducible de U1 en PySpark. Se conserva `../Dim-Duracion-de-viajes/` como trabajo anterior.
Las dimensiones de los demás integrantes permanecen independientes.

**Revisión con la rúbrica oficial:** [matriz final U1](../reports/cumplimiento_u1/revision_final.md).
Incluye pendientes técnicos/personales y referencias MLlib entrenadas guardadas
por separado; persistencia sigue ganadora. No se declara cumplimiento total ni
se asigna nota sin evidencia de sustentación y capturas personales.

## Ejecución integrada de U1

Desde la raíz del repositorio, con el contenedor existente `lambda26-pyspark` iniciado:

```bash
.venv/bin/python pyspark/jupyter/UNIDAD1/Dim-Disponibilidad/ejecutar_u1.py
```

El lanzador usa biblioteca estándar en la `.venv` local; no instala paquetes.
PySpark, MLlib y las verificaciones se ejecutan **dentro de Docker**. Por defecto
solo utiliza los 290 gzip locales del 10 de noviembre, no lee `.env.u2` ni necesita
credenciales. No inicia ni modifica servicios. El contenedor conserva el montaje
`./jupyter:/opt`; no se han cambiado Dockerfile ni Compose para esta integración.

La configuración pública está en [config/u1-local.json](../config/u1-local.json).
Los cortes existentes se mantienen: validación a las 16:00 y prueba a las 20:00 UTC.

```bash
# Ejecución completa nueva, sin sobrescribir ninguna anterior:
.venv/bin/python pyspark/jupyter/UNIDAD1/Dim-Disponibilidad/ejecutar_u1.py --nueva

# Configuración explícita y periodo inclusivo en días UTC:
.venv/bin/python pyspark/jupyter/UNIDAD1/Dim-Disponibilidad/ejecutar_u1.py --config pyspark/jupyter/UNIDAD1/Dim-Disponibilidad/config/u1-local.json --desde 2026-11-10 --hasta 2026-11-10
```

El comando sin `--nueva` reutiliza una ejecución integrada completa **solo si**
coinciden SHA-256 de entradas y código, configuración, imagen Docker y versiones.
Verifica todas las salidas y vuelve a cargar las selecciones para reproducir sus
predicciones de prueba. Un resultado parcial o fallido nunca se reutiliza. Si una
salida compatible está alterada, finaliza con error; `--nueva` permite recalcular
en otro directorio sin tocarla. No se mezclan etapas de ejecuciones diferentes.

Resultados en `data/ejecuciones/<id>/`: `silver/`, `gold/`, `modelos/`, configuración,
log y `manifiesto.json` con linaje, hashes, versiones, etapas, conteos, métricas,
duración, recarga y errores. Cada reutilización también tiene manifiesto propio
y referencia a su ejecución fuente. La salida del proceso es distinta de cero si
falla una verificación. No se borran raw ni ejecuciones anteriores.

Contrato, resultados, comandos de pruebas, ampliación futura y contraste documental:
[entrega reproducible de U1](../reports/integracion/entrega_u1.md).
El [notebook](../notebooks/01_disponibilidad_batch.ipynb) conserva las salidas de las
fases anteriores y añade una sección independiente de integración.

## Pregunta y alcance

¿Con qué precisión podemos predecir los conteos reportados por tipo de bicicletas
convencionales y eléctricas en cada estación de Citi Bike a partir de su historial
de disponibilidad reportada?

La exactitud física de esos conteos no está confirmada. Cumplir las reglas de
elegibilidad no equivale a garantizar que reflejen la cantidad física real.

Se comparan horizontes de **5, 10 y 15 minutos**, sin elegir uno definitivo.
Persistencia fue seleccionada por MAE de validación en las seis combinaciones;
los candidatos LR/RF sí se entrenaron y evaluaron, pero no ganaron ese criterio.
Persistencia no necesita entrenamiento. La unidad de observación es estación e
instante; los objetivos son conteos reportados futuros por tipo. Los datos de
entrenamiento son capturas GBFS históricas, **no los CSV mensuales de viajes**.

Relación con [el brief del equipo](../../../../../docs/proyecto-sello/brief.md) y
[el producto U1](../../../../../docs/proyecto-sello/u1-producto.md):

- La disponibilidad complementa la planificación y el comportamiento operativo,
  pero mide inventario disponible, mientras que las dimensiones existentes se
  centran en viajes y demanda. No debe llamarse demanda al número de bicicletas
  disponibles: también influyen devoluciones, redistribución y mantenimiento.
- El brief asigna duración a este integrante y supone un histórico común de CSV
  y eventos de viajes. Esta dimensión introduce otra fuente y otro contrato de
  datos; requerirá acordar una ampliación del planteamiento del equipo.
- La dimensión de estaciones existente clasifica utilización histórica. Esta
  dimensión pronosticará existencias por tipo, un objetivo distinto aunque
  comparta `station_id`. La compatibilidad temporal de IDs deberá comprobarse.
- Las métricas y resultados de duración en el producto U1 son resultados previos;
  no constituyen resultados de disponibilidad. No se han reescrito esos documentos.

## Arquitectura prevista y estado

```text
Histórico / U1:
API GBFS → capturador externo → Supabase Storage (citibike-raw)
         → descarga incremental local [OPCIONAL; IMPLEMENTADA]
         → PySpark batch → calidad/Silver → preparación/Gold en Parquet
         → comparación MLlib/persistencia → selección por validación
         → evaluación 5/10/15 min → contratos/pipelines y recarga [IMPLEMENTADO]

Futuro / U2 [NO IMPLEMENTADO PARA ESTA DIMENSIÓN]:
API GBFS → productor → Kafka → Spark Structured Streaming
         → variables compatibles con U1 → modelo guardado
         → PostgreSQL → Grafana
```

Supabase conserva los originales históricos. La futura inferencia recibirá la API
por Kafka; no sustituirá ese flujo por lectura continua de Storage. El capturador
que produjo el histórico no está implementado en esta carpeta ni se ha localizado
su código en la rama. Se ha comprobado el resultado almacenado, no su funcionamiento.

La fase 1 conserva los originales. La fase 2 incorpora el notebook de lectura y
diagnóstico, sin aplicar limpieza, deduplicación, imputación o entrenamiento.
La lectura de gzip no transforma ni reserializa las capturas.

## Entorno local y contenedor existente

| Ejecución | Entorno | Responsabilidad |
|---|---|---|
| Descargador y pruebas | Python local en `.venv/` de la raíz del repositorio | REST de Storage, archivos originales y manifiestos |
| Jupyter y procesamiento U1 | Servicio `pyspark` existente | Lectura, Silver, Gold, MLlib, evaluación y verificación |
| Capturador histórico | Externo; código no localizado en esta rama | API hacia Storage |

El [Dockerfile](../../../../Dockerfile) parte de Python 3.10, instala Java 21,
PySpark 4.2.0, Notebook 7.6.2 y pandas 2.3.3, usa `/opt` y arranca Jupyter Notebook.
El [Compose](../../../../compose.yml) define el contenedor `lambda26-pyspark` y publica
Jupyter en el puerto local 4487 y Spark UI en 4043 (puerto interno 4040).
No define un clúster Spark con workers: PySpark se utiliza dentro de ese entorno.

El volumen existente `./jupyter:/opt`, relativo a `pyspark/compose.yml`, ya expone:

```text
Host:       pyspark/jupyter/UNIDAD1/Dim-Disponibilidad/data/raw/
Contenedor: /opt/UNIDAD1/Dim-Disponibilidad/data/raw/
```

Los informes también quedan bajo `/opt/UNIDAD1/Dim-Disponibilidad/reports/descargas/`.
No hace falta otro volumen ni cambiar servicios. `.venv/` y `pyspark/.env.u2`
permanecen fuera del montaje `jupyter`: no se necesitan credenciales Supabase en
el contenedor para procesar archivos ya descargados.

La descarga no requiere Docker ni instalar Spark localmente. Para las siguientes
fases se reutilizará `docker compose -f pyspark/compose.yml up -d pyspark`, ejecutado
desde la raíz, cuando corresponda iniciar el entorno. No se han arrancado
contenedores durante esta fase ni modificado Dockerfile/Compose.

## Acceso y estructura comprobados

Inspección de solo lectura del 16 de noviembre de 2026:

- Se encontró `pyspark/.env.u2`, ignorado por Git, con las variables de conexión.
- La clave `NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY` obtuvo HTTP 200 pero una lista
  vacía. Esto no demuestra que el bucket esté vacío ni acredita permiso de descarga.
- Con `SUPABASE_SECRET_KEY`, **ya existente en el mismo archivo**, se pudo listar
  y descargar una muestra de cada una de las tres fuentes de disponibilidad.
  No se solicitaron nuevas claves ni se cambiaron políticas, permisos u objetos.
- Se observaron 2.866 objetos en 12 fuentes, con carpetas del **2026-11-09 al
  2026-11-16 UTC**. Es un inventario de ese momento; el último día estaba en curso.

| Fuente | Objetos observados | Uso previsto |
|---|---:|---|
| `station_status` | 2.089 | Serie temporal de disponibilidad |
| `station_information` | 8 | Identidad, ubicación y capacidad de estaciones |
| `vehicle_types` | 8 | Correspondencia entre tipo y propulsión |
| `system_alerts` | 697 | Contexto opcional, fuera de la selección predeterminada |
| `free_bike_status`, `gbfs`, `gbfs_versions`, `system_calendar`, `system_hours`, `system_information`, `system_pricing_plans`, `system_regions` | 8 por fuente | Otras fuentes disponibles |

Organización observada, preservada localmente sin aplanar carpetas:

```text
citibike-raw/
  station_status/
    2026-11-09/
      2026-11-09T16-35-08-575Z-c11a7c98-3ee1-4299-8960-fcc6171e7705.json.gz
  station_information/<fecha>/<timestamp>-<uuid>.json.gz
  vehicle_types/<fecha>/<timestamp>-<uuid>.json.gz
```

La muestra `station_status` es JSON GBFS 2.3 con claves raíz `data`, `last_updated`,
`ttl` y `version`; `data.stations` contiene 2.520 estaciones. Se observaron
`station_id`, `num_bikes_available`, `num_ebikes_available`,
`vehicle_types_available`, `num_bikes_disabled`, `num_docks_available`,
`num_docks_disabled`, `is_installed`, `is_renting`, `is_returning` y `last_reported`.
`station_information` contiene `data.stations` con capacidad, nombre y coordenadas.
`vehicle_types` contiene `data.vehicle_types` con `vehicle_type_id`, `form_factor`
y `propulsion_type`.

En el catálogo descargado del 10 de noviembre se observó `vehicle_type_id=1`
con propulsión `human` y `vehicle_type_id=2` con `electric_assist`; la preparación
futura deberá resolver esa correspondencia desde el catálogo, sin fijarla como
regla universal. Una estación de la muestra tenía `last_reported=86400`, que no
corresponde al período capturado si se interpreta como Unix: debe revisarse en la
fase de calidad. Se conserva el dato original sin corregirlo aquí.

No existe `captured_at` en las muestras. Se distinguen tres tiempos:

1. **Timestamp UTC del nombre:** el descargador lo interpreta como instante de
   archivo/captura según la convención observada. La `Z` indica UTC. No se ha
   verificado el reloj ni la lógica del capturador externo.
2. **`last_updated` del JSON:** tiempo Unix de actualización del feed.
3. **`last_reported` de cada estación:** tiempo Unix reportado para esa estación.

No son intercambiables; `created_at`/`updated_at` de Storage son metadatos de
almacenamiento y tampoco sustituyen al instante de observación.

En preparación se deberá validar el significado de cada tipo de vehículo,
consistencia de conteos, estados operativos, frecuencia, huecos y antigüedad de
reportes. No se calcula todavía `convencionales = total - eléctricas` ni se
presupone que toda estación tenga la misma composición.

## Preparación local

Desde la raíz del repositorio, en Linux/WSL (se usa `fcntl` para el bloqueo local):

```bash
python3 -m venv .venv
.venv/bin/python --version
```

No ejecutar `pip install`: se usa exclusivamente biblioteca estándar, compatible
con Python 3.10 o superior. `.venv/` ya está excluido de Git. También puedes activar
el entorno con `source .venv/bin/activate`; los comandos siguientes usan su ruta
explícita para evitar el Python global.

El ejemplo sin secretos está en [`.env.example`](../.env.example). Variables:

- `NEXT_PUBLIC_SUPABASE_URL`: origen HTTPS del proyecto.
- `NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY`: acceso publicable, opción predeterminada.
- `SUPABASE_BUCKET`: por defecto `citibike-raw`.
- `SUPABASE_SECRET_KEY`: solo se usa al indicar `--credencial secreta`.

`--env-file` admite cualquier ruta local, incluso fuera del repositorio. Si se
omite, se busca el primer archivo existente en este orden: `.env` de la dimensión,
`pyspark/.env.u2`, `.env.u2` de la raíz. No se exploran directorios externos.
Las variables exportadas en el proceso prevalecen sobre el archivo. Se admiten
comillas y comentarios dotenv, pero no expansión de variables ni comandos shell.
No se imprime la URL ni las claves, tampoco en excepciones o manifiestos.
No hay cambio automático de clave publicable a secreta.

## Primera descarga y criterio temporal

Ambos extremos son **días UTC incluidos**: `--desde 2026-11-10 --hasta 2026-11-10`
selecciona `[2026-11-10T00:00:00Z, 2026-11-11T00:00:00Z)`. No usa el huso horario
del equipo, Lima o Nueva York. La selección usa el timestamp del nombre, no la
fecha de modificación del objeto ni `last_updated` dentro del JSON. Un nombre no
reconocido o una discrepancia con su carpeta de fecha se informa como error.

Primero consultar el plan (lista metadatos y verifica archivos locales; no descarga
cuerpos ni escribe manifiestos, resúmenes o directorios):

```bash
.venv/bin/python pyspark/jupyter/UNIDAD1/Dim-Disponibilidad/src/extraccion.py \
  --env-file pyspark/.env.u2 --credencial secreta \
  --desde 2026-11-10 --hasta 2026-11-10 --dry-run
```

Descargar ese día con la credencial ya configurada que permitió leer el bucket:

```bash
.venv/bin/python pyspark/jupyter/UNIDAD1/Dim-Disponibilidad/src/extraccion.py \
  --env-file pyspark/.env.u2 --credencial secreta \
  --desde 2026-11-10 --hasta 2026-11-10
```

Repetir exactamente el comando comprueba la incrementalidad. Para un archivo de
configuración externo, sustituir solo `--env-file /ruta/privada/.env.u2`.
Con una clave publicable que tenga acceso, omitir `--credencial secreta`.

Por defecto se descargan `station_status`, `station_information` y `vehicle_types`.
`--fuente station_status` restringe la selección; puede repetirse para otras
fuentes. `--todas-las-fuentes` selecciona todo el bucket. Los directorios de salida
se anclan a esta dimensión, independientemente del directorio de ejecución.
`--raw-dir` y `--reports-dir` permiten destinos alternativos; deben excluirse de Git
si se ubican fuera de los directorios locales ya ignorados. El destino predeterminado
es el que comparte el volumen Docker existente.

## Garantías y límites del descargador

- Solo utiliza POST de listado de Storage y GET de objetos: no crea, actualiza o
  elimina archivos remotos. Rechaza redirecciones para no reenviar credenciales.
- Lista por nombre con `limit`/`offset` (100 por página por defecto) y recorre
  subcarpetas. Descubre las carpetas antes de seleccionar días. Los nombres de
  carpeta de fecha observados permiten evitar listados fuera del intervalo.
- Guarda cada objeto en `data/raw/<ruta relativa del bucket>` y conserva sus
  bytes gzip. Comprueba tamaño disponible, ETag y el CRC/final de gzip.
- El manifiesto `reports/descargas/manifiesto.json` conserva metadatos del listado
  remoto, identificador/versión, ETag, fechas, tamaño, cabeceras HTTP seleccionadas,
  SHA-256 local, fecha del nombre y fecha de descarga. Identifica el origen con una
  huella, sin guardar URL ni credenciales.
- Antes de omitir un candidato exige coincidencia de versión/metadatos, existencia,
  tamaño y SHA-256 local. `last_accessed_at` se conserva pero no dispara descargas.
  Un archivo ausente, alterado o sin registro válido se vuelve a descargar.
- Comprueba también la existencia de los demás archivos registrados y cuenta los
  ausentes; solo restaura los que pertenezcan al intervalo y fuentes seleccionados.
- Descarga primero a `.part`; publica mediante reemplazo atómico al terminar.
  Escribe el manifiesto atómicamente después de cada éxito. Un original anterior
  válido se conserva si falla su actualización. Los `.part` se reinician desde cero
  en el próximo intento; no son archivos completos ni reanudaciones por bytes.
- Tres intentos como máximo por operación, con espera limitada y timeout de
  30 segundos. Se reintentan fallos transitorios y transferencias truncadas, no
  errores de autorización. Opciones: `--intentos`, `--timeout`, `--page-size`.
- Un bloqueo impide dos descargas con el mismo directorio de informes. Se libera
  al terminar el proceso; no hace falta eliminar manualmente `descarga.lock`.
- Genera `resumen-<instante UTC>.json` y un resumen por consola, con omitidos,
  descargados, fallos y reintentos. Códigos: 0 éxito, 1 error, 2 sin capturas
  seleccionadas/visibles, 130 interrupción. Un listado vacío no se interpreta como
  prueba de bucket vacío. Una interrupción abrupta puede dejar sin resumen final,
  pero mantiene los éxitos ya registrados.
- Nunca elimina archivos locales porque desaparezcan del remoto. Un manifiesto
  corrupto o de otro origen detiene la ejecución en lugar de sobrescribirse.
- El listado con offsets no es una instantánea transaccional del bucket. Para días
  en curso o cambios concurrentes, repetir el mismo rango reconcilia nuevos objetos.
  Un ETag que cambie entre listado y descarga provoca error y exige reinventariar.

`data/`, `.json.gz`, `.env*`, `.venv/`, `.part`, `.tmp` e informes locales están
excluidos de Git; `.env.example` se versiona sin valores reales. Las carpetas de
datos/informes se crean localmente y Git no conserva directorios vacíos.

## Verificación

```bash
.venv/bin/python -m unittest discover \
  -s pyspark/jupyter/UNIDAD1/Dim-Disponibilidad/tests -v
```

Las pruebas usan Storage simulado, sin red: segunda ejecución, versión modificada,
archivo ausente, corrupción local del mismo tamaño, recuperación de transferencia
truncada, conservación del original ante error, gzip inválido, paginación y
subcarpetas, límites UTC, dry-run sin escrituras, permisos, reintentos, bloqueo y
manifiesto inválido. No modifican capturas remotas.

Resultado de la verificación del 16 de noviembre de 2026:

| Comprobación | Resultado |
|---|---|
| Pruebas locales en `.venv` (Python 3.14.10) | 20 aprobadas; sin paquetes adicionales |
| Sintaxis de los módulos | Comprobada con gramática Python 3.10; no se ejecutó el contenedor |
| Dry-run del 2026-11-10 UTC | 290 candidatos; cero cuerpos descargados |
| Primera descarga real de ese día | 290 archivos, 20.377.285 bytes, cero fallos/reintentos |
| Composición | 288 `station_status`, 1 `station_information`, 1 `vehicle_types` |
| Segunda ejecución del mismo comando | 290 omitidos, cero descargas, cero fallos |
| Inventario frente a manifiesto y disco | Coincidencia de los 290 objetos; ningún `.part` pendiente |
| Transferencia incompleta | Recuperación comprobada con transporte simulado; no se alteró ningún objeto remoto |

Los manifiestos y dos resúmenes reales quedaron en `reports/descargas/`, excluidos
de Git. Las capturas originales están en `data/raw/`, disponibles en la ruta del
volumen descrita arriba. Se comprobó la correspondencia del montaje en Compose;
no se inició Docker ni se ejecutó Spark para esta verificación.

Referencias del protocolo:
[Storage REST](https://supabase.com/docs/reference/self-hosting-storage/retrieve-an-object)
y [claves de API](https://supabase.com/docs/guides/getting-started/api-keys).


## Fase 2: inspección local con PySpark

Entrada: las capturas ya descargadas en `data/raw/`, compartidas como
`/opt/UNIDAD1/Dim-Disponibilidad/data/raw/`. Esta fase no importa configuración de
Supabase ni requiere credenciales. No se ejecuta el descargador.

- [Notebook principal](../notebooks/01_disponibilidad_batch.ipynb): ejecutar todas las
  celdas desde Jupyter del contenedor existente.
- `src/lectura.py`: inspección de claves/tipos de todos los originales y esquemas
  explícitos separados para las tres fuentes. Conserva archivo, captura UTC,
  tiempos Unix originales y timestamps de feed/estación. Añade hora y fecha de
  pared de `America/New_York`, manteniendo la sesión Spark en UTC.
- `src/diagnostico.py`: nulos, duplicados y conflictos, cadencia/huecos,
  antigüedad, correspondencia de IDs, catálogo de propulsión y consistencia de
  cantidades. No altera la cardinalidad de la tabla principal.
- `tests/test_lectura.py`: prueba con gzip sintéticos en Docker; conserva filas
  repetidas incluso con el mismo `last_reported`, comprueba huso horario,
  cadencia y rechazo de catálogos ambiguos.
- `reports/inspeccion/diagnostico.json`: resumen generado local, ignorado por Git.
  `reports/inspeccion/hallazgos.md`: conclusiones y reglas propuestas versionadas.
  `reports/descargas/` conserva su función original de fase 1.

Ejecución desde la raíz, con el servicio existente ya iniciado:

```bash
docker exec -w /opt/UNIDAD1/Dim-Disponibilidad lambda26-pyspark \
  python -m unittest discover -s tests -p test_lectura.py -v
docker exec -w /opt/UNIDAD1/Dim-Disponibilidad/notebooks lambda26-pyspark \
  jupyter nbconvert --to notebook --execute --inplace \
  --ExecutePreprocessor.timeout=1200 01_disponibilidad_batch.ipynb
```

El notebook también puede abrirse desde Jupyter en el puerto 4487. Resuelve su raíz
sin depender del directorio desde el que se abra. Solo escribe el propio notebook
al ejecutarlo con `--inplace` y el resumen de inspección; no sobrescribe capturas.
No materializa aún datos preparados o modelos.

La comparación con el catálogo es exploratoria: no se usa un catálogo diario
posterior como enriquecimiento causal de todos los estados anteriores. Con varios
snapshots de catálogo se deberá resolver una asociación temporal válida antes de
modelar. Los intervalos de reportes por estación no equivalen a la frecuencia del
capturador, especialmente si el reporte es viejo o no cambia.


**Fase 2 verificada en Docker:** notebook completo, siete celdas de código, cero
errores, Python 3.10.22 y PySpark 4.2.0. Se inspeccionaron 725.706 observaciones
de 2.520 estaciones en 288 capturas de estado; la cadencia del feed tiene mediana
de 300 s. Las discrepancias de conteos y reportes antiguos impiden asumir todavía
una etiqueta única de disponibilidad. Consulta los
[hallazgos y reglas propuestas](../reports/inspeccion/hallazgos.md).

El contenedor conserva `./jupyter:/opt`. Para ejecutarlo se reconstruyó la imagen
desde el Dockerfile existente porque la etiqueta local antigua arrancaba Conda
bajo `/opt/conda`, oculto por ese montaje. Dockerfile y Compose no tienen cambios;
Spark sigue en 4.2.0, coincidente con los notebooks existentes. No se reejecutaron
los notebooks de los compañeros ni se validó el flujo U2 del equipo.


## Fase 3: reglas de calidad y Silver

El mismo notebook añade investigación detallada y generación de Silver mediante
`src/calidad.py`. Véanse [reglas y resultados](../reports/calidad/reglas_y_hallazgos.md).
Se ejecuta en el mismo Docker con los originales locales; no importa configuración
Supabase ni necesita credenciales.

Los objetivos observacionales canónicos proceden de `vehicle_types_available`,
resolviendo el catálogo: `human` y `electric_assist`. No se reconstruyen cantidades
por resta ni se fuerzan sumas. Se preservan el total, el campo eléctrico escalar,
el array original, los campos opcionales y todos los tiempos/trazabilidad. Las
columnas `q_*` señalan discrepancias, cambios con un mismo reporte y otros problemas.
La diferencia con el total o la extensión eléctrica no invalida automáticamente
el registro ni suprime los conteos por tipo.

Cada objetivo tiene elegibilidad propia. Ausencias, conteos negativos o tipos
repetidos afectan al objetivo correspondiente, sin convertirse a cero. La identidad,
los tiempos y los estados operativos inválidos y los conflictos dentro de una
captura son motivos de invalidez. Inactividad, antigüedad y falta de definición del
objetivo son motivos diferentes para no usar una fila en entrenamiento. La tolerancia
provisional de frescura es 450 segundos; se documenta su base y sus límites.

Salidas locales, excluidas de Git:

```text
data/silver/<ejecucion UTC>/aceptados/fecha_captura_utc=YYYY-MM-DD/*.parquet
data/silver/<ejecucion UTC>/excluidos/fecha_captura_utc=YYYY-MM-DD/*.parquet
reports/calidad/resultado.json
reports/calidad/plan_fecha.txt
```

Los aceptados son elegibles para al menos un objetivo. Los excluidos conservan sus
campos originales y motivos, distinguiendo `invalido`, `inactivo` y `activo` sin
objetivos elegibles. Cada fila de entrada pertenece a un único conjunto. Los motivos
pueden solaparse: no se suman para obtener el número de excluidos.

Cada ejecución escribe en un directorio nuevo, sin sobrescribir raw ni otras
ejecuciones. **Leer solo la ejecución indicada por `resultado.json`**, no concatenar
todo `data/silver/`, que puede contener ejecuciones exploratorias previas. La versión
vigente es `silver-v2-tipos-gbfs`; una ejecución exploratoria de consenso v1 se
conserva localmente pero no define la política final.

Se verifican conteos, nombres y tipos del esquema y el multiconjunto completo
(`exceptAll` en ambas direcciones). El notebook incluye `explain(formatted)` y
comprueba `PartitionFilters` al filtrar `fecha_captura_utc`. Con un solo día local
hay una partición real de fecha: no se afirma una mejora de rendimiento medida
entre múltiples fechas.

Prueba de contratos Silver:

```bash
docker exec -w /opt/UNIDAD1/Dim-Disponibilidad lambda26-pyspark \
  python -m unittest discover -s tests -p test_calidad.py -v
```

El comando de ejecución completa del notebook es el indicado en fase 2. Los campos
`objetivo_*` son conteos observados en el instante actual, no etiquetas desplazadas
5/10/15 minutos. Los horizontes y el entrenamiento permanecen pendientes.


## Fase 4: Gold temporal con el día local disponible

Por instrucción posterior se suspendió la ampliación: siguen siendo **290 archivos
locales del 2026-11-10 UTC**. El inventario remoto y el código opcional
`src/ampliacion.py` se conservan para uso futuro; no se ejecutan desde el notebook.
El inventario halló 1.831 archivos pendientes hasta el 16 de noviembre y espacio
suficiente, pero no se descargaron. No confundir disponibilidad remota con histórico
realmente procesado.

`src/preparacion.py` construye variables, objetivos, controles y gráficos de
agregados pequeños sin instalar dependencias adicionales. La sección **Fase 4** del
notebook es independiente: iniciar su primera celda y ejecutar hacia abajo en el
Docker existente. Se conservaron las salidas históricas de fases 2 y 3; no se
reprocesan ni se sobrescribe su Silver al ejecutar solo esta sección.

La entrada está fijada en `reports/preparacion/entradas.json`: Silver
`20261117T040700694357Z`, versión `silver-v2-tipos-gbfs`, 707.762 aceptados y
17.944 excluidos, una única ejecución. Se leen ambos conjuntos para conservar
la continuidad de calidad e identificar huecos. No se mezclan versiones de Silver.
El manifiesto incluye ruta, tamaño y SHA-256 de los 290 originales y de las partes
Parquet leídas, más cortes, tolerancias y versiones. Se verifican los hashes antes
y después. Ese manifiesto es un artefacto local que debe conservarse para reproducir
la ejecución con sus datos ignorados por Git.

Variables: valores actuales reportados de ambos tipos; rezagos por tiempo de
5/10/15 min con sus timestamps reales; cambios respecto a cada rezago; media,
mínimo, máximo, desviación poblacional y número de muestras de los 15 minutos
**anteriores**; hora, minuto, día ISO, fin de semana y codificación cíclica de hora
en America/New_York. Ventanas y rezagos quedan dentro de tramos continuos por tipo;
un salto >450 s o una observación no elegible rompe el tramo. Ausencias de historia
quedan nulas, no se rellenan cantidades con cero. `n15=0` significa cero muestras.

Los objetivos reportados se emparejan con timestamps de captura a 5/10/15 min,
eligiendo el más cercano dentro de ±30 s (desempate: anterior). La tolerancia
representa el 10% de la cadencia de ~300 s, holgada frente al jitter de pocos
segundos observado. No se utiliza simplemente lead por posición. Se elige primero
la captura más cercana y después se aplica su calidad, sin saltar a otra más
conveniente. Se conservan horizonte efectivo y fecha observada del objetivo.

Cortes UTC comunes: train antes de 16:00, validación [16:00,20:00), prueba desde
20:00 hasta fin del día. Se purga cada objetivo que toque/cruce el siguiente corte.
No hay reparto aleatorio. Con un día esta es una separación técnica entre franjas
horarias distintas, no evidencia de generalización diaria, semanal o estacional.

La lista de predictores permitidos se guarda explícitamente. `f_*`, `y_*`, estratos,
motivos y los indicadores globales retrospectivos de reportes repetidos se usan
solo para auditoría/evaluación, nunca como variables predictoras. No se incorporan
metadatos de estaciones, evitando uniones con versiones posteriores al instante.
El significado de los tipos es la convención semántica ya declarada en Silver.

Se mantienen los indicadores de discrepancia iniciales y futuros. En fase 5 se
compararán las **mismas predicciones** por tipo/horizonte/partición sobre todos los
elegibles, sin discrepancias y con discrepancias. Indicadores desconocidos se
informan aparte. Elegibilidad no garantiza exactitud física: se pronostican
conteos reportados; no existe verdad física independiente para validarlos.

Salidas (una carpeta nueva por ejecución):

```text
data/gold/<ejecucion UTC>/aceptados/fecha_captura_utc=.../particion_temporal=.../*.parquet
data/gold/<ejecucion UTC>/excluidos/fecha_captura_utc=.../particion_temporal=.../*.parquet
data/gold/<ejecucion UTC>/entradas.json
data/gold/<ejecucion UTC>/verificacion.json
reports/preparacion/resultado.json
reports/preparacion/plan_fecha.txt
reports/preparacion/*.svg
reports/preparacion/hallazgos.md
```

Gold es ancho: una fila estación/captura/horizonte, con dos objetivos y elegibilidad
por tipo, sin duplicar los predictores por bicicleta. Se particiona por fecha UTC
inicial y split, no por estación; horizonte es columna filtrable. Los ejemplos sin
ningún objetivo elegible se guardan con motivos por tipo. Las incidencias se solapan
y no deben sumarse como filas. Leer solo la ejecución indicada en `resultado.json`.

Las pruebas focalizadas de huecos, calidad futura, tolerancia y purga de cortes se
pueden ejecutar con:

```bash
docker exec -w /opt/UNIDAD1/Dim-Disponibilidad lambda26-pyspark \
  python -m unittest discover -s tests -p test_preparacion.py -v
```

No se ajustaron parámetros. Imputadores, escaladores o codificadores que aprendan
parámetros deberán ajustarse exclusivamente con train en fase 5. Las variables de
historia nulas requieren una decisión explícita allí; no se ocultaron con ceros.
Los resultados cuantitativos finales y las limitaciones están en
[hallazgos de preparación](../reports/preparacion/hallazgos.md).


## Fase 5: entrenamiento y evaluación experimental

Se usa exclusivamente Gold `20261117T044523230826Z`, versión
`gold-v1-conteos-reportados`, con sus particiones existentes. La sección **Fase 5**
del notebook es independiente: ejecutar desde su primera celda hacia abajo en
el Docker existente. No descargar, regenerar Gold ni cambiar cortes para usarla.

`src/modelado.py` contiene la lista explícita de predictores, imputación ajustada
solo con train, comparación de candidatos, selección por validación, contratos,
recarga, métricas globales/por estrato/por estación y gráficos de agregados pequeños.
La política previa está en [protocolo de fase 5](../reports/modelado/protocolo.md).
Los resultados están en [hallazgos de fase 5](../reports/modelado/hallazgos.md).

Se compararon persistencia, dos LR ridge y dos Random Forest acotados por cada
tipo/horizonte (30 comparaciones). La selección usa MAE de validación después de
acotar predicciones negativas a cero, sin cota superior ni redondeo a enteros.
Empate ≤1e-6: persistencia, LR 0.01, LR 0.1, RF pequeño y RF mayor. Se guardó la
selección congelada antes de leer/evaluar prueba. Nunca se refit con prueba o se
eligió horizonte a partir de ella. Se informan MAE, RMSE, R² y conteos por estrato,
cambio/igual y estación; R² es nulo si el objetivo del subgrupo no varía.

Las medianas por variable y los indicadores de ausencia permiten mantener todas
las filas de evaluación. Los valores aprendidos proceden únicamente de train.
El vector de LR/RF tiene 60 posiciones: 30 variables imputadas y 30 indicadores.
No se usan station_id, objetivos, tiempos futuros, estratos o indicadores futuros
como predictores. station_id se conserva para el resumen por estación.

**Resultado de esta ejecución:** persistencia ganó por MAE de validación en las
seis combinaciones. Por ello los seis artefactos seleccionados son contratos de
persistencia, sin parámetros aprendidos ni necesidad de entrenamiento. Los modelos
LR/RF sí se entrenaron para la comparación, pero no se presentan como ganadores.
La recarga de los seis contratos reprodujo todas las predicciones de prueba con
diferencia máxima 0.0. La prueba sintética también verifica la recarga de un pipeline
entrenado con sus transformaciones para los casos en que resulte seleccionado.

Salida local verificada:

```text
data/modelos/20261117T052627988920Z/
  politica_previa.json
  seleccion_congelada.json
  proveniencia_gold.json
  seleccionados/<tipo>_<h>min/contrato.json
  comparacion_validacion.csv
  comparacion_prueba.csv
  subgrupos_prueba.csv
  estaciones/<tipo>_<h>min/persistencia/*.parquet
  predicciones/<tipo>_<h>min/*.parquet
  resultado.json
```

La salida registra versión y huellas de Gold, cortes, lista y orden de predictores,
parámetros, medianas de train, versiones de software/código y duración. Los CSV/JSON
conservan precisión numérica; las tablas Markdown solo formatean la presentación.
Los gráficos y el informe quedan en `reports/modelado/`; los modelos, predicciones
y tablas extensas están bajo `data/`, excluido de Git. Las nuevas ejecuciones crean
otra carpeta, sin sobrescribir estos resultados.

Pruebas de los contratos de modelado:

```bash
docker exec -w /opt/UNIDAD1/Dim-Disponibilidad lambda26-pyspark \
  python -m unittest discover -s tests -p test_modelado.py -v
```

Los resultados son experimentales: un día ya inspeccionado no es prueba final
independiente. Persistencia se beneficia de los casos sin cambio; el informe muestra
por separado su error cuando cambia el objetivo. Las métricas se refieren a conteos
reportados cuya exactitud física no está confirmada. No se implementó U2 ni la
integración final y no se hicieron commits ni push.
