# Disponibilidad de bicicletas por estación — Unidad 1

[Guía operativa única y comandos desde la raíz](../../../../OPERACION-DISPONIBILIDAD.md)


Responsable: **Jose Miguel Condo Huamani**. PySpark batch sobre snapshots GBFS,
con calidad, Gold temporal y comparación de modelos. La operación en tiempo real se documenta en [Disponibilidad U2](../../UNIDAD2/Dim-Disponibilidad/README.md).
Se conservan Dim-Duracion-de-viajes y las dimensiones de los compañeros.

[Comando de ejecución](#entorno-y-comando-de-ejecución) · [Resultados ampliados](reports/ampliacion/resultados_ampliados.md)

## Ejecución vigente y evidencia de la entrega

La ejecución seleccionada es **`20261117T171917344468Z`**, completa y verificada.
"Vigente" identifica sus resultados guardados; no significa la carpeta más reciente
ni una ejecución del código actualizado después de obtenerlos. No se mezclan versiones.

| Elemento | Referencia explícita |
|---|---|
| Resultados locales vigentes | `data/ejecuciones/20261117T171917344468Z/` |
| Selección para U2 | [config/ejecucion_seleccionada.json](config/ejecucion_seleccionada.json) |
| Notebook ejecutado que acredita los resultados | [Notebook ampliado archivado](notebooks/archivo/01_historico_ampliado_20261117T171917344468Z.ipynb): 11 celdas ejecutadas, cero errores, **local[2]** |
| Notebook para próximas ejecuciones | [Notebook principal](notebooks/01_disponibilidad_batch.ipynb), actualizado y con salidas limpias; predeterminado futuro **local[4]** |
| Resultados y limitaciones actuales | [Resultados ampliados](reports/ampliacion/resultados_ampliados.md) |
| Comparación de modelos | [Comparación completa](reports/ampliacion/comparacion_modelos.md) |
| Cobertura y reglas aplicadas | [Cobertura/calidad](reports/ampliacion/cobertura_y_calidad.md) y [metodología](reports/ampliacion/metodologia.md) |
| Cumplimiento y aporte personal pendiente | [Revisión actualizada](reports/ampliacion/cumplimiento_actualizado.md) y [evidencias personales](reports/ampliacion/evidencias_personales.md) |
| Cambio posterior de paralelismo | [Prueba pequeña de spark-master](reports/configuracion/spark_master.md), sin repetir el flujo completo |

### Referencia de contratos para U2

`config/ejecucion_seleccionada.json` fija el ID de ejecución, las versiones de
origen, los hashes de manifiesto/selección/notebook y **los seis contratos
seleccionados** por tipo y horizonte. Cada entrada de `contratos` contiene:

- `tipo` y `horizonte_min`, para seleccionar explícitamente una combinación;
- `ruta_original` y `sha256_original`, que identifican su archivo local exacto;
- `contrato`, copia íntegra del JSON original, pequeña y versionable.

Las rutas del archivo son relativas a la raíz de esta dimensión; el campo
`raiz_rutas_desde_repositorio` indica dónde está esa raíz. Los originales están en:

```text
data/ejecuciones/20261117T171917344468Z/modelos/seleccionados/<tipo>_<horizonte>min/contrato.json
```

U2 lee esa referencia y verifica tipo/horizonte explícitos, sin
ordenar carpetas por fecha, buscar "la última" ni sustituir la selección si falta
un artefacto. Puede consultar las copias `contrato` versionadas; si usa los archivos
locales, debe comprobar sus hashes y fallar si faltan o difieren. Los seis ganadores
son **persistencia**, sin fit ni pesos MLlib. Las referencias MLlib conservadas
en `modelos/referencias_mllib/` son comparadores, no sustitutos de esos ganadores.
No se ha elegido un horizonte definitivo.

Este JSON es una referencia de artefactos, **no** el archivo para `--config` del
lanzador. `config/u1-local.json` sigue definiendo los parámetros de futuras
ejecuciones. Cambiar esa configuración no cambia la ejecución seleccionada.

## Pregunta y alcance

¿Con qué precisión podemos predecir los conteos reportados por tipo de bicicletas
convencionales y eléctricas disponibles por estación a 5, 10 y 15 minutos usando
su historial? No se elige un horizonte definitivo mediante prueba.

Los objetivos son cantidades **reportadas** según vehicle_types_available y su
catálogo, cuya exactitud física no está confirmada. No se calcula convencionales
como total menos eléctricas. Elegibilidad no garantiza exactitud física.

## Histórico y periodos fijados

Se incorporó el histórico disponible hasta el **16/11/2026**, último día UTC
terminado al inventariar: **2.121 archivos**, 149.089.863 bytes. Incluye 2.105 estados
(89 del 9 parcial y 288 por día del 10 al 16), más ocho snapshots de cada catálogo.
La descarga incremental incorporó 1.831 archivos y conservó los 290 existentes;
la segunda pasada omitió los 2.121, sin transferencias. Raw permanece intacto.

| Intervalo UTC | Uso |
|---|---|
| 9/11, desde 16:35 | Contexto pasado, sin emisiones aceptadas para entrenar |
| 10–13/11 completos | Entrenamiento |
| 14/11 completo | Validación |
| 15–16/11 completos | Prueba reservada |

Los cortes a medianoche se fijaron antes de entrenar. Las etiquetas que alcanzan
el siguiente periodo se purgan. No se rellenan ausencias de estaciones. Siete días
completos no demuestran estacionalidad ni patrones semanales repetibles. Las fechas
UTC difieren del calendario de New York: train abarca del lunes 9 a las 19:00 al
viernes 13 a las 19:00 locales; validación incluye viernes por la noche y sábado.
El histórico preliminar de un
día ya inspeccionado se conserva por separado, sin presentarlo como prueba final.

[Protocolo previo y metodología](reports/ampliacion/metodologia.md).

## Resultados ampliados verificados

Ejecución única `20261117T171917344468Z`: 5.302.934 observaciones de entrada,
5.150.956 aceptadas Silver y 14.748.181 pares inicial/objetivo Gold aceptados
entre los tres horizontes. Cada par conserva ambos tipos, sin duplicar filas.

Se entrenaron y guardaron **30 modelos MLlib**. Persistencia ganó por MAE de
validación en las seis combinaciones; no requiere entrenamiento. El mejor MLlib
por validación fue LR sin regularización en cada combinación y se conserva como
referencia, sin sustituir al ganador. MAE posterior en los días de prueba:

| Horizonte | Convencionales, persistencia | Eléctricas, persistencia |
|---|---:|---:|
| 5 min | 0.07996535 | 0.18320925 |
| 10 min | 0.14172653 | 0.30528616 |
| 15 min | 0.19407588 | 0.40343664 |

[Resultados, subgrupos y límites](reports/ampliacion/resultados_ampliados.md),
[comparación de todos los modelos](reports/ampliacion/comparacion_modelos.md) y
[revisión de cumplimiento actualizada](reports/ampliacion/cumplimiento_actualizado.md).
También se conservan copias literales pequeñas de las tablas originales:
[validación](reports/ampliacion/comparacion_validacion.csv),
[prueba](reports/ampliacion/comparacion_prueba.csv) y
[subgrupos](reports/ampliacion/subgrupos_prueba.csv). Son 36, 36 y 144 filas
agregadas respectivamente, no predicciones individuales. Su ejecución de origen,
tamaño y SHA-256 están en la referencia seleccionada; se copiaron sin recalcular métricas.
Las recargas de ganadores y referencias MLlib reprodujeron las predicciones
con diferencia máxima cero. Notebook archivado desde kernel limpio: 11 celdas, cero errores;
[evidencia verificable](reports/ampliacion/verificacion_notebook.json).
Se conserva también el [notebook ampliado ejecutado](notebooks/archivo/01_historico_ampliado_20261117T171917344468Z.ipynb),
para mantener esa evidencia cuando el principal se vuelva a ejecutar.
La ejecución ampliada utilizó **`local[2]`**. El principal incorpora ahora la
configuración de paralelismo actualizada y tiene sus salidas limpias: este cambio
se verificó con una prueba pequeña, sin volver a ejecutar el procesamiento completo.

La mayor parte de los pares conserva el conteo entre extremos; el error aumenta
cuando cambia. LR mejora levemente el MAE dentro del subconjunto con cambios,
pero pierde globalmente frente a persistencia. Este diagnóstico de prueba no
cambia la selección ni elige un horizonte. Las discrepancias se mantienen y
sus subgrupos tienen errores distintos; no se afirma una explicación causal.

## Tests locales de Disponibilidad

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

## Entorno y comando de ejecución

Desde la raíz, con `lambda26-pyspark` iniciado usando el Docker/Compose existentes:

```bash
.venv/bin/python pyspark/jupyter/UNIDAD1/Dim-Disponibilidad/ejecutar_u1.py --notebook
```

Ese comando ejecuta el [notebook principal](notebooks/01_disponibilidad_batch.ipynb)
desde un kernel limpio y utiliza **el mismo** `src/flujo_u1.py` de la CLI. Reutiliza
resultados completos compatibles después de verificar hashes y recarga. Añadir
`--nueva` fuerza una ejecución nueva sin sobrescribir las anteriores.
Para futuras ejecuciones, el valor predeterminado es **`local[4]`**. Se puede
reducir explícitamente sin cambiar memoria ni servicios:

```bash
.venv/bin/python pyspark/jupyter/UNIDAD1/Dim-Disponibilidad/ejecutar_u1.py --spark-master 'local[2]' --notebook
```

Precedencia: `--spark-master` → `spark_master` del JSON → `local[4]` de la
configuración predeterminada. Se admite `local[N]` con N entero positivo.
Los JSON anteriores con `cores: 2` siguen funcionando y se normalizan a
`spark_master: "local[2]"` en memoria; no se reescriben. Se rechazan ambos campos
si se contradicen. Flujo y notebook usan la misma fábrica de sesión. En Run All
interactivo se respeta el contexto del último lanzamiento; una sesión activa
con otro master requiere un kernel nuevo.

Este cambio de código/configuración modifica la huella de compatibilidad.
Los comandos completos pueden iniciar una nueva preparación y entrenamiento si
no encuentran resultados compatibles: la ejecución histórica no se convierte en
una ejecución `local[4]` ni se alteran sus manifiestos. Para verificar solo el
paralelismo, usar la [prueba pequeña documentada](reports/configuracion/spark_master.md).

La [reutilización ampliada](reports/ampliacion/verificacion_reutilizacion.json)
se comprobó con el código anterior en la ejecución `20261117T191621979673Z`:
84,67 s con `local[2]`, sin fit ni descarga. No es una medición de `local[4]`.

```bash
# Flujo completo nuevo dentro del notebook:
.venv/bin/python pyspark/jupyter/UNIDAD1/Dim-Disponibilidad/ejecutar_u1.py --notebook --nueva

# El mismo flujo sin interfaz de notebook:
.venv/bin/python pyspark/jupyter/UNIDAD1/Dim-Disponibilidad/ejecutar_u1.py

# Configuración explícita del periodo ampliado:
.venv/bin/python pyspark/jupyter/UNIDAD1/Dim-Disponibilidad/ejecutar_u1.py --config pyspark/jupyter/UNIDAD1/Dim-Disponibilidad/config/u1-local.json --notebook
```

[config/u1-local.json](config/u1-local.json) es ahora la configuración ampliada.
`--desde`/`--hasta` son días UTC inclusivos y no cambian los cortes. Un periodo
incompatible se rechaza. Para ampliar nuevamente, inventariar primero y fijar
cortes prospectivos nuevos, reservando días sin consultar sus métricas.
[config/u1-preliminar.json](config/u1-preliminar.json) conserva los antiguos cortes
como referencia; el código histórico exacto está archivado localmente.

El lanzador y descargador usan biblioteca estándar en la `.venv` local. Si falta,
crear `python3 -m venv .venv` desde la raíz; no se instalan paquetes globales.
PySpark/MLlib/Jupyter se ejecutan en Docker: Python 3.10, Java 21, Spark 4.2.0.
Dockerfile y Compose no cambian. La ejecución ampliada ya completada usó
`local[2]`, 4 GiB del driver y 32 particiones: fueron límites del job, sin modificar
servicios. El valor predeterminado futuro cambia únicamente a `local[4]`;
la memoria permanece en 4 GiB y las particiones en 32. El equipo tiene
12 CPU lógicas y unos 16 GB de RAM; dos hilos fueron una elección conservadora,
no un límite del hardware. `spark_master` en el JSON permite configurar futuras
ejecuciones considerando memoria y procesos concurrentes. Cambiarlo exige una
sesión Spark nueva y no altera los resultados ya guardados.

El montaje **./jupyter:/opt** mantiene accesibles datos y resultados. Raw absoluto:

```text
Host: /home/miguel/proyec-bigdata/grupo-x-citibike-intelligent-mobility/pyspark/jupyter/UNIDAD1/Dim-Disponibilidad/data/raw/
Docker: /opt/UNIDAD1/Dim-Disponibilidad/data/raw/
```

La reproducción verificada usa estos originales locales. Raw, ejecuciones y
pesos de modelos están fuera de Git: otra persona necesita disponer de los mismos archivos
(comprobables con el manifiesto de entradas) o descargarlos con acceso autorizado.
Clonar el repositorio por sí solo no aporta el histórico ni los pipelines MLlib;
sí aporta las copias pequeñas de los contratos seleccionados y las métricas resumidas.

Por defecto no se lee dotenv, no se conecta a Supabase y no se necesitan credenciales.
Descarga opcional explícita con el backend incremental existente:

```bash
.venv/bin/python pyspark/jupyter/UNIDAD1/Dim-Disponibilidad/ejecutar_u1.py --descargar --env-file pyspark/.env.u2 --credencial secreta --notebook
```

La credencial secreta es la configuración existente que permite listar este bucket;
no se solicita otra clave ni se eleva el acceso automáticamente. Usar publicable
si su política permite listar. El archivo de entorno no se copia a Docker ni se
imprimen sus valores. El inventario previo comprueba espacio y reserva; no hay
escrituras remotas. `.env*`, `.venv`, raw, temporales y manifiestos están ignorados.

## Arquitectura y relación con el equipo

Se elige **Lambda** porque se necesita histórico auditable y reentrenamiento batch,
además de una ruta de baja latencia. Kappa requeriría un log duradero con
replay de todos los estados; el histórico verificado actual es Storage. En U1 solo
se materializa batch: no se declara una arquitectura Lambda completa desplegada.

```text
U1: API GBFS → capturador externo → Storage → descarga incremental → raw
    → lectura PySpark → calidad/Silver → preparación/Gold → comparación y artefactos
U2: API → productor → Kafka → Structured Streaming → calidad por tipo
    → persistencia → PostgreSQL → Grafana
```

El capturador es externo; su código no se localizó en esta rama. U2 consulta GBFS directamente y aplica los seis contratos seleccionados de U1.

La disponibilidad mide existencias reportadas, no demanda ni duración de viajes.
Complementa la planificación operativa sin sustituir las dimensiones de otros.
La [propuesta de actualización del brief](reports/ampliacion/propuesta_brief_disponibilidad.md)
conserva literalmente la pregunta central y solo propone cambiar el aporte propio;
no se modificaron los documentos ni los trabajos de los compañeros.

## Reglas, variables y modelos

- Lectura separada de station_status, station_information y vehicle_types, con
  esquema explícito. Tabla principal: estación/captura. Sin joins de capacidad o
  coordenadas futuras. Trazabilidad: archivo, captura UTC, last_updated/last_reported.
- Silver v2 conserva originales, flags y excluidos con motivos por objetivo.
  Inactividad se distingue de invalidez. Se mantienen las reglas justificadas:
  frescura/continuidad de 450 s y tipos explícitos presentes/no negativos.
  [Investigación y reglas](reports/calidad/reglas_y_hallazgos.md).
- Gold v3 conserva flags iniciales y futuros, con exclusión de emisiones del
  contexto parcial. Sus 30 predictores son conteos actuales, rezagos por tiempo
  5/10/15, cambios, estadísticas de los 15 minutos pasados y calendario New York.
  No usa objetivos ni tiempos/calidad futuros para predecir. Tolerancia ±30 s.
- Imputación por mediana e indicadores de ausencia aprendidos solo con train.
  No se convierten todas las ausencias en cero ni se rellenan huecos artificialmente.
- Comparación: persistencia; LR regParam 0/.01/.1; RF 8/d4 y 16/d6. Son 30 ajustes
  MLlib y 36 comparaciones. Selección por MAE de validación, RMSE/R² adicionales,
  clipping de negativas sin redondeo y política de empate previa. Prueba se evalúa
  después de congelar selección, sin refit ni elegir horizonte.
- Se guardan todos los candidatos aprendidos y la referencia MLlib por problema,
  además del ganador real. Persistencia no necesita entrenamiento si resulta
  seleccionada. Importancias RF se conservan e interpretan sin atribuir causalidad.
- Se evalúan todos, con/sin discrepancias, cambia/igual y resumen por estación.
  Los subgrupos reconcilian y los gráficos usan solo agregados pequeños.

## Salidas, verificación y conservación

Cada ejecución tiene `data/ejecuciones/<id>/` con raw inventariado por SHA-256,
configuración, versiones, conteos, Silver/Gold aceptados y excluidos, modelos,
comparaciones, análisis, gráficos, log y manifiesto. Nunca se concatenan versiones.
El proceso falla si fallan esquema, reconciliación, unicidad, tiempos, purga o
recarga. La prueba de PartitionFilters libera la caché para confirmar disco real.

El notebook incluye RDD (map/reduceByKey) sobre categorías de calidad y reconciliación
con DataFrame, además de jobs antes/después de construir el plan y ejecutar count.
Las pruebas temporales cubren huecos, contexto previo y límites entre particiones.

```bash
PYTHONPATH=pyspark/jupyter/UNIDAD1/Dim-Disponibilidad .venv/bin/python -m unittest discover -s pyspark/jupyter/UNIDAD1/Dim-Disponibilidad/tests -v
docker exec -w /opt/UNIDAD1/Dim-Disponibilidad lambda26-pyspark python -m unittest discover -s tests -v
```

La ejecución de un día permanece como antecedente:
[notebook preliminar](notebooks/archivo/00_prueba_preliminar_un_dia.ipynb),
[README histórico](archivo/README_preliminar_un_dia.md) y sus informes de fases 1–6.
Esos números no representan el histórico ampliado. Código/configuración originales
con hashes: `data/archivo_preliminar/20261117T142303450900Z/`; los artefactos iniciales
no se modificaron.

### Salidas históricas conservadas

| Ruta o ejecución | Estado y significado |
|---|---|
| `data/silver/`, `data/gold/`, `data/modelos/`, `data/referencias_mllib/` | Salidas preliminares independientes del ensayo de un día. No son la entrega ampliada vigente. |
| `data/archivo_preliminar/`, `data/archivo_intentos/` | Conservación de código, configuración y diagnósticos anteriores; no fuentes que se concatenen al resultado actual. |
| `20261117T142303450900Z` / `20261117T144109506933Z` | Integración y reutilización preliminares de un día. |
| `20261117T161730447719Z` / `20261117T163218298611Z` | Intentos ampliados interrumpidos/fallidos; sus salidas parciales no se seleccionan. |
| `20261117T191621979673Z` | Verificación por reutilización de la ejecución vigente, sin nuevos ajustes; no una nueva versión ganadora. |
| `reports/inspeccion/`, `reports/calidad/`, `reports/preparacion/`, `reports/modelado/`, `reports/integracion/` | Informes y cifras del desarrollo preliminar. La investigación de calidad fundamenta reglas reutilizadas, pero sus cifras no reemplazan las de `reports/ampliacion/`. |
| `reports/cumplimiento_u1/`, `archivo/README_preliminar_un_dia.md`, notebook preliminar archivado | Revisión y documentación históricas anteriores a ampliar datos. |

Estas rutas permanecen intactas. No se renombraron ni borraron para crear la referencia vigente.

### Qué queda versionable y qué permanece local

| Versionable en Git | Local e ignorado |
|---|---|
| Código, tests y scripts | `.venv/`, cachés y temporales |
| Configuraciones JSON sin secretos y `.env.example` | `.env`, `.env.*` reales y credenciales |
| README, informes Markdown y planes de ejecución pequeños | Capturas `.json.gz`, CSV de datos y Parquet |
| Notebooks de trabajo y evidencias archivadas pequeñas | Todo `data/`, incluyendo Silver, Gold, pesos y predicciones masivas |
| Gráficos SVG, diagnósticos y métricas JSON resumidas | `reports/descargas/` e inventario local de entradas `reports/preparacion/entradas.json` |
| Los tres CSV agregados actuales y los seis contratos dentro de la referencia JSON | Estado parcial `reports/modelado/en_curso.json` |

Las excepciones CSV de `.gitignore` son explícitas para esos tres resúmenes de
esta dimensión; no habilitan otros CSV ni abren `data/`. No se hicieron commits
ni push: "versionable" significa que Git permite incorporarlos posteriormente.

La [revisión oficial anterior](reports/cumplimiento_u1/revision_final.md) documenta
los pendientes detectados sobre un día. Esta ampliación aborda los técnicos; las
capturas con reloj/perfil, historial/publicación real, pitch y defensa individual
siguen siendo evidencia personal del estudiante. No se asigna una nota automática.
No se implementa U2 ni se hacen commits/push.
