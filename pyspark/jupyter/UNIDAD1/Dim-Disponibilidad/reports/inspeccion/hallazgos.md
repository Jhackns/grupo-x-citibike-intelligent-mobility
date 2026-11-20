# Hallazgos de fase 2 — histórico local de disponibilidad

> Antecedente de la prueba preliminar de un día. Los resultados ampliados se documentan en [reports/ampliacion](../ampliacion/metodologia.md); no sustituir sus cifras con las de este informe.
> El notebook principal se actualizó; la versión preliminar está [archivada](../../notebooks/archivo/00_prueba_preliminar_un_dia.ipynb).

## Ejecución verificada

Notebook: [01_disponibilidad_batch.ipynb](../../notebooks/01_disponibilidad_batch.ipynb).
Ejecutado completo con siete celdas de código y cero errores, dentro de
`lambda26-pyspark`, Python 3.10.22 y PySpark 4.2.0. El informe reproducible es
`diagnostico.json` en esta carpeta (resultado local ignorado por Git).
No hubo acceso a Supabase ni nuevas descargas de capturas.

La imagen local antigua bajo la etiqueta del servicio usaba Conda en `/opt/conda`;
el bind mount `/opt` ocultaba su Python e impedía arrancar. Se reconstruyó usando
el Dockerfile ya existente, sin editar Dockerfile ni Compose. La versión declarada
y efectiva de Spark sigue siendo 4.2.0, coincidente con las salidas guardadas de
los cuatro notebooks U1 del equipo y el conector Kafka declarado en U2.
No se han reejecutado los notebooks de los compañeros: compatibilidad de versión
comprobada, validación integral de aquellos flujos pendiente.

Se confirmó mediante Docker el bind mount del directorio `pyspark/jupyter` del
host a `/opt`. Los 290 originales mantienen su SHA-256 frente al manifiesto de
fase 1. La prueba sintética de lectura pasó en Spark: conserva repeticiones,
verifica cambio de fecha local, cadencia de 300 s y catálogo ambiguo. En `.venv`
siguen pasando las 20 pruebas de descarga; la prueba Spark se omite en ese entorno
porque allí no se instala PySpark.

## Estructura y volumen

| Fuente | Archivos locales | Registros |
|---|---:|---:|
| station_status | 288 | 725.706 |
| station_information | 1 | 2.520 |
| vehicle_types | 1 | 2 |

Los tres JSON tienen `data`, `last_updated`, `ttl`, `version`, pero sus arrays son
respectivamente `data.stations`, `data.stations` y `data.vehicle_types`. Se leyeron
por separado, con esquemas explícitos; `station_status` es la tabla principal.
Se inspeccionaron las claves de todos los gzip: cero errores de estructura,
JSON corruptos, arrays nulos/vacíos o timestamps de nombre no interpretables.

Hay 2.520 estaciones distintas. Cero IDs nulos o duplicados en los catálogos,
cero estaciones sin correspondencia entre ambos feeds y cero tipos desconocidos
en los arrays. Estos resultados permiten las comparaciones diagnósticas sin
multiplicar filas. No se enriqueció de forma permanente la tabla principal.

## Tiempos y frecuencia

Todos los intervalos siguientes están expresados en UTC:

- Capturas según nombre: 2026-11-10 00:00:02.493 a 23:55:01.713.
- `last_updated` interno: 2026-11-09 23:58:44 a 2026-11-10 23:54:05.
- `last_reported`: mínimo 1970-01-02 00:00:00 y máximo 2026-11-10 23:53:49.
- No hay `captured_at` en JSON: la fecha del nombre es una convención de archivo,
  conservada junto al tiempo del feed y al de cada estación, no su sustituto.

| Reloj / intervalos distintos | Mínimo | Mediana | P95 | Máximo | Saltos >450 s |
|---|---:|---:|---:|---:|---:|
| Archivo | 295,933 s | 300,071 s | 301,362 s | 304,289 s | 0 |
| Feed interno | 284 s | 300 s | 302 s | 321 s | 0 |
| Reporte por estación | 3 s | 303 s | 365 s | 1.794.249.520 s | 1.976 |

El feed tiene 287 intervalos, sin reutilización ni retrocesos de `last_updated`.
**La cadencia observada es aproximadamente cinco minutos.** La cadencia de reportes
individuales no representa la frecuencia del capturador: su máximo está dominado
por timestamps antiguos/anómalos. Los cuantiles son aproximados (`percentile_approx`).

No hay huecos interiores >450 s entre capturas observadas de una estación. Sin
embargo, una estación (`2100948436010992354`) aparece en 234 capturas y deja de
aparecer después de aproximadamente 19:25 UTC; las otras tienen 288. Faltan 54
combinaciones frente a una cuadrícula completa de 2.520 × 288. Es ausencia en el
extremo del período, no un salto entre dos observaciones existentes; su causa no
se puede establecer con este día. No se han creado filas de relleno.

La fecha de pared `America/New_York` contiene 151.200 observaciones el 9 de noviembre
y 574.506 el 10. Un día UTC no equivale a un día local completo. Se conservan los
enteros Unix y timestamps UTC; la hora local es una columna derivada, sin cambiar
la zona de la sesión Spark.

## Conteos de bicicletas: correspondencia e inconsistencias

El catálogo observado contiene ID `1` con `human` e ID `2` con `electric_assist`.
Los estados contienen `num_bikes_available`, `num_ebikes_available` y un array
`vehicle_types_available` con `vehicle_type_id` y `count`. No hay un campo escalar
explícito llamado número de bicicletas convencionales.

Para inspección se obtuvieron `convencionales_catalogo` y `electricas_catalogo`
resolviendo el catálogo, sin join por cada entrada y sin restar automáticamente.

| Comprobación | Observaciones señaladas |
|---|---:|
| Suma de tipos diferente de num_bikes_available | 27.067 |
| electric_assist del array diferente de num_ebikes_available | 20.920 |
| human del array diferente de total menos eléctricas | 7.673 |
| Cantidades negativas | 0 |
| Eléctricas escalares mayores al total | 0 |
| Conteos nulos/negativos o tipos repetidos en el array | 0 |
| Tipos human/electric_assist sin resolver | 0 |

Ejemplo verificado: estación `66db41bf-0aca-11e7-82f6-3863bb44ef7c`, captura
2026-11-10 18:40:01.621 UTC: total 32, eléctricas escalares 20, pero array human=12
más electric_assist=19. No se puede reconciliar ese registro suponiendo que todas
las representaciones son equivalentes. El diagnóstico no permite atribuir la causa
ni elegir cuál conteo es la verdad. Debe resolverse antes de definir las etiquetas
futuras de entrenamiento; conservar cada representación mientras se investiga.

## Nulos, repeticiones, antigüedad y capacidad

- No hay nulos en campos principales de estado, trazabilidad ni tiempos.
  `num_scooters_available` y `num_scooters_unavailable` faltan en 20.667 filas cada
  uno. Son campos opcionales observados en parte del histórico, conservados como
  nulos; no se han convertido a cero.
- Cero duplicados exactos excedentes dentro de un archivo y cero claves repetidas
  estación/captura. La comparación de payload canoniza el orden del array solo
  para diagnóstico y no modifica los registros originales.
- Hay 359 claves estación/last_reported repetidas; 58 tienen más de una variante
  de contenido. Variantes significa diferencias en cualquier campo del estado,
  incluidos los opcionales, no necesariamente solo en cantidades de bicicletas.
  Esto confirma que deduplicar por estación/last_reported puede perder información.
- 17.913 observaciones tienen `is_installed=0` o `is_renting=0`; no se encontraron
  estados operativos fuera de 0/1. No se han eliminado estaciones no operativas.
- 16.767 observaciones tienen last_reported más de 15 minutos anterior al feed;
  15.247 superan 24 horas. Son conteos solapados. Ningún reporte es posterior al feed.
  El timestamp 86400 corresponde a enero de 1970 y requiere una regla de validez,
  no interpretarlo automáticamente como una estación reportando desde entonces.
- Ningún feed supera los 15 minutos de edad frente al nombre del archivo, ni es
  posterior al archivo.
- 22.615 observaciones superan capacidad al sumar bicicletas y docks disponibles y
  deshabilitados. La comparación usa el único snapshot diario de información;
  no prueba por sí sola que el estado sea inválido. Deben revisarse semántica,
  capacidad vigente y posible desfase entre fuentes.

## Reglas propuestas y comprobaciones pendientes

1. Mantener una zona raw inmutable y todas las observaciones; marcar en futuras
   capas, sin eliminar todavía, parseos inválidos, nulos críticos, tipos desconocidos,
   negativos y conflictos. No confundir cantidad igual con duplicado.
2. Conservar los conteos escalares y por tipo como variables independientes hasta
   investigar las discrepancias y acordar una fuente para cada etiqueta. No usar
   `total - eléctricas` como verdad sin una política explícita y verificable.
3. Tratar timestamps de 1970, edades >15 min y >24 h y estados no operativos como
   indicadores separados. Validar umbrales por estación y contexto antes de decidir
   exclusiones de entrenamiento; no tomar 15 minutos como regla definitiva.
4. Comprobar unicidad y vigencia temporal antes de cada enriquecimiento. Con varios
   catálogos, realizar correspondencias as-of sin información futura. El catálogo
   diario disponible puede ser posterior a estados de primeras horas.
5. Medir huecos interiores y ausencia en extremos por separado. Los 450 s son una
   tolerancia exploratoria alrededor de los cinco minutos observados. Para objetivos
   de 5/10/15 min habrá que definir tolerancia al instante objetivo y tratamiento de
   ausencias; no interpolar ni inventar objetivos en esta fase.
6. Antes de modelar: ampliar evaluación a más días ya disponibles localmente cuando
   los haya, investigar la estación que desaparece, verificar semántica de capacidad
   y desfase de conteos. Con este único día no se validan ciclos semanales ni cambio
   horario de Nueva York. El código del capturador externo no se ha verificado.

No se eliminaron registros, deduplicaron estados, escribieron Parquet preparados,
entrenaron modelos ni implementó U2. Los hallazgos se limitan al histórico local
inspeccionado y a las comprobaciones ejecutadas.
