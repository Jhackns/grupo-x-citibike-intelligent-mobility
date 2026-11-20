# Fase 3: investigación, reglas de calidad y Silver

> Antecedente de la prueba preliminar de un día. Los resultados ampliados se documentan en [reports/ampliacion](../ampliacion/metodologia.md); no sustituir sus cifras con las de este informe. Las reglas de calidad siguen vigentes; los conteos de este informe corresponden al periodo preliminar.
> El notebook principal se actualizó; la versión preliminar está [archivada](../../notebooks/archivo/00_prueba_preliminar_un_dia.ipynb).

## Ejecución verificada

Versión vigente: **silver-v2-tipos-gbfs**, ejecución UTC `20261117T040700694357Z`. Notebook completo ejecutado en el contenedor existente con PySpark 4.2.0 y Python 3.10.22, sin errores. Se verificaron lectura de vuelta, esquema y multiconjunto. No se accedió a Supabase, no se descargaron capturas ni se modificaron Dockerfile o Compose.

[Notebook con salidas](../../notebooks/01_disponibilidad_batch.ipynb), [reglas](../../src/calidad.py) y `resultado.json` local son las evidencias. Los resultados de esta fase complementan, sin alterar, el diagnóstico de fase 2.

## Definiciones consultadas y límites

[Citi Bike](https://citibikenyc.com/system-data) remite a GBFS. [GBFS 2.3](https://github.com/MobilityData/gbfs/blob/v2.3/gbfs.md#station_statusjson) define total funcional, conteos por tipo y suma recomendada; la disponibilidad depende de `is_renting`. `num_ebikes_available` no está definido en esa versión. No se localizó una definición adicional del operador que explique sus diferencias. La norma no confirma la causa de las discrepancias observadas.

| ID observado | form_factor | propulsion_type |
|---|---|---|
| 1 | bicycle | human |
| 2 | bicycle | electric_assist |

Este catálogo identifica el significado de los conteos del array. No identifica cuál campo refleja mejor la realidad física cuando discrepan. La causa permanece **no confirmada**: no se atribuye a reservas, baterías, asincronía, vehículos deshabilitados ni scooters.

Los campos `num_scooters_available` y `num_scooters_unavailable` están presentes: cada uno tiene 705.039 ceros y 20.667 ausencias/nulos, sin positivos. Solo aparecen IDs 1 y 2 en los arrays. No hay evidencia de scooters operativos en este histórico. Su nombre no demuestra su existencia.

## Distribución de las 27.067 discrepancias

Se define delta = `num_bikes_available - (count_tipo1 + count_tipo2)`, manteniendo los originales. Son comparaciones, no correcciones. Ambos tipos y el total están presentes en las 725.706 observaciones.

Hay 14.339 diferencias positivas y 12.728 negativas. El rango es de -16 a 58. Las diferencias de ±1 suman 25.217 casos. Ninguna discrepancia se explica por sumar el conteo de scooters; coincidencias aisladas con disabled no justifican una fórmula.

### Distribución completa

| Delta | Observaciones |
|---:|---:|
| -16 | 1 |
| -14 | 1 |
| -13 | 1 |
| -12 | 1 |
| -11 | 1 |
| -8 | 4 |
| -7 | 2 |
| -6 | 5 |
| -5 | 3 |
| -4 | 14 |
| -3 | 83 |
| -2 | 766 |
| -1 | 11.846 |
| 1 | 13.371 |
| 2 | 849 |
| 3 | 79 |
| 4 | 18 |
| 5 | 6 |
| 6 | 2 |
| 8 | 1 |
| 9 | 3 |
| 10 | 4 |
| 11 | 1 |
| 12 | 1 |
| 16 | 1 |
| 28 | 1 |
| 33 | 1 |
| 58 | 1 |

### Ejemplos originales de ambos signos

Todos los ejemplos siguientes corresponden a 2026-11-10 00:00:02.493 UTC. El notebook y JSON guardan archivo completo, `last_updated`, `last_reported` y array original.

| Estación | Total | Tipo 1 | Tipo 2 | Eléctricas escalar | Delta |
|---|---:|---:|---:|---:|---:|
| 0f45bcf6-7028-4584-a51e-4129847dbebc | 37 | 18 | 18 | 19 | 1 |
| 1057229c-f8f8-4014-80cd-f40421c61cf0 | 2 | 1 | 0 | 0 | 1 |
| 1786697166576476490 | 36 | 18 | 17 | 17 | 1 |
| 00d3e65e-b614-4408-aaa9-939ed96ce645 | 38 | 10 | 29 | 29 | -1 |
| 067d5007-bda5-4697-b0a2-ccc2837f295c | 3 | 1 | 3 | 2 | -1 |
| 0cb63737-64c9-42a9-9479-280d49ed015a | 21 | 21 | 1 | 0 | -1 |

### Concentración por estación y hora

Se observan discrepancias en **2.124 de 2.520 estaciones**, por lo que no se limitan a una estación. Las tres con más incidencias son:

| Estación | Discrepancias / observaciones | Tasa |
|---|---:|---:|
| b0bac4ef-bb57-4598-a433-75c281170d5b | 288 / 288 | 100.0% |
| f72a253f-3cef-4da8-9591-0cbd3a692162 | 195 / 288 | 67.7083% |
| 66dcf640-0aca-11e7-82f6-3863bb44ef7c | 189 / 288 | 65.625% |

La primera discrepa en las 288 capturas, siempre con delta positivo. Esta persistencia no establece una causa. El JSON conserva las primeras 20 estaciones por número de incidencias.

| Hora America/New_York | Discrepancias | Observaciones | Tasa |
|---:|---:|---:|---:|
| 00 | 492 | 30.240 | 1.627% |
| 01 | 195 | 30.240 | 0.6448% |
| 02 | 170 | 30.240 | 0.5622% |
| 03 | 162 | 30.240 | 0.5357% |
| 04 | 152 | 30.240 | 0.5026% |
| 05 | 297 | 30.240 | 0.9821% |
| 06 | 699 | 30.240 | 2.3115% |
| 07 | 1.708 | 30.240 | 5.6481% |
| 08 | 1.972 | 30.240 | 6.5212% |
| 09 | 1.783 | 30.240 | 5.8962% |
| 10 | 857 | 30.240 | 2.834% |
| 11 | 1.362 | 30.240 | 4.504% |
| 12 | 1.065 | 30.240 | 3.5218% |
| 13 | 1.401 | 30.240 | 4.6329% |
| 14 | 1.253 | 30.234 | 4.1443% |
| 15 | 1.786 | 30.228 | 5.9084% |
| 16 | 1.699 | 30.228 | 5.6206% |
| 17 | 2.515 | 30.228 | 8.3201% |
| 18 | 2.288 | 30.228 | 7.5691% |
| 19 | 1.780 | 30.240 | 5.8862% |
| 20 | 1.028 | 30.240 | 3.3995% |
| 21 | 1.194 | 30.240 | 3.9484% |
| 22 | 770 | 30.240 | 2.5463% |
| 23 | 439 | 30.240 | 1.4517% |

El máximo es 8,3201% a las 17 h, frente a 0,5026% a las 04 h. Se trata de un único día UTC que atraviesa dos fechas locales: las horas 19–23 son del 9 de noviembre y las 00–18 del 10. No es evidencia causal ni permite generalizar ciclos semanales.

### Estados operativos y antigüedad

| installed/renting/returning | Discrepancias | Observaciones | Tasa |
|---|---:|---:|---:|
| 1/1/1 | 27.049 | 707.793 | 3.8216% |
| 1/0/0 | 18 | 2.907 | 0.6192% |
| 0/0/0 | 0 | 15.006 | 0.0% |

| Edad del reporte frente al feed | Discrepancias | Observaciones | Tasa |
|---|---:|---:|---:|
| 0_450s | 27.060 | 708.855 | 3.8174% |
| 451_900s | 6 | 84 | 7.1429% |
| 901_86400s | 1 | 1.520 | 0.0658% |
| mayor_24h | 0 | 15.247 | 0.0% |

27.060 discrepancias ocurren con edad ≤450 s y ninguna con edad >24 h. La tasa de 451–900 s es mayor pero se basa en solo 84 observaciones. Los reportes antiguos no explican el volumen principal de discrepancias.

## Comparación de las representaciones eléctricas

| Comprobación | Observaciones |
|---|---:|
| escalar_ausente | 0 |
| escalar_cero | 159.132 |
| tipo2_ausente | 0 |
| tipo2_cero | 159.326 |
| ambas_presentes | 725.706 |
| coinciden | 704.786 |
| difieren | 20.920 |
| escalar_mayor | 11.282 |
| escalar_menor | 9.638 |

Las 20.920 diferencias no son valores ausentes: ambos campos están presentes en todos los registros. Cero es un conteo observado y no equivale a nulo. No se completa el campo escalar ausente usando cero ni se reemplaza un conteo del array con la extensión.

## Conflictos y repetición

Los **58** conflictos estación/last_reported ocurren entre capturas distintas; hay **cero** conflictos dentro de una captura. De esos grupos, 51 cambian is_renting, 51 is_returning, 51 el array por tipo, 50 el total, 47 num_bikes_disabled, 41 la extensión eléctrica y uno is_installed. Las categorías se solapan. Cambios entre capturas no se interpretan como duplicados descartables.

Las reglas anotan variantes por captura y por reporte. Un conflicto dentro de una captura impediría elegir un estado único y se excluirían sus versiones con motivo explícito. No existe ese caso en el día inspeccionado. Los duplicados exactos se conservan; la prueba sintética comprueba su multiplicidad.

## Frescura e inactividad

| installed/renting/returning | Filas | P50/P95 edad (s) | >450 s | >900 s | >24 h |
|---|---:|---|---:|---:|---:|
| 1/1/1 | 707.793 | [57, 130] | 31 | 2 | 0 |
| 1/0/0 | 2.907 | [17492, 2263784] | 1.954 | 1.903 | 453 |
| 0/0/0 | 15.006 | [1794219211, 1794263645] | 14.866 | 14.862 | 14.794 |

El umbral operativo provisional de **450 s** es 1,5 cadencias de 300 s y supera
el P95 activo de 130 s. Solo 31 reportes de estaciones plenamente operativas lo
superan; usando 900 s serían 2. Elegir 900 s retendría 29 observaciones activas
adicionales, pero toleraría tres períodos de captura sin actualización. La política
elige mayor frescura para disponibilidades de corto plazo. No se presenta como
umbral validado para todo el sistema ni como cumplimiento de GBFS; requiere
sensibilidad al ampliar el histórico. La regla se aplica también a edad del feed
frente al archivo; en este día ningún feed supera el umbral.

Estación inactiva y registro inválido se distinguen. is_renting=0/is_installed=0
impiden usar el conteo como disponibilidad alquilable, sin transformar el stock
a cero. is_returning=0 por sí solo no impide alquilar. El valor last_reported=86400
se marca como anomalía observada; **no** se afirma que sea un sentinel documentado.

## Estación con cobertura parcial

`2100948436010992354`, Review Ave & Greenpoint Ave, tiene 234 capturas: 163 con
estados 1/1/1 y 71 con 0/0/0. Desde 13:35:02.695 hasta 19:25:01.660 UTC aparece
inactiva; en su último estado reporta ceros y last_reported=86400. Después faltan
54 capturas hasta el fin del día. Sigue presente en el catálogo diario con capacidad
17, pero ese snapshot es anterior al cambio. No se concluye que la estación fuera
retirada físicamente ni se inventan estados posteriores. No se completan huecos
ni se arrastran ceros o el último valor.

## Política final y fiabilidad de los objetivos

- `objetivo_convencionales` es el count explícito del tipo human, no una resta.
- `objetivo_electricas` es el count explícito del tipo electric_assist.
- Se entrenaría sobre **cantidades reportadas según la definición por tipo**.
  No existe validación física independiente. La exactitud física de las distintas
  representaciones sigue pendiente; las discrepancias no se han explicado.
- Cada objetivo necesita tipo definido sin ambigüedad, una entrada única, count
  presente y no negativo. Si falla, ese objetivo queda nulo y con motivo.
- No se exige acuerdo con total o num_ebikes_available para aceptar un count por
  tipo. Son controles de discrepancia que se preservan para futuras evaluaciones
  estratificadas; no se modifica ninguna cantidad para hacerlos coincidir.
- Los criterios comunes son trazabilidad, identidad, tiempos del feed y estados
  operativos válidos; estación instalada/alquilando y reporte/feed dentro de la
  tolerancia de frescura. Conflictos dentro de captura invalidarían la observación.
- Cantidades auxiliares negativas o inconsistentes se señalan; no invalidan por sí
  solas los conteos explícitos de otros tipos. Catálogo ambiguo deja los objetivos
  pendientes, sin asignar tipos por nombre de columna ni imputar ceros.
- Las observaciones iguales de tiempos distintos y los cambios con igual
  last_reported se conservan. No se deduplica por estación/last_reported.

La elegibilidad permite futuros experimentos con etiquetas observacionales; no
significa que se haya elegido modelo, horizonte o definición de verdad física.
Los objetivos 5/10/15 minutos todavía no se construyen. Las tasas por estación y
hora muestran posible sesgo de calidad y deberán acompañar la evaluación futura.

## Reconciliación y evidencia Parquet

| Conjunto | Filas |
|---|---:|
| Entrada | 725.706 |
| Aceptados Silver | 707.762 |
| Excluidos conservados | 17.944 |

Los 707.762 aceptados son elegibles para ambos objetivos en este día. Las pruebas sintéticas comprueban casos donde solo uno es elegible. Los 17.944 excluidos se dividen en 17.913 observaciones inactivas y 31 activas antiguas; no hay registros estructuralmente inválidos en esta entrada.

**Impacto de discrepancias:** 27.047 de las 27.067 filas se conservan aceptadas con sus indicadores. Solo 20 quedan excluidas por inactividad/frescura, no por discrepancia. No se eliminan esos registros: permanecen completos en excluidos.

| Combinación exclusiva de motivos | Filas |
|---|---:|
| estacion_no_alquila, reporte_antiguo | 3.621 |
| reporte_antiguo | 31 |
| estacion_no_alquila, tiempo_reporte_no_utilizable, reporte_antiguo | 13.199 |
| estacion_no_alquila | 1.093 |

La suma de combinaciones es 17.944. Las incidencias por motivo se solapan (17.913 estación no alquila, 16.851 reporte antiguo y 13.199 tiempo no utilizable) y **no** se suman para reconciliar registros.

Destino verificado dentro del contenedor: `/opt/UNIDAD1/Dim-Disponibilidad/data/silver/20261117T040700694357Z`. Contiene `aceptados/` y `excluidos/`, ambos particionados por `fecha_captura_utc=2026-11-10`. Leer exclusivamente esta ejecución; no concatenar ejecuciones previas. El directorio corresponde al mismo proyecto bajo `data/silver/` en el host.

La lectura de vuelta comprobó nombres y tipos de todas las columnas. Parquet
relaja nullability; no se exige igualdad de esa propiedad. Se verificaron conteos,
disjunción aceptado/excluido y equivalencia de multiconjuntos mediante exceptAll
en ambos sentidos, conservando multiplicidades y nulos. Archivo de origen, tiempos,
cantidades originales, array e indicadores permanecen en ambos conjuntos.

El notebook ejecuta explain(formatted). `plan_fecha.txt` local contiene:

```text
FileScan parquet
PartitionFilters: [isnotnull(fecha_captura_utc), (fecha_captura_utc = 2026-11-10)]
```

El conteo filtrado se reconcilió con el de entrada elegible para esa fecha. Existe
una única fecha UTC en el histórico; la evidencia verifica el filtro de partición,
no una mejora de rendimiento medida frente a varias fechas.

## Verificaciones y límites

La prueba de calidad pasó en el Docker existente, cubriendo conteos por objetivo,
ausencias frente a ceros, tipos repetidos/negativos, discrepancias preservadas,
conflictos dentro de captura, inactividad, antigüedad, repeticiones entre capturas,
lectura Parquet, esquema, multiplicidades y filtro de partición. El notebook final
terminó sin errores. No se cambiaron servicios del equipo ni versiones de Spark.
El hash de los originales se verifica frente al manifiesto de fase 1.

No hay datos físicos de contraste, una definición documentada adicional de la
extensión eléctrica, historial suficiente para generalizar umbrales ni causa
confirmada de las discrepancias. La interpretación por tipo es explícita y
reproducible, pero no elimina esa incertidumbre. No se entrenó ni implementó U2.

## Cierre de fase 3 y contrato para Gold

La elección de `vehicle_types_available` es una **convención explícita de objetivo**:
permite usar una misma representación con identificador y conteo por tipo,
respaldada por el catálogo local. No se eligió porque demuestre ser más exacta
físicamente que `num_ebikes_available`, ni porque su suma coincida mejor con el total.
La documentación del formato define significados, no arbitra qué representación
es físicamente correcta en las discrepancias. Elegibilidad es cumplimiento de las
reglas adoptadas; **no es garantía de exactitud**.

Gold preserva los indicadores de discrepancia del instante inicial y del
objetivo. En fase 5 se deberán comparar las mismas predicciones, para cada tipo,
horizonte y partición, sobre: todos los elegibles, subconjunto sin discrepancias y
subconjunto con discrepancias. Casos con indicadores desconocidos se informarán
aparte, sin convertirlos en ausencia de discrepancia. Las métricas no medirán
directamente precisión frente a conteos físicos independientes, que no existen
en este histórico. Ningún modelo se entrena durante estas fases de preparación.
