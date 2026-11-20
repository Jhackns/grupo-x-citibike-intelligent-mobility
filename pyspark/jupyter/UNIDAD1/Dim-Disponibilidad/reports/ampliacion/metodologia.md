# U1 ampliada: protocolo fijado antes del entrenamiento

Inventario obtenido el 17/11/2026, limitado a capturas anteriores al 17/11/2026
00:00 UTC, último día UTC terminado: **16/11/2026**. Se reutilizaron configuración,
Storage, paginación, validación, reintentos, temporales y manifiesto del descargador.
No se modificaron políticas ni archivos remotos. Credenciales solo en el host.

| Periodo UTC | Capturas station_status | Uso fijado |
|---|---:|---|
| 09/11, desde 16:35:08.575 | 89 | Contexto retrospectivo; no día de entrenamiento |
| 10–13/11 | 4 × 288 | Train |
| 14/11 | 288 | Validación |
| 15–16/11 | 2 × 288 | Prueba reservada |

Hay 2.121 archivos: 2.105 estados y ocho capturas de cada catálogo. El inventario
suma 149.089.863 bytes; faltaban 1.831 archivos, 128.712.578 bytes. Había
37.622.530.048 bytes libres y se reservaron al menos 5 GiB para procesamiento.
La descarga incorporó exactamente esos pendientes, sin errores/reintentos. Una
segunda pasada omitió los 2.121 archivos sin transferencias y verificó los hashes
de los 290 originales preliminares. La ejecución ampliada comprobó los relojes
internos: mediana de 300,003 s entre capturas y de 300 s entre last_updated.
No se encontraron huecos interiores por estación mayores de 450 s. El nombre
o el conteo por sí solos no demuestran frescura ni presencia antes/después de
las primeras/últimas observaciones de cada estación.

Los cortes se fijaron a medianoche UTC antes de consultar métricas: 14/11 para
validación, 15/11 para prueba, final exclusivo 17/11. No son repartos aleatorios.
Se usa el 9 parcial para rezagos del primer día completo, sin aceptar emisiones
de ese tramo como ejemplos de entrenamiento. Las etiquetas futuras que alcanzan
la partición siguiente se excluyen de su horizonte. El contexto pasado puede
atravesar un límite de partición si ya era observable; las etiquetas no lo cruzan.

La selección de los últimos dos días como prueba permite observar un domingo y
un lunes fuera del train. La validación es sábado y train martes–viernes: el
cambio de régimen limita los resultados. Siete días completos son un único ciclo,
no evidencia repetida de estacionalidad ni de un patrón semanal generalizable.
No se ajustarán cortes, horizonte, reglas ni hiperparámetros después de ver test.
Estos nombres de días corresponden al calendario UTC. En New York los cortes
son a las 19:00 del día anterior: validación incluye viernes por la noche y
sábado, y prueba sábado por la noche, domingo y lunes. No son días locales
completos ni una demostración de patrones repetidos de fin de semana.

## Reglas y contrato conservados

Se mantienen `silver-v2-tipos-gbfs`, frescura/continuidad de 450 s y tolerancia
temporal de ±30 s. La definición por tipo sigue siendo explícita, sin calcular
convencionales mediante una resta. Las discrepancias de representaciones no
invalidan automáticamente todo el registro; los flags iniciales y futuros se
conservan en Gold y se usan para evaluar, nunca como predictores futuros.
Elegibilidad no significa exactitud física confirmada. Los umbrales siguen siendo
provisionales: ampliar la muestra no los convierte en una validación física.

No se unen coordenadas/capacidad de station_information como variables. El
catálogo vehicle_types define la semántica de los IDs; se comprueba que no tenga
asignaciones ambiguas. No se agregan atributos futuros de estaciones ni se
multiplican filas por uniones de snapshots diarios de metadatos.

La preparación añade el inicio explícito del modelado para excluir
emisiones del contexto parcial. Se conserva el resto de variables, calidad y
emparejamiento por tiempo, con registro de fecha observada y horizonte efectivo.
No se rellenan huecos ni se deduplica por estación/last_reported.

La implementación vigente es `gold-v3-emparejamiento-estrecho`: resuelve primero
las coincidencias con claves y timestamps y añade después los payloads. La versión
anterior replicaba columnas anchas nueve veces y consumió unos 30 GiB temporales
antes de escribir Gold. Se interrumpió, sin borrar datos ni cambiar reglas. Una
regresión sintética comparó V2/V3 en ambos sentidos con `exceptAll`: 33 candidatos
de dos estaciones, con jitter, huecos, calidad distinta por tipo, contexto inicial,
cortes y objetivos ausentes; todos coincidieron excepto la etiqueta de versión.
La nueva función también rechaza claves de entrada repetidas y exige tres
emisiones/horizontes por observación antes de aplicar elegibilidad.

## Comparación preregistrada

Por tipo y horizonte 5/10/15 minutos se comparan:

- Persistencia, sin fit.
- Regresión lineal sin penalización y ridge 0.01/0.1: tres regularizaciones.
  El caso 0 permite comparar contra ausencia de penalización, sin búsqueda amplia.
- RF de 8 árboles/profundidad 4 y de 16/profundidad 6, semilla 42 y límites
  originales de memoria/instancias. No se aumenta la complejidad por ver test.

Son 30 ajustes MLlib y 36 comparaciones por partición evaluada. Se mantiene MAE
de validación como criterio, con RMSE/R² adicionales; en empates ≤1e-6 se prefiere
persistencia, ridge .01, ridge .1, LR sin regularizar, RF pequeño y RF mayor.
Se acotan predicciones negativas a cero y no se redondean. Transformaciones e
imputaciones aprendidas se ajustan solo con train. Se omiten columnas totalmente
nulas en train, con registro de nombres/orden/medianas y ausencia como indicador.

Todos los candidatos de una combinación evalúan las mismas filas. Se congela
la selección antes de prueba. Después se informa a todos, con/sin discrepancias,
cambia/igual y resumen por estación. No se decide horizonte usando test. Si
persistencia pierde se conserva el ganador real; si gana no se simula entrenamiento.

Se guardan todos los pipelines aprendidos en `modelos/candidatos/`, los contratos
ganadores en `seleccionados/` y el mejor MLlib por validación en `referencias_mllib/`.
Los contratos apuntan a los pipelines conservados, evitando copiar los pesos del
mismo candidato. Se verifican recarga y predicciones de ganadores y referencias.
`featureImportances` de los doce RF queda en `modelos/interpretacion/`: importancia
por impureza sobre train, no causalidad ni verdad física. Predictores correlacionados
y flags de ausencia pueden afectar esa atribución.

## Ejecución y conservación

Se usa el punto de entrada existente, con su orquestador compartido desde el
notebook. `--notebook --nueva` inicia un kernel limpio y ejecuta todas sus celdas.
No hay un pipeline alternativo. El contexto de Jupyter es público y no copia
variables de credenciales. El montaje `./jupyter:/opt` y los servicios no cambian.
La reserva de 4 GiB del driver y las 32 particiones se limitan a este job; las
cachés de datos completos se apoyan en disco y los vectores de ML usan proyección.

El ejercicio RDD reduce categorías de calidad/discrepancia y reconcilia con SQL.
La demostración de evaluación diferida comprueba jobs antes/después de construir
un plan y de ejecutar count. Solo se recopilan agregados pequeños. La prueba de
partition pruning libera primero la caché: una lectura nueva de la misma ruta
puede reutilizarla y mostrar `PartitionFilters: []`, que no prueba descarte de
particiones de disco. Se exige la fecha en el filtro físico y se reconcilia su N
contra los conteos previos a escribir.

La prueba preliminar de un día permanece en
`notebooks/archivo/00_prueba_preliminar_un_dia.ipynb`, las ejecuciones originales
y `data/archivo_preliminar/20261117T142303450900Z/` (código original con SHA-256 y
configuración conservados). Sus métricas no se presentan como resultados ampliados.
La ejecución `20261117T161730447719Z` fue interrumpida expresamente para corregir
la demostración de pruning: conserva estado fallido, log y notebook. No alimenta
la ejecución siguiente ni se mezclan sus salidas Silver/Gold.
La ejecución `20261117T163218298611Z` también conserva estado fallido y notebook:
se detuvo para reducir esos intermedios de Gold. Ninguna de esas ejecuciones
interrumpidas llegó a seleccionar modelos ni alimenta versiones posteriores.

## Cierre observado

Ejecución ampliada completa: `20261117T171917344468Z`, con notebook nuevo desde
kernel limpio (11 celdas de código, cero errores). [Resultados y límites](resultados_ampliados.md),
[comparación](comparacion_modelos.md) y [cumplimiento](cumplimiento_actualizado.md).
La configuración de este job fue `local[2]`, driver de 4 GiB y 32 particiones;
no era un límite del equipo, que dispone de 12 CPU lógicas y unos 16 GB de RAM.

Duración monotónica registrada: 5.433,20 s. Gold ocupó 2.389,38 s; comparación y
selección, 1.687,53 s; evaluación, 969,92 s. Los timestamps de pared abarcan más:
el monitoreo no tiene muestras entre 18:41:18 y 19:05:25 UTC. No se confirmó la
causa de esa discontinuidad; no se atribuye todo ese intervalo a cómputo Spark.
La reserva mínima observada de disco fue 12.427.743.232 bytes, sin borrar raw
ni ejecuciones anteriores. La recarga de los seis ganadores y las seis referencias
MLlib produjo diferencia máxima cero en todos los pares de prueba verificados.
