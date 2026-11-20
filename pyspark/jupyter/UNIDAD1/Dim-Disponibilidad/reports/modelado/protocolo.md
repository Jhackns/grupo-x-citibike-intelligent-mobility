# Protocolo de fase 5 — fijado antes de prueba

> Antecedente de la prueba preliminar de un día. Los resultados ampliados se documentan en [reports/ampliacion](../ampliacion/metodologia.md); no sustituir sus cifras con las de este informe.
> El notebook principal se actualizó; la versión preliminar está [archivada](../../notebooks/archivo/00_prueba_preliminar_un_dia.ipynb).

Gold único: `gold-v1-conteos-reportados`, ejecución `20261117T044523230826Z`.
Se mantienen las particiones existentes: train antes de 16:00 UTC, validación
16:00–20:00, prueba 20:00–24:00 del 2026-11-10; objetivos purgados en los cortes.
No se descargan datos, no se ajustan cortes ni se elige horizonte con prueba.

## Predictores y ausencias

Se usan los 30 predictores numéricos explícitos de fase 4: por cada tipo, actual,
lag5/cambio5, lag10/cambio10, lag15/cambio15, media/min/max/std/n de historia de
15 minutos; hora, minuto, día ISO, fin de semana, seno y coseno de hora local.
Los nombres y orden están en `src/modelado.py:FEATURES` y en cada contrato.
station_id solo se usa para trazabilidad y evaluación por estación: este ensayo
no introduce codificación de miles de categorías con un solo día de historia.

Quedan fuera y_*, f_*, tiempos de objetivo, horizonte efectivo, estratos, motivos,
indicadores futuros y los indicadores retrospectivos calculados con el día entero.
Una selección automática de todas las columnas numéricas violaría el contrato.

Por combinación se ajusta Imputer de mediana con train, relativeError=0.001, y se
incluyen indicadores binarios de ausencia. Un cero aprendido como mediana no es
una sustitución arbitraria de todas las ausencias por cero. Columnas completamente
nulas en train se omiten para todos los candidatos; no se consultan validación o
prueba para esa decisión. VectorAssembler usa las medianas en orden explícito y
luego los indicadores; handleInvalid=error. No se descartan filas por nulos de
historia, manteniendo las mismas filas de evaluación para todos los candidatos.

## Candidatos acotados

- Persistencia: predicción igual al conteo actual reportado. No tiene fit.
- LR ridge con regParam 0.01 y 0.1, elasticNetParam=0, maxIter=40, tol=1e-6.
  Regularizar ayuda ante la dependencia entre rezagos, valores y cambios.
  Su estandarización interna se ajusta en fit exclusivamente con train.
- RF con 8 árboles/profundidad 4 y 16 árboles/profundidad 6; maxBins=32,
  minInstancesPerNode=20, featureSubsetStrategy=sqrt, maxMemoryInMB=128,
  subsamplingRate=1, seed=42. Se acota crecimiento para local[2]/driver 3 GB.

No es una búsqueda exhaustiva ni demuestra el límite de ninguna familia de modelos.
Se usan los mismos ejemplos de validación/prueba por tipo y horizonte. La mediana
es común a LR/RF dentro de cada combinación; persistencia no la necesita.

## Decisión previa y evaluación

Todas las predicciones se truncan a mínimo cero, sin cota superior ni redondeo a
enteros. Se conserva prediction_raw y se cuentan negativas. No se convierte una
predicción nula en cero; provoca error. La selección usa el MAE de validación de
esa política. RMSE y R² son métricas complementarias. R² de objetivos constantes
se informa como nulo, no como infinito.

Empate: MAE a no más de 1e-6 del mínimo. Prioridad fija: persistencia, LR 0.01,
LR 0.1, RF 8/4, RF 16/6. Se favorece simplicidad y determinismo, sin desempatar por
prueba, RMSE o R². Las seis combinaciones se mantienen separadas.

Después de comparar validación se guardan los artefactos elegidos y el archivo
seleccion_congelada.json con hora UTC y SHA-256. Solo después se leen filas de
prueba. Se evalúan también candidatos no elegidos como comparación descriptiva;
sus resultados no cambian ganador, parámetros, tratamiento de ausencias u horizonte.
No se refit con train+validación. Si gana persistencia, se conserva como tal.

Se informan métricas globales, sin/con discrepancias y casos desconocidos, más
cambio/igual del objetivo respecto al actual. Son las mismas predicciones con
subconjuntos de evaluación, no modelos ajustados por estrato. Se conservan conteos
para evitar que los estados sin cambio oculten el error cuando hay movimiento.
El resumen por estación incluye n, MAE, RMSE, R² y distribución de MAE por estación;
no se confunde el promedio macro de estaciones con el error micro global.

## Artefactos y alcance

Se guarda el pipeline seleccionado con SQLTransformer, ImputerModel, assembler y
regresor, o solo contrato/configuración si gana persistencia. Se recarga y compara
cada predicción de prueba con tolerancia 1e-10. Se guardan versión de Gold, archivos
con SHA-256, cortes, parámetros, orden del vector, medianas de train, métricas,
duración y versiones de software/código. Los modelos y tablas extensas viven en
`data/modelos/<ejecucion>/`, fuera de Git. El notebook conserva resultados y gráficos
de agregados pequeños; no se convierte el histórico a pandas.

Se predicen conteos reportados por tipo, cuya exactitud física sigue sin confirmar.
Elegibilidad no la garantiza. Un único día previamente inspeccionado no constituye
prueba final independiente ni permite generalizar a otros días, fines de semana
o estaciones del año. No se implementa U2 ni integración final.
