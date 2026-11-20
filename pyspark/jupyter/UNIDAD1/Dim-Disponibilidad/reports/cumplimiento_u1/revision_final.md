> Revisión histórica realizada sobre un día, anterior a la ampliación. Las evidencias técnicas posteriores están en [reports/ampliacion](../ampliacion/metodologia.md). Esta matriz no describe el estado del código ampliado.

# Revisión final de cumplimiento U1

Consulta de la guía oficial: **17/11/2026**. Dimensión: disponibilidad reportada por
tipo. Responsable: Jose Miguel Condo Huamani. Rama: `u2-duracion-miguel`.

Fuente principal: [rúbrica oficial U1, sección 5 y tablas técnicas](https://262bigdata.github.io/lambda26/proyecto-sello/u1-producto/).
La matriz provisional de fase 6 queda sustituida por esta revisión. Los estados
indican respaldo documental/técnico, no una calificación A–D. No se asigna nota
sin observar defensa individual ni autenticar capturas personales.

**Datos usados: un único día UTC, 10/11/2026.** Son 290 originales: 288 capturas
station_status, una station_information y una vehicle_types. Capturas de estados
entre 00:00:02.493 y 23:55:01.713 UTC; cadencia observada cercana a cinco minutos.
En America/New_York son partes del 9 y 10 de noviembre, no dos días completos.
Train, validación y prueba son horarios distintos de ese mismo día. No hay
evaluación final independiente ni evidencia de generalización semanal/estacional.
Las etiquetas son conteos reportados por tipo, sin exactitud física confirmada.

## Matriz de los siete criterios

Pesos y organización: [tabla 3 oficial de U1](https://262bigdata.github.io/lambda26/proyecto-sello/u1-producto/).
Rutas relativas a `Dim-Disponibilidad/`, excepto los documentos comunes indicados.
Verificado significa respaldo en código/salidas reales; parcial señala evidencia
incompleta; pendiente personal exige actuación del estudiante.

| Requisito | Evidencia concreta | Estado | Pendiente |
|---|---|---|---|
| 1. Decisión arquitectónica y fundamento (12%) | README y `reports/integracion/entrega_u1.md`: fuentes, ruta batch y futura ruta en vivo. `docs/proyecto-sello/u1-producto.md` del equipo declara Lambda. | **Parcial** | Explicitar Lambda frente a Kappa para disponibilidad y armonizar fuente/pregunta con el brief común, todavía centrado en duración/CSV. U2 está prevista, no construida. |
| 2. Uso de Spark y procesamiento paralelo (16%) | `src/lectura.py::esquema/leer_fuente`, `calidad.py::aplicar_reglas`, `preparacion.py::construir_variables/construir_gold/explorar`; notebook con transformaciones, agregados y planes. | **Parcial** | No hay operaciones RDD explícitas en módulos ni celdas. Falta la demostración RDD y explicación observable de transformación frente a acción de S2. DataFrames no sustituyen ese ejercicio. Entorno probado: `local[2]`, no clúster multinodo. |
| 3. Persistencia analítica con particiones (16%) | Notebook `gold-07`: escritura/lectura, `exceptAll` y salida real con `PartitionFilters`; `reports/preparacion/plan_fecha.txt`. Manifiesto fase 6: Silver/Gold y reconciliación. | **Verificado técnico** | Preparar capturas personales y explicar el plan. El filtro conservado corresponde a Gold de fase 4; fase 6 tiene su propia distribución física y verificación de filas/esquema. No se midió ganancia entre varias fechas. |
| 4. Reproducción completa desde el cuaderno (12%) | `ejecutar_u1.py`, `src/flujo_u1.py`; ejecución nueva `20261117T142303450900Z` y reutilización `20261117T144109506933Z`, completas. Secciones del notebook ejecutadas separadamente. | **Parcial** | CLI completa verificada; no se acreditó todo el notebook seguido desde kernel limpio. Secciones históricas fijan versiones Silver/Gold diferentes y JSON locales ignorados. La sección fase 6 presenta/verifica una ejecución previa. Falta demostrar el cuaderno continuo sin ajustes manuales. |
| 5. Aprendizaje distribuido, métricas y conservación (16%) | `src/modelado.py`, notebook `modelado-03/05`, informes de modelado/integración: 24 ajustes MLlib, 30 comparaciones de validación y 30 de prueba, MAE/RMSE/R². Seis contratos de persistencia y ahora seis PipelineModel MLlib de referencia guardados y recargados. | **Mínimo técnico respaldado; S4 parcial** | Referencia aprendida no es ganador global. Detalles S4 pendientes: tercera regularización y relevancia RF. La aceptación formal de persistencia ganadora más referencia entrenada corresponde al docente; no se garantiza A. |
| 6. Documentación y evidencia de ejecución (8%) | README, informes, salidas del notebook, manifiestos, hashes y `reports/integracion/verificacion.md` con 31 pruebas y recarga. | **Parcial** | No hay capturas personales con identidad/reloj. Timestamps de logs no las sustituyen. La dimensión está sin seguimiento en Git: `git ls-files` vacío para la carpeta. Un clon publicado no contiene todavía este aporte. No se hacen commits/push en la revisión. |
| 7. Defensa individual (20%) | Responsable/aporte declarados en README y carpeta propia. Sin registro de exposición ni respuestas personales. | **Pendiente personal** | Comprobar personalmente los seis aspectos de la tabla siguiente; la automatización no demuestra autoría ni dominio. |

## Interpretación con las sesiones enlazadas

| Requisito | Evidencia concreta | Estado | Pendiente |
|---|---|---|---|
| S1: propuesta arquitectónica propia | README con tecnologías, flujos, límites; elección Lambda en documento común. | **Parcial** | Justificar la decisión aplicada a la nueva dimensión y su relación con la pregunta del equipo. |
| S2: ejercicio RDD y ejecución diferida | Transformaciones, agregaciones y planes presentes; búsqueda en código/celdas sin `.rdd`, `parallelize` u operaciones RDD. | **Sin evidencia RDD** | S2 pide conversión y al menos dos operaciones RDD, además de explicar una fase/optimización del plan. No se añadió un ejercicio durante esta revisión. |
| S3: calidad aplicada | Esquemas explícitos; nulos/vacíos críticos marcados; imputación de predictores solo train; duplicados exactos y variantes por captura/reporte diagnosticados. Cero duplicados excedentes en este día; repeticiones temporales conservadas con justificación. | **Controles verificados** | Explicar por qué no deduplicar por estación/last_reported ni llenar cantidades ausentes con cero. Se usan `when`/elegibilidad e Imputer, no llamadas literales `.na.fill/.na.drop`; justificar la adaptación si se evalúa S3 literalmente. Reflexión y capturas personales pendientes. |
| S3: organización física | Gold histórico por fecha/partición temporal; Gold integrado por fecha/horizonte; conteos y lectura de vuelta guardados. | **Verificado; alcance limitado** | Mostrar cantidad de archivos y balance con el único día; no afirmar un ensayo multifecha. |
| S4: regularización alternativa | `CANDIDATOS`: ridge 0.01 y 0.1; además dos RF y persistencia. | **Parcial** | Hay **dos**, no tres, regularizaciones. RF y persistencia no constituyen una tercera. No se amplían los experimentos en esta revisión. |
| S4: otro algoritmo y explicación de variables | Dos RF ajustados/evaluados por combinación, con métricas guardadas; sin salida de `featureImportances`. | **Parcial** | Falta relevancia RF e interpretación. Los coeficientes LR ahora conservados no la sustituyen. |
| S4: utilidad del resultado | Error condicionado a cambios/discrepancias e interpretación de persistencia en informes. | **Parcial** | No hay umbral operativo acordado que demuestre aceptabilidad para redistribución de bicicletas ni evaluación futura independiente. |
| S5: demo y preguntas propias | Código, notebook, contratos y referencias disponibles localmente. | **Pendiente personal** | Preparar exposición de 8 minutos, demo de 5 y preguntas de 5. Esta revisión no comprueba que el estudiante pueda responder. |

Fuentes: [S1, §§3.7 y 4.4](https://262bigdata.github.io/lambda26/sesiones/S01_Arquitectura_Big_Data_Lambda_Kappa/),
[S2, §4.4](https://262bigdata.github.io/lambda26/sesiones/S02_Fundamentos_PySpark_Transformaciones_Lazy_Evaluation/),
[S3, §4.4](https://262bigdata.github.io/lambda26/sesiones/S03_Procesamiento_Calidad_Datos_Particionamiento/),
[S4, §§4.1 y 4.4](https://262bigdata.github.io/lambda26/sesiones/S04_ML_Distribuido_Regresion_Spark_MLlib/),
[S5, §4](https://262bigdata.github.io/lambda26/sesiones/S05_Evaluacion_Unidad_1/).

Las entregas individuales de sesión solicitan reflexiones propias; S4 agrega PDF
con nombre indicado y feedback personal. No se encontraron esos PDF en la dimensión.
No se fabrican respuestas ni se traslada automáticamente su formato a otra entrega.

## MLlib entrenado frente a persistencia seleccionada

El Gold fuente es `gold-v1-conteos-reportados`, en
`data/ejecuciones/20261117T142303450900Z/gold/aceptados`. Se mantienen los cortes
16:00/20:00 UTC del 10/11/2026 y fin exclusivo 11/11/2026 00:00 UTC. No hubo datos
nuevos. Los 12 ajustes LR y 12 RF originales están documentados con sus métricas
reales aunque sus objetos entrenados no se guardaran.

Al inventariar `data/modelos/` y `data/ejecuciones/` solo había contratos de
persistencia (`entrenamiento_necesario=false`), sin pipelines aprendidos.
Para conservar el componente entrenado exigido se recuperaron **solo seis
referencias**, una por problema, elegidas por MAE de validación ya registrado y
la política de empate original, restringiendo a candidatos MLlib. No se eligió
un horizonte global ni se usó prueba para seleccionar. No se repitieron los 24
candidatos ni se añadieron configuraciones.

| Tipo | Minutos | Referencia MLlib | MAE validación histórico | Ganador global conservado |
|---|---:|---|---:|---|
| Convencionales | 5 | lr_ridge_001 | 0.141018746969 | persistencia |
| Convencionales | 10 | lr_ridge_01 | 0.241178961033 | persistencia |
| Convencionales | 15 | lr_ridge_01 | 0.321189691655 | persistencia |
| Eléctricas | 5 | lr_ridge_001 | 0.278595939600 | persistencia |
| Eléctricas | 10 | lr_ridge_01 | 0.444443089058 | persistencia |
| Eléctricas | 15 | lr_ridge_01 | 0.571080868673 | persistencia |

Se ajustó exclusivamente cada candidato elegido con el mismo train. Las medianas
de su preprocesamiento coinciden con las históricas. Son recuperaciones nuevas,
no los objetos originales. Las diferencias en MAE/RMSE/R² históricos son menores
que 2e-12; no se afirma igualdad binaria entre entrenamientos nuevos.

Los seis PipelineModel incluyen transformaciones y orden de variables; cada
`referencia.json` registra parámetros, versiones, coeficientes, medianas, métricas
de validación/prueba, diferencias históricas y procedencia. Se guardan aparte:

```text
data/referencias_mllib/20261117T154118860932Z/
  manifiesto.json
  <tipo>_<h>min/
    referencia.json
    pipeline/
    predicciones_prueba/
```

Se recargaron los seis pipelines y se reprodujeron **677.376 predicciones**:
diferencia máxima 0.0, sin nulos, mismas claves y conteos. Los hashes del manifiesto,
selección y los 223 artefactos originales de fase 6 permanecen intactos.
**Persistencia sigue siendo ganadora y no requiere entrenamiento.** Una referencia
MLlib no debe presentarse como seleccionada para el sistema. La predicción final
aplica el clipping original mediante `src.modelado.predecir`, además del pipeline.

Comando de recuperación/reutilización, desde la raíz del repositorio:

```bash
docker exec -e 'PYSPARK_SUBMIT_ARGS=--driver-memory 3g pyspark-shell' -w /opt/UNIDAD1/Dim-Disponibilidad lambda26-pyspark python scripts/conservar_referencias_mllib.py --fuente data/ejecuciones/20261117T142303450900Z
```

Segunda ejecución comprobada: `modo=reutilizada_sin_fit`, recarga de las seis
referencias sin ningún ajuste. El script comprueba referencias compatibles antes
de entrenar. No accede a Supabase ni cambia Silver/Gold o resultados anteriores.

## Sustentación y capturas personales

Los seis aspectos se toman de la [tabla 4 oficial U1](https://262bigdata.github.io/lambda26/proyecto-sello/u1-producto/).

| Requisito | Evidencia concreta | Estado | Pendiente |
|---|---|---|---|
| Aporte propio | Responsable declarado y carpeta individual. | **Parcial** | Explicar decisiones y responder personalmente; demostrar autoría. |
| Explicación y orden | Informes disponibles; exposición no observada. | **Pendiente personal** | Ensayar claridad, vocabulario y tiempos. |
| Conducta y presentación | Sin observación presencial/grabación. | **Pendiente personal** | Verificación docente de puntualidad, presentación y actitud institucionales. |
| Repositorio y prácticas | Estructura organizada; dimensión todavía sin seguimiento en Git. | **Parcial** | Incorporación posterior autorizada al historial real; topics académicos remotos no comprobados. No fabricar commits ni fechas. |
| Documentación pública | Markdown local; `mkdocs.yml` solo enlaza documentos comunes centrados en duración. | **Parcial** | Publicar disponibilidad y comprobar URL/navegación. No hay evidencia de publicación de esta dimensión. |
| Introducción ejecutiva visual | Gráficos técnicos SVG; sin PPTX/PDF o enlace de pitch encontrado. | **Pendiente personal** | Preparar el pitch previo a la demo. Los gráficos no prueban una exposición. |

[S5, §4](https://262bigdata.github.io/lambda26/sesiones/S05_Evaluacion_Unidad_1/)
requiere reloj e identidad visibles sin recortar y coherencia con el historial
real. No se encontraron capturas que lo acrediten. Notebook, SVG y logs son
evidencia técnica, no sustitutos de capturas personales ni de defensa oral.

Capturar personalmente: arquitectura/pregunta, esquema, transformación/agregado
y plan, calidad antes/después, Parquet con lectura/filtro, comparación de tres
métricas, contrato de persistencia y referencia entrenada recargada. No mostrar
secretos. El estudiante debe formular sus reflexiones y responder con sus palabras.

## Prioridad de pendientes, sin ampliar el proyecto

1. Ejercicio RDD, explicación de ejecución diferida y prueba continua del notebook
   desde entorno limpio. La CLI no acredita por sí sola ese requisito del cuaderno.
2. Detalles S4: tercera regularización e importancia RF; presentar claramente el
   ganador de referencia aprendido frente a persistencia ganadora global.
3. Alinear cambio de dimensión en documentos comunes y argumento Lambda/Kappa.
4. Capturas propias, publicación, historial real, pitch y defensa personal.

Solo se revisó cumplimiento y se materializaron las referencias cuyos pesos
faltaban. No se repitió el pipeline ni la búsqueda completa; raw y ejecuciones
anteriores permanecen intactos. Sin descargas, U2, cambios Docker/Compose ni
modificaciones a dimensiones de compañeros. Sin commits/push.
