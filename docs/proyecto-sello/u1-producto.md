# Producto de Unidad 1 (U1): Procesamiento Batch y Modelado Predictivo Histórico

## Portada y Metadatos del Equipo

| Campo | Valor |
| :--- | :--- |
| **Proyecto** | CitiBike Intelligent Mobility |
| **Equipo** | Data Geniuses |
| **Sección** | S1 |
| **Repositorio** | `https://github.com/Jhackns/grupo-x-citibike-intelligent-mobility` |
| **Producto** | Unidad 1 — Procesamiento Batch y Modelado Predictivo sobre histórico |
| **Dataset** | Citi Bike NYC Tripdata (5 CSVs, 4,993,137 registros iniciales) |

### Integrantes y roles

| Integrante | Rol / énfasis | Dimensión U1 |
| :--- | :--- | :--- |
| **Harry Jack Ascuna Mamani** | Batch / Spark / Demanda histórica | Demanda histórica y predicción temporal |
| **Grimaldo Arredondo Martinez** | Streaming / Kafka / Estaciones | Utilización histórica de estaciones |
| **Jose Miguel Condo Huamani** | BI / ML / Duración | Análisis y modelado de duración de viajes |
| **Cristhian Chuquitarqui Chura** | Observabilidad / Grafana / Espacial | Patrones espaciales y concentración |

---

## 1. Arquitectura de Datos y Capas Medallón (Común del Equipo)

El equipo adopta una arquitectura **Lambda** con un pipeline batch (U1) y un pipeline
streaming (U2). La capa batch se implementa con **PySpark** y sigue el patrón de
**capas medallón (Bronze → Silver → Gold)** para garantizar trazabilidad, calidad y
reproducibilidad de cada transformación.

### 1.1 Capa Bronze — Ingesta raw

* Ingesta de los **5 CSVs** de Citi Bike NYC con `StructType` **explícito**, evitando
  la inferencia de esquema y tipando correctamente cada columna (`StringType`,
  `TimestampType`, `DoubleType`).
* Lectura distribuida con `spark.read.schema(...).option("header", True)` desde
  `/opt/UNIDAD1/data/*.csv`.
* Los datos de la capa Bronze se mantienen **inalterados** como fuente de trazabilidad.

```python
schema_citibike = StructType([
    StructField("ride_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("ended_at", TimestampType(), True),
    # ... start_station, end_station, coordenadas y member_casual
])

df = spark.read.schema(schema_citibike).option("header", True).csv(DATA_PATH)
```

### 1.2 Capa Silver — Limpieza y calidad

* Cálculo de `duration_minutes = (ended_at - started_at) / 60.0`.
* **Filtro de duración válida:** viajes entre **1.0 y 180.0 minutos** (se descartan
  outliers y viajes con duración no física).
* **Tratamiento de nulos:** descarte de registros con nulos en campos clave
  (`start_station_name`, `end_station_name`, `rideable_type`, `member_casual`).
* **Deduplicación** de viajes por `ride_id` para evitar conteos dobles.

> Resultado Silver: de **4,993,137** registros ingeridos quedan **4,972,819** viajes
> válidos (20,318 descartados por calidad).

### 1.3 Capa Gold — Agregaciones y persistencia analítica

* Agregaciones por combinaciones de interés (fecha, hora, tipo de usuario, tipo de
  bicicleta, estación, zona).
* Persistencia en **Parquet** (formato columnar y comprimido) particionado para
  consumo eficiente por los reportes y el dashboard común.

---

## 2. Dimensión 1: Demanda Histórica y Predicción Temporal (Harry Jack Ascuna Mamani)

### 2.1 Pregunta de negocio

> ¿Cómo varía históricamente la demanda de viajes según **hora, día y tipo de usuario**,
> y qué **cantidad de viajes** puede esperarse bajo estas condiciones?

La dimensión permite **anticipar intervalos de alta y baja demanda** para dimensionar
flota, planificar la operación y preparar el modelo de inferencia en streaming (U2).

### 2.2 Ingeniería de características

Se extraen factores temporales a partir de `started_at` y se agrega la demanda:

| Feature | Descripción | Tipo |
| :--- | :--- | :--- |
| `trip_date` | Fecha del viaje | Fecha |
| `start_hour` | Hora de inicio (0–23) | Numérica |
| `day_of_week` | Día de la semana (1–7, ISO) | Numérica |
| `is_weekend` | Binario (1 = fin de semana, 0 = entre semana) | Binaria |
| `member_casual` | Tipo de usuario | Categórica |
| `rideable_type` | Tipo de bicicleta | Categórica |

La **variable dependiente (Y)** se define mediante la agregación:

```python
df_demanda = (
    df_feat.groupBy("trip_date", "start_hour", "day_of_week", "is_weekend",
                    "member_casual", "rideable_type")
           .agg(count("*").alias("trip_count"))
)
```

* Total de combinaciones (filas del dataset modelable): **2,983**.
* `trip_count` observado: rango **1 a 12,186** viajes; media **≈ 1,667.05**.

### 2.3 Pipeline de Machine Learning en Spark MLlib

* `StringIndexer` + `OneHotEncoder` para las categóricas (`member_casual`,
  `rideable_type`).
* `VectorAssembler` para empaquetar las variables temporales y categóricas
  codificadas en la columna `features`.
* Partición **Train (80%) / Test (20%)** con `randomSplit([0.8, 0.2], seed=42)`
  reproducible.
* Modelo: **LinearRegression** (regresor distribuido de Spark MLlib) con
  `labelCol="trip_count"`.

```python
lr = LinearRegression(featuresCol="features", labelCol="trip_count")
model = lr.fit(train)

predicciones = model.transform(test)
```

### 2.4 Resultados cuantitativos y métricas formales

| Métrica | Valor obtenido | Interpretación |
| :--- | ---: | :--- |
| **RMSE** | 1439.6690 | Error cuadrático medio raíz en viajes por intervalo |
| **MAE** | 1028.4467 | Error absoluto medio en viajes por intervalo |
| **R²** | 0.4805 | Varianza explicada por los ciclos temporales |

```text
====================================================
MÉTRICAS DE CALIDAD DEL MODELO (Capa Silver/Gold)
====================================================
  RMSE :  1439.6690  (Root Mean Squared Error)
  MAE  :  1028.4467  (Mean Absolute Error)
  R2   :     0.4805  (Coeficiente de determinación)
====================================================
```

### 2.5 Análisis crítico de negocio

* El **R² ≈ 0.48** indica que **~48% de la varianza de la demanda** se explica
  mediante ciclos temporales (hora, día de semana, fin de semana) y la segmentación
  por tipo de usuario y bicicleta. Es un resultado razonable para un modelo
  **puramente temporal**.
* **Limitaciones reconocidas:** no se incluyen variables exógenas como **clima /
  lluvia**, eventos especiales ni disponibilidad de bicicletas, que podrían elevar el
  poder explicativo del modelo.
* **Utilidad operativa:** pese al error absoluto (~1,028 viajes), el modelo es útil
  para **dimensionar flota** y detectar **periodos críticos estable**; el detalle fino
  debe complementarse con las dimensiones por estación y espacial del equipo.

### 2.6 Artefacto persistido

* Ruta Gold: `pyspark/jupyter/UNIDAD1/gold/demanda_predictions.parquet`
* Formato: **Parquet** (columnar, comprimido) — Capa Gold.
* Contenido: predicciones `trip_count` vs `prediction` por combinación temporal y
  segmento, listas para consumo del dashboard.

---

## 3. Dimensión 2: Utilización Histórica de Estaciones (Grimaldo Arredondo Martinez)

### 3.1 Pregunta de negocio

> ¿Qué estaciones concentran históricamente la mayor demanda y cuáles presentan mayor
> probabilidad de registrar niveles elevados de utilización?

Esta dimensión mira la pregunta central del equipo desde los puntos físicos de origen y
destino de los viajes: identifica qué estaciones son críticas hoy (ranking descriptivo) y
cuáles tienen mayor probabilidad de tener un **día de alta demanda** en el futuro
(clasificación), para apoyar la asignación de cuadrillas de rebalanceo y la planificación
operativa.

### 3.2 Ingeniería de características y variable objetivo

Los viajes se agregan a granularidad **estación-día** (no estación-viaje), porque la
decisión operativa relevante es diaria:

| Feature | Descripción | Tipo |
| :--- | :--- | :--- |
| `day_of_week` | Día de la semana (1–7) | Categórica (OHE) |
| `is_weekend` | Binario (1 = fin de semana) | Binaria |
| `member_ratio` | Proporción de viajes de socios (`member`) ese día en esa estación | Numérica |
| `electric_ratio` | Proporción de viajes en bicicleta eléctrica ese día en esa estación | Numérica |

* **Variable objetivo (`es_alta_demanda`):** binaria; 1 si `viajes_totales_dia` de esa
  estación-día está en el **cuartil superior** (percentil 75), 0 en caso contrario.
* **Umbral estadístico y reproducible:** el percentil 75 se calcula con
  `percentile_approx` **únicamente sobre el período de entrenamiento** (primeros 24 de
  32 días del rango disponible) y se aplica igual al conjunto de prueba, evitando fuga
  de información desde el futuro hacia la definición de la etiqueta.
* **Sin circularidad:** el modelo no usa el propio conteo de viajes como predictor —
  predice la probabilidad de un día de alta demanda a partir del *patrón* de uso
  (día, fin de semana, mezcla de usuario y de bicicleta), información conocida con
  antelación.
* Total estaciones con viajes iniciados: **2,305**. Filas del dataset estación-día:
  **70,388** (52,285 entrenamiento / 18,103 prueba, split temporal).
* Umbral de alta demanda (P75 en train): **93 viajes/día**. Balance de clases:
  18,362 días de alta demanda vs. 52,026 de demanda regular.

### 3.3 Calidad de datos aplicada

| Control | Resultado |
| :--- | ---: |
| Filas Bronze ingeridas | 4,993,137 |
| Filas tras filtrar `start_station_id` nulo | 4,990,109 |
| Duplicados por `ride_id` eliminados | 0 (verificado con `dropDuplicates`) |
| Nulos en `end_station_id` / `end_station_name` descartados | 14,702 / 13,727 |
| Filas Silver finales (sin nulos de estación) | 4,975,407 |

El conteo de viajes iniciados por estación se calculó por dos caminos independientes —
`groupBy().agg()` sobre DataFrame y `map`/`reduceByKey` sobre el RDD subyacente— y
ambos coincidieron exactamente (4,990,109 viajes), confirmando la correctitud de la
agregación distribuida.

### 3.4 Pipeline de Machine Learning en Spark MLlib

* `StringIndexer` + `OneHotEncoder` sobre `day_of_week` (variable cíclica, no ordinal).
* `VectorAssembler` combina `day_of_week_ohe`, `is_weekend`, `member_ratio` y
  `electric_ratio` en `features`.
* **Split temporal** (no aleatorio): entrenamiento con los primeros 24 días, prueba con
  los últimos 8 — el modelo nunca ve datos del futuro al entrenar.
* Tres configuraciones comparadas con las mismas tres métricas
  (`areaUnderROC`, `f1`, `accuracy`):

```python
lr_base = LogisticRegression(featuresCol="features", labelCol="es_alta_demanda", regParam=0.0)
lr_reg  = LogisticRegression(featuresCol="features", labelCol="es_alta_demanda", regParam=0.1)
rf      = RandomForestClassifier(featuresCol="features", labelCol="es_alta_demanda", numTrees=50, maxDepth=6)
```

### 3.5 Resultados cuantitativos y métricas formales

| Modelo | AUC | F1 | Accuracy |
| :--- | ---: | ---: | ---: |
| LogisticRegression (sin regularización) | 0.6386 | 0.5894 | 0.7026 |
| LogisticRegression (L2, regParam=0.1) | 0.6352 | 0.5932 | 0.7109 |
| **RandomForestClassifier (50 árboles)** | **0.7861** | **0.5947** | **0.7139** |

```text
Modelo ganador: RandomForestClassifier (50 arboles)
  AUC      : 0.7861
  F1       : 0.5947
  Accuracy : 0.7139
```

El modelo ganador se persistió en `/opt/UNIDAD1/gold/modelo_estaciones_alta_demanda` y
se verificó recargándolo desde disco: el AUC recalculado (0.7861) coincide exactamente
con el de la evaluación original.

### 3.6 Análisis crítico de negocio

* El **RandomForestClassifier supera claramente a ambas regresiones logísticas en AUC**
  (0.7861 vs. ~0.64), lo que indica que la relación entre el patrón de uso diario
  (día de la semana, mezcla de usuario/bicicleta) y la probabilidad de alta demanda es
  **no lineal** — el bosque captura interacciones (p. ej. fin de semana × alta
  proporción de bicicleta eléctrica) que un modelo lineal no puede.
* **Accuracy ≈ 71%** con clases desbalanceadas (26% de días son de alta demanda) es un
  resultado razonable pero mejorable; el F1 (~0.59) muestra que aún hay margen para
  reducir falsos negativos, relevante porque el costo operativo de *no* anticipar un
  día de alta demanda (estación vacía, usuarios varados) suele ser mayor que el de una
  alerta de más.
* **Limitación reconocida:** el modelo usa solo señales propias del histórico de la
  estación; no incorpora clima ni eventos especiales (fuera de alcance del Brief S2).
* **Utilidad operativa:** el ranking de estaciones (Top 1: *Pier 61 at Chelsea Piers*,
  18,220 viajes iniciados, 0.37% de participación) y la clasificación de alta demanda
  permiten priorizar qué estaciones monitorear primero para rebalanceo.

### 3.7 Artefactos persistidos (Capa Gold)

* `pyspark/jupyter/UNIDAD1/gold/estaciones_utilizacion/` — Parquet particionado por
  `es_alta_demanda` (70,388 filas escritas y reconciliadas al leer de vuelta;
  `PartitionFilters` confirmado en el plan físico).
* `pyspark/jupyter/UNIDAD1/gold/estaciones_ranking/` — ranking descriptivo por estación.
* `pyspark/jupyter/UNIDAD1/gold/modelo_estaciones_alta_demanda/` — `PipelineModel`
  ganador (RandomForestClassifier), guardado con `.write().overwrite().save(...)`.
* Notebook: `pyspark/jupyter/UNIDAD1/Dim-Estaciones/utilizacion_estaciones.ipynb`,
  ejecutado de punta a punta sin errores sobre el dataset completo (4,993,137 registros).

### 3.8 Dimensiones Pendientes de los Integrantes (Placeholders)

#### Dimensión 3: Análisis y Modelado de Duración de Viajes

* **Responsable:** Jose Miguel Condo Huamani
* **Estado:** Pendiente de completar.

<!-- SECCIÓN PENDIENTE DE COMPLETAR POR JOSE MIGUEL CONDO: Incluir regresión de duration_minutes, distribuciones y métricas de error -->

#### Dimensión 4: Patrones Espaciales y Concentración

* **Responsable:** Cristhian Chuquitarqui Chura
* **Estado:** Pendiente de completar.

<!-- SECCIÓN PENDIENTE DE COMPLETAR POR CRISTHIAN CHUQUITARQUI: Incluir clústeres espaciales/cuadrantes y evaluación de nivel de actividad -->

---

## 4. Criterios de Calidad, Trazabilidad y Reproducibilidad

### 4.1 Entorno reproducible (Docker / WSL2)

El pipeline corre dentro del contenedor `lambda26-pyspark` (`docker compose up -d`),
lo que garantiza un entorno idéntico entre desarrolladores:

| Componente | Versión |
| :--- | :--- |
| **PySpark** | 4.2.0 |
| **Python** | 3.10 |
| **Java (JRE)** | OpenJDK 21 |
| **Jupyter / Notebook** | Book 7.x / Notebook 7.x |
| **Sistema** | Linux (Ubuntu Slim) en WSL2 |

### 4.2 Trazabilidad del pipeline

* Cada etapa medallón deja un artefacto persistido y verificable
  (Parquet en la Capa Gold).
* Semilla fija (`seed=42`) para reproducibilidad de la partición Train/Test.
* Esquema explícito documentado en el propio notebook.

### 4.3 Verificación de ejecución

* El cuaderno `pyspark/jupyter/UNIDAD1/Dim-Demanda-historica/demanda_historica_prediccion.ipynb`
  se ejecutó **en orden secuencial sin errores** (código de salida 0), celdas atómicas
  con validaciones intermedias (`show(5)`, `count()`, `printSchema()`).
* Métricas de calidad capturadas en sección 2.4 corresponden a la ejecución real del
  pipeline sobre el dataset completo de Citi Bike NYC (julio 2026).