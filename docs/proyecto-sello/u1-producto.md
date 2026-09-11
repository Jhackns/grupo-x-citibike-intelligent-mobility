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

* Ruta Gold: `pyspark/jupyter/UNIDAD1/gold/demanda_historica_prediccion`
* Formato: **Parquet** (columnar, comprimido) — Capa Gold.
* Contenido: predicciones `trip_count` vs `prediction` por combinación temporal y
  segmento, listas para consumo del dashboard.

---

## 3. Dimensión 2: Utilización Histórica de Estaciones (Grimaldo Arredondo Martinez)

### 3.1 Pregunta de negocio

> ¿Cuáles son las **estaciones de mayor utilización** de la red Citi Bike y es posible
> **clasificarlas automáticamente** como estaciones de **alta o baja demanda** a partir
> de su volumen histórico de actividad?

La clasificación resultante permite priorizar reabastecimiento de flota, carga de
estacionamientos y mantenimiento en las estaciones críticas de la red.

### 3.2 Ingeniería de características

Se agrega por estación de origen/destino y se construyen variables operativas:

| Feature | Descripción | Tipo |
| :--- | :--- | :--- |
| `salidas` | Viajes iniciados en la estación | Numérica |
| `llegadas` | Viajes finalizados en la estación | Numérica |
| `visitas_totales` | `salidas + llegadas` (nivel de actividad) | Numérica |
| `lat_promedio / lng_promedio` | Ubicación promedio de la estación | Numérica |
| `dias_activos` | Días distintos con actividad | Numérica |
| `horas_activas` | Horas distintas del día con actividad | Numérica |
| `viajes_promedio_por_hora` | `salidas / horas_activas` (ritmo operativo) | Numérica |

La **variable dependiente (Y)** se construye con un **umbral de percentil 75**:

* **Umbral P75 de `visitas_totales` = 5,854.00** visitas.
* Estaciones **alta demanda** (`label=1`): **599**; **baja demanda** (`label=0`): **1,706**
  (total: **2,305** estaciones analizadas).

### 3.3 Pipeline de Machine Learning en Spark MLlib

* `VectorAssembler` con las 5 variables númericas finales → columna `features`.
* Partición **Train (80%) / Test (20%)** con `randomSplit([0.8, 0.2], seed=42)`:
  **1,901** / **404**.
* Modelos candidatos: **LogisticRegression** y **RandomForestClassifier**,
  seleccionados por **AUC-ROC** sobre test.

```text
AUC LogisticRegression : 0.9988
AUC RandomForest       : 0.9987
>> Modelo seleccionado por AUC: LogisticRegression
```

### 3.4 Resultados cuantitativos y métricas formales

| Métrica | Valor obtenido | Interpretación |
| :--- | ---: | :--- |
| **Accuracy** | 0.9851 | 98.5% de estaciones bien clasificadas |
| **Precision** | 0.9852 | 98.5% de las altas-demanda predichas son reales |
| **Recall** | 0.9851 | 98.5% de las altas-demanda reales fueron detectadas |
| **F1-Score** | 0.9851 | Balance armónico precision/recall |
| **AUC-ROC** | 0.9988 | Capacidad discriminante casi perfecta |

**Matriz de confusión (Test, 404 estaciones):**

```text
+-----+----------+-----+
|label|prediction|count|
+-----+----------+-----+
|  0.0|       0.0|  297|   (True Negatives)
|  0.0|       1.0|    1|   (False Positives)
|  1.0|       0.0|    5|   (False Negatives)
|  1.0|       1.0|  101|   (True Positives)
+-----+----------+-----+
```

### 3.5 Análisis crítico de negocio

* El modelo distingue con altísima confiabilidad las estaciones de alta demanda:
  la clasificación es estable y repetible sobre el histórico completo.
* Las estaciones mejor clasificadas corresponden a nodos de transporte
  concurridos (plazas y avenidas principales de Manhattan), lo que valida la
  coherencia del resultado con la operación real.
* **Utilidad:** mapear estaciones críticas para balanceo de flota y expandir la
  clasificación con datos en tiempo real en la dimensión streaming (U2).

### 3.6 Artefacto persistido

* Ruta Gold: `pyspark/jupyter/UNIDAD1/gold/estaciones_predictions.parquet`
* Formato: **Parquet** (columnar, comprimido) — Capa Gold.
* Contenido: por estación — `estacion_id`, `estacion_nombre`, volúmenes, `label`,
  `prediction`, `prob_alta_demanda` y `ranking` global de utilización.

---

## 4. Dimensión 3: Análisis y Modelado de Duración de Viajes (Jose Miguel Condo Huamani)

### 4.1 Pregunta de negocio

> ¿Qué **duración** en minutos puede esperarse para un viaje dado el contexto:
> hora, día, tipo de usuario, tipo de bicicleta y **distancia aproximada** recorrida?

Estimarla permite dimensionar la rotación de la flota, detectar viajes anómalos
y planificar la redistribución de bicicletas entre estaciones.

### 4.2 Ingeniería de características

* **Y:** `duration_minutes` (cálculo directo: `(ended_at - started_at) / 60`),
  restringida al filtro Silver **1–180 min**.
* Dataset de modelado: **4,971,847** viajes válidos.
* Estadísticas: duración media **≈ 12.90 min** (σ ≈ 11.93); distancia media
  **≈ 2.09 km**.

| Feature | Descripción | Tipo |
| :--- | :--- | :--- |
| `start_hour` | Hora de inicio (0–23) | Numérica |
| `day_of_week` | Día de la semana (1–7 ISO) | Numérica |
| `is_weekend` | Binario (1 = fin de semana) | Binaria |
| `rideable_type` | Tipo de bicicleta (clásica/eléctrica) | Categórica |
| `member_casual` | Usuario miembro u ocasional | Categórica |
| `approx_distance_km` | Distancia euclidiana aproximada (`lat/lng → km`) | Numérica |

### 4.3 Pipeline de Machine Learning en Spark MLlib

* `StringIndexer` + codificación de categóricas y `VectorAssembler` → `features`.
* Partición **Train (80%) / Test (20%)** (seed=42): **3,978,452** / **993,395**.
* Modelos candidatos: **LinearRegression** y **RandomForestRegressor**,
  seleccionados por **menor RMSE**.

```text
+---------------------+----------+---------+------+
|Modelo               |RMSE (min)|MAE (min)|R2    |
+---------------------+----------+---------+------+
|LinearRegression     |9.2532    |4.6383   |0.3962|
|RandomForestRegressor|9.0613    |4.6707   |0.4209|
+---------------------+----------+---------+------+
>> Modelo seleccionado por menor RMSE: RandomForestRegressor
```

### 4.4 Resultados cuantitativos y métricas formales

| Métrica | Valor obtenido (RandomForest) | Interpretación |
| :--- | ---: | :--- |
| **RMSE** | 9.0613 min | Error típico en minutos por viaje |
| **MAE** | 4.6707 min | Error absoluto medio en minutos |
| **R²** | 0.4209 | Varianza explicada por las variables del contexto |

### 4.5 Análisis crítico de negocio

* El error medio (~4.7 min sobre viajes de ~13 min) es aceptable para usos
  operativos gruesos, y el R² ≈ 0.42 supera al modelo puramente lineal, lo que
  demuestra el aporte del componente no lineal (bosques aleatorios).
* **Limitaciones:** la distancia aproximada es euclidiana (no la ruta real) y no
  se usan condiciones de tráfico/clima; incorporarlas mejoraría la precisión.
* **Utilidad:** presupuestar rotación de flota por franja horaria y detectar
  desviaciones (viajes improductivos o fallas) comparando duración real vs estimada.

### 4.6 Artefacto persistido

* Ruta Gold: `pyspark/jupyter/UNIDAD1/gold/duracion_predictions.parquet`
* Formato: **Parquet** (columnar, comprimido) — Capa Gold.
* Contenido: por viaje del conjunto de test — contexto, `duration_minutes` real y
  `prediction` del modelo.

---

## 5. Dimensión 4: Patrones Espaciales y Concentración (Cristhian Chuquitarqui Chura)

### 5.1 Pregunta de negocio

> ¿Cómo se **concentra geográficamente** la demanda de viajes en la ciudad y qué
> **nivel de actividad** por cuadrante/hora puede predecirse para dimensionar la
> red?

El modelo identifica **zonas de alta actividad** y su evolución horaria para la
ubicación estratégica de estaciones y la logística de redistribución.

### 5.2 Ingeniería de características (cuadrícula geográfica)

* Se discretiza el espacio en **cuadrantes de ≈ 0.01°** (lat/lng redondeados a
  2 decimales) y se combina con la **hora** de inicio.
* Identificador reproducible: `cell_key = "<cell_lat>_<cell_lng>"`.
* Variable objetivo: `trip_count` por `(cell_key, start_hour)`.

| Feature | Descripción |
| :--- | :--- |
| `cell_lat` | Latitud del cuadrante (redondeada a 2 decimales) |
| `cell_lng` | Longitud del cuadrante (redondeada a 2 decimales) |
| `start_hour` | Hora de inicio (0–23) |
| `day_of_week` | Día de la semana (1–7 ISO) |
| `trip_count` | **Y**: viajes observados en el cuadrante/hora |

* Total de **celdas/hora analizadas: 13,520**. `trip_count` observado: rango
  **1 a 13,786**; media **≈ 367.74**.

### 5.3 Pipeline de Machine Learning en Spark MLlib

* `VectorAssembler` con las coordenadas del cuadrante, hora y día → `features`.
* Partición **Train (80%) / Test (20%)** (seed=42): **10,867** / **2,653**.
* Modelo: **RandomForestRegressor** (`labelCol="trip_count"`).

### 5.4 Resultados cuantitativos y métricas formales

| Métrica | Valor obtenido | Interpretación |
| :--- | ---: | :--- |
| **RMSE** | 345.0841 viajes | Error típico en viajes por celda/hora |
| **MAE** | 135.5580 viajes | Error absoluto medio en viajes por celda/hora |
| **R²** | 0.8281 | Varianza explicada por la ubicación y hora |

### 5.5 Análisis crítico de negocio

* El **R² ≈ 0.83** es notablemente alto: la **ubicación (cuadrante) y la hora**
  explican la mayor parte de la variación de la demanda, validando la existencia
  de **patrones espaciales estables** en la red.
* El **MAE ≈ 136 viajes** sobre celdas con actividad media de ~368 viajes y picos
  de 13,786 indica buena precisión relativa incluso en zonas de alta demanda.
* **Utilidad:** alimenta el mapa de calor de la red (dashboard) y la planificación
  de **ubicación de nuevas estaciones** y **rebalanceo** de flota por zona/hora.

### 5.6 Artefacto persistido

* Ruta Gold: `pyspark/jupyter/UNIDAD1/gold/espacial_predictions.parquet`
* Formato: **Parquet** (columnar, comprimido) — Capa Gold.
* Contenido: por cuadrante/hora — coordenadas, `trip_count` real y `prediction`,
  listas para el consumo del mapa de calor del dashboard.

---

## 6. Criterios de Calidad, Trazabilidad y Reproducibilidad

### 6.1 Entorno reproducible (Docker / WSL2)

El pipeline corre dentro del contenedor `lambda26-pyspark` (`docker compose up -d`),
lo que garantiza un entorno idéntico entre desarrolladores:

| Componente | Versión |
| :--- | :--- |
| **PySpark** | 4.2.0 |
| **Python** | 3.10 |
| **Java (JRE)** | OpenJDK 21 |
| **Jupyter / Notebook** | Book 7.x / Notebook 7.x |
| **Sistema** | Linux (Ubuntu Slim) en WSL2 |

### 6.2 Trazabilidad del pipeline

* Cada etapa medallón deja un artefacto persistido y verificable
  (Parquet en la Capa Gold).
* Semilla fija (`seed=42`) para reproducibilidad de la partición Train/Test.
* Esquema explícito documentado en el propio notebook.

### 6.3 Verificación de ejecución

* Los **4 cuadernos** de las dimensiones del equipo se ejecutaron **en orden
  secuencial sin errores** (código de salida 0): 

| Cuaderno | Celdas código | Errores | Modelo seleccionado |
| :--- | ---: | ---: | :--- |
| `Dim-Demanda-historica/demanda_historica_prediccion.ipynb` | 22 | 0 | LinearRegression |
| `Dim-Estaciones/estaciones_prediccion.ipynb` | 21 | 0 | LogisticRegression |
| `Dim-Duracion-de-viajes/citibike_dim.ipynb` | 17 | 0 | RandomForestRegressor |
| `Dim-Espacial/espacial_prediccion.ipynb` | 18 | 0 | RandomForestRegressor |

* Todas las celdas son atómicas, con validaciones intermedias
  (`show(5)`, `count()`, `printSchema()`).
* Las métricas de calidad de las secciones 2 a 5 corresponden a la **ejecución real**
  de cada pipeline sobre el dataset completo de Citi Bike NYC (julio 2026).