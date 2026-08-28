* **Nombre del equipo:** Data Geniuses
* **Sección:** S1
* **Repositorio (URL):** `____________________________`
* **Topics del repositorio configurados (sí/no):** No, pendiente de creación del repositorio
* **Topic requerido:** `grupo-____-citibike-intelligent-mobility`

### Integrantes:

| Integrante | Rol o énfasis previsto |
| :--- | :--- |
| **Harry Jack Ascuna Mamani** | Batch / Spark / preparación de datos |
| **Grimaldo Arredondo Martinez** | Streaming / Kafka / Structured Streaming |
| **Jose Miguel Condo Huamani** | BI / Machine Learning |
| **Cristhian Chuquitarqui Chura** | Observabilidad / Grafana / integración |

> *Nota:* Estos roles representan énfasis de coordinación. Cada integrante desarrollará de extremo a extremo su dimensión U1 y su dimensión U2.

---

## 2. Dominio del proyecto

* **Nombre del proyecto:** CitiBike Intelligent Mobility
* **Problema o necesidad que resuelve:** Citi Bike genera grandes volúmenes de registros de viajes, estaciones, horarios, tipos de bicicleta y usuarios. El proyecto busca transformar esos datos en indicadores y predicciones que permitan reconocer patrones de demanda, anticipar niveles de utilización y apoyar el monitoreo y la planificación operativa del sistema.
* **Dominio de datos:** Movilidad urbana inteligente y sistemas de bicicletas compartidas. Se analizan viajes, horarios, duración, estaciones, coordenadas, tipo de bicicleta y tipo de usuario.
* **Pregunta central de negocio:** ¿Cómo analizar y anticipar la demanda y el comportamiento operativo de los viajes de Citi Bike NYC a partir de información histórica y eventos procesados en tiempo real, para apoyar el monitoreo y la planificación del sistema de movilidad compartida?
* **Usuarios / actores principales:** Analistas de movilidad urbana, responsables de operación y planificación, analistas de datos y responsables de monitoreo operativo.
* **Arquitectura Big Data prevista - Lambda o Kappa, y por qué:** **Lambda.** Se requiere combinar una capa batch para procesar el histórico con PySpark y entrenar modelos predictivos, con una capa streaming que reproduzca eventos en Kafka y los procese mediante Spark Structured Streaming. Las U1 se reportarán en notebooks y las U2 en un Grafana común.
* **Fuente de datos batch:** Registros públicos de viajes de Citi Bike NYC. El equipo ya trabaja con cinco archivos CSV que suman 4,993,137 registros. El esquema incluye `ride_id`, `rideable_type`, `started_at`, `ended_at`, estaciones e IDs de origen/destino, coordenadas y `member_casual`.
* **Fuente de eventos en tiempo real:** Streaming simulado de forma realista a partir de los mismos registros de Citi Bike, ordenados cronológicamente y publicados progresivamente en un tópico Kafka propuesto como `citibike-rides`.
* **¿Continúa un proyecto de un ciclo anterior, o es un dominio nuevo?:** Es un proyecto nuevo de este ciclo.

---

## 3. Dimensiones de análisis y fuentes previstas

Cada integrante propone dos dimensiones de la misma pregunta central: una U1 predictiva sin inferencia en tiempo real y una U2 predictiva con inferencia en streaming. Todas convergen en una sola historia final.

| Integrante | Tipo | Dimensión (como pregunta) | Indicador | Fuente batch | Fuente streaming |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Harry Jack Ascuna Mamani** | Predictiva (U1, batch) | ¿Cómo varía la demanda histórica y qué cantidad de viajes puede esperarse según condiciones temporales? | Viajes por intervalo; demanda estimada | Histórico Citi Bike | Eventos Citi Bike acumulados |
| **Harry Jack Ascuna Mamani** | Predictiva (U2, streaming) | ¿Cuál será la demanda durante la siguiente ventana temporal según el flujo reciente? | Demanda actual y proyectada | Histórico para entrenamiento | Kafka `citibike-rides` |
| **Grimaldo Arredondo Martinez** | Predictiva (U1, batch) | ¿Qué estaciones concentran mayor demanda y cuáles presentan mayor probabilidad de alta utilización? | Viajes/estación; probabilidad de alta demanda | Histórico Citi Bike | Eventos Citi Bike acumulados |
| **Grimaldo Arredondo Martinez** | Predictiva (U2, streaming) | ¿Qué estaciones podrían registrar mayor demanda durante la siguiente ventana temporal? | Demanda actual y proyectada por estación | Histórico por estación | Kafka `citibike-rides` |
| **Jose Miguel Condo Huamani** | Predictiva (U1, batch) | ¿Cómo varía la duración de los viajes y qué duración puede esperarse según sus características? | Duración media; duración estimada | Histórico Citi Bike | Eventos Citi Bike acumulados |
| **Jose Miguel Condo Huamani** | Predictiva (U2, streaming) | ¿Cómo evoluciona la duración reciente y qué duración media puede esperarse en la siguiente ventana? | Duración actual y proyectada | Histórico de duración | Kafka `citibike-rides` |
| **Cristhian Chuquitarqui Chura** | Predictiva (U1, batch) | ¿Qué zonas concentran la actividad y cuáles presentan mayor probabilidad de alta actividad futura? | Viajes/zona; nivel estimado | Histórico espacial Citi Bike | Eventos espaciales acumulados |
| **Cristhian Chuquitarqui Chura** | Predictiva (U2, streaming) | ¿Qué zonas presentan mayor concentración actual y cuáles podrían concentrar más actividad en la siguiente ventana? | Actividad actual y proyectada por zona | Histórico espacial | Kafka `citibike-rides` |

---

### Dimensión: Demanda histórica y predicción (integrante: Harry Jack Ascuna Mamani)
* **Tipo:** predictiva sin tiempo real (U1, batch)
* **Dimensión y relación con la pregunta central:** ¿Cómo varía históricamente la demanda de viajes de Citi Bike según hora, día y tipo de usuario, y qué cantidad de viajes puede esperarse bajo determinadas condiciones temporales? Se relaciona con la pregunta central porque permite comprender y anticipar el nivel de utilización general del sistema.
* **Indicador(es):** Número de viajes por hora y por día. Demanda por tipo de usuario y bicicleta. Volumen de viajes estimado por el modelo.
* **Decisión o acción que habilita:** Identificar periodos históricamente críticos y anticipar intervalos de alta o baja demanda para apoyar la planificación operativa.
* **Instrumento/fuente batch:** Registros históricos Citi Bike. Campos clave: `ride_id`, `started_at`, `rideable_type` y `member_casual`.
* **Instrumento/fuente streaming:** Los mismos eventos Citi Bike usados por U2, acumulados como histórico y procesados por lotes.
* **Modelo predictivo:** Regresión sobre variables temporales y categóricas. Variable objetivo: cantidad de viajes por intervalo. Corre al ejecutar el pipeline batch.
* **Salida:** Notebook Jupyter/PySpark con tablas, gráficos, demanda real, demanda predicha y métricas de error.
* **¿Con qué otra(s) dimensión(es) del equipo se combina?:** Demanda U2, utilización por estación y patrones espaciales.
* **Lista inicial de requisitos:**
    1. El sistema debe agregar los viajes históricos por intervalos temporales definidos.
    2. El sistema debe calcular indicadores de demanda segmentados por hora, día y tipo de usuario.
    3. El sistema debe entrenar y evaluar un modelo capaz de estimar la cantidad de viajes esperada.

---

### Dimensión: Demanda en tiempo real (integrante: Harry Jack Ascuna Mamani)
* **Tipo:** predictiva con tiempo real (U2, streaming)
* **Dimensión y relación con la pregunta central:** ¿Cuál será la demanda de viajes de Citi Bike durante la siguiente ventana temporal a partir del comportamiento observado en el flujo reciente de eventos? Extiende la dimensión de demanda histórica hacia el monitoreo y pronóstico en vivo.
* **Indicador(es):** Viajes recibidos por minuto o ventana. Tendencia o media móvil de demanda. Cantidad de viajes proyectada para la siguiente ventana.
* **Decisión o acción que habilita:** Detectar incrementos de demanda y anticipar periodos de utilización elevada.
* **Instrumento/fuente batch:** Histórico Citi Bike utilizado para entrenamiento del modelo temporal.
* **Instrumento/fuente streaming:** Eventos publicados en Kafka y consumidos mediante Spark Structured Streaming.
* **Modelo predictivo:** Modelo de series de tiempo sobre demanda agregada por ventanas. La inferencia se actualiza con el flujo.
* **Salida:** Panel del Grafana común con demanda actual, serie reciente y demanda pronosticada.
* **¿Con qué otra(s) dimensión(es) del equipo se combina?:** Demanda por estación y actividad espacial.
* **Lista inicial de requisitos:**
    1. El sistema debe consumir continuamente eventos Citi Bike desde Kafka.
    2. El sistema debe calcular la demanda en ventanas temporales mediante Structured Streaming.
    3. El sistema debe mostrar en Grafana el valor observado y el pronosticado.

---

### Dimensión: Utilización histórica de estaciones (integrante: Grimaldo Arredondo Martinez)
* **Tipo:** predictiva sin tiempo real (U1, batch)
* **Dimensión y relación con la pregunta central:** ¿Qué estaciones concentran históricamente la mayor demanda y cuáles presentan mayor probabilidad de registrar niveles elevados de utilización? Analiza la pregunta central desde los puntos físicos de origen y destino de los viajes.
* **Indicador(es):** Viajes iniciados y finalizados por estación. Participación y ranking por estación. Probabilidad estimada de alta demanda.
* **Decisión o acción que habilita:** Identificar estaciones estratégicas que requieren mayor atención operativa.
* **Instrumento/fuente batch:** Campos: `start_station_name`, `start_station_id`, `end_station_name`, `end_station_id`, `started_at` y `ride_id`.
* **Instrumento/fuente streaming:** Acumulación histórica de los mismos eventos que posteriormente se publicarán por Kafka.
* **Modelo predictivo:** Clasificación para estimar alta demanda. El umbral de clase se definirá con un criterio estadístico reproducible. Corre en batch.
* **Salida:** Notebook con ranking de estaciones, distribución de demanda, clasificación y métricas del modelo.
* **¿Con qué otra(s) dimensión(es) del equipo se combina?:** Demanda general, estación U2 y componente espacial.
* **Lista inicial de requisitos:**
    1. El sistema debe contabilizar viajes originados y finalizados por estación.
    2. El sistema debe identificar estaciones de mayor utilización con criterios reproducibles.
    3. El sistema debe estimar mediante clasificación la probabilidad de alta demanda.

---

### Dimensión: Demanda futura por estación (integrante: Grimaldo Arredondo Martinez)
* **Tipo:** predictiva con tiempo real (U2, streaming)
* **Dimensión y relación con la pregunta central:** ¿Qué estaciones podrían registrar mayor demanda durante la siguiente ventana temporal considerando los eventos recibidos recientemente? Permite anticipar cambios de utilización a nivel de estación.
* **Indicador(es):** Viajes actuales por estación. Tasa de llegada y tendencia temporal. Demanda proyectada por estación.
* **Decisión o acción que habilita:** Reconocer estaciones cuya actividad está aumentando y priorizar su seguimiento.
* **Instrumento/fuente batch:** Histórico agregado por estación utilizado para entrenamiento.
* **Instrumento/fuente streaming:** Eventos Kafka con `ride_id`, `started_at`, `start_station_id`, `start_station_name`, `end_station_id` y `end_station_name`.
* **Modelo predictivo:** Serie de tiempo por estación o por subconjunto de estaciones de mayor volumen, según viabilidad computacional. Corre en streaming.
* **Salida:** Grafana con ranking actual, demanda por estación, evolución reciente y pronóstico.
* **¿Con qué otra(s) dimensión(es) del equipo se combina?:** Demanda general y patrones espaciales.
* **Lista inicial de requisitos:**
    1. El sistema debe agrupar eventos en tiempo real por estación.
    2. El sistema debe calcular la tendencia de demanda mediante ventanas temporales.
    3. El sistema debe publicar en Grafana la demanda actual y pronosticada de las estaciones monitoreadas.

---

### Dimensión: Duración de los viajes (integrante: Jose Miguel Condo Huamani)
* **Tipo:** predictiva sin tiempo real (U1, batch)
* **Dimensión y relación con la pregunta central:** ¿Cómo varía históricamente la duración de los viajes y qué duración puede esperarse de acuerdo con las características disponibles del viaje? Complementa la pregunta central con una medida del comportamiento operativo de cada viaje.
* **Indicador(es):** Duración media y dispersión. Duración por tipo de usuario y bicicleta. Duración estimada por el modelo.
* **Decisión o acción que habilita:** Identificar patrones de uso y estimar la duración esperada de determinados viajes.
* **Instrumento/fuente batch:** Campos: `started_at`, `ended_at`, `rideable_type`, `member_casual`, `start_lat`, `start_lng`, `end_lat` y `end_lng`.
* **Instrumento/fuente streaming:** Acumulación histórica de eventos equivalentes a los de U2.
* **Modelo predictivo:** Regresión con variable objetivo `duration_minutes`. Corre una vez al ejecutar el pipeline batch y se evalúa con métricas apropiadas.
* **Salida:** Notebook con distribución de duración, comparaciones, duración real frente a predicha y métricas de evaluación.
* **¿Con qué otra(s) dimensión(es) del equipo se combina?:** Demanda temporal, duración U2 y comportamiento espacial.
* **Lista inicial de requisitos:**
    1. El sistema debe calcular la duración válida de cada viaje.
    2. El sistema debe analizar la duración según las características disponibles.
    3. El sistema debe estimar mediante regresión la duración esperada.

---

### Dimensión: Duración proyectada en streaming (integrante: Jose Miguel Condo Huamani)
* **Tipo:** predictiva con tiempo real (U2, streaming)
* **Dimensión y relación con la pregunta central:** ¿Cómo está evolucionando la duración de los viajes observados y qué duración media puede esperarse durante la siguiente ventana temporal? Traslada el análisis de duración histórica hacia una señal temporal observada y pronosticada durante la ejecución.
* **Indicador(es):** Duración media reciente. Duración por ventana y tendencia. Duración promedio proyectada.
* **Decisión o acción que habilita:** Identificar cambios anómalos o incrementos sostenidos en los tiempos de viaje.
* **Instrumento/fuente batch:** Histórico de duración agregada por intervalos utilizado para entrenamiento.
* **Instrumento/fuente streaming:** Eventos Kafka con `started_at`, `ended_at` y variables de segmentación disponibles.
* **Modelo predictivo:** Serie de tiempo sobre duración agregada por intervalo. La inferencia se actualiza en vivo sobre el flujo.
* **Salida:** Grafana con duración promedio actual, histórico reciente y duración proyectada.
* **¿Con qué otra(s) dimensión(es) del equipo se combina?:** Demanda U2 y patrones de estaciones/zonas.
* **Lista inicial de requisitos:**
    1. El sistema debe calcular automáticamente la duración de los eventos recibidos.
    2. El sistema debe generar agregaciones por ventanas temporales.
    3. El sistema debe mostrar la duración observada y proyectada en el Grafana común.

---

### Dimensión: Patrones espaciales (integrante: Cristhian Chuquitarqui Chura)
* **Tipo:** predictiva sin tiempo real (U1, batch)
* **Dimensión y relación con la pregunta central:** ¿Qué zonas concentran históricamente la actividad de Citi Bike y qué zonas presentan mayor probabilidad de registrar alta actividad futura? Complementa la pregunta central con la distribución geográfica de la demanda.
* **Indicador(es):** Viajes por zona. Orígenes y destinos por zona. Nivel o probabilidad estimada de alta actividad.
* **Decisión o acción que habilita:** Localizar espacialmente áreas prioritarias del sistema.
* **Instrumento/fuente batch:** Campos: `start_lat`, `start_lng`, `end_lat`, `end_lng`, `start_station_id`, `end_station_id` y `started_at`.
* **Instrumento/fuente streaming:** Acumulación histórica de las lecturas espaciales del mismo flujo Citi Bike.
* **Modelo predictivo:** Clasificación del nivel de actividad o regresión sobre volumen agregado por zona. Las zonas se definirán con un criterio espacial reproducible. Corre en batch.
* **Salida:** Notebook con distribución espacial, zonas críticas y predicción de actividad.
* **¿Con qué otra(s) dimensión(es) del equipo se combina?:** Estaciones, demanda general y actividad espacial U2.
* **Lista inicial de requisitos:**
    1. El sistema debe agrupar espacialmente los registros mediante coordenadas.
    2. El sistema debe calcular la concentración histórica de viajes.
    3. El sistema debe generar una estimación del nivel de actividad futuro.

---

### Dimensión: Concentración espacial futura (integrante: Cristhian Chuquitarqui Chura)
* **Tipo:** predictiva con tiempo real (U2, streaming)
* **Dimensión y relación con la pregunta central:** ¿Qué zonas presentan actualmente mayor concentración de viajes y cuáles podrían concentrar mayor actividad durante la siguiente ventana temporal? Permite observar en vivo la dimensión espacial y anticipar el desplazamiento de la demanda.
* **Indicador(es):** Viajes actuales por zona. Tasa y variación de eventos por zona. Volumen proyectado por zona.
* **Decisión o acción que habilita:** Detectar desplazamientos geográficos de la demanda y concentrar el monitoreo en zonas de mayor actividad prevista.
* **Instrumento/fuente batch:** Histórico espacial agregado utilizado para entrenamiento.
* **Instrumento/fuente streaming:** Eventos Kafka con `started_at`, `start_lat`, `start_lng`, `end_lat`, `end_lng`, `start_station_id` y `end_station_id`.
* **Modelo predictivo:** Serie temporal aplicada al volumen agregado por zona. Corre en vivo sobre el flujo.
* **Salida:** Grafana común con actividad actual por zona, tendencia, ranking y actividad proyectada.
* **¿Con qué otra(s) dimensión(es) del equipo se combina?:** Demanda general y demanda por estación.
* **Lista inicial de requisitos:**
    1. El sistema debe asignar cada evento a una zona espacial reproducible.
    2. El sistema debe calcular la actividad por zona mediante ventanas temporales.
    3. El sistema debe presentar simultáneamente actividad actual y pronosticada en Grafana.

---

## Alcance del proyecto

### Qué SÍ cubre este proyecto en conjunto:
* Procesamiento histórico de registros Citi Bike con PySpark.
* Limpieza, transformaciones, agregaciones y persistencia analítica.
* Modelos predictivos U1 sobre datos históricos.
* Simulación realista de eventos Citi Bike mediante Kafka.
* Procesamiento con Spark Structured Streaming.
* Modelos temporales e inferencia U2 sobre el flujo.
* Notebooks U1 y un Grafana común para todas las dimensiones U2.
* Integración de demanda, estaciones, duración y patrones espaciales en una sola historia analítica.

### Qué NO cubre - fuera de alcance:
* Control físico de bicicletas o estaciones.
* Modificación de la operación real de Citi Bike.
* Acceso o tratamiento de datos personales de usuarios.
* Desarrollo de la aplicación oficial de Citi Bike.
* Reservas o desbloqueo real de bicicletas.
* Despliegue de sensores físicos.
* Uso de variables externas como clima o tráfico mientras no sean incorporadas y documentadas formalmente.
