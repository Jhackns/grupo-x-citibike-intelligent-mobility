# Verificación ejecutada de U2, etapa 2

Verificación real el **19 de noviembre de 2026 UTC** (18 de noviembre en Lima).
Alcance: Kafka → Spark Structured Streaming → reglas U1 → persistencia → Parquet.
No se descargó histórico, modificó U1, entrenó modelos ni implementó las etapas
PostgreSQL/Grafana/evaluación diferida. No se hicieron commits ni push.

## Ejecución y artefactos vigentes

Configuración explícita: [streaming-verificacion.json](../../config/streaming-verificacion.json).
Spark 4.2.0 / Scala 2.13.18 / conector Kafka `_2.13:4.2.0`, cliente JVM 3.9.2,
broker aislado 3.9.1. Imagen PySpark existente, sin reconstrucción.
Prueba: **local[2]**, driver 2 GiB, shuffle 2, trigger 10 s, watermark/ventana 1 min.
Valores normales de futuras ejecuciones: local[4], watermark 10 min, ventana 5 min.

Rutas relativas a `pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/`:

```text
data/streaming/verificacion_etapa2_v2/
  salidas/
    _pipeline.json
    _schema_observaciones.json
    observaciones/batch-<batch_id>/   # Parquet + _batch.json, publicación atómica
    rechazados/                     # Parquet + _spark_metadata
    predicciones/                   # Parquet + _spark_metadata
    metricas/                       # Parquet + _spark_metadata
  checkpoints/
    ingesta/
    rechazados/
    predicciones/
    metricas/
```

Raíz absoluta host:
`/home/miguel/proyec-bigdata/grupo-x-citibike-intelligent-mobility/pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/data/streaming/verificacion_etapa2_v2/`.
En Docker: `/opt/UNIDAD2/Dim-Disponibilidad/data/streaming/verificacion_etapa2_v2/`.
No se selecciona una carpeta por orden ni fecha. La configuración fija offsets
de arranque **anteriores a publicar**: particiones 0/1/2 = 3480/3264/3332.
En los reinicios mandan los checkpoints; no esos offsets iniciales.

Referencia U1: `../../UNIDAD1/Dim-Disponibilidad/config/ejecucion_seleccionada.json`.
Se verificaron los seis contratos originales de **20261117T171917344468Z**,
su selección congelada, hashes, tipos/horizontes y procedencia. Persistencia
seleccionada, versión `modelado-v2-regularizacion-interpretacion`, requiere solo
disponibilidad actual. Los candidatos MLlib de referencia no se cargaron.

## API, Kafka y resultados reales

El productor existente publicó únicamente capturas reales de GBFS 2.3; no se
introdujeron fixtures en el tópico. Catálogo actual compatible: 1 bicycle/human,
2 bicycle/electric_assist. Cada captura tuvo 2.519 estaciones/eventos, todos con
ACK Kafka y cero eventos sin confirmar. Evidencias:
[producción 1](produccion_real_1.json), [producción 2](produccion_real_2.json).

| capture_id | Captura UTC | last_updated UTC | Elegibles de cada tipo | Predicciones |
|---|---|---|---:|---:|
| 711ed0d6-0ca5-4677-bd13-1449df7cd112 | 02:53:33.321 | 02:51:35 | 2.443 | 14.658 |
| b2def812-793f-4e66-9e02-7a08286a83a4 | 02:54:43.655 | 02:53:37 | 2.444 | 14.664 |
| c669df96-9bac-4edd-8a4a-ede88b233afc | 03:19:31.444 | 03:18:36 | 2.444 | 14.664 |

Todas las horas corresponden al 2026-11-19. Las dos primeras capturas se
solicitaron con intervalo 60 s (TTL 60 s): separación observada **70,334 s**.
El reloj last_updated de la fuente avanzó **122 s**. Son magnitudes diferentes;
esta muestra no demuestra una periodicidad universal. La tercera captura se
solicitó manualmente después de verificar el reinicio; el hueco de 24 min 47,789 s
es del ensayo, no una caída observada de la API ni la cadencia de un productor continuo.

[Lectura de vuelta final](verificacion_real.json):

| Comprobación | Resultado |
|---|---:|
| Observaciones guardadas, event_id únicos | 7.557 |
| No elegibles, conservadas también en rechazados | 226 |
| Elegibles convencionales / eléctricas | 7.331 / 7.331 |
| Predicciones por tipo y horizonte | 7.331 en cada una de las seis combinaciones |
| Total predicciones, prediccion_id únicos | 43.986 |
| Diferencias respecto al conteo explícito actual | 0 |
| Diferencias de origen u horizonte exacto (+300/+600/+900 s) | 0 |
| Contratos originales recargados y SHA verificados | 6 |

Observaciones incluye los rechazos: no sumar 7.557 + 226 como total de entrada.
En esta muestra todos los rechazos afectaron a ambos objetivos; el código también
admite rechazo parcial y tres predicciones para el tipo válido. Los campos y motivos
quedan guardados; no se eliminan las discrepancias para forzar igualdad.

Ventanas **finalizadas** de un minuto tras la tercera captura:

| Ventana UTC [inicio, fin) | Observaciones | Elegibles por tipo | Rechazo total | Cualquier discrepancia |
|---|---:|---:|---:|---:|
| 02:53–02:54 | 2.519 | 2.443 | 76 | 81 |
| 02:54–02:55 | 2.519 | 2.444 | 75 | 100 |

Ambas sin contratos inválidos. Suman las primeras 5.038 observaciones; la ventana
03:19–03:20 conserva 2.519 en estado y no se escribe todavía. Watermark final:
**03:18:31.444 UTC**. No se fuerzan cierres artificiales. El microbatch sin nuevas
entradas eliminó 5.038 IDs antiguos del estado de deduplicación y dejó 2.519.
En el ensayo real no hubo eventos descartados por watermark; eso se comprobó
por separado con fixtures tardíos, no se inventó tráfico inválido en Kafka.

Latencia captura→calidad observada: mínimo **156,556 s**, máximo **629,501 s**,
media **448,408 s**. Incluye el tiempo deliberado entre publicación y arranque de
las verificaciones. **No es un benchmark de latencia online** y algunas predicciones
se emitieron después de su fecha objetivo. El batch Kafka de la tercera captura
tardó unos **19,186 s**; superó el trigger de 10 s, advertencia registrada por Spark.
No se atribuyen estos tiempos a local[4] ni se asegura un SLA de 5 minutos.

## Checkpoints, reinicio y modo persistente

Resúmenes pequeños de las tres aplicaciones correctas, con progreso y contratos:
[ejecuciones.json](ejecuciones.json). Manifiestos completos locales en
`data/streaming/ejecuciones/<UUID>.json`.

| Ejecución | Modo | Resultado |
|---|---|---|
| 11a350b2-9830-47ce-ad45-9fa2b46256c6 | availableNow, cuatro consultas secuenciales | Dos primeras capturas: 5.038 observaciones / 29.322 predicciones |
| beba4bd6-f0f6-48a4-bc9c-f389d4b06f77 | availableNow, mismos checkpoints, sin publicaciones nuevas | Cero nuevas entradas y mismos archivos/conteos |
| 8f74298c-631d-4d6e-bc8c-dabe87d33cc6 | processingTime, cuatro consultas concurrentes, 60 s | Solo tercera captura nueva; cierre limpio y ventanas anteriores finalizadas |

La prueba de reinicio recargó el estado, conservó los **mismos query id** y cambió
runId. [Antes](antes_reinicio.json), [después](despues_reinicio.json),
[comparación](reinicio.json) y [SHA256 de los ocho Parquet anteriores](archivos_antes_reinicio.json).
No se modificaron archivos previos ni aparecieron filas duplicadas. El modo
persistente reanudó después ese mismo estado y procesó la captura nueva.

Esto demuestra recuperación por reinicio normal y reintento de batch publicado
en la prueba sintética. **No se simuló pérdida de disco ni un kill en cada punto
de fallo**, no hay transacción global entre las cuatro salidas y no se afirma
exactly-once de extremo a extremo. Las garantías y eventos tardíos se detallan
en [streaming.md](../../streaming.md).

Se conserva el ensayo previo `data/streaming/verificacion_etapa2/`: procesó las
dos primeras capturas pero falló al serializar UUID del progreso de Spark al
cerrar. Su manifiesto `0c0d21aa-e8ed-43cb-9f53-54e4413b76f3.json` no acredita cierre
correcto y conserva su temporal/error. Se corrigió el registro y se eligieron
las rutas **v2 explícitamente**; no se borraron ni editaron checkpoints anteriores
ni se forzó su compatibilidad. También se resolvió el login Hadoop del UID del
host con montajes de identidad propios, sin cambiar la imagen.

## Pruebas sintéticas, separadas de datos reales

[Cuatro pruebas Spark correctas](pruebas_sinteticas.json), local[2]:

1. Paridad de calidad con U1: nueve muestras, 24 columnas, `exceptAll` en ambos
   sentidos sin diferencias. Casos: conteos por tipo discrepantes, nulo frente a
   cero, ausencia/negatividad por objetivo, actividad, devoluciones cerradas,
   reporte/feed antiguos y reloj de reporte ausente.
2. Los seis contratos producen 18 predicciones fixture iguales a la función
   original de persistencia U1, con IDs deterministas y horizontes exactos.
3. El mismo batch publicado dos veces conserva dos filas y sus archivos;
   otro fingerprint se rechaza.
4. Spark stateful: seis IDs distintos sobreviven, se eliminan duplicados técnicos,
   se conserva una llegada fuera de orden tolerada, se descartan dos demasiado
   tardías y se elimina estado antiguo. Reinicio sin entradas mantiene seis filas;
   ventana finalizada con tres observaciones.

Además se ejecutaron de nuevo las **21 pruebas Python de etapa 1**, correctas.
La detección local de PySpark se ajustó para distinguir la carpeta `pyspark/`
del repositorio de una instalación real: en host las cuatro Spark se omiten;
su ejecución efectiva es la de Docker anterior. No se instala PySpark en `.venv`.

## Reproducir la lectura de verificación sin volver a publicar

Desde la raíz del repositorio, con la imagen existente y archivos locales:

```bash
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/lanzar_streaming.py --config pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/config/streaming-verificacion.json --verificar --produccion pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/reports/streaming/produccion_real_1.json pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/reports/streaming/produccion_real_2.json --informe reports/streaming/relectura.json
```

No necesita API ni Kafka activos, pero sí que exista la red Docker propia
conservada y estén disponibles los originales U1, las salidas y los JAR cacheados.
Para una nueva prueba de ingesta seguir [los comandos operativos](../../streaming.md)
con rutas explícitas nuevas; la retención Kafka puede haber vencido los offsets
de este ensayo. No republicar para intentar reproducir el mismo capture_id.

## Conservación y pendientes

[conservacion.json](conservacion.json) comprueba 10.169 archivos U1 con mismos
tamaños/mtime y sin altas/bajas; SHA256 de Dockerfile/Compose del equipo y del
productor del compañero sin cambios. U1 fue montado en modo lectura.
Código/configuraciones/documentación e informes pequeños son versionables;
`data/` (Parquet, estado, offsets, staging, JAR), `.venv` y credenciales siguen
ignorados. No se borraron volúmenes ni checkpoints.

Los procesos acotados de productor y Spark terminaron; el broker
`citibike-disponibilidad-kafka` se detuvo con `compose.kafka.yml stop`.
El estado comprobado de contenedores se registra en [servicios_finales.json](servicios_finales.json).
Se conservaron activos los servicios del equipo que ya estaban funcionando.

Frente a la guía U2 hay evidencia de consumo Spark, calidad, ventanas/watermark,
checkpointing y persistencia, **no de U2 completa**. Siguen pendientes evaluación
diferida con observaciones futuras, PostgreSQL, Grafana/KPIs, prueba sostenida y
de carga, costos/escalado y decisiones de vencimiento al servir predicciones.
El estudiante debe capturar personalmente la demostración en sus terminales y
sustentar calidad, tres relojes, límites del watermark, persistencia seleccionada
y garantías condicionadas de reinicio; los JSON técnicos no sustituyen esa defensa.
