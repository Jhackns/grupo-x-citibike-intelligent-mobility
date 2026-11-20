# Cumplimiento U1 — histórico ampliado

> La evidencia completa de esta ejecución corresponde a **local[2]** y al notebook archivado. El principal incorpora ahora [paralelismo configurable](../configuracion/spark_master.md), con pruebas pequeñas verificadas; no se volvió a ejecutar el flujo completo ni se atribuyen estos tiempos a local[4].

Contraste realizado el 17/11/2026 con la [rúbrica oficial](https://262bigdata.github.io/lambda26/proyecto-sello/u1-producto/). Las precisiones de comparación/interpretación proceden de [S4](https://262bigdata.github.io/lambda26/sesiones/S04_ML_Distribuido_Regresion_Spark_MLlib/); las de ejecución y defensa, de [S5](https://262bigdata.github.io/lambda26/sesiones/S05_Evaluacion_Unidad_1/). No se asigna una calificación ni se sustituye la evaluación docente.

Ejecución técnica de referencia: `20261117T171917344468Z`. Notebook completo nuevo desde kernel limpio, sin reutilizar el experimento preliminar. Los archivos de `data/` son locales e ignorados por Git; los informes publican sus resultados sin secretos.

| Requisito | Evidencia concreta | Estado | Pendiente |
|---|---|---|---|
| Arquitectura justificada (12 %) | [Propuesta del brief](propuesta_brief_disponibilidad.md): diagrama, decisión Lambda/Kappa, histórico auditable, replay y riesgos de duplicar lógica. README separa batch real y streaming futuro. | Evidencia técnica disponible | Acordar con el equipo la propuesta del aporte propio; no se reescribió el brief común. |
| Spark distribuido y plan (16 %) | `src/lectura.py`, `src/calidad.py`, `src/diagnostico.py`; notebook ejecutado; `analisis/rdd_y_lazy.json` reconcilia map/reduceByKey con DataFrame y registra cero jobs al construir el plan, jobs al ejecutar count. | Verificado | Demostrar y explicar personalmente transformación/acción. |
| Parquet particionado y lectura de vuelta (16 %) | Silver/Gold de esta ejecución; `analisis/plan_gold_fecha.txt` sin InMemoryTableScan, con fecha en PartitionFilters; conteos, esquema, multiconjuntos, unicidad y purga reconciliados. | Verificado | Captura auténtica del plan y sus conteos. |
| Notebook reproducible (12 %) | `notebooks/archivo/01_historico_ampliado_20261117T171917344468Z.ipynb`: 11 celdas de código, secuencia 1–11 y cero errores; llama al mismo `src/flujo_u1.py` que la CLI. Configuración y hashes en manifiesto. | Verificado en Docker existente | El estudiante debe ejecutar las celdas pertinentes durante su demo. |
| ML entrenado, comparado, evaluado y guardado (16 %) | [36 comparaciones por partición](comparacion_modelos.md), 30 ajustes MLlib, MAE/RMSE/R²; tres LR, dos RF; doce archivos de importancia; contratos ganadores y mejores referencias MLlib con recarga verificada. | Verificado técnicamente | Explicar el ganador por validación y la incertidumbre operativa; no atribuir entrenamiento a persistencia. |
| Evidencias/documentación (8 %) | README, metodología, resultados, cobertura, notebook y manifiestos con versiones, entradas, métricas, tiempos y errores. | Técnica disponible; evidencia personal pendiente | Capturas propias con reloj/perfil, historial coherente y documentación publicada. No se crearon commits ni push. |
| Sustentación individual (20 %) | [Guía de evidencias personales](evidencias_personales.md), preguntas y rutas para la demostración. | Pendiente del estudiante | Autoría/aporte, exposición, presentación/actitud, repositorio/topics, publicación y pitch; responder preguntas en vivo. |

## Pendientes técnicos de la revisión anterior

- Lambda/Kappa y propuesta del brief: documentadas sin alterar las dimensiones ajenas.
- RDD y ejecución diferida: ejecutados sobre las 5.302.934 observaciones, con agregados pequeños y reconciliación.
- Tercera configuración: LR sin regularización, además de ridge 0.01/0.1, fijadas antes de prueba. Son tres configuraciones acotadas; no una búsqueda exhaustiva.
- Importancia RF: guardada para ambos tamaños de bosque, tipos y horizontes; top variables en notebook e informe. Es atribución por impureza, no causalidad.
- Notebook limpio: ejecución completa verificada, no celdas sueltas ni salidas copiadas del día preliminar.

El ganador global conserva su contrato y sus métricas. Cuando es persistencia, no requiere fit. El mejor candidato **entrenado MLlib** se identifica por validación en `modelos/referencias_mllib/<tipo_horizonte>/contrato.json`, con enlace al pipeline guardado en `modelos/candidatos/`; no se presenta como vencedor global si perdió. Las recargas de ambos roles reproducen todas las predicciones verificadas.

## Límites que permanecen

Una semana y dos días UTC de prueba no demuestran generalización estacional ni repetibilidad semanal. Los objetivos son reportes por tipo, sin verdad física confirmada. No se fijó un umbral de MAE operativamente aceptable, ni se validaron decisiones reales de redistribución. No se eligió un horizonte definitivo.

La entrega técnica no acredita capturas personales, publicación, aprobación del cambio de brief, defensa ni autoría demostrada. Los informes de sesiones individuales, PDF, reflexión y feedback personal deben completarse si corresponden a la entrega docente. La prueba preliminar y los intentos interrumpidos permanecen identificados por separado. U2 sigue sin implementar.
