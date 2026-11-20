# Verificación ejecutada de etapa 4 — 19/11/2026

**Resultado:** Grafana aprovisionado, consultas PostgreSQL de solo lectura,
actualización con API/Kafka/Spark/SQL reales y telemetría técnica verificados.
No se reentrenó, modificó U1 ni ampliaron tolerancias para aumentar cobertura.
No se inspeccionó visualmente el dashboard en un navegador.

## Auditoría previa

[19.572 vencidas auditadas](vencidas.md), con
[resultado ejecutado](vencidas.json): 19.554 sin captura en ±30 s y 18 con objetivo
futuro no elegible. Cero falsos faltantes y cero objetivos declarados inválidos
que fueran elegibles. No se corrigió el algoritmo porque no se demostró defecto.
Se conservaron 283.408 pendientes históricas bloqueadas por avance de la fuente,
con cero ya cerrables; no se convirtieron en vencidas por el mero paso del reloj.

## Prueba real nueva, separada de etapa 3

Flujo: `disponibilidad-etapa4-vivo-20261119-v1`.
Configuraciones: `config/streaming-etapa4.json`, `config/postgres-etapa4.json`.
Contratos: selección U1 `20261117T171917344468Z`, los seis de persistencia.
Spark explícitamente **local[2]**, sin cambio de memoria; default futuro local[4].
Consulta API configurada a 60 s y TTL observado de 60 s, no a dos minutos.

| Ensayo acotado | Capturas UTC | Eventos con ACK | Observaciones SQL nuevas |
|---|---|---:|---:|
| [Primero](ejecucion_viva.json) | 15:16:31.839, 15:17:34.102, 15:18:37.402 | 7.557 | 7.557 |
| [Reinicio](ejecucion_viva_reinicio.json) | 15:22:08.650, 15:23:10.874 | 5.038 | 5.038 |
| Total | 5 capturas × 2.519 estaciones | 12.595 | 12.595 |

Se usaron los mismos checkpoints del flujo de etapa 4 en ambos ensayos. La pausa
entre ensayos fue intencional; no se presenta como cadencia de la API. Los
intervalos dentro de cada ensayo fueron aproximadamente 62–63 s. Los manifiestos
conservan capture_id, configuración, PID, catálogos, contadores y cierre.
[Resumen SQL final](resumen_sql.json): **12.595 observaciones, 382 rechazos totales
o parciales y 73.278 predicciones**, todas todavía pendientes en ese flujo. Los
rechazos son un subconjunto de observaciones, no registros que se deban sumar.

La prueba nueva no esperó el horizonte de 15 minutos más tolerancia/margen tardío.
Su evidencia es de visualización/actualización y cierre del flujo; las métricas de
los seis pares tipo/horizonte se verificaron con los resultados reales conservados
de etapa 3, no con objetivos sintéticos ni con la observación actual como futuro.

## Grafana y correspondencia SQL

[Verificación completa por API](verificacion.json), ejecutada de 15:45:41 a
15:47:23 UTC, estado `verificada`, sin errores:

- Grafana **11.6.11**; conexiones PostgreSQL y Prometheus con health OK.
- Dashboard UID `citibike-disponibilidad-u2`, **16 paneles**, targets idénticos al
  JSON versionado; las **11 consultas SQL** se ejecutaron con éxito.
- Usuario real `disponibilidad_grafana`, sesión read_only=on; SELECT de vistas
  permitido, escritura de tablas base, CREATE y TEMP no permitidos. Intento de
  UPDATE sin filas rechazado. La API no devuelve la contraseña de la fuente.
- Los seis conteos, MAE, RMSE y sesgo coinciden con el informe SQL de etapa 3;
  comparación numérica con tolerancia 1e-10, sin redondear predicciones.
- Cobertura y motivos coinciden: 410.514 predicciones históricas, 107.534 evaluadas,
  19.572 vencidas y 283.408 pendientes. El total reconcilia sin solapar estados.
- Calidad histórica: 70.532 observaciones, 2.113 rechazos totales/parciales,
  1.487 con discrepancias y 69.045 sin discrepancias; desconocidas 0 en esa muestra.
- Filtros de estación y periodo contrastados contra consulta SQL directa. Una
  estación inexistente no devuelve filas. El horizonte y el tipo se verificaron
  mediante las seis combinaciones, no solo sus valores iniciales.
- Serie observado/predicho: **28 fechas objetivo**, ambas medias sobre los mismos
  pares evaluados; las fechas sin observación permanecen NULL en ambas series.
  No se rellenan huecos ni se sustituye el resultado futuro por el actual.

Métricas históricas seleccionadas; conteos reportados, no exactitud física:

| Tipo | Min | n evaluadas | MAE | RMSE | Sesgo |
|---|---:|---:|---:|---:|---:|
| Convencionales | 5 | 21.996 | 0,05255501 | 0,24626691 | 0,00200036 |
| Convencionales | 10 | 21.995 | 0,09706752 | 0,35583660 | 0,00650148 |
| Convencionales | 15 | 9.776 | 0,13850245 | 0,44178258 | 0,00675123 |
| Eléctricas | 5 | 21.996 | 0,16839425 | 0,49456034 | −0,00727405 |
| Eléctricas | 10 | 21.995 | 0,29734030 | 0,70506239 | −0,01327574 |
| Eléctricas | 15 | 9.776 | 0,39177578 | 0,84812299 | −0,02229951 |

La tabla redondea solo para lectura; los JSON conservan precisión completa. No se
usaron estos errores de prueba online para seleccionar otro predictor.

## Actualización, ausencia y parada

[actualizacion_viva.json](actualizacion_viva.json) conserva consultas reales a
Grafana mientras llegaban eventos del segundo ensayo. En convencionales/5 min el
conteo observado pasó de **7.328 a 9.771**, con estado ACTIVO, y después DETENIDO.
Se comprobaron 100 predicciones todavía VIGENTE al consultar. En ese momento
n=0 produjo **MAE/RMSE/sesgo NULL**, no cero; estación inexistente devolvió vacío.

[sin_telemetria.json](sin_telemetria.json) comprueba que, una vez terminado un
colector acotado, el exportador responde **HTTP 503** por archivo antiguo y Grafana
muestra **TELEMETRIA ANTIGUA**. Se distingue de DETENIDO con telemetría reciente.
Posteriormente se reactivó solo el colector para completar la verificación técnica,
y se cerró de nuevo al finalizar. No se dejaron productores continuos ejecutando.

## Telemetría técnica

Prometheus registró **tres targets up**: aplicaciones, Kafka y Grafana. Evidencia
en `verificacion.json`: contadores de eventos confirmados, duraciones de las cuatro
consultas Spark, offsets de las tres particiones Kafka, estados de componentes y
backlog estimado disponible (0 en las tres particiones al verificar). Este último
no garantiza confirmación SQL ni ausencia futura de retraso.

Se encontraron **46 series custom** sin etiquetas station_id/event_id/capture_id/
run_id. Se comprobó actividad previa del productor en la historia de Prometheus y
su estado 0 después del cierre; broker y PostgreSQL seguían activos durante esa
comprobación. No se atribuye esta telemetría a otros flujos históricos.

El primer arranque del kafka-exporter falló por DNS del broker aún no disponible;
se recuperó iniciando primero el broker y reiniciando solo el exportador. Los tres
targets estaban up en la verificación final. No se cambió la infraestructura ajena.

## Pruebas sintéticas y conservación

**6 pruebas focalizadas OK** en `tests/test_observabilidad.py`: nulos frente a
ceros, procesos desconocidos, etiquetas acotadas, declaración de tipos Prometheus,
variables SQL y huecos del gráfico. Son pruebas sintéticas, distintas del ensayo
real descrito arriba. La [suite local completa](pruebas.json) terminó con **27 OK
y 17 omitidas**: las últimas requieren Spark/SQL en Docker y no se repitieron en
esta fase porque su núcleo permaneció intacto; conservan la evidencia anterior.
Se validaron sintaxis de los 38 archivos Python disponibles,
JSON de configuración/dashboard y `git diff --check`.

[conservacion.json](conservacion.json) compara **10.169 archivos U1** por tamaño y
mtime y **9 archivos protegidos** por SHA-256, incluidos Dockerfile/Compose del
equipo y el núcleo streaming/SQL previo: sin cambios. No se detectaron los secretos
locales en archivos públicos de esta dimensión. No se modificaron los ignorados de
Git: `data/`, credenciales, capturas, Parquet, checkpoints y `.venv` siguen fuera;
código, configuraciones públicas, SQL, JSON del dashboard y estos informes son
versionables. No hubo commits ni push.

Estado final de servicios y conservación de volúmenes:
[servicios_finales.json](servicios_finales.json).

## Evidencia personal y pendientes U2

El estudiante debe abrir el tablero, capturar pantalla con estación/tipo/horizonte/
periodo, curva y métricas, cobertura y estados de parada, y sustentarlos. No se
presentan capturas de navegador como realizadas. Debe explicar la diferencia entre
conteo reportado y físico, persistencia seleccionada frente a candidatos MLlib U1,
ventanas/watermark, pendientes por avance y límites de idempotencia.

Pendientes: operación continua prolongada, prueba de carga y fallos, alertas con
notificaciones, hardware/JMX completo, cotización oficial del despliegue elegido,
backup/restauración y HA. La equivalencia académica de telemetría operativa con
flujo empresarial/IoT y el brief de equipo necesitan acuerdo/sustentación; este
informe no concede por sí mismo todos los criterios de U2. No se modificó ninguna
dimensión ajena.
