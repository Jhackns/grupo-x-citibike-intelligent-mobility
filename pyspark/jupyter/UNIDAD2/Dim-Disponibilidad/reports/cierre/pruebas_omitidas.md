# Las 17 omisiones locales y su evidencia efectiva

La suite local de etapa 4 registró 44 pruebas: 27 correctas y 17 omitidas, no
17 fallidas. `pyspark.sql` no está instalado en `.venv`: Spark se ejecuta en Docker.
`skipUnless(HAS_SPARK, ...)` produce dos motivos literales:

- `Ejecutar con spark-submit en Docker`: las cuatro de `test_streaming.py`.
- `Ejecutar --sql --pruebas-sql en Docker`: las trece de `test_postgres.py`.

La carpeta del repositorio llamada `pyspark/` no es una instalación Python de
Spark. No se instalaron dependencias globales ni se alteró esa separación.

El informe anterior ya tenía 4+13 OK sin omisiones en Docker. Al comprobar la
vigencia, `test_streaming.py` era posterior a su informe: se **reejecutaron solo
esas cuatro**, con el lanzador existente, **local[2], 26,356 s, 4 OK, sin fallos,
errores u omisiones**. Las fuentes del test SQL, `postgres.py` y `001` son anteriores
a su informe de trece pruebas; se reutiliza esa evidencia válida sin repetir
fixtures SQL ni la prueba real de más de quince minutos. No hubo entrenamiento.

Evidencia: [inventario por nombre, línea y hash](pruebas_omitidas.json),
[resultado Spark actual](pruebas_streaming_actuales.json),
[resultado SQL previo](../postgres/pruebas_sinteticas.json),
[omisión host original](../observabilidad/pruebas.json).

| # | Prueba exacta, archivo y línea | Qué verifica | Estado y evidencia | Pendiente que no resuelve |
|---|---|---|---|---|
| 1 | `StreamingTests.test_01_paridad_calidad_u1`, `tests/test_streaming.py:67` | 9 muestras y 24 columnas de calidad equivalentes a U1; tipos, nulos, actividad, antigüedad. | Reejecutada OK. | No demuestra exactitud física de GBFS. |
| 2 | `StreamingTests.test_02_predicciones_paridad_persistencia_u1`, `tests/test_streaming.py:87` | 6 contratos, 18 predicciones iguales a U1, horizontes e IDs deterministas. | Reejecutada OK. | No valida error online sin observaciones futuras. |
| 3 | `StreamingTests.test_03_reintento_sink_no_duplica`, `tests/test_streaming.py:106` | Repetir batch conserva 2 filas y archivos; rechaza otro fingerprint. | Reejecutada OK. | No prueba todos los puntos de caída ni transacción distribuida. |
| 4 | `StreamingTests.test_04_duplicados_retrasos_estado_ventanas_y_reinicio`, `tests/test_streaming.py:117` | Deduplicación, dos tardíos descartados, estado eliminado, ventana y reinicio. | Reejecutada OK, fixture Spark. | Falta demostración controlada del descarte tardío y duplicado con eventos reales. |
| 5 | `SQLTests.test_01_limite_inclusivo_y_desempate_anterior`, `tests/test_postgres.py:76` | −30 s inclusivo, empate anterior, reloj de cierre y métricas. | Evidencia SQL previa OK. | Reloj sintético, no SLA. |
| 6 | `SQLTests.test_02_no_saltar_objetivo_invalido`, `tests/test_postgres.py:88` | No elige otro candidato para eludir calidad futura. | Evidencia SQL previa OK. | La calidad física sigue incierta. |
| 7 | `SQLTests.test_03_sin_objetivo_y_sin_avance`, `tests/test_postgres.py:94` | Sin progreso queda pendiente; con progreso y sin objetivo vence, sin ceros. | Evidencia SQL previa OK. | La operación detenida no cerrará automáticamente pendientes por reloj. |
| 8 | `SQLTests.test_04_tardio_antes_de_cerrar_y_cierre_inmutable`, `tests/test_postgres.py:103` | Admite llegada antes de cierre; no reevalúa terminales; estrato discrepante. | Evidencia SQL previa OK. | No cubre correcciones posteriores al cierre. |
| 9 | `SQLTests.test_05_horizonte_15_y_huecos`, `tests/test_postgres.py:112` | Horizonte 15 min con hueco invalida trayectoria. | Evidencia SQL previa OK. | No sustituye una prueba real a 15 min, que tiene evidencia separada de etapa 3. |
| 10 | `SQLTests.test_06_identidad_y_rollback`, `tests/test_postgres.py:117` | Reintento idéntico, colisión rechazada y rollback sin parciales. | Evidencia SQL previa OK. | No es exactly-once entre Kafka, archivos y SQL. |
| 11 | `SQLTests.test_07_sink_spark_reintento_mismo_batch`, `tests/test_postgres.py:129` | Spark/JDBC real sobre 9 fixtures; repetir batch mantiene 18 predicciones. | Evidencia SQL previa OK. | Reinicio con datos API se comprueba en `reports/postgres/reinicio.json`, no aquí. |
| 12 | `SQLTests.test_08_extremo_positivo_y_continuidad_a_15_minutos`, `tests/test_postgres.py:149` | +30 s inclusivo y continuidad de 15 min; no cierra antes. | Evidencia SQL previa OK. | Supuestos de margen tardío no garantizan todas las llegadas reales. |
| 13 | `SQLTests.test_09_inactividad_intermedia_rompe_trayecto`, `tests/test_postgres.py:157` | Una inactividad intermedia impide evaluar como trayecto válido. | Evidencia SQL previa OK. | No determina la causa de inactividad del sistema físico. |
| 14 | `SQLTests.test_10_rechazo_reloj_invalido_se_preserva_sin_cero`, `tests/test_postgres.py:163` | Conserva texto original; timestamp y conteo inválidos quedan NULL. | Evidencia SQL previa OK. | No sustituye toda la validación del contrato de ingesta. |
| 15 | `SQLTests.test_11_fuera_de_tolerancia_no_es_objetivo`, `tests/test_postgres.py:171` | 30,001 s se excluye; observado/error ausentes. | Evidencia SQL previa OK. | La tolerancia no se ajusta para aumentar cobertura. |
| 16 | `SQLTests.test_12_sobre_invalido_no_oculta_captura_mas_cercana`, `tests/test_postgres.py:177` | Candidato cercano con sobre inválido no se oculta tras otro válido. | Evidencia SQL previa OK. | No resuelve exactitud física ni cambios de definición no soportados. |
| 17 | `SQLTests.test_13_evaluador_aplaza_si_ingesta_ocupada`, `tests/test_postgres.py:183` | Dos conexiones reales: lock de carga aplaza cierre sin bloquear; luego evalúa. | Evidencia SQL previa OK. | No es prueba de carga ni de alta disponibilidad. |

Todos los 17 casos tienen evidencia sintética efectiva; **ningún requisito queda
sin verificar solo por la omisión del host**. Sí quedan requisitos de la guía sin
prueba correspondiente: sensor físico/alerta real (S7), descarte tardío y duplicado
reales y comparación de dos triggers (S8), costos cuantificados/escalado y tres
alertas propuestas con datos (S9), publicación literal de KPIs en Pushgateway y
contraste PromQL en dos momentos (S11), además de publicación documental y defensa.
El detalle distingue el alcance de la rúbrica general frente a mínimos de sesión.

No se ejecutaron ahora la prueba real de 30 minutos, entrenamiento, una nueva
captura ni el evaluador SQL. La evidencia real ya válida se conserva. Las consultas
READ ONLY adicionales de cobertura, percentiles y separación de flujos están en
este mismo directorio; verifican las magnitudes de cierre sin cambiar resultados.

Repetición opcional de los tests existentes (no necesaria para leer este cierre):

```bash
D=pyspark/jupyter/UNIDAD2/Dim-Disponibilidad
.venv/bin/python "$D/lanzar_streaming.py" --pruebas --spark-master 'local[2]'
.venv/bin/python "$D/lanzar_streaming.py" --sql --pruebas-sql --spark-master 'local[2]'
```

El segundo requiere PostgreSQL propio activo y crea flujos `fixture-*` separados;
no ejecutarlo esperando una operación de solo lectura ni volver a medir latencias
reales con su reloj simulado. Las pruebas cierran Spark y el contenedor transitorio.
