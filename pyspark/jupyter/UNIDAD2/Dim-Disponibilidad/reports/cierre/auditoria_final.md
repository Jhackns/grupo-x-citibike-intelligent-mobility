# Cierre y auditoría final de U2 — Dim-Disponibilidad

**Dictamen: implementación funcional con verificaciones reales, pero cumplimiento
académico parcial. No se declara U2 completa ni se asigna una nota de sustentación.**
Se consultaron la guía oficial vigente y S6–S12 el **19/11/2026**. Se revisaron código,
contratos U1, informes, SQL, checkpoints, configuración y el dashboard renderizado.
No se añadió funcionalidad, reentrenó, descargó histórico ni publicaron eventos
nuevos. Los dos flujos SQL de esta auditoría se leyeron sin ejecutar el evaluador.

Fuentes normativas:
[Producto U2](https://262bigdata.github.io/lambda26/proyecto-sello/u2-producto/),
[S6](https://262bigdata.github.io/lambda26/sesiones/S06_Ingesta_Eventos_Empresariales_Kafka/),
[S7](https://262bigdata.github.io/lambda26/sesiones/S07_Ingesta_Eventos_IoT_Sensores_Tiempo_Real/),
[S8](https://262bigdata.github.io/lambda26/sesiones/S08_Procesamiento_Streaming_Spark_Ventanas_Watermarking/),
[S9](https://262bigdata.github.io/lambda26/sesiones/S09_Observabilidad_Pipeline_Grafana_Costos/),
[S10](https://262bigdata.github.io/lambda26/sesiones/S10_Series_Tiempo_Inferencia_Streaming_MLlib/),
[S11](https://262bigdata.github.io/lambda26/sesiones/S11_BI_ML_Distribuido_Spark_Grafana/),
[S12](https://262bigdata.github.io/lambda26/sesiones/S12_Evaluacion_Unidad_2/).

## Tabla final de la rúbrica general

Estados: **verificado** = evidencia técnica concreta; **parcial** = implementación
o correspondencia normativa incompleta; **sin evidencia** = no se acredita.
Los mínimos específicos de sesión se distinguen de los ocho criterios generales;
no se convierten automáticamente todos los ejemplos del docente en otra arquitectura.
Tampoco se omiten las divergencias cuando la sesión exige una evidencia concreta.

| Requisito / peso | Evidencia concreta en esta dimensión | Estado | Pendiente y naturaleza |
|---|---|---|---|
| 1. Kafka empresarial / 12 % | `contratos/evento.v1.schema.json`, `src/disponibilidad_u2/productor.py`, `reports/verificacion_real.json`: publicación/consumo de 5.038 eventos; tópico, clave, ACK e identidad documentados. | **Parcial en correspondencia de negocio; Kafka verificado.** | La fuente es estado operativo GBFS. No hay flujo empresarial diferenciado ni evidencia de equivalencia aprobada. S6 muestra además dos microservicios y Kafka UI; no existen en esta dimensión. Limitación de alcance, no fallo del broker. |
| 2. IoT/sensores / 10 % | API GBFS real, catálogos y esquema comprobados; dos tipos explícitos y controles de cantidades en calidad. | **Parcial; origen físico no acreditado.** | No hay sensor propio/puente MQTT, prueba de dispositivo físico ni alerta de rango provocada sobre una lectura real como pide S7. La tabla de capacidades U2 admite telemetría simulada como ejemplo, mientras S7 exige dispositivo real en su mínimo: no resolver esa diferencia a favor propio sin acuerdo docente. |
| 3. Spark streaming / 14 % | `ejecutar_streaming.py`, `calidad_streaming.py`, `reports/streaming/verificacion_real.json`: 7.557 observaciones, 43.986 predicciones y dos ventanas finalizadas; checkpoints/reinicio. Cuatro tests actuales Docker OK. | **Verificado en el flujo general; S8 parcial.** | Faltan descarte tardío y duplicado controlados con eventos reales y comparación documentada de dos triggers. Las fixtures no acreditan esos casos reales. |
| 4. Observabilidad / 12 % | `observabilidad/dashboards/disponibilidad.json`, `sql/002_observabilidad.sql`, `reports/observabilidad/verificacion.json`: 16 paneles, tres targets up en ensayo previo, RO y correlación SQL; revisión visual de este cierre. | **Operativa con fallos y límites documentados.** | Denominador del panel de cobertura incorrecto para inmaduras; tasas negativas en ventanas discontinuas; rango efectivo de evaluadas no visible en su tabla. Las magnitudes correctas están en el informe de resultados. |
| 5. Costos/escalado / 8 % | `costos.md`, tamaños reales `reports/observabilidad/resumen_sql.json`, volumen observado y límites Compose. | **Parcial.** | Fórmula paramétrica y supuesto de 20 KiB no calibrado; sin costo monetario total ni plantilla S9 completa con valores medidos, sin criterios cuantificados de escalado basados en carga sostenida. No se inventa una cotización. |
| 6. BI/ML distribuido / 12 % | Spark produce predicciones y SQL/KPIs; `reports/observabilidad/actualizacion_viva.json` pasa 7.328→9.771; Firefox distingue prueba corta e histórico, filtros y NULL. | **Función BI verificada; mínimos S11 parciales.** | No hay Pushgateway con dos KPIs batch ni pareja de consultas PromQL registrada en dos instantes durante el flujo. La alternativa continua PostgreSQL/scrape es funcional, pero no acredita literalmente esa entrega. Falta integración/presentación en tablero común del equipo indicada por S12. |
| 7. Inferencia temporal / 12 % | Seis contratos U1 `20261117T171917344468Z`; `predictor_streaming.py`; SQL: 107.534 pares futuros evaluados, MAE/RMSE/sesgo por tipo/horizonte, conservación de discrepancias y cambios. | **Verificada experimentalmente; atribución temporal parcial en UI.** | Persistencia no es MLlib entrenado. La comparación LR/RF y recargas existen en U1 y se enlazan abajo. La prueba corta de etapa 4 tiene cero evaluaciones; no atribuirle las métricas de etapa 3. No demuestra generalización. |
| 8. Sustentación / 20 % | Código, informes, comandos y evidencia técnica disponibles. | **No evaluada.** | Aporte personal, exposición, presentación, repositorio, documentación publicada y pitch/demo requieren evidencia específica; no basta con capturas técnicas. |

No se calcula una nota final sobre 20: no se observó la sustentación y varios
criterios requieren interpretación/evidencia pendiente. Los pesos se conservan de
la rúbrica, no se redistribuyen por el alcance implementado.

## Mínimos de sesiones y entregables no acreditados

| Requisito complementario | Evidencia / comprobación | Estado | Pendiente |
|---|---|---|---|
| S6: cadena de negocio propia y manejo de mensaje malformado | Ingesta Python/contrato y pruebas de ausencias/errores en `tests/test_ingesta.py`; calidad sobre datos reales. | Parcial | No hay dos microservicios ni Kafka UI propios; no consta caso malformado real publicado sin caída del consumidor. Acordar la adaptación académica, sin implementar otro flujo en este cierre. |
| S7: dispositivo y alerta real | Solo API de tercero y validación de estado. | Sin evidencia específica | Dispositivo/puente y alerta física. Un feed público no prueba por sí mismo esos elementos. |
| S8: dos triggers, tardío y duplicado reales | Test 4 actual verifica fixtures; `reports/postgres/reinicio.json` preserva conteos/hashes tras reinicio SQL real. | Parcial | Comparación de batchId entre dos triggers y demostración real de descarte/deduplicación. Conservar autoría, reloj y perfil en las capturas personales. |
| S9: tres alertas propuestas y plantilla completa | Umbral de frescura y caducidad de telemetría en `observabilidad.md`; costo paramétrico. | Parcial | No hay tabla de tres alertas justificadas con mediciones ni plantilla económica completada. La falta no se reduce a configurar notificaciones. |
| S10: baseline frente a dos MLlib y artefacto recargado | U1 `reports/ampliacion/comparacion_modelos.md`, `resultados_ampliados.md`, CSV con MAE/RMSE/R², 30 ajustes; referencias MLlib guardadas y recargas con diferencia 0. | Evidencia existente reutilizada | Preparar la defensa de la equivalencia: Gold usa emparejamiento por timestamps, no `lead()` posicional literal. No cambiar el etiquetado correcto para copiar el ejemplo. |
| S10: inferencia del seleccionado | Persistencia seleccionada en seis contratos; test 2 actual recarga y compara con función U1, cero diferencias. | Verificado | No presentar persistencia como PipelineModel entrenado. Los PipelineModel de referencia siguen siendo referencias, no ganadores. |
| S11: publicación de KPIs batch y cambio PromQL en vivo | KPIs SQL y cambio real en Grafana; historia Prometheus contiene cambios. | Alternativa funcional; entrega literal parcial | No hay Pushgateway ni dos consultas contemporáneas conservadas. Consultar ahora query_range histórico no sustituye esa demostración durante el flujo. |
| S12: producto U2 adaptado y brief coherente | README técnico; brief común aún asigna duración a Jose Miguel. Propuesta de cambio en U1 `reports/ampliacion/propuesta_brief_disponibilidad.md`. | Parcial | Acuerdo del equipo y actualización del producto U2/brief común; no se reescriben dimensiones ajenas en esta auditoría. |
| S12: tablero compartido y demo individual | Dashboard propio aislado, operativo. | Parcial | Coordinación de la entrega grupal y demostración individual en el tablero acordado. |
| Documentación publicada | `mkdocs.yml` del repositorio navega solo brief/U1; no incluye esta dimensión U2. No se aportó URL publicada de este indicador. | Sin evidencia de publicación U2 | Publicar documentación navegable; los Markdown locales son evidencia técnica, no prueba de publicación. |
| Topics académicos y trazabilidad remota | Rama local `u2-duracion-miguel`; U1/U2 de Disponibilidad aún figuran sin seguimiento en Git, además del .gitignore ya modificado. | Parcial/no verificado remoto | No se comprobaron topics remotos ni se publicaron commits. Los cambios locales aún no son una entrega remota versionada. Se respeta la orden de no commit/push. |
| PDFs/reflexiones de S6–S11 | Informes técnicos Markdown. | Sin evidencia personal completa | No constan todos los PDFs nominados y reflexiones personales solicitados por las sesiones. No confundir este informe automatizado con una reflexión o defensa del estudiante. |

El ejemplo didáctico usa herramientas y fuentes concretas. Se documentan las
diferencias; solo el docente puede aceptar una equivalencia cuando el mínimo de
sesión es más específico que el criterio general. No se añade sensor, servicio,
Pushgateway, modelo ni integración de compañeros para ocultar esos pendientes.

## Resultados, cobertura y relojes

[Tabla completa por tipo/horizonte y percentiles](resultados_y_latencias.md),
[datos de auditoría](resultados.json), [madurez al cierre](cobertura_al_cierre.json).
Son consultas READ ONLY sobre los dos flow_id explícitos. Se muestra tanto la
foto al terminar cada prueba como la auditoría posterior, para no penalizar
pronósticos todavía inmaduros ni ocultar los maduros bloqueados.

La prueba de etapa 3 cubre 03:50–04:20 UTC del 19/11/2026; sus pares evaluados
proceden de una parte anterior de esa ventana, detallada por horizonte. La prueba
corta de etapa 4 cubre 15:16–15:23 UTC y **no tiene MAE/RMSE/sesgo**. En su cierre
todas sus predicciones eran inmaduras: cobertura madura NULL. En la auditoría
posterior son maduras sin avance: cobertura 0 %, sin error predictivo medible.

Captura→SQL en etapa 3: mediana **28,195 s**, p95 **226,061 s**, n=410.514
predicciones; en etapa 4 **25,292 / 33,190 s**, n=73.278. Antigüedad del feed por
captura: **42,090 / 74,400 s**, n=28, y **28,102 / 74,952 s**, n=5. El documento
separa también intervalos de consulta, marca Spark y demora de evaluación, con
su granularidad y periodo. No se usa la duración completa del ensayo como latencia.

## Fallos técnicos demostrados, no corregidos funcionalmente en este cierre

1. **Cobertura del panel:** `generar_dashboard.py`, consulta `cobertura`, divide
   evaluadas entre `count(*)`, incluyendo inmaduras. En el cierre corto daba 0 %
   con denominador maduro cero: debería ser NULL según la definición solicitada.
   El informe calcula la cobertura correcta; el panel queda pendiente de corregir.
   No se cambian estados ni tolerancias para aumentar el porcentaje.
2. **Tasas técnicas negativas:** [promql_tasas.json](promql_tasas.json) registra
   tres muestras negativas tanto de ACK/s como consultas/s entre 15:10–15:40 UTC.
   Primera a **15:22:20 UTC**: ACK/s = **−43,8087**, que no es una tasa válida de
   entregas confirmadas. [contador_discontinuo.json](contador_discontinuo.json)
   conserva la ventana: **7557 → NaN → 2519** entre dos productores registrados
   con las mismas etiquetas. La discontinuidad de reinicio/datos desconocidos
   afecta el cálculo; no representa entregas físicas negativas. Revisar la
   exposición/consulta de counters ante reinicios sin rellenar desconocidos con
   cero ni introducir etiquetas de alta cardinalidad. Las métricas SQL no dependen
   de esa tasa y siguen reconciliadas.
3. **Periodo efectivo en la tabla de errores:** la consulta `metricas` devuelve
   n/MAE/RMSE/sesgo, sin min/max de orígenes u objetivos realmente evaluados.
   El dashboard muestra flow_id y rango solicitado, y las consultas separan bien
   los flujos; falta mostrar explícitamente el intervalo efectivo de muestras.
   Una captura aislada del panel sin sus variables puede atribuirlo al ensayo
   equivocado. El informe y las URL reproducibles identifican el periodo real.

4. **Cierre del exportador HTTP:** `servicios_finales.json` registra exit_code
   **137** y OOMKilled=false para `citibike-disponibilidad-metricas`. No se presenta
   como cierre limpio. Es compatible con una terminación forzada tras la espera
   de Docker; `servir_metricas.py` no implementa un manejador SIGTERM explícito y
   el proceso es PID 1 sin init configurado. La causa exacta de la señal no se
   instrumentó. El exportador es de lectura y se conservaron archivos/volúmenes;
   queda pendiente verificar/corregir el apagado controlado, sin hacerlo aquí.

No se modificó código de producción, dashboard, esquema, checkpoints ni U1 para
corregir estos puntos: la instrucción actual es auditar sin añadir capacidades.
Su estado no se oculta con una declaración de cumplimiento total.

## Inspección visual y evidencia personal

[Revisión visual](revision_visual.md): Firefox **160.0** real en modo headless,
perfil temporal aislado, login sin exponer clave, imágenes efectivamente vistas.
Se comprobaron filtros, fechas UTC, tabla ampliada, NULL, periodo sin datos y
DETENIDO con telemetría reciente. No se presenta API como inspección visual.
El primer intento llegó solo a login y se conserva identificado como tal.

Las imágenes técnicas automatizadas no incluyen reloj del escritorio ni el perfil
personal del estudiante y **no sustituyen** sus capturas académicas. Las sesiones
piden autoría visible y coherencia de fechas con el historial; no se fabricó esa
evidencia ni se hicieron commits para simularla.

| Subaspecto de sustentación | Evidencia actual | Estado / pendiente personal |
|---|---|---|
| Aporte individual | Contratos, código y resultados de esta dimensión. | Falta demostración de autoría y explicación propia. |
| Comunicación y orden | README y secuencia técnica reproducible. | Falta presentación observada y ajuste al tiempo. |
| Presentación personal/actitud | No evaluable desde el repositorio. | Corresponde al estudiante en la sustentación. |
| Repositorio/estándares | Estructura, tests e ignorados; cambios sin publicar. | Verificar topics, commits autorizados, reproducibilidad y entrega remota. |
| Documentación navegable publicada | Markdown local, sin evidencia de publicación U2. | Publicación y navegación accesibles al evaluador. |
| Pitch/demo | No consta diapositiva o material ejecutivo de Disponibilidad U2. | Preparar evolución desde U1 y demo técnica propia; coordinar las diapositivas de equipo. |

S12 orienta una apertura grupal de alcance/arquitectura/asignación y una demo y
preguntas por integrante. Preparar esa entrega y las reflexiones personales;
no basta con mostrar que Docker inicia. No se evaluó la prueba teórico-práctica.

## Qué se ejecutó y conservación

- Reejecución necesaria: cuatro tests Spark existentes, 26,356 s, todos correctos;
  trece SQL reutilizados con evidencia vigente. [Inventario de las 17](pruebas_omitidas.md).
- Consultas SQL READ ONLY de resultados, cobertura y percentiles; consultas
  históricas Prometheus y revisión Firefox. No hubo capturas API nuevas ni Kafka
  productor ni evaluador activo. El colector solo actualizó la tabla propia de estado.
- Se iniciaron exclusivamente PostgreSQL, Grafana, Prometheus y exportador de esta
  dimensión para leer sus datos. Spark de pruebas finalizó. Los colectores y
  navegadores temporales se cerraron. El exportador HTTP terminó con 137 sin OOM;
  PostgreSQL, Grafana y Prometheus con 0. Kafka-exporter conserva código 2 de un
  estado anterior y no se inició en esta auditoría; no se declara su arranque
  actual verificado. [Estado final](servicios_finales.json).
- [Conservación](conservacion.json): U1 y núcleo de producción comprobados; no se
  alteran datos originales ni dimensiones ajenas. Se conservan volúmenes y checkpoints.
- README actualizado con inicio, verificación y parada. No se hizo commit ni push.

Las limitaciones de operación continua, carga, HA, backup/restauración y exactitud
física siguen declaradas. No todas son criterios explícitos de esta rúbrica, pero
impiden extrapolar los ensayos acotados a un servicio productivo garantizado.
