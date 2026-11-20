# Auditoría actualizada: correcciones demostradas de U2

Estado técnico: correcciones implementadas y verificadas localmente. **No se declara
cumplimiento académico total**: siguen pendientes fuente/dispositivo conforme a la
interpretación docente, integración grupal, publicación autorizada y sustentación.
La auditoría anterior en `reports/cierre/` se conserva como evidencia histórica.

## Evaluación y dashboard

Definición única en `src/disponibilidad_u2/consultas_evaluacion.py`, utilizada por
el dashboard y los informes. Numerador = evaluadas maduras; denominador =
predicciones cuyo plazo terminó. Las inmaduras se excluyen; sin denominador el
porcentaje es NULL. Se distinguen pendientes inmaduras, maduras sin avance,
finalización demostrada del flujo y cerrables. Las vencidas se desglosan por banda,
calidad futura, trayectoria, ambigüedad y otras causas, sin solapar motivos.

La migración aditiva `sql/003_cierre_evaluacion.sql` añade fechas de ejecución y
marcas de confirmación. No modifica `001`, reglas U1 ni decisiones históricas.
El corte fin_prueba utiliza solo datos/confirmaciones disponibles entonces; el
corte auditoria_actual usa un instante común en cada informe. No se mezclan
versiones ni se seleccionan automáticamente carpetas recientes.

El dashboard conserva el UID y ahora tiene **17 paneles**: el adicional resuelve
el pendiente batch de S11. La tabla de errores declara n/alcance y periodos efectivos
de origen/objetivo; la de cobertura contiene numerador/denominador y ambos cortes.
Los errores no se presentan como representativos de todas las predicciones.
La consulta inicial excedió30s: se corrigió el filtro temprano por flujo y se
separó la lectura de tiempos nativos del JSON. No se amplió timeout ni memoria. En la revisión visual apareció además un timeout
para la última captura: el plan integraba el CTE y repetía la frontera temporal
por predicción. Materializar esa frontera una vez redujo la consulta de timeout
a30s a3,83s, con conteos idénticos. Se conservan planes y respuestas
`plan_cola_antes.json`, `plan_cola_despues.json`, `consulta_cola_antes.json` y
`consulta_cola_despues.json`. Al cierre2451 eran inmaduras/denominador0; al
auditar2451 eran maduras sin avance tras apagado/denominador2451, sin error
estimable.
[Verificación SQL/API y comparación de tasas](verificacion.json).

## Prueba continua y cohorte congelada

Flujo `disponibilidad-correcciones-20261119-v1`, referencia explícita en
`config/ejecucion_u2_seleccionada.json`, selección U1 `20261117T171917344468Z`.
**27 capturas / 68.013 observaciones / 397.050 predicciones**. Consulta configurada
60s/TTL60, Spark local[2]. Se conservan checkpoints/salidas de etapas anteriores.

Cohorte primaria: `8ff5bace-0ecf-499d-aa8a-bf43d57a3a0a`, origen
19/11/2026 22:48:04.779 UTC. Se mantuvo captura hasta superar su plazo máximo
**23:13:34.779 UTC** (15min+30s+600s), avanzar la fuente y resolver los seis grupos.
Cierre de procesos **23:17:26.702 UTC**; la última captura emitida fue
**23:16:04.412 UTC**. No se cambió la cohorte por sus resultados.

Por tipo, a5/15min: 2450 evaluadas/2451 maduras, cobertura99,9592%; un caso queda
vencido por trayectoria/inactividad. A10min: cero evaluadas y2451 vencidas por banda,
cobertura0%, MAE/RMSE/sesgo NULL. El candidato anterior dista30,084s y el siguiente
34,521s; no se redondearon timestamps ni ampliaron ±30s. La captura continuada
no puede recuperar un instante pasado que la API no entregó en banda.

[Resultados, motivos, periodos y latencias](informe.md), [datos completos](resultados.json).
Las predicciones posteriores y la última captura se muestran separadas de la
cohorte. Otras cohortes maduras del mismo flujo tienen evaluaciones a10min, pero
no se usan para ocultar la cobertura cero de la cohorte primaria. Se mantienen
estratos con/sin cambios y discrepancias en JSON. Son conteos reportados; la
exactitud física sigue sin confirmarse. No hubo entrenamiento ni selección nueva.

## Observabilidad y operación

| Pendiente demostrado | Corrección | Evidencia | Estado |
|---|---|---|---|
| Counter desconocido NaN y reset | Se omite la muestra desconocida; no se inventa0. rate histórico filtra NaN antes del cálculo, sin clamp de tasas. | `verificacion.json`: original tiene tres negativos, corregida cero; metadata counter. | Verificado. |
| Exportador HTTP terminó137 | SIGTERM/SIGINT atendido; shutdown desde hilo distinto de serve_forever y cierre del socket. | `reinicio_exportador.json`: exit0, OOM=false, alerta firing durante parada y resuelta al volver. | Verificado real. |
| Periodos/denominadores ausentes | Campos explícitos y dos cortes, tabla ampliada, errores solo del subconjunto. | Consultas versionadas, resultados JSON y revisión visual. | Verificado SQL/API y visualmente con navegador real; `revision_visual.md`. |
| Triggers sin comparación | Replay de tres eventos API originales, sin tocar cantidades/fechas; 10s y30s. | `replay_s8.json`: batchId4 frente1 a45s, tres eventos únicos y un tardío descartado por modo. | Verificado con transporte controlado por archivos. |
| Alertas propuestas incompletas | Tres reglas con umbrales ligados a TTL/cadencia/volumen medidos. | `observabilidad/alertas.yml`; promtool y prueba real del target down. | La primera se provocó; no se indujo fallo de negocio para las otras dos. |
| KPIs batch S11 | Media de bicicletas y fracción de estaciones elegibles sin bicicletas; n/fecha, Pushgateway1.11.4 fijado por digest. | `kpis_batch.json`, `kpis_verificados_prometheus.json`, panel17. | Publicación solo local; no es stream continuo ni inventario físico actual. |
| Dos consultas PromQL | Ensayo complementario de dos capturas con configs/checkpoints propios y el mismo lanzador. | `promql_en_vivo.json`: ACK2519→5038, predicciones SQL0→14688 durante la ejecución. | Verificado en vivo; prueba corta, no precisión a15min. |

El replay conserva la procedencia y bytes originales; introduce deliberadamente
el orden/duplicado por archivos. No se presenta como retraso espontáneo del broker.
La comparación de batchId incluye microbatches sin datos y **no es benchmark**.
Un intento falló al serializar UUID de progreso Spark4.2: se corrigió únicamente
la serialización del informe, se conservó el intento y se repitió esa prueba.

Los dos ensayos de negocio usan el mismo código, reglas, contratos e infraestructura,
con flow_id y checkpoints distintos. La verificación PromQL no se mezcla con el
periodo ni con el almacenamiento calibrado del ensayo de27 capturas. La medida
física fue congelada antes de ese ensayo complementario.

## Requisitos académicos: qué sí y qué no

Se releyeron [U2](https://262bigdata.github.io/lambda26/proyecto-sello/u2-producto/) y
[S6](https://262bigdata.github.io/lambda26/sesiones/S06_Ingesta_Eventos_Empresariales_Kafka/),
[S7](https://262bigdata.github.io/lambda26/sesiones/S07_Ingesta_Eventos_IoT_Sensores_Tiempo_Real/),
[S8](https://262bigdata.github.io/lambda26/sesiones/S08_Procesamiento_Streaming_Spark_Ventanas_Watermarking/),
[S9](https://262bigdata.github.io/lambda26/sesiones/S09_Observabilidad_Pipeline_Grafana_Costos/),
[S11](https://262bigdata.github.io/lambda26/sesiones/S11_BI_ML_Distribuido_Spark_Grafana/).

| Requisito | Evidencia actual | Estado / pendiente restante |
|---|---|---|
| Empresarial/S6 | Estado operativo del dominio de estaciones, contrato, tópico, ACK y consumo. | Parcial: no son eventos de alquiler/venta ni cadena de microservicios. Justificación de negocio sí; equivalencia docente pendiente. No se inventó fuente transaccional. |
| IoT/S7 | Feed GBFS real de tercero, campos/tipos verificados. | No demuestra sensor propio, MQTT o alerta de rango físico real. La guía general admite telemetría como ejemplo; el mínimo S7 pide dispositivo. No se declara satisfecha esa diferencia. |
| Spark/S8 | Flujo API/Kafka real con ventanas/checkpoints y verificación controlada de triggers/duplicados/tardíos reales. | Evidencia técnica; se identifica el transporte del replay y falta sustentación/capturas personales. |
| Observabilidad/S9 | Reglas, métricas corregidas, dashboard y prueba de parada/reinicio. | Implementado localmente; faltan notificación, carga sostenida y demostración personal, sin declararlas obligatorias todas para la rúbrica. |
| Costos/S9 | Precios oficiales fechados y calibración física medida. | Estimación cuantificada USD144/mes en escenario, gasto cloud nuevo real0; hipótesis/retención/operación declaradas. No es compra ni SLA. |
| BI/S11 | KPIs batch Pushgateway y evolución PromQL/SQL durante ensayo real. | Verificado localmente; coordinación del tablero compartido y defensa del estudiante pendientes. |
| Inferencia | Persistencia seleccionada U1, seis contratos y evaluación futura/cobertura. | Verificada experimentalmente; referencias MLlib siguen diferenciadas. Cobertura0 a10min en cohorte primaria limita la precisión estimable. |
| Publicación | MkDocs propio construido con --strict, fuentes y dependencias fijadas. | Preparado, **no publicado por instrucción**. No cambia el MkDocs del equipo ni el brief. |
| Sustentación | Guía personal y propuesta de actualización del brief. | Autoría, PDFs/reflexiones, pitch, topics/commits remotos y exposición no acreditados por automatización. |

## Costos y documentación publicable

[Costos cuantificados](../../costos.md), fuente oficial
[AWS Lightsail](https://aws.amazon.com/lightsail/pricing/) consultada19/11/2026.
Escenario: VM Linux16GB/4vCPU USD84, bloque500GB USD50 y snapshot200GB USD10,
total **USD144/mes**, sin impuestos/operación/sobreconsumos. Se asume región,
tráfico y retención; no se contrata nada. El gasto cloud nuevo real es0; electricidad
local no medida. El incremento físico aproximado53,238MiB/captura reemplaza el
supuesto no calibrado anterior, conservando sus límites (índices/bloat, sin WAL).

`mkdocs-u2.yml` y `documentacion/` son fuentes versionables; el sitio generado se
ubica bajo `data/publicacion/site`, ignorado. Build estricto verificado, sin publicar.
La documentación distingue estados reportados de sensores/transacciones, recoge
la propuesta de brief sin aplicarla y mantiene separadas las evidencias personales.

Conservación y apagado final: [conservacion.json](conservacion.json) y
[servicios_finales.json](servicios_finales.json). U1 y dimensiones ajenas intactas,
sin commits ni push; todos los procesos propios de prueba detenidos y volúmenes
incluido el nuevo de Pushgateway conservados.

Revisión visual técnica: [resultados y capturas](revision_visual.md). Conservación de U1/núcleo y secretos: [conservacion.json](conservacion.json). Construcción local sin publicación: [documentacion_local.json](documentacion_local.json).

## Estado final y limitación operativa restante

Se detuvieron los siete contenedores propios y los procesos temporales; los cuatro
servicios ajenos observados siguen activos. Cinco volúmenes y checkpoints intactos.
El exportador HTTP propio terminó0, OOMKilled=false. Kafka terminó143; el exporter
Kafka de terceros terminó2 al stop de Compose, OOMKilled=false. Los últimos logs
no explican el código2; no se atribuye a OOM ni se afirma salida limpia0 para ese
binario. Queda documentar su convención de salida si se exige normalizarla; no se
reinició ni cambió su implementación fuera del pendiente HTTP demostrado.
[Estado preciso](servicios_finales.json).

Los USD144/mes son solo estimación documental. No se crearon, contrataron ni
desplegaron recursos cloud de pago y no se activaron servicios de pago.
