# Verificación de ingesta U2 — datos reales y pruebas sintéticas

Ejecución real final: **`2026-11-18T02-29-24.060Z-398970a0`**, UTC. [Manifiesto público acotado](verificacion_real.json). Los IDs de abajo identifican las capturas, sin depender de la carpeta más reciente.

## Publicación y consumo reales

| capture_id | Captura UTC | last_updated UTC | Estaciones de origen | ACKs | Consumidos únicos | Duplicados observados |
|---|---|---|---:|---:|---:|---:|
| `e6cbdb35-dc80-4165-8cd2-78d4a583227e` | 2026-11-18T02:29:31.298Z | 2026-11-18T02:28:37.000Z | 2519 | 2519 | 2519 | 0 |
| `4e1ddc61-450a-45f2-ab94-35b8c98ec3e1` | 2026-11-18T02:30:33.258Z | 2026-11-18T02:29:36.000Z | 2519 | 2519 | 2519 | 0 |

Grupo propio: `citibike-disponibilidad-verificacion-f6aaa399-365b-487b-bb29-f1727bb3efb3`. Topic `citibike-disponibilidad-estaciones-v1`, tres particiones 0/1/2. El grupo se suscribió y resolvió sus offsets al final antes de publicar. Se verificaron contrato, identidad, claves station_id, relojes, referencias de catálogos y hash de los IDs de estaciones. No se modificaron offsets de otros grupos.

Se confirmaron **5038 eventos**, sin eventos pendientes, fallos HTTP/publicación ni reintentos en este ensayo. Hubo 5 consultas: índice, ambos catálogos y dos status; no hubo consultas por estación.

Separación observada: **61.960 s**, con configuración de prueba 60 s y TTL 60. El reloj last_updated avanzó **59 s**. El valor normal del productor sigue siendo 300 s. Dos capturas no demuestran una cadencia constante de toda la fuente.

Hubo 2519 estaciones comunes y **2333** mantuvieron el mismo array de conteos por tipo. Sus eventos se conservaron con capture_id/event_id distintos. La partición se mantuvo por estación. Igualdad entre extremos no prueba ausencia de movimientos físicos intermedios.

No se observaron duplicados durante esta ventana de consumo; no se demuestra exactly-once ni ausencia de duplicados posteriores. Los ACKs de una sola réplica tampoco acreditan tolerancia a pérdida del broker/disco.

## Verificaciones adicionales

- [Consumo individual posterior](verificacion_individual.json): otro grupo propio recuperó la primera captura final, 2.519 eventos, sin consultar la API ni publicar más.
- [Sondeo de fuente](fuente_gbfs.json): índice oficial, endpoints descubiertos, versión, TTL, campos y muestra. Es una consulta separada sin publicación.
- [Tópico desde el host](topico.json) y [desde Docker](topico_desde_docker.txt): conectividad, tres particiones, réplica 1, retention.ms=86400000 y retention.bytes=1073741824 por partición.
- [Evento realmente consumido](../contratos/ejemplo_evento_real.json): estado original y los tres relojes, no una muestra sintética.
- El manifiesto registra Python 3.14.10 y kafka-python 3.0.11, configuración, hashes de código y referencia explícita U1. Los seis contratos originales U1 fueron comprobados sin entrenar ni inferir.

## Pruebas sintéticas

**21 pruebas OK**, última ejecución en 0,174 s: muestras artificiales de dos estaciones, timestamps, catálogos, IDs, valores ausentes/cero, reintentos limitados, recuperación de outbox, bloqueo de otro productor, TTL, espera desde captura, tópico existente y resolución anticipada de offsets. Las fallas de entrega y recuperación se simularon; no se indujo una caída real del broker ni un corte abrupto del proceso.

## Hallazgos corregidos durante implementación

1. El volumen nuevo propio era root y Kafka usa UID 1000: se añadió una inicialización de permisos limitada a ese volumen. El broker arranca sin tocar almacenamiento ajeno.
2. kafka-python 3.0.11 describe particiones con replica_nodes y requiere solicitar explícitamente las claves de configuración. Se adaptó al formato observado, sin alterar propiedades del tópico.
3. El [primer ensayo](verificacion_inicial_intervalo.json) publicó y consumió otros 5.038 eventos, pero el arranque de catálogos acortó la separación a 53,563 s. Se conservó ese ensayo y se corrigió la espera para medir desde la recepción del status, con prueba unitaria y repetición acotada. En total se publicaron cuatro capturas de desarrollo, 10.076 eventos; solo las dos de la tabla son la verificación final de cadencia.

Los intentos fallidos conservan manifiestos locales en data/ejecuciones; no se presentan como verificados.

## Estado al terminar y límites

Los productores y consumidores de prueba terminaron; no se dejó el productor continuo ejecutándose. El broker propio se detuvo después de las comprobaciones y su volumen se conserva para reanudar con los comandos del README. El Kafka externo detenido, PySpark y los otros servicios no se modificaron.

No se implementaron Spark Structured Streaming, calidad/inferencia en vivo, evaluación diferida, PostgreSQL ni Grafana. Los conteos son reportes del operador, no verdad física validada. Las obligaciones restantes de U2 y la sustentación personal se detallan en el README.
