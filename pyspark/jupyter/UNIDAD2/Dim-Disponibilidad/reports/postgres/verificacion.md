# Verificación U2 etapa 3: PostgreSQL y evaluación diferida

Ejecución real del **19 de noviembre de 2026 UTC** (18 de noviembre en Lima).
Flujo explícito: `disponibilidad-etapa3-vivo-20261119-v1`.
Configuraciones: [streaming-etapa3.json](../../config/streaming-etapa3.json) y
[postgres-verificacion.json](../../config/postgres-verificacion.json).
Sin entrenamiento nuevo, descargas históricas ni Grafana.

## Evidencias y periodo real

- [Productor real](produccion_real.json): 28 capturas, **70.532 ACK Kafka**,
  2.519 estaciones por captura, cero fallos de HTTP/publicación y cero reintentos.
- [Reconciliación ejecutada](verificacion_real.json): Parquet/SQL, contratos,
  calidad, estados, cobertura, métricas por ventana y estratos, relojes y muestras.
- [Trece pruebas sintéticas](pruebas_sinteticas.json): ejecutadas en PostgreSQL
  real sobre flujos `fixture-*`, separados del flujo API y con reloj simulado.
- [Reinicio](reinicio.json), [antes](antes_reinicio.json),
  [después](despues_reinicio.json): conteos y huellas de contenido completo SQL.
- [Aplicaciones, configuraciones, hashes y errores](ejecuciones.json).
- [Conservación de U1/etapa 2](conservacion.json) y
  [estado final de servicios](servicios_finales.json).

Capturas desde **2026-11-19 03:50:03.928 UTC** hasta
**2026-11-19 04:20:00.139 UTC**: **29 min 56,211 s** observados. Supera 15 min +
30 s de tolerancia + 600 s de tardíos, sin adelantar el reloj ni fabricar futuros.
No es una prueba semanal/estacional ni una nueva comparación de modelos.

Se recargaron los seis contratos originales de persistencia seleccionados en U1
**20261117T171917344468Z**, con SHA, versión y procedencia. El catálogo GBFS real
siguió siendo 1 bicycle/human y 2 bicycle/electric_assist. Los objetivos siguen
siendo conteos explícitos reportados por tipo, **sin confirmación de exactitud
física**. Los modelos MLlib de referencia U1 no se reentrenaron ni sustituyeron
a persistencia.

## Persistencia, reconciliación y cobertura

| Comprobación | Resultado |
|---|---:|
| Observaciones SQL, coincidentes con Parquet | 70.532 |
| Rechazos totales o parciales, incluidos también en observaciones | 2.113 |
| Predicciones SQL | 410.514 |
| Predicciones comparadas individualmente contra Parquet | 410.514 |
| Diferencias de ID/valor/origen/objetivo/tipo/horizonte/versión/contrato | 0 |
| Diferencias de calidad actual y valor de persistencia | 0 |
| Filas de estado de evaluación (una por predicción) | 410.514 |
| Lotes SQL confirmados | 27 |
| Evaluadas | 107.534 |
| Vencidas sin resultado válido | 19.572 |
| Pendientes, aún sin cierre por la barrera temporal/de progreso | 283.408 |
| Pendientes **ya cerrables** sin resolver | 0 |

Reconciliación: **107.534 + 19.572 + 283.408 = 410.514**. Cobertura evaluada sobre
todas las predicciones: **26,195 %**. La cola futura de un ensayo de media hora es
grande por los horizontes y el margen de 10 minutos: no se fuerza a vencer al
detener la API. Sin nuevas capturas que hagan avanzar el frontier seguirá pendiente.

Los vencimientos se desglosan en **19.554 sin captura en ±30 s** y **18 con
objetivo no elegible**. La cadencia de consulta de 60 s más red crea separaciones
mayores de 60 s; ciertas fechas ideales quedan fuera de todas las bandas.
No se amplió la tolerancia para mejorar cobertura ni se escogió una etiqueta
inválida como cero. Tampoco se saltó el candidato más cercano para elegir uno
con mejor calidad. En esta muestra ambos tipos tuvieron la misma cobertura.

| Horizonte | Predicciones por tipo | Evaluadas por tipo | Vencidas por tipo | Pendientes por tipo |
|---|---:|---:|---:|---:|
| 5 min | 68.419 | 21.996 | 9.779 | 36.644 |
| 10 min | 68.419 | 21.995 | 4 | 46.420 |
| 15 min | 68.419 | 9.776 | 3 | 58.640 |

La primera cohorte cerró las seis combinaciones: **2.444 evaluadas y una vencida
por objetivo no elegible en cada combinación**. Las 2.445 predicciones de cada
tipo/horizonte de esa cohorte quedaron resueltas. Esto verifica específicamente
el horizonte de 15 minutos con futuros reales y margen completo.

## Métricas reales de persistencia

Error firmado = predicho − observado. Presentación a cuatro decimales;
[JSON](verificacion_real.json) conserva los valores sin redondear y todos los
estratos/ventanas UTC de origen de cinco minutos. No se redondearon predicciones
para calcular errores ni se seleccionó un nuevo horizonte con estos resultados.

| Tipo | Horizonte | n evaluadas | MAE | RMSE | Sesgo |
|---|---:|---:|---:|---:|---:|
| Convencionales | 5 | 21.996 | 0,0526 | 0,2463 | 0,0020 |
| Convencionales | 10 | 21.995 | 0,0971 | 0,3558 | 0,0065 |
| Convencionales | 15 | 9.776 | 0,1385 | 0,4418 | 0,0068 |
| Eléctricas | 5 | 21.996 | 0,1684 | 0,4946 | −0,0073 |
| Eléctricas | 10 | 21.995 | 0,2973 | 0,7051 | −0,0133 |
| Eléctricas | 15 | 9.776 | 0,3918 | 0,8481 | −0,0223 |

Se conservan periodos estables y con cambios. A 15 minutos:

| Tipo | Sin cambios: n / MAE | Con cambios: n / MAE |
|---|---|---|
| Convencionales | 8.619 / 0 | 1.157 / 1,1703 |
| Eléctricas | 6.995 / 0 | 2.781 / 1,3772 |

El error cero del subconjunto estable es consecuencia del contrato de
persistencia. El agregado no describe por sí solo la capacidad de anticipar cambios.

Se propagaron los indicadores de discrepancia de origen y objetivo. Ejemplo a
15 minutos (OR de las tres banderas U1 de ambos instantes):

| Tipo | Sin discrepancias: n / MAE | Con discrepancias: n / MAE |
|---|---|---|
| Convencionales | 9.411 / 0,1267 | 365 / 0,4438 |
| Eléctricas | 9.411 / 0,3595 | 365 / 1,2247 |

Los mayores errores del estrato discrepante son una observación de este ensayo;
no establecen causalidad ni cuál campo es físicamente correcto. La implementación
también contempla el estrato desconocido. No se descartaron todas las discrepancias.

## Frecuencia, antigüedad y latencias separadas

Consulta configurada: **60 s**, respetando TTL 60 s. Intervalo real entre capturas:
mínimo **62,013 s**, media **66,526 s**, máximo **75,392 s**. La actualización
last_updated tuvo deltas observados **0, 59, 60, 61, 71 y 120 s**. No se confunde
intervalo de consulta con actualización de fuente ni se afirma cadencia universal.

| Medida | Resultado |
|---|---|
| Antigüedad captura − last_updated del feed | 6,132–99,240 s; media 40,931 s |
| Antigüedad captura − last_reported, predicciones elegibles | Media 107,399 s |
| Captura → confirmación post-commit SQL | Mín. 17,566 s; media 55,378 s; P95 226,061 s; máx. 288,074 s |
| Predicciones confirmadas después de su fecha objetivo | 0 en este ensayo |
| Fecha objetivo → evaluación SQL confirmada | Mín. 675,145 s; media 882,266 s; máx. 1.077,059 s |

La marca post-commit es una cota superior de primera visibilidad, no una medición
del instante interno exacto de commit. El retraso de evaluación incluye la espera
deliberada por tolerancia/tardíos y el backlog. La media captura→SQL incluye una
interrupción/reinicio durante el ensayo: **no se presenta como SLA ni como operación
ininterrumpida**. La duración total de la prueba no se usa como latencia online.

## Reinicio, errores encontrados y correcciones

Los [snapshots](reinicio.json) de las seis tablas mantuvieron **conteos y hashes
de todo su contenido**, incluidos valores y marcas de confirmación. El reinicio
posterior al snapshot inicial se ejecutó sin nuevas capturas, con el mismo query id
**3b74fd62-a69f-46fe-9d0e-98f2a34dfe61** y otro runId. No procesó nuevas filas.
Se conservaron tanto el volumen SQL como los checkpoints. El hash es evidencia
de comparación de contenido, no una garantía de entrega distribuida exactly-once.

Las huellas recorren el contenido completo de las tablas en el PostgreSQL limitado
a una CPU; son comprobaciones de integridad fuera de la ruta de ingesta. Su
duración no se mezcla con la latencia de publicación de predicciones.

Se conservaron los intentos anteriores y sus manifiestos originales:

1. Un `docker stop` al JVM terminó Python con exit 143 sin cerrar su informe
   (`e0b0e5e7-...`, estado original en_curso). Se añadió
   [detener_streaming.py](../../detener_streaming.py) para señalar al Python propio;
   los cierres siguientes con ese helper terminaron correctamente.
2. La aplicación `14bb764f-...` falló con **55P03, lock timeout**: el evaluador
   esperaba un lock de ingesta. El cierre mostró también StackOverflowError en
   Java. Se cambió a adquisición no bloqueante del evaluador y rondas de 4.000.
   La nueva prueba de concurrencia pasó. El lote 22 ya confirmado se recuperó
   como reutilizado tras reiniciar, sin insertar de nuevo sus registros.
3. La primera reconciliación `f1541ac0-...` agotó **57014, statement timeout** al
   comparar calidad masivamente. Se materializan los indicadores del origen una
   sola vez y se comparan objetos completos. La verificación posterior
   **8a730302-6e46-48f4-9794-ceae8f8f0249** terminó correctamente, conservando el
   timeout de 90 s y sin aumentar memoria ni cambiar los servicios del equipo.

La carga SQL utiliza bloques de 500 y una transacción por microbatch; las
observaciones/rechazos/predicciones/pendientes/recibo se confirman juntas.
Constraints, recibo por batch_id y checkpoints cubren reintentos bajo los supuestos
documentados. No se simularon pérdida de disco ni todos los puntos de caída.

## Pruebas sintéticas y conservación

**Trece pruebas Spark/JDBC/SQL correctas**, incluidas:

- Fronteras −30/+30 s inclusivas, fuera de banda por 1 ms y desempate anterior.
- Espera de margen y de progreso; futuro ausente sin imputación a cero.
- Objetivo más cercano inválido sin saltarlo por otro; contrato inválido con fecha
  válida no oculta la captura; reloj inválido se conserva en payload.
- Llegada tardía previa al cierre, cierre inmutable, huecos e inactividad intermedia.
- Métricas conocidas (MAE/RMSE 3, sesgo −3), discrepancias, rollback y colisiones.
- Sink Spark real sobre nueve fixtures de calidad: 18 pronósticos; repetir batch
  mantiene datos y confirmación. Evaluador con lock ocupado aplaza sin error.

Se volvieron a ejecutar las 21 pruebas Python de ingesta, correctas. En host se
omiten las Spark porque PySpark se ejecuta en Docker; no se presenta esa omisión
como prueba ejecutada. La evidencia efectiva SQL/Spark es la de Docker.

[Conservación](conservacion.json): **10.169 archivos U1** con mismo tamaño/mtime,
sin altas/bajas; mismo SHA de Dockerfile/Compose del equipo y de los cuatro
archivos de implementación de etapa 2 protegidos. El lanzador se amplió con
`--sql`, conservando las opciones anteriores y el default local[4]. Esta ejecución
usó **local[2]**, driver 2 GiB, sin atribuir sus tiempos a local[4].

El productor y las aplicaciones Spark propias terminaron. Kafka y PostgreSQL
propios quedaron detenidos, con volúmenes y cinco checkpoints de etapas 2/3
conservados. El contenedor Jupyter y los servicios Cloudshield siguen activos.
No se hicieron commits ni push.

## Reproducir y límites

[postgres.md](../../postgres.md) contiene setup, esquema, configuración pública,
comandos de ejecución/consulta/pruebas y parada. Para releer esta entrega, iniciar
únicamente PostgreSQL propio y verificar la configuración seleccionada:

```bash
docker compose -f pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/compose.postgres.yml up -d postgres-disponibilidad
.venv/bin/python pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/lanzar_streaming.py --sql --config pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/config/streaming-etapa3.json --config-sql pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/config/postgres-verificacion.json --verificar-sql --informe reports/postgres/relectura.json
docker compose -f pyspark/jupyter/UNIDAD2/Dim-Disponibilidad/compose.postgres.yml stop postgres-disponibilidad
```

Requiere conservar volumen/secreto, red propia, JAR cacheados, Parquet y originales
U1. No necesita la API ni nuevas capturas; no iniciar de nuevo el productor para
intentar obtener los mismos capture_id.

La prueba cubre media hora y la muestra de la madrugada UTC. No demuestra
generalización horaria, semanal o estacional ni tolerancia ilimitada a tardíos.
Cobertura es parte del resultado: los errores se calculan solo donde hay futuro
válido. Un flujo detenido no convierte la cola pendiente en observaciones faltantes
definitivas. El régimen de consulta difiere del histórico U1; no sustituye aquella
evaluación ni permite reseleccionar modelos/horizontes con este ensayo.

Grafana, panel de negocio y operación sostenida siguen pendientes. Para la
sustentación, el estudiante debe capturar personalmente las terminales de productor,
Spark/SQL, consultas de estados/métricas y recuperación; explicar tres relojes,
margen tardío, cobertura, estados estables/con cambios, discrepancias y límites de
las garantías. Los JSON técnicos no sustituyen esa demostración personal.
