# Resultados y latencias de cierre U2

Fuentes: consultas **READ ONLY** conservadas junto a este informe:
[consulta_resultados.sql](consulta_resultados.sql),
[consulta_cobertura_al_cierre.sql](consulta_cobertura_al_cierre.sql),
[consulta_latencias.sql](consulta_latencias.sql). Se consultaron únicamente dos
flow_id explícitos; no se mezclaron fixtures, corridas ni versiones U1.
Valores completos: [resultados.json](resultados.json),
[cobertura_al_cierre.json](cobertura_al_cierre.json), [latencias.json](latencias.json).
No se ejecutó el evaluador ni se modificaron estados SQL.

## Definición de cobertura

Plazo terminado: `cierre_no_antes_utc <= instante_de_corte`, es decir, fecha
objetivo + tolerancia de 30 s + margen tardío de 600 s. La cobertura principal es
`evaluadas con plazo terminado / predicciones con plazo terminado`. El denominador
**excluye inmaduras** e **incluye pendientes maduras bloqueadas por la fuente**.
Si no ha terminado ningún plazo, el cociente es NULL, no 0 % ni 100 %.
Las barreras de reloj y avance siguen separadas. No se amplió ninguna tolerancia.

La fracción evaluadas/(evaluadas+vencidas) se conserva en JSON como indicador
condicional a casos resueltos; **no es cobertura global**, porque oculta las maduras
sin resolver. Los estados reconcilian sin solapamiento y no hubo cierres anticipados.

## Ensayo real de etapa 3: resultados al auditar

Flujo `disponibilidad-etapa3-vivo-20261119-v1`, 28 capturas y 70.532 observaciones.
Origen completo **19/11/2026 03:50:03.928–04:20:00.139 UTC**. Corte de auditoría:
2026-11-19T16:08:37.119667+00:00. En ese momento todas sus predicciones habían
terminado el plazo. Los errores proceden únicamente de pares futuros elegibles,
sobre conteos reportados por tipo. Sesgo = predicho − observado.

| Tipo | Min | MAE | RMSE | Sesgo | Evaluadas | Vencidas | Inmaduras | Maduras sin avance | Plazo terminado | Cobertura % |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| convencionales | 5 | 0.052555 | 0.246267 | 0.002000 | 21996 | 9779 | 0 | 36644 | 68419 | 32.1490 |
| convencionales | 10 | 0.097068 | 0.355837 | 0.006501 | 21995 | 4 | 0 | 46420 | 68419 | 32.1475 |
| convencionales | 15 | 0.138502 | 0.441783 | 0.006751 | 9776 | 3 | 0 | 58640 | 68419 | 14.2884 |
| electricas | 5 | 0.168394 | 0.494560 | -0.007274 | 21996 | 9779 | 0 | 36644 | 68419 | 32.1490 |
| electricas | 10 | 0.297340 | 0.705062 | -0.013276 | 21995 | 4 | 0 | 46420 | 68419 | 32.1475 |
| electricas | 15 | 0.391776 | 0.848123 | -0.022300 | 9776 | 3 | 0 | 58640 | 68419 | 14.2884 |

Total: **410.514 predicciones = 107.534 evaluadas + 19.572 vencidas + 283.408
pendientes maduras sin avance**. Cero inmaduras y cero pendientes cerrables al
auditar. La parada explica que la barrera de progreso no permita cerrar muchas
predicciones aunque ya haya pasado su plazo. No se oculta ese déficit de cobertura.
[Auditoría de las vencidas](../observabilidad/vencidas.md): 19.554 sin captura en
banda y 18 no elegibles; sin defecto de emparejamiento demostrado.

### Periodo real de los pares evaluados, común a ambos tipos

| Min | Orígenes evaluados UTC | Objetivos ideales UTC | Capturas futuras observadas UTC |
| --- | --- | --- | --- |
| 5 | 2026-11-19T03:50:03.928+00:00 → 2026-11-19T04:03:25.251+00:00 | 2026-11-19T03:55:03.928+00:00 → 2026-11-19T04:08:25.251+00:00 | 2026-11-19T03:54:34.039+00:00 → 2026-11-19T04:08:09.849+00:00 |
| 10 | 2026-11-19T03:50:03.928+00:00 → 2026-11-19T03:59:05.107+00:00 | 2026-11-19T04:00:03.928+00:00 → 2026-11-19T04:09:05.107+00:00 | 2026-11-19T04:00:08.896+00:00 → 2026-11-19T04:09:17.776+00:00 |
| 15 | 2026-11-19T03:50:03.928+00:00 → 2026-11-19T03:53:23.036+00:00 | 2026-11-19T04:05:03.928+00:00 → 2026-11-19T04:08:23.036+00:00 | 2026-11-19T04:04:35.445+00:00 → 2026-11-19T04:08:09.849+00:00 |

No son métricas de U1 ni de la prueba corta de las 15:16 UTC. El número de filas
estación/tipo no equivale a tantos periodos independientes: comparten capturas.
No se deduce precisión semanal ni estacional de esta media hora.

### Cobertura al cierre del informe de etapa 3

Instante histórico **04:34:55.662 UTC**, posterior a todos sus cierres terminales.
Esto contrasta la definición cuando todavía existían predicciones inmaduras:

| Min, cada tipo | Plazo terminado | Inmaduras excluidas | Evaluadas | Vencidas | Maduras sin avance | Cobertura % |
| --- | --- | --- | --- | --- | --- | --- |
| 5 | 65977 | 2442 | 21996 | 9779 | 34202 | 33.3389 |
| 10 | 53766 | 14653 | 21995 | 4 | 31767 | 40.9088 |
| 15 | 43994 | 24425 | 9776 | 3 | 34215 | 22.2212 |

Los conteos son por tipo; convencionales y eléctricas coinciden en estos estados.
El denominador cambia con el instante de corte, no con la elección de otro modelo.

## Prueba corta de etapa 4, independiente de la anterior

Flujo `disponibilidad-etapa4-vivo-20261119-v1`: cinco capturas,
**15:16:31.839–15:23:10.874 UTC**, 12.595 observaciones y 73.278 predicciones.

| Tipo | Min | Evaluadas | Vencidas | Inmaduras al auditar | Maduras sin avance | MAE / RMSE / sesgo | Cobertura actual % |
| --- | --- | --- | --- | --- | --- | --- | --- |
| convencionales | 5 | 0 | 0 | 0 | 12213 | NULL / NULL / NULL | 0.0 |
| convencionales | 10 | 0 | 0 | 0 | 12213 | NULL / NULL / NULL | 0.0 |
| convencionales | 15 | 0 | 0 | 0 | 12213 | NULL / NULL / NULL | 0.0 |
| electricas | 5 | 0 | 0 | 0 | 12213 | NULL / NULL / NULL | 0.0 |
| electricas | 10 | 0 | 0 | 0 | 12213 | NULL / NULL / NULL | 0.0 |
| electricas | 15 | 0 | 0 | 0 | 12213 | NULL / NULL / NULL | 0.0 |

**Al finalizar el ensayo (15:24:30.593 UTC), las 73.278 eran inmaduras y la cobertura
sobre plazo terminado era NULL**, porque el denominador era cero. Horas después
están maduras, bloqueadas por falta de avance, y su cobertura actual es 0 %; no se
transformó ninguna en vencida ni se inventó un resultado futuro. Por tanto la
prueba corta no valida error predictivo a 15 minutos. El panel actual calcula una
fracción sobre todas las predicciones: esa fórmula penaliza inmaduras y queda
señalada como defecto pendiente, no se utiliza como cobertura principal aquí.

## Relojes y latencias

Mediana y p95 de las muestras reales, en **segundos**. No se usa la duración total
del ensayo como latencia. Fuente = captura − last_updated; reporte = captura −
last_reported. Son antigüedades, no tiempo de procesamiento. El intervalo de
consulta se mide entre capturas completas, no entre actualizaciones del feed.

### disponibilidad-etapa3-vivo-20261119-v1

| Medida | n / granularidad | Mediana s | p95 s | Periodo base UTC |
| --- | --- | --- | --- | --- |
| antiguedad_fuente_en_captura | 28 / capturas | 42.090 | 74.400 | 2026-11-19T03:50:03.928+00:00 → 2026-11-19T04:20:00.139+00:00 |
| antiguedad_reporte_en_captura | 70532 / observaciones | 104.139 | 202.240 | 2026-11-19T03:50:03.928+00:00 → 2026-11-19T04:20:00.139+00:00 |
| captura_a_calidad_spark | 70532 / observaciones | 8.701 | 14.578 | 2026-11-19T03:50:03.928+00:00 → 2026-11-19T04:20:00.139+00:00 |
| captura_a_prediccion_sql_confirmada | 410514 / predicciones | 28.195 | 226.061 | 2026-11-19T03:50:03.928+00:00 → 2026-11-19T04:20:00.139+00:00 |
| captura_futura_a_evaluacion_confirmada | 107534 / predicciones evaluadas | 945.946 | 1045.504 | 2026-11-19T03:54:34.039+00:00 → 2026-11-19T04:09:17.776+00:00 |
| intervalo_consulta_incluye_reinicio | 27 / intervalos entre capturas | 65.401 | 73.270 | 2026-11-19T03:50:03.928+00:00 → 2026-11-19T04:20:00.139+00:00 |
| objetivo_ideal_a_evaluacion_confirmada | 107534 / predicciones evaluadas | 944.397 | 1047.719 | 2026-11-19T03:55:03.928+00:00 → 2026-11-19T04:09:05.107+00:00 |

### disponibilidad-etapa4-vivo-20261119-v1

| Medida | n / granularidad | Mediana s | p95 s | Periodo base UTC |
| --- | --- | --- | --- | --- |
| antiguedad_fuente_en_captura | 5 / capturas | 28.102 | 74.952 | 2026-11-19T15:16:31.839+00:00 → 2026-11-19T15:23:10.874+00:00 |
| antiguedad_reporte_en_captura | 12595 / observaciones | 86.402 | 209.849 | 2026-11-19T15:16:31.839+00:00 → 2026-11-19T15:23:10.874+00:00 |
| captura_a_calidad_spark | 12595 / observaciones | 9.021 | 13.828 | 2026-11-19T15:16:31.839+00:00 → 2026-11-19T15:23:10.874+00:00 |
| captura_a_prediccion_sql_confirmada | 73278 / predicciones | 25.292 | 33.190 | 2026-11-19T15:16:31.839+00:00 → 2026-11-19T15:23:10.874+00:00 |
| intervalo_consulta_incluye_reinicio | 4 / intervalos entre capturas | 62.781 | 189.056 | 2026-11-19T15:16:31.839+00:00 → 2026-11-19T15:23:10.874+00:00 |


En etapa 4 no existen muestras de latencia de evaluación: n=0, mediana y p95 NULL.
No se sustituyen por los valores históricos de etapa 3. Los periodos exactos de
confirmación SQL y evaluación están en JSON; la última evaluación confirmada de
etapa 3 fue **04:24:06.361526 UTC**.

La marca `procesado_utc` de Spark se calcula dentro del microbatch; la medida
captura_a_calidad_spark aproxima su marca de procesamiento y **no mide el fin de
escritura completo**. La confirmación SQL se marca después del commit de datos;
es una cota superior registrada de la primera visibilidad, no un cronómetro del
navegador. La demora de evaluación incluye la espera deliberada de 600 s,
tolerancia, progreso y cola de evaluación; no es latencia online de inferencia.

La antigüedad de reporte incluye estados inactivos y valores originales muy
antiguos, con máximos extremos; no se interpreta su máximo como retraso de Spark.
No se recortaron ni sustituyeron esos valores para mejorar percentiles. Los n de
predicciones comparten relojes de microbatch y no son muestras temporales independientes.

La pausa entre los dos ensayos de etapa 4 aparece en su intervalo agregado
(máximo **211,248 s**, p95 189,056 s); no se atribuye a la frecuencia normal de la
API. Dentro de cada ensayo los intervalos fueron 62,263 / 63,300 / 62,224 s:
mediana 62,263 s, p95 63,196 s, **n=3**, solo una caracterización preliminar.

El backlog y el arranque de etapa 2 dieron latencias mayores en su ensayo anterior
(ver `../streaming/verificacion_real.json`); no se mezclan con estas dos poblaciones.
