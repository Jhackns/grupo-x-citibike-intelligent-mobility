# Evaluación: cohorte, cobertura y periodo

Flujo explícito `disponibilidad-correcciones-20261119-v1`; 27 capturas y 68013 observaciones. Cohorte `8ff5bace-0ecf-499d-aa8a-bf43d57a3a0a`, origen 2026-11-19T22:48:04.779+00:00; plazo mínimo 2026-11-19T23:13:34.779+00:00. Frontier al cerrar cohorte 2026-11-19T23:14:57.753+00:00; captura final emitida 2026-11-19T23:16:04.412Z.

Numerador: evaluadas maduras. Denominador: predicciones cuyo plazo terminó (objetivo+30s+600s). Inmaduras excluidas; sin denominador, cobertura NULL. Los errores son solo del **subconjunto evaluado**, no del conjunto completo.

| Tipo | Min | n evaluada | MAE subconjunto | RMSE subconjunto | Sesgo | Numerador | Denominador | Cobertura % |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| convencionales | 5 | 2450 | 0.222041 | 0.556960 | -0.008980 | 2450 | 2451 | 99.959200 |
| convencionales | 10 | 0 | NULL | NULL | NULL | 0 | 2451 | 0.000000 |
| convencionales | 15 | 2450 | 0.438776 | 0.931315 | -0.031429 | 2450 | 2451 | 99.959200 |
| electricas | 5 | 2450 | 0.529796 | 1.089842 | -0.043265 | 2450 | 2451 | 99.959200 |
| electricas | 10 | 0 | NULL | NULL | NULL | 0 | 2451 | 0.000000 |
| electricas | 15 | 2450 | 0.970204 | 1.902630 | -0.114694 | 2450 | 2451 | 99.959200 |

A 10 min los candidatos están a −30,084 y +34,521 segundos: fuera de banda. No hay MAE/RMSE estimable ni se cambia de cohorte o tolerancia para ocultarlo. A 5/15 min existe un rechazo por trayectoria/inactividad en cada tipo. Los grupos quedaron cerrados tras mantener captura hasta superar el horizonte máximo y margen.

El dashboard diferencia fin_prueba/auditoria_actual, muestra periodos efectivos y causas. Las predicciones posteriores, especialmente la última captura, se separan del resultado primario: que estén inmaduras al cierre no es error cero ni falta de cobertura madura. [Datos públicos filtrados](evidencias/resultados.json).

| Tipo | Min | Orígenes evaluados desde UTC | Hasta UTC | Objetivos desde UTC | Hasta UTC |
| --- | --- | --- | --- | --- | --- |
| convencionales | 5 | 2026-11-19T22:48:04.779+00:00 | 2026-11-19T22:48:04.779+00:00 | 2026-11-19T22:53:04.779+00:00 | 2026-11-19T22:53:04.779+00:00 |
| convencionales | 10 | None | None | None | None |
| convencionales | 15 | 2026-11-19T22:48:04.779+00:00 | 2026-11-19T22:48:04.779+00:00 | 2026-11-19T23:03:04.779+00:00 | 2026-11-19T23:03:04.779+00:00 |
| electricas | 5 | 2026-11-19T22:48:04.779+00:00 | 2026-11-19T22:48:04.779+00:00 | 2026-11-19T22:53:04.779+00:00 | 2026-11-19T22:53:04.779+00:00 |
| electricas | 10 | None | None | None | None |
| electricas | 15 | 2026-11-19T22:48:04.779+00:00 | 2026-11-19T22:48:04.779+00:00 | 2026-11-19T23:03:04.779+00:00 | 2026-11-19T23:03:04.779+00:00 |

## Latencias

| Medida | n | Mediana s | p95 s | Desde UTC | Hasta UTC |
| --- | --- | --- | --- | --- | --- |
| antiguedad_fuente_en_captura | 27 | 32.300000 | 58.687800 | 2026-11-19T22:48:04.779+00:00 | 2026-11-19T23:16:04.412+00:00 |
| antiguedad_reporte_en_captura | 68013 | 86.410000 | 176.997000 | 2026-11-19T22:48:04.779+00:00 | 2026-11-19T23:16:04.412+00:00 |
| captura_a_calidad_spark | 68013 | 10.601702 | 16.169462 | 2026-11-19T22:48:04.779+00:00 | 2026-11-19T23:16:04.412+00:00 |
| captura_a_prediccion_sql_confirmada | 397050 | 34.975476 | 59.101830 | 2026-11-19T22:48:04.779+00:00 | 2026-11-19T23:16:04.412+00:00 |
| captura_futura_a_evaluacion_confirmada | 81006 | 761.175523 | 809.582379 | 2026-11-19T22:53:22.394+00:00 | 2026-11-19T23:04:02.997+00:00 |
| intervalo_consulta_incluye_reinicio | 26 | 64.065500 | 67.554500 | 2026-11-19T22:48:04.779+00:00 | 2026-11-19T23:16:04.412+00:00 |
| objetivo_ideal_a_evaluacion_confirmada | 81006 | 752.682521 | 800.959159 | 2026-11-19T22:53:04.779+00:00 | 2026-11-19T23:03:39.3+00:00 |

Antigüedad de fuente/reporte no es procesamiento. La marca Spark no es fin de escritura; SQL confirmado incluye espera y persistencia; evaluación incluye margen deliberado y colas. n por predicción comparte capturas: no equivale a n periodos independientes. No se infiere generalización semanal/estacional ni precisión física.
