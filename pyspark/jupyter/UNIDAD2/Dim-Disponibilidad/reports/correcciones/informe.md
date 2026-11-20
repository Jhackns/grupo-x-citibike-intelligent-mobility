# Correcciones U2: resultados de la prueba continua

Fuente explícita: `disponibilidad-correcciones-20261119-v1`; 27 capturas, 68013 observaciones SQL.

Cohorte primaria congelada: `8ff5bace-0ecf-499d-aa8a-bf43d57a3a0a`, origen 2026-11-19T22:48:04.779+00:00. Plazo mínimo 2026-11-19T23:13:34.779+00:00. Frontier observado al cerrar cohorte 2026-11-19T23:14:57.753+00:00. Captura final emitida 2026-11-19T23:16:04.412Z. Cierre de procesos 2026-11-19T23:17:26.702Z.

La captura siguió hasta superar 15 min + 30 s + 600 s de margen y cerrar los seis grupos. No se alteraron cantidades, tolerancias ni calidad. Los errores siguientes describen **solo el subconjunto evaluado**, no todos los pronósticos; las ausencias no tienen error conocido.

| Tipo | Min | n evaluada | MAE del subconjunto | RMSE del subconjunto | Sesgo | Numerador | Denominador maduro | Cobertura % | Vencidas |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| convencionales | 5 | 2450 | 0.222041 | 0.556960 | -0.008980 | 2450 | 2451 | 99.959200 | 1 |
| convencionales | 10 | 0 | NULL — sin muestra | NULL — sin muestra | NULL — sin muestra | 0 | 2451 | 0.000000 | 2451 |
| convencionales | 15 | 2450 | 0.438776 | 0.931315 | -0.031429 | 2450 | 2451 | 99.959200 | 1 |
| electricas | 5 | 2450 | 0.529796 | 1.089842 | -0.043265 | 2450 | 2451 | 99.959200 | 1 |
| electricas | 10 | 0 | NULL — sin muestra | NULL — sin muestra | NULL — sin muestra | 0 | 2451 | 0.000000 | 2451 |
| electricas | 15 | 2450 | 0.970204 | 1.902630 | -0.114694 | 2450 | 2451 | 99.959200 | 1 |

A 10 minutos no hubo captura en la banda de ±30 s: el resultado es vencimiento sin error medible, no MAE cero. No se cambió de cohorte después de conocer ese resultado. Los desfases y motivos exactos están en resultados.json; otras cohortes maduras del flujo se reportan por separado.

| Tipo | Min | Motivo comprobado | n |
| --- | --- | --- | --- |
| convencionales | 5 | hueco_o_inactividad_en_trayecto | 1 |
| convencionales | 10 | sin_captura_objetivo_en_tolerancia | 2451 |
| convencionales | 15 | hueco_o_inactividad_en_trayecto | 1 |
| electricas | 5 | hueco_o_inactividad_en_trayecto | 1 |
| electricas | 10 | sin_captura_objetivo_en_tolerancia | 2451 |
| electricas | 15 | hueco_o_inactividad_en_trayecto | 1 |

## ultima_captura

| Tipo | Min | Corte | Evaluadas | Vencidas | Inmaduras | Sin avance con flujo finalizado | Cerrables | Numerador | Denominador | Cobertura % |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| convencionales | 5 | auditoria_actual | 0 | 0 | 0 | 2451 | 0 | 0 | 2451 | 0.000000 |
| convencionales | 5 | fin_prueba | 0 | 0 | 2451 | 0 | 0 | 0 | 0 | NULL — sin muestra |
| convencionales | 10 | auditoria_actual | 0 | 0 | 0 | 2451 | 0 | 0 | 2451 | 0.000000 |
| convencionales | 10 | fin_prueba | 0 | 0 | 2451 | 0 | 0 | 0 | 0 | NULL — sin muestra |
| convencionales | 15 | auditoria_actual | 0 | 0 | 0 | 2451 | 0 | 0 | 2451 | 0.000000 |
| convencionales | 15 | fin_prueba | 0 | 0 | 2451 | 0 | 0 | 0 | 0 | NULL — sin muestra |
| electricas | 5 | auditoria_actual | 0 | 0 | 0 | 2451 | 0 | 0 | 2451 | 0.000000 |
| electricas | 5 | fin_prueba | 0 | 0 | 2451 | 0 | 0 | 0 | 0 | NULL — sin muestra |
| electricas | 10 | auditoria_actual | 0 | 0 | 0 | 2451 | 0 | 0 | 2451 | 0.000000 |
| electricas | 10 | fin_prueba | 0 | 0 | 2451 | 0 | 0 | 0 | 0 | NULL — sin muestra |
| electricas | 15 | auditoria_actual | 0 | 0 | 0 | 2451 | 0 | 0 | 2451 | 0.000000 |
| electricas | 15 | fin_prueba | 0 | 0 | 2451 | 0 | 0 | 0 | 0 | NULL — sin muestra |

## posteriores_a_cohorte

| Tipo | Min | Corte | Evaluadas | Vencidas | Inmaduras | Sin avance con flujo finalizado | Cerrables | Numerador | Denominador | Cobertura % |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| convencionales | 5 | auditoria_actual | 23330 | 2 | 0 | 36764 | 3628 | 23330 | 63724 | 36.611010 |
| convencionales | 5 | fin_prueba | 23330 | 2 | 31862 | 4902 | 3628 | 23330 | 31862 | 73.222020 |
| convencionales | 10 | auditoria_actual | 12252 | 2 | 0 | 46568 | 4902 | 12252 | 63724 | 19.226665 |
| convencionales | 10 | fin_prueba | 12252 | 2 | 44117 | 2451 | 4902 | 12252 | 19607 | 62.487887 |
| convencionales | 15 | auditoria_actual | 0 | 0 | 0 | 58822 | 4902 | 0 | 63724 | 0.000000 |
| convencionales | 15 | fin_prueba | 0 | 0 | 56372 | 2450 | 4902 | 0 | 7352 | 0.000000 |
| electricas | 5 | auditoria_actual | 23372 | 2 | 0 | 36764 | 3586 | 23372 | 63724 | 36.676919 |
| electricas | 5 | fin_prueba | 23372 | 2 | 31862 | 4902 | 3586 | 23372 | 31862 | 73.353838 |
| electricas | 10 | auditoria_actual | 12252 | 2 | 0 | 46568 | 4902 | 12252 | 63724 | 19.226665 |
| electricas | 10 | fin_prueba | 12252 | 2 | 44117 | 2451 | 4902 | 12252 | 19607 | 62.487887 |
| electricas | 15 | auditoria_actual | 0 | 0 | 0 | 58822 | 4902 | 0 | 63724 | 0.000000 |
| electricas | 15 | fin_prueba | 0 | 0 | 56372 | 2450 | 4902 | 0 | 7352 | 0.000000 |

## flujo_completo

| Tipo | Min | Corte | Evaluadas | Vencidas | Inmaduras | Sin avance con flujo finalizado | Cerrables | Numerador | Denominador | Cobertura % |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| convencionales | 5 | auditoria_actual | 25780 | 3 | 0 | 36764 | 3628 | 25780 | 66175 | 38.957310 |
| convencionales | 5 | fin_prueba | 25780 | 3 | 31862 | 4902 | 3628 | 25780 | 34313 | 75.131874 |
| convencionales | 10 | auditoria_actual | 12252 | 2453 | 0 | 46568 | 4902 | 12252 | 66175 | 18.514545 |
| convencionales | 10 | fin_prueba | 12252 | 2453 | 44117 | 2451 | 4902 | 12252 | 22058 | 55.544474 |
| convencionales | 15 | auditoria_actual | 2450 | 1 | 0 | 58822 | 4902 | 2450 | 66175 | 3.702304 |
| convencionales | 15 | fin_prueba | 2450 | 1 | 56372 | 2450 | 4902 | 2450 | 9803 | 24.992349 |
| electricas | 5 | auditoria_actual | 25822 | 3 | 0 | 36764 | 3586 | 25822 | 66175 | 39.020778 |
| electricas | 5 | fin_prueba | 25822 | 3 | 31862 | 4902 | 3586 | 25822 | 34313 | 75.254277 |
| electricas | 10 | auditoria_actual | 12252 | 2453 | 0 | 46568 | 4902 | 12252 | 66175 | 18.514545 |
| electricas | 10 | fin_prueba | 12252 | 2453 | 44117 | 2451 | 4902 | 12252 | 22058 | 55.544474 |
| electricas | 15 | auditoria_actual | 2450 | 1 | 0 | 58822 | 4902 | 2450 | 66175 | 3.702304 |
| electricas | 15 | fin_prueba | 2450 | 1 | 56372 | 2450 | 4902 | 2450 | 9803 | 24.992349 |

`fin_prueba` usa metadatos y confirmaciones disponibles al cierre; `auditoria_actual` aplica el reloj de auditoría. Un plazo todavía inmaduro no entra al denominador. Una pendiente madura sin avance después de una finalización demostrada se identifica como tal, sin convertirla en vencida.

## Latencias y periodos

| Medida | n | Mediana s | p95 s | Desde UTC | Hasta UTC |
| --- | --- | --- | --- | --- | --- |
| antiguedad_fuente_en_captura | 27 | 32.300000 | 58.687800 | 2026-11-19T22:48:04.779+00:00 | 2026-11-19T23:16:04.412+00:00 |
| antiguedad_reporte_en_captura | 68013 | 86.410000 | 176.997000 | 2026-11-19T22:48:04.779+00:00 | 2026-11-19T23:16:04.412+00:00 |
| captura_a_calidad_spark | 68013 | 10.601702 | 16.169462 | 2026-11-19T22:48:04.779+00:00 | 2026-11-19T23:16:04.412+00:00 |
| captura_a_prediccion_sql_confirmada | 397050 | 34.975476 | 59.101830 | 2026-11-19T22:48:04.779+00:00 | 2026-11-19T23:16:04.412+00:00 |
| captura_futura_a_evaluacion_confirmada | 81006 | 761.175523 | 809.582379 | 2026-11-19T22:53:22.394+00:00 | 2026-11-19T23:04:02.997+00:00 |
| intervalo_consulta_incluye_reinicio | 26 | 64.065500 | 67.554500 | 2026-11-19T22:48:04.779+00:00 | 2026-11-19T23:16:04.412+00:00 |
| objetivo_ideal_a_evaluacion_confirmada | 81006 | 752.682521 | 800.959159 | 2026-11-19T22:53:04.779+00:00 | 2026-11-19T23:03:39.3+00:00 |

Antigüedad del feed/reporte no es procesamiento. Captura→marca Spark no mide el fin de escritura; captura→SQL confirmado incluye espera y persistencia. Demora de evaluación incluye margen deliberado y colas. Los n por predicción comparten capturas y no son muestras temporales independientes.

Ver [verificación de cobertura y tasas](verificacion.json), [reinicio y alerta HTTP](reinicio_exportador.json), [KPIs batch](kpis_batch.json) y [auditoría actualizada](auditoria_actualizada.md).
