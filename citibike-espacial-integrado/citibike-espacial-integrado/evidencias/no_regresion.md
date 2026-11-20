# No regresión: 20261120-002509 (antes) vs 20261120-002852 (después)

| Comprobación | Antes | Después | ¿Regresión? |
|---|---:|---:|:---:|
| Estado de la corrida | OK | OK | no |
| Compuertas OK | 21/21 | 21/21 | no |
| Eventos leídos de citibike-rides | 67,194 | 50,852 | — |
| Ratio de eventos válidos | 0.99999 | 0.99998 | no |
| Ventanas de 15 min cerradas | 12 | 12 | no |
| Descartados por watermark (prueba) | 1 | 1 | — |
| Latencia p50 (ms) | 2,527 | 2,493 | no |
| Latencia p95 (ms) | 4,776 | 4,774 | no |
| Micro-lote p50 (ms) | 340 | 352 | no |
| Micro-lote p95 (ms) | 660.05 | 754 | no |
| MAE en vivo (modelo U2) | 33.3972 | 16.3473 | no |
| MAE en test (referencia) | 18.2092 | 18.2092 | — |
| Duración total (s) | 208.1 | 208.3 | — |
