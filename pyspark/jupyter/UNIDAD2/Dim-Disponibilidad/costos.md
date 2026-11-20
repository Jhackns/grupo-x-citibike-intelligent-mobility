# Costos cuantificados y criterios de escalado

Consulta oficial **19/11/2026**: [AWS Lightsail](https://aws.amazon.com/lightsail/pricing/).
Escenario de planificación en N.Virginia: Linux/Unix General Purpose con IPv4.
Los **USD 144/mes son únicamente una estimación documental**. No se crearon,
contrataron ni desplegaron recursos cloud de pago; gasto cloud nuevo real **USD 0**.
Todo permanece local; no se activaron servicios de pago.
La ejecución fue local. No se presenta estimación como factura.

| Concepto | Precio oficial / supuesto | Estimación mensual USD |
|---|---|---:|
| VM | 16GB,4vCPU,320GB SSD y6TB transferencia incluidos | 84 |
| Bloque adicional | 500GB supuestos ×0,10 USD/GB-mes | 50 |
| Snapshot | 200GB retenidos supuestos ×0,05 USD/GB-mes | 10 |
| Total escenario continuo | Sin impuestos, trabajo operativo ni sobreconsumo | **144** |

Sin bloque adicional, el subtotal sería USD94/mes, con menor margen de disco.
No se añade PostgreSQL administrado: corre en la misma VM. No duplicar ese costo.
Se supone transferencia100GB/mes, inferior a la incluida; no se midió tráfico
facturable. Región, elegibilidad y sobreconsumos deben revisarse antes de contratar.

## Calibración con datos reales

La prueba produjo 27 capturas y 68013 observaciones. El aumento físico de cuatro tablas fue 1507254272 bytes, aproximadamente **53.238MiB/captura**. Es una diferencia de asignación con índices/bloat, afectada por vacuum, no tamaño lógico puro por flujo ni WAL. Archivos Parquet/checkpoints del ensayo: 78258719 bytes lógicos medidos.

| Frecuencia supuesta | Capturas/día | SQL GiB/día proyectados |
| --- | --- | --- |
| 300 | 288 | 14.973 |
| 60 | 1440 | 74.866 |

Siete días a300s: aproximadamente 104.813GiB SQL antes de margen2x, WAL, Parquet, imágenes y backups. Para presupuestar se suponen 7d de retención, margen2x y40GB para sistema/imágenes; **no se activó purga ni se eliminaron datos**. Sin retención futura el costo/disco crecerá: no afirmar operación ilimitada con un bloque fijo.

La medición corta incluye muchas evaluaciones aún pendientes: cierres y mantenimiento
pueden aumentar espacio. El supuesto anterior de20KiB no se presenta ya como
medición: se sustituye por esta calibración aproximada, todavía no validada durante días.

## Local y escalado

En local se reutilizan equipo/Docker. Electricidad y amortización no se midieron.
Con potencia **supuesta** de80W, 24h consumen1,92kWh; multiplicar por la tarifa
real de la factura, no inventar gasto monetario. Hardware existente no es gratis.

Criterios propuestos: captura→SQL p95>150s durante10min (mitad del horizonte mínimo5min),
backlog>dos capturas sostenido2min, o capacidad prevista<7d/margen de disco<30%.
Investigar primero colas/SQL/IO y mediciones antes de ampliar recursos. CPU>70%
sostenida15min es un criterio futuro: falta instrumentar hardware y probar carga.
No se aumentó memoria automáticamente ni se desplegó autoscaling/HA.

Los límites no son reservas. La VM propuesta necesita benchmark: no se garantiza
que4vCPU sostengan dos Spark, broker, SQL y observabilidad24/7. El ensayo usó local[2].
Quedan precio final de cuenta/impuestos, operación, restauración/HA y política de
retención efectivamente acordada antes de un despliegue continuo.

Datos: [costos_cuantificados.json](reports/correcciones/costos_cuantificados.json).
