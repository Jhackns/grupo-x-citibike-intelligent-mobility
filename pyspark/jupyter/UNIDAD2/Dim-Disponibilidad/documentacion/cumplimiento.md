# Cumplimiento

Contraste con [Producto U2](https://262bigdata.github.io/lambda26/proyecto-sello/u2-producto/)
y sus sesiones enlazadas; no se asigna nota de sustentación.

| Requisito | Evidencia y pendiente |
|---|---|
| Empresarial/S6 | Contrato/tópico/ACK/consumo de estados reales. Es un evento del dominio operativo, no transacciones ni cadena de microservicios; equivalencia con el instrumento requiere acuerdo docente. |
| IoT/S7 | Feed de tercero, no sensor propio ni puente MQTT o alerta física provocada. No se inventó origen físico. |
| Spark/S8 | Flujo real con ventanas/watermark/checkpoints. Triggers y tardíos se verifican con llegadas controladas de eventos originales, diferenciadas de retraso espontáneo de API/Kafka. |
| Observabilidad/S9 | Cobertura, periodos, counters y apagado corregidos; tres reglas y prueba de reinicio/alerta. Sin notificación ni carga prolongada. |
| Costos/S9 | Estimación con precios oficiales y volumen medido; hipótesis distintas de factura. Retención presupuestaria no implementada. |
| BI/S11 | KPIs/predicción y dos KPIs batch en Pushgateway; los snapshots no son inventario actual. Falta coordinación de tablero común. |
| Inferencia | Persistencia U1 y evaluación real; no llamar global a MAE del subconjunto. n=0 no equivale a error0. |
| Publicación/S12 | Sitio local revisable. Publicar, topics/commits remotos, pitch y defensa siguen pendientes personales. |

GBFS contiene estados, cantidades y fechas, no alquileres individuales ni lecturas
de un sensor propio. station_id/last_reported no acreditan cómo midió la fuente.
La guía general admite telemetría como ejemplo, mientras el mínimo específico S7
pide dispositivo real: no se declara equivalencia aprobada por cuenta propia.
