# Producto U2 adaptado

¿Con qué precisión se predicen los conteos reportados por tipo/estación a partir
de su historial? Se conservan los tres horizontes; no se elige uno después de prueba.
La decisión operativa es monitoreo y planificación de rebalanceo; disponibilidad
no equivale a demanda, cantidad o duración de viajes.

Fuente: [Citi Bike System Data](https://citibikenyc.com/system-data), índice GBFS
oficial configurable. Se comprobó versión 2.3 y station_status, station_information
y vehicle_types. Los catálogos se verifican: 1=bicycle/human y 2=bicycle/electric_assist.
La descripción del formato no determina qué campo es físicamente más correcto.

Contrato citibike.disponibilidad.v1: un evento por estación/captura, event_id
estable con capture_id durante reintentos, clave Kafka station_id, fechas UTC
separadas, estación original, fuente y versiones de catálogos. Los ACK son
confirmaciones, no intentos. Estados iguales de capturas distintas se conservan.
No se afirma exactly-once entre Kafka, archivos y PostgreSQL.

Seis contratos explícitos de U1 20261117T171917344468Z seleccionan persistencia.
LR/RF se entrenaron y compararon en U1, conservados como referencias; no se
presentan como desplegados. No se reconstruye convencional=total−eléctrica.
Nulos no son ceros; calidad y discrepancias se conservan por objetivo. Antigüedad
heredada 450 s; tolerancia de emparejamiento ±30 s y margen tardío 600 s.
La inferencia no usa cantidades ni indicadores futuros.
