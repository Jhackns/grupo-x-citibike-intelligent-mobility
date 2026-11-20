# Métricas, reinicios y alertas

Counters desconocidos se omiten; gauges desconocidos permanecen NaN. Un reset
válido se conserva: no se inventa cero. El fallo 7557→NaN→2519 generó tasas
negativas; se corrige la exposición nueva y se excluyen muestras NaN antes de
rate para consultar historia. No se recortan tasas negativas con clamp_min.

```promql
rate((disponibilidad_productor_eventos_confirmados_total == disponibilidad_productor_eventos_confirmados_total)[2m:5s])
```

El exportador atiende SIGTERM desde otro hilo y cierra serve_forever: prueba real
exit_code=0, OOMKilled=false, alerta en parada y recuperación al reiniciar. El antiguo
137 no demostraba OOM. No hay etiquetas de estación/evento/ejecución.

| Regla | Umbral y justificación |
|---|---|
| Telemetría ausente | 30 s, seis scrapes5s y caducidad30s; firing y resolución probados realmente. |
| Capturas sin avance | 2*max(intervalo,TTL)+30 s, sostenido30s con productor vivo:150s en ensayo,630s con intervalo300. |
| Backlog sostenido | Más de dos capturas durante2min con Spark vivo; N observado2519, umbral5038. Es estimación, no confirmación SQL. |

No se provocó una avería de negocio para las otras dos reglas; no hay notificaciones
externas. Pushgateway se usa solo para dos KPIs batch de captura explícita:
media por tipo y fracción de estaciones elegibles sin bicicletas, con n/fecha.
No se usa Pushgateway para cada evento del productor continuo.

La cobertura excluye inmaduras y muestra numerador/denominador, periodos efectivos,
fin de prueba/auditoría y motivos. Los errores pertenecen solo al subconjunto
observado elegible. Históricos sin muestras no pasan a cero.

## Revisión y parada final

Se revisaron imágenes de Firefox real: periodos de errores, cobertura madura,
NULL en inmaduras, fecha/n del snapshot y estado DETENIDO con colector reciente.
Al cesar telemetría se indica TELEMETRIA ANTIGUA. La tabla ancha usa scroll.
La última captura pasó de0/0 al cierre (inmadura) a0/2451 tras apagado, sin error
futuro estimable. Se corrigió un timeout por cálculo repetido de frontera;
materializarlo una vez redujo esa consulta de30s a3,83s sin ampliar recursos.

Al cierre todos los componentes propios quedaron detenidos y los cinco volúmenes
conservados. HTTP propio salió0; Kafka143; Kafka exporter de terceros2 con
OOMKilled=false, causa exacta del código2 no confirmada. No se presenta ese
exporter como salida0 ni se inventa diagnóstico OOM. Los procesos ajenos siguen
activos. Evidencias técnicas completas: reports/correcciones del repositorio.
