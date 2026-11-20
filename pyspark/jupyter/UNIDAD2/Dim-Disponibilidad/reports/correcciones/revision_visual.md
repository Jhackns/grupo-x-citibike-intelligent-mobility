# Revisión visual técnica local

Firefox real en modo headless, con perfil aislado. Las imágenes se inspeccionaron
mediante view_image; no se presenta una consulta API como revisión visual.

| Vista | Evidencia | Resultado |
|---|---|---|
| Errores y periodos | [panel5](visual/01-errores-periodo.png) | n2450, métricas de subconjunto, 2450/2451 y fechas UTC visibles. Hay scroll horizontal. |
| Cohorte10min | [panel6](visual/02-cohorte10-cobertura.png) | 0/2451 y ausencia explícita de periodos evaluados; no error cero. |
| Última captura | [panel6](visual/03-cola-fin-vs-ahora.png) | Antes inmaduras, ahora maduras sin avance; denominadores0 frente2451. NULL frente0%. |
| Batch | [panel17](visual/04-batch-fecha.png) | n2451, fecha fija UTC, edad y dos KPIs. No es dato actual. |
| Proceso detenido | [panel1](visual/06-flujo-detenido.png) | DETENIDO con colector reciente. Sin refresco de telemetría, indica TELEMETRIA ANTIGUA. |
| Sitio | [índice](visual/05-sitio-local.png) | Render local, no publicación. |

Se conservaron intentos previos en JSON: la revisión encontró un timeout real
de30s en la tabla de última captura. La frontera se materializa una vez; la
respuesta corregida tardó3,83s y conserva los conteos, sin ampliar recursos.

Para revisión personal: iniciar componentes según README; iniciar un solo
observar.py si se inspeccionan datos con productor parado; seleccionar flujo
`disponibilidad-correcciones-20261119-v1`. Para cohorte inicial usar origen
2026-11-19T22:48:04.779Z y una ventana estrecha; para cola usar
2026-11-19T23:16:04.412Z. Cambiar horizonte5/10/15 y tipo; contrastar con informe.md.
Para panel17 usar periodo que incluya una muestra Prometheus posterior al push.
Las capturas técnicas no acreditan autoría, video, PDFs ni sustentación personal.
