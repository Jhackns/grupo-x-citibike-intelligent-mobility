# Revisión visual real de Grafana

Se utilizó **Firefox 160.0**, modo headless, WebDriver BiDi nativo, perfil temporal
independiente y login dentro del navegador. No se instalaron Playwright/Selenium
ni paquetes nuevos. Se leyeron los archivos secretos localmente sin imprimirlos,
ponerlos en la URL ni copiarlos a documentación. La automatización transitoria
está fuera del producto, bajo `/tmp`; no se añade otra aplicación a la dimensión.

Las imágenes PNG listadas fueron abiertas e inspeccionadas, además de registrar el
texto renderizado. Esto **sí es revisión visual**, distinta de la comprobación API
anterior. No se modificó el dashboard durante esta revisión.

| Comprobación | Evidencia visual | Resultado / límite |
|---|---|---|
| Datos y fechas | [01-historico.png](visual/01-historico.png) | Variables de flujo/estación/tipo/horizonte; rango 03:40–04:40 UTC; fechas tipadas; curva observado/predicho y tabla de errores reales. |
| Filtros estación/tipo/horizonte | [03-filtro-estacion.png](visual/03-filtro-estacion.png) | Cambia a estación `2098359443836787630`, eléctricas, 15 min: n=0, tres vencidas y errores ausentes; no conserva por error los agregados globales. |
| Ensayo corto independiente | [07-metricas-prueba-corta.png](visual/07-metricas-prueba-corta.png) | Flow etapa 4 y rango 15:10–15:40 UTC; n=0 y MAE/RMSE/sesgo “Sin datos / sin muestras”. No muestra los errores del ensayo histórico. |
| Periodo vacío | [05-sin-datos.png](visual/05-sin-datos.png) | 00:00–01:00 UTC sin filas: “Sin datos / sin muestras”, no valores cero inventados. |
| Tabla completa de métricas | [06-metricas-historicas.png](visual/06-metricas-historicas.png) | Panel 5 ampliado: n=21.996, MAE≈0,0526, RMSE≈0,246, sesgo≈0,00200. El redondeo es visual; JSON conserva precisión. |
| Conteos y antigüedad | [09-conteos-estacion.png](visual/09-conteos-estacion.png) | Panel 2 ampliado: conteo explícito, elegibilidad, discrepancia y ANTIGUA; no se presenta como inventario físico actual. |
| Latencia y unidad | [10-latencia-segundos.png](visual/10-latencia-segundos.png) | Eje temporal UTC y leyendas “Media captura a SQL (s)” / “P95 captura a SQL (s)”; picos reales visibles, sin confundirlos con antigüedad de fuente. |
| Parada confirmada visualmente | [12-estado-detenido-verificado.png](visual/12-estado-detenido-verificado.png) | Panel 1 con **DETENIDO**, última captura 04:20 UTC y fuente 04:19:46 UTC. Colector propio vivo durante la captura, productor y Spark apagados. Se cerró también ese colector al terminar. |
| Telemetría antigua | [01-historico.png](visual/01-historico.png) y [08-estado-detenido.png](visual/08-estado-detenido.png) | TELEMETRIA ANTIGUA en los intentos realizados entre colectores; se conserva y no se llama DETENIDO. La imagen 12 es la comprobación posterior con telemetría reciente. |
| Calidad y métricas técnicas históricas | [04-sin-muestras.png](visual/04-sin-muestras.png) | Calidad SQL 12.595/382/517 discrepantes; ventanas de latencia y telemetría histórica en el periodo de etapa 4. Se detectó la anomalía de tasas y se contrastó contra PromQL, sin asumir un simple efecto gráfico. |

Registros renderizados: [revision_visual.json](revision_visual.json),
[detalle ampliado](revision_visual_detalle.json),
[parada coordinada](revision_visual_parada.json). El primer intento solo llegó a
login: `visual/intento-login/informe.json` lo identifica como **no completado**;
sus imágenes no acreditan paneles. Las imágenes 02/08/11 contienen carga parcial
o telemetría antigua y no se usan como prueba de estado detenido.

## Hallazgos de presentación y procedencia

- Las tablas normales son estrechas: algunas columnas quedan fuera del área
  visible. Ampliar el panel permite ver MAE/RMSE/sesgo y conteo/calidad. No se afirma
  que la vista compacta sea suficiente para una presentación clara.
- Los tiempos se muestran en UTC y las columnas de edad indican `_s`. La latencia
  tiene segundos en leyenda, pero los paneles técnicos mezclan tasas, contadores
  y filas en un mismo eje sin overrides de unidad; es una limitación de lectura.
- La variable flow_id y el periodo seleccionado están visibles, y se verificó
  que no se mezclan etapa 3 y 4. **El panel de errores no muestra min/max efectivos
  de las muestras evaluadas**: solo el rango solicitado. La trazabilidad temporal
  es parcial en UI; los intervalos reales constan en el informe de resultados.
- La captura 04 muestra una tasa negativa de ACK/s. Las consultas conservadas en
  `promql_tasas.json` y `contador_discontinuo.json` demuestran tres muestras
  negativas asociadas a reinicio/NaN; no se valida esa tasa como correcta.
- No se hizo una demo visual nueva con API entrando: se reutiliza la evidencia
  real previa `../observabilidad/actualizacion_viva.json`. Aquí se revisaron las
  salidas existentes con el productor parado, según el alcance de cierre.

## Revisión personal exacta

Estas capturas automatizadas **no sustituyen las personales** con reloj del
sistema y usuario/perfil del estudiante visibles, sin recortar, exigidas por las
sesiones. Tampoco demuestran autoría, exposición, pitch o publicación MkDocs.

1. Desde la raíz, seguir **Operación completa** del README. Para inspección sin
   nuevos datos iniciar solo PostgreSQL, Grafana, Prometheus/exportador y, en una
   terminal separada, `.venv/bin/python "$D/observar.py"`. No iniciar otro colector
   si `ejecutar_observado.py` ya lo está administrando.
2. Abrir esta URL histórica exacta:

   `http://127.0.0.1:13000/d/citibike-disponibilidad-u2?orgId=1&from=2026-11-19T03:40:00.000Z&to=2026-11-19T04:40:00.000Z&timezone=utc&var-flujo=disponibilidad-etapa3-vivo-20261119-v1&var-estacion=__all&var-tipo=convencionales&var-horizonte=5`

3. Mantener visibles flujo, variables y UTC. En el menú del título elegir **View**
   para el panel de errores (ID 5), disponibilidad (ID 2) y latencia (ID 9). Los
   parámetros `&viewPanel=5`, `&viewPanel=2` o `&viewPanel=9` abren directamente la
   vista ampliada. Revisar todas las columnas, no solo las dos primeras.
4. Comparar convencionales y eléctricas y 5/10/15 con
   `resultados_y_latencias.md`. No atribuir sus métricas al ensayo posterior:
   sus objetivos evaluados son aproximadamente 03:55–04:09 UTC según horizonte,
   aunque el selector permita 03:40–04:40.
5. Cambiar flujo a `disponibilidad-etapa4-vivo-20261119-v1` y periodo
   **2026-11-19 15:10–15:40 UTC**. Debe verse n=0 y errores sin muestras. No son
   errores cero. Al cierre original la cobertura madura era NULL; el panel aún
   usa la fracción antigua, por lo que el porcentaje debe explicarse como pendiente.
6. Cambiar a **00:00–01:00 UTC**: los paneles de negocio deben quedar sin datos.
   Volver al flujo histórico, filtrar la estación `2098359443836787630`, eléctricas,
   15 min: comprobar n=0 y ausencia de resultados evaluables para ese caso.
7. Con el colector activo y productor/Spark detenidos debe aparecer DETENIDO.
   Al cerrar el colector y esperar más de 30 s aparecerá TELEMETRIA ANTIGUA; con
   todo apagado la UI ya no estará disponible. Las cantidades antiguas no pasan a cero.
8. Capturar personalmente los pasos pertinentes, explicar los dos fallos técnicos
   abiertos y los límites de las cifras. Preparar pitch y defensa según la tabla
   final. Al terminar, Ctrl+C y los comandos **Detener** del README; nunca `down -v`.
