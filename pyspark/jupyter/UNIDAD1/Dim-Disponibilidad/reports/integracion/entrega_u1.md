# Integración reproducible y entrega U1 — Disponibilidad

> Antecedente de la prueba preliminar de un día. Los resultados ampliados se documentan en [reports/ampliacion](../ampliacion/metodologia.md); no sustituir sus cifras con las de este informe.
> El notebook principal se actualizó; la versión preliminar está [archivada](../../notebooks/archivo/00_prueba_preliminar_un_dia.ipynb).

## Pregunta, dimensión y alcance

¿Con qué precisión podemos predecir los conteos **reportados por tipo** de
bicicletas convencionales y eléctricas por estación a 5, 10 y 15 minutos usando
su historial de disponibilidad? No se ha elegido un horizonte definitivo.
La exactitud física de estos reportes no está confirmada. Elegibilidad significa
cumplimiento de reglas observacionales, no garantía de exactitud.

Esta dimensión reemplaza el trabajo personal de duración para este experimento;
el código y los resultados de duración se conservan. Los documentos comunes
`docs/proyecto-sello/brief.md` y `u1-producto.md` todavía describen duración y CSV
de viajes. Disponibilidad usa estados GBFS y mide existencias reportadas, no
demanda de viajes. La armonización de esa diferencia con el planteamiento del
equipo sigue pendiente; no se reescriben las dimensiones de los compañeros.

## Arquitectura comprobable en el código

```text
Histórico externo: API GBFS → capturador externo → Supabase Storage citibike-raw
U1 implementada:
  extraccion.py (opcional) → data/raw/*.json.gz originales
  lectura.py → calidad.py → Silver aceptados + excluidos con motivos
  preparacion.py → Gold temporal aceptados + excluidos por horizonte
  modelado.py → persistencia + LR/RF → selección por MAE validación
              → prueba experimental → artefactos → recarga y reproducción
  ejecutar_u1.py [local] → src/flujo_u1.py [Docker existente]
                       → manifiesto y log por ejecución
```

El capturador no se ha localizado en la rama: no se acredita su implementación.
El descargador incremental existe y conserva rutas; **no se ejecuta** en fase 6.
El flujo por defecto funciona sin conexión a Supabase ni credenciales.

**Futura U2, no implementada para esta dimensión:** API → productor → Kafka →
Spark Structured Streaming → variables compatibles con U1 → contrato/pipeline
seleccionado → PostgreSQL → Grafana. El contrato guardado no constituye integración
ni servicio de inferencia. Supabase seguirá siendo el histórico.

## Entorno y comandos exactos

Host: Python en `.venv` de la raíz; lanzador, manifiestos y descarga opcional usan
solo biblioteca estándar. Si no existe, crear con `python3 -m venv .venv` en la raíz;
no hace falta instalar paquetes locales para el lanzador. Docker existente:
Python 3.10, Java 21, PySpark 4.2.0 y Jupyter del Dockerfile del equipo.
No se actualizan versiones ni se reconstruye la imagen en esta fase.

El servicio `pyspark` del Compose `pyspark/compose.yml` debe estar iniciado según
la configuración local del equipo. El lanzador verifica que `lambda26-pyspark`
esté activo y que el montaje escribible corresponda al proyecto. No lo crea ni
lo reinicia automáticamente. No lee ni imprime el token de Jupyter.

```bash
# Desde la raíz del repositorio; ninguna descarga:
.venv/bin/python pyspark/jupyter/UNIDAD1/Dim-Disponibilidad/ejecutar_u1.py --nueva

# Después: reutilización compatible con comprobación de hashes y recarga:
.venv/bin/python pyspark/jupyter/UNIDAD1/Dim-Disponibilidad/ejecutar_u1.py

# Ayuda sin iniciar Spark ni acceder a credenciales:
.venv/bin/python pyspark/jupyter/UNIDAD1/Dim-Disponibilidad/ejecutar_u1.py --help

# Pruebas estándar locales (las que requieren Spark se omiten aquí):
PYTHONPATH=pyspark/jupyter/UNIDAD1/Dim-Disponibilidad .venv/bin/python -m unittest discover -s pyspark/jupyter/UNIDAD1/Dim-Disponibilidad/tests -v

# Todas las pruebas dentro del entorno real PySpark:
docker exec -w /opt/UNIDAD1/Dim-Disponibilidad lambda26-pyspark python -m unittest discover -s tests -v
```

El prefijo PYTHONPATH del comando local permite importar `src/` y el lanzador
desde la raíz sin instalar la dimensión como paquete global.

Raw absoluto en el host:
`/home/miguel/proyec-bigdata/grupo-x-citibike-intelligent-mobility/pyspark/jupyter/UNIDAD1/Dim-Disponibilidad/data/raw/`.
En Docker es `/opt/UNIDAD1/Dim-Disponibilidad/data/raw/`, gracias al montaje intacto
`./jupyter:/opt`. Los resultados también son accesibles desde ambos entornos.

`config/u1-local.json` fija periodo inclusivo UTC, cortes, frescura, huecos,
tolerancia y recursos. `--config <ruta.json>` admite los mismos nombres; `--desde`
y `--hasta` sobreescriben solo el periodo. No cambian los cortes. El programa
rechaza cortes incompatibles y claves desconocidas. Los candidatos e hiperparámetros
están fijados y versionados en `src/modelado.py`, no se ajustan según prueba.

## Contratos y reglas

Se leen separadamente station_status (tabla principal), station_information y
vehicle_types, con esquemas explícitos y lista exacta de archivos inventariados.
Se bloquean feeds corruptos, vacíos o con nombre temporal no reconocido. El catálogo
solo define el significado del ID; no se unen capacidades o coordenadas al modelo.
No hay unión de metadatos que multiplique filas ni atribución retroactiva de atributos.
Una ambigüedad del catálogo deja objetivos pendientes según las reglas Silver.
Para otro periodo se exigen capturas locales de las tres fuentes dentro del periodo;
no se arrastra automáticamente un catálogo posterior.

Se conserva `captura_utc` del nombre original: el JSON no incluye `captured_at`.
`last_updated` y `last_reported` son relojes internos distintos que se preservan.
El calendario usa America/New_York, manteniendo tiempos originales en UTC.

Silver v2 conserva cantidades originales, archivo y todos los indicadores.
Convencionales = count explícito human (ID 1 en este catálogo); eléctricas = count
electric_assist (ID 2). No se calcula convencionales como total menos eléctricas.
Se utiliza el array por tipo como definición reproducible y común a ambos objetivos,
sin afirmar que sea físicamente más correcto que `num_ebikes_available`.
Las discrepancias se marcan, se conservan en Gold para instante inicial y futuro,
y no invalidan automáticamente todo el registro.

Se exige instalación/alquiler, tiempo utilizable, frescura ≤450 s y count del
tipo presente/no negativo/no ambiguo. Inactividad se distingue de invalidez.
`is_returning=0` por sí solo no impide alquiler. Se conservan repeticiones entre
capturas y cambios con igual last_reported; no se deduplica por ese par.
No se rellenan ausencias con ceros. Reglas y justificación de umbrales:
[fase 3](../calidad/reglas_y_hallazgos.md).

Gold conserva las observaciones de ambas salidas Silver para detectar interrupciones
y no ocultar inactividad. Sus **30 predictores** son, por cada tipo: actual,
rezagos 5/10/15 minutos y cambios frente a ellos; media/mínimo/máximo/desviación/
número de observaciones de los 15 minutos estrictamente anteriores. Se añaden
hora, minuto, día ISO, fin de semana y seno/coseno horario locales. Station_id sirve
para trazabilidad y evaluación, no es predictor en este experimento.

Rezagos y objetivos usan timestamps, con ±30 s: 10% de la cadencia observada de
300 s y mayor que su jitter. Ninguna característica usa valores futuros. Ventanas
y objetivos no atraviesan huecos >450 s o inactividad. Se selecciona primero la
captura más próxima, desempate a la anterior, y después se aplica su calidad:
no se salta una captura mala buscando una etiqueta conveniente. Se conserva
timestamp observado, horizonte efectivo y motivos por objetivo. No hay relleno.

Los cortes comunes a todas las estaciones siguen siendo 16:00/20:00 UTC del
10/11/2026, fin exclusivo 11/11/2026 00:00 UTC. Las etiquetas que alcanzan el
siguiente periodo se excluyen de su horizonte. El registro inicial y el objetivo
deben ser elegibles por tipo. El contexto histórico previo puede llegar a validación
y prueba porque ya estaba disponible; las etiquetas nunca cruzan al siguiente periodo.

Silver se particiona por fecha UTC de captura. En la ejecución integrada Gold
se particiona por fecha UTC inicial y horizonte (tres horizontes, cardinalidad
acotada); partición temporal permanece como columna. Es una distribución física
distinta a la salida histórica de fase 4, sin cambiar filas, cortes ni variables.
No se particiona por estación y no se concatenan salidas de diferentes ejecuciones.

## Entrenamiento, selección y evaluación

Por cada tipo/horizonte: persistencia, dos regresiones ridge (0.01/0.1), RF de
8 árboles/profundidad 4 y 16/profundidad 6. Son **24 ajustes MLlib**, además de
seis líneas base sin entrenamiento. Misma población de evaluación por combinación.
La mediana por variable y los indicadores de ausencia se ajustan solo con train;
una columna totalmente nula en train se omite y se registra. El orden de predictores
y vector queda guardado. No se usan etiquetas, tiempos ni calidad futuros como
predictores. Estos últimos solo sirven para auditar/evaluar.

La política previa elige MAE de validación; empate absoluto ≤1e-6 resuelto por
persistencia, LR 0.01, LR 0.1, RF pequeño y RF mayor. Predicciones negativas se
acotan a cero, sin redondear ni imponer capacidad máxima. Se informan MAE, RMSE,
R² y n. R² no se define cuando el objetivo es constante. Se congela selección
en disco con SHA-256 antes de evaluar prueba; no hay refit ni elección de horizonte.

Se comparan todos los candidatos y se guardan métricas globales, con/sin discrepancias
(desconocidos por separado), cambia/igual y tablas por estación para ganador y
persistencia. Subgrupos disjuntos reconcilian con el N global. Los gráficos usan
como máximo 200 agregados; no se recopila todo el histórico en pandas/driver.

Cuando gana un estimador MLlib se guarda el PipelineModel con transformaciones;
cuando gana persistencia se guarda su **contrato/configuración sin parámetros
entrenados**. En la ejecución original se conservaron parámetros y métricas,
sin pesos de candidatos no seleccionados. La [revisión final](../cumplimiento_u1/revision_final.md)
añadió referencias MLlib entrenadas por separado para la evidencia exigida, sin
reemplazar los contratos ganadores. No se presenta persistencia
como un modelo MLlib entrenado. La recarga verifica todas las predicciones elegidas
y las métricas por estación con tolerancia numérica 1e-10.

Se fija la semilla de RF y se registra la distribución/configuración del trabajo.
La reproducibilidad comprobada aquí es la de entradas, contratos, reglas y
predicciones de los artefactos recargados. No se promete identidad binaria entre
ajustes nuevos de algoritmos distribuidos: cambiar el particionado/orden de
procesamiento puede variar sus resultados. Las comparativas de cada ejecución
se conservan con su propio linaje; no se atribuyen las métricas históricas de otro
ajuste al nuevo. Los conteos y cortes sí deben reconciliar para estos datos.

## Resultados, evidencia y limitaciones

El informe de la ejecución completa y su reutilización verificadas está en
[verificación de fase 6](verificacion.md). Los resultados anteriores y las tablas
completas LR/RF frente a persistencia permanecen en [fase 5](../modelado/hallazgos.md).
Los hashes y el linaje del manifiesto de fase 6 son la autoridad para sus propias
salidas, no las rutas históricas de fases 3–5.

Solo existe un día UTC (10/11/2026), que abarca parte de dos días locales. No
demuestra generalización semanal, fines de semana o estacionalidad. Prueba ya fue
inspeccionada en fases previas: los resultados son experimentales, sin evaluación
final independiente y frente a conteos reportados, sin verdad física contrastada.
Muchas filas correlacionadas no equivalen a días independientes. Que persistencia
gane o tenga R² alto no prueba anticipación de cambios: su error es cero cuando
no cambia el conteo, por definición. Se informa el error condicionado a cambios.

## Incorporar más días después de esta entrega

No ejecutar estos pasos durante fase 6. Primero conservar/revisar el inventario
existente y, con autorización, inventariar de nuevo mediante el descargador original:

```bash
.venv/bin/python pyspark/jupyter/UNIDAD1/Dim-Disponibilidad/src/extraccion.py --env-file pyspark/.env.u2 --credencial secreta --desde 2026-11-11 --hasta 2026-11-16 --todas-las-fuentes --dry-run
```

La opción `secreta` es explícita porque el acceso publicable previamente configurado
no permitía listar; no se solicita otra clave ni se modifica RLS. Usar `publicable`
si el acceso existente ya basta. `.env.u2` permanece ignorado; el lanzador no
copia su ruta ni sus valores al manifiesto. Las fechas del ejemplo no implican
inventario remoto actualizado ni garantía de día completo. Confirmar cobertura
y espacio antes de descargar. La integración reserva al menos 2 GiB para resultados;
esa comprobación no sustituye la comprobación de espacio para ampliar raw.

Tras acordar nuevos cortes prospectivos, guardar un JSON sin secretos con periodo,
cortes y fin exclusivo; revisar catálogo y frecuencia en el nuevo periodo. Ejemplo
de uso posterior (la descarga requiere activación explícita):

```bash
.venv/bin/python pyspark/jupyter/UNIDAD1/Dim-Disponibilidad/ejecutar_u1.py --config /ruta/configuracion-u1.json --descargar --env-file pyspark/.env.u2 --credencial secreta --nueva
```

Esto reutiliza `extraccion.py` y sus temporales/reintentos/manifiesto; no borra
remotos. Después procesa exclusivamente los originales seleccionados. No sirve
para retocar cortes según los resultados de este día. Reservar días futuros sin
inspeccionar para evaluación independiente y valorar ventanas cronológicas múltiples.

## Contraste documental y rúbrica

La matriz siguiente conserva el estado provisional al cerrar fase 6, cuando
todavía no se había proporcionado la guía oficial. La posterior
[revisión final con la rúbrica y sesiones oficiales](../cumplimiento_u1/revision_final.md)
la sustituye como diagnóstico vigente. `u1-producto.md` local es una memoria del
equipo, no la rúbrica evaluadora. No se asignan puntuaciones sin defensa personal.

| Aspecto documentado de U1 | Evidencia de esta dimensión | Alcance o pendiente |
|---|---|---|
| Pregunta y dimensión predictiva histórica | README, objetivos por tipo y 5/10/15 min | Alinear formalmente cambio de duración/CSV con brief del equipo |
| PySpark y esquema explícito | src/lectura.py, notebook y flujo Docker | Verificación local; no clúster multinodo ni prueba de escalabilidad |
| Calidad, trazabilidad y medallón | calidad.py; Silver/Gold con archivos y motivos; manifiesto | Objetivos observacionales; exactitud física no contrastada |
| Variables, preparación y persistencia analítica | preparacion.py; Gold Parquet; pruebas temporales | Un día; metadatos de estaciones no se usan como predictores |
| Modelado predictivo y comparación | modelado.py, 24 fits MLlib, métricas vs persistencia | Los seis ganadores actuales no requieren entrenamiento |
| Métricas y análisis crítico | MAE/RMSE/R² globales, estratos/cambios/estaciones | No hay prueba final independiente ni evidencia de generalización |
| Reproducibilidad en Docker y notebook | ejecutar_u1.py, manifiesto, recarga, sección fase 6 | Requiere raws locales fuera de Git y contenedor ya disponible |
| Integración con el producto común | Documentación de disponibilidad sin alterar compañeros | Pregunta y fuente comunes todavía describen el trabajo previo |
| Criterios formales, pesos y entregables de la rúbrica vigente | Sin fuente oficial disponible | **Sin evidencia; contraste oficial pendiente** |

No se acredita U2, despliegue, capturador ni integración de Grafana. Tampoco se
acreditan requisitos de presentación o evaluación institucional no proporcionados.
