# Etapa 4: Grafana y observabilidad de Disponibilidad

**Referencia vigente:** `config/ejecucion_u2_seleccionada.json` y
[correcciones verificadas](reports/correcciones/auditoria_actualizada.md).
Los comandos de operación actuales están en [README](README.md). Esta página
conserva la descripción de etapa4; el nuevo ensayo principal tiene27 capturas,
cohorte cerrada,17 paneles, Pushgateway local y tres reglas Prometheus.
[Revisión visual real](reports/correcciones/revision_visual.md) ·
[comparación fin de prueba/auditoría](reports/correcciones/informe.md).



Estado técnico verificado el **19 de noviembre de 2026**. Se añade un dashboard
propio, PostgreSQL de solo lectura, Prometheus y telemetría del flujo existente.
Se conservan las etapas anteriores, los checkpoints y la selección de U1
`20261117T171917344468Z`: seis contratos de **persistencia**, sin entrenamiento
ni carga de un modelo MLlib para inferir. Los conteos siguen siendo reportados
por tipo; elegibilidad no demuestra exactitud física.

```text
API → productor → Kafka → Spark/calidad/persistencia → Parquet → consumidor SQL
                                                               ↓
                                               PostgreSQL → vistas SELECT → Grafana
Logs/progreso Spark + procesos propios → observar.py → exporter → Prometheus ↗
Kafka propio → kafka-exporter ──────────────────────────────────────────────↗
```

La auditoría previa de las **19.572 vencidas** está en
[el informe](reports/observabilidad/vencidas.md). No se demostró un defecto y no se
cambiaron tolerancias, calidad, emparejamiento ni evaluaciones históricas.

## Componentes, versiones y permisos

`compose.observabilidad.yml` es adicional y aislado. No cambia Dockerfile,
Compose del equipo ni sus redes. Reutiliza PostgreSQL, Kafka y la imagen PySpark
existentes. No había un Prometheus de esta dimensión para reutilizar; se incorpora
uno propio. Las nuevas imágenes se fijan por digest:

| Componente | Versión verificada | Acceso desde el host |
|---|---|---|
| Grafana | 11.6.11 | http://127.0.0.1:13000 |
| Prometheus | 3.5.0 | http://127.0.0.1:19090 |
| Exportador de aplicaciones, biblioteca estándar | Imagen PySpark existente, sin iniciar Spark | http://127.0.0.1:19108/metrics |
| kafka-exporter | 1.8.0 | Solo red Docker propia, puerto 9308 |

La red es `citibike-disponibilidad_default`. Los servicios no reciben el socket
Docker. El colector **local** `observar.py` utiliza Docker CLI para comprobar
únicamente procesos/contenedores propios y escribir un archivo de métricas. El
exportador monta solo ese directorio, no las credenciales ni el resto de `data/`.
No se añaden paquetes Python: se reutiliza `.venv` y la dependencia Kafka ya fijada.

`sql/002_observabilidad.sql` añade los esquemas propios
`observabilidad_disponibilidad` (estado de componentes) y
`grafana_disponibilidad` (vistas para el tablero). No modifica `001_disponibilidad.sql`.
El usuario `disponibilidad_grafana` solo puede conectar y consultar las vistas:
no tiene permisos de escritura en tablas base, CREATE ni TEMP, y su sesión usa
`default_transaction_read_only=on` y timeout de consultas de 30 s. Las vistas
exponen campos seleccionados, no el payload original completo.

`preparar_observabilidad.py` genera y conserva contraseñas aleatorias en archivos
locales con permisos 600: `data/secretos/grafana_pg_password` y
`data/secretos/grafana_admin_password`. No imprime sus valores. El administrador
web es `disponibilidad_admin`; utiliza su archivo local para introducir la clave
en el navegador, sin copiarla a informes. El secreto PostgreSQL se cifra mediante
`psql \password` y se renderiza únicamente en
`data/observabilidad/provisioning/datasources/datasources.yaml`, excluido de Git.
La plantilla versionable usa un marcador, nunca una contraseña. No se cambia la
clave del propietario SQL. Estos permisos y el rechazo de escritura se verificaron
mediante la conexión que realmente utiliza Grafana.

Los puertos están limitados a loopback; PostgreSQL y Kafka usan la red local sin
TLS. Esto no constituye una configuración segura para exposición pública.

## Iniciar desde la raíz del repositorio

Los comandos siguientes presuponen `.venv` y la instalación de etapas 1–3.
En otra máquina: `python3 -m venv .venv`, instalar
`requirements.txt` de esta dimensión y preparar los componentes según
[postgres.md](postgres.md) y [streaming.md](streaming.md). No reinstalar paquetes
globalmente. Los artefactos U1 seleccionados deben estar disponibles localmente.

```bash
D=pyspark/jupyter/UNIDAD2/Dim-Disponibilidad
docker compose -f "$D/compose.kafka.yml" up -d kafka-disponibilidad
docker compose -f "$D/compose.postgres.yml" up -d postgres-disponibilidad
docker inspect --format '{{.State.Health.Status}}' citibike-disponibilidad-kafka
docker inspect --format '{{.State.Health.Status}}' citibike-disponibilidad-postgres
```

Continuar solo cuando **ambos** estén `healthy`. El esquema SQL de etapa 3 debe
estar inicializado según `postgres.md`; esta preparación no lo sustituye.

```bash
.venv/bin/python "$D/preparar_observabilidad.py"
.venv/bin/python "$D/generar_dashboard.py"
docker compose -f "$D/compose.observabilidad.yml" up -d
```

El primer intento del kafka-exporter de esta prueba arrancó antes de que el broker
estuviera disponible en DNS; se corrigió el orden de arranque y se reinició solo
ese exportador. No se modificaron listeners ni servicios ajenos. Si se reproduce:

```bash
docker compose -f "$D/compose.observabilidad.yml" restart kafka-exporter-disponibilidad
```

Abrir http://127.0.0.1:13000/d/citibike-disponibilidad-u2 . El dashboard se
aprovisiona desde `observabilidad/dashboards/disponibilidad.json`; los cambios se
hacen en `generar_dashboard.py` y se regenera el JSON, no en una copia manual del
navegador. El refresco del dashboard es 30 s y el scrape técnico 5 s.

### Ensayo acotado y ejecución continua

El lanzador siguiente envuelve **los ejecutables existentes**, registra sus PID,
logs y manifiestos exactos, y los detiene al terminar. No crea otro procesamiento.
Los defaults siguen siendo `local[4]`, intervalo de consulta 300 s. Estos ensayos
usaron explícitamente `local[2]` e intervalo 60 s, sin aumentar memoria.

```bash
.venv/bin/python "$D/ejecutar_observado.py" --capturas 3 --intervalo 60 --spark-master 'local[2]' --pausa-verificacion-segundos 45 --informe "$D/reports/observabilidad/mi_ensayo.json"
```

En otra terminal, mientras el ensayo está activo:

```bash
.venv/bin/python "$D/verificar_actualizacion.py"
```

La comprobación pasiva está acotada al flujo de `config/observabilidad.json`: espera
hasta 240 s por un cambio y 180 s por el cierre. Su comprobación de métricas sin
muestras corresponde a este ensayo corto, sin evaluaciones aún; no sirve para
validar un flujo largo que ya tenga etiquetas. No genera capturas por su cuenta.
Para una ejecución continua, iniciar en primer plano y detener con Ctrl+C:

```bash
.venv/bin/python "$D/ejecutar_observado.py" --intervalo 300 --informe "$D/reports/observabilidad/ejecucion_continua.json"
```

Por defecto `--capturas 0` significa continuo. También existe
`--duracion-segundos`. Las rutas explícitas son
`config/streaming-etapa4.json` y `config/postgres-etapa4.json`, flujo
`disponibilidad-etapa4-vivo-20261119-v1`, salida/checkpoints
`data/streaming/verificacion_etapa4/` y `data/sql/verificacion_etapa4/checkpoint`.
Reejecutar conserva esos checkpoints y continúa el mismo flujo. Para otro ensayo
independiente hay que indicar **ambas** configuraciones, con otro flow_id y rutas;
no se escoge automáticamente una carpeta de modelos o de datos reciente.

El registro local `data/observabilidad/registro.json` enlaza los procesos del
lanzamiento. No ejecutar colectores simultáneos sobre el mismo registro. Para
consultar el estado detenido después del ensayo se puede mantener **solo** el
colector local en primer plano y cerrarlo con Ctrl+C:

```bash
.venv/bin/python "$D/observar.py"
.venv/bin/python "$D/verificar_observabilidad.py"
```

El segundo comando va en otra terminal. `--exigir-historia-activa` exige además
que Prometheus conserve actividad real del productor en los últimos 30 minutos;
no debe usarse días después para simular una prueba en vivo. La verificación
histórica requiere que siga disponible la evidencia SQL de etapa 3. Los informes
se actualizan al ejecutar estas herramientas; usar Git/una copia si se necesita
conservar una instantánea adicional. Las evidencias originales de esta entrega
están resumidas en [verificacion.md](reports/observabilidad/verificacion.md).

## Dashboard y significado de los filtros

Hay **16 paneles**, con variables de flujo, estación, tipo, horizonte y periodo.
El flujo histórico de etapa 3 es `disponibilidad-etapa3-vivo-20261119-v1`; el nuevo
ensayo de etapa 4 es otro flujo. No se mezclan sus métricas. Las métricas técnicas
muestran el lanzamiento registrado actualmente, independientemente del filtro
histórico de negocio, y el panel lo indica.

| Información | Criterio y tratamiento de ausencias |
|---|---|
| Última captura, fuente y estado | Fechas UTC y edad respecto de ahora; independiente del periodo histórico seleccionado. |
| Estado de procesos | ACTIVO, DETENIDO, SIN CAPTURAS, SIN TELEMETRIA, TELEMETRIA ANTIGUA o SIN CAPTURAS RECIENTES. Proceso vivo no prueba frescura. |
| Disponibilidad actual | Última observación de cada estación, cantidades explícitas por tipo, elegibilidad y antigüedad; no se resta total menos eléctricas. |
| Últimas predicciones | Persistencia por estación/tipo/horizonte, origen/objetivo y condición VIGENTE/CADUCADA. El periodo filtra el origen. |
| Observado frente a predicho | Ambas series sobre los mismos pares evaluados, alineadas por **fecha objetivo ideal UTC**. El detalle conserva fecha observada y horizonte efectivo. Huecos NULL, sin unirlos ni rellenarlos. |
| MAE/RMSE/sesgo/n y estratos | Mismos pares evaluados; sesgo = predicho − observado. Estratos con/sin cambios y discrepancias. n=0 deja métricas NULL. |
| Cobertura | Evaluadas/vencidas/pendientes; periodo por origen. Sin predicciones no se inventa cobertura 100 %. |
| Pendientes | Inmaduras por reloj; maduras sin avance suficiente de la fuente; cerrables por ambas barreras. No reclasifica las bloqueadas como vencidas solo por reloj. |
| Calidad | Observaciones, rechazos totales o parciales y discrepancias true/false/desconocidas. Rechazos son subconjunto y no se suman de nuevo a observaciones. |
| Latencia | Promedio y p95 por ventana de 5 min, desde captura hasta disponibilidad SQL registrada de la predicción; no duración total del ensayo. |
| Motivos de vencimiento | Conteos por motivo sin cambiar las decisiones persistidas. |
| Telemetría técnica | Estado, contadores del productor, progreso Spark y offsets/backlog estimado Kafka. |

Las vistas consultadas y sus SQL reproducibles están en
`sql/002_observabilidad.sql` y `observabilidad/consultas.json` (11 consultas SQL).
Para todas las estaciones las curvas son medias de los pares disponibles, no la
suma física del sistema. Filtrar una estación permite ver sus conteos. Una
estación inexistente produce “sin datos”, nunca una bicicleta o error cero.

El umbral de captura reciente es `2 * max(intervalo_configurado, ttl) + 30 s`:
150 s en la prueba de 60 s y 630 s con consulta de 300 s/TTL60. Es una señal
operativa configurable mediante el intervalo y el margen, no una regla de calidad
ni un cambio en la tolerancia de evaluación. La telemetría caduca tras 30 s; el
exportador responde HTTP 503 si su archivo es más antiguo. El estado desconocido
se conserva como NULL/NaN. Los reportes previos de etapa 3 siguen siendo históricos.

## Métricas técnicas y garantías

`telemetria.py` interpreta los logs JSON y el progreso nativo de Spark, evitando
alterar los fingerprints de etapas 2–3. Se protege contra reutilización de PID
con sus ticks de inicio y se comprueba el comando propio. La detección histórica
de procesos parados exige registro explícito; no se infiere por falta de logs.

- Productor: consultas HTTP, capturas, eventos **confirmados**, fallos y reintentos;
  edad del último ACK de captura. Los contadores reinician con el productor.
- Spark: entradas/tasas, duración de lote, estado y filas descartadas por watermark,
  watermark y offsets de las cuatro consultas. Los valores retenidos al detenerse
  son del último progreso, acompañados del estado de proceso.
- Kafka: broker alcanzable, particiones y offsets del tópico propio. El backlog
  combina offset de broker con endOffset observado por Spark: es **estimado**,
  no una medida de confirmación SQL. Muestras ausentes o incoherentes dan NaN.
- SQL: estado de consumidor y progreso de evaluación disponible en sus informes.
- Prometheus: targets `up` y métricas propias de Grafana. Las etiquetas custom
  se limitan a componente, consulta y las tres particiones. Se descartan métricas
  de grupos Kafka: Spark conserva offsets en su checkpoint y no ofrece mediante
  esos grupos una medida correcta de su avance; además evita UUID de grupos.

No hay etiquetas station_id/event_id/capture_id/run_id, ni Docker socket expuesto.
No se implementaron JMX completo del broker, métricas de hardware, alertas con
notificaciones, HA ni prueba de carga. El scrape mide disponibilidad del exporter;
los estados de componentes y la edad del archivo distinguen un exporter vivo de
un flujo detenido. No se afirma exactly-once adicional a las garantías reales ya
documentadas en `streaming.md` y `postgres.md`.

## Detener solo esta dimensión

Primero Ctrl+C en `ejecutar_observado.py` y esperar su cierre confirmado. También
cerrar `observar.py` si se ejecutó separadamente. Después:

```bash
docker compose -f "$D/compose.observabilidad.yml" stop
docker compose -f "$D/compose.kafka.yml" stop kafka-disponibilidad
docker compose -f "$D/compose.postgres.yml" stop postgres-disponibilidad
```

No usar `down -v`. Se conservan los volúmenes Grafana/Prometheus/Pushgateway/Kafka/PostgreSQL,
los Parquet y los checkpoints. Al finalizar esta entrega todos estos procesos
propios quedan detenidos; los servicios ajenos permanecen activos como estaban.

## Evidencias y límites

[Auditoría de vencidas](reports/observabilidad/vencidas.md),
[verificación ejecutada](reports/observabilidad/verificacion.md),
[estimación de costos](costos.md). Código/configuración/JSON del dashboard/informes
pequeños son versionables; `data/`, secretos, capturas, Parquet, checkpoints y
`.venv` siguen ignorados. No se cambió U1 ni se repitió entrenamiento.

En la etapa4 histórica se verificaron aprovisionamiento y consultas mediante
API. La corrección vigente añade inspección visual real, documentada arriba. Quedan a cargo del estudiante las
capturas de pantalla con filtros, series y estados, y la sustentación de la
semántica de conteos, incertidumbre, fronteras temporales y costos. La prueba
de etapa4 fue corta y no esperó evaluaciones a 15 minutos: los seis horizontes y
métricas se comprobaron con la evidencia SQL real conservada de etapa 3. No se
presenta la prueba nueva como otra validación completa de evaluación diferida.
No demuestra operación continua, exactitud física ni generalización estacional.
