# Arquitectura y operación

```text
U1: API → capturador → Supabase → descarga → batch → Silver/Gold → artefactos
U2: API → productor → Kafka → Spark/calidad/persistencia → Parquet → PostgreSQL/evaluación
Grafana ← PostgreSQL de solo lectura
Grafana ← Prometheus ← productor/Kafka/Spark y Pushgateway batch
```

Productor/colector locales en .venv; Spark por spark-submit en Docker, sin notebook.
Spark4.2.0/Scala2.13.18, conector Kafka4.2.0 y JDBC42.7.14. No se cambian servicios
ajenos. El ensayo usó local[2]; el default futuro sigue siendo local[4].

Desde la raíz del repositorio:

```bash
D=pyspark/jupyter/UNIDAD2/Dim-Disponibilidad
.venv/bin/python "$D/preparar_postgres.py"
docker compose -f "$D/compose.kafka.yml" up -d kafka-disponibilidad
docker compose -f "$D/compose.postgres.yml" up -d postgres-disponibilidad
docker inspect --format '{{.State.Health.Status}}' citibike-disponibilidad-kafka citibike-disponibilidad-postgres
```

Con ambos healthy y el esquema de etapa 3 ya inicializado:

```bash
.venv/bin/python "$D/preparar_observabilidad.py"
docker compose -f "$D/compose.observabilidad.yml" up -d
.venv/bin/python "$D/verificar_correcciones.py"
```

Para otra ejecución explícita (produce datos nuevos; no es necesario para leer):

```bash
.venv/bin/python "$D/ejecutar_observado.py" --config "$D/config/streaming-correcciones.json" --config-sql "$D/config/postgres-correcciones.json" --cohorte-inicial --intervalo 60 --duracion-segundos 2100 --spark-master 'local[2]' --informe "$D/reports/correcciones/otra_ejecucion.json"
```

Continúa los mismos checkpoints. Para otra prueba independiente indicar otro
flow_id y rutas mediante ambas configuraciones; no borrar ni escoger carpetas
recientes automáticamente. La frecuencia normal es 300 s; el ensayo usa 60 s/TTL60.
Grafana está en http://127.0.0.1:13000/d/citibike-disponibilidad-u2 . Usuario local
disponibilidad_admin; contraseña únicamente en data/secretos/grafana_admin_password.

Ctrl+C en lanzador y esperar cierre. Cerrar observar.py si se inició aparte;
no mantener dos colectores. Detener conservando volúmenes/checkpoints:

```bash
docker compose -f "$D/compose.observabilidad.yml" stop
docker compose -f "$D/compose.kafka.yml" stop kafka-disponibilidad
docker compose -f "$D/compose.postgres.yml" stop postgres-disponibilidad
```

No usar down -v. No se publicaron docs ni se hicieron commits/push.
