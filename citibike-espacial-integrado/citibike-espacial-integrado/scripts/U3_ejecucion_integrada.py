"""
U3 · Ejecución integrada (S13) — CitiBike Intelligent Mobility · Dimensión Espacial (Cristhian Chuquitarqui)

Una sola ejecución que recorre las tres capas del sistema y deja evidencia:

  1. Compuertas PREVIAS   ¿están los datos, Kafka, Pushgateway, Prometheus y los artefactos de U1/U2?
  2. Flujo BATCH (U1)     compuertas de CALIDAD sobre la capa Gold: existencia, completitud, rango, plausibilidad
  3. Flujo STREAMING (U2) lee citibike-rides de Kafka durante una ventana fija, asigna zonas, cierra ventanas
                          de 15 min con watermark y predice t+1 con el modelo de U2 (inferencia en vivo)
                          + caso de error a propósito: un evento inválido y uno tardío
  4. BI/ML                publica los KPI (p50/p95, MAE en vivo, compuertas) en Pushgateway y confirma
                          que Prometheus tiene los valores DE ESTA corrida (por run_id)
  5. MANIFIESTO           salidas/u3/manifiestos/manifiesto_<runId>.json

Uso (dentro del contenedor spark, con el sistema levantado):
    docker compose exec spark python scripts/U3_ejecucion_integrada.py
    docker compose exec spark python scripts/U3_ejecucion_integrada.py --duracion 120
    docker compose exec spark python scripts/U3_ejecucion_integrada.py --forzar-fallo   # demo: una compuerta detiene

Código de salida: 0 si todas las compuertas que detienen pasan, 1 si alguna falla.
"""
import argparse
import glob
import json
import math
import os
import socket
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

RAIZ = Path(__file__).resolve().parents[1]
U1, U2, U3 = RAIZ / "salidas/u1", RAIZ / "salidas/u2", RAIZ / "salidas/u3"
KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
PUSHGATEWAY = os.getenv("PUSHGATEWAY_URL", "http://pushgateway:9091")
PROMETHEUS = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
TOPIC = "citibike-rides"
NY = ZoneInfo("America/New_York")
VENTANA_MIN = 15
VENTANA_MS = VENTANA_MIN * 60 * 1000

compuertas = []


def compuerta(nombre, etapa, ok, detalle, detiene=True):
    """Registra una compuerta. detiene=True: si falla, la ejecución se corta. detiene=False: solo advierte."""
    compuertas.append({"nombre": nombre, "etapa": etapa, "ok": bool(ok), "detiene": detiene, "detalle": detalle})
    marca = "OK " if ok else ("FALLA" if detiene else "AVISO")
    print(f"  [{marca}] {etapa:9s} {nombre}: {detalle}")
    return ok


def http_ok(url):
    try:
        with urllib.request.urlopen(url, timeout=4) as r:
            return r.status == 200
    except Exception:
        return False


def puerto_abierto(hostport):
    host, port = hostport.split(":")
    try:
        with socket.create_connection((host, int(port)), timeout=4):
            return True
    except OSError:
        return False


def percentil(valores, p):
    if not valores:
        return None
    v = sorted(valores)
    k = (len(v) - 1) * p
    i, j = math.floor(k), math.ceil(k)
    return v[i] + (v[j] - v[i]) * (k - i)


def detenidas():
    return [c for c in compuertas if c["detiene"] and not c["ok"]]


# ---------------------------------------------------------------- 1. compuertas previas
def compuertas_previas(forzar_fallo):
    print("\n1) Compuertas previas")
    csv = sorted(glob.glob(str(RAIZ / "datos/*.csv")))
    compuerta("datos CSV", "previa", len(csv) >= 5, f"{len(csv)} CSV en datos/ (se esperan 5)")
    compuerta("kafka", "previa", puerto_abierto(KAFKA), f"conexión a {KAFKA}")
    compuerta("pushgateway", "previa", http_ok(f"{PUSHGATEWAY}/-/ready"), f"{PUSHGATEWAY}/-/ready")
    compuerta("prometheus", "previa", http_ok(f"{PROMETHEUS}/-/ready"), f"{PROMETHEUS}/-/ready", detiene=False)
    faltan_u1 = [p for p in ("gold_zona_hora", "metricas_u1.json") if not (U1 / p).exists()]
    compuerta("artefactos U1", "previa", not faltan_u1, "ok" if not faltan_u1 else f"faltan {faltan_u1}: corre la sección U1 del notebook")
    faltan_u2 = [p for p in ("models/modelo_zonas_ts", "media_slot", "zonas_top.json", "estaciones_zona.json", "metricas_modelo.json")
                 if not (U2 / p).exists()]
    compuerta("artefactos U2", "previa", not faltan_u2, "ok" if not faltan_u2 else f"faltan {faltan_u2}: corre U2_01")
    if forzar_fallo:
        compuerta("prueba de fallo (--forzar-fallo)", "previa", (RAIZ / "datos/NO_EXISTE.csv").exists(),
                  "se pide un archivo que no existe para demostrar que la compuerta detiene la corrida")


# ---------------------------------------------------------------- 2. batch: calidad de la capa Gold (U1)
def flujo_batch(spark, F):
    print("\n2) Flujo batch (U1): compuertas de calidad sobre Gold")
    t0 = time.time()
    gold = spark.read.parquet(str(U1 / "gold_zona_hora"))
    r = gold.agg(F.count("*").alias("filas"),
                 F.sum(F.when(F.col("zona").isNull() | F.col("hora_ts").isNull(), 1).otherwise(0)).alias("nulos"),
                 F.sum(F.when((F.col("salidas") < 0) | (F.col("llegadas") < 0), 1).otherwise(0)).alias("negativos"),
                 F.sum("actividad").alias("actividad"), F.countDistinct("zona").alias("zonas")).first()
    m1 = json.loads((U1 / "metricas_u1.json").read_text(encoding="utf-8"))
    compuerta("existencia", "batch", r["filas"] > 0, f"{r['filas']:,} filas zona-hora en {r['zonas']} zonas")
    compuerta("completitud", "batch", r["nulos"] == 0, f"{r['nulos']} filas sin zona u hora")
    compuerta("rango", "batch", r["negativos"] == 0, f"{r['negativos']} filas con salidas o llegadas negativas")
    conservado = m1["silver_filas"] / m1["bronze_filas"]
    compuerta("plausibilidad: Silver conserva >= 95 % de Bronze", "batch", conservado >= 0.95, f"{100 * conservado:.2f} %")
    esperado = 2 * m1["silver_filas"]          # cada viaje = 1 salida + 1 llegada
    compuerta("plausibilidad: Gold cuadra con Silver", "batch", abs(r["actividad"] - esperado) <= 0.001 * esperado,
              f"actividad Gold {r['actividad']:,} vs 2 x Silver {esperado:,}")
    return {"gold_filas": r["filas"], "zonas": r["zonas"], "silver_conservado_pct": round(100 * conservado, 2),
            "duracion_s": round(time.time() - t0, 1)}


# ---------------------------------------------------------------- 3. streaming acotado con inferencia (U2)
def flujo_streaming(spark, F, duracion_s):
    from pyspark.ml import PipelineModel
    from pyspark.sql.types import StructType, StringType, LongType

    print(f"\n3) Flujo streaming (U2): {duracion_s} s leyendo {TOPIC} con el modelo de U2")
    t0 = time.time()
    top = json.loads((U2 / "zonas_top.json").read_text(encoding="utf-8"))["zonas"]
    ids_top = [z["zona"] for z in top]
    est_zona = json.loads((U2 / "estaciones_zona.json").read_text(encoding="utf-8"))["estaciones"]
    meta = json.loads((U2 / "metricas_modelo.json").read_text(encoding="utf-8"))
    mae_test = next(r["MAE"] for r in meta["resultados"] if r["modelo"] == meta["ganador"])
    modelo = PipelineModel.load(str(U2 / "models/modelo_zonas_ts"))
    media_slot = {(r["zona"], r["finde"], r["hora"]): r["media_slot"]
                  for r in spark.read.parquet(str(U2 / "media_slot")).collect()}
    mapa = spark.createDataFrame(list(est_zona.items()), "stationId string, zona string")

    esquema = (StructType().add("tipoEvento", StringType()).add("stationId", StringType())
               .add("timestamp", LongType()).add("publishedAt", LongType()))
    valido = (F.col("tipoEvento").isin("viaje.salida", "viaje.llegada") & F.col("stationId").isNotNull()
              & (F.col("timestamp") > 0) & F.col("publishedAt").isNotNull())
    eventos = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA).option("subscribe", TOPIC)
               .option("startingOffsets", "latest").option("failOnDataLoss", "false").load()
               .select(F.from_json(F.col("value").cast("string"), esquema).alias("e")).select("e.*")
               .withColumn("isValid", F.coalesce(valido, F.lit(False)))
               .withColumn("eventTime", F.timestamp_millis("timestamp"))
               .withColumn("latenciaMs", F.unix_millis(F.current_timestamp()) - F.col("publishedAt")))

    est = {"leidos": 0, "validos": 0, "latencias": [], "ventanas": 0, "evaluadas": 0, "err_abs": 0.0, "ventana_ms": None}
    historial = defaultdict(lambda: deque(maxlen=4))
    neto, pendientes = {}, {}

    def calidad(df, _bid):
        filas = df.select("isValid", "latenciaMs").collect()
        est["leidos"] += len(filas)
        est["validos"] += sum(1 for f in filas if f["isValid"])
        est["latencias"] += [f["latenciaMs"] for f in filas if f["isValid"] and f["latenciaMs"] is not None]

    def features(zona, w_ms):
        obj = datetime.fromtimestamp((w_ms + VENTANA_MS) / 1000, NY)
        finde, media = int(obj.weekday() >= 5), media_slot.get((zona, int(obj.weekday() >= 5), obj.hour), 0.0)
        h = ([media] * 4 + list(historial[zona]))[-4:]
        hora = obj.hour + obj.minute / 60
        return {"zona": zona, "obj_ms": w_ms + VENTANA_MS, "a_t": float(h[3]), "a_t1": float(h[2]), "a_t2": float(h[1]),
                "a_t3": float(h[0]), "neto_t": float(neto.get(zona, 0)), "hora_sin": math.sin(2 * math.pi * hora / 24),
                "hora_cos": math.cos(2 * math.pi * hora / 24), "finde_obj": float(finde), "media_slot_obj": float(media)}

    def ventanas(df, _bid):
        filas = df.select(F.unix_millis("window.start").alias("w"), "zona", "salidas", "llegadas").collect()
        por_ventana = defaultdict(dict)
        for f in filas:
            por_ventana[f["w"]][f["zona"]] = f
        for w in sorted(por_ventana):
            est["ventanas"] += 1
            if est["ventanas"] == 1:      # la primera ventana empezó a medias (calentamiento): no se evalúa
                continue
            obs = por_ventana[w]
            for z in ids_top:
                a = (obs[z]["salidas"] + obs[z]["llegadas"]) if z in obs else 0
                neto[z] = (obs[z]["llegadas"] - obs[z]["salidas"]) if z in obs else 0
                if (z, w) in pendientes:
                    est["err_abs"] += abs(pendientes.pop((z, w)) - a)
                    est["evaluadas"] += 1
                historial[z].append(a)
            for p in modelo.transform(spark.createDataFrame([features(z, w) for z in ids_top])).select("zona", "obj_ms", "prediction").collect():
                pendientes[(p["zona"], p["obj_ms"])] = max(0.0, p["prediction"])
            est["ventana_ms"] = w

    act = (eventos.filter("isValid").join(mapa, "stationId").withWatermark("eventTime", "10 minutes")
           .groupBy(F.window("eventTime", f"{VENTANA_MIN} minutes"), "zona")
           .agg(F.sum(F.when(F.col("tipoEvento") == "viaje.salida", 1).otherwise(0)).alias("salidas"),
                F.sum(F.when(F.col("tipoEvento") == "viaje.llegada", 1).otherwise(0)).alias("llegadas")))
    ck = U3 / "checkpoints" / RUN_ID
    q_cal = (eventos.writeStream.queryName("u3_calidad").foreachBatch(calidad).trigger(processingTime="5 seconds")
             .option("checkpointLocation", str(ck / "calidad")).start())
    q_act = (act.writeStream.queryName("u3_actividad").outputMode("append").foreachBatch(ventanas)
             .trigger(processingTime="5 seconds").option("checkpointLocation", str(ck / "actividad")).start())

    # Caso de error a propósito, a mitad de la ventana: un evento inválido y un viaje tardío (2 h antes del watermark)
    time.sleep(min(60, duracion_s // 2))
    wm = (q_act.lastProgress or {}).get("eventTime", {}).get("watermark")
    wm_ms = datetime.fromisoformat(wm.replace("Z", "+00:00")).timestamp() * 1000 if wm and not wm.startswith("1970") else time.time() * 1000
    ahora = int(time.time() * 1000)
    estacion = next(iter(est_zona))
    prueba = [{"tipoEvento": "viaje.desconocido", "stationId": estacion, "timestamp": ahora, "publishedAt": ahora, "origen": "u3-prueba-invalido"},
              {"tipoEvento": "viaje.salida", "stationId": estacion, "timestamp": int(wm_ms - 2 * 3600 * 1000), "publishedAt": ahora,
               "origen": "u3-prueba-tardio"}]
    (spark.createDataFrame([(e["stationId"], json.dumps(e)) for e in prueba], "key string, value string")
     .write.format("kafka").option("kafka.bootstrap.servers", KAFKA).option("topic", TOPIC).save())
    print("  inyectados: 1 evento inválido y 1 viaje tardío")

    time.sleep(max(0, duracion_s - (time.time() - t0)))
    progresos = q_act.recentProgress
    duraciones = [p["durationMs"]["triggerExecution"] for p in q_act.recentProgress + q_cal.recentProgress
                  if p.get("numInputRows", 0) > 0]
    descartados = sum(o.get("numRowsDroppedByWatermark", 0) for p in progresos for o in p.get("stateOperators", []))
    errores = {q.name: str(q.exception()) for q in (q_cal, q_act) if q.exception()}
    q_cal.stop(); q_act.stop()

    lat = est["latencias"]
    k = {"eventos_leidos": est["leidos"], "eventos_validos": est["validos"], "eventos_invalidos": est["leidos"] - est["validos"],
         "ratio_validos": round(est["validos"] / est["leidos"], 5) if est["leidos"] else None,
         "descartados_por_watermark": descartados, "ventanas_cerradas": est["ventanas"],
         "predicciones_evaluadas": est["evaluadas"],
         "mae_en_vivo": round(est["err_abs"] / est["evaluadas"], 4) if est["evaluadas"] else None, "mae_test": mae_test,
         "latencia_p50_ms": percentil(lat, .5), "latencia_p95_ms": percentil(lat, .95),
         "latencia_promedio_ms": round(sum(lat) / len(lat), 1) if lat else None,
         "microlote_p50_ms": percentil(duraciones, .5), "microlote_p95_ms": percentil(duraciones, .95),
         "ultima_ventana_nyc": datetime.fromtimestamp(est["ventana_ms"] / 1000, NY).strftime("%Y-%m-%d %H:%M") if est["ventana_ms"] else None,
         "duracion_s": round(time.time() - t0, 1)}
    print("\n  Compuertas de calidad del streaming")
    compuerta("sin errores en las consultas", "streaming", not errores, errores or "ok")
    compuerta("hay eventos en Kafka", "streaming", k["eventos_leidos"] > 0,
              f"{k['eventos_leidos']:,} eventos leídos (si es 0: ¿corre cbe-productor-viajes?)")
    compuerta("ventanas cerradas >= 3", "streaming", k["ventanas_cerradas"] >= 3, f"{k['ventanas_cerradas']} ventanas de 15 min")
    compuerta("evento inválido detectado", "streaming", k["eventos_invalidos"] >= 1, f"{k['eventos_invalidos']} inválido(s)", detiene=False)
    compuerta("viaje tardío descartado por watermark", "streaming", descartados >= 1, f"{descartados} fila(s)", detiene=False)
    compuerta("ratio de válidos >= 99 %", "streaming", (k["ratio_validos"] or 0) >= 0.99, f"{k['ratio_validos']}", detiene=False)
    compuerta("latencia p95 < 60 s", "streaming", k["latencia_p95_ms"] is not None and k["latencia_p95_ms"] < 60000,
              f"p50 {k['latencia_p50_ms']} ms · p95 {k['latencia_p95_ms']} ms", detiene=False)
    compuerta("MAE en vivo <= 2 x MAE de test", "streaming", k["mae_en_vivo"] is not None and k["mae_en_vivo"] <= 2 * mae_test,
              f"en vivo {k['mae_en_vivo']} vs test {mae_test}", detiene=False)
    return k


# ---------------------------------------------------------------- 4. publicar KPI y confirmar en Prometheus
def publicar(kpis, estado):
    print("\n4) BI/ML: publicar KPI en Pushgateway y confirmar en Prometheus")
    lin = [f'lambda26_u3_run_info{{run_id="{RUN_ID}",estado="{estado}"}} 1',
           f"lambda26_u3_compuertas_ok {sum(c['ok'] for c in compuertas)}",
           f"lambda26_u3_compuertas_total {len(compuertas)}",
           f"lambda26_u3_run_timestamp_seconds {int(time.time())}"]
    for nombre, v in kpis.items():
        if isinstance(v, (int, float)) and not isinstance(v, bool) and v is not None:
            lin.append(f"lambda26_u3_{nombre} {float(v)}")
    req = urllib.request.Request(f"{PUSHGATEWAY}/metrics/job/lambda26_u3_integracion", method="PUT",
                                 data=("\n".join(lin) + "\n").encode())
    try:
        urllib.request.urlopen(req, timeout=5).close()
        compuerta("KPI publicados en Pushgateway", "bi", True, f"{len(lin)} métricas", detiene=False)
    except Exception as ex:
        compuerta("KPI publicados en Pushgateway", "bi", False, str(ex), detiene=False)
        return
    # Se confirma por run_id: un valor de una corrida anterior NO cuenta (hallazgo de S14)
    q = urllib.parse.quote(f'lambda26_u3_run_info{{run_id="{RUN_ID}"}}')
    confirmado = False
    for _ in range(12):
        try:
            with urllib.request.urlopen(f"{PROMETHEUS}/api/v1/query?query={q}", timeout=5) as r:
                if json.load(r)["data"]["result"]:
                    confirmado = True
                    break
        except Exception:
            pass
        time.sleep(5)
    compuerta("Prometheus tiene los KPI de ESTA corrida", "bi", confirmado,
              f"run_id={RUN_ID}" if confirmado else "no apareció en 60 s", detiene=False)
    # Las 2 compuertas de BI se evalúan DESPUÉS del primer envío: se re-publica el conteo final para que
    # Grafana muestre el mismo total que el manifiesto. POST solo reemplaza estas métricas del grupo.
    final = (f"lambda26_u3_compuertas_ok {sum(c['ok'] for c in compuertas)}\n"
             f"lambda26_u3_compuertas_total {len(compuertas)}\n")
    try:
        urllib.request.urlopen(urllib.request.Request(f"{PUSHGATEWAY}/metrics/job/lambda26_u3_integracion",
                                                      method="POST", data=final.encode()), timeout=5).close()
        print(f"  conteo final de compuertas re-publicado: {sum(c['ok'] for c in compuertas)}/{len(compuertas)}")
    except Exception as ex:
        print(f"  aviso: no se pudo re-publicar el conteo final de compuertas ({ex})")


# ---------------------------------------------------------------- main
def main():
    global RUN_ID
    ap = argparse.ArgumentParser(description="Ejecución integrada U3 (batch + streaming + BI/ML) con compuertas y manifiesto")
    ap.add_argument("--duracion", type=int, default=240, help="segundos de streaming (por defecto 240)")
    ap.add_argument("--forzar-fallo", action="store_true", help="agrega una compuerta que falla a propósito")
    args = ap.parse_args()
    RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    inicio = time.time()
    print(f"=== U3 · Ejecución integrada · run_id {RUN_ID} ===")

    compuertas_previas(args.forzar_fallo)
    batch, streaming, version = {}, {}, None
    if not detenidas():
        from pyspark.sql import SparkSession, functions as F
        spark = (SparkSession.builder.appName(f"U3_Integracion_{RUN_ID}").master("local[*]")
                 .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
                 .config("spark.sql.shuffle.partitions", "4").config("spark.sql.session.timeZone", "America/New_York")
                 .config("spark.ui.showConsoleProgress", "false").getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        version = spark.version
        batch = flujo_batch(spark, F)
        if not detenidas():
            try:
                streaming = flujo_streaming(spark, F, args.duracion)
            except Exception as ex:      # el manifiesto se escribe igual: la falla queda registrada
                compuerta("streaming terminó sin excepción", "streaming", False, f"{type(ex).__name__}: {str(ex)[:300]}")
            for q in spark.streams.active:
                q.stop()
    estado = "OK" if not detenidas() else "DETENIDA"
    kpis = {**{f"batch_{k}": v for k, v in batch.items()}, **streaming,
            "duracion_total_s": round(time.time() - inicio, 1)}
    if any(c["nombre"] == "pushgateway" and c["ok"] for c in compuertas):
        publicar(kpis, estado)

    manifiesto = {"run_id": RUN_ID, "estado": estado,
                  "inicio_utc": datetime.fromtimestamp(inicio, timezone.utc).isoformat(timespec="seconds"),
                  "fin_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                  "parametros": {"duracion_streaming_s": args.duracion, "forzar_fallo": args.forzar_fallo,
                                 "ventana_min": VENTANA_MIN, "watermark": "10 minutes", "topic": TOPIC},
                  "versiones": {"spark": version, "python": sys.version.split()[0]},
                  "compuertas": compuertas,
                  "resumen_compuertas": {"ok": sum(c["ok"] for c in compuertas), "total": len(compuertas),
                                         "detienen_fallidas": [c["nombre"] for c in detenidas()]},
                  "kpis": kpis}
    (U3 / "manifiestos").mkdir(parents=True, exist_ok=True)
    ruta = U3 / "manifiestos" / f"manifiesto_{RUN_ID}.json"
    ruta.write_text(json.dumps(manifiesto, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"\n5) Manifiesto: {ruta.relative_to(RAIZ)}")
    print(f"=== Estado {estado} · compuertas {manifiesto['resumen_compuertas']['ok']}/{len(compuertas)} OK · "
          f"{kpis['duracion_total_s']} s ===")
    sys.exit(0 if estado == "OK" else 1)


RUN_ID = None
if __name__ == "__main__":
    main()
