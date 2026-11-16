"""
U2 - Dimension Estaciones (Grimaldo Arredondo) - fuente IoT / TELEMETRIA.

Los anclajes (docks) de cada estacion Citi Bike son dispositivos con sensores
que reportan cuantas bicicletas tienen ancladas. El sistema publica ese estado
en el feed abierto GBFS (`station_status.json`), con la hora del ultimo reporte
del dispositivo (`last_reported`). Este script lo lee en vivo y lo publica en el
topic `citibike-estaciones`.

Ajustes de esquema y volumen (criterio 2):
  - Solo se siguen las estaciones TOP de la dimension (artifacts/estaciones_top.json,
    generado por el notebook 01): mismo conjunto que el modelo de series de tiempo.
  - Enlace con el historico: el feed usa un `station_id` UUID, pero los CSV usan el
    `short_name` (p. ej. "6140.05"). Se resuelve con `station_information.json`
    y el evento sale con `stationId` = short_name, igual que en `citibike-rides`.
  - `timestamp` = `last_reported` (hora del sensor), no la hora de lectura: el
    feed puede repetir el mismo reporte en varias lecturas. Spark deduplica con
    (stationId, timestamp) dentro del watermark.
  - Volumen = NUM_ESTACIONES / GBFS_INTERVAL_S eventos por segundo (20 / 30 s ~ 0.67 ev/s).
"""
import json
import os
import time
import urllib.request

from kafka import KafkaProducer

BASE = os.getenv("GBFS_BASE", "https://gbfs.citibikenyc.com/gbfs/en")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC_ESTACIONES", "citibike-estaciones")
INTERVAL_S = int(os.getenv("GBFS_INTERVAL_S", "30"))
TOP_JSON = os.getenv("ESTACIONES_TOP_JSON", "/work/artifacts/estaciones_top.json")
NUM_ESTACIONES = int(os.getenv("GBFS_NUM_ESTACIONES", "20"))


def log(**campos):
    print(json.dumps({"service": "u2-estaciones", "component": "producer-gbfs", **campos}), flush=True)


def leer(nombre):
    req = urllib.request.Request(f"{BASE}/{nombre}.json", headers={"User-Agent": "lambda26-u2-estaciones"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def estaciones_objetivo():
    """(short_name, nombre) de las estaciones a seguir (las top del notebook 01)."""
    with open(TOP_JSON, encoding="utf-8") as f:
        top = json.load(f)
    return [(e["station_id"], e.get("station_name")) for e in top["estaciones"]][:NUM_ESTACIONES]


def enlazar(objetivo):
    """short_name del CSV -> (uuid del feed, nombre, capacidad). Primero por short_name; si no, por nombre exacto."""
    info = leer("station_information")["data"]["stations"]
    por_short = {e.get("short_name"): e for e in info}
    por_nombre = {(e.get("name") or "").strip().lower(): e for e in info}
    enlazadas, sin_enlace, por_que = {}, [], {}
    for short_name, nombre in objetivo:
        e = por_short.get(short_name)
        via = "short_name"
        if e is None and nombre:
            e, via = por_nombre.get(nombre.strip().lower()), "nombre"
        if e is None:
            sin_enlace.append(short_name)
            continue
        enlazadas[short_name] = (e["station_id"], e.get("name"), e.get("capacity"))
        por_que[via] = por_que.get(via, 0) + 1
    return enlazadas, sin_enlace, por_que


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    objetivo = estaciones_objetivo()
    enlazadas, sin_enlace, por_que = enlazar(objetivo)
    log(status="starting", topic=TOPIC, intervalS=INTERVAL_S, objetivo=len(objetivo),
        enlazadas=len(enlazadas), enlazadasPor=por_que, sinEnlace=sin_enlace)
    if not enlazadas:
        raise SystemExit("Ninguna estacion top se encontro en station_information (revisa short_name).")

    ciclo = 0
    while True:
        ciclo += 1
        try:
            estado = {e["station_id"]: e for e in leer("station_status")["data"]["stations"]}
        except Exception as ex:
            log(status="gbfs_unreachable", error=str(ex))
            time.sleep(INTERVAL_S)
            continue

        publicados = 0
        for short_name, (uuid, nombre, capacidad) in enlazadas.items():
            e = estado.get(uuid)
            if e is None:
                log(status="station_missing", stationId=short_name)
                continue
            evento = {
                "tipoEvento": "estacion.estado",
                "stationId": short_name,
                "gbfsStationId": uuid,
                "stationName": nombre,
                "capacidad": capacidad,
                "bicisDisponibles": e.get("num_bikes_available"),
                "ebikesDisponibles": e.get("num_ebikes_available"),
                "docksDisponibles": e.get("num_docks_available"),
                "bicisInhabilitadas": e.get("num_bikes_disabled"),
                "docksInhabilitados": e.get("num_docks_disabled"),
                "enServicio": bool(e.get("is_installed") == 1 and e.get("is_renting") == 1),
                "origen": "gbfs",
                "timestamp": int(e.get("last_reported", 0)) * 1000,   # hora del sensor
                "publishedAt": int(time.time() * 1000),
            }
            producer.send(TOPIC, key=short_name, value=evento)
            publicados += 1
        producer.flush()
        log(status="published", ciclo=ciclo, publicados=publicados)
        time.sleep(INTERVAL_S)


if __name__ == "__main__":
    main()
