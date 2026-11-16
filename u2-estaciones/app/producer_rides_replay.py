"""
U2 - Dimension Estaciones (Grimaldo Arredondo) - fuente de EVENTOS EMPRESARIALES.

Reproduce en orden cronologico un dia del historico real de Citi Bike NYC
(los mismos CSV de U1) y publica cada viaje como DOS eventos de negocio en el
topic `citibike-rides`:

  - viaje.salida   -> en `started_at`, con la estacion de origen
  - viaje.llegada  -> en `ended_at`,   con la estacion de destino

La key de Kafka es el stationId: todos los eventos de una estacion caen en la
misma particion y conservan su orden.

El `timestamp` del evento es la hora ORIGINAL del viaje (event time). El reloj
de la reproduccion se comprime con SPEEDUP (60 = 1 minuto historico por cada
segundo real), asi un dia completo se reproduce en 24 minutos y las ventanas
de 15 minutos se cierran cada ~15 s reales. `publishedAt` es la hora real de
publicacion (sirve para medir latencia de ingesta en Spark).

Por defecto se reproduce un dia del periodo de PRUEBA (>= 2026-07-24, el mismo
corte temporal de U1) para no reproducir datos con los que se entreno el modelo.
"""
import csv
import glob
import json
import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from kafka import KafkaProducer

DATA_GLOB = os.getenv("RIDES_CSV_GLOB", "/work/data/*.csv")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC_RIDES", "citibike-rides")
REPLAY_DATE = os.getenv("REPLAY_DATE", "2026-07-28")          # dia del periodo de prueba
REPLAY_START_HOUR = int(os.getenv("REPLAY_START_HOUR", "6"))   # empezar con actividad
SPEEDUP = float(os.getenv("SPEEDUP", "60"))                    # segundos historicos por segundo real
LOOP = os.getenv("REPLAY_LOOP", "false").lower() == "true"     # al terminar, vuelve a empezar
TZ = ZoneInfo("America/New_York")                               # los CSV vienen en hora local de NYC
FORMATOS = ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S")


def log(**campos):
    print(json.dumps({"service": "u2-estaciones", "component": "producer-rides", **campos}), flush=True)


def parse_ts(texto):
    for fmt in FORMATOS:
        try:
            return datetime.strptime(texto, fmt).replace(tzinfo=TZ)
        except ValueError:
            continue
    return None


def cargar_eventos():
    """Lee los CSV y arma la lista de eventos (salidas y llegadas) del dia elegido."""
    archivos = sorted(glob.glob(DATA_GLOB))
    if not archivos:
        raise SystemExit(f"No hay CSV en {DATA_GLOB}: copia los CSV de U1 a pyspark/jupyter/UNIDAD2/Dim-Estaciones/data/")

    inicio_dia = datetime.strptime(REPLAY_DATE, "%Y-%m-%d").replace(hour=REPLAY_START_HOUR, tzinfo=TZ)
    fin_dia = datetime.strptime(REPLAY_DATE, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=TZ)

    eventos, vistos, descartados = [], set(), 0
    for ruta in archivos:
        with open(ruta, newline="", encoding="utf-8") as f:
            for fila in csv.DictReader(f):
                # Filtro barato primero: solo interesan los viajes que tocan el dia reproducido
                # (asi el set de deduplicacion guarda ~150 mil ids y no 5 millones).
                if not (fila["started_at"].startswith(REPLAY_DATE) or fila["ended_at"].startswith(REPLAY_DATE)):
                    continue
                # Mismas reglas de calidad que la capa Silver de U1
                if fila["ride_id"] in vistos:
                    descartados += 1
                    continue
                ini, fin = parse_ts(fila["started_at"]), parse_ts(fila["ended_at"])
                if ini is None or fin is None or not fila["start_station_id"] or not fila["end_station_id"]:
                    descartados += 1
                    continue
                duracion_min = (fin - ini).total_seconds() / 60
                if not (1.0 <= duracion_min <= 180.0):
                    descartados += 1
                    continue
                vistos.add(fila["ride_id"])
                comun = {"rideId": fila["ride_id"], "rideableType": fila["rideable_type"],
                         "memberCasual": fila["member_casual"]}
                if inicio_dia <= ini <= fin_dia:
                    eventos.append((ini, "viaje.salida", fila["start_station_id"], fila["start_station_name"], comun))
                if inicio_dia <= fin <= fin_dia:
                    eventos.append((fin, "viaje.llegada", fila["end_station_id"], fila["end_station_name"], comun))

    ordenados = sorted(eventos, key=lambda e: e[0])  # orden cronologico por event time
    log(status="loaded", archivos=len(archivos), replayDate=REPLAY_DATE, desdeHora=REPLAY_START_HOUR,
        eventos=len(ordenados), descartadosCalidad=descartados)
    return ordenados


def main():
    eventos = cargar_eventos()
    if not eventos:
        raise SystemExit(f"El dia {REPLAY_DATE} no tiene viajes en los CSV. Cambia REPLAY_DATE.")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
    )
    log(status="starting", topic=TOPIC, speedup=SPEEDUP, eventos=len(eventos))

    vuelta = 0
    while True:
        vuelta += 1
        t0_hist = eventos[0][0]
        t0_real = time.time()
        enviados = 0
        for ts, tipo, station_id, station_name, comun in eventos:
            # Espera hasta el instante real que le corresponde a este evento
            objetivo = t0_real + (ts - t0_hist).total_seconds() / SPEEDUP
            espera = objetivo - time.time()
            if espera > 0:
                time.sleep(espera)
            evento = {
                "tipoEvento": tipo,
                "stationId": station_id,
                "stationName": station_name,
                **comun,
                "origen": "citibike-tripdata-replay",
                "timestamp": int(ts.timestamp() * 1000),
                "publishedAt": int(time.time() * 1000),
            }
            producer.send(TOPIC, key=station_id, value=evento)
            enviados += 1
            if enviados % 2000 == 0:
                log(status="published", enviados=enviados, total=len(eventos),
                    horaHistorica=ts.strftime("%Y-%m-%d %H:%M"), vuelta=vuelta)
        producer.flush()
        log(status="replay_finished", enviados=enviados, vuelta=vuelta)
        if not LOOP:
            break
        # En modo bucle se repite el mismo dia desplazado 24 h: el event time sigue creciendo
        # y el watermark no descarta la segunda vuelta como dato tardio.
        eventos = [(ts + timedelta(days=1), *resto) for ts, *resto in eventos]


if __name__ == "__main__":
    main()
