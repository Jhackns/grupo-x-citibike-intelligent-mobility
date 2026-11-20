"""Lectura batch local GBFS: ninguna conexión ni importación del descargador."""

import gzip
import json
from pathlib import Path

from pyspark.sql import functions as F, types as T


def estructura_local(raw):
    """Inspección de todos los originales: claves reales, tipos y errores por fuente."""
    result = {}
    for folder in sorted(Path(raw).iterdir()):
        if not folder.is_dir():
            continue
        files = sorted(folder.rglob("*.json.gz"))
        keys, root_keys, data_keys, errors = {}, set(), set(), []
        for path in files:
            try:
                with gzip.open(path, "rt", encoding="utf-8") as stream:
                    obj = json.load(stream)
                root_keys.update(obj)
                data_keys.update(obj.get("data", {}))
                for array in obj.get("data", {}).values():
                    if isinstance(array, list):
                        for row in array:
                            for name, value in row.items():
                                keys.setdefault(name, set()).add(type(value).__name__)
            except (OSError, ValueError, TypeError, AttributeError, EOFError) as exc:
                errors.append({"archivo": str(path.relative_to(raw)), "error": type(exc).__name__})
        result[folder.name] = {"archivos": len(files), "raiz": sorted(root_keys),
                               "data": sorted(data_keys),
                               "campos": {k: sorted(v) for k, v in sorted(keys.items())},
                               "errores": errors}
    return result


def esquema(fuente):
    tipos = "array<struct<vehicle_type_id:string,count:bigint>>"
    campos = {
        "station_status": """station_id string, num_bikes_available bigint,
            num_ebikes_available bigint, num_bikes_disabled bigint,
            num_docks_available bigint, num_docks_disabled bigint,
            is_installed bigint, is_renting bigint, is_returning bigint,
            last_reported bigint, num_scooters_available bigint,
            num_scooters_unavailable bigint, vehicle_types_available """ + tipos,
        "station_information": """station_id string, name string, short_name string,
            lon double, lat double, region_id string, capacity bigint,
            rental_uris map<string,string>, is_charging boolean""",
        "vehicle_types": """vehicle_type_id string, form_factor string,
            propulsion_type string, max_range_meters double""",
    }
    if fuente not in campos:
        raise ValueError("Fuente no soportada; inspeccionar antes de definir otro esquema")
    array = "vehicle_types" if fuente == "vehicle_types" else "stations"
    # Todos los campos son nullable: el diagnóstico debe evidenciar ausencias.
    return T.StructType.fromDDL(
        f"data struct<{array}:array<struct<{campos[fuente]}>>>, "
        "last_updated bigint, ttl bigint, version string, _corrupt_record string")


def leer_fuente(spark, raw, fuente, archivos=None):
    # La integración fija una lista inventariada; el notebook conserva su API.
    paths = sorted(str(p) for p in archivos) if archivos is not None else sorted(
        str(p) for p in (Path(raw) / fuente).rglob("*.json.gz"))
    if not paths:
        raise FileNotFoundError(f"No hay capturas locales para {fuente}")
    feed = (spark.read.schema(esquema(fuente)).option("multiLine", True)
            .option("mode", "PERMISSIVE").json(paths)
            .withColumn("archivo_origen", F.input_file_name()))
    stamp = F.regexp_extract("archivo_origen",
        r"/(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d{3}Z)-", 1)
    feed = (feed.withColumn("captura_utc", F.to_timestamp(stamp, "yyyy-MM-dd'T'HH-mm-ss-SSS'Z'"))
            .withColumn("last_updated_utc", F.timestamp_seconds("last_updated")))
    array = "vehicle_types" if fuente == "vehicle_types" else "stations"
    # explode conserva todas las entradas, incluso iguales; arrays vacíos quedan
    # contabilizados en el feed, sin inventar una estación nula.
    rows = (feed.select("archivo_origen", "captura_utc", "last_updated", "last_updated_utc",
                        "ttl", "version", F.explode(f"data.{array}").alias("registro"))
            .select("archivo_origen", "captura_utc", "last_updated", "last_updated_utc",
                    "ttl", "version", "registro.*")
            .withColumn("captura_nueva_york", F.from_utc_timestamp("captura_utc", "America/New_York"))
            .withColumn("fecha_nueva_york", F.to_date("captura_nueva_york")))
    if fuente == "station_status":
        rows = rows.withColumn("last_reported_utc", F.timestamp_seconds("last_reported"))
    return feed, rows
