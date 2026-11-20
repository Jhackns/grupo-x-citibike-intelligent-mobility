"""Ejecutar en Docker: lectura real de gzip y contratos de cardinalidad/tiempo."""

import gzip
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from pyspark.sql import SparkSession
    from src.lectura import leer_fuente, estructura_local
    from src.diagnostico import compatibilidad, cantidades_por_catalogo, intervalos
    SPARK = True
except ImportError:
    SPARK = False


@unittest.skipUnless(SPARK, "Requiere PySpark del contenedor; no instalar en .venv")
class LecturaTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]").appName("pruebas-disponibilidad")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.sql.shuffle.partitions", "2").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_conserva_repeticiones_y_no_deduplica_por_last_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            record = {"station_id": "A", "num_bikes_available": 3,
                      "num_ebikes_available": 1, "last_reported": 1794268800,
                      "vehicle_types_available": [{"vehicle_type_id": "H", "count": 2},
                                                  {"vehicle_type_id": "E", "count": 1}]}
            for minute in (0, 5):
                folder = root / "station_status" / "2026-11-10"
                folder.mkdir(parents=True, exist_ok=True)
                path = folder / f"2026-11-10T00-{minute:02}-00-000Z-abc.json.gz"
                with gzip.open(path, "wt") as stream:
                    json.dump({"last_updated":1794268800 + minute*60,
                               "data":{"stations":[record, record]}}, stream)
            feed, df = leer_fuente(self.spark, root, "station_status")
            self.assertEqual(feed.count(), 2)
            self.assertEqual(df.count(), 4)  # Dos iguales por archivo y reporte repetido.
            self.assertEqual(df.select("archivo_origen").distinct().count(), 2)
            self.assertEqual(str(df.first().fecha_nueva_york), "2026-11-09")
            self.assertEqual(intervalos(feed, "last_updated_utc").first().intervalo_s, 300)
            types = self.spark.createDataFrame([("H", "human"), ("E", "electric_assist")],
                                                ["vehicle_type_id", "propulsion_type"])
            checked = cantidades_por_catalogo(df, types)
            self.assertEqual(checked.count(), 4)
            self.assertEqual(checked.first().convencionales_catalogo, 2)
            self.assertEqual(estructura_local(root)["station_status"]["archivos"], 2)
            info = self.spark.createDataFrame([("A",), ("A",)], ["station_id"])
            audit = compatibilidad(df, info, types)
            self.assertEqual(audit["station_information"]["ids_repetidos"], 1)
            self.assertEqual(df.count(), 4)  # Auditar claves no enriquece ni multiplica.
            ambiguous = self.spark.createDataFrame([("H", "human"), ("H", "electric_assist")],
                                                   ["vehicle_type_id", "propulsion_type"])
            with self.assertRaises(ValueError):
                cantidades_por_catalogo(df, ambiguous)


if __name__ == "__main__":
    unittest.main()
