"""Contratos Silver: objetivos independientes, preservación y reconciliación."""

from datetime import datetime, timezone
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
try:
    from pyspark.sql import SparkSession, functions as F
    from src.calidad import aplicar_reglas, guardar_y_verificar
    SPARK = True
except ImportError:
    SPARK = False


@unittest.skipUnless(SPARK, 'Ejecutar en PySpark Docker; sin instalar dependencias en .venv')
class CalidadTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master('local[2]').appName('test-calidad')
            .config('spark.sql.session.timeZone', 'UTC').config('spark.sql.shuffle.partitions', '2').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_elegibilidad_preservacion_exclusion_y_particiones(self):
        epoch = 1794300000
        def row(id, total=12, electricas=2, human=10, elec=2, **changes):
            return {'station_id': id, 'captura_utc': datetime.fromtimestamp(epoch + 100, timezone.utc),
                'archivo_origen': 'file:///raw/a.json.gz', 'last_updated': epoch,
                'last_reported': epoch - 10, 'num_bikes_available': total,
                'num_ebikes_available': electricas, 'num_scooters_available': None,
                'is_installed': 1, 'is_renting': 1, 'is_returning': 1,
                'vehicle_types_available': [('H', human), ('E', elec)], **changes}
        rows = [row('ambos'), row('conv', vehicle_types_available=[('H',10)]),
                row('elec', vehicle_types_available=[('E',2)]), row('discrepancia',total=13,electricas=5),
                row('inactivo', is_renting=0, last_reported=86400),
                row('antiguo', last_reported=epoch-1000),
                row('sin_electrica', num_ebikes_available=None),
                row('conflicto'), row('conflicto', total=15), row('repetido'),
                row('repetido', captura_utc=datetime.fromtimestamp(epoch+200, timezone.utc)),
                row('duplicado'), row('duplicado'), row('sin_devolucion', is_returning=0),
                row('human_negativo', human=-1),
                row('tipo2_duplicado', vehicle_types_available=[('H',10),('E',1),('E',1)])]
        schema = '''station_id string, captura_utc timestamp, archivo_origen string,
            last_updated long, last_reported long, num_bikes_available long,
            num_ebikes_available long, num_scooters_available long,
            is_installed long, is_renting long, is_returning long,
            vehicle_types_available array<struct<vehicle_type_id:string,count:long>>'''
        df = self.spark.createDataFrame(rows, schema)
        tipos = self.spark.createDataFrame([('H','human'), ('E','electric_assist')],
                                           ['vehicle_type_id','propulsion_type'])
        result = aplicar_reglas(df, tipos).cache()
        allrows = result.collect()
        byid = {r.station_id:r for r in allrows}
        self.assertEqual(len(allrows), len(rows))
        self.assertTrue(byid['ambos'].elegible_convencionales)
        self.assertTrue(byid['ambos'].elegible_electricas)
        self.assertTrue(byid['conv'].elegible_convencionales)
        self.assertFalse(byid['conv'].elegible_electricas)
        self.assertFalse(byid['elec'].elegible_convencionales)
        self.assertTrue(byid['elec'].elegible_electricas)
        self.assertFalse(byid['inactivo'].registro_invalido)
        self.assertEqual(byid['inactivo'].tipo_registro, 'inactivo')
        self.assertFalse(byid['antiguo'].aceptado_silver)
        self.assertTrue(byid['conflicto'].registro_invalido)
        self.assertIsNone(byid['sin_electrica'].num_ebikes_available)
        self.assertEqual(byid['sin_electrica'].objetivo_electricas, 2)
        self.assertTrue(byid['discrepancia'].aceptado_silver)
        self.assertEqual(byid['discrepancia'].objetivo_convencionales, 10)
        self.assertEqual(byid['discrepancia'].objetivo_electricas, 2)
        self.assertTrue(byid['sin_devolucion'].aceptado_silver)
        self.assertFalse(byid['human_negativo'].elegible_convencionales)
        self.assertTrue(byid['human_negativo'].elegible_electricas)
        self.assertTrue(byid['tipo2_duplicado'].elegible_convencionales)
        self.assertFalse(byid['tipo2_duplicado'].elegible_electricas)
        self.assertEqual(sum(r.station_id=='repetido' for r in allrows),2)
        self.assertEqual(sum(r.station_id=='duplicado' for r in allrows),2)
        self.assertTrue(all(r.num_scooters_available is None for r in allrows))
        with tempfile.TemporaryDirectory() as tmp:
            silver, excluded, counts = guardar_y_verificar(result, Path(tmp)/'run')
            self.assertEqual(counts['entrada'], len(rows))
            self.assertEqual(counts['aceptados']+counts['excluidos'],len(rows))
            self.assertTrue(counts['multiconjunto_reconciliado'])
            from src.diagnostico import demostrar_rdd_y_ejecucion
            demonstration=demostrar_rdd_y_ejecucion(silver.unionByName(excluded),Path(tmp)/'demo')
            self.assertTrue(demonstration['rdd_reconcilia_con_dataframe'])
            self.assertEqual(sum(x['n'] for x in demonstration['categorias_rdd']),len(rows))
            self.assertEqual(demonstration['jobs_antes'],demonstration['jobs_tras_plan'])
            consulta = silver.where(F.col('fecha_captura_utc')==F.lit('2026-11-10').cast('date'))
            self.assertIn('PartitionFilters:',consulta._jdf.queryExecution().executedPlan().toString())
            with self.assertRaises(FileExistsError):
                guardar_y_verificar(result, Path(tmp)/'run')
        ambiguous = tipos.union(self.spark.createDataFrame([('H','electric_assist')],tipos.schema))
        pending = aplicar_reglas(df.where("station_id = 'ambos'"), ambiguous).first()
        self.assertFalse(pending.aceptado_silver)
        self.assertIsNone(pending.objetivo_convencionales)
        result.unpersist()


if __name__ == '__main__':
    unittest.main()
