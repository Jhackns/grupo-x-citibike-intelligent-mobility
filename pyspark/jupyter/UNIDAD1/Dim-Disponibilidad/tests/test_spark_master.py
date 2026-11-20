"""Smoke test: la fábrica compartida usa 4/2 hilos sobre 32 filas sintéticas."""
import importlib.util
import unittest

from src.ejecucion import configuracion, crear_spark


@unittest.skipUnless(importlib.util.find_spec('pyspark'), 'PySpark se ejecuta en Docker')
class SparkMasterTests(unittest.TestCase):
    def test_master_real_y_paralelismo_sin_raw_ni_modelos(self):
        for master, expected in ((None, 4), ('local[2]', 2)):
            with self.subTest(master=master or 'predeterminado'):
                cfg = configuracion(spark_master=master)
                spark = crear_spark(cfg, app_name='U1-prueba-pequena-spark-master')
                try:
                    spark.sparkContext.setLogLevel('ERROR')
                    self.assertEqual(spark.sparkContext.master, f'local[{expected}]')
                    self.assertEqual(spark.sparkContext.defaultParallelism, expected)
                    self.assertEqual(spark.conf.get('spark.sql.shuffle.partitions'), str(cfg['shuffle_partitions']))
                    self.assertEqual(cfg['driver_memory'], '4g')
                    result = spark.range(32, numPartitions=8).rdd.map(lambda row: row.id * 2).sum()
                    self.assertEqual(result, 992)
                    print(f'Verificado {spark.sparkContext.master}: paralelismo={expected}, filas=32, suma={result}', flush=True)
                finally:
                    spark.stop()


if __name__ == '__main__':
    unittest.main()
