"""Contratos temporales: tolerancia, huecos, calidad futura y purga de cortes."""
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
try:
    from pyspark.sql import SparkSession, functions as F
    from src.preparacion import construir_variables, construir_gold, validar_entrada, verificar_gold
    SPARK = True
except ImportError:
    SPARK = False


@unittest.skipUnless(SPARK, 'Requiere PySpark del Docker existente')
class PreparacionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master('local[2]').appName('test-gold')
            .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','2').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_tiempo_no_posiciones_huecos_y_cortes(self):
        epoch = 1794268800  # 2026-11-10 00:00 UTC
        df = (self.spark.createDataFrame([(x,) for x in (0,308,600,1200,1500,1800,2100)], ['s'])
            .withColumn('station_id',F.lit('A'))
            .withColumn('captura_utc', F.timestamp_seconds(F.col('s')+epoch))
            .withColumn('captura_nueva_york',F.from_utc_timestamp('captura_utc','America/New_York'))
            .withColumn('archivo_origen',F.concat(F.lit('raw/'),F.col('s').cast('string')))
            .withColumn('last_updated',F.col('s')+epoch-5).withColumn('last_reported',F.col('s')+epoch-10)
            .withColumn('regla_version',F.lit('test'))
            .withColumn('elegible_convencionales', F.col('s') != 1500)
            .withColumn('elegible_electricas',F.lit(True))
            .withColumn('objetivo_convencionales',F.when(F.col('elegible_convencionales'),F.col('s')/300))
            .withColumn('objetivo_electricas',F.lit(2).cast('long'))
            .withColumn('num_bikes_available',F.lit(5)).withColumn('num_ebikes_available',F.lit(2))
            .withColumn('vehicle_types_available',F.array(F.struct(F.lit('1').alias('vehicle_type_id'), F.lit(3).alias('count')))))
        for c in ('q_discrepancia_total','q_discrepancia_convencionales','q_discrepancia_electricas'):
            df = df.withColumn(c,F.col('s') == 308)
        validar_entrada(df)
        variables, features = construir_variables(df)
        variables = variables.cache()
        gold = construir_gold(variables,'2026-11-10 00:20:00','2026-11-10 00:30:00',
                              '2026-11-10 00:40:00').cache()
        rows = {(r.s,r.horizonte_min):r for r in gold.collect()}
        self.assertEqual(len(rows),21)
        self.assertEqual(rows[0,5].horizonte_efectivo_s,308)
        self.assertTrue(rows[0,5].aceptado_gold)
        self.assertEqual(rows[0,5].estrato_discrepancia,'con_discrepancias')
        self.assertFalse(rows[0,15].aceptado_gold)  # no captura a 15 min, pese a existir tercera fila futura
        self.assertIn('sin_captura_objetivo_en_tolerancia',rows[0,15].motivos_gold_electricas)
        self.assertFalse(rows[600,10].aceptado_gold)  # hueco y objetivo en validación
        self.assertIn('hueco_o_inactividad_en_trayecto',rows[600,10].motivos_gold_electricas)
        self.assertIn('objetivo_cruza_corte_temporal',rows[600,10].motivos_gold_electricas)
        self.assertFalse(rows[1200,5].elegible_gold_convencionales)  # calidad en el futuro
        self.assertTrue(rows[1200,5].elegible_gold_electricas)
        self.assertFalse(rows[1500,5].aceptado_gold)  # validación -> prueba
        self.assertTrue(rows[1800,5].aceptado_gold)
        v = variables.where('s = 1200').first()
        self.assertIsNone(v.electricas_lag5)
        self.assertIsNone(v.electricas_media15)
        self.assertEqual(v.electricas_n15,0)  # cantidad de muestras, no imputación de bicicletas
        changed = df.withColumn('objetivo_electricas',F.when(F.col('s')==1800,999).otherwise(2))
        altered, _ = construir_variables(changed)
        altered = altered.where('s = 1200').first()
        self.assertEqual([v[c] for c in features],[altered[c] for c in features])
        self.assertTrue(verificar_gold(gold,features))
        context=construir_gold(variables,'2026-11-10 00:20:00','2026-11-10 00:30:00',
                              '2026-11-10 00:40:00',inicio_modelado='2026-11-10 00:05:00')
        excluded=context.where('s=0 AND horizonte_min=5').first()
        self.assertFalse(excluded.aceptado_gold)
        self.assertIn('emision_fuera_periodo',excluded.motivos_gold_electricas)
        # El contexto anterior sigue disponible para construir rezagos.
        self.assertIsNotNone(variables.where('s=308').first().electricas_lag5)
        with self.assertRaises(ValueError):
            validar_entrada(df.unionByName(df.limit(1)))
        gold.unpersist(); variables.unpersist()


if __name__ == '__main__':
    unittest.main()
