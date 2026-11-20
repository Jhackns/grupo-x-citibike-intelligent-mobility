"""Fit exclusivo de train, política fija, métricas, ausencias y recarga."""
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
try:
    from pyspark.sql import SparkSession, functions as F
    from pyspark.ml import PipelineModel
    from pyspark.ml.regression import LinearRegression
    from src.modelado import (ajustar_preproceso,acotar,metricas,tabla_metricas,elegir,
                              guardar_json,cargar_seleccion,predecir)
    SPARK=True
except ImportError:
    SPARK=False


@unittest.skipUnless(SPARK,'Usar PySpark Docker, sin instalar en .venv')
class ModeladoTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark=(SparkSession.builder.master('local[2]').appName('test-modelado')
            .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','2').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_mediana_train_orden_y_recarga_pipeline(self):
        schema='convencionales_actual double, convencionales_lag5 double, label double'
        train=self.spark.createDataFrame([(float(i),None if i==0 else float(i-1),float(i)+0.1) for i in range(10)],schema)
        valid=self.spark.createDataFrame([(1000.0,None,1000.1)],schema)
        prep,info=ajustar_preproceso(train,['convencionales_actual','convencionales_lag5'])
        self.assertEqual(info['medianas_train']['convencionales_lag5'],4.0)
        transformed=prep.transform(valid).first()
        self.assertEqual(transformed['convencionales_lag5__mediana'],4.0)
        self.assertEqual(transformed['convencionales_lag5__ausente'],1.0)
        self.assertEqual(info['orden_vector'],['convencionales_actual__mediana','convencionales_lag5__mediana',
                                              'convencionales_actual__ausente','convencionales_lag5__ausente'])
        model=LinearRegression(labelCol='label',regParam=0.1,maxIter=10).fit(prep.transform(train))
        pipe=PipelineModel(stages=prep.stages+[model])
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)
            pipe.write().save(str(path/'pipeline'))
            guardar_json(path/'contrato.json',{'entrenamiento_necesario':True})
            loaded,_=cargar_seleccion(path)
            expected=predecir(valid,pipe,'convencionales_actual').first().prediction
            self.assertAlmostEqual(predecir(valid,loaded,'convencionales_actual').first().prediction,expected,places=10)
            ref=path/'referencia'
            guardar_json(ref/'contrato.json',{'entrenamiento_necesario':True,'pipeline_relativo':'../pipeline'})
            reference,_=cargar_seleccion(ref)
            self.assertAlmostEqual(predecir(valid,reference,'convencionales_actual').first().prediction,expected,places=10)
            guardar_json(path/'contrato.json',{'entrenamiento_necesario':False})
            baseline,_=cargar_seleccion(path)
            self.assertIsNone(baseline)
            self.assertEqual(predecir(valid,baseline,'convencionales_actual').first().prediction,1000.0)
        with self.assertRaises(ValueError):
            ajustar_preproceso(train,['f_num_bikes_available'])

    def test_empates_clipping_metricas_y_subgrupos(self):
        self.assertEqual(elegir([{'candidato':'lr_ridge_001','MAE':1.0},
                                 {'candidato':'persistencia','MAE':1.0000005}])['candidato'],'persistencia')
        df=self.spark.createDataFrame([(0.,-2.,0.),(2.,2.,1.),(5.,4.,5.)],['label','prediction_raw','actual'])
        pred=acotar(df).withColumn('cambio',F.col('label')!=F.col('actual'))
        result=metricas(pred)
        self.assertEqual(result['n'],3)
        self.assertEqual(result['predicciones_negativas_raw'],1)
        self.assertAlmostEqual(result['MAE'],1/3)
        self.assertAlmostEqual(result['RMSE'],(1/3)**0.5)
        self.assertEqual(sum(r.n for r in tabla_metricas(pred,['cambio']).collect()),3)
        invalid=self.spark.createDataFrame([(1.,None)],'label double,prediction_raw double')
        self.assertIsNone(acotar(invalid).first().prediction)
        with self.assertRaises(ValueError):
            metricas(acotar(invalid))


if __name__=='__main__':
    unittest.main()
