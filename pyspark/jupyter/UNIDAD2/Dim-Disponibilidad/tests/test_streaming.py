"""Pruebas Spark pequeñas: sin API, Kafka real ni entrenamiento."""
from pathlib import Path
from datetime import datetime
from copy import deepcopy
from tempfile import TemporaryDirectory
import importlib.util
import json
import sys
import types
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'tests')]
from disponibilidad_u2.utilidades import guardar,serializar
from disponibilidad_u2.config_streaming import cargar_streaming
try:
    # El directorio pyspark/ del repositorio también puede ser un namespace;
    # solo pyspark.sql identifica una instalación real, no ese directorio.
    HAS_SPARK=importlib.util.find_spec('pyspark.sql') is not None
except ModuleNotFoundError:
    HAS_SPARK=False
if HAS_SPARK:
    from pyspark.sql import SparkSession,functions as F,types as T
    from disponibilidad_u2.predictor_streaming import cargar_u1,predecir
    from disponibilidad_u2.calidad_streaming import normalizar,aplicar_calidad,deduplicar,metricas_ventana,CATALOGO_U1,CATALOGO_DDL
    from disponibilidad_u2.salidas_streaming import publicar_batch
    from disponibilidad_u2.contrato import construir_captura
    from test_ingesta import muestra


@unittest.skipUnless(HAS_SPARK,'Ejecutar con spark-submit en Docker')
class StreamingTests(unittest.TestCase):
    evidence={}

    @classmethod
    def setUpClass(cls):
        cls.spark=(SparkSession.builder.appName('U2-pruebas-streaming')
            .config('spark.ui.enabled','false').config('spark.ui.showConsoleProgress','false')
            .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','2')
            .config('spark.sql.adaptive.enabled','false').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')
        cls.cfg=cargar_streaming()
        cls.u1=cargar_u1(cls.cfg['u1_reference'])

    @classmethod
    def tearDownClass(cls):cls.spark.stop()

    def quality_fixture(self):
        raw,_,_=muestra();base=deepcopy(raw['feed']['data']['stations'][0]);cases=[]
        edits=[{}, {'vehicle_types_available':[{'vehicle_type_id':'1','count':4},{'vehicle_type_id':'2','count':None}]},
               {'is_renting':0},{'is_returning':0},{'last_reported':1699999549},
               {'is_installed':None},{'last_reported':None},
               {'vehicle_types_available':[{'vehicle_type_id':'1','count':-1},{'vehicle_type_id':'2','count':3}]}]
        for i,update in enumerate(edits):
            cases.append({**deepcopy(base),'station_id':str(i),**update})
        raw['feed']['data']['stations']=cases
        events=construir_captura(raw)['events']
        stale=deepcopy(raw)
        stale['feed']['last_updated']=1699999554
        stale['feed']['data']['stations']=[{**deepcopy(base),'station_id':'8','last_reported':1699999550}]
        events.extend(construir_captura(stale)['events'])
        schema='json_original string,kafka_key string,kafka_topic string,kafka_partition int,kafka_offset bigint,kafka_timestamp timestamp,kafka_timestamp_type int'
        rows=[(serializar(e).decode(),e['station_id'],'citibike-disponibilidad-fixture',0,i,
               datetime.fromisoformat(e['captured_at_utc'].replace('Z','+00:00')),0) for i,e in enumerate(events)]
        return normalizar(self.spark.createDataFrame(rows,schema),self.u1,self.cfg)

    def test_01_paridad_calidad_u1(self):
        base=self.quality_fixture().cache()
        try:
            q=aplicar_calidad(base,self.u1)
            ref=self.u1['modulos']['calidad'].aplicar_reglas(base,self.spark.createDataFrame(CATALOGO_U1,CATALOGO_DDL),450)
            columns=['event_id','convencionales_tipo','electricas_tipo','tipo_registro','edad_reporte_s',
                'objetivo_convencionales','objetivo_electricas','elegible_convencionales','elegible_electricas',
                'motivos_no_elegible_convencionales','motivos_no_elegible_electricas']+[c for c in ref.columns if c.startswith('q_')]
            self.assertEqual(q.select(columns).exceptAll(ref.select(columns)).count(),0)
            self.assertEqual(ref.select(columns).exceptAll(q.select(columns)).count(),0)
            rows={r.station_id:r.asDict() for r in q.collect()}
            self.assertEqual((rows['0']['convencionales_tipo'],rows['0']['electricas_tipo']),(4,3))
            self.assertTrue(rows['0']['q_discrepancia_total'])
            self.assertIsNone(rows['1']['electricas_tipo']);self.assertTrue(rows['1']['elegible_convencionales'])
            self.assertFalse(rows['2']['elegible_convencionales']);self.assertTrue(rows['3']['elegible_convencionales'])
            for sid in ('4','5','6','8'):self.assertFalse(rows[sid]['elegible_convencionales'])
            self.assertIsNone(rows['7']['convencionales_tipo']);self.assertTrue(rows['7']['elegible_electricas'])
            self.evidence['paridad']={'muestras':len(rows),'columnas_comparadas':len(columns),'exceptAll_ambos_sentidos':0}
        finally:base.unpersist()

    def test_02_predicciones_paridad_persistencia_u1(self):
        quality=aplicar_calidad(self.quality_fixture(),self.u1).cache()
        try:
            pred=predecir(quality,self.u1)
            count=pred.count();self.assertEqual(pred.select('prediccion_id').distinct().count(),count)
            self.assertEqual(pred.where('CAST(objetivo_utc AS DOUBLE)-CAST(origen_utc AS DOUBLE) != horizonte_min*60').count(),0)
            modelado=types.ModuleType('u1_baseline_lectura')
            path=self.u1['root']/'src/modelado.py'
            exec(compile(path.read_text(),str(path),'exec'),modelado.__dict__)
            for tipo in ('convencionales','electricas'):
                eligible=quality.where('elegible_pred_'+tipo).withColumn(tipo+'_actual',F.col('objetivo_'+tipo))
                baseline=modelado.predecir(eligible,None,tipo+'_actual').select('event_id','prediction')
                check=pred.where(F.col('tipo')==tipo).join(baseline,'event_id')
                self.assertEqual(check.where('valor_predicho != prediction').count(),0)
            self.assertEqual(pred.select('prediccion_id').exceptAll(predecir(quality,self.u1).select('prediccion_id')).count(),0)
            self.evidence['predicciones']={'contratos_cargados':len(self.u1['contratos']),'predicciones_fixture':count,
                'persistencia_u1_diferencias':0,'ids_deterministas':True,'horizontes_segundos':[300,600,900]}
        finally:quality.unpersist()

    def test_03_reintento_sink_no_duplica(self):
        with TemporaryDirectory() as tmp:
            root=Path(tmp);df=self.spark.createDataFrame([('a',),('b',)],'event_id string')
            publicar_batch(df,0,root/'out',root/'stage','fingerprint-fixture')
            files={p.name:p.stat().st_size for p in (root/'out/batch-00000000000000000000').iterdir()}
            publicar_batch(df,0,root/'out',root/'stage','fingerprint-fixture')
            self.assertEqual(self.spark.read.option('recursiveFileLookup','true').parquet(str(root/'out')).count(),2)
            self.assertEqual(files,{p.name:p.stat().st_size for p in (root/'out/batch-00000000000000000000').iterdir()})
            with self.assertRaises(ValueError):publicar_batch(df,0,root/'out',root/'stage','otra-ejecucion')
            self.evidence['reintento_foreachBatch']={'filas_antes':2,'filas_despues':2,'mismo_batch_id':0}

    def test_04_duplicados_retrasos_estado_ventanas_y_reinicio(self):
        with TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'in';source.mkdir()
            schema='event_id string,_clave_dedup string,station_id string,tiempo_evento timestamp,elegible_pred_convencionales boolean,elegible_pred_electricas boolean,contrato_valido boolean,q_discrepancia_total boolean,q_discrepancia_electricas boolean,q_discrepancia_convencionales boolean,latencia_procesamiento_s double'
            reader=self.spark.readStream.schema(schema).json(str(source));progress=[]
            def record(eid,time):
                return {'event_id':eid,'_clave_dedup':eid,'station_id':'misma-estacion','tiempo_evento':'2023-01-01T'+time+'Z',
                    'elegible_pred_convencionales':True,'elegible_pred_electricas':True,'contrato_valido':True,
                    'q_discrepancia_total':False,'q_discrepancia_electricas':False,'q_discrepancia_convencionales':False,
                    'latencia_procesamiento_s':0.0}
            def drain():
                q=(deduplicar(reader,'1 minutes').writeStream.format('parquet').option('path',str(root/'out'))
                   .option('checkpointLocation',str(root/'cp')).trigger(availableNow=True).start())
                q.awaitTermination();progress.extend(q.recentProgress)
            batches=[ [record('A','00:00:10'),record('A','00:00:10'),record('B','00:00:20')],
                      [record('A','00:00:10'),record('D','00:00:30'),record('C','00:02:20')],
                      [record('demasiado-tarde','00:00:05'),record('E','00:03:30')],
                      [record('A','00:00:10'),record('F','00:04:50')] ]
            for i,rows in enumerate(batches):
                (source/f'{i}.json').write_text('\n'.join(json.dumps(x) for x in rows))
                drain()
            actual=self.spark.read.parquet(str(root/'out'))
            self.assertEqual({r.event_id for r in actual.select('event_id').collect()},{'A','B','C','D','E','F'})
            n=actual.count();drain();self.assertEqual(self.spark.read.parquet(str(root/'out')).count(),n)
            states=[op for p in progress for op in p.get('stateOperators',[])]
            dropped=sum(op.get('numRowsDroppedByWatermark',0) for op in states)
            removed=sum(op.get('numRowsRemoved',0) for op in states)
            self.assertGreaterEqual(dropped,2);self.assertGreater(removed,0)
            observed=self.spark.readStream.schema(schema).parquet(str(root/'out'))
            q=(metricas_ventana(observed,{'watermark':'1 minutes','window':'1 minutes'}).writeStream.format('parquet')
                .option('path',str(root/'windows')).option('checkpointLocation',str(root/'wcp'))
                .outputMode('append').trigger(availableNow=True).start())
            q.awaitTermination()
            windows=self.spark.read.parquet(str(root/'windows'))
            first=windows.where("ventana_inicio_utc=TIMESTAMP '2023-01-01 00:00:00'").first()
            self.assertEqual(first.observaciones,3)
            self.evidence['estado_y_ventanas']={'eventos_unicos':n,'reinicio_sin_entradas_nuevas':n,
                'tardios_descartados':dropped,'estado_eliminado':removed,
                'ultima_medicion_filas_estado':states[-1]['numRowsTotal'],'primera_ventana_observaciones':first.observaciones}


if __name__=='__main__':
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(StreamingTests))
    guardar(ROOT/'reports/streaming/pruebas_sinteticas.json',{'pruebas':result.testsRun,'fallos':len(result.failures),
        'errores':len(result.errors),'omitidas':len(result.skipped),'evidencia':StreamingTests.evidence})
    raise SystemExit(not result.wasSuccessful())
