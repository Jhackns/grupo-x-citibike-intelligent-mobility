"""Fixtures en flujos SQL separados, reloj simulado explícito; no son API real."""
import argparse
import importlib.util
import json
from pathlib import Path
import sys
from datetime import datetime,timedelta,timezone
import unittest
from uuid import uuid4

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'tests')]
from disponibilidad_u2.postgres import Base,cargar_sql,escribir_json,SinkSQL
from disponibilidad_u2.utilidades import guardar
try:HAS_SPARK=importlib.util.find_spec('pyspark.sql') is not None
except ModuleNotFoundError:HAS_SPARK=False
T0=datetime(2020,1,1,tzinfo=timezone.utc)


def fecha(seconds):return (T0+timedelta(seconds=seconds)).isoformat()


@unittest.skipUnless(HAS_SPARK,'Ejecutar --sql --pruebas-sql en Docker')
class SQLTests(unittest.TestCase):
    evidence={}

    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark=(SparkSession.builder.appName('Pruebas-SQL-Disponibilidad')
          .config('spark.sql.session.timeZone','UTC').config('spark.ui.showConsoleProgress','false')
          .config('spark.sql.shuffle.partitions','2').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):cls.spark.stop()

    def setUp(self):
        self.cfg=cargar_sql();self.cfg['flow_id']='fixture-'+str(uuid4())
        self.db=Base(self.spark,self.cfg)
        self.db.registrar({'tolerance_seconds':30,'gap_seconds':450,'late_seconds':600,'tipo':'fixture_sintetico'})
        self.batch=0

    def tearDown(self):self.db.close()

    def obs(self,id,second,qty=4,eligible=True,station='S',disc=False):
        return {'_clave_dedup':id,'event_id':id,'capture_id':'captura-'+id,'station_id':station,
          'captura_utc':fecha(second),'last_updated_utc_original':fecha(second),
          'last_reported_utc_original':fecha(second),'contrato_valido':True,
          'objetivo_convencionales':qty,'objetivo_electricas':3,
          'elegible_pred_convencionales':eligible,'elegible_pred_electricas':True,
          'regla_version':'silver-v2-tipos-gbfs','catalogo_modelo_compatible':True,
          'q_discrepancia_total':disc,'q_discrepancia_convencionales':False,'q_discrepancia_electricas':False}

    def pred(self,id,h=5):
        return {'prediccion_id':'pred-'+id+'-'+str(h),'event_id':id,'station_id':'S','origen_utc':fecha(0),
          'objetivo_utc':fecha(h*60),'tipo':'convencionales','horizonte_min':h,'valor_predicho':4.0,
          'version_predictor':'fixture','contrato_sha256':'fixture','ejecucion_u1':'fixture_sintetico'}

    def insert(self,observations,predictions=()):
        f=self.cfg['flow_id'];b=self.batch;self.batch+=1
        escribir_json(self.db,'observaciones',b,[json.dumps(x) for x in observations])
        if predictions:escribir_json(self.db,'predicciones',b,[json.dumps(x) for x in predictions])
        self.db.execute('INSERT INTO disponibilidad.lotes(flujo,batch_id,observaciones,rechazos,predicciones) VALUES(?,?::bigint,?::int,0,?::int)',
          (f,b,len(observations),len(predictions)))
        self.db.execute('''INSERT INTO disponibilidad.evaluaciones(flujo,prediccion_id,cierre_no_antes_utc)
           SELECT flujo,prediccion_id,objetivo_utc+interval '630 seconds' FROM disponibilidad.predicciones
           WHERE flujo=? AND batch_id=?::bigint ON CONFLICT DO NOTHING''',(f,b))
        self.db.conn.commit();self.db.confirmar_lote(b)

    def evaluation(self):
        return self.db.json('SELECT row_to_json(e)::text FROM disponibilidad.evaluaciones e WHERE flujo=?',(self.cfg['flow_id'],))

    def frontier(self,second):return self.obs('frontier-'+str(second),second,station='OTRA')

    def test_01_limite_inclusivo_y_desempate_anterior(self):
        self.insert([self.obs('a',0),self.obs('anterior',270,7),self.obs('posterior',330,9),self.frontier(930)],[self.pred('a')])
        self.assertEqual(self.db.evaluar(fecha(929.999)),0)
        self.assertEqual(self.db.evaluar(fecha(930)),1)
        e=self.evaluation();self.assertEqual(e['estado'],'evaluada');self.assertEqual(e['observacion_objetivo_id'],'anterior')
        self.assertEqual(e['horizonte_efectivo_s'],270);self.assertEqual(e['error_emparejamiento_s'],-30)
        self.assertEqual(e['error'],-3);self.assertTrue(e['cambio'])
        metrics=self.db.json('''SELECT row_to_json(m)::text FROM disponibilidad.metricas m
            WHERE flujo=? AND periodo='todos' AND discrepancias='todos' ''',(self.cfg['flow_id'],))
        self.assertEqual((metrics['mae'],metrics['rmse'],metrics['sesgo'],metrics['n']),(3,3,-3,1))
        self.evidence['banda_y_empate']={'cierre_anterior_sin_evaluar':True,'extremo_inclusivo_s':-30,'desempate':'anterior','mae':3,'rmse':3,'sesgo':-3}

    def test_02_no_saltar_objetivo_invalido(self):
        self.insert([self.obs('a',0),self.obs('invalido',300,eligible=False),self.obs('alternativa',305),self.frontier(1000)],[self.pred('a')])
        self.db.evaluar(fecha(1000));e=self.evaluation()
        self.assertEqual((e['estado'],e['motivo'],e['observacion_objetivo_id']),('vencida','objetivo_no_elegible','invalido'))
        self.assertIsNone(e['error'])

    def test_03_sin_objetivo_y_sin_avance(self):
        self.insert([self.obs('a',0)],[self.pred('a')])
        self.assertEqual(self.db.evaluar(fecha(2000)),0)
        self.assertEqual(self.evaluation()['estado'],'pendiente')
        self.insert([self.frontier(1000)])
        self.db.evaluar(fecha(2000));e=self.evaluation()
        self.assertEqual((e['estado'],e['motivo']),('vencida','sin_captura_objetivo_en_tolerancia'))
        self.assertIsNone(e['valor_observado']);self.assertIsNone(e['error'])

    def test_04_tardio_antes_de_cerrar_y_cierre_inmutable(self):
        self.insert([self.obs('a',0),self.obs('lejano',325),self.frontier(929)],[self.pred('a')])
        self.assertEqual(self.db.evaluar(fecha(929)),0)
        self.insert([self.obs('tardio',300,6,disc=True),self.frontier(930)])
        self.db.evaluar(fecha(930));e=self.evaluation()
        self.assertEqual((e['observacion_objetivo_id'],e['estrato_discrepancia']),('tardio','con_discrepancias'))
        self.assertEqual(self.db.evaluar(fecha(2000)),0);self.assertEqual(self.evaluation(),e)
        self.evidence['tardios']={'objetivo_recibido_antes_cierre':'seleccionado','revaluacion_cerrada':False,'sin_avance':'pendiente'}

    def test_05_horizonte_15_y_huecos(self):
        self.insert([self.obs('a',0),self.obs('futuro',900),self.frontier(1530)],[self.pred('a',15)])
        self.db.evaluar(fecha(1530));e=self.evaluation()
        self.assertEqual((e['estado'],e['motivo']),('vencida','hueco_o_inactividad_en_trayecto'))

    def test_06_identidad_y_rollback(self):
        o=self.obs('a',0);self.insert([o],[self.pred('a')])
        escribir_json(self.db,'observaciones',0,[json.dumps(o)]);self.db.conn.commit()
        with self.assertRaises(ValueError):escribir_json(self.db,'observaciones',0,[json.dumps({**o,'objetivo_convencionales':999})])
        self.db.conn.rollback()
        e=self.db.json('SELECT to_json(convencionales)::text FROM disponibilidad.observaciones WHERE flujo=?',(self.cfg['flow_id'],))
        self.assertEqual(e,4)
        escribir_json(self.db,'observaciones',1,[json.dumps(self.obs('incompleto',1))]);self.db.conn.rollback()
        n=self.db.json('SELECT to_json(count(*))::text FROM disponibilidad.observaciones WHERE flujo=?',(self.cfg['flow_id'],))
        self.assertEqual(n,1)
        self.evidence['transacciones']={'colision_contenido':'rechazada','rollback_sin_filas_parciales':True,'reintento_misma_identidad_no_duplica':True}

    def test_07_sink_spark_reintento_mismo_batch(self):
        from test_streaming import StreamingTests
        from disponibilidad_u2.config_streaming import cargar_streaming
        from disponibilidad_u2.predictor_streaming import cargar_u1
        from disponibilidad_u2.calidad_streaming import aplicar_calidad
        cfg=cargar_streaming();u1=cargar_u1(cfg['u1_reference'])
        fixture=StreamingTests();fixture.spark=self.spark;fixture.cfg=cfg;fixture.u1=u1
        q=aplicar_calidad(fixture.quality_fixture(),u1).cache();q.count()
        try:
            sink=SinkSQL(self.cfg,u1,{})
            sink(q,0)
            before=self.db.json('SELECT row_to_json(t)::text FROM disponibilidad.lotes t WHERE flujo=?',(self.cfg['flow_id'],));self.db.conn.commit()
            sink(q,0)
            after=self.db.json('SELECT row_to_json(t)::text FROM disponibilidad.lotes t WHERE flujo=?',(self.cfg['flow_id'],))
            self.assertEqual(before,after)
            n=self.db.json('SELECT to_json(count(*))::text FROM disponibilidad.predicciones WHERE flujo=?',(self.cfg['flow_id'],))
            self.assertEqual(n,18)
            self.evidence['sink_real']={'observaciones':9,'predicciones':n,'repetir_batch_id_sin_cambios':True}
        finally:q.unpersist()

    def test_08_extremo_positivo_y_continuidad_a_15_minutos(self):
        self.insert([self.obs('a',0),self.obs('intermedia1',300),self.obs('intermedia2',600),
                     self.obs('objetivo',930,7),self.frontier(1530)],[self.pred('a',15)])
        self.assertEqual(self.db.evaluar(fecha(1529.999)),0)
        self.db.evaluar(fecha(1530));e=self.evaluation()
        self.assertEqual((e['estado'],e['horizonte_efectivo_s'],e['error_emparejamiento_s']),('evaluada',930,30))
        self.assertEqual(e['valor_observado'],7)

    def test_09_inactividad_intermedia_rompe_trayecto(self):
        self.insert([self.obs('a',0),self.obs('intermedia',300,eligible=False),
                     self.obs('objetivo',600),self.frontier(1230)],[self.pred('a',10)])
        self.db.evaluar(fecha(1230));e=self.evaluation()
        self.assertEqual((e['estado'],e['motivo']),('vencida','hueco_o_inactividad_en_trayecto'))

    def test_10_rechazo_reloj_invalido_se_preserva_sin_cero(self):
        o=self.obs('malformado',0,qty=None,eligible=False)
        o.update(contrato_valido=False,captura_utc='invalida',last_updated_utc_original='invalida',
                 last_reported_utc_original='invalida',elegible_pred_electricas=False)
        self.insert([o]);r=self.db.json('SELECT row_to_json(o)::text FROM disponibilidad.observaciones o WHERE flujo=?',(self.cfg['flow_id'],))
        self.assertIsNone(r['convencionales']);self.assertIsNone(r['captura_utc'])
        self.assertEqual(r['payload']['captura_utc'],'invalida')

    def test_11_fuera_de_tolerancia_no_es_objetivo(self):
        self.insert([self.obs('a',0),self.obs('fuera',330.001),self.frontier(1000)],[self.pred('a')])
        self.db.evaluar(fecha(1000));e=self.evaluation()
        self.assertEqual((e['estado'],e['motivo']),('vencida','sin_captura_objetivo_en_tolerancia'))
        self.assertIsNone(e['valor_observado'])

    def test_12_sobre_invalido_no_oculta_captura_mas_cercana(self):
        bad=self.obs('invalido',300,eligible=False);bad['contrato_valido']=False
        self.insert([self.obs('a',0),bad,self.obs('alternativa',301),self.frontier(1000)],[self.pred('a')])
        self.db.evaluar(fecha(1000));e=self.evaluation()
        self.assertEqual((e['estado'],e['motivo'],e['observacion_objetivo_id']),('vencida','objetivo_no_elegible','invalido'))

    def test_13_evaluador_aplaza_si_ingesta_ocupada(self):
        self.insert([self.obs('a',0),self.obs('futuro',300),self.frontier(930)],[self.pred('a')])
        writer=Base(self.spark,self.cfg)
        try:
            writer.execute('SELECT pg_advisory_xact_lock(hashtextextended(?,0))',(self.cfg['flow_id'],))
            self.assertEqual(self.db.evaluar(fecha(930)),0)
            self.assertEqual(self.evaluation()['estado'],'pendiente')
        finally:writer.conn.rollback();writer.close()
        self.assertEqual(self.db.evaluar(fecha(930)),1)
        self.evidence['concurrencia']={'carga_bloqueada':'evaluacion_aplazada_sin_error','liberar_carga':'evaluacion_completada'}


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--config');p.add_argument('--config-sql');p.parse_args()
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(SQLTests))
    guardar(ROOT/'reports/postgres/pruebas_sinteticas.json',{'pruebas':result.testsRun,'errores':len(result.errors),
       'fallos':len(result.failures),'omitidas':len(result.skipped),'evidencia':SQLTests.evidence,
       'nota':'SQL real sobre fixtures y reloj simulado; flow_id fixture-* independientes del flujo API.'})
    raise SystemExit(not result.wasSuccessful())
