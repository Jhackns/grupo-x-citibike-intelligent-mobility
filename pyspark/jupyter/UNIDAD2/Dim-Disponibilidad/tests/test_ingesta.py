"""Muestras sintéticas: no conectan a la API, Kafka, Spark ni Supabase."""
from pathlib import Path
from tempfile import TemporaryDirectory
from collections import Counter
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import patch,MagicMock
import json
import sys
import threading
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from disponibilidad_u2.configuracion import cargar
from disponibilidad_u2.gbfs import validar_feed,descubrir_indice,tipos_catalogo,ClienteGBFS
from disponibilidad_u2.contrato import construir_captura,validar_evento
from disponibilidad_u2.kafka_io import publicar_captura,comprobar_topico,consumidor_verificacion
from disponibilidad_u2.productor import Ingesta,bloqueo
from disponibilidad_u2.utilidades import ahora,fecha_utc,digest,guardar


def muestra():
    def feed(data):return {'version':'2.3','last_updated':1700000000,'ttl':60,'data':data}
    status=feed({'stations':[
        {'station_id':'a','num_bikes_available':15,'num_ebikes_available':10,
         'vehicle_types_available':[{'vehicle_type_id':'1','count':4},{'vehicle_type_id':'2','count':3}],
         'last_reported':1699999990,'is_installed':1,'is_renting':1,'is_returning':1,'vendor_extension':{'x':7}},
        {'station_id':'b','num_bikes_available':0,'vehicle_types_available':[
            {'vehicle_type_id':'1','count':0},{'vehicle_type_id':'2','count':0}],
         'last_reported':1699999980,'is_installed':0,'is_renting':0,'is_returning':0}]})
    info=feed({'stations':[{'station_id':'a','capacity':20},{'station_id':'b','capacity':30}]})
    types=feed({'vehicle_types':[{'vehicle_type_id':'1','form_factor':'bicycle','propulsion_type':'human'},
                                {'vehicle_type_id':'2','form_factor':'bicycle','propulsion_type':'electric_assist'}]})
    urls={name:'https://example.invalid/'+name+'.json' for name in ('station_status','station_information','vehicle_types')}
    index=feed({'en':{'feeds':[{'name':k,'url':v} for k,v in urls.items()]}})
    catalogs={}
    for k,f in (('station_information',info),('vehicle_types',types)):
        catalogs[k]={'feed':f,'ref':{'catalog_id':digest(f),'gbfs_version':'2.3','feed_url':urls[k],
            'captured_at_utc':'2023-11-14T22:13:20.000Z','last_updated_original':1700000000,
            'last_updated_utc':'2023-11-14T22:13:20.000Z','ttl':60,'archivo_relativo':k+'.json'},'expiry':3600}
    raw={'feed':status,'captured_at_utc':'2023-11-14T22:13:25.000Z','index_url':'https://example.invalid/gbfs.json',
         'feed_url':urls['station_status'],'catalogos':catalogs}
    return raw,index,urls


class Future:
    def __init__(self,fail=False):self.fail=fail
    def get(self,timeout=None):
        if self.fail:raise TimeoutError('sintético')
        return SimpleNamespace(partition=0,offset=7)


class Publisher:
    def __init__(self,failures=0):self.failures=failures;self.sent=[]
    def send(self,event):
        self.sent.append(deepcopy(event));fail=len(self.sent)<=self.failures
        return Future(fail)
    def close(self):pass


class IngestaTests(unittest.TestCase):
    def setUp(self):
        self.cfg=cargar();self.cfg['retry_backoff_seconds']=0.000001
        self.raw,self.index,self.urls=muestra()

    def test_indice_descubre_urls_y_no_las_inventa(self):
        self.assertEqual(descubrir_indice(self.index),self.urls)
        self.index['data']['en']['feeds'].pop()
        with self.assertRaises(ValueError):descubrir_indice(self.index)

    def test_version_timestamp_y_arrays_invalidos(self):
        for modify in (lambda f:f.update(version='3.0'),lambda f:f.update(last_updated='2023-11-14T22:13:20Z'),
                       lambda f:f.update(ttl=-1),lambda f:f['data'].update(stations=None)):
            f=deepcopy(self.raw['feed']);modify(f)
            with self.assertRaises(ValueError):validar_feed(f,'station_status')

    def test_ids_duplicados_o_ausentes_rechazan_captura_completa(self):
        self.raw['feed']['data']['stations'][1]['station_id']='a'
        with self.assertRaises(ValueError):construir_captura(self.raw)
        del self.raw['feed']['data']['stations'][1]['station_id']
        with self.assertRaises(ValueError):construir_captura(self.raw)

    def test_catalogo_real_explicito_y_cambio_no_silencioso(self):
        c=construir_captura(self.raw)
        self.assertTrue(c['summary']['u1_catalog_compatible'])
        self.raw['catalogos']['vehicle_types']['feed']['data']['vehicle_types'][0]['propulsion_type']='electric_assist'
        c=construir_captura(self.raw)
        self.assertFalse(c['summary']['u1_catalog_compatible'])
        self.assertIn('catalogo_no_equivalente_a_u1',c['events'][0]['ingestion_issues'])

    def test_catalogo_ambiguo_rechazado(self):
        feed=self.raw['catalogos']['vehicle_types']['feed']
        feed['data']['vehicle_types'].append(deepcopy(feed['data']['vehicle_types'][0]))
        with self.assertRaises(ValueError):tipos_catalogo(feed)

    def test_originales_discrepancias_nulos_y_ceros_se_preservan(self):
        old=deepcopy(self.raw)
        c=construir_captura(self.raw)
        self.assertEqual(self.raw,old)
        for source,event in zip(self.raw['feed']['data']['stations'],c['events']):self.assertEqual(source,event['station'])
        self.assertEqual(c['events'][0]['station']['vehicle_types_available'][0]['count'],4)
        self.assertNotIn('num_ebikes_available',c['events'][1]['station'])
        self.assertEqual(c['events'][1]['station']['num_bikes_available'],0)

    def test_tres_relojes_y_utc(self):
        e=construir_captura(self.raw)['events'][0]
        self.assertEqual(e['captured_at_utc'],'2023-11-14T22:13:25.000Z')
        self.assertEqual(e['last_updated_utc'],'2023-11-14T22:13:20.000Z')
        self.assertEqual(e['last_reported_utc'],'2023-11-14T22:13:10.000Z')
        self.assertEqual(e['last_reported_original'],1699999990)
        self.assertEqual(fecha_utc('2023-11-14T17:13:20-05:00'),'2023-11-14T22:13:20.000Z')
        with self.assertRaises(ValueError):fecha_utc('2023-11-14T22:13:20')

    def test_campos_de_estado_ausentes_quedan_marcados_no_en_cero(self):
        s=self.raw['feed']['data']['stations'][0]
        del s['last_reported'];del s['vehicle_types_available']
        e=construir_captura(self.raw)['events'][0]
        self.assertIsNone(e['last_reported_utc'])
        self.assertNotIn('last_reported',e['station'])
        self.assertNotIn('vehicle_types_available',e['station'])
        self.assertIn('campo_ausente:vehicle_types_available',e['ingestion_issues'])
        validar_evento(e)

    def test_contrato_tamper_y_campos_requeridos(self):
        for k in ('event_id','capture_id','station_id','source','catalogs'):
            e=construir_captura(self.raw)['events'][0];del e[k]
            with self.assertRaises(ValueError):validar_evento(e)
        e=construir_captura(self.raw)['events'][0];e['event_id']='incorrecto'
        with self.assertRaises(ValueError):validar_evento(e)

    def test_capturas_iguales_en_momentos_distintos_no_se_deduplican(self):
        a=construir_captura(self.raw)
        self.raw['captured_at_utc']='2023-11-14T22:18:25.000Z'
        b=construir_captura(self.raw)
        self.assertEqual(len(a['events']),len(b['events']))
        self.assertNotEqual(a['summary']['capture_id'],b['summary']['capture_id'])
        self.assertEqual(a['events'][0]['station'],b['events'][0]['station'])
        self.assertNotEqual(a['events'][0]['event_id'],b['events'][0]['event_id'])

    def test_reintentos_reutilizan_identidad_y_solo_cuentan_confirmados(self):
        c=construir_captura(self.raw);publisher=Publisher(failures=1);counts=Counter()
        result=publicar_captura(c,publisher,self.cfg,counts,threading.Event(),lambda _:None)
        self.assertTrue(result['complete']);self.assertEqual(result['confirmed_events'],2)
        self.assertEqual(counts['eventos_confirmados'],2)
        self.assertEqual(counts['reintentos_publicacion'],1)
        self.assertEqual(publisher.sent[0],publisher.sent[2])

    def test_outbox_recupera_captura_sin_consultar_otra_vez(self):
        cfg={**self.cfg,'publish_attempts':1}
        fake_client=SimpleNamespace(capturar=lambda:deepcopy(self.raw))
        with TemporaryDirectory() as tmp:
            first=Ingesta(cfg,threading.Event(),Path(tmp),publisher=Publisher(failures=2),client=fake_client)
            result=first.una_captura();self.assertFalse(result['complete']);self.assertTrue(first.pending.exists())
            saved=json.loads(first.pending.read_text())
            def never():raise AssertionError('No consultar otra captura durante recuperación')
            pub=Publisher();second=Ingesta(cfg,threading.Event(),Path(tmp),publisher=pub,client=SimpleNamespace(capturar=never))
            recovered=second.una_captura()
            self.assertTrue(recovered['complete']);self.assertEqual(recovered['capture_id'],result['capture_id'])
            self.assertEqual([e['event_id'] for e in saved['events']],[e['event_id'] for e in pub.sent])
            self.assertFalse(second.pending.exists())

    def test_captura_invalida_nunca_se_publica(self):
        def fail():raise ValueError('sintético')
        with TemporaryDirectory() as tmp:
            pub=Publisher();engine=Ingesta(self.cfg,threading.Event(),Path(tmp),publisher=pub,client=SimpleNamespace(capturar=fail))
            with self.assertRaises(ValueError):engine.una_captura()
            self.assertFalse(pub.sent);self.assertFalse(engine.pending.exists())

    def test_cache_catalogos_no_descarga_por_estacion(self):
        clock=[0];queries=Counter()
        feeds={self.raw['index_url']:self.index,self.urls['station_status']:self.raw['feed'],
               **{self.urls[k]:v['feed'] for k,v in self.raw['catalogos'].items()}}
        def fetch(url):queries[url]+=1;return deepcopy(feeds[url])
        with TemporaryDirectory() as tmp:
            cfg={**self.cfg,'gbfs_index_url':self.raw['index_url']}
            client=ClienteGBFS(cfg,threading.Event(),Counter(),Path(tmp),fetch=fetch,clock=lambda:clock[0])
            client.capturar();client.capturar()
            self.assertEqual(queries[self.urls['station_status']],2)
            self.assertEqual(queries[self.urls['vehicle_types']],1)
            self.assertEqual(queries[self.urls['station_information']],1)
            clock[0]=3601;client.capturar()
            self.assertEqual(queries[self.urls['station_information']],2)
            self.assertEqual(queries[self.urls['vehicle_types']],1)

    def test_http_reintentos_limitados(self):
        attempts=[];counts=Counter()
        def fail(_):attempts.append(1);raise ValueError('JSON inválido sintético')
        with TemporaryDirectory() as tmp:
            c=ClienteGBFS(self.cfg,threading.Event(),counts,Path(tmp),fetch=fail)
            with self.assertRaises(RuntimeError):c.consultar(self.raw['index_url'],'gbfs')
        self.assertEqual(len(attempts),3);self.assertEqual(counts['reintentos_http'],2)

    def test_bloqueo_evitar_productores_simultaneos(self):
        with TemporaryDirectory() as tmp, bloqueo(Path(tmp)):
            with self.assertRaises(RuntimeError):
                with bloqueo(Path(tmp)):pass

    def test_frecuencia_respeta_ttl_y_configuracion(self):
        with TemporaryDirectory() as tmp:
            e=Ingesta(self.cfg,threading.Event(),Path(tmp))
            self.assertEqual(e.intervalo({'ttl':60}),300)
            self.assertEqual(e.intervalo({'ttl':600}),600)

    def test_espera_se_mide_desde_captura_no_desde_inicio_de_catalogos(self):
        clock=[100]
        cfg={**self.cfg,'poll_seconds':60}
        with TemporaryDirectory() as tmp:
            e=Ingesta(cfg,threading.Event(),Path(tmp),clock=lambda:clock[0])
            e.last_capture_clock=100  # Los catálogos pudieron haber empezado mucho antes.
            clock[0]=110
            self.assertEqual(e.espera({'ttl':60}),50)
            clock[0]=165
            self.assertEqual(e.espera({'ttl':60}),0)
            e.last_capture_clock=None  # Recuperación de una captura vieja desde outbox.
            self.assertEqual(e.espera({'ttl':60}),60)

    def test_configuracion_rechaza_topic_ajeno_y_url_con_credenciales(self):
        with self.assertRaises(ValueError):cargar(topic='citibike-viajes-eventos')
        with self.assertRaises(ValueError):cargar(gbfs_index_url='https://user:secret@example.invalid/gbfs.json')
        with self.assertRaises(ValueError):cargar(gbfs_index_url='https://example.invalid/gbfs.json?token=x')

    def test_topico_existente_se_inspecciona_sin_alterarlo(self):
        admin=MagicMock()
        admin.list_topics.return_value={self.cfg['topic']}
        admin.describe_topics.return_value=[{'partitions':[{'replica_nodes':[1]} for _ in range(3)]}]
        values={'retention.ms':'86400000','retention.bytes':'1073741824','cleanup.policy':'delete','min.insync.replicas':'1'}
        admin.describe_configs.return_value={'topic':{self.cfg['topic']:{k:{'value':v} for k,v in values.items()}}}
        with patch('kafka.admin.KafkaAdminClient',return_value=admin):
            result=comprobar_topico(self.cfg,crear=True)
            self.assertFalse(result['created']);admin.create_topics.assert_not_called();admin.alter_configs.assert_not_called()
            admin.describe_topics.return_value=[{'partitions':[{'replica_nodes':[1]}]}]
            with self.assertRaises(RuntimeError):comprobar_topico(self.cfg)
            admin.alter_configs.assert_not_called()

    def test_consumidor_resuelve_offsets_antes_de_publicar(self):
        consumer=MagicMock();consumer.assignment.return_value={'partition-0'}
        with patch('kafka.KafkaConsumer',return_value=consumer) as factory:
            _,group=consumidor_verificacion(self.cfg,desde_final=True)
            consumer.seek_to_end.assert_called_once();consumer.position.assert_called_once_with('partition-0')
            self.assertLess(consumer.method_calls.index(unittest.mock.call.seek_to_end()),
                            consumer.method_calls.index(unittest.mock.call.position('partition-0')))
            self.assertTrue(group.startswith('citibike-disponibilidad-verificacion-'))
            self.assertFalse(factory.call_args.kwargs['enable_auto_commit'])


if __name__=='__main__':unittest.main()
