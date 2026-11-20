"""Proceso continuo, catálogos compartidos y outbox recuperable por captura."""
from collections import Counter
from contextlib import contextmanager
import fcntl
import json
import time
from pathlib import Path
from .contrato import construir_captura,validar_evento
from .gbfs import ClienteGBFS
from .kafka_io import Publicador, publicar_captura
from .utilidades import guardar, log,digest


@contextmanager
def bloqueo(data_dir):
    data_dir.mkdir(parents=True,exist_ok=True)
    with (data_dir/'productor.lock').open('a') as stream:
        try:fcntl.flock(stream,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:raise RuntimeError('Ya existe un productor de esta dimensión') from None
        try:yield
        finally:fcntl.flock(stream,fcntl.LOCK_UN)


class Ingesta:
    def __init__(self,cfg,stop,data_dir,publisher=None,client=None,clock=time.monotonic):
        self.cfg,self.stop,self.data_dir=cfg,stop,Path(data_dir)
        self.counters=Counter({k:0 for k in ('consultas','capturas','eventos_confirmados','fallos_http',
            'fallos_captura','fallos_publicacion','reintentos_http','reintentos_publicacion')})
        self.client=client or ClienteGBFS(cfg,stop,self.counters,self.data_dir)
        self.publisher=publisher
        self.pending=self.data_dir/'estado/pendiente.json'
        self.clock=clock;self.last_capture_clock=None

    def una_captura(self):
        self.last_capture_clock=None
        if self.pending.exists():
            capture=json.loads(self.pending.read_text())
            if capture.get('kafka_topic')!=self.cfg['topic'] or capture.get('bootstrap_servers')!=self.cfg['bootstrap_servers']:
                raise RuntimeError('Outbox pendiente pertenece a otro tópico/broker; no se redirige')
            for event in capture['events']:validar_evento(event)
            ids={e['event_id'] for e in capture['events']}
            if (len(ids)!=len(capture['events']) or len(ids)!=capture['summary']['expected_events'] or
                not set(capture['confirmed']).issubset(ids) or
                any(e['capture_id']!=capture['summary']['capture_id'] for e in capture['events']) or
                digest(sorted(e['station_id'] for e in capture['events']))!=capture['summary']['station_ids_sha256']):
                raise ValueError('Outbox inconsistente; no se publica otra identidad')
            log('recuperando_captura',capture_id=capture['summary']['capture_id'],confirmados=len(capture['confirmed']))
        else:
            # Una respuesta inválida nunca llega al productor Kafka.
            raw=self.client.capturar()
            self.last_capture_clock=self.clock()
            capture=construir_captura(raw)
            capture.update(kafka_topic=self.cfg['topic'],bootstrap_servers=self.cfg['bootstrap_servers'])
            guardar(self.pending,capture)
        if self.publisher is None:self.publisher=Publicador(self.cfg)
        summary=publicar_captura(capture,self.publisher,self.cfg,self.counters,self.stop,
            lambda value:guardar(self.pending,value))
        if summary['complete']:
            self.counters['capturas']+=1
        else:
            self.counters['fallos_captura']+=1
        summary['counters']=dict(self.counters)
        guardar(self.data_dir/'resumenes'/(summary['capture_id']+'.json'),summary)
        if summary['complete']:self.pending.unlink()
        log('captura_confirmada' if summary['complete'] else 'captura_incompleta',
            capture_id=summary['capture_id'],esperados=summary['expected_events'],
            confirmados=summary['confirmed_events'],sin_confirmar=summary['unconfirmed_events'],contadores=dict(self.counters))
        return summary

    def intervalo(self,summary):
        return max(self.cfg['poll_seconds'],summary.get('ttl',0))

    def espera(self,summary):
        # El coste inicial de descargar catálogos no acorta la separación entre estados.
        elapsed=0 if self.last_capture_clock is None else self.clock()-self.last_capture_clock
        return max(0,self.intervalo(summary)-elapsed)

    def close(self):
        if self.publisher is not None:self.publisher.close()
