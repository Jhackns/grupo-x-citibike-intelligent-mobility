"""Tópico propio, productor confirmado y consumidor con grupo de verificación."""
import time
from collections import Counter
from uuid import uuid4
from .utilidades import serializar, log


def cliente_config(cfg):
    return {'bootstrap_servers':cfg['bootstrap_servers'].split(','),
            'request_timeout_ms':cfg['kafka_request_timeout_ms'],
            'client_id':'citibike-disponibilidad-u2'}


def comprobar_topico(cfg, crear=False):
    from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType
    admin = KafkaAdminClient(**cliente_config(cfg))
    expected={'retention.ms':str(cfg['retention_ms']),'retention.bytes':str(cfg['retention_bytes']),
              'cleanup.policy':'delete','min.insync.replicas':'1'}
    try:
        exists=cfg['topic'] in admin.list_topics()
        if not exists:
            if not crear:raise RuntimeError('Tópico ausente: ejecutar topico --crear explícitamente')
            admin.create_topics([NewTopic(cfg['topic'],cfg['partitions'],cfg['replication_factor'],topic_configs=expected)])
        desc=admin.describe_topics([cfg['topic']])[0]
        parts=desc['partitions']
        if len(parts)!=cfg['partitions'] or any(len(p['replica_nodes'])!=cfg['replication_factor'] for p in parts):
            raise RuntimeError('Particiones/réplicas existentes incompatibles; no se modifican')
        resources=admin.describe_configs([ConfigResource(ConfigResourceType.TOPIC,cfg['topic'],
            configs={k:None for k in expected})],config_filter='all')
        # kafka-python 3.0.11 devuelve {tipo: {nombre: {clave: metadatos}}}.
        found=next(v[cfg['topic']] for v in resources.values() if cfg['topic'] in v)
        actual={k:str(found[k]['value']) for k in expected}
        if actual!=expected:
            raise RuntimeError('Configuración existente del tópico incompatible; no se modifica automáticamente')
        return {'topic':cfg['topic'],'partitions':len(parts),'replication_factor':cfg['replication_factor'],
                'config':actual,'created':not exists,'bootstrap_servers':cfg['bootstrap_servers']}
    finally:admin.close()


class Publicador:
    def __init__(self,cfg):
        from kafka import KafkaProducer
        self.cfg=cfg
        self.producer=KafkaProducer(**cliente_config(cfg),acks='all',enable_idempotence=True,
            retries=cfg['kafka_retries'],max_in_flight_requests_per_connection=1,
            retry_backoff_ms=500,delivery_timeout_ms=cfg['kafka_delivery_timeout_ms'],
            max_block_ms=cfg['kafka_max_block_ms'],allow_auto_create_topics=False,
            key_serializer=lambda x:x.encode('utf-8'),value_serializer=serializar,compression_type='gzip')

    def send(self,event):
        return self.producer.send(self.cfg['topic'],key=event['station_id'],value=event)

    def close(self):
        self.producer.close(timeout=self.cfg['confirmation_timeout_seconds'])


def publicar_captura(capture,publisher,cfg,counters,stop,checkpoint):
    """Reintenta el MISMO objeto/IDs; solo cuenta éxitos con respuesta del broker."""
    confirmed=capture['confirmed']
    for attempt in range(cfg['publish_attempts']):
        pending=[e for e in capture['events'] if e['event_id'] not in confirmed]
        if not pending or stop.is_set():break
        if attempt:
            counters['reintentos_publicacion']+=len(pending)
            if stop.wait(cfg['retry_backoff_seconds']*2**(attempt-1)):break
        futures=[]
        errors=Counter()
        deadline=time.monotonic()+cfg['confirmation_timeout_seconds']
        for event in pending:
            if stop.is_set() or time.monotonic()>=deadline:break
            try:
                futures.append((event,publisher.send(event)))
            except Exception as exc:
                counters['fallos_publicacion']+=1
                errors[type(exc).__name__]+=1
                break  # No bloquear max_block_ms por cada estación si el broker falla.
        for event,future in futures:
            try:
                metadata=future.get(timeout=max(0,deadline-time.monotonic()))
                confirmed[event['event_id']]={'partition':metadata.partition,'offset':metadata.offset}
                counters['eventos_confirmados']+=1
            except Exception as exc:
                counters['fallos_publicacion']+=1
                # Un timeout puede ser entrega ambigua. No equivale a pérdida demostrada.
                errors[type(exc).__name__]+=1
        checkpoint(capture)
        if errors:
            log('confirmaciones_pendientes',capture_id=capture['summary']['capture_id'],
                intento=attempt+1,errores_por_tipo=dict(errors),confirmados=len(confirmed))
    n=len(capture['events']);ok=len(confirmed)
    return {**capture['summary'],'confirmed_events':ok,'unconfirmed_events':n-ok,
            'complete':ok==n,'kafka_topic':cfg['topic'],'key':'station_id'}


def consumidor_verificacion(cfg,desde_final=False):
    from kafka import KafkaConsumer
    group='citibike-disponibilidad-verificacion-'+str(uuid4())
    consumer=KafkaConsumer(cfg['topic'],**cliente_config(cfg),group_id=group,enable_auto_commit=False,
        auto_offset_reset='earliest',allow_auto_create_topics=False,consumer_timeout_ms=1000,
        session_timeout_ms=6000,heartbeat_interval_ms=2000)
    deadline=time.monotonic()+30
    while not consumer.assignment():
        consumer.poll(timeout_ms=500)
        if time.monotonic()>deadline:
            consumer.close();raise RuntimeError('Sin asignación de particiones para verificación')
    if desde_final:consumer.seek_to_end()
    else:consumer.seek_to_beginning()
    # Resolver AHORA: seek_to_end es diferido; hacerlo después de publicar perdería la captura.
    for partition in consumer.assignment():consumer.position(partition)
    return consumer,group
