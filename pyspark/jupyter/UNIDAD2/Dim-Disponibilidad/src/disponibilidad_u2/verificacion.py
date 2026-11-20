"""Consumo acotado con grupo propio; valida exactamente una capture_id."""
import json
import time
from .contrato import validar_evento
from .utilidades import digest, epoch


def verificar_captura(consumer,summary,timeout=60):
    expected=summary['expected_events'];capture_id=summary['capture_id']
    seen={};duplicates=0;deadline=time.monotonic()+timeout
    while time.monotonic()<deadline and len(seen)<expected:
        batches=consumer.poll(timeout_ms=500,max_records=10000)
        for records in batches.values():
            for record in records:
                event=json.loads(record.value)
                if event.get('capture_id')!=capture_id:continue
                validar_evento(event)
                if record.key!=event['station_id'].encode('utf-8'):
                    raise AssertionError('Clave Kafka no corresponde a station_id')
                if event['captured_at_utc']!=summary['captured_at_utc'] or event['last_updated_utc']!=summary['last_updated_utc']:
                    raise AssertionError('Relojes de captura inconsistentes')
                if event['catalogs']!=summary['catalogs']:
                    raise AssertionError('Catálogos no corresponden a la captura')
                eid=event['event_id']
                if eid in seen:
                    duplicates+=1
                    if digest(seen[eid]['event'])!=digest(event):
                        raise AssertionError('Mismo event_id con contenido distinto')
                else:seen[eid]={'event':event,'partition':record.partition,'offset':record.offset}
    station_ids=[v['event']['station_id'] for v in seen.values()]
    if len(seen)!=expected or len(set(station_ids))!=expected or digest(sorted(station_ids))!=summary['station_ids_sha256']:
        raise AssertionError('No se reconcilió la captura consumida con las estaciones de origen')
    report={'capture_id':capture_id,'expected_events':expected,'unique_events_consumed':len(seen),
            'duplicate_records_observed':duplicates,'contract_valid':True,'keys_valid':True,
            'station_ids_sha256':digest(sorted(station_ids)),
            'partitions_observed':sorted({v['partition'] for v in seen.values()}),
            'sample_event':next(iter(seen.values()))['event']}
    return report,seen


def continuidad(first,second):
    a={v['event']['station_id']:v for v in first.values()}
    b={v['event']['station_id']:v for v in second.values()}
    common=set(a)&set(b)
    if not common:raise AssertionError('No hay estaciones comunes para comprobar continuidad')
    unchanged=0
    for sid in common:
        x,y=a[sid],b[sid]
        if x['event']['capture_id']==y['event']['capture_id'] or x['event']['event_id']==y['event']['event_id']:
            raise AssertionError('Capturas distintas perdieron identidad')
        if x['partition']!=y['partition']:
            raise AssertionError('La estación cambió de partición sin cambiar el tópico')
        if x['event']['station'].get('vehicle_types_available')==y['event']['station'].get('vehicle_types_available'):
            unchanged+=1
    e1=next(iter(a.values()))['event'];e2=next(iter(b.values()))['event']
    return {'common_stations':len(common),'same_type_counts_preserved':unchanged,
            'different_ids_between_captures':True,'station_partition_stable':True,
            'capture_interval_seconds':epoch(e2['captured_at_utc'])-epoch(e1['captured_at_utc']),
            'feed_last_updated_delta_seconds':epoch(e2['last_updated_utc'])-epoch(e1['last_updated_utc'])}
