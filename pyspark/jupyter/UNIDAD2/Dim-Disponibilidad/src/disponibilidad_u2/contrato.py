"""Identidad de captura y evento; preservación del estado GBFS original."""
from copy import deepcopy
from uuid import UUID, uuid4, uuid5
from .gbfs import tipos_catalogo, validar_feed
from .utilidades import digest, fecha_utc

SCHEMA_VERSION = 'citibike.disponibilidad.v1'
REQUIRED_STATE = ('num_bikes_available','vehicle_types_available','is_installed','is_renting','is_returning','last_reported')


def id_evento(capture_id, station_id):
    return str(uuid5(UUID(capture_id), station_id))


def reporte_utc(station):
    value = station.get('last_reported')
    if type(value) is not int:return None
    try:
        return fecha_utc(value)
    except (ValueError,TypeError,OverflowError,OSError):
        return None


def construir_captura(raw, capture_id=None):
    feed = validar_feed(raw['feed'], 'station_status')
    captured = fecha_utc(raw['captured_at_utc'])
    capture_id = capture_id or str(uuid4())
    UUID(capture_id)
    cats = raw['catalogos']
    types = tipos_catalogo(cats['vehicle_types']['feed'])
    mapping = {v['vehicle_type_id']:(v['form_factor'],v['propulsion_type']) for v in types}
    compatible = mapping.get('1')==('bicycle','human') and mapping.get('2')==('bicycle','electric_assist')
    information = validar_feed(cats['station_information']['feed'], 'station_information')
    info_ids = {r['station_id'] for r in information['data']['stations']}
    references = {k:deepcopy(cats[k]['ref']) for k in ('station_information','vehicle_types')}
    events = []
    for state in feed['data']['stations']:
        sid = state['station_id']
        issues = ['campo_ausente:'+k for k in REQUIRED_STATE if k not in state or state[k] is None]
        if reporte_utc(state) is None:issues.append('last_reported_no_utilizable')
        if sid not in info_ids:issues.append('estacion_no_presente_en_catalogo')
        counts = state.get('vehicle_types_available')
        if not isinstance(counts, list):
            issues.append('array_tipos_no_utilizable')
        else:
            seen = set()
            for v in counts:
                if not isinstance(v, dict):
                    issues.append('elemento_tipo_invalido');continue
                vid = v.get('vehicle_type_id')
                if not isinstance(vid, str) or vid not in mapping:issues.append('tipo_sin_catalogo')
                if isinstance(vid,str):
                    if vid in seen:issues.append('tipo_repetido')
                    seen.add(vid)
                if type(v.get('count')) is not int or v['count']<0:issues.append('conteo_tipo_no_utilizable')
        if not compatible:issues.append('catalogo_no_equivalente_a_u1')
        event = {
            'schema_version':SCHEMA_VERSION,'capture_id':capture_id,'event_id':id_evento(capture_id,sid),
            'station_id':sid,'captured_at_utc':captured,
            'source':{'system':'citibike','gbfs_index_url':raw['index_url'],
                      'feed_url':raw['feed_url'],'gbfs_version':feed['version']},
            'last_updated_original':feed['last_updated'],'last_updated_utc':fecha_utc(feed['last_updated']),
            'last_reported_original':state.get('last_reported'),'last_reported_utc':reporte_utc(state),
            'station':deepcopy(state),'catalogs':deepcopy(references),
            'vehicle_types_catalog':deepcopy(types),'u1_catalog_compatible':compatible,
            'ingestion_issues':sorted(set(issues))}
        validar_evento(event)
        events.append(event)
    summary = {'capture_id':capture_id,'captured_at_utc':captured,'last_updated_original':feed['last_updated'],
               'last_updated_utc':fecha_utc(feed['last_updated']),'ttl':feed['ttl'],
               'source':events[0]['source'],'expected_events':len(events),
               'station_ids_sha256':digest(sorted(e['station_id'] for e in events)),
               'catalogs':references,'vehicle_types_catalog':types,'u1_catalog_compatible':compatible,
               'events_with_ingestion_issues':sum(bool(e['ingestion_issues']) for e in events)}
    return {'summary':summary,'events':events,'confirmed':{}}


def validar_evento(event):
    required = ('schema_version','event_id','capture_id','station_id','source','captured_at_utc',
                'last_updated_original','last_updated_utc','last_reported_original','last_reported_utc',
                'station','catalogs','vehicle_types_catalog','u1_catalog_compatible','ingestion_issues')
    if not isinstance(event, dict) or set(event)!=set(required):
        raise ValueError('Evento incompleto')
    if event['schema_version']!=SCHEMA_VERSION or not isinstance(event['station_id'],str) or not event['station_id'].strip():
        raise ValueError('Versión o identidad de estación inválida')
    if id_evento(event['capture_id'],event['station_id'])!=event['event_id']:
        raise ValueError('Identidad de evento inconsistente')
    state = event['station']
    if not isinstance(state,dict) or state.get('station_id')!=event['station_id']:
        raise ValueError('Estado original no corresponde a la estación')
    if fecha_utc(event['captured_at_utc'])!=event['captured_at_utc'] or not event['captured_at_utc'].endswith('Z'):
        raise ValueError('La captura debe conservarse en UTC')
    if fecha_utc(event['last_updated_original'])!=event['last_updated_utc']:
        raise ValueError('Reloj de fuente inconsistente')
    if state.get('last_reported')!=event['last_reported_original'] or reporte_utc(state)!=event['last_reported_utc']:
        raise ValueError('Reloj original de estación inconsistente')
    source=event['source']
    if not isinstance(source,dict) or source.get('gbfs_version')!='2.3' or not all(
        isinstance(source.get(k),str) and source[k] for k in ('system','feed_url','gbfs_index_url')):
        raise ValueError('Fuente no identificada')
    for name in ('vehicle_types','station_information'):
        ref=event['catalogs'].get(name)
        if not isinstance(ref,dict) or not isinstance(ref.get('catalog_id'),str) or len(ref['catalog_id'])!=64:
            raise ValueError('Referencia de catálogo inválida')
        fecha_utc(ref['captured_at_utc']);fecha_utc(ref['last_updated_utc'])
    if not isinstance(event['vehicle_types_catalog'],list) or type(event['u1_catalog_compatible']) is not bool:
        raise ValueError('Semántica de catálogo ausente')
    if not isinstance(event['ingestion_issues'],list) or any(not isinstance(x,str) for x in event['ingestion_issues']):
        raise ValueError('Diagnóstico de ingesta inválido')
    return event
