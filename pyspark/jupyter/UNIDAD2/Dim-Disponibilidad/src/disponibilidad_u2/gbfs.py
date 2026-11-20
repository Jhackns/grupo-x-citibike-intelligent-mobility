"""Descubrimiento GBFS, HTTP acotado y catálogos por snapshot, no por estación."""
from copy import deepcopy
import json
import time
from urllib.request import Request, urlopen
from .configuracion import url_publica
from .utilidades import ahora, digest, epoch, fecha_utc, guardar, log

FUENTES = ('station_status', 'station_information', 'vehicle_types')


def validar_feed(value, nombre):
    if not isinstance(value, dict) or value.get('version') != '2.3':
        raise ValueError('Versión GBFS no soportada; inspeccionar antes de cambiar el contrato')
    if type(value.get('last_updated')) is not int:
        raise ValueError('GBFS 2.3 requiere last_updated Unix entero')
    fecha_utc(value['last_updated'])
    ttl = value.get('ttl')
    if type(ttl) is not int or ttl < 0:
        raise ValueError('TTL GBFS inválido')
    if not isinstance(value.get('data'), dict):
        raise ValueError('data GBFS ausente')
    if nombre != 'gbfs':
        key = 'vehicle_types' if nombre == 'vehicle_types' else 'stations'
        rows = value['data'].get(key)
        if not isinstance(rows, list) or not rows or any(not isinstance(r, dict) for r in rows):
            raise ValueError('Array GBFS ausente, vacío o inválido')
        idkey = 'vehicle_type_id' if nombre == 'vehicle_types' else 'station_id'
        ids = [r.get(idkey) for r in rows]
        if any(not isinstance(i, str) or not i.strip() for i in ids) or len(ids) != len(set(ids)):
            raise ValueError('IDs ausentes o repetidos dentro de la misma captura/catálogo')
    return value


def descubrir_indice(value):
    validar_feed(value, 'gbfs')
    data = value['data']
    locale = data.get('en')
    if not isinstance(locale, dict):
        candidates = [v for v in data.values() if isinstance(v, dict) and isinstance(v.get('feeds'), list)]
        if not candidates:
            raise ValueError('Índice GBFS sin feeds por idioma')
        locale = candidates[0]
    feeds = locale.get('feeds')
    if not isinstance(feeds, list):
        raise ValueError('Índice GBFS inválido')
    result = {}
    for name in FUENTES:
        matches = [v.get('url') for v in feeds if isinstance(v, dict) and v.get('name') == name]
        if len(matches) != 1 or not isinstance(matches[0],str):
            raise ValueError('Feed necesario ausente o ambiguo: '+name)
        result[name] = url_publica(matches[0])
    return result


def tipos_catalogo(value):
    validar_feed(value, 'vehicle_types')
    rows = value['data']['vehicle_types']
    if any(not isinstance(r.get(k),str) for r in rows for k in ('form_factor','propulsion_type')):
        raise ValueError('Catálogo sin semántica explícita de los tipos')
    return [{k:r[k] for k in ('vehicle_type_id','form_factor','propulsion_type')} for r in rows]


class ClienteGBFS:
    def __init__(self, cfg, stop, counters, data_dir, fetch=None, clock=time.monotonic):
        self.cfg,self.stop,self.counters,self.data_dir = cfg,stop,counters,data_dir
        self.fetch,self.clock = fetch or self._http,clock
        self.indice,self.urls,self.cache = None,{},{}
        self.index_expiry = 0

    def _http(self, url):
        req = Request(url, headers={'Accept':'application/json','User-Agent':'Citibike-Disponibilidad-U2/1.0 academico'})
        with urlopen(req, timeout=self.cfg['http_timeout_seconds']) as response:
            raw = response.read(32*1024*1024+1)
            if len(raw)>32*1024*1024:
                raise ValueError('Respuesta GBFS excede el límite')
            return json.loads(raw, parse_constant=lambda _: (_ for _ in ()).throw(ValueError('JSON no finito')))

    def consultar(self, url, name):
        for attempt in range(self.cfg['http_attempts']):
            if self.stop.is_set():
                raise InterruptedError('Apagado solicitado')
            self.counters['consultas'] += 1
            try:
                value = self.fetch(url)
                return validar_feed(value, name)
            except Exception as exc:
                if isinstance(exc, InterruptedError):raise
                self.counters['fallos_http'] += 1
                log('consulta_fallida', fuente=name, intento=attempt+1, error_tipo=type(exc).__name__)
                if attempt+1==self.cfg['http_attempts']:
                    raise RuntimeError('Consulta GBFS fallida: '+name) from None
                self.counters['reintentos_http'] += 1
                if self.stop.wait(self.cfg['retry_backoff_seconds']*2**attempt):
                    raise InterruptedError('Apagado durante reintento')

    def descubrir(self):
        if self.indice is None or self.clock() >= self.index_expiry:
            value = self.consultar(self.cfg['gbfs_index_url'], 'gbfs')
            urls = descubrir_indice(value)
            # Un cambio de endpoints fuerza snapshots nuevos antes de publicar.
            if urls != self.urls:self.cache.clear()
            self.indice,self.urls = value,urls
            self.index_expiry = self.clock()+max(self.cfg['index_refresh_seconds'],value['ttl'])
        return self.urls

    def catalogos(self):
        self.descubrir()
        for name, interval in (('station_information','information_refresh_seconds'),('vehicle_types','types_refresh_seconds')):
            if name not in self.cache or self.clock() >= self.cache[name]['expiry']:
                value = self.consultar(self.urls[name], name)
                if name=='vehicle_types':tipos_catalogo(value)
                fetched = ahora(); version = digest(value)
                item = {'catalog_id':version,'feed_url':self.urls[name],'gbfs_version':value['version'],
                        'captured_at_utc':fetched,'last_updated_original':value['last_updated'],
                        'last_updated_utc':fecha_utc(value['last_updated']),'ttl':value['ttl'],
                        'archivo_relativo':'catalogos/'+name+'/'+version+'.json'}
                target=self.data_dir/item['archivo_relativo']
                if target.exists():
                    if digest(json.loads(target.read_text()))!=version:
                        raise ValueError('Snapshot de catálogo alterado')
                else:guardar(target,value)
                self.cache[name] = {'ref':item,'feed':value,
                    'expiry':self.clock()+max(self.cfg[interval],value['ttl'])}
        return self.cache

    def capturar(self):
        cats = self.catalogos()
        feed = self.consultar(self.urls['station_status'], 'station_status')
        return {'captured_at_utc':ahora(),'feed':feed,'feed_url':self.urls['station_status'],
                'index_url':self.cfg['gbfs_index_url'],'catalogos':deepcopy(cats)}
