"""Configuración pública. No lee Supabase ni requiere credenciales de U1."""
from pathlib import Path
import json
import os
import re
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[2]


def url_publica(url):
    p = urlsplit(url)
    if p.scheme != 'https' or not p.netloc or p.username or p.password or p.query or p.fragment:
        raise ValueError('GBFS requiere una URL HTTPS pública sin credenciales ni query')
    return url


def cargar(path=None, **overrides):
    defaults = json.loads((ROOT/'config/local.json').read_text())
    extra = json.loads(Path(path).read_text()) if path else {}
    if not isinstance(extra, dict) or set(extra)-set(defaults):
        raise ValueError('Claves desconocidas en configuración pública')
    cfg = defaults | extra
    for env, key in (('DISP_GBFS_INDEX_URL', 'gbfs_index_url'),
                     ('DISP_KAFKA_BOOTSTRAP_SERVERS', 'bootstrap_servers'), ('DISP_KAFKA_TOPIC', 'topic')):
        if env in os.environ:
            cfg[key] = os.environ[env]
    cfg.update({k:v for k,v in overrides.items() if v is not None})
    url_publica(cfg['gbfs_index_url'])
    if not re.fullmatch(r'citibike-disponibilidad[-._a-zA-Z0-9]*', cfg['topic']):
        raise ValueError('El tópico debe pertenecer al espacio citibike-disponibilidad')
    if not all(re.fullmatch(r'[a-zA-Z0-9_.-]+:[0-9]+', s) for s in cfg['bootstrap_servers'].split(',')):
        raise ValueError('Bootstrap debe ser host:puerto, sin credenciales')
    for k in ('partitions', 'replication_factor', 'http_attempts', 'publish_attempts', 'kafka_retries',
              'retention_ms','retention_bytes','kafka_request_timeout_ms','kafka_delivery_timeout_ms','kafka_max_block_ms'):
        if type(cfg[k]) is not int or cfg[k] <= 0:
            raise ValueError('Entero positivo requerido: '+k)
    for k in ('poll_seconds','information_refresh_seconds','types_refresh_seconds','index_refresh_seconds',
              'http_timeout_seconds','retry_backoff_seconds','confirmation_timeout_seconds'):
        if isinstance(cfg[k], bool) or not isinstance(cfg[k], (float,int)) or cfg[k] <= 0:
            raise ValueError('Valor positivo requerido: '+k)
    return cfg
