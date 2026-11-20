"""Parámetros públicos de streaming y aislamiento de rutas propias."""
import json
import re
from pathlib import Path
from .configuracion import ROOT


def cargar_streaming(path=None, **overrides):
    defaults=json.loads((ROOT/'config/streaming.json').read_text())
    given=json.loads(Path(path).read_text()) if path else {}
    if not isinstance(given,dict) or set(given)-set(defaults):raise ValueError('Configuración streaming desconocida')
    cfg=defaults|given|{k:v for k,v in overrides.items() if v is not None}
    if not re.fullmatch(r'local\[[1-9][0-9]*\]',cfg['spark_master']):raise ValueError('Usar local[N] explícito')
    if cfg['driver_memory'] not in ('1g','2g'):raise ValueError('Driver acotado a 1g/2g')
    for k in ('trigger','watermark','window'):
        if not re.fullmatch(r'[1-9][0-9]* (seconds|minutes)',cfg[k]):raise ValueError('Duración inválida: '+k)
    for k in ('shuffle_partitions','max_offsets_per_trigger','max_files_per_trigger','max_catalogs_per_batch','max_future_clock_seconds'):
        if type(cfg[k]) is not int or cfg[k]<1:raise ValueError('Entero positivo requerido: '+k)
    if not cfg['topic'].startswith('citibike-disponibilidad'):raise ValueError('Tópico ajeno')
    if not all(re.fullmatch(r'[a-zA-Z0-9_.-]+:[0-9]+',s) for s in cfg['bootstrap_servers'].split(',')):
        raise ValueError('Bootstrap público host:puerto, sin credenciales')
    roots=[]
    for k in ('output_dir','checkpoint_dir'):
        p=(ROOT/cfg[k]).resolve()
        if not p.is_relative_to((ROOT/'data/streaming').resolve()):raise ValueError('Ruta fuera de data/streaming propio')
        roots.append(p)
    if roots[0]==roots[1] or roots[0].is_relative_to(roots[1]) or roots[1].is_relative_to(roots[0]):
        raise ValueError('Separar salidas y checkpoints')
    if not isinstance(cfg['starting_offsets'],(str,dict)):raise ValueError('Offsets iniciales inválidos')
    return cfg
