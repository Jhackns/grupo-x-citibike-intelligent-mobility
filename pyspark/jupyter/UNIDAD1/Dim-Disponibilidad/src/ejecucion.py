"""Configuración y manifiestos locales de U1, sin Spark ni credenciales."""
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
VERSION = 'u1-integrado-v3-spark-master'
FUENTES = ('station_status', 'station_information', 'vehicle_types')
STAMP = re.compile(r'^(\d{4}-\d{2}-\d{2})T\d{2}-\d{2}-\d{2}-\d{3}Z-')


def ahora():
    return datetime.now(timezone.utc).isoformat()


def guardar(path, obj):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str, allow_nan=False)+'\n')
    os.replace(tmp, path)


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024*1024), b''):
            h.update(block)
    return h.hexdigest()


def huella(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(',', ':'), default=str).encode()).hexdigest()


def normalizar_spark_master(value):
    """Paralelismo local explícito; evita seleccionar todos los CPU por defecto."""
    if not isinstance(value, str) or not re.fullmatch(r'local\[[1-9][0-9]*\]', value):
        raise ValueError('spark_master debe usar local[N], con N entero positivo; por ejemplo local[4] o local[2]')
    return value


def configuracion(path=None, desde=None, hasta=None, spark_master=None):
    defaults = json.loads((ROOT/'config/u1-local.json').read_text())
    given = json.loads(Path(path).read_text()) if path else {}
    if not isinstance(given, dict) or set(given)-(set(defaults) | {'cores'}):
        raise ValueError('Configuración: claves desconocidas; usar config/u1-local.json como plantilla sin secretos')
    cfg = defaults | given
    # Compatibilidad de lectura con configuraciones anteriores; una sola autoridad
    # en la configuración efectiva, sin reescribir JSON/manifiestos históricos.
    if 'cores' in given:
        if type(given['cores']) is not int or not 1 <= given['cores'] <= 4:
            raise ValueError('Configuración antigua: cores debe ser un entero entre 1 y 4')
        legacy_master = f"local[{given['cores']}]"
        if 'spark_master' in given and given['spark_master'] != legacy_master:
            raise ValueError('cores y spark_master contradictorios; conservar solo spark_master')
        cfg['spark_master'] = given.get('spark_master', legacy_master)
    cfg.pop('cores', None)
    if spark_master is not None:
        cfg['spark_master'] = spark_master
    cfg['spark_master'] = normalizar_spark_master(cfg['spark_master'])
    if desde:
        cfg['desde'] = desde
    if hasta:
        cfg['hasta'] = hasta
    start, end = date.fromisoformat(cfg['desde']), date.fromisoformat(cfg['hasta'])
    cuts = [datetime.strptime(cfg[k], '%Y-%m-%d %H:%M:%S') for k in (
        'corte_validacion_utc', 'corte_prueba_utc', 'fin_exclusivo_utc')]
    if not datetime.combine(start, datetime.min.time()) < cuts[0] < cuts[1] < cuts[2] <= datetime.combine(end+timedelta(days=1), datetime.min.time()):
        raise ValueError('Periodo/cortes UTC incompatibles: desde < validación < prueba < fin <= hasta+1 día')
    if cfg.get('inicio_modelado_utc'):
        beginning=datetime.strptime(cfg['inicio_modelado_utc'],'%Y-%m-%d %H:%M:%S')
        if not datetime.combine(start,datetime.min.time()) <= beginning < cuts[0]:
            raise ValueError('Inicio de modelado fuera del periodo de entrenamiento')
        if any(t.time()!=datetime.min.time() for t in [beginning,*cuts]):
            raise ValueError('El histórico ampliado usa límites a medianoche UTC')
    for key, low, high in [('max_edad_s',1,86400),('hueco_max_s',1,86400),('tolerancia_s',1,30),
                            ('shuffle_partitions',1,128)]:
        if type(cfg[key]) is not int or not low <= cfg[key] <= high:
            raise ValueError('Configuración numérica fuera de rango: '+key)
    if cfg['driver_memory'] not in ('2g','3g','4g'):
        raise ValueError('driver_memory debe ser 2g, 3g o 4g')
    return cfg


def crear_spark(cfg, app_name='Disponibilidad-U1-integrada'):
    """Sesión común al flujo, notebook y prueba pequeña; sin modificar memoria."""
    from pyspark.sql import SparkSession
    master = normalizar_spark_master(cfg['spark_master'])
    spark = (SparkSession.builder.master(master).appName(app_name)
        .config('spark.sql.session.timeZone', 'UTC')
        .config('spark.sql.shuffle.partitions', cfg['shuffle_partitions'])
        .config('spark.ui.enabled', 'false')
        .config('spark.ui.showConsoleProgress', 'false').getOrCreate())
    if spark.sparkContext.master != master:
        raise RuntimeError('La sesión Spark activa usa otro master; iniciar un kernel/proceso nuevo')
    return spark


def inventario(raw, cfg):
    raw = Path(raw)
    result = []
    for source in FUENTES:
        for path in sorted((raw/source).rglob('*.json.gz')):
            match = STAMP.match(path.name)
            if not match:
                raise ValueError('Nombre de captura no reconocido; inspeccionar antes de incorporar: '+path.name)
            if cfg['desde'] <= match[1] <= cfg['hasta']:
                if path.is_symlink() or not path.resolve().is_relative_to(raw.resolve()):
                    raise ValueError('Raw debe contener archivos regulares dentro de su raíz')
                result.append({'ruta':path.relative_to(raw).as_posix(), 'fuente':source,
                               'bytes':path.stat().st_size, 'sha256':sha(path)})
        if not any(x['fuente']==source for x in result):
            raise FileNotFoundError('No hay capturas locales del periodo para '+source+
                                    '; no se toman catálogos futuros ni se descarga automáticamente')
    return result


def codigo():
    paths = sorted((ROOT/'src').glob('*.py'))+[ROOT/'ejecutar_u1.py']
    return {p.relative_to(ROOT).as_posix():sha(p) for p in paths}


def artefactos(root):
    """Hash de cada salida persistida; excluye solo el manifiesto y el log activo."""
    root = Path(root)
    return [{'ruta':p.relative_to(root).as_posix(),'bytes':p.stat().st_size,'sha256':sha(p)}
            for p in sorted(root.rglob('*')) if p.is_file() and p.name not in ('manifiesto.json','ejecucion.log')]


def comprobar_artefactos(root, esperados):
    if not esperados or artefactos(root) != esperados:
        raise AssertionError('Artefactos incompletos o modificados: se rechaza la reutilización')


def compatible(ejecuciones, fingerprint):
    for path in sorted(Path(ejecuciones).glob('*/manifiesto.json'), reverse=True):
        try:
            m = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        # No se encadenan reutilizaciones ni se reanuda una etapa parcial.
        if m.get('estado')=='completada' and m.get('modo')=='nueva' and m.get('huella_compatibilidad')==fingerprint:
            comprobar_artefactos(path.parent, m['artefactos'])
            return path.parent, m
    return None
