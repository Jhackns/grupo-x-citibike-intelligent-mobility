"""Métricas de baja cardinalidad a partir de evidencia real, sin tocar el flujo Spark."""
from datetime import datetime
import json
import math
from pathlib import Path
import subprocess
import urllib.request
import urllib.parse

CONSULTAS={'disponibilidad_observaciones':'observaciones','disponibilidad_predicciones':'predicciones',
           'disponibilidad_rechazados':'rechazados','disponibilidad_metricas':'metricas'}


def pid_activo(pid):
    if pid is None:return False
    try:
        status=Path(f'/proc/{int(pid)}/stat').read_text().split(') ',1)[1].split()[0]
        args=Path(f'/proc/{int(pid)}/cmdline').read_bytes()
        return status not in ('Z','X') and b'Dim-Disponibilidad/' in args
    except (OSError,ValueError):return False


def proceso_activo(spec,controlado):
    if not controlado:return None
    if not spec:return False
    if not pid_activo(spec.get('pid')):return False
    if spec.get('inicio_ticks'):
        try:return Path(f"/proc/{spec['pid']}/stat").read_text().split(') ',1)[1].split()[19]==spec['inicio_ticks']
        except OSError:return False
    return True


def docker_activo(name):
    r=subprocess.run(['docker','inspect','--format','{{json .State}}',name],text=True,capture_output=True,timeout=10)
    if r.returncode:return None
    state=json.loads(r.stdout)
    return bool(state['Running'] and state.get('Health',{}).get('Status','healthy')=='healthy')


def leer_json(path):
    if not path:return {}
    try:return json.loads(Path(path).read_text())
    except (OSError,ValueError):return {}


def eventos(path):
    if not path:return []
    try:
        with Path(path).open('rb') as f:
            f.seek(max(0,Path(path).stat().st_size-131072));raw=f.read()
    except OSError:return []
    out=[]
    for line in raw.decode(errors='replace').splitlines():
        try:
            x=json.loads(line)
            if isinstance(x,dict):out.append(x)
        except ValueError:pass
    return out


def timestamp(value):
    try:return datetime.fromisoformat(value.replace('Z','+00:00')).timestamp()
    except (ValueError,AttributeError,TypeError):return None


def metrica(name,value,labels=None,kind='gauge'):
    if not name.startswith('disponibilidad_'):raise ValueError('Prefijo métrico inválido')
    labels=labels or {}
    if set(labels)-{'componente','consulta','partition'}:raise ValueError('Etiqueta no permitida')
    if kind not in ('counter','gauge'):raise ValueError('Tipo de métrica no soportado')
    # NaN dentro de un counter interrumpe la detección de resets en rate().
    # Desconocido = serie ausente; nunca se fabrica el cero inicial de otro proceso.
    if kind=='counter':
        if value is None or not math.isfinite(float(value)):return ''
        if float(value)<0:raise ValueError('Un counter no admite cantidades negativas')
    label='{'+','.join(k+'='+json.dumps(str(v)) for k,v in sorted(labels.items()))+'}' if labels else ''
    val='NaN' if value is None else str(int(value) if isinstance(value,bool) else value)
    return f'# TYPE {name} {kind}\n{name}{label} {val}\n'


def construir(registro,now,prometheus_url):
    """Valores desconocidos son NaN; etiquetas son conjuntos pequeños, nunca IDs."""
    text=metrica('disponibilidad_observador_comprobado_timestamp_seconds',now)
    estados={};flow=registro.get('flujo_activo')
    controlled=registro.get('controlado',False);processes=registro.get('procesos',{})
    for component in ('productor','spark','sql'):
        estados[component]=proceso_activo(processes.get(component),controlled)
    for component,name in [('kafka','citibike-disponibilidad-kafka'),('postgres','citibike-disponibilidad-postgres')]:
        estados[component]=docker_activo(name)
    for name,value in estados.items():text+=metrica('disponibilidad_componente_up',value,{'componente':name})
    logs=eventos(processes.get('productor',{}).get('log'))
    last=next((x for x in reversed(logs) if 'contadores' in x),{})
    counters=last.get('contadores',{})
    for key in ('consultas','capturas','eventos_confirmados','fallos_http','fallos_captura','fallos_publicacion','reintentos_http','reintentos_publicacion'):
        text+=metrica('disponibilidad_productor_'+key+'_total',counters.get(key),kind='counter')
    confirmed=next((x for x in reversed(logs) if x.get('evento')=='captura_confirmada'),{})
    text+=metrica('disponibilidad_productor_ultima_confirmacion_timestamp_seconds',timestamp(confirmed.get('ts_utc')))
    text+=metrica('disponibilidad_captura_estaciones_ultima',confirmed.get('esperados'))
    current=leer_json(processes.get('spark',{}).get('runtime'))
    end_offsets={}
    for name,p in current.get('consultas',{}).items():
        if name not in CONSULTAS:continue
        if 'progress' in p:p=(p['progress'] or [{}])[-1]
        labels={'consulta':CONSULTAS[name]}
        for key,suffix in [('numInputRows','filas_ultimo_lote'),('inputRowsPerSecond','entrada_filas_s'),('processedRowsPerSecond','procesadas_filas_s')]:
            text+=metrica('disponibilidad_spark_'+suffix,p.get(key),labels)
        duration=p.get('durationMs',{}).get('triggerExecution')
        text+=metrica('disponibilidad_spark_duracion_lote_seconds',duration/1000 if duration is not None else None,labels)
        text+=metrica('disponibilidad_spark_progreso_timestamp_seconds',timestamp(p.get('timestamp')),labels)
        state=p.get('stateOperators')
        if state:
            text+=metrica('disponibilidad_spark_filas_estado',sum(o.get('numRowsTotal',0) for o in state),labels)
            text+=metrica('disponibilidad_spark_descartes_watermark_ultimo_lote',sum(o.get('numRowsDroppedByWatermark',0) for o in state),labels)
        wm=timestamp(p.get('eventTime',{}).get('watermark'))
        if wm and wm>0:text+=metrica('disponibilidad_spark_retraso_watermark_seconds',now-wm,labels)
        if name=='disponibilidad_observaciones':
            for s in p.get('sources',[]):
                offsets=s.get('endOffset')
                try:
                    if isinstance(offsets,str):offsets=json.loads(offsets)
                    end_offsets=offsets.get('citibike-disponibilidad-estaciones-v1',{}) if offsets else {}
                except (ValueError,AttributeError):pass
    try:
        q=urllib.parse.urlencode({'query':'kafka_topic_partition_current_offset{topic="citibike-disponibilidad-estaciones-v1"}'})
        with urllib.request.urlopen(prometheus_url+'/api/v1/query?'+q,timeout=3) as r:results=json.load(r)['data']['result']
        for sample in results:
            part=sample['metric']['partition'];end=float(sample['value'][1]);consumed=end_offsets.get(part)
            value=end-float(consumed) if consumed is not None and end>=float(consumed) else None
            text+=metrica('disponibilidad_kafka_backlog_estimado_eventos',value,{'partition':part})
    except Exception:pass # Ausencia de serie no se fabrica como cero.
    sql=leer_json(processes.get('sql',{}).get('runtime'))
    text+=metrica('disponibilidad_sql_cierres_ejecucion_total',sql.get('evaluaciones_cerradas'),kind='counter')
    seen=set();lines=[]
    for line in text.splitlines():
        if line.startswith('# TYPE '):
            name=line.split()[2]
            if name in seen:continue
            seen.add(name)
        lines.append(line)
    return '\n'.join(lines)+'\n',estados,flow
