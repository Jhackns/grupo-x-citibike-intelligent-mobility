"""Cliente de verificación local, sin registrar cabeceras ni credenciales."""
import base64
from datetime import datetime
import json
import urllib.request
import urllib.error
from .configuracion import ROOT


def literal(value):return "'"+str(value).replace("'","''")+"'"


def renderizar(sql,variables):
    for key in ('flujo','estacion','tipo','horizonte'):
        sql=sql.replace('${'+key+':sqlstring}',literal(variables[key]))
    return sql


def api(path,data=None):
    cfg=json.loads((ROOT/'config/observabilidad.json').read_text())
    password=(ROOT/'data/secretos/grafana_admin_password').read_text().strip()
    auth=base64.b64encode(('disponibilidad_admin:'+password).encode()).decode()
    req=urllib.request.Request(cfg['grafana_url']+path,data=json.dumps(data).encode() if data is not None else None,
        headers={'Authorization':'Basic '+auth,'Content-Type':'application/json'})
    try:
        with urllib.request.urlopen(req,timeout=60) as response:return json.load(response)
    except urllib.error.HTTPError as exc:
        try:
            result=json.load(exc)
            detail=result.get('message') or result.get('results',{}).get('A',{}).get('error','Error de consulta')
        except Exception:detail='Error HTTP'
        raise RuntimeError(f'Grafana HTTP {exc.code}: {str(detail)[:350]}') from None


def consultar_grafana(sql,variables,desde='2026-11-19T03:40:00Z',hasta='2026-11-19T04:40:00Z',format='table'):
    ms=lambda x:str(int(datetime.fromisoformat(x.replace('Z','+00:00')).timestamp()*1000))
    data=api('/api/ds/query',{'from':ms(desde),'to':ms(hasta),'queries':[{'refId':'A','datasource':{'uid':'disponibilidad-pg','type':'postgres'},
      'rawSql':renderizar(sql,variables),'format':format,'maxDataPoints':1000,'intervalMs':5000}]})
    result=data['results']['A']
    if result.get('error'):raise RuntimeError('Consulta Grafana falló: '+result['error'][:500])
    out=[]
    for frame in result.get('frames',[]):
        fields=[f['name'] for f in frame['schema']['fields']]
        out.extend(dict(zip(fields,row)) for row in zip(*frame.get('data',{}).get('values',[])))
    return out
