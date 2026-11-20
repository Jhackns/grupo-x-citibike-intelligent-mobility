#!/usr/bin/env python3
"""Publica dos KPIs batch reales de una captura explícita; no se utiliza para el stream continuo."""
from pathlib import Path
import argparse,json,sys,urllib.request,urllib.parse
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT/'src'))
from disponibilidad_u2.observabilidad_sql import consultar
from disponibilidad_u2.grafana_api import literal
from disponibilidad_u2.utilidades import guardar,ahora

def main():
    p=argparse.ArgumentParser();p.add_argument('--flujo',required=True);p.add_argument('--capture-id',required=True)
    p.add_argument('--url',default='http://127.0.0.1:19091');p.add_argument('--informe',type=Path,default=ROOT/'reports/correcciones/kpis_batch.json');a=p.parse_args()
    data=consultar('''WITH x AS(SELECT flujo,capture_id,captura_utc,'convencionales' tipo,convencionales cantidad,elegible_convencionales elegible
      FROM disponibilidad.observaciones UNION ALL SELECT flujo,capture_id,captura_utc,'electricas',electricas,elegible_electricas
      FROM disponibilidad.observaciones),r AS(SELECT tipo,min(captura_utc) captura_utc,count(*) estaciones,
      count(*) FILTER(WHERE elegible AND cantidad IS NOT NULL) elegibles,
      avg(cantidad) FILTER(WHERE elegible AND cantidad IS NOT NULL) bicicletas_media,
      avg((cantidad=0)::int) FILTER(WHERE elegible AND cantidad IS NOT NULL) fraccion_sin_bicicletas,
      extract(epoch FROM min(captura_utc)) captura_epoch FROM x WHERE flujo='''+literal(a.flujo)+' AND capture_id='+literal(a.capture_id)+
      ' GROUP BY tipo) SELECT coalesce(jsonb_agg(to_jsonb(r)),\'[]\'::jsonb) FROM r')
    assert len(data)==2 and all(r['elegibles']>0 for r in data),'Captura inexistente o sin datos elegibles por tipo'
    for row in data:
        metrics={'disponibilidad_batch_bicicletas_media':row['bicicletas_media'],
          'disponibilidad_batch_fraccion_sin_bicicletas':row['fraccion_sin_bicicletas'],
          'disponibilidad_batch_datos_timestamp_seconds':row['captura_epoch'],
          'disponibilidad_batch_estaciones_elegibles':row['elegibles']}
        body=''.join('# TYPE '+k+' gauge\n'+k+' '+str(v)+'\n' for k,v in metrics.items()).encode()
        url=a.url+'/metrics/job/disponibilidad_batch/tipo/'+urllib.parse.quote(row['tipo'],safe='')
        with urllib.request.urlopen(urllib.request.Request(url,data=body,method='PUT',headers={'Content-Type':'text/plain; version=0.0.4'}),timeout=15) as response:assert response.status==200
    guardar(a.informe,{'estado':'publicado_localmente','fecha_utc':ahora(),'flujo':a.flujo,'capture_id':a.capture_id,'destino':'Pushgateway local propio','datos':data,
      'alcance':'Snapshot batch fijo, no inventario físico actual ni inferencia continua; no hubo publicación externa'})
    print({'estado':'publicado_localmente','tipos':2,'kpis_negocio':2})

if __name__=='__main__':main()
