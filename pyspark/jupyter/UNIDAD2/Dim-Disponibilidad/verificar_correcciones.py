#!/usr/bin/env python3
"""Verifica cobertura, periodos y counters corregidos; no modifica datos de negocio."""
from pathlib import Path
import argparse,json,math,sys,urllib.request,urllib.parse
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT/'src'))
from disponibilidad_u2.consultas_evaluacion import resumen
from disponibilidad_u2.observabilidad_sql import consultar
from disponibilidad_u2.grafana_api import consultar_grafana,literal,api
from disponibilidad_u2.utilidades import guardar,ahora


def prom(endpoint,**params):
    with urllib.request.urlopen('http://127.0.0.1:19090/api/v1/'+endpoint+'?'+urllib.parse.urlencode(params),timeout=30) as r:return json.load(r)['data']


def main():
    p=argparse.ArgumentParser();p.add_argument('--informe',type=Path,default=ROOT/'reports/correcciones/verificacion.json');a=p.parse_args()
    report={'inicio_utc':ahora(),'estado':'en_curso','errores':[]}
    try:
        flows=['disponibilidad-etapa3-vivo-20261119-v1','disponibilidad-etapa4-vivo-20261119-v1','disponibilidad-correcciones-20261119-v1']
        measurements=[]
        for flow in flows:
            q=resumen('flujo='+literal(flow),comparar_cortes=True,filtro_flujo='flujo='+literal(flow))
            rows=consultar('SELECT coalesce(jsonb_agg(to_jsonb(r)),\'[]\'::jsonb) FROM ('+q+')r')
            for x in rows:
                assert x['predicciones']==x['evaluadas']+x['vencidas']+x['pendientes']
                assert x['pendientes']==x['pendientes_inmaduras']+x['pendientes_sin_avance']+x['pendientes_cerrables']
                assert x['pendientes_sin_avance']==x['pendientes_flujo_finalizado']+x['pendientes_sin_avance_no_atribuido']
                assert x['vencidas']==sum(x[k] for k in ('vencidas_sin_captura','vencidas_objetivo_no_elegible','vencidas_trayecto','vencidas_ambiguas','vencidas_otras'))
                assert x['denominador']==x['predicciones']-x['pendientes_inmaduras']
                if x['denominador']==0:assert x['cobertura_pct'] is None
                else:assert math.isclose(x['cobertura_pct'],100*x['numerador']/x['denominador'],abs_tol=1e-10)
                if x['n']==0:assert all(x[k] is None for k in ('mae','rmse','sesgo','evaluadas_origen_inicio_utc','evaluadas_objetivo_fin_utc'))
            if 'etapa4' in flow:
                end=[x for x in rows if x['momento']=='fin_prueba'];assert len(end)==6 and all(x['denominador']==0 and x['cobertura_pct'] is None for x in end)
            measurements.extend(rows)
        report['evaluacion']=measurements
        queries=json.loads((ROOT/'observabilidad/consultas.json').read_text());v={'flujo':flows[0],'estacion':'__all','tipo':'convencionales','horizonte':'5'}
        values=consultar_grafana(queries['metricas'],v);assert values[0]['n']==21996
        assert values[0]['evaluadas_origen_inicio_utc'] and values[0]['evaluadas_objetivo_fin_utc']
        report['grafana']={'metricas':values,'cobertura':consultar_grafana(queries['cobertura'],v)}
        assert len(report['grafana']['cobertura'])==2
        # Regresión del defecto observado visualmente: filtro de una captura tardía
        # no debe recalcular la frontera por cada fila ni superar el timeout RO.
        from datetime import datetime,timedelta
        current=json.loads((ROOT/'reports/correcciones/resultados.json').read_text())
        origin=datetime.fromisoformat(current['ultima_captura_emitida_utc'].replace('Z','+00:00'))
        tail=consultar_grafana(queries['cobertura'],{'flujo':current['flujo'],'estacion':'__all','tipo':'convencionales','horizonte':'5'},
            origin.isoformat(),(origin+timedelta(milliseconds=1)).isoformat())
        at_end=next(x for x in tail if x['momento']=='fin_prueba')
        at_audit=next(x for x in tail if x['momento']=='auditoria_actual')
        assert at_end['predicciones']>0 and at_end['pendientes_inmaduras']==at_end['predicciones']
        assert at_end['denominador']==0 and at_end['cobertura_pct'] is None
        assert at_audit['denominador']==at_audit['predicciones']
        assert at_audit['pendientes_flujo_finalizado']==at_audit['predicciones']
        report['grafana']['ultima_captura']=tail
        meta=prom('metadata',metric='disponibilidad_productor_eventos_confirmados_total')
        assert any(m['type']=='counter' for m in meta['disponibilidad_productor_eventos_confirmados_total'])
        report['prometheus_tipo']=meta
        comparisons=[]
        for metric in ('eventos_confirmados','consultas'):
            base='disponibilidad_productor_'+metric+'_total';expressions=['rate('+base+'[2m])','rate(('+base+' == '+base+')[2m:5s])'];r=[]
            for expr in expressions:
                results=prom('query_range',query=expr,start='2026-11-19T15:10:00Z',end='2026-11-19T15:40:00Z',step='5s')['result']
                vals=[float(v[1]) for s in results for v in s['values'] if math.isfinite(float(v[1]))]
                assert vals
                r.append({'promql':expr,'n':len(vals),'minimo':min(vals),'negativos':sum(x<0 for x in vals)})
            assert r[0]['negativos']>0 and r[1]['negativos']==0
            comparisons.append(r)
        report['tasas_historicas']=comparisons
        report['estado']='verificada'
    except Exception as e:
        report['estado']='fallida';report['errores'].append({'tipo':type(e).__name__,'detalle':str(e)[:350]});raise
    finally:
        report['fin_utc']=ahora();guardar(a.informe,report)
    print({'estado':report['estado'],'filas_evaluacion':len(measurements),'comparaciones_tasa':2})

if __name__=='__main__':main()
