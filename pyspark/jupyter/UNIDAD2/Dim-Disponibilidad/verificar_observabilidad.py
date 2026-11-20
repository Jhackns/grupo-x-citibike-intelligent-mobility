#!/usr/bin/env python3
"""Contrasta Grafana, SQL y Prometheus usando evidencia conservada y datos reales."""
from pathlib import Path
import argparse
import json
import math
import sys
import time
import urllib.request
import urllib.parse
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'src'))
from disponibilidad_u2.grafana_api import api,consultar_grafana,literal
from disponibilidad_u2.observabilidad_sql import consultar
from disponibilidad_u2.utilidades import guardar,ahora


def prom(path):
    with urllib.request.urlopen('http://127.0.0.1:19090'+path,timeout=15) as r:return json.load(r)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--exigir-historia-activa',action='store_true',
                        help='Exige productor activo en Prometheus en los últimos 30 minutos; usar tras un ensayo real.')
    args=parser.parse_args()
    queries=json.loads((ROOT/'observabilidad/consultas.json').read_text())
    old=json.loads((ROOT/'reports/postgres/verificacion_real.json').read_text())
    report={'inicio_utc':ahora(),'estado':'en_curso','errores':[]}
    try:
        health=api('/api/health');assert health['version']=='11.6.11'
        ds=api('/api/datasources/uid/disponibilidad-pg');assert ds['user']=='disponibilidad_grafana'
        assert ds['secureJsonFields']['password'] and not ds.get('secureJsonData',{}).get('password')
        assert api('/api/datasources/uid/disponibilidad-pg/health')['status']=='OK'
        assert api('/api/datasources/uid/disponibilidad-prom/health')['status']=='OK'
        dashboard=api('/api/dashboards/uid/citibike-disponibilidad-u2')['dashboard']
        assert dashboard['uid']=='citibike-disponibilidad-u2'
        source=json.loads((ROOT/'observabilidad/dashboards/disponibilidad.json').read_text())
        assert [p.get('targets') for p in dashboard['panels']]==[p.get('targets') for p in source['panels']]
        report['grafana']={'version':health['version'],'postgres_health':'OK','prometheus_health':'OK',
                          'dashboard_uid':dashboard['uid'],'paneles':len(dashboard['panels']),'json_provisionado_coincide':True}
        variables={'flujo':old['flujo'],'estacion':'__all','tipo':'convencionales','horizonte':'5'}
        security=consultar_grafana('''SELECT current_user::text rol,current_setting('transaction_read_only') solo_lectura,
            has_table_privilege(current_user,'grafana_disponibilidad.predicciones','SELECT') lee_vista,
            has_table_privilege(current_user,(SELECT c.oid FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid
                WHERE n.nspname='disponibilidad' AND c.relname='predicciones'),'INSERT,UPDATE,DELETE') escribe_base,
            has_schema_privilege(current_user,'grafana_disponibilidad','CREATE') crea,
            has_database_privilege(current_user,'disponibilidad_u2','TEMP') temporales''',variables)[0]
        assert security=={'rol':'disponibilidad_grafana','solo_lectura':'on','lee_vista':True,'escribe_base':False,'crea':False,'temporales':False}
        report['privilegios']=security
        try:consultar_grafana('UPDATE grafana_disponibilidad.predicciones SET valor_predicho=valor_predicho WHERE false',variables)
        except RuntimeError as e:report['intento_escritura']='rechazado'
        else:raise AssertionError('Grafana no debe poder escribir')
        expected_metrics={(m['tipo'],m['horizonte_min']):m for m in old['metricas_globales'] if m['periodo']==m['discrepancias']=='todos'}
        expected_coverage={(m['tipo'],m['horizonte_min']):m for m in old['por_tipo_horizonte']}
        comparisons=[];expired=0
        for (tipo,h),m in expected_metrics.items():
            v={**variables,'tipo':tipo,'horizonte':str(h)}
            measured=consultar_grafana(queries['metricas'],v)[0]
            cov=consultar_grafana(queries['cobertura'],v)[0]
            assert measured['n']==m['n']
            for k in ('mae','rmse','sesgo'):assert math.isclose(measured[k],m[k],abs_tol=1e-10)
            for k in ('predicciones','evaluadas','vencidas','pendientes'):assert cov[k]==expected_coverage[tipo,h][k]
            assert cov['pendientes_cerrables']==cov['pendientes_inmaduras']==0
            assert cov['pendientes_sin_avance']==cov['pendientes']
            reasons=consultar_grafana(queries['vencidas'],v);assert sum(x['n'] for x in reasons)==cov['vencidas']
            expired+=cov['vencidas'];comparisons.append({'tipo':tipo,'horizonte':h,'metricas':measured,'cobertura':cov,'vencidas':reasons})
        assert expired==19572
        report['comparaciones_historicas']=comparisons
        quality=consultar_grafana(queries['calidad'],variables)[0]
        assert quality['observaciones']==old['conteos']['observaciones'] and quality['rechazos_totales_o_parciales']==old['conteos']['rechazos']
        assert quality['discrepantes']+quality['sin_discrepancias']+quality['calidad_desconocida']==quality['observaciones']
        report['calidad']=quality
        station=old['muestra_evaluaciones'][0]['station_id'];v={**variables,'estacion':station}
        measured=consultar_grafana(queries['metricas'],v)[0]
        sql='''SELECT jsonb_build_object('n',count(*) FILTER(WHERE estado='evaluada'),
             'mae',avg(abs(error)) FILTER(WHERE estado='evaluada')) FROM disponibilidad.predicciones p
             JOIN disponibilidad.evaluaciones e USING(flujo,prediccion_id)
             WHERE p.flujo='''+literal(v['flujo'])+' AND station_id='+literal(station)+" AND tipo='convencionales' AND horizonte_min=5"
        direct=consultar(sql)
        assert measured['n']==direct['n'] and math.isclose(measured['mae'],direct['mae'],abs_tol=1e-10)
        reduced=consultar_grafana(queries['cobertura'],variables,'2026-11-19T03:50:00Z','2026-11-19T03:56:00Z')[0]
        direct_n=consultar("SELECT to_json(count(*)) FROM disponibilidad.predicciones WHERE flujo="+literal(variables['flujo'])+
           " AND tipo='convencionales' AND horizonte_min=5 AND origen_utc BETWEEN timestamptz '2026-11-19T03:50:00Z' AND timestamptz '2026-11-19T03:56:00Z'")
        assert reduced['predicciones']==direct_n
        assert consultar_grafana(queries['metricas'],{**v,'estacion':'inexistente'})==[]
        report['filtros']={'estacion':station,'metricas_grafana':measured,'metricas_sql':direct,'periodo_reducido_predicciones':direct_n,'inexistente_sin_filas':True}
        series=consultar_grafana(queries['observado_predicho'],variables,format='time_series')
        fields=[k for k in series[0] if k.lower()!='time'];assert len(fields)==2
        assert all((r[fields[0]] is None)==(r[fields[1]] is None) for r in series)
        assert any(r[fields[1]] is None for r in series)
        report['alineacion']={'campo_temporal':'fecha objetivo UTC','puntos':len(series),'pares_misma_muestra':True,'huecos_null_sin_relleno':True}
        # Verificar todos los demás targets SQL del JSON, no solo los agregados.
        report['consultas_sql']={}
        for name,sql in queries.items():
            rows=consultar_grafana(sql,variables,format='time_series' if name in ('observado_predicho','latencia') else 'table')
            report['consultas_sql'][name]={'filas':len(rows),'estado':'OK'}
        targets=prom('/api/v1/targets')['data']['activeTargets']
        report['prometheus_targets']=[{'job':t['labels']['job'],'health':t['health']} for t in targets]
        assert {t['labels']['job'] for t in targets}=={'disponibilidad-aplicaciones','disponibilidad-kafka','disponibilidad-grafana','disponibilidad-batch'} and all(t['health']=='up' for t in targets)
        expressions=['disponibilidad_productor_eventos_confirmados_total','disponibilidad_spark_duracion_lote_seconds',
            'kafka_topic_partition_current_offset{topic="citibike-disponibilidad-estaciones-v1"}',
            'disponibilidad_componente_up','disponibilidad_kafka_backlog_estimado_eventos']
        technical={}
        for expression in expressions:
            values=prom('/api/v1/query?'+urllib.parse.urlencode({'query':expression}))['data']['result']
            assert values,expression
            technical[expression]=[{'labels':x['metric'],'value':x['value'][1]} for x in values]
        series_names=prom('/api/v1/series?'+urllib.parse.urlencode({'match[]':'{__name__=~"disponibilidad_.*"}'}))['data']
        assert all(not(set(s)&{'station_id','event_id','capture_id','run_id'}) for s in series_names)
        active_history=prom('/api/v1/query?'+urllib.parse.urlencode({'query':'max_over_time(disponibilidad_componente_up{componente="productor"}[30m])'}))['data']['result']
        was_active=bool(active_history and float(active_history[0]['value'][1])==1)
        if args.exigir_historia_activa:assert was_active,'No hay evidencia reciente de productor activo'
        report['tecnica']={'series':technical,'series_custom':len(series_names),'sin_etiquetas_alta_cardinalidad':True,
                          'productor_activo_verificado_en_historia':was_active,
                          'exigir_historia_activa':args.exigir_historia_activa}
        report['estado']='verificada'
    except Exception as e:
        report['estado']='fallida';report['errores'].append({'tipo':type(e).__name__,'detalle':str(e)[:400]})
        raise
    finally:
        report['fin_utc']=ahora();guardar(ROOT/'reports/observabilidad/verificacion.json',report)
    print({'estado':report['estado'],'paneles':len(dashboard['panels']),'comparaciones_modelo_horizonte':6,'targets_prometheus':len(targets)})


if __name__=='__main__':main()
