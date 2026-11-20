#!/usr/bin/env python3
"""Dos consultas PromQL durante un ensayo API real corto, independiente de la cohorte."""
from pathlib import Path
import json,math,os,signal,subprocess,sys,time,urllib.request,urllib.parse
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT/'src'))
from disponibilidad_u2.telemetria import eventos,pid_activo
from disponibilidad_u2.grafana_api import consultar_grafana,literal
from disponibilidad_u2.utilidades import guardar,ahora


def main():
    observer=ROOT/'data/observabilidad/observador.json'
    if observer.exists():
        previous=json.loads(observer.read_text());pid=previous.get('pid')
        if pid_activo(pid) and (ROOT/'observar.py').as_posix().encode() in Path(f'/proc/{pid}/cmdline').read_bytes():
            os.kill(pid,signal.SIGTERM)
            for _ in range(60):
                if json.loads(observer.read_text()).get('fin_utc'):break
                time.sleep(0.5)
            else:raise RuntimeError('El colector previo no cerró')
    f='disponibilidad-promql-comprobacion-v1';report={'inicio_utc':ahora(),'flujo':f,'muestras':[],'estado':'en_curso'}
    logfile=ROOT/'data/verificacion/promql_driver.log';logfile.parent.mkdir(exist_ok=True)
    with logfile.open('w') as log:
        command=[sys.executable,str(ROOT/'ejecutar_observado.py'),'--config',str(ROOT/'config/streaming-promql.json'),
          '--config-sql',str(ROOT/'config/postgres-promql.json'),'--spark-master','local[2]','--capturas','2','--intervalo','60',
          '--pausa-verificacion-segundos','45','--informe',str(ROOT/'reports/correcciones/ensayo_promql.json')]
        proc=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT);deadline=time.monotonic()+360
        try:
            while proc.poll() is None and time.monotonic()<deadline:
                try:
                    registry=json.loads((ROOT/'data/observabilidad/registro.json').read_text())
                    if registry.get('flujo_activo')==f:
                        expected=next((e['contadores']['eventos_confirmados'] for e in reversed(eventos(registry.get('procesos',{}).get('productor',{}).get('log'))) if 'contadores' in e),None)
                        query='disponibilidad_productor_eventos_confirmados_total'
                        with urllib.request.urlopen('http://127.0.0.1:19090/api/v1/query?'+urllib.parse.urlencode({'query':query}),timeout=10) as r:values=json.load(r)['data']['result']
                        if expected is not None and values and float(values[0]['value'][1])==expected and (not report['muestras'] or expected!=report['muestras'][-1]['confirmados']):
                            sql='SELECT count(*) predicciones,max(origen_utc) ultimo_origen_utc,max(objetivo_utc) ultimo_objetivo_utc FROM grafana_disponibilidad.predicciones WHERE flujo='+literal(f)
                            data=consultar_grafana(sql,{'flujo':f,'estacion':'__all','tipo':'convencionales','horizonte':'5'})
                            report['muestras'].append({'consulta_utc':ahora(),'promql':query,'confirmados':expected,'prometheus':values,'grafana_sql':data})
                            guardar(ROOT/'reports/correcciones/promql_en_vivo.json',report)
                except (OSError,ValueError,RuntimeError):pass
                time.sleep(3)
            if proc.poll() is None:raise TimeoutError('El ensayo PromQL excedió su límite')
            assert proc.returncode==0,'El flujo complementario terminó con error'
            assert len(report['muestras'])>=2 and report['muestras'][0]['confirmados']!=report['muestras'][-1]['confirmados']
            assert any(s['grafana_sql'][0]['predicciones']>0 for s in report['muestras'])
            report.update(estado='verificada',fin_utc=ahora(),alcance='Consultas realizadas durante el ensayo API/Kafka/Spark/SQL con productor finito y consumidores drenando; no evaluación de precisión.')
        finally:
            if proc.poll() is None:proc.send_signal(signal.SIGTERM);proc.wait(timeout=150)
            guardar(ROOT/'reports/correcciones/promql_en_vivo.json',report)
    print({'estado':report['estado'],'consultas_en_vivo':len(report['muestras'])})

if __name__=='__main__':main()
