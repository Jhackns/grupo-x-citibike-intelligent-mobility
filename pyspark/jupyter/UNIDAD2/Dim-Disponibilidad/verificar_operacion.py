#!/usr/bin/env python3
"""Prueba acotada del exportador propio: SIGTERM, alerta, reinicio y métricas."""
from pathlib import Path
import json,subprocess,sys,time,urllib.request
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT/'src'))
from disponibilidad_u2.utilidades import guardar,ahora


def main():
    report={'inicio_utc':ahora(),'estado':'en_curso'}
    compose=['docker','compose','-f',str(ROOT/'compose.observabilidad.yml')]
    def alerts():
        with urllib.request.urlopen('http://127.0.0.1:19090/api/v1/alerts',timeout=10) as f:return json.load(f)['data']['alerts']
    try:
        subprocess.run(['docker','exec','citibike-disponibilidad-prometheus','promtool','check','config','/etc/prometheus/prometheus.yml'],check=True,capture_output=True)
        with urllib.request.urlopen('http://127.0.0.1:19108/metrics',timeout=5) as f:before=f.read().decode()
        assert '_total NaN' not in before and 'disponibilidad_captura_umbral_antiguedad_seconds 150' in before
        start=time.monotonic();subprocess.run(compose+['stop','exporter-disponibilidad'],check=True,capture_output=True)
        s=json.loads(subprocess.run(['docker','inspect','--format','{{json .State}}','citibike-disponibilidad-metricas'],check=True,capture_output=True,text=True).stdout)
        report['apagado']={'duracion_s':time.monotonic()-start,'exit_code':s['ExitCode'],'oom_killed':s['OOMKilled']};assert s['ExitCode']==0 and not s['OOMKilled']
        deadline=time.monotonic()+75
        while time.monotonic()<deadline:
            observed=alerts()
            fired=[a for a in observed if a['labels']['alertname']=='DisponibilidadTelemetriaAusente' and a['state']=='firing']
            if fired:break
            time.sleep(5)
        else:raise RuntimeError('La alerta real de telemetría no llegó a firing')
        report['alerta_real']=fired
    finally:
        subprocess.run(compose+['up','-d','exporter-disponibilidad'],check=True,capture_output=True)
    deadline=time.monotonic()+30
    while time.monotonic()<deadline:
        try:
            with urllib.request.urlopen('http://127.0.0.1:19108/metrics',timeout=5) as f:after=f.read().decode()
            assert '_total NaN' not in after
            if not any(a['labels']['alertname']=='DisponibilidadTelemetriaAusente' for a in alerts()):break
        except Exception:pass
        time.sleep(3)
    else:raise RuntimeError('El exportador o la alerta no se recuperaron tras reinicio')
    report.update(estado='verificada',fin_utc=ahora(),metricas_tras_reinicio=True,alerta_resuelta=True)
    guardar(ROOT/'reports/correcciones/reinicio_exportador.json',report)
    print({'estado':'verificada','exportador_exit_code':0,'alerta_real':'firing y resuelta'})

if __name__=='__main__':main()
