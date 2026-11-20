"""Consultas de consola para el lanzador disponibilidad."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
from uuid import UUID
from urllib.request import urlopen
from urllib.error import URLError

ROOT = Path(__file__).resolve().parent
U1 = ROOT.parents[1] / 'UNIDAD1/Dim-Disponibilidad'
sys.path[:0] = [str(ROOT/'src'), str(U1)]
from disponibilidad_u2.configuracion import cargar
from disponibilidad_u2.config_streaming import cargar_streaming
from disponibilidad_u2.postgres import cargar_sql
from disponibilidad_u2.referencia_u1 import comprobar_referencia
from disponibilidad_u2.kafka_io import consumidor_verificacion
from disponibilidad_u2.telemetria import eventos, proceso_activo, leer_json
from disponibilidad_u2.observabilidad_sql import consultar


def config():
    return cargar_streaming(ROOT/'config/streaming-operacion.json'), cargar_sql(ROOT/'config/postgres-operacion.json')


def previo():
    stream, _ = config()
    ref = comprobar_referencia(stream['u1_reference'])
    if ref['contratos_originales_verificados'] != 6:
        raise SystemExit('Faltan contratos locales U1')
    print('Selección U1', ref['ejecucion_id'], '· seis contratos verificados')
    subprocess.run(['docker','info','--format','{{.ServerVersion}}'], check=True)
    for port, name in [(19092,'kafka'),(15432,'postgres'),(13000,'grafana'),(19090,'prometheus'),(19091,'pushgateway'),(19108,'metricas')]:
        with socket.socket() as probe:
            try:
                probe.bind(('127.0.0.1', port))
                print(port, 'libre')
            except OSError:
                owners = subprocess.check_output(['docker','ps','--filter',f'publish={port}','--format','{{.Names}}']).decode().split()
                expected = 'citibike-disponibilidad-' + name
                # El filtro publish de Docker puede identificar el puerto interno.
                mapping = subprocess.run(['docker','port',expected],capture_output=True,text=True)
                if ('127.0.0.1:'+str(port)) not in mapping.stdout.split() or (owners and owners != [expected]):
                    raise SystemExit(f'Puerto {port} ocupado sin correspondencia con {expected}')
                print(port, expected)


def reutilizar():
    from src.ejecucion import comprobar_artefactos, sha
    ref = json.loads((U1/'config/ejecucion_seleccionada.json').read_text())
    path = U1/ref['manifiesto_origen']['ruta']
    if sha(path) != ref['manifiesto_origen']['sha256']:
        raise SystemExit('Manifiesto diferente al seleccionado')
    manifest = json.loads(path.read_text())
    if manifest['estado'] != 'completada':
        raise SystemExit('Ejecución incompleta')
    comprobar_artefactos(path.parent, manifest['artefactos'])
    print('Resultados verificados para la configuración registrada:', path.parent)


def main(action):
    if action == 'previo':
        return previo()
    if action == 'reutilizar':
        return reutilizar()
    registry = leer_json(ROOT/leer_json(ROOT/'config/observabilidad.json')['registro'])
    processes = registry.get('procesos', {})
    if action == 'estado':
        subprocess.run(['docker','ps','-a','--filter','name=citibike-disponibilidad','--format','{{.Names}}  {{.Status}}'], check=True)
        for name, url in [('Grafana','http://127.0.0.1:13000/api/health'), ('Prometheus','http://127.0.0.1:19090/-/ready'), ('Exportador','http://127.0.0.1:19108/metrics'), ('Pushgateway','http://127.0.0.1:19091/-/ready')]:
            try:
                with urlopen(url,timeout=2) as response:
                    print(name, url, response.status)
            except (URLError, TimeoutError):
                print(name, url, 'sin respuesta')
        for name in ('productor','spark','sql'):
            print(name, proceso_activo(processes.get(name),registry.get('controlado')))
    elif action == 'logs':
        paths = [processes.get(name,{}).get('log') for name in ('productor','spark','sql')]
        logs = [p for p in paths if p and Path(p).is_file()]
        if not logs:
            raise SystemExit('Todavía no hay logs registrados')
        os.execvp('tail',['tail','-n','12','-F',*logs])
    elif action == 'muestra':
        consumer, group = consumidor_verificacion(cargar())
        try:
            print('Grupo',group)
            rows = consumer.poll(timeout_ms=8000,max_records=1)
            for batch in rows.values():
                for msg in batch:
                    e=json.loads(msg.value)
                    print({k:e.get(k) for k in ('capture_id','station_id','event_id')})
                    return
            raise SystemExit('Sin mensaje retenido durante la espera')
        finally:
            consumer.close()
    elif action == 'resumen':
        _, sql = config()
        if registry.get('flujo_activo') != sql['flow_id']:
            raise SystemExit('Aún no hay captura de la operación en el registro actual')
        records = eventos(processes.get('productor',{}).get('log'))
        ack = next((x for x in reversed(records) if x.get('evento')=='captura_confirmada'),None)
        if not ack:
            raise SystemExit('Esperando primera captura confirmada')
        cid = str(UUID(ack['capture_id']))
        counts = consultar(f"""WITH o AS (SELECT flujo,observacion_id FROM disponibilidad.observaciones
            WHERE flujo='disponibilidad-operacion-v1' AND capture_id='{cid}')
            SELECT json_build_object('procesados_sql',(SELECT count(*) FROM o),
            'predicciones_guardadas',(SELECT count(*) FROM o JOIN disponibilidad.predicciones p
            ON p.flujo=o.flujo AND p.event_id=o.observacion_id))""")
        print(dict(captura=cid, estaciones=ack['esperados'], confirmados=ack['confirmados'], **counts))


if __name__ == '__main__':
    main(sys.argv[1])
