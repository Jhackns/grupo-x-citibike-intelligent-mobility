"""Operaciones administrativas acotadas al PostgreSQL propio, sin imprimir secretos."""
import json
import subprocess


def ejecutar(sql):
    cmd=['docker','exec','-i','citibike-disponibilidad-postgres','psql','-X','-qAt',
         '-v','ON_ERROR_STOP=1','-U','disponibilidad_owner','-d','disponibilidad_u2','-f','-']
    result=subprocess.run(cmd,input=sql,text=True,capture_output=True)
    if result.returncode:raise RuntimeError('Falló SQL propio; salida omitida para proteger la configuración')
    return result.stdout.strip()


def consultar(sql):return json.loads(ejecutar(sql))
