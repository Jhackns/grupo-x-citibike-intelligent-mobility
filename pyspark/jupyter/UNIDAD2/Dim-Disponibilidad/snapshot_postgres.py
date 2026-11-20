#!/usr/bin/env python3
"""Resumen de contenido SQL propio para comparar reinicios; no exporta filas."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'src'))
from disponibilidad_u2.postgres import cargar_sql
from disponibilidad_u2.utilidades import guardar,ahora

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--config-sql',type=Path)
p.add_argument('--output',type=Path,required=True)
a=p.parse_args();cfg=cargar_sql(a.config_sql)
command=['docker','exec','-i','citibike-disponibilidad-postgres','psql','-X','-qAt','-v','ON_ERROR_STOP=1',
  '-U','disponibilidad_owner','-d','disponibilidad_u2','--set','flujo='+cfg['flow_id'],'-f','-']
started=ahora()
result=subprocess.run(command,input=(ROOT/'sql/snapshot.sql').read_text(),text=True,capture_output=True,check=True)
summary={'inicio_utc':started,'fecha_utc':ahora(),'flujo':cfg['flow_id'],'tablas':json.loads(result.stdout)}
guardar(a.output,summary)
print(json.dumps({'flujo':cfg['flow_id'],'tablas':{k:v['n'] for k,v in summary['tablas'].items()}}))
