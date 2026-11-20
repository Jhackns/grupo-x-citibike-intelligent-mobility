#!/usr/bin/env python3
"""SIGTERM al proceso Python propio dentro de spark-submit, sin tocar otros servicios."""
import argparse
import subprocess
import time

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--contenedor',required=True)
a=p.parse_args()
project=subprocess.check_output(['docker','inspect','--format','{{index .Config.Labels "com.docker.compose.project"}}',a.contenedor],text=True).strip()
if project!='citibike-disponibilidad-streaming':raise SystemExit('Se rechaza detener un contenedor ajeno')
code='''from pathlib import Path
import os,signal
targets=[]
for p in Path('/proc').iterdir():
    if not p.name.isdigit():continue
    try:a=(p/'cmdline').read_bytes().decode().split('\\0')
    except (OSError,UnicodeError):continue
    if a and Path(a[0]).name.startswith('python') and any(x in a[1:] for x in (
       '/opt/UNIDAD2/Dim-Disponibilidad/ejecutar_streaming.py',
       '/opt/UNIDAD2/Dim-Disponibilidad/ejecutar_persistencia.py')):targets.append(int(p.name))
if len(targets)!=1:raise SystemExit('No se identificó un único proceso Python propio')
os.kill(targets[0],signal.SIGTERM)
print('SIGTERM enviado al proceso Python propio')
'''
subprocess.run(['docker','exec',a.contenedor,'python3','-c',code],check=True)
deadline=time.monotonic()+90
while time.monotonic()<deadline:
    result=subprocess.run(['docker','inspect','--format','{{.State.Running}}',a.contenedor],text=True,capture_output=True)
    if result.returncode or result.stdout.strip()=='false':
        print('Contenedor propio finalizado; volúmenes y checkpoints conservados.');break
    time.sleep(1)
else:raise SystemExit('El proceso no terminó en 90 s; no se fuerza su eliminación')
