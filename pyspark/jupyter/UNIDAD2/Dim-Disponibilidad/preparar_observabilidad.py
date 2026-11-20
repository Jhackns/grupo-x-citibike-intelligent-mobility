#!/usr/bin/env python3
"""Provisiona solo vistas/rol propios y configura credenciales locales sin mostrarlas."""
from pathlib import Path
import json
import os
import secrets
import sys
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'src'))
from disponibilidad_u2.observabilidad_sql import ejecutar

def secret(name):
    p=ROOT/'data/secretos'/name;p.parent.mkdir(parents=True,exist_ok=True)
    try:fd=os.open(p,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    except FileExistsError:
        if p.stat().st_mode&0o077:raise ValueError('El secreto local debe tener permisos 600')
    else:
        with os.fdopen(fd,'w') as f:f.write(secrets.token_hex(32)+'\n')
    return p.read_text().strip()

if __name__=='__main__':
    (ROOT/'data/observabilidad/metricas').mkdir(parents=True,exist_ok=True)
    pwd=secret('grafana_pg_password');secret('grafana_admin_password')
    ejecutar((ROOT/'sql/002_observabilidad.sql').read_text())
    ejecutar((ROOT/'sql/003_cierre_evaluacion.sql').read_text())
    # psql cifra la clave antes de enviarla al servidor; stdout/errores no se vuelcan.
    ejecutar('\\password disponibilidad_grafana\n'+pwd+'\n'+pwd+'\n')
    cfg=json.loads((ROOT/'observabilidad/datasources.template.json').read_text())
    cfg['datasources'][0]['secureJsonData']['password']=pwd
    p=ROOT/'data/observabilidad/provisioning/datasources/datasources.yaml';p.parent.mkdir(parents=True,exist_ok=True)
    fd=os.open(p,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
    with os.fdopen(fd,'w') as f:json.dump(cfg,f,indent=2)
    print('Vistas y rol propios preparados. Configuración secreta local conservada fuera de Git.')
