#!/usr/bin/env python3
"""Lanzador local estándar: ejecuta spark-submit en el contenedor propio."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from uuid import uuid4

ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'src'))
from disponibilidad_u2.config_streaming import cargar_streaming
from disponibilidad_u2.utilidades import guardar


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config',type=Path)
    p.add_argument('--spark-master')
    p.add_argument('--trigger')
    p.add_argument('--disponibles',action='store_true',help='Procesar lo disponible y finalizar, conservando estado')
    p.add_argument('--duracion-segundos',type=int,default=0)
    p.add_argument('--replay-real',action='store_true',help='Verifica triggers y tardíos con eventos reales locales')
    p.add_argument('--muestra-real',type=Path)
    p.add_argument('--pruebas',action='store_true',help='Solo pruebas sintéticas pequeñas, sin API/Kafka')
    p.add_argument('--dependencias',action='store_true',help='Solo verificar versiones y classpath')
    p.add_argument('--verificar',action='store_true',help='Leer y verificar salidas existentes, sin consumir ni predecir')
    p.add_argument('--sql',action='store_true',help='Etapa 3: salida durable de etapa 2 → PostgreSQL')
    p.add_argument('--config-sql',type=Path)
    p.add_argument('--inicializar',action='store_true',help='Solo --sql: crear/verificar esquema propio')
    p.add_argument('--evaluar',action='store_true',help='Solo --sql: cerrar evaluaciones maduras')
    p.add_argument('--verificar-sql',action='store_true')
    p.add_argument('--pruebas-sql',action='store_true',help='Pruebas sintéticas en flujos SQL separados')
    p.add_argument('--produccion',type=Path,nargs='+',help='Manifiestos explícitos de las capturas esperadas')
    p.add_argument('--informe')
    a=p.parse_args()
    if a.replay_real and (a.sql or not a.muestra_real):p.error('--replay-real requiere --muestra-real y no admite --sql')
    if (a.config_sql or a.inicializar or a.evaluar or a.verificar_sql or a.pruebas_sql) and not a.sql:
        p.error('Las opciones SQL requieren --sql')
    if a.sql and (a.pruebas or a.verificar or a.dependencias):p.error('No mezclar los modos SQL y de etapa 2')
    if a.duracion_segundos<0:p.error('La duración no puede ser negativa')
    a.informe=a.informe or ('reports/postgres/verificacion_sql.json' if a.sql else 'reports/streaming/verificacion_salidas.json')
    cfg=cargar_streaming(a.config,spark_master=a.spark_master,trigger=a.trigger)
    identity=ROOT/'data/streaming/sistema';identity.mkdir(parents=True,exist_ok=True)
    (identity/'passwd').write_text('root:x:0:0:root:/root:/bin/sh\n'+
        f'disponibilidad:x:{os.getuid()}:{os.getgid()}:Disponibilidad:/tmp:/bin/sh\n')
    (identity/'group').write_text(f'root:x:0:\ndisponibilidad:x:{os.getgid()}:\n')
    public=ROOT/'data/streaming/lanzamientos'/(str(uuid4())+'.json')
    guardar(public,cfg)
    inner='/opt/UNIDAD2/Dim-Disponibilidad/'
    target='verificar_s8.py' if a.replay_real else ('tests/test_streaming.py' if a.pruebas else ('verificar_streaming.py' if a.verificar else 'ejecutar_streaming.py'))
    compose=['docker','compose','-f',str(ROOT/'compose.streaming.yml')]
    packages=cfg['kafka_package'];sql_public=None
    if a.sql:
        from disponibilidad_u2.postgres import cargar_sql
        sql_cfg=cargar_sql(a.config_sql)
        sql_public=ROOT/'data/streaming/lanzamientos'/(str(uuid4())+'.json');guardar(sql_public,sql_cfg)
        target='tests/test_postgres.py' if a.pruebas_sql else 'ejecutar_persistencia.py'
        compose+=['-f',str(ROOT/'compose.sql-cliente.yml')]
        packages+=','+sql_cfg['jdbc_package']
    command=compose+['run','--rm','--no-deps',
        'streaming-disponibilidad','--master',cfg['spark_master'],'--driver-memory',cfg['driver_memory'],
        '--packages',packages,'--repositories','https://repo.maven.apache.org/maven2',
        '--conf','spark.jars.ivy='+inner+'data/dependencias/ivy',
        '--conf','spark.ui.enabled=false',inner+target]
    if not a.pruebas:
        command+=['--config',inner+public.relative_to(ROOT).as_posix()]
        if a.sql:
            command+=['--config-sql',inner+sql_public.relative_to(ROOT).as_posix()]
            for name in ('inicializar','evaluar','verificar_sql'):
                if getattr(a,name):command.append('--'+name.replace('_','-'))
            if a.verificar_sql:command+=['--informe',inner+a.informe]
        if a.replay_real:
            command+=['--muestra-real',inner+a.muestra_real.resolve().relative_to(ROOT).as_posix(),'--informe',inner+a.informe]
        if a.verificar:
            if not a.produccion:raise ValueError('--verificar requiere --produccion explícita')
            command+=['--produccion',*[inner+f.resolve().relative_to(ROOT).as_posix() for f in a.produccion],
                      '--informe',inner+a.informe]
        if a.disponibles:command.append('--disponibles')
        if a.dependencias:command.append('--dependencias')
        if a.duracion_segundos:command+=['--duracion-segundos',str(a.duracion_segundos)]
    env=dict(os.environ,DISP_UID=str(os.getuid()),DISP_GID=str(os.getgid()))
    return subprocess.run(command,env=env).returncode


if __name__=='__main__':raise SystemExit(main())
