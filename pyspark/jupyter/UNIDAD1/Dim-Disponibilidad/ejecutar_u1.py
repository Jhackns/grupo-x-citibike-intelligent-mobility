#!/usr/bin/env python3
"""Lanzador estándar local: el procesamiento y ML se ejecutan en Docker."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys

from src.ejecucion import ROOT, ahora, configuracion, guardar

CONTENEDOR = 'lambda26-pyspark'
OPT = '/opt/UNIDAD1/Dim-Disponibilidad'


def crear_ejecucion(contexto=None):
    """Preparación común a CLI y Run All; no procesa ni conecta a servicios."""
    run=ROOT/'data/ejecuciones'/datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    run.mkdir(parents=True,exist_ok=False)
    context=contexto or {}
    manifest={**context,'id':run.name,'estado':'iniciando','inicio_utc':ahora(),
              'modo':'pendiente','descarga_solicitada':False,'errores':[]}
    guardar(run/'manifiesto.json',manifest)
    if context.get('configuracion'):
        guardar(run/'configuracion.json',context['configuracion'])
    return run


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config', type=Path, help='JSON sin secretos; plantilla config/u1-local.json')
    p.add_argument('--desde', help='Primer día UTC inclusivo')
    p.add_argument('--hasta', help='Último día UTC inclusivo; no modifica cortes temporales')
    p.add_argument('--spark-master', metavar='local[N]', help='Paralelismo local; prevalece sobre JSON, predeterminado local[4]')
    p.add_argument('--nueva', action='store_true', help='Forzar nueva ejecución; nunca sobrescribe resultados')
    p.add_argument('--notebook', action='store_true', help='Ejecutar todo el notebook desde un kernel limpio usando el mismo orquestador')
    p.add_argument('--descargar', action='store_true', help='Opt-in: ejecutar primero extraccion.py')
    p.add_argument('--env-file', type=Path, help='Solo para --descargar; nunca se copia a Docker ni al manifiesto')
    p.add_argument('--credencial', choices=('publicable','secreta'), default='publicable', help='Solo descarga; no se eleva automáticamente')
    args = p.parse_args(argv)
    run = crear_ejecucion()
    manifest = json.loads((run/'manifiesto.json').read_text())
    manifest['descarga_solicitada']=args.descargar
    guardar(run/'manifiesto.json', manifest)
    try:
        if args.env_file and not args.descargar:
            raise ValueError('--env-file solo se utiliza junto con --descargar')
        cfg = configuracion(args.config, args.desde, args.hasta, args.spark_master)
        guardar(run/'configuracion.json', cfg)
        if args.descargar:
            from src.configuracion import cargar_configuracion
            from src.extraccion import intervalo
            from src.ampliacion import inventariar_historico, descargar_inventario
            remote_cfg=cargar_configuracion(args.env_file,args.credencial)
            start,end=intervalo(cfg['desde'],cfg['hasta'])
            inventory=inventariar_historico(remote_cfg,start,end)
            guardar(remote_cfg.reports/('inventario-'+run.name+'.json'),inventory)
            if not inventory['espacio_suficiente']:
                raise ValueError('Espacio insuficiente según inventario remoto y reserva Spark')
            manifest['descarga']=descargar_inventario(remote_cfg,inventory)
            if manifest['descarga']['errores']:
                guardar(run/'manifiesto.json',manifest)
                raise RuntimeError('Descarga incompleta; no se inicia el procesamiento')
            manifest['descarga_finalizada_utc'] = ahora()
        result = subprocess.run(['docker','inspect','--format',
            '{{json .State.Running}} {{.Image}}',CONTENEDOR],check=True,capture_output=True,text=True)
        active, image = result.stdout.strip().split()
        if active != 'true':
            raise RuntimeError('El contenedor lambda26-pyspark debe estar iniciado con el Compose existente')
        mounts = json.loads(subprocess.check_output(['docker','inspect','--format','{{json .Mounts}}',CONTENEDOR],text=True))
        host_jupyter = ROOT.parents[1].resolve()
        if not any(m['Destination']=='/opt' and Path(m['Source']).resolve()==host_jupyter and m['RW'] for m in mounts):
            raise RuntimeError('No se verificó el montaje escribible ./jupyter:/opt del proyecto')
        manifest.update({'configuracion':cfg,'docker_imagen_id':image,'montaje_opt_verificado':True})
        manifest['git_head'] = subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()
        manifest['rama'] = subprocess.check_output(['git','branch','--show-current'],cwd=ROOT,text=True).strip()
        guardar(run/'manifiesto.json', manifest)
        # Contexto público para Run All desde Jupyter, sin copiar ninguna variable secreta.
        guardar(ROOT/'data/contexto_notebook.json',{'configuracion':cfg,'docker_imagen_id':image,
            'montaje_opt_verificado':True,'git_head':manifest['git_head'],'rama':manifest['rama']})
        submit_args = '--master '+cfg['spark_master']+' --driver-memory '+cfg['driver_memory']+' pyspark-shell'
        command = ['docker','exec','-e','PYSPARK_SUBMIT_ARGS='+submit_args,
                   '-w',OPT,CONTENEDOR,'python','-m','src.flujo_u1','--ejecucion','data/ejecuciones/'+run.name]
        if args.notebook:
            program="""from pathlib import Path
import nbformat
from nbclient import NotebookClient
p=Path('notebooks/01_disponibilidad_batch.ipynb')
nb=nbformat.read(p,as_version=4)
try:
    NotebookClient(nb,timeout=21600,kernel_name='python3',resources={'metadata':{'path':str(p.parent.resolve())}}).execute()
    print('Notebook completo ejecutado desde kernel limpio')
finally:
    nbformat.write(nb,p)
"""
            command=['docker','exec','-e','PYSPARK_SUBMIT_ARGS='+submit_args,
                '-e','U1_RUN_DIR='+OPT+'/data/ejecuciones/'+run.name,
                '-e','U1_FORZAR_NUEVA='+str(int(args.nueva)),
                '-w',OPT,CONTENEDOR,'python','-c',program]
        elif args.nueva:
            command.append('--nueva')
        print('Ejecución:',run,flush=True)
        with (run/'ejecucion.log').open('w') as log:
            with subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True) as proc:
                for line in proc.stdout:
                    log.write(line);log.flush();print(line,end='',flush=True)
                if proc.wait():
                    raise RuntimeError('El procesamiento Docker falló; consultar manifiesto y ejecución.log')
        final = json.loads((run/'manifiesto.json').read_text())
        if final.get('estado') != 'completada':
            raise RuntimeError('No se verificó el éxito del procesamiento')
        print('Resultado:',final['modo'],'; manifiesto:',run/'manifiesto.json')
        return 0
    except (Exception, KeyboardInterrupt) as exc:
        manifest = json.loads((run/'manifiesto.json').read_text())
        manifest.update({'estado':'fallida','fin_utc':ahora()})
        # No se persisten argumentos de subprocess (pueden incluir rutas privadas).
        message = str(exc) if isinstance(exc,(ValueError,RuntimeError,AssertionError)) else type(exc).__name__
        manifest.setdefault('errores',[]).append({'fase':'lanzador','tipo':type(exc).__name__,'mensaje':message})
        guardar(run/'manifiesto.json',manifest)
        print('Error:',message,'; manifiesto:',run/'manifiesto.json',file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
