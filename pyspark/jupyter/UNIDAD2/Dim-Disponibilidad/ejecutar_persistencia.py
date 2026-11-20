#!/usr/bin/env python3
"""Salida durable Spark etapa 2 → transacciones SQL → evaluación diferida."""
import argparse
import fcntl
import hashlib
import json
from pathlib import Path
import signal
import sys
import threading
import time
from uuid import uuid4

ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'src'))
from disponibilidad_u2.config_streaming import cargar_streaming
from disponibilidad_u2.postgres import Base,SinkSQL,cargar_sql,contrato_sql
from disponibilidad_u2.predictor_streaming import cargar_u1
from disponibilidad_u2.utilidades import ahora,guardar,log


def main():
    from pyspark.sql import SparkSession,types as T
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config',type=Path,required=True)
    p.add_argument('--config-sql',type=Path,required=True)
    p.add_argument('--inicializar',action='store_true')
    p.add_argument('--disponibles',action='store_true')
    p.add_argument('--duracion-segundos',type=int,default=0)
    p.add_argument('--evaluar',action='store_true',help='Solo cerrar evaluaciones maduras, sin ingerir')
    p.add_argument('--verificar-sql',action='store_true')
    p.add_argument('--informe',type=Path)
    args=p.parse_args();cfg=cargar_sql(args.config_sql);stream=cargar_streaming(args.config)
    stop=threading.Event();spark=None;db=None;query=None;lock=None
    for sig in (signal.SIGINT,signal.SIGTERM):signal.signal(sig,lambda *_:stop.set())
    report={'inicio_utc':ahora(),'estado':'en_curso','configuracion_sql':cfg,'configuracion_stream':stream,'errores':[]}
    file=ROOT/'data/sql/ejecuciones'/(str(uuid4())+'.json')
    try:
        spark=(SparkSession.builder.appName('Disponibilidad-U2-Postgres')
            .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions',stream['shuffle_partitions'])
            .config('spark.ui.showConsoleProgress','false').getOrCreate())
        spark.sparkContext.setLogLevel('WARN')
        if spark.version!=stream['spark_version'] or spark.sparkContext.master!=stream['spark_master']:
            raise ValueError('Runtime Spark distinto de configuración')
        report['codigo_sha256']={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in
            [ROOT/'ejecutar_persistencia.py',ROOT/'src/disponibilidad_u2/postgres.py',ROOT/'sql/001_disponibilidad.sql']}
        db=Base(spark,cfg)
        report['software']={'spark':spark.version,'master':spark.sparkContext.master,
          'postgresql':db.conn.getMetaData().getDatabaseProductVersion(),'jdbc':db.conn.getMetaData().getDriverVersion()}
        db.iniciar()
        if args.inicializar:
            report['estado']='completada';return 0
        u1=cargar_u1(stream['u1_reference']);source=ROOT/stream['output_dir']
        stamp=json.loads((source/'_pipeline.json').read_text())
        contract=contrato_sql(cfg,stream,u1,stamp);db.registrar(contract);report['contrato']=contract
        db.confirmar_rondas()
        def drenar_evaluaciones():
            total=0
            while True:
                n=db.evaluar();total+=n
                if n<cfg['evaluation_limit']:return total
        if args.verificar_sql:
            from disponibilidad_u2.verificacion_sql import verificar
            result=verificar(spark,db,source,cfg,u1)
            if args.informe:guardar(args.informe,result)
            report['verificacion']=result;report['estado']='completada';return 0
        if args.evaluar:
            report['cerradas']=drenar_evaluaciones();report['estado']='completada';return 0
        if not db.json('SELECT to_json(pg_try_advisory_lock(hashtextextended(?,0)))::text',('worker:'+cfg['flow_id'],)):
            raise ValueError('Ya existe un consumidor SQL de este flujo')
        db.conn.commit()
        cp=ROOT/cfg['checkpoint_dir'];cp.parent.mkdir(parents=True,exist_ok=True)
        lock=(cp.parent/(cp.name+'.lock')).open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        rows=db.json('SELECT to_json(count(*))::text FROM disponibilidad.lotes WHERE flujo=?',(cfg['flow_id'],))
        if rows and not (cp/'metadata').is_file():raise ValueError('Falta checkpoint de una carga SQL existente; recuperación explícita requerida')
        schema=T.StructType.fromJson(json.loads((source/'_schema_observaciones.json').read_text()))
        incoming=(spark.readStream.schema(schema).option('recursiveFileLookup','true').option('pathGlobFilter','*.parquet')
          .option('maxFilesPerTrigger',stream['max_files_per_trigger']).parquet(str(source/'observaciones')))
        writer=(incoming.writeStream.queryName('disponibilidad_postgres').option('checkpointLocation',str(cp))
            .foreachBatch(SinkSQL(cfg,u1,contract)))
        query=(writer.trigger(availableNow=True) if args.disponibles else writer.trigger(processingTime=cfg['trigger'])).start()
        report['query_id']=str(query.id);report['run_id']=str(query.runId);guardar(file,report)
        log('sql_iniciado',flujo=cfg['flow_id'],checkpoint=str(cp))
        started=time.monotonic();last=0;report['evaluaciones_cerradas']=0
        while query.isActive and not stop.is_set():
            if time.monotonic()-last>=cfg['evaluation_poll_seconds']:
                report['evaluaciones_cerradas']+=db.evaluar();last=time.monotonic()
                guardar(file,report)
            if args.duracion_segundos and time.monotonic()-started>=args.duracion_segundos:break
            stop.wait(1)
        if query.exception():raise RuntimeError('Falló el sink SQL')
        if query.isActive:query.stop()
        report['evaluaciones_cerradas']+=drenar_evaluaciones() if args.disponibles else db.evaluar()
        report['estado']='completada';return 0
    except Exception as exc:
        error={'tipo':type(exc).__name__}
        if isinstance(exc,(ValueError,AssertionError,FileNotFoundError)):error['detalle']=str(exc)[:500]
        try:
            error['sqlstate']=exc.java_exception.getSQLState()
            msg=exc.java_exception.getServerErrorMessage()
            if msg is not None:error['detalle_sql']=msg.getMessage()[:300]
        except Exception:pass
        report['estado']='fallida';report['errores'].append(error)
        log('sql_fallido',error_tipo=type(exc).__name__)
        # No serializar mensajes JDBC que puedan contener datos de conexión.
        return 1
    finally:
        if query is not None:
            if query.isActive:query.stop()
            report['progreso']=json.loads(json.dumps(query.recentProgress,default=str))
        if db is not None:db.close()
        if spark is not None:spark.stop()
        if lock is not None:lock.close()
        report['fin_utc']=ahora();guardar(file,report)
        log('sql_finalizado',estado=report['estado'],manifiesto=str(file))


if __name__=='__main__':raise SystemExit(main())
