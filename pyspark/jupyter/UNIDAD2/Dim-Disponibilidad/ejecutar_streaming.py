#!/usr/bin/env python3
"""Kafka → calidad U1 → persistencia; proceso spark-submit con cuatro checkpoints."""
from pathlib import Path
import argparse
import json
import signal
import sys
import threading
import time
import fcntl
from uuid import uuid4

ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'src'))
from disponibilidad_u2.config_streaming import cargar_streaming
from disponibilidad_u2.predictor_streaming import cargar_u1,predecir,sha
from disponibilidad_u2.calidad_streaming import normalizar,deduplicar,calidad_grupo,CATALOGO_U1,metricas_ventana
from disponibilidad_u2.salidas_streaming import CalidadSink
from disponibilidad_u2.utilidades import ahora,guardar,digest,log


def main():
    from pyspark.sql import SparkSession,types as T,functions as F
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config',type=Path)
    p.add_argument('--disponibles',action='store_true')
    p.add_argument('--dependencias',action='store_true')
    p.add_argument('--duracion-segundos',type=int,default=0)
    a=p.parse_args();cfg=cargar_streaming(a.config);stop=threading.Event();queries=[];spark=None
    for sig in (signal.SIGINT,signal.SIGTERM):signal.signal(sig,lambda *_:stop.set())
    report={'inicio_utc':ahora(),'estado':'en_curso','configuracion':cfg,'consultas':{},'errores':[]}
    runtime=ROOT/'data/streaming/ejecuciones'/(str(uuid4())+'.json');lock=None
    try:
        spark=(SparkSession.builder.appName('Citibike-Disponibilidad-U2')
            .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions',cfg['shuffle_partitions'])
            .config('spark.sql.streaming.noDataMicroBatches.enabled','true')
            .config('spark.sql.adaptive.enabled','false').config('spark.ui.showConsoleProgress','false').getOrCreate())
        spark.sparkContext.setLogLevel('WARN')
        scala=spark.sparkContext._jvm.scala.util.Properties.versionNumberString()
        if spark.version!=cfg['spark_version'] or scala!=cfg['scala_version'] or spark.sparkContext.master!=cfg['spark_master']:
            raise ValueError('Spark/Scala/master reales no corresponden a configuración')
        spark.sparkContext._jvm.java.lang.Thread.currentThread().getContextClassLoader().loadClass('org.apache.spark.sql.kafka010.KafkaSourceProvider')
        report['software']={'spark':spark.version,'scala':scala,'master':spark.sparkContext.master,'kafka_package':cfg['kafka_package']}
        if a.dependencias:
            report['estado']='completada';return 0
        u1=cargar_u1(cfg['u1_reference'])
        report['u1']={'ejecucion':u1['ref']['ejecucion_id'],'referencia_sha256':u1['referencia_sha256'],
                      'contratos':[{'tipo':c['tipo'],'horizonte_min':c['horizonte_min'],'sha256':c['sha256']} for c in u1['contratos']]}
        out=ROOT/cfg['output_dir'];cp=ROOT/cfg['checkpoint_dir']
        cp.parent.mkdir(parents=True,exist_ok=True)
        lock=(cp.parent/(cp.name+'.lock')).open('a')
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:raise ValueError('Otro consumidor usa estos checkpoints') from None
        fingerprint=digest({'config':{k:v for k,v in cfg.items() if k not in ('spark_master','driver_memory','trigger','max_offsets_per_trigger','max_files_per_trigger','starting_offsets')},
            'u1':u1['referencia_sha256'],'codigo':{str(x.relative_to(ROOT)):sha(x) for x in [ROOT/'ejecutar_streaming.py',
                *[ROOT/'src/disponibilidad_u2'/f for f in ('calidad_streaming.py','predictor_streaming.py','salidas_streaming.py')]]}})
        stamp=out/'_pipeline.json'
        if stamp.exists():
            if json.loads(stamp.read_text())['fingerprint']!=fingerprint:raise ValueError('Checkpoints/salidas de otra configuración; usar rutas nuevas explícitas')
        else:
            if out.exists() and any(out.iterdir()) or cp.exists() and any(cp.iterdir()):raise ValueError('Rutas no vacías sin contrato de pipeline')
            guardar(stamp,{'fingerprint':fingerprint,'configuracion_inicial':cfg,'u1':report['u1']})
        for name in ('observaciones','rechazados','predicciones','metricas'):
            checkpoint='ingesta' if name=='observaciones' else name
            if (out/name).exists() and next((out/name).rglob('*.parquet'),None) and not (cp/checkpoint/'metadata').exists():
                raise ValueError('Falta checkpoint de una salida existente; no se reprocesa automáticamente')
            (out/name).mkdir(parents=True,exist_ok=True)
        raw=(spark.readStream.format('kafka').option('kafka.bootstrap.servers',cfg['bootstrap_servers'])
            .option('subscribe',cfg['topic']).option('startingOffsets',json.dumps(cfg['starting_offsets']) if isinstance(cfg['starting_offsets'],dict) else cfg['starting_offsets'])
            .option('maxOffsetsPerTrigger',cfg['max_offsets_per_trigger']).option('failOnDataLoss','true').load()
            .selectExpr('CAST(value AS STRING) AS json_original','CAST(key AS STRING) AS kafka_key',
                        'topic AS kafka_topic','partition AS kafka_partition','offset AS kafka_offset',
                        'timestamp AS kafka_timestamp','timestampType AS kafka_timestamp_type'))
        normalized=normalizar(raw,u1,cfg)
        schema=calidad_grupo(spark.createDataFrame([],normalized.schema),CATALOGO_U1,u1).schema
        report['schema_observaciones']=schema.jsonValue()
        guardar(out/'_schema_observaciones.json',schema.jsonValue())
        def track(q,name):
            queries.append(q)
            if a.disponibles:q.awaitTermination()
            return q
        def trigger(writer):
            return writer.trigger(availableNow=True) if a.disponibles else writer.trigger(processingTime=cfg['trigger'])
        ingest=track(trigger(deduplicar(normalized,cfg['watermark']).writeStream.queryName('disponibilidad_observaciones')
            .option('checkpointLocation',str(cp/'ingesta')).foreachBatch(CalidadSink(u1,cfg,fingerprint))).start(),'observaciones')
        def source():
            return (spark.readStream.schema(schema).option('recursiveFileLookup','true').option('pathGlobFilter','*.parquet')
                    .option('maxFilesPerTrigger',cfg['max_files_per_trigger']).parquet(str(out/'observaciones')))
        def file_sink(df,name):
            return track(trigger(df.writeStream.queryName('disponibilidad_'+name).format('parquet').outputMode('append')
                .option('path',str(out/name)).option('checkpointLocation',str(cp/name))).start(),name)
        file_sink(source().where(~F.col('elegible_pred_convencionales') | ~F.col('elegible_pred_electricas')),'rechazados')
        file_sink(predecir(source(),u1),'predicciones')
        file_sink(metricas_ventana(source(),cfg),'metricas')
        report['estado']='activa';guardar(runtime,report)
        log('streaming_iniciado',modo='disponibles' if a.disponibles else 'persistente',salidas=str(out),checkpoints=str(cp))
        started=time.monotonic()
        while not a.disponibles and not stop.is_set():
            if any(q.exception() for q in queries):raise RuntimeError('Falló una consulta streaming')
            if a.duracion_segundos and time.monotonic()-started>=a.duracion_segundos:break
            for q in queries:
                if q.lastProgress:report['consultas'][q.name]=json.loads(json.dumps(q.lastProgress,default=str))
            guardar(runtime,report);stop.wait(1)
        for q in queries:
            if q.exception():raise RuntimeError('Consulta streaming fallida')
        report['estado']='completada';return 0
    except Exception as exc:
        report['estado']='fallida';report['errores'].append({'tipo':type(exc).__name__,'mensaje':str(exc)[:1500]})
        raise
    finally:
        for q in reversed(queries):
            if q.isActive:q.stop()
            report['consultas'][q.name]={'id':str(q.id),'runId':str(q.runId),
                'progress':json.loads(json.dumps(q.recentProgress,default=str))}
        report['fin_utc']=ahora()
        try:guardar(runtime,report)
        finally:
            if spark is not None:spark.stop()
            if lock is not None:lock.close()
        log('streaming_finalizado',estado=report['estado'],informe=str(runtime))


if __name__=='__main__':raise SystemExit(main())
