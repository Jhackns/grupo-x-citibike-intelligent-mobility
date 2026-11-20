#!/usr/bin/env python3
"""Reproduce llegadas controladas de eventos API reales: dos triggers, duplicado y tardío.

No consulta API, no publica Kafka y no cambia eventos; sus salidas nunca alimentan SQL.
"""
import argparse,hashlib,json,sys,time
from pathlib import Path
from datetime import datetime,timezone
from uuid import uuid4
from pyspark.sql import SparkSession,types as T
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT/'src'))
from disponibilidad_u2.config_streaming import cargar_streaming
from disponibilidad_u2.predictor_streaming import cargar_u1
from disponibilidad_u2.calidad_streaming import normalizar,deduplicar
from disponibilidad_u2.salidas_streaming import CalidadSink
from disponibilidad_u2.utilidades import guardar,ahora


def main():
    p=argparse.ArgumentParser();p.add_argument('--config');p.add_argument('--muestra-real',required=True)
    p.add_argument('--informe',default=str(ROOT/'reports/correcciones/replay_s8.json'));a=p.parse_args()
    cfg=cargar_streaming(a.config);sample=json.loads(Path(a.muestra_real).read_text());assert len(sample)==3
    times=[datetime.fromisoformat(x['captura_utc'].replace('Z','+00:00')) for x in sample]
    assert times[1]>times[0] and (times[2]-times[0]).total_seconds()>=24*60
    spark=SparkSession.builder.appName('Disponibilidad-verificacion-S8-real').config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','2').config('spark.ui.showConsoleProgress','false').getOrCreate()
    spark.sparkContext.setLogLevel('ERROR');u1=cargar_u1(cfg['u1_reference']);runs=[];query=None
    schema=T.StructType.fromDDL('json_original string,kafka_key string,kafka_topic string,kafka_partition int,kafka_offset bigint,kafka_timestamp timestamp,kafka_timestamp_type int')
    try:
        for trigger in (10,30):
            own=ROOT/'data/verificacion'/('s8-'+str(trigger)+'-'+str(uuid4()));source=own/'entrada';source.mkdir(parents=True)
            own_cfg={**cfg,'output_dir':str((own/'salidas').relative_to(ROOT))}
            def write(i,rows):(source/(str(i)+'.json')).write_text('\n'.join(json.dumps(x,ensure_ascii=False) for x in rows))
            write(0,[sample[0],sample[0]])
            data=spark.readStream.schema(schema).json(str(source));normalized=normalizar(data,u1,cfg)
            writer=deduplicar(normalized,'10 minutes').writeStream.foreachBatch(CalidadSink(u1,own_cfg,'replay-real-'+str(trigger)))
            query=writer.option('checkpointLocation',str(own/'checkpoint')).trigger(processingTime=str(trigger)+' seconds').start()
            start=time.monotonic();b=c=False
            while time.monotonic()-start<45:
                elapsed=time.monotonic()-start
                if elapsed>=12 and not b:write(1,[sample[1]]);b=True
                if elapsed>=24 and not c:write(2,[sample[2]]);c=True
                if query.exception():raise query.exception()
                time.sleep(0.5)
            at45=json.loads(json.dumps(query.lastProgress,default=str))
            deadline=time.monotonic()+75
            while time.monotonic()<deadline:
                progress=query.lastProgress or {};wm=progress.get('eventTime',{}).get('watermark')
                if wm and datetime.fromisoformat(wm.replace('Z','+00:00'))>times[0]:break
                time.sleep(1)
            else:raise TimeoutError('Watermark no avanzó con los eventos reales')
            write(3,[sample[0]]) # Llegada controlada del original antiguo, sin cambiar su timestamp.
            deadline=time.monotonic()+75
            while time.monotonic()<deadline:
                states=[op for pr in query.recentProgress for op in pr.get('stateOperators',[])]
                dropped=sum(x.get('numRowsDroppedByWatermark',0) for x in states)
                if dropped>=1:break
                if query.exception():raise query.exception()
                time.sleep(1)
            else:raise TimeoutError('No se comprobó el descarte tardío')
            allprogress=json.loads(json.dumps(query.recentProgress,default=str));query.stop();query=None
            saved=spark.read.option('recursiveFileLookup','true').parquet(str(own/'salidas/observaciones'))
            ids=[r.event_id for r in saved.select('event_id').collect()]
            assert len(ids)==len(set(ids))==3 and set(ids)=={r['event_id'] for r in sample}
            runs.append({'trigger_s':trigger,'batch_id_a_45s':at45.get('batchId'),'progreso_a_45s':at45,
              'entrada_eventos':5,'salida_eventos_unicos':len(ids),'originales_inalterados':True,
              'tardios_descartados':dropped,'estado_eliminado':sum(x.get('numRowsRemoved',0) for x in states),
              'progreso':allprogress,'checkpoint':str((own/'checkpoint').relative_to(ROOT)),
              'hashes_originales':[hashlib.sha256(x['json_original'].encode()).hexdigest() for x in sample]})
        result={'estado':'verificada','fecha_utc':ahora(),'spark':spark.version,'master':spark.sparkContext.master,'ejecuciones':runs,
          'procedencia':'reports/correcciones/muestra_replay_real.json','alcance':'Eventos API reales preservados; transporte/llegadas controladas por archivos. No es una llegada tardía espontánea del broker ni una prueba de carga.'}
        guardar(Path(a.informe),result);print({'estado':'verificada','triggers':[10,30],'datos':'API real, replay controlado'})
    finally:
        if query is not None:query.stop()
        spark.stop()

if __name__=='__main__':main()
