#!/usr/bin/env python3
"""Verifica salidas guardadas sin consultar Kafka/API ni ejecutar inferencia nueva."""
from pathlib import Path
import argparse
import json
import sys
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'src'))
from disponibilidad_u2.config_streaming import cargar_streaming
from disponibilidad_u2.predictor_streaming import cargar_u1,predecir
from disponibilidad_u2.utilidades import guardar,ahora


def main():
    from pyspark.sql import SparkSession,functions as F
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config',type=Path,required=True)
    p.add_argument('--produccion',type=Path,nargs='+',required=True)
    p.add_argument('--informe',type=Path,required=True)
    a=p.parse_args();cfg=cargar_streaming(a.config);u1=cargar_u1(cfg['u1_reference'])
    spark=SparkSession.builder.appName('Verificar-Disponibilidad-U2').config('spark.sql.session.timeZone','UTC').getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    try:
        out=ROOT/cfg['output_dir'];obs=spark.read.option('recursiveFileLookup','true').parquet(str(out/'observaciones')).cache()
        pred=(spark.read.parquet(str(out/'predicciones')) if next((out/'predicciones').rglob('*.parquet'),None)
              else spark.createDataFrame([],predecir(obs.limit(0),u1).schema)).cache()
        expected={}
        for p in a.produccion:
            m=json.loads(p.read_text());assert m['estado']=='completada'
            expected.update({c['capture_id']:c['expected_events'] for c in m['capturas']})
        bycap={r.capture_id:r['count'] for r in obs.groupBy('capture_id').count().collect()}
        assert bycap==expected,(bycap,expected)
        n=obs.count();np=pred.count()
        assert obs.select('event_id').distinct().count()==n
        assert pred.select('prediccion_id').distinct().count()==np
        eligible=obs.agg(*[F.sum(F.col('elegible_pred_'+t).cast('long')).alias(t) for t in ('convencionales','electricas')]).first().asDict()
        assert np==3*sum(eligible.values())
        check=pred.join(obs.select('event_id',F.col('captura_utc').alias('_origen'),
                'objetivo_convencionales','objetivo_electricas'),'event_id')
        wrong=(F.col('valor_predicho')!=F.when(F.col('tipo')=='convencionales',F.col('objetivo_convencionales')).otherwise(F.col('objetivo_electricas')).cast('double'))
        wrong=wrong | (F.col('origen_utc')!=F.col('_origen')) | ((F.col('objetivo_utc').cast('long')-F.col('origen_utc').cast('long'))!=F.col('horizonte_min')*60)
        assert check.where(wrong).count()==0
        for c in u1['contratos']:
            assert pred.where((F.col('tipo')==c['tipo']) & (F.col('horizonte_min')==c['horizonte_min']) &
                              (F.col('contrato_sha256')!=c['sha256'])).count()==0
        assert pred.where(F.col('ejecucion_u1')!=u1['ref']['ejecucion_id']).count()==0
        rejected_expected=obs.where('NOT elegible_pred_convencionales OR NOT elegible_pred_electricas').count()
        reject=(spark.read.parquet(str(out/'rechazados')) if next((out/'rechazados').rglob('*.parquet'),None) else obs.limit(0))
        assert reject.count()==rejected_expected
        assert reject.select('event_id').distinct().count()==rejected_expected
        schema=json.loads((out/'_schema_observaciones.json').read_text())
        assert {f['name'] for f in schema['fields']}==set(obs.columns)
        metrics=spark.read.parquet(str(out/'metricas')) if next((out/'metricas').rglob('*.parquet'),None) else None
        windows=[r.asDict(recursive=True) for r in metrics.orderBy('ventana_inicio_utc').limit(50).collect()] if metrics is not None else []
        latency=obs.agg(F.min('latencia_procesamiento_s').alias('min_s'),F.max('latencia_procesamiento_s').alias('max_s'),
            F.avg('latencia_procesamiento_s').alias('media_s')).first().asDict()
        report={'estado':'verificada','fecha_utc':ahora(),'output_dir':cfg['output_dir'],'checkpoint_dir':cfg['checkpoint_dir'],
            'capturas':bycap,'observaciones':n,'rechazados_totales_o_parciales':rejected_expected,
            'elegibles_por_tipo':eligible,'predicciones':np,'event_id_y_prediccion_id_unicos':True,
            'persistencia_y_horizontes_diferencias':0,'contratos_originales_recargados':6,
            'ejecucion_u1':u1['ref']['ejecucion_id'],'latencia_captura_a_calidad':latency,
            'conteos_prediccion':[r.asDict() for r in pred.groupBy('capture_id','tipo','horizonte_min').count().orderBy('capture_id','tipo','horizonte_min').collect()],
            'ventanas_finalizadas':windows,'esquemas':{'observaciones':obs.schema.simpleString(),'predicciones':pred.schema.simpleString()},
            'muestra_predicciones':[r.asDict(recursive=True) for r in pred.select('prediccion_id','event_id','station_id','origen_utc','objetivo_utc','tipo','horizonte_min','valor_predicho','version_predictor').limit(6).collect()]}
        # Solo se serializan resúmenes pequeños; no se colectan las tablas completas.
        report=json.loads(json.dumps(report,default=str));guardar(a.informe,report)
        print(json.dumps({k:report[k] for k in ('estado','observaciones','rechazados_totales_o_parciales','predicciones','ejecucion_u1')}),flush=True)
    finally:spark.stop()


if __name__=='__main__':main()
