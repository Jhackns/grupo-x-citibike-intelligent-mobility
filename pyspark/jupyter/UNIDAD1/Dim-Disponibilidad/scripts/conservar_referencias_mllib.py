#!/usr/bin/env python3
"""Conserva solo el mejor candidato MLlib histórico por tipo/horizonte.

Utilidad histórica del cierre preliminar. El punto de entrada ampliado ya guarda
todos los candidatos y referencias; esa ejecución no se vuelve a entrenar aquí.
Auditoría U1: no cambia la selección global ni ejecuta otra búsqueda. Si existen
referencias compatibles, las recarga sin fit. Los originales de fase 6 son lectura.
"""
import argparse
import json
from pathlib import Path
import platform
import sys
from datetime import datetime, timezone
from time import perf_counter

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.ejecucion import guardar, sha, ahora, artefactos, comprobar_artefactos, codigo

VERSION = 'referencia-mllib-u1-v1'


def exigir(ok, message):
    if not ok:
        raise AssertionError(message)


def verificar(spark, gold, output, manifest):
    from pyspark.sql import functions as F
    from pyspark.ml import PipelineModel
    from src.modelado import datos_combo, predecir, metricas
    proofs = []
    for ref in manifest['referencias']:
        key = ref['clave']
        pipe = PipelineModel.load(str(output/key/'pipeline'))
        test = datos_combo(gold.where("particion_temporal='prueba'"),ref['tipo'],ref['horizonte_min'])
        reloaded = predecir(test,pipe,ref['tipo']+'_actual')
        saved = spark.read.parquet(str(output/key/'predicciones_prueba'))
        keys = ['station_id','captura_utc','horizonte_min']
        n = ref['metricas_prueba']['n']
        for df in (saved,reloaded):
            exigir(df.count()==n,'Recarga: cardinalidad diferente')
            exigir(not df.groupBy(*keys).count().where('count>1').limit(1).count(),'Claves duplicadas')
        delta = saved.select(*keys,'prediction').join(reloaded.select(*keys,F.col('prediction').alias('recargada')),keys,'full').agg(
            F.count('*').alias('n'),F.sum((F.col('prediction').isNull()|F.col('recargada').isNull()).cast('long')).alias('nulos'),
            F.max(F.abs(F.col('prediction')-F.col('recargada'))).alias('max_diferencia')).first().asDict()
        exigir(delta['n']==n and delta['nulos']==0 and delta['max_diferencia']<=1e-10,'Predicciones no reproducibles')
        m = metricas(saved)
        exigir(all(abs(m[k]-ref['metricas_prueba'][k])<=1e-10 for k in ('MAE','RMSE','R2')),'Métricas no reproducibles')
        proofs.append({'clave':key,**delta})
    exigir(len(proofs)==6,'Faltan referencias por tipo/horizonte')
    return proofs


def main():
    from pyspark.sql import SparkSession, functions as F
    from pyspark import StorageLevel
    from pyspark.ml import PipelineModel
    from pyspark.ml.regression import LinearRegression, RandomForestRegressor
    from src.modelado import (FEATURES, CANDIDATOS, POLITICA, ajustar_preproceso, datos_combo,
                              predecir, metricas, elegir)
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fuente',required=True,type=Path,help='Directorio integrado completo de fase 6')
    args=parser.parse_args()
    source=args.fuente.resolve()
    source_manifest=json.loads((source/'manifiesto.json').read_text())
    exigir(source_manifest['estado']=='completada' and source_manifest['modo']=='nueva','Fuente no es una ejecución completa')
    comprobar_artefactos(source,source_manifest['artefactos'])
    exigir(codigo()==source_manifest['codigo_sha256'],'Los módulos originales cambiaron')
    embedded=source/'modelos/referencias_mllib'
    if embedded.is_dir():
        contracts=sorted(embedded.glob('*/contrato.json'))
        exigir(len(contracts)==6,'Faltan contratos MLlib en la ejecución integrada')
        print(json.dumps({'modo':'referencias_integradas_existentes_sin_fit',
            'fuente':str(source),'referencias':[str(p) for p in contracts],
            'verificacion':'usar ejecutar_u1.py; no se crea otro flujo ni se repite entrenamiento'}),flush=True)
        return
    source_sha=sha(source/'manifiesto.json')
    freeze=json.loads((source/'modelos/seleccion_congelada.json').read_text())
    original=json.loads((source/'modelos/resultado.json').read_text())
    root=ROOT/'data/referencias_mllib'
    existing=[]
    for path in sorted(root.glob('*/manifiesto.json')):
        m=json.loads(path.read_text())
        if m.get('estado')=='completada' and m.get('fuente_manifiesto_sha256')==source_sha and m.get('script_sha256')==sha(__file__):
            existing.append((path.parent,m))
    out = existing[-1][0] if existing else root/datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    manifest = existing[-1][1] if existing else {
        'version':VERSION,'estado':'en_curso','inicio_utc':ahora(),'fuente':str(source),
        'fuente_manifiesto_sha256':source_sha,'seleccion_global_sha256':sha(source/'modelos/seleccion_congelada.json'),
        'gold_fuente':str(source/'gold/aceptados'),'gold_version':source_manifest['versiones']['gold'],
        'configuracion':source_manifest['configuracion'],'script_sha256':sha(__file__),
        'candidatos_elegidos_por':'MAE de validación histórico, restringido a familias MLlib; política de empate original',
        'rol':'referencias entrenadas; no ganadoras globales; persistencia seleccionada permanece intacta',
        'referencias':[],'errores':[]}
    start=perf_counter()
    spark=None
    if not existing:
        out.mkdir(parents=True,exist_ok=False)
        guardar(out/'manifiesto.json',manifest)
    try:
        spark=(SparkSession.builder.master('local[2]').appName('Revision-U1-referencias-MLlib')
            .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions',8)
            .config('spark.ui.enabled','false').config('spark.ui.showConsoleProgress','false').getOrCreate())
        spark.sparkContext.setLogLevel('ERROR')
        exigir(spark.version==source_manifest['versiones']['spark'] and platform.python_version()==source_manifest['versiones']['python'],
               'Entorno incompatible con la ejecución original')
        gold=spark.read.parquet(str(source/'gold/aceptados'))
        if existing:
            comprobar_artefactos(out,manifest['artefactos'])
            proofs=verificar(spark,gold,out,manifest)
            print(json.dumps({'modo':'reutilizada_sin_fit','directorio':str(out),'recarga':proofs}),flush=True)
            return
        manifest['software']={'spark':spark.version,'python':platform.python_version()}
        for selection in freeze['selecciones']:
            tipo,h=selection['tipo'],selection['h']
            candidate=elegir([x for x in freeze['comparativa_validacion']
                if x['tipo']==tipo and x['horizonte_min']==h and x['familia']!='persistencia'])
            spec=next(c for c in CANDIDATOS if c['id']==candidate['candidato'])
            exigir(spec['params']==candidate['parametros'],'Parámetros históricos diferentes')
            key=f'{tipo}_{h}min'
            combo=datos_combo(gold,tipo,h)
            train=combo.where("particion_temporal='train'").persist(StorageLevel.MEMORY_AND_DISK)
            prior=source/'modelos/seleccionados'/key
            prior_contract=json.loads((prior/'contrato.json').read_text())
            if prior_contract['candidato']==candidate['candidato'] and (prior/'pipeline').is_dir():
                pipe=PipelineModel.load(str(prior/'pipeline'))
                prepinfo=prior_contract['preprocesamiento']
                fit=False
            else:
                print('Conservar referencia:',key,candidate['candidato'],'(pesos históricos no disponibles; solo este fit)',flush=True)
                prep,prepinfo=ajustar_preproceso(train)
                estimator=(LinearRegression if spec['familia']=='lr' else RandomForestRegressor)(
                    labelCol='label',featuresCol='features',predictionCol='prediction',**spec['params'])
                fitted=estimator.fit(prep.transform(train))
                pipe=PipelineModel(stages=prep.stages+[fitted])
                fit=True
            metrics={}
            for split in ('validacion','prueba'):
                pred=predecir(combo.where(F.col('particion_temporal')==split),pipe,tipo+'_actual').persist(StorageLevel.MEMORY_AND_DISK)
                metrics[split]=metricas(pred)
                if split=='prueba':
                    pred.select('station_id','captura_utc','horizonte_min','label','prediction_raw','prediction').write.mode('errorifexists').parquet(
                        str(out/key/'predicciones_prueba'))
                pred.unpersist()
            pipe.write().save(str(out/key/'pipeline'))
            historic_test=next(x for x in original['metricas_prueba'] if x['tipo']==tipo and x['horizonte_min']==h and x['candidato']==candidate['candidato'])
            exigir(metrics['validacion']['n']==candidate['n'] and metrics['prueba']['n']==historic_test['n'],'Poblaciones cambiaron')
            last=pipe.stages[-1]
            ref={'clave':key,'tipo':tipo,'horizonte_min':h,'candidato':candidate['candidato'],'familia':spec['familia'],
                'parametros':spec['params'],'ajuste_nuevo_necesario':fit,'n_train':train.count(),
                'preprocesamiento':prepinfo,'predictores':FEATURES,'politica':POLITICA,
                'criterio_global':'Persistencia sigue seleccionada por MAE de validación; esta referencia no la sustituye',
                'metricas_historicas_validacion':candidate,'metricas_validacion':metrics['validacion'],'metricas_prueba':metrics['prueba'],
                'deltas_historicos_validacion':{k:metrics['validacion'][k]-candidate[k] for k in ('MAE','RMSE','R2')},
                'deltas_historicos_prueba':{k:metrics['prueba'][k]-historic_test[k] for k in ('MAE','RMSE','R2')},
                'medianas_coinciden_con_historicas':prepinfo['medianas_train']==selection['preprocesamiento']['medianas_train'],
                'alcance':'mismo día ya inspeccionado; objetivos reportados por tipo; exactitud física no confirmada'}
            if spec['familia']=='lr':
                ref['intercepto']=last.intercept
                ref['coeficientes_orden_vector']=last.coefficients.toArray().tolist()
            guardar(out/key/'referencia.json',ref)
            manifest['referencias'].append(ref)
            guardar(out/'manifiesto.json',manifest)
            train.unpersist()
        manifest['recarga_verificada']=verificar(spark,gold,out,manifest)
        comprobar_artefactos(source,source_manifest['artefactos'])
        exigir(source_sha==sha(source/'manifiesto.json'),'Manifiesto original modificado')
        exigir(manifest['seleccion_global_sha256']==sha(source/'modelos/seleccion_congelada.json'),'Selección original modificada')
        manifest.update({'estado':'completada','fin_utc':ahora(),'duracion_s':perf_counter()-start,
                         'artefactos':artefactos(out)})
        guardar(out/'manifiesto.json',manifest)
        print(json.dumps({'estado':'completada','directorio':str(out),'referencias':6,'fits_nuevos':sum(x['ajuste_nuevo_necesario'] for x in manifest['referencias']),
                          'recarga':manifest['recarga_verificada']}),flush=True)
    except BaseException as exc:
        if not existing:
            manifest.update({'estado':'fallida','fin_utc':ahora(),'duracion_s':perf_counter()-start,
                             'errores':[{'tipo':type(exc).__name__,'mensaje':str(exc)[:2000]}]})
            guardar(out/'manifiesto.json',manifest)
        raise
    finally:
        if spark:
            spark.stop()


if __name__=='__main__':
    main()
