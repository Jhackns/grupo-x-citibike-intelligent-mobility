"""Experimento temporal MLlib sobre Gold fijado; selección solo con validación."""

from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
import hashlib
import json
from html import escape

from pyspark.sql import functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import Imputer, SQLTransformer, VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark import StorageLevel

VERSION = 'modelado-v2-regularizacion-interpretacion'
EPS_EMPATE = 1e-6
FEATURES = [c for t in ('convencionales','electricas') for c in (
    t+'_actual',t+'_lag5',t+'_cambio5',t+'_lag10',t+'_cambio10',t+'_lag15',t+'_cambio15',
    t+'_media15',t+'_min15',t+'_max15',t+'_std15',t+'_n15')] + [
    'hora_local','minuto_local','dia_semana_iso','fin_semana','hora_seno','hora_coseno']
CANDIDATOS = [
    {'id':'persistencia','familia':'persistencia','params':{}},
    {'id':'lr_ridge_001','familia':'lr','params':{'regParam':0.01,'elasticNetParam':0.0,'maxIter':40,'tol':1e-6,'standardization':True}},
    {'id':'lr_ridge_01','familia':'lr','params':{'regParam':0.1,'elasticNetParam':0.0,'maxIter':40,'tol':1e-6,'standardization':True}},
    {'id':'lr_sin_regularizar','familia':'lr','params':{'regParam':0.0,'elasticNetParam':0.0,'maxIter':40,'tol':1e-6,'standardization':True}},
    {'id':'rf_8_depth4','familia':'rf','params':{'numTrees':8,'maxDepth':4,'maxBins':32,'minInstancesPerNode':20,
        'featureSubsetStrategy':'sqrt','maxMemoryInMB':128,'subsamplingRate':1.0,'seed':42}},
    {'id':'rf_16_depth6','familia':'rf','params':{'numTrees':16,'maxDepth':6,'maxBins':32,'minInstancesPerNode':20,
        'featureSubsetStrategy':'sqrt','maxMemoryInMB':128,'subsamplingRate':1.0,'seed':42}},
]
POLITICA = {'criterio':'MAE_validacion','tolerancia_empate_absoluta':EPS_EMPATE,
    'prioridad_empate':[c['id'] for c in CANDIDATOS],
    'negativas':'max(0,prediccion_raw); no cota superior ni redondeo a enteros',
    'nulos':'mediana train por variable + indicador de ausencia; columna completamente nula en train se omite',
    'station_id':'solo trazabilidad/evaluacion; no predictor de alta cardinalidad en este experimento',
    'r2_constante':'null cuando la varianza del objetivo es cero',
    'prueba':'lectura y evaluación después de guardar seleccion_congelada.json; no refit ni elección de horizonte'}


def ahora():
    return datetime.now(timezone.utc).isoformat()


def guardar_json(path, data):
    Path(path).parent.mkdir(parents=True,exist_ok=True)
    Path(path).write_text(json.dumps(data,ensure_ascii=False,default=str,indent=2)+'\n',encoding='utf-8')


def ajustar_preproceso(train, features=FEATURES):
    if not features or any(c not in FEATURES for c in features) or len(set(features)) != len(features):
        raise ValueError('Predictores fuera de la lista explícita permitida')
    profile = train.agg(F.count('*').alias('_n'),*[F.sum(
        (F.col(c).isNull() | F.isnan(c)).cast('long')).alias(c) for c in features]).first().asDict()
    if not profile['_n']:
        raise ValueError('Train vacío')
    used = [c for c in features if profile[c] < profile['_n']]
    if not used:
        raise ValueError('No hay predictores observados en train')
    flags = [c+'__ausente' for c in used]
    imputed = [c+'__mediana' for c in used]
    sql = 'SELECT *, '+', '.join(f'CAST((`{c}` IS NULL OR isnan(`{c}`)) AS DOUBLE) AS `{c}__ausente`' for c in used)+' FROM __THIS__'
    pipeline = Pipeline(stages=[SQLTransformer(statement=sql),
        Imputer(strategy='median',relativeError=0.001,inputCols=used,outputCols=imputed),
        VectorAssembler(inputCols=imputed+flags,outputCol='features',handleInvalid='error')]).fit(train)
    info = {'ajuste':'solo_train','filas_train':profile['_n'],'nulos_train':{c:profile[c] for c in features},
        'predictores':used,'omitidos_totalmente_nulos_train':[c for c in features if c not in used],
        'orden_vector':imputed+flags,'medianas_train':pipeline.stages[1].surrogateDF.first().asDict(),
        'relativeError_mediana':0.001,'schema_entrada':train.select(*used).schema.jsonValue()}
    return pipeline, info


def acotar(df):
    # greatest ignora NULL en Spark: el when explícito evita convertirlo en cero.
    return df.withColumn('prediction',F.when(F.col('prediction_raw').isNotNull(),
        F.greatest(F.lit(0.0),F.col('prediction_raw'))))


def predecir(df, pipeline, actual):
    if pipeline is None:
        return acotar(df.withColumn('prediction_raw',F.col(actual).cast('double')))
    return acotar(pipeline.transform(df).withColumnRenamed('prediction','prediction_raw'))


def tabla_metricas(df, grupos=()):
    invalid = (F.col('label').isNull() | F.col('prediction').isNull() | F.isnan('prediction') |
               (F.abs('prediction') == F.lit(float('inf'))))
    result = df.groupBy(*grupos).agg(F.count('*').alias('n'),
        F.sum(invalid.cast('long')).alias('invalidas'),
        F.avg(F.abs(F.col('label')-F.col('prediction'))).alias('MAE'),
        F.avg(F.pow(F.col('label')-F.col('prediction'),2)).alias('_mse'),
        F.var_pop('label').alias('_var'),
        F.sum((F.col('prediction_raw') < 0).cast('long')).alias('predicciones_negativas_raw'))
    return (result.withColumn('RMSE',F.sqrt('_mse'))
        .withColumn('R2',F.when(F.col('_var') > 0,1-F.col('_mse')/F.col('_var'))).drop('_mse','_var'))


def metricas(df):
    m = tabla_metricas(df).first().asDict()
    if not m['n'] or m['invalidas']:
        raise ValueError('Predicciones vacías o inválidas; no se descartan filas silenciosamente')
    return m


def elegir(comparacion):
    best = min(x['MAE'] for x in comparacion)
    priority = POLITICA['prioridad_empate']
    return min((x for x in comparacion if x['MAE'] <= best+EPS_EMPATE),key=lambda x:priority.index(x['candidato']))


def datos_combo(gold, tipo, horizonte):
    # Columnas futuras y discrepancias solo para evaluación; no entran al assembler.
    audit = [c for c in gold.columns if c.startswith('q_discrepancia') or c.startswith('f_q_discrepancia')]
    return (gold.where((F.col('horizonte_min')==horizonte) & F.col('elegible_gold_'+tipo))
        .select('station_id','captura_utc','f_captura_utc','particion_temporal','horizonte_min',
                'estrato_discrepancia',*FEATURES,*audit,F.col('y_'+tipo).cast('double').alias('label'))
        .withColumn('cambio_objetivo',F.when(F.col('label') != F.col(tipo+'_actual'),'cambia').otherwise('igual')))


def comparar_validacion(gold_train_valid, destino):
    """No acepta prueba y guarda selección/artefactos ANTES de su evaluación."""
    if gold_train_valid.where(~F.col('particion_temporal').isin('train','validacion')).limit(1).count():
        raise ValueError('La comparación no acepta filas de prueba')
    destino = Path(destino)
    destino.mkdir(parents=True,exist_ok=False)
    inicio = ahora()
    guardar_json(destino/'politica_previa.json',{'version':VERSION,'politica':POLITICA,'candidatos':CANDIDATOS,'predictores':FEATURES})
    studies, comparativa = [], []
    for tipo in ('convencionales','electricas'):
        for h in (5,10,15):
            print(f'Comparación {tipo} {h} min',flush=True)
            combo = datos_combo(gold_train_valid,tipo,h)
            train = combo.where("particion_temporal='train'").persist(StorageLevel.MEMORY_AND_DISK)
            valid = combo.where("particion_temporal='validacion'").persist(StorageLevel.MEMORY_AND_DISK)
            t0 = perf_counter()
            prep, preinfo = ajustar_preproceso(train)
            preinfo['duracion_s'] = perf_counter()-t0
            tr = prep.transform(train).select('label','features').persist(StorageLevel.MEMORY_AND_DISK)
            va = prep.transform(valid).select('label','features').persist(StorageLevel.MEMORY_AND_DISK)
            ntrain,nvalid = tr.count(),va.count()
            preinfo['nulos_validacion'] = valid.agg(*[F.sum((F.col(c).isNull() | F.isnan(c)).cast('long')).alias(c)
                for c in FEATURES]).first().asDict()
            # Estación ilustrativa elegida únicamente en train, por cobertura y desempate ID.
            estacion = train.groupBy('station_id').count().orderBy(F.desc('count'),'station_id').first()['station_id']
            fitted, comparison = {}, []
            for candidate in CANDIDATOS:
                t0 = perf_counter()
                cid = candidate['id']
                if candidate['familia']=='persistencia':
                    model, pipe = None,None
                    fit_s = 0.0
                    pred = predecir(valid,None,tipo+'_actual')
                else:
                    estimator = (LinearRegression if candidate['familia']=='lr' else RandomForestRegressor)(
                        labelCol='label',featuresCol='features',predictionCol='prediction',**candidate['params'])
                    model = estimator.fit(tr)
                    fit_s = perf_counter()-t0
                    pipe = PipelineModel(stages=prep.stages+[model])
                    # Conservar candidatos evita perder pesos aunque gane persistencia.
                    pipe.write().save(str(destino/'candidatos'/f'{tipo}_{h}min'/cid/'pipeline'))
                    if candidate['familia']=='rf':
                        guardar_json(destino/'interpretacion'/f'{tipo}_{h}min'/(cid+'.json'),{
                            'tipo':tipo,'horizonte_min':h,'candidato':cid,'datos':'solo train',
                            'metodo':'reduccion de impureza; asociación del modelo, no causalidad',
                            'importancias':[{'variable_vector':name,'importancia':float(value)}
                                for name,value in zip(preinfo['orden_vector'],model.featureImportances.toArray())]})
                    # va ya está transformado por el preprocesador ajustado con train.
                    pred = acotar(model.transform(va).withColumnRenamed('prediction','prediction_raw'))
                eval0 = perf_counter()
                score = metricas(pred)
                assert score['n']==nvalid
                record = {'tipo':tipo,'horizonte_min':h,'candidato':cid,**score,
                    'familia':candidate['familia'],'parametros':candidate['params'],
                    'n_train':ntrain,'duracion_ajuste_s':fit_s,'duracion_validacion_s':perf_counter()-eval0}
                comparison.append(record);comparativa.append(record);fitted[cid]=pipe
                guardar_json(destino/'validacion_parcial.json',{'estado':'comparacion_en_curso','resultados':comparativa})
                print(f"  {cid}: MAE validación={score['MAE']:.8f}",flush=True)
            winner = elegir(comparison)
            key = f'{tipo}_{h}min'
            artifact = destino/'seleccionados'/key
            artifact.mkdir(parents=True)
            pipe = fitted[winner['candidato']]
            contract = {'version':VERSION,'tipo':tipo,'horizonte_min':h,'candidato':winner['candidato'],
                'entrenamiento_necesario':pipe is not None,'politica':POLITICA,
                'columna_actual':tipo+'_actual','campos_requeridos':preinfo['predictores'] if pipe else [tipo+'_actual'],
                'preprocesamiento':preinfo if pipe else None,'orden_vector':preinfo['orden_vector'] if pipe else [],
                'parametros':winner['parametros'],'metricas_validacion':winner,
                'spark_version':train.sparkSession.version,
                'restricciones_entrada':'actual numérico no nulo y >=0; resto nullable con mediana aprendida en train; sin campos futuros',
                'objetivo':'conteo reportado por tipo; exactitud física no confirmada'}
            if pipe is not None:
                contract['pipeline_relativo']=f'../../candidatos/{key}/{winner["candidato"]}/pipeline'
            guardar_json(artifact/'contrato.json',contract)
            best_ml = elegir([r for r in comparison if r['familia']!='persistencia'])
            reference = {**contract,'candidato':best_ml['candidato'],'entrenamiento_necesario':True,
                'rol':'mejor MLlib por validación; no reemplaza al ganador global',
                'es_ganador_global':best_ml['candidato']==winner['candidato'],
                'campos_requeridos':preinfo['predictores'],'preprocesamiento':preinfo,
                'orden_vector':preinfo['orden_vector'],'parametros':best_ml['parametros'],
                'metricas_validacion':best_ml,
                'pipeline_relativo':f'../../candidatos/{key}/{best_ml["candidato"]}/pipeline'}
            guardar_json(destino/'referencias_mllib'/key/'contrato.json',reference)
            studies.append({'tipo':tipo,'h':h,'models':fitted,'ganador':winner['candidato'],
                'mejor_mllib':best_ml['candidato'],
                'estacion_ejemplo':estacion,'preprocesamiento':preinfo,'comparacion':comparison,
                'n_train':ntrain,'n_validacion':nvalid,'artefacto':str(artifact)})
            tr.unpersist();va.unpersist();train.unpersist();valid.unpersist()
    frozen = {'version':VERSION,'inicio_utc':inicio,'congelada_utc':ahora(),'politica':POLITICA,
        'comparativa_validacion':comparativa,
        'selecciones':[{k:v for k,v in s.items() if k not in ('models','comparacion')} for s in studies]}
    guardar_json(destino/'seleccion_congelada.json',frozen)
    guardar_json(destino/'validacion_parcial.json',{'estado':'seleccion_congelada','resultados':comparativa})
    return {'studies':studies,'frozen':frozen,'freeze_sha256':hashlib.sha256((destino/'seleccion_congelada.json').read_bytes()).hexdigest()}


def cargar_seleccion(artefacto):
    artifact = Path(artefacto)
    contract = json.loads((artifact/'contrato.json').read_text())
    model = PipelineModel.load(str(artifact/contract.get('pipeline_relativo','pipeline'))) if contract['entrenamiento_necesario'] else None
    return model,contract


def svg_observado(df, path, titulo):
    """Media de predicción por valor observado; máximo 200 agregados al driver."""
    points = df.groupBy('label').agg(F.avg('prediction').alias('media_predicha'),F.count('*').alias('n')).orderBy('label').limit(201).collect()
    if len(points)>200:
        raise ValueError('Demasiados valores para el gráfico acotado')
    maximum = max([max(float(r.label),float(r.media_predicha)) for r in points] or [1]) or 1
    content = [f'<svg xmlns="http://www.w3.org/2000/svg" width="650" height="420"><rect width="100%" height="100%" fill="white"/>',
        f'<text x="20" y="25" font-size="15">{escape(titulo)}</text>',
        '<path d="M60 50 V350 H600 M60 350 L600 50" stroke="#aaa" fill="none"/>']
    for r in points:
        x=60+float(r.label)/maximum*540;y=350-float(r.media_predicha)/maximum*300
        content.append(f'<circle cx="{x}" cy="{y}" r="3" fill="#2563eb"><title>Observado {r.label}; media predicha {r.media_predicha}; n={r.n}</title></circle>')
    content += [f'<text x="60" y="385">Observado →; media predicha ↑; escala 0–{maximum:.2f}</text>',
        '<text x="60" y="405">Línea gris: identidad. Predicciones sin redondear.</text></svg>']
    Path(path).write_text('\n'.join(content),encoding='utf-8')


def evaluar_prueba(gold_prueba, study, destino):
    """Evalúa todos los candidatos ya congelados; jamás ajusta ni selecciona."""
    destino=Path(destino)
    assert hashlib.sha256((destino/'seleccion_congelada.json').read_bytes()).hexdigest()==study['freeze_sha256']
    if gold_prueba.where(F.col('particion_temporal')!='prueba').limit(1).count():
        raise ValueError('Se esperaban solo filas de prueba')
    results,subgroups,stations,reloads,reference_reloads=[],[],[],[],[]
    start = ahora()
    for s in study['studies']:
        tipo,h=s['tipo'],s['h'];actual=tipo+'_actual';key=f'{tipo}_{h}min'
        test=datos_combo(gold_prueba,tipo,h).persist(StorageLevel.MEMORY_AND_DISK)
        ntest=test.count()
        assert ntest>0
        chosen=None
        for candidate in CANDIDATOS:
            cid=candidate['id'];t0=perf_counter()
            keep=[c for c in test.columns if c not in FEATURES or c==actual]
            pred=predecir(test,s['models'][cid],actual).select(*keep,'prediction_raw','prediction').persist(StorageLevel.MEMORY_AND_DISK)
            scores=metricas(pred)
            assert scores['n']==ntest
            results.append({'tipo':tipo,'horizonte_min':h,'candidato':cid,'seleccionado':cid==s['ganador'],
                **scores,'duracion_prueba_s':perf_counter()-t0})
            # Estratos y cambio/igual para todos: mismas filas por combinación.
            for dim in ('estrato_discrepancia','cambio_objetivo'):
                for row in tabla_metricas(pred,[dim]).collect():
                    subgroups.append({'tipo':tipo,'horizonte_min':h,'candidato':cid,'dimension':dim,**row.asDict()})
            if cid in {s['ganador'],'persistencia'}:
                per_station=tabla_metricas(pred,['station_id'])
                path=destino/'estaciones'/key/cid
                per_station.write.mode('errorifexists').parquet(str(path))
                macro=per_station.agg(F.count('*').alias('estaciones'),F.avg('MAE').alias('MAE_macro'),
                    F.percentile_approx('MAE',[0.5,0.95]).alias('p50_p95_MAE_estacion')).first().asDict()
                worst=[r.asDict() for r in per_station.where('n >= 10').orderBy(F.desc('MAE'),'station_id').limit(10).collect()]
                stations.append({'tipo':tipo,'horizonte_min':h,'candidato':cid,**macro,'mayor_error_n_ge10':worst,'parquet':str(path)})
            if cid==s.get('mejor_mllib'):
                model,contract=cargar_seleccion(destino/'referencias_mllib'/key)
                recovered=predecir(test,model,contract['columna_actual'])
                keys=['station_id','captura_utc','horizonte_min']
                delta=(pred.select(*keys,'prediction').join(recovered.select(*keys,F.col('prediction').alias('recargada')),keys,'full')
                    .agg(F.count('*').alias('n'),F.sum((F.col('prediction').isNull()|F.col('recargada').isNull()).cast('long')).alias('nulos'),
                         F.max(F.abs(F.col('prediction')-F.col('recargada'))).alias('max_diferencia')).first().asDict())
                assert delta['n']==ntest and delta['nulos']==0 and delta['max_diferencia']<=1e-10
                reference_reloads.append({'tipo':tipo,'horizonte_min':h,'candidato':cid,**delta})
            if cid==s['ganador']:
                model,contract=cargar_seleccion(s['artefacto'])
                recovered=predecir(test,model,contract['columna_actual'])
                keys=['station_id','captura_utc','horizonte_min']
                delta=(pred.select(*keys,'prediction').join(recovered.select(*keys,F.col('prediction').alias('recargada')),keys,'full')
                    .agg(F.count('*').alias('n'),F.sum((F.col('prediction').isNull()|F.col('recargada').isNull()).cast('long')).alias('nulos'),
                         F.max(F.abs(F.col('prediction')-F.col('recargada'))).alias('max_diferencia')).first().asDict())
                assert delta['n']==ntest and delta['nulos']==0 and delta['max_diferencia']<=1e-10
                reloads.append({'tipo':tipo,'horizonte_min':h,**delta})
                pred.select(*keys,'f_captura_utc','label',actual,'prediction_raw','prediction',
                    'estrato_discrepancia','cambio_objetivo',*[c for c in pred.columns if c.startswith('q_discrepancia')
                    or c.startswith('f_q_discrepancia')]).write.mode('errorifexists').parquet(str(destino/'predicciones'/key))
                svg_observado(pred,destino/(key+'_observado.svg'),f'{tipo} {h} min: {cid} (prueba experimental)')
                # Ejemplo temporal elegido por cobertura de train, no por error en prueba.
                sample=[r.asDict() for r in pred.where(F.col('station_id')==s['estacion_ejemplo']).select(
                    'captura_utc','f_captura_utc','label','prediction',actual).orderBy('captura_utc').limit(200).collect()]
                guardar_json(destino/(key+'_ejemplo_estacion.json'),{'station_id':s['estacion_ejemplo'],'seleccion':'cobertura de train','observaciones':sample})
            pred.unpersist()
        test.unpersist()
    assert hashlib.sha256((destino/'seleccion_congelada.json').read_bytes()).hexdigest()==study['freeze_sha256']
    return {'lectura_prueba_utc':start,'fin_utc':ahora(),'metricas_prueba':results,'subgrupos_prueba':subgroups,
            'resumen_estaciones':stations,'recarga_verificada':reloads,'recarga_referencias_mllib':reference_reloads,
            'seleccion_modificada_tras_prueba':False}
