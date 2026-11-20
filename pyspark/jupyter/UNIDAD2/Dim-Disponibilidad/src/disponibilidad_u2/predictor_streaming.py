"""Carga estricta de los seis contratos originales; inferencia de persistencia."""
import hashlib
import json
import types
from pathlib import Path
from pyspark.sql import functions as F
from .configuracion import ROOT


def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def cargar_u1(reference):
    path=(ROOT/reference).resolve();ref=json.loads(path.read_text());root=path.parent.parent
    meta=ref['manifiesto_origen'];mp=root/meta['ruta']
    if sha(mp)!=meta['sha256']:raise ValueError('Manifiesto U1 distinto de la referencia explícita')
    manifest=json.loads(mp.read_text())
    if manifest['id']!=ref['ejecucion_id'] or manifest['estado']!='completada':raise ValueError('Procedencia U1 inválida')
    if sha(root/ref['seleccion_congelada']['ruta'])!=ref['seleccion_congelada']['sha256']:raise ValueError('Selección U1 alterada')
    contracts=[]
    for item in ref['contratos']:
        file=root/item['ruta_original'];c=json.loads(file.read_text())
        if sha(file)!=item['sha256_original'] or c!=item['contrato']:raise ValueError('Contrato original U1 no corresponde')
        if (c['tipo'],c['horizonte_min'])!=(item['tipo'],item['horizonte_min']):raise ValueError('Combinación de contrato incorrecta')
        if c['candidato']!='persistencia' or c['entrenamiento_necesario'] or c['campos_requeridos']!=[c['tipo']+'_actual']:
            raise ValueError('Esta etapa solo admite la persistencia seleccionada, sin modelos alternativos automáticos')
        contracts.append({**c,'sha256':item['sha256_original'],'ruta_original':item['ruta_original']})
    if len(contracts)!=6 or {(c['tipo'],c['horizonte_min']) for c in contracts}!={(t,h) for t in ('convencionales','electricas') for h in (5,10,15)}:
        raise ValueError('Se requieren los seis contratos únicos')
    modules={}
    for name in ('calidad','lectura'):
        file=root/'src'/(name+'.py')
        if sha(file)!=manifest['codigo_sha256']['src/'+name+'.py']:raise ValueError('Lógica U1 no corresponde a la ejecución seleccionada')
        module=types.ModuleType('u1_'+name+'_verificado')
        # compile/exec evita escribir __pycache__ en U1, también montado read-only.
        exec(compile(file.read_text(),str(file),'exec'),module.__dict__)
        modules[name]=module
    if modules['calidad'].VERSION!=ref['versiones_origen']['silver']:raise ValueError('Versión de reglas distinta')
    return {'ref':ref,'manifest':manifest,'root':root,'contratos':contracts,'modulos':modules,'referencia_sha256':sha(path)}


def predecir(observaciones,u1):
    specs=[]
    for c in u1['contratos']:
        specs.append(F.struct(F.lit(c['tipo']).alias('tipo'),F.lit(c['horizonte_min']).alias('horizonte_min'),
            F.lit(c['version']).alias('version_predictor'),F.lit(c['sha256']).alias('contrato_sha256')))
    df=observaciones.withColumn('_p',F.explode(F.array(*specs)))
    df=(df.where(F.when(F.col('_p.tipo')=='convencionales',F.col('elegible_pred_convencionales'))
                   .otherwise(F.col('elegible_pred_electricas')))
        .withColumn('tipo',F.col('_p.tipo')).withColumn('horizonte_min',F.col('_p.horizonte_min'))
        .withColumn('version_predictor',F.col('_p.version_predictor'))
        .withColumn('contrato_sha256',F.col('_p.contrato_sha256'))
        .withColumn('valor_predicho',F.when(F.col('tipo')=='convencionales',F.col('objetivo_convencionales'))
            .otherwise(F.col('objetivo_electricas')).cast('double'))
        .withColumn('origen_utc',F.col('captura_utc'))
        .withColumn('objetivo_utc',F.expr('captura_utc + make_interval(0,0,0,0,0,horizonte_min,0)'))
        .withColumn('predictor',F.lit('persistencia'))
        .withColumn('ejecucion_u1',F.lit(u1['ref']['ejecucion_id']))
        .withColumn('prediccion_id',F.sha2(F.to_json(F.struct('event_id','tipo','horizonte_min','version_predictor','contrato_sha256')),256)))
    audit=[c for c in observaciones.columns if c.startswith('q_') or c.startswith('elegible_') or c.startswith('motivos_')]
    return df.select('prediccion_id','event_id','capture_id','station_id','origen_utc','objetivo_utc','tipo','horizonte_min',
        'valor_predicho','predictor','version_predictor','contrato_sha256','ejecucion_u1','catalog_version',
        'kafka_topic','kafka_partition','kafka_offset','kafka_timestamp','procesado_utc',*audit)
