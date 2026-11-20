"""Orquestación U1 en el contenedor existente; sin acceso remoto implícito."""
import argparse
from contextlib import contextmanager
import csv
from datetime import datetime
import json
from pathlib import Path
import platform
import shutil
from time import perf_counter

from .ejecucion import (ROOT, VERSION, FUENTES, ahora, guardar, sha, huella, inventario,
                        codigo, artefactos, compatible, comprobar_artefactos,
                        configuracion, crear_spark)


def exigir(condition, message):
    if not condition:
        raise AssertionError(message)


def csv_tabla(path, rows):
    keys = list(dict.fromkeys(k for r in rows for k in r))
    with Path(path).open('w', newline='') as stream:
        writer = csv.DictWriter(stream,fieldnames=keys)
        writer.writeheader();writer.writerows(rows)


def recargar_y_verificar(spark, fuente, manifest):
    """Comprueba cada predicción elegida, sus claves y el resumen por estación."""
    from pyspark.sql import functions as F
    from .modelado import datos_combo, cargar_seleccion, predecir, metricas
    root = Path(fuente)
    gold = spark.read.parquet(str(root/'gold/aceptados')).where("particion_temporal='prueba'")
    selected = json.loads((root/'modelos/seleccion_congelada.json').read_text())
    result = json.loads((root/'modelos/resultado.json').read_text())
    proofs = []
    for selection in selected['selecciones']:
        tipo,h = selection['tipo'],selection['h']
        key = f'{tipo}_{h}min'
        model, contract = cargar_seleccion(root/'modelos/seleccionados'/key)
        original = spark.read.parquet(str(root/'modelos/predicciones'/key))
        recovered = predecir(datos_combo(gold,tipo,h),model,contract['columna_actual'])
        keys = ['station_id','captura_utc','horizonte_min']
        n = original.count()
        exigir(n>0 and recovered.count()==n, 'Recarga: cardinalidad diferente')
        for df in (original,recovered):
            exigir(not df.groupBy(*keys).count().where('count > 1').limit(1).count(), 'Recarga: claves duplicadas')
        delta = (original.select(*keys,'prediction').join(recovered.select(*keys,F.col('prediction').alias('recargada')),keys,'full')
            .agg(F.count('*').alias('n'),F.sum((F.col('prediction').isNull() | F.col('recargada').isNull()).cast('long')).alias('nulos'),
                 F.max(F.abs(F.col('prediction')-F.col('recargada'))).alias('max_diferencia')).first().asDict())
        exigir(delta['n']==n and delta['nulos']==0 and delta['max_diferencia']<=1e-10, 'Recarga no reproduce predicciones')
        m = metricas(original)
        reference = next(r for r in result['metricas_prueba'] if r['tipo']==tipo and r['horizonte_min']==h and r['seleccionado'])
        exigir(m['n']==reference['n'] and abs(m['MAE']-reference['MAE'])<=1e-10,'Métricas no reproducibles')
        station = spark.read.parquet(str(root/'modelos/estaciones'/key/contract['candidato']))
        summary = station.agg(F.sum('n').alias('n'),(F.sum(F.col('n')*F.col('MAE'))/F.sum('n')).alias('MAE')).first()
        exigir(summary.n==n and abs(summary.MAE-m['MAE'])<=1e-10,'Conteos/métricas por estación no reconcilian')
        proofs.append({'tipo':tipo,'horizonte_min':h,**delta,'MAE_reproducido':m['MAE']})
    exigir(len(proofs)==6,'Faltan artefactos seleccionados')
    return proofs


def ejecutar(run, nueva=False):
    from pyspark.sql import functions as F
    from pyspark import StorageLevel
    from . import calidad, preparacion, modelado
    from .lectura import leer_fuente
    if not __debug__:
        raise RuntimeError('No ejecutar Python con -O: desactivaría verificaciones anteriores')
    run = Path(run).resolve()
    manifest = json.loads((run/'manifiesto.json').read_text())
    cfg = configuracion(run/'configuracion.json')
    t0 = perf_counter()
    spark = None
    manifest.update({'version':VERSION,'estado':'en_curso','etapas':[], 'errores':[]})

    @contextmanager
    def etapa(nombre):
        item = {'nombre':nombre,'estado':'en_curso','inicio_utc':ahora()}
        manifest['etapas'].append(item)
        guardar(run/'manifiesto.json',manifest)
        print('Etapa:',nombre,flush=True)
        start = perf_counter()
        try:
            yield item
            item['estado']='completada'
        except BaseException:
            item['estado']='fallida'
            raise
        finally:
            item['fin_utc']=ahora();item['duracion_s']=perf_counter()-start
            guardar(run/'manifiesto.json',manifest)

    try:
        spark = crear_spark(cfg)
        spark.sparkContext.setLogLevel('WARN')
        manifest['spark_master_efectivo'] = spark.sparkContext.master
        software = {'spark':spark.version,'python':platform.python_version(),
            'java':spark.sparkContext._jvm.java.lang.System.getProperty('java.version'),
            'docker_imagen_id':manifest['docker_imagen_id']}
        manifest['versiones'] = {'integracion':VERSION,'silver':calidad.VERSION,'gold':preparacion.VERSION,'modelado':modelado.VERSION,**software}
        with etapa('inventario_local'):
            inputs = inventario(ROOT/'data/raw',cfg)
            code = codigo()
            fingerprint = huella({'entradas':inputs,'configuracion':cfg,'codigo':code,'software':software})
            manifest.update({'huella_compatibilidad':fingerprint,'entradas':inputs,'codigo_sha256':code,
                             'configuracion':cfg,'archivos_raw':len(inputs)})
            old = None if nueva else compatible(run.parent,fingerprint)
        if old:
            source, previous = old
            manifest.update({'modo':'reutilizada','ejecucion_fuente':source.name,
                'linaje':previous['linaje'],'conteos':previous['conteos'],'metricas':previous['metricas']})
            with etapa('verificar_reutilizacion'):
                manifest['recarga_verificada'] = recargar_y_verificar(spark,source,previous)
                comprobar_artefactos(source,previous['artefactos'])
        else:
            manifest['modo']='nueva'
            exigir(shutil.disk_usage(run).free > 2*1024**3,'Se requieren al menos 2 GiB libres para esta ejecución')
            manifest['linaje'] = {'raw':'data/raw','silver':str(run/'silver'),'gold':str(run/'gold'),
                'modelos':str(run/'modelos'),'silver_ejecucion':run.name,'gold_ejecucion':run.name,'modelos_ejecucion':run.name}
            with etapa('lectura') as step:
                tables = {}
                sources = []
                for source in FUENTES:
                    files = [ROOT/'data/raw'/i['ruta'] for i in inputs if i['fuente']==source]
                    feed,rows = leer_fuente(spark,ROOT/'data/raw',source,archivos=files)
                    feed = feed.persist(StorageLevel.MEMORY_AND_DISK)
                    array = 'vehicle_types' if source=='vehicle_types' else 'stations'
                    exigir(feed.count()==len(files), 'Número de feeds no coincide con inventario')
                    exigir(not feed.where(F.col('_corrupt_record').isNotNull() | F.col('captura_utc').isNull() |
                        F.col('data.'+array).isNull() | (F.size('data.'+array)==0)).limit(1).count(),
                        'Feed corrupto, sin timestamp o sin registros: '+source)
                    # Se respeta fin_exclusivo también en lectura; no hay contexto futuro externo.
                    rows = rows.where(F.col('captura_utc') < F.to_timestamp(F.lit(cfg['fin_exclusivo_utc'])))
                    tables[source] = rows.persist(StorageLevel.DISK_ONLY)
                    sources.append({'fuente':source,'archivos':len(files),'registros':tables[source].count()})
                    feed.unpersist()
                manifest['fuentes']=sources
                status = tables['station_status']
                step['cobertura'] = status.agg(F.count('*').alias('registros'),F.countDistinct('station_id').alias('estaciones'),
                    F.min('captura_utc').alias('inicio'),F.max('captura_utc').alias('fin')).first().asDict()
                exigir(step['cobertura']['registros']>0,'Periodo sin estados')
            with etapa('calidad_silver') as step:
                quality = calidad.aplicar_reglas(status,tables['vehicle_types'],max_edad_s=cfg['max_edad_s']).persist(StorageLevel.DISK_ONLY)
                accepted,rejected,reconciliation = calidad.guardar_y_verificar(quality,run/'silver')
                step['reconciliacion']=reconciliation
                manifest['conteos']={'silver':reconciliation}
                quality.unpersist()
                silver = accepted.unionByName(rejected)
                for df in tables.values():
                    df.unpersist()
            with etapa('demostracion_rdd_y_lazy'):
                from .diagnostico import demostrar_rdd_y_ejecucion
                manifest['rdd_y_lazy']=demostrar_rdd_y_ejecucion(silver,run/'analisis')
            with etapa('preparacion_gold') as step:
                variables, features = preparacion.construir_variables(silver,hueco_s=cfg['hueco_max_s'],tolerancia_s=cfg['tolerancia_s'])
                exigir(features==modelado.FEATURES,'Contrato de predictores diferente al de modelado')
                variables = variables.persist(StorageLevel.DISK_ONLY)
                manifest['analisis']=preparacion.explorar(silver,variables)
                guardar(run/'analisis/resumen.json',manifest['analisis'])
                preparacion.grafico_svg(run/'analisis/cobertura.svg',manifest['analisis']['cobertura_fecha'],
                    'fecha_captura_utc',['observaciones','aceptados'],'Cobertura UTC: originales y elegibles')
                preparacion.grafico_svg(run/'analisis/por_hora.svg',manifest['analisis']['por_hora'],
                    'hora_local',['convencionales_media','electricas_media'],'Conteos reportados por hora local (descriptivo)')
                gold = preparacion.construir_gold(variables,cfg['corte_validacion_utc'],cfg['corte_prueba_utc'],cfg['fin_exclusivo_utc'],
                    tolerancia_s=cfg['tolerancia_s'],inicio_modelado=cfg.get('inicio_modelado_utc')).persist(StorageLevel.DISK_ONLY)
                preparacion.verificar_gold(gold,features,cfg['tolerancia_s'])
                n = gold.count()
                exigir(n==reconciliation['entrada']*3,'Gold perdió o duplicó emisiones/horizontes antes de calidad')
                counts = {}
                counts_by_date = {}
                for flag,name in ((True,'aceptados'),(False,'excluidos')):
                    part = gold.where(F.col('aceptado_gold')==flag)
                    expected = part.count()
                    counts_by_date[name]=[r.asDict() for r in part.groupBy('fecha_captura_utc').count().collect()]
                    part.write.mode('errorifexists').partitionBy('fecha_captura_utc','horizonte_min').parquet(str(run/'gold'/name))
                    back = (spark.read if expected else spark.read.schema(gold.schema)).parquet(str(run/'gold'/name))
                    exigir(back.count()==expected,'Gold: conteos de vuelta diferentes')
                    exigir({f.name:f.dataType.simpleString() for f in back.schema}=={f.name:f.dataType.simpleString() for f in gold.schema},'Gold: esquema diferente')
                    exigir(not part.exceptAll(back.select(part.columns)).limit(1).count() and
                            not back.select(part.columns).exceptAll(part).limit(1).count(),'Gold: filas diferentes tras Parquet')
                    counts[name]=expected
                exigir(sum(counts.values())==n,'Gold no reconcilia')
                step['conteos']={'candidatos':n,**counts}
                manifest['conteos']['gold']=step['conteos']
                manifest['conteos']['gold_por_fecha']=counts_by_date
                manifest['predictores']=features
                gold.unpersist();variables.unpersist()
                gold_ok = spark.read.parquet(str(run/'gold/aceptados')).persist(StorageLevel.DISK_ONLY)
                preparacion.verificar_gold(gold_ok,features,cfg['tolerancia_s'])
                date=(cfg.get('inicio_modelado_utc') or cfg['corte_validacion_utc'])[:10]
                # Una lectura nueva de la misma ruta también puede reutilizar la
                # caché. Liberarla explícitamente antes de comprobar pruning.
                gold_ok.unpersist(blocking=True)
                scan=spark.read.parquet(str(run/'gold/aceptados')).where(F.col('fecha_captura_utc')==F.lit(date).cast('date'))
                plan=scan._jdf.queryExecution().executedPlan().toString()
                filters=plan.split('PartitionFilters:',1)[-1].split('PushedFilters:',1)[0]
                exigir('InMemoryTableScan' not in plan and date in filters and 'fecha_captura_utc' in filters,
                    'Falta filtro físico de fecha sobre Parquet; no sirve filtrar solo una caché')
                (run/'analisis/plan_gold_fecha.txt').write_text(plan)
                expected_date=sum(r['count'] for r in counts_by_date['aceptados'] if str(r['fecha_captura_utc'])==date)
                exigir(scan.count()==expected_date,'El filtro de fecha no reconcilia con Gold antes de escribir')
                gold_ok.persist(StorageLevel.DISK_ONLY)
                combos=[]
                for tipo in ('convencionales','electricas'):
                    rows = gold_ok.where('elegible_gold_'+tipo).groupBy('horizonte_min','particion_temporal').count().collect()
                    exigir(len(rows)==9 and all(r['count']>0 for r in rows),'Faltan combinaciones de train/validación/prueba')
                    combos += [{'tipo':tipo,**r.asDict()} for r in rows]
                manifest['conteos']['por_tipo_horizonte_particion']=combos
                guardar(run/'gold/entradas.json',{'ejecucion':run.name,'silver':str(run/'silver'),'version':preparacion.VERSION,
                    'configuracion':cfg,'predictores':features,'conteos':manifest['conteos']['gold'],'desglose':combos})
            with etapa('entrenamiento_comparacion_seleccion'):
                study = modelado.comparar_validacion(gold_ok.where("particion_temporal IN ('train','validacion')"),run/'modelos')
                manifest['seleccion_sha256']=study['freeze_sha256']
            with etapa('evaluacion_prueba'):
                result = modelado.evaluar_prueba(gold_ok.where("particion_temporal='prueba'"),study,run/'modelos')
                # Cada dimensión de evaluación es una partición disjunta del mismo N.
                for m in result['metricas_prueba']:
                    for dimension in ('estrato_discrepancia','cambio_objetivo'):
                        group = [s for s in result['subgrupos_prueba'] if s['tipo']==m['tipo'] and s['horizonte_min']==m['horizonte_min']
                                 and s['candidato']==m['candidato'] and s['dimension']==dimension]
                        exigir(sum(s['n'] for s in group)==m['n'],'Subgrupos no reconcilian')
                result.update({'version':modelado.VERSION,'ejecucion':run.name,'gold_fuente':str(run/'gold'),
                    'gold_version':preparacion.VERSION,'configuracion':cfg,'seleccion_sha256':study['freeze_sha256'],
                    'comparativa_validacion':study['frozen']['comparativa_validacion']})
                modelado.guardar_json(run/'modelos/resultado.json',result)
                for key,name in [('comparativa_validacion','comparacion_validacion.csv'),('metricas_prueba','comparacion_prueba.csv'),
                                 ('subgrupos_prueba','subgrupos_prueba.csv')]:
                    csv_tabla(run/'modelos'/name,result[key])
                manifest['metricas']=[r for r in result['metricas_prueba'] if r['seleccionado']]
                manifest['ajustes_mllib']=6*sum(c['familia']!='persistencia' for c in modelado.CANDIDATOS)
                manifest['recarga_referencias_mllib']=result['recarga_referencias_mllib']
                manifest['selecciones']=[{'tipo':s['tipo'],'horizonte_min':s['h'],'candidato':s['ganador'],
                    'requiere_entrenamiento':s['ganador']!='persistencia'} for s in study['studies']]
                gold_ok.unpersist()
            with etapa('verificacion_final'):
                manifest['recarga_verificada']=recargar_y_verificar(spark,run,manifest)
                exigir(sha(run/'modelos/seleccion_congelada.json')==manifest['seleccion_sha256'],'Selección alterada tras prueba')
            manifest['artefactos']=artefactos(run)
        with etapa('raw_intacto'):
            exigir(inputs==inventario(ROOT/'data/raw',cfg),'Raw cambió durante la ejecución')
            exigir(code==codigo(),'Código cambió durante la ejecución; resultado no reproducible')
        manifest['estado']='completada'
        manifest['fin_utc']=ahora();manifest['duracion_s']=perf_counter()-t0
        guardar(run/'manifiesto.json',manifest)
        print('U1 completada:',manifest['modo'],run.name,flush=True)
        return manifest
    except BaseException as exc:
        manifest.update({'estado':'fallida','fin_utc':ahora(),'duracion_s':perf_counter()-t0})
        manifest['errores'].append({'tipo':type(exc).__name__,'mensaje':str(exc)[:2000]})
        guardar(run/'manifiesto.json',manifest)
        raise
    finally:
        if spark:
            spark.stop()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ejecucion',required=True,type=Path)
    parser.add_argument('--nueva',action='store_true')
    args=parser.parse_args()
    ejecutar(args.ejecucion,args.nueva)
