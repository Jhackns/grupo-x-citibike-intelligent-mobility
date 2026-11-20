"""Adaptación de contrato Kafka a la función batch U1 sin modificarla."""
import json
from pyspark.sql import functions as F, types as T
from .contrato import validar_evento


CATALOGO_DDL='vehicle_type_id string, form_factor string, propulsion_type string'
CATALOGO_U1=[{'vehicle_type_id':'1','form_factor':'bicycle','propulsion_type':'human'},
             {'vehicle_type_id':'2','form_factor':'bicycle','propulsion_type':'electric_assist'}]


def schema_evento(u1):
    estado=u1['modulos']['lectura'].esquema('station_status')['data'].dataType['stations'].dataType.elementType
    cat=T.StructType.fromDDL('catalog_id string, feed_url string, gbfs_version string, captured_at_utc string, '
        'last_updated_original bigint, last_updated_utc string, ttl bigint, archivo_relativo string')
    return T.StructType([
        T.StructField(k,T.StringType()) for k in ('schema_version','event_id','capture_id','station_id','captured_at_utc',
                                                 'last_updated_utc','last_reported_utc')]+[
        T.StructField('last_updated_original',T.LongType()),
        T.StructField('source',T.StructType.fromDDL('system string, gbfs_index_url string, feed_url string, gbfs_version string')),
        T.StructField('station',estado),T.StructField('catalogs',T.StructType([
            T.StructField('vehicle_types',cat),T.StructField('station_information',cat)])),
        T.StructField('vehicle_types_catalog',T.ArrayType(T.StructType.fromDDL(CATALOGO_DDL))),
        T.StructField('u1_catalog_compatible',T.BooleanType()),T.StructField('ingestion_issues',T.ArrayType(T.StringType()))])


def errores_json(raw):
    try:
        x=json.loads(raw,parse_constant=lambda _:(_ for _ in ()).throw(ValueError()))
        validar_evento(x)
        return []
    except Exception as exc:return ['contrato_invalido:'+type(exc).__name__]


def normalizar(raw,u1,cfg):
    """raw tiene metadatos Kafka nombrados explícitamente, también en fixtures."""
    errors=F.udf(errores_json,T.ArrayType(T.StringType()),useArrow=False)
    e=raw.withColumn('_e',F.from_json('json_original',schema_evento(u1)))
    statefields=u1['modulos']['lectura'].esquema('station_status')['data'].dataType['stations'].dataType.elementType.names
    df=e.select('json_original','kafka_key','kafka_topic','kafka_partition','kafka_offset','kafka_timestamp','kafka_timestamp_type',
        F.col('_e.event_id').alias('event_id'),F.col('_e.capture_id').alias('capture_id'),
        F.col('_e.captured_at_utc').alias('captured_at_original'),F.col('_e.source').alias('source'),
        F.col('_e.catalogs').alias('catalogs'),F.col('_e.ingestion_issues').alias('ingestion_issues'),
        F.col('_e.last_updated_original').alias('last_updated'),
        F.col('_e.last_updated_utc').alias('last_updated_utc_original'),
        F.col('_e.last_reported_utc').alias('last_reported_utc_original'),
        F.col('_e.vehicle_types_catalog').alias('_catalog_data'),
        F.col('_e.catalogs.vehicle_types.catalog_id').alias('catalog_version'),
        *[F.col('_e.station.'+k).alias(k) for k in statefields])
    df=(df.withColumn('captura_utc',F.try_to_timestamp('captured_at_original'))
        .withColumn('last_updated_original_json',F.get_json_object('json_original','$.last_updated_original'))
        .withColumn('last_reported_original_json',F.get_json_object('json_original','$.last_reported_original'))
        .withColumn('errores_contrato',errors('json_original')))
    extra=F.filter(F.array(
        F.when(~F.coalesce(F.col('station_id')==F.col('kafka_key'),F.lit(False)),F.lit('clave_kafka_inconsistente')),
        F.when(F.col('captura_utc').isNull(),F.lit('captura_utc_invalida')),
        F.when(F.col('captura_utc').cast('double')>F.col('kafka_timestamp').cast('double')+cfg['max_future_clock_seconds'],
               F.lit('reloj_captura_futuro'))),lambda x:x.isNotNull())
    df=(df.withColumn('errores_contrato',F.array_union('errores_contrato',extra))
        .withColumn('contrato_valido',F.size('errores_contrato')==0)
        .withColumn('tiempo_evento',F.when(F.col('contrato_valido'),F.col('captura_utc')).otherwise(F.col('kafka_timestamp')))
        .withColumn('_clave_dedup',F.when(F.col('contrato_valido'),F.col('event_id')).otherwise(F.concat_ws(':',
            F.lit('kafka'),F.col('kafka_topic'),F.col('kafka_partition'),F.col('kafka_offset'))))
        .withColumn('archivo_origen',F.concat_ws(':',F.lit('kafka'),F.col('kafka_topic'),F.col('kafka_partition'),F.col('kafka_offset'))))
    return df


def deduplicar(df,watermark):
    return df.withWatermark('tiempo_evento',watermark).dropDuplicatesWithinWatermark(['_clave_dedup'])


def calidad_grupo(df,catalogo,u1):
    spark=df.sparkSession
    vehicles=spark.createDataFrame(catalogo,CATALOGO_DDL)
    result=u1['modulos']['calidad'].aplicar_reglas(df,vehicles,max_edad_s=u1['ref']['configuracion_origen']['max_edad_s'])
    semantics=sorted(catalogo,key=lambda r:str(r.get('vehicle_type_id')))==CATALOGO_U1
    result=result.withColumn('catalogo_modelo_compatible',F.lit(semantics))
    guard=F.when(~F.col('catalogo_modelo_compatible'),F.array(F.lit('catalogo_no_compatible_modelo_u1'))).otherwise(
        F.array().cast('array<string>'))
    for tipo in ('convencionales','electricas'):
        result=(result.withColumn('motivos_pred_'+tipo,F.array_union(F.array_union(
            F.col('motivos_no_elegible_'+tipo),F.col('errores_contrato')),guard))
            .withColumn('elegible_pred_'+tipo,F.size('motivos_pred_'+tipo)==0))
    return (result.withColumn('procesado_utc',F.current_timestamp())
        .withColumn('latencia_procesamiento_s',F.col('procesado_utc').cast('double')-F.col('captura_utc').cast('double'))
        .withColumn('alcance_diagnostico_mismo_reporte',F.lit('microbatch_y_version_catalogo')))


def aplicar_calidad(batch,u1,max_catalogs=16):
    groups=batch.select('catalog_version',F.to_json('_catalog_data').alias('catalog_json')).distinct().limit(max_catalogs+1).collect()
    if len(groups)>max_catalogs:raise ValueError('Demasiadas versiones de catálogo en un microbatch')
    result=None
    for group in groups:
        subset=batch.where(F.col('catalog_version').eqNullSafe(group.catalog_version) &
            F.to_json('_catalog_data').eqNullSafe(group.catalog_json))
        catalog=json.loads(group.catalog_json) if group.catalog_json else []
        quality=calidad_grupo(subset,catalog,u1)
        result=quality if result is None else result.unionByName(quality)
    return result if result is not None else calidad_grupo(batch,CATALOGO_U1,u1)


def metricas_ventana(observaciones,cfg):
    discrepancy=F.coalesce(F.col('q_discrepancia_total'),F.lit(False)) | F.coalesce(
        F.col('q_discrepancia_electricas'),F.lit(False)) | F.coalesce(F.col('q_discrepancia_convencionales'),F.lit(False))
    return (observaciones.withWatermark('tiempo_evento',cfg['watermark'])
        .groupBy(F.window('tiempo_evento',cfg['window']))
        .agg(F.count('*').alias('observaciones'),
             F.sum(F.col('elegible_pred_convencionales').cast('long')).alias('elegibles_convencionales'),
             F.sum(F.col('elegible_pred_electricas').cast('long')).alias('elegibles_electricas'),
             F.sum((~F.col('contrato_valido')).cast('long')).alias('contratos_invalidos'),
             F.sum((~F.col('elegible_pred_convencionales') & ~F.col('elegible_pred_electricas')).cast('long')).alias('rechazo_total'),
             F.sum(discrepancy.cast('long')).alias('observaciones_discrepantes'),
             F.avg('latencia_procesamiento_s').alias('latencia_procesamiento_media_s'))
        .select(F.col('window.start').alias('ventana_inicio_utc'),F.col('window.end').alias('ventana_fin_utc'),
            'observaciones','elegibles_convencionales','elegibles_electricas','contratos_invalidos','rechazo_total',
            'observaciones_discrepantes','latencia_procesamiento_media_s'))
