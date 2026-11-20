"""Reglas conservadoras y auditables de Silver, sin modificar cantidades raw."""

from functools import reduce
from operator import or_
from pathlib import Path

from pyspark.sql import functions as F

VERSION = "silver-v2-tipos-gbfs"


def motivos(condiciones):
    """Array sin nulos; cada fila se cuenta una vez aunque tenga varios motivos."""
    return F.filter(F.array(*[F.when(F.coalesce(cond, F.lit(False)), F.lit(nombre))
                              for nombre, cond in condiciones.items()]), lambda x: x.isNotNull())


def firmas_estado(df):
    columnas = [c for c in df.columns if c.startswith('num_') or c in (
        'station_id', 'is_installed', 'is_renting', 'is_returning', 'last_reported', 'vehicle_types_available')]
    return df.withColumn('_firma', F.to_json(F.struct(*[
        F.sort_array(c).alias(c) if c == 'vehicle_types_available' else F.col(c) for c in columnas])))


def investigar(estados):
    """Tablas exploratorias sobre datos originales; no se deduplica la entrada."""
    df = firmas_estado(estados)
    por_captura = df.groupBy('station_id', 'captura_utc').agg(
        F.count('*').alias('filas'), F.countDistinct('_firma').alias('variantes'))
    por_reporte = df.groupBy('station_id', 'last_reported').agg(
        F.count('*').alias('filas'), F.countDistinct('captura_utc').alias('capturas'),
        F.countDistinct('_firma').alias('variantes'),
        *[F.countDistinct(F.to_json(F.struct(c))).alias('variantes_' + c)
          for c in ('num_bikes_available', 'num_ebikes_available', 'num_bikes_disabled',
                    'vehicle_types_available', 'is_installed', 'is_renting', 'is_returning')])
    edad = F.col('last_updated') - F.col('last_reported')
    edades = estados.groupBy('is_installed', 'is_renting', 'is_returning').agg(
        F.count('*').alias('filas'), F.percentile_approx(edad, [0.5, 0.95]).alias('p50_p95_edad_s'),
        *[F.sum(F.when(edad > n, 1).otherwise(0)).alias('mayor_' + str(n) + 's')
          for n in (450, 900, 86400)])
    return por_captura, por_reporte, edades


def perfilar_discrepancias(cantidades):
    """Distribuciones con denominadores; no confundir concentración con volumen."""
    df = (cantidades.withColumn('delta_total_tipos', F.col('num_bikes_available') -
        F.aggregate('vehicle_types_available', F.lit(0).cast('long'), lambda a, v: a + v['count']))
        .withColumn('hora_nueva_york', F.hour('captura_nueva_york'))
        .withColumn('edad_s', F.col('last_updated') - F.col('last_reported'))
        .withColumn('edad_grupo', F.when(F.col('edad_s').isNull(), 'ausente')
            .when(F.col('edad_s') < 0, 'futuro').when(F.col('edad_s') <= 450, '0_450s')
            .when(F.col('edad_s') <= 900, '451_900s').when(F.col('edad_s') <= 86400, '901_86400s')
            .otherwise('mayor_24h')))
    def agrupar(*keys):
        return df.groupBy(*keys).agg(F.count('*').alias('observaciones'),
            F.sum(F.when(F.col('delta_total_tipos') != 0, 1).otherwise(0)).alias('discrepancias'),
            F.sum(F.when(F.col('delta_total_tipos') > 0, 1).otherwise(0)).alias('positivas'),
            F.sum(F.when(F.col('delta_total_tipos') < 0, 1).otherwise(0)).alias('negativas')).withColumn(
                'porcentaje', F.round(100 * F.col('discrepancias') / F.col('observaciones'), 4))
    por_estacion = agrupar('station_id')
    def filas(frame):
        return [r.asDict(recursive=True) for r in frame.collect()]
    campos = ['station_id', 'archivo_origen', 'captura_utc', 'last_updated', 'last_reported',
              'num_bikes_available', 'vehicle_types_available', 'num_ebikes_available',
              'convencionales_catalogo', 'electricas_catalogo', 'delta_total_tipos']
    electricas = df.agg(*[F.sum(F.when(cond, 1).otherwise(0)).alias(key) for key, cond in {
        'escalar_ausente': F.col('num_ebikes_available').isNull(),
        'escalar_cero': F.col('num_ebikes_available') == 0,
        'tipo2_ausente': F.col('electricas_catalogo').isNull(),
        'tipo2_cero': F.col('electricas_catalogo') == 0,
        'ambas_presentes': F.col('num_ebikes_available').isNotNull() & F.col('electricas_catalogo').isNotNull(),
        'coinciden': F.col('num_ebikes_available') == F.col('electricas_catalogo'),
        'difieren': F.col('num_ebikes_available') != F.col('electricas_catalogo'),
        'escalar_mayor': F.col('num_ebikes_available') > F.col('electricas_catalogo'),
        'escalar_menor': F.col('num_ebikes_available') < F.col('electricas_catalogo'),
    }.items()]).first().asDict()
    return {'electricas': electricas,
            'por_hora_local': filas(agrupar('hora_nueva_york').orderBy('hora_nueva_york')),
            'por_estados': filas(agrupar('is_installed', 'is_renting', 'is_returning')),
            'por_antiguedad': filas(agrupar('edad_grupo').orderBy('edad_grupo')),
            'estaciones_con_discrepancias': por_estacion.where('discrepancias > 0').count(),
            'top_estaciones': filas(por_estacion.orderBy(F.desc('discrepancias'), 'station_id').limit(20)),
            'ejemplos_positivos': filas(df.where('delta_total_tipos > 0').select(*campos)
                                      .orderBy('captura_utc','station_id').limit(3)),
            'ejemplos_negativos': filas(df.where('delta_total_tipos < 0').select(*campos)
                                      .orderBy('captura_utc','station_id').limit(3))}


def aplicar_reglas(estados, vehiculos, max_edad_s=450):
    """Conteos observacionales por tipo GBFS. Ausentes permanecen nulos.

    La definición canónica es count del tipo GBFS; total y extensión eléctrica
    no sustituyen ese dato ni invalidan automáticamente otro objetivo. Mapping
    es una referencia semántica auditada, no una variable futura de entrenamiento.
    """
    if max_edad_s <= 0:
        raise ValueError('El umbral de frescura debe ser positivo')
    catalogo = vehiculos.select('vehicle_type_id', 'propulsion_type').distinct().collect()
    mapping = {}
    for row in catalogo:
        mapping.setdefault(row.vehicle_type_id, set()).add(row.propulsion_type)
    ambiguo = any(k is None or len(v) != 1 for k, v in mapping.items())
    ids = {p: [k for k, v in mapping.items() if v == {p}] for p in ('human', 'electric_assist')}
    df = firmas_estado(estados)
    cc = df.groupBy('station_id', 'captura_utc').agg(
        F.count('*').alias('filas_misma_captura'), F.countDistinct('_firma').alias('variantes_misma_captura'))
    cr = df.groupBy('station_id', 'last_reported').agg(
        F.countDistinct('_firma').alias('variantes_mismo_reporte'))
    df = df.join(cc, ['station_id', 'captura_utc'], 'left').join(cr, ['station_id', 'last_reported'], 'left')
    for propulsion, name in (('human', 'convencionales_tipo'), ('electric_assist', 'electricas_tipo')):
        subset = F.filter('vehicle_types_available', lambda v: v.vehicle_type_id.isin(ids[propulsion]))
        df = df.withColumn(name, F.when(F.lit(not ambiguo and bool(ids[propulsion])) &
            (F.size(subset) == len(ids[propulsion])) &
            (F.size(F.array_distinct(F.transform(subset, lambda v: v.vehicle_type_id))) == len(ids[propulsion])) &
            F.forall(subset, lambda v: v['count'].isNotNull() & (v['count'] >= 0)),
            F.aggregate(subset, F.lit(0).cast('long'), lambda a, v: a + v['count'])))
    df = (df.withColumn('regla_version', F.lit(VERSION))
        .withColumn('umbral_frescura_s', F.lit(max_edad_s))
        .withColumn('fecha_captura_utc', F.to_date('captura_utc'))
        .withColumn('edad_reporte_s', F.col('last_updated') - F.col('last_reported'))
        .withColumn('edad_feed_s', F.col('captura_utc').cast('double') - F.col('last_updated'))
        .withColumn('q_catalogo_ambiguo', F.lit(ambiguo))
        .withColumn('q_tipos_desconocidos', F.coalesce(F.exists('vehicle_types_available',
            lambda v: ~v.vehicle_type_id.isin(list(mapping)) | v.vehicle_type_id.isNull()), F.lit(True)))
        .withColumn('q_array_tipos_invalido', F.col('vehicle_types_available').isNull() |
            F.coalesce(F.exists('vehicle_types_available', lambda v: v['count'].isNull() | (v['count'] < 0)), F.lit(True)) |
            (F.size('vehicle_types_available') != F.size(F.array_distinct(
                F.transform('vehicle_types_available', lambda v: v.vehicle_type_id)))))
        .withColumn('q_discrepancia_total', F.aggregate('vehicle_types_available', F.lit(0).cast('long'),
            lambda a, v: a + v['count']) != F.col('num_bikes_available'))
        .withColumn('q_discrepancia_electricas', F.col('electricas_tipo') != F.col('num_ebikes_available'))
        .withColumn('q_discrepancia_convencionales', F.col('convencionales_tipo') !=
            F.col('num_bikes_available') - F.col('num_ebikes_available'))
        .withColumn('q_cantidad_negativa', F.coalesce(reduce(or_, [F.col(c) < 0
            for c in estados.columns if c.startswith('num_')]), F.lit(False)))
        .withColumn('q_conflicto_captura', F.coalesce(F.col('variantes_misma_captura') > 1, F.lit(False)))
        .withColumn('q_cambio_mismo_reporte', F.coalesce(F.col('variantes_mismo_reporte') > 1, F.lit(False)))
        .withColumn('q_reporte_anomalo_86400', F.col('last_reported') == 86400)
        .withColumn('q_reporte_antiguo', F.col('edad_reporte_s') > max_edad_s)
        .withColumn('q_estacion_inactiva', F.coalesce((F.col('is_installed') == 0) |
            (F.col('is_renting') == 0), F.lit(False)))
        .withColumn('q_devoluciones_cerradas', F.col('is_returning') == 0))
    invalidos = {
        'identidad_o_trazabilidad_ausente': F.col('station_id').isNull() | (F.trim('station_id') == '') |
            F.col('captura_utc').isNull() | F.col('archivo_origen').isNull() | (F.trim('archivo_origen') == ''),
        'tiempo_feed_invalido': F.col('last_updated').isNull() | (F.col('last_updated') <= 0) | (F.col('edad_feed_s') < 0),
        'estado_operativo_invalido': reduce(or_, [F.col(c).isNull() | ~F.col(c).isin(0, 1)
            for c in ('is_installed', 'is_renting', 'is_returning')]),
        'conflicto_dentro_captura': F.col('q_conflicto_captura'),
    }
    df = df.withColumn('motivos_invalidez', motivos(invalidos)).withColumn(
        'registro_invalido', F.size('motivos_invalidez') > 0)
    comunes = {'registro_invalido': F.col('registro_invalido'),
        'estacion_no_alquila': F.col('q_estacion_inactiva'),
        'tiempo_reporte_no_utilizable': F.col('last_reported').isNull() | (F.col('last_reported') <= 0) |
            F.col('q_reporte_anomalo_86400') | (F.col('edad_reporte_s') < 0),
        'reporte_antiguo': F.coalesce(F.col('q_reporte_antiguo'), F.lit(False)),
        'feed_antiguo': F.col('edad_feed_s') > max_edad_s,
        'catalogo_pendiente': F.lit(ambiguo)}
    conv = {**comunes, 'tipo_convencional_pendiente': F.lit(not ids['human']),
        'convencionales_ausentes': F.col('convencionales_tipo').isNull(),
        'conteo_convencional_invalido': F.col('convencionales_tipo') < 0}
    elec = {**comunes, 'tipo_electrico_pendiente': F.lit(not ids['electric_assist']),
        'electricas_ausentes': F.col('electricas_tipo').isNull(),
        'conteo_electrico_invalido': F.col('electricas_tipo') < 0}
    df = (df.withColumn('motivos_no_elegible_convencionales', motivos(conv))
        .withColumn('motivos_no_elegible_electricas', motivos(elec))
        .withColumn('elegible_convencionales', F.size('motivos_no_elegible_convencionales') == 0)
        .withColumn('elegible_electricas', F.size('motivos_no_elegible_electricas') == 0)
        .withColumn('objetivo_convencionales', F.when(F.col('elegible_convencionales'), F.col('convencionales_tipo')))
        .withColumn('objetivo_electricas', F.when(F.col('elegible_electricas'), F.col('electricas_tipo')))
        .withColumn('aceptado_silver', F.col('elegible_convencionales') | F.col('elegible_electricas'))
        .withColumn('tipo_registro', F.when(F.col('registro_invalido'), 'invalido')
            .when(F.col('q_estacion_inactiva'), 'inactivo').otherwise('activo'))
        .withColumn('motivos_exclusion', F.when(~F.col('aceptado_silver'), F.array_union(
            F.col('motivos_no_elegible_convencionales'), F.col('motivos_no_elegible_electricas')))
            .otherwise(F.array().cast('array<string>'))).drop('_firma'))
    return df


def guardar_y_verificar(df, destino):
    """Escritura nueva por ejecución, sin sobrescribir raw ni Silver anterior."""
    destino = Path(destino)
    if destino.exists():
        raise FileExistsError('El destino ya existe; usa un directorio nuevo de ejecución')
    counts = df.groupBy('aceptado_silver').count().collect()
    accepted = sum(r['count'] for r in counts if r.aceptado_silver)
    excluded = sum(r['count'] for r in counts if not r.aceptado_silver)
    for flag, name in ((True, 'aceptados'), (False, 'excluidos')):
        (df.where(F.col('aceptado_silver') == flag).write.mode('errorifexists')
            .partitionBy('fecha_captura_utc').parquet(str(destino / name)))
    # Un dataset vacío particionado puede no tener footer Parquet; usar schema
    # explícito solo en ese caso. Los no vacíos se verifican por inferencia de disco.
    spark = df.sparkSession
    def read(name, n):
        reader = spark.read if n else spark.read.schema(df.schema)
        return reader.parquet(str(destino / name))
    silver, rejected = read('aceptados', accepted), read('excluidos', excluded)
    a, b = silver.count(), rejected.count()
    assert (a, b) == (accepted, excluded)
    assert a + b == df.count()
    def tipos(schema):
        # Parquet relaja nullability; se comprueban nombres y tipos recursivos.
        return {field.name: field.dataType.simpleString() for field in schema}
    assert tipos(silver.schema) == tipos(df.schema) == tipos(rejected.schema)
    # exceptAll verifica también multiplicidades: no pierde ni duplica filas.
    vuelta = silver.unionByName(rejected).select(df.columns)
    assert df.exceptAll(vuelta).limit(1).count() == 0
    assert vuelta.exceptAll(df).limit(1).count() == 0
    assert silver.where(~F.col('aceptado_silver')).count() == 0
    assert rejected.where(F.col('aceptado_silver')).count() == 0
    return silver, rejected, {'entrada': a + b, 'aceptados': a, 'excluidos': b,
                             'schema_verificado': True, 'multiconjunto_reconciliado': True}
