"""Gold temporal de conteos reportados. No ajusta parámetros ni entrena modelos."""

from functools import reduce
from operator import or_
from pathlib import Path
from html import escape

from pyspark.sql import functions as F, Window
from .calidad import motivos

VERSION = 'gold-v3-emparejamiento-estrecho'
TIPOS = ('convencionales', 'electricas')
DISCREPANCIAS = ('q_discrepancia_total', 'q_discrepancia_convencionales', 'q_discrepancia_electricas')


def validar_entrada(df):
    if df.sparkSession.conf.get('spark.sql.session.timeZone') != 'UTC':
        raise ValueError('La sesión debe usar UTC')
    if df.where(F.col('station_id').isNull() | F.col('captura_utc').isNull()).limit(1).count():
        raise ValueError('Revisar IDs/tiempos nulos antes de Gold')
    if df.groupBy('station_id', 'captura_utc').count().where('count > 1').limit(1).count():
        raise ValueError('Claves estación/captura repetidas: resolver sin deduplicación arbitraria')


def construir_variables(df, hueco_s=450, tolerancia_s=30):
    """Ventanas exclusivamente retrospectivas. Un tramo nuevo impide cruzar huecos
    o estados no elegibles. Cada tipo tiene su propia continuidad de calidad.
    """
    if not 0 < tolerancia_s < 150 or hueco_s < 300:
        raise ValueError('Revisar tolerancia y umbral de huecos')
    out = df.withColumn('_t_us', F.unix_micros('captura_utc'))
    orden = Window.partitionBy('station_id').orderBy('_t_us')
    out = out.withColumn('intervalo_previo_s',
        (F.col('_t_us') - F.lag('_t_us').over(orden)) / 1e6)
    features = []
    for tipo in TIPOS:
        eligible = 'elegible_' + tipo
        value = 'objetivo_' + tipo
        segmento = 'segmento_' + tipo
        prev = F.lag(eligible).over(orden)
        inicio = (F.col('intervalo_previo_s').isNull() | (F.col('intervalo_previo_s') > hueco_s)
                  | ~F.col(eligible) | ~F.coalesce(prev, F.lit(False)))
        out = out.withColumn(segmento, F.sum(F.when(inicio, 1).otherwise(0)).over(
            orden.rowsBetween(Window.unboundedPreceding, 0)))
        w = Window.partitionBy('station_id', segmento).orderBy('_t_us')
        out = out.withColumn(tipo + '_actual', F.col(value))
        features.append(tipo + '_actual')
        for minutos in (5, 10, 15):
            # Última observación en la banda de tiempo alrededor del rezago deseado.
            # El extremo superior sigue siendo estrictamente anterior a la emisión.
            lagw = w.rangeBetween(-int((minutos*60+tolerancia_s)*1e6),
                                 -int((minutos*60-tolerancia_s)*1e6))
            name = f'{tipo}_lag{minutos}'
            out = out.withColumn('_rezago', F.last(F.when(F.col(eligible),
                F.struct('captura_utc', F.col(value).alias('valor'))), ignorenulls=True).over(lagw))
            out = (out.withColumn(name, F.col('_rezago.valor'))
                .withColumn(name + '_utc', F.col('_rezago.captura_utc'))
                .withColumn(f'{tipo}_cambio{minutos}', F.col(value) - F.col(name)).drop('_rezago'))
            features += [name, f'{tipo}_cambio{minutos}']
        past = w.rangeBetween(-900_000_000, -1)  # excluye el instante inicial
        for nombre, expression in (
            ('media15', F.avg(value)), ('min15', F.min(value)), ('max15', F.max(value)),
            ('std15', F.stddev_pop(value)), ('n15', F.count(value))):
            name = tipo + '_' + nombre
            out = out.withColumn(name, expression.over(past))
            features.append(name)
    out = (out.withColumn('hora_local', F.hour('captura_nueva_york'))
        .withColumn('minuto_local', F.minute('captura_nueva_york'))
        .withColumn('dia_semana_iso', F.pmod(F.dayofweek('captura_nueva_york') + 5, F.lit(7)) + 1)
        .withColumn('fin_semana', F.col('dia_semana_iso').isin(6, 7).cast('int'))
        .withColumn('hora_seno', F.sin(2 * 3.141592653589793 *
            (F.col('hora_local') + F.col('minuto_local')/60) / 24))
        .withColumn('hora_coseno', F.cos(2 * 3.141592653589793 *
            (F.col('hora_local') + F.col('minuto_local')/60) / 24))
        .withColumn('max_tiempo_features_utc', F.col('captura_utc')))
    features += ['hora_local', 'minuto_local', 'dia_semana_iso', 'fin_semana', 'hora_seno', 'hora_coseno']
    return out, features


def construir_gold(variables, corte_validacion, corte_prueba, fin_exclusivo,
                   horizontes=(5, 10, 15), tolerancia_s=30, inicio_modelado=None):
    """Empareja por timestamp más cercano (desempate: anterior), no por posición.

    Usa equijoin de buckets de 30s con banda residual: evita producto cartesiano
    por estación. Selecciona la captura más cercana antes de aplicar calidad.
    No salta una captura inválida buscando una etiqueta más conveniente.
    """
    if not 0 < tolerancia_s < 150 or any(h <= 0 for h in horizontes):
        raise ValueError('Horizontes/tolerancia inválidos')
    validar_entrada(variables)
    spark = variables.sparkSession
    bounds = spark.sql(f"SELECT timestamp'{corte_validacion}' v, timestamp'{corte_prueba}' p, timestamp'{fin_exclusivo}' f").first()
    if not bounds.v < bounds.p < bounds.f:
        raise ValueError('Cortes no cronológicos')
    qcols = [c for c in variables.columns if c.startswith('q_')]
    # Indicadores globales de mismo reporte son retrospectivos: auditoría, nunca features.
    target_cols = ['station_id', 'captura_utc', '_t_us', 'archivo_origen', 'last_updated',
                   'last_reported', 'regla_version', 'num_bikes_available', 'num_ebikes_available',
                   'vehicle_types_available', *qcols]
    target_cols += [c + '_' + tipo for tipo in TIPOS for c in ('objetivo', 'elegible', 'segmento')]
    # Resolver el emparejamiento solo con claves/tiempos evita replicar todos los
    # payloads nueve veces (tres horizontes × tres buckets) antes de la ventana.
    future = variables.select(*[F.col(c).alias('f_' + c) for c in ('station_id','captura_utc','_t_us')]).withColumn(
        '_bucket_f', F.floor(F.col('f__t_us') / 30_000_000))
    base = (variables.select('station_id','captura_utc','_t_us')
        .withColumn('horizonte_min', F.explode(F.array(*[F.lit(h) for h in horizontes])))
        .withColumn('_esperado_us', F.col('_t_us') + F.col('horizonte_min')*60_000_000)
        .withColumn('_bucket_ideal', F.floor(F.col('_esperado_us') / 30_000_000))
        .withColumn('_bucket', F.explode(F.array(F.col('_bucket_ideal')-1,
                                               F.col('_bucket_ideal'), F.col('_bucket_ideal')+1))))
    # Tolerancia limitada a 30 s: como máximo buckets vecino/anterior/siguiente.
    if tolerancia_s > 30:
        raise ValueError('Este índice temporal admite tolerancia <=30s')
    matched = base.join(future, (F.col('station_id') == F.col('f_station_id')) &
        (F.col('_bucket') == F.col('_bucket_f')) &
        (F.abs(F.col('f__t_us') - F.col('_esperado_us')) <= tolerancia_s*1e6) &
        (F.col('f__t_us') > F.col('_t_us')), 'left')
    w = Window.partitionBy('station_id', 'captura_utc', 'horizonte_min').orderBy(
        F.abs(F.col('f__t_us') - F.col('_esperado_us')).asc_nulls_last(), F.col('f__t_us').asc_nulls_last())
    keys = (matched.withColumn('_rn', F.row_number().over(w)).where('_rn = 1')
        .select('station_id','captura_utc','horizonte_min','f_captura_utc','f__t_us'))
    out = keys.join(variables,['station_id','captura_utc'],'inner')
    payload = variables.select(F.col('station_id').alias('_join_station'),
        F.col('captura_utc').alias('_join_future_capture'),
        *[F.col(c).alias('f_'+c) for c in target_cols if c not in ('station_id','captura_utc','_t_us')])
    out = out.join(payload,(F.col('station_id')==F.col('_join_station')) &
        (F.col('f_captura_utc')==F.col('_join_future_capture')),'left')
    out = (out
        .withColumn('particion_temporal', F.when(F.col('captura_utc') < F.lit(bounds.v), 'train')
            .when(F.col('captura_utc') < F.lit(bounds.p), 'validacion').otherwise('prueba'))
        .withColumn('limite_objetivo_utc', F.when(F.col('particion_temporal') == 'train', F.lit(bounds.v))
            .when(F.col('particion_temporal') == 'validacion', F.lit(bounds.p)).otherwise(F.lit(bounds.f)))
        .withColumn('horizonte_efectivo_s', (F.col('f__t_us') - F.col('_t_us')) / 1e6)
        .withColumn('error_emparejamiento_s', F.col('horizonte_efectivo_s') - F.col('horizonte_min')*60)
        .withColumn('gold_version', F.lit(VERSION)))
    for tipo in TIPOS:
        reasons = {'inicial_no_elegible': ~F.col('elegible_' + tipo),
            'sin_captura_objetivo_en_tolerancia': F.col('f_captura_utc').isNull(),
            'objetivo_no_elegible': F.col('f_captura_utc').isNotNull() & ~F.col('f_elegible_' + tipo),
            'hueco_o_inactividad_en_trayecto': F.col('f_captura_utc').isNotNull() &
                (F.col('segmento_' + tipo) != F.col('f_segmento_' + tipo)),
            'objetivo_cruza_corte_temporal': F.col('f_captura_utc') >= F.col('limite_objetivo_utc'),
            'emision_fuera_periodo': (F.col('captura_utc') >= F.lit(bounds.f)) |
                (F.col('captura_utc') < F.to_timestamp(F.lit(inicio_modelado)) if inicio_modelado else F.lit(False))}
        out = (out.withColumn('motivos_gold_' + tipo, motivos(reasons))
            .withColumn('elegible_gold_' + tipo, F.size('motivos_gold_' + tipo) == 0)
            .withColumn('y_' + tipo, F.when(F.col('elegible_gold_' + tipo), F.col('f_objetivo_' + tipo))))
    # Estratos disjuntos evaluados al futuro; están prohibidos como predictores.
    init_known = reduce(lambda a,b:a & b, [F.col(c).isNotNull() for c in DISCREPANCIAS])
    target_known = reduce(lambda a,b:a & b, [F.col('f_' + c).isNotNull() for c in DISCREPANCIAS])
    any_disc = reduce(or_, [F.col(c) for c in DISCREPANCIAS] + [F.col('f_'+c) for c in DISCREPANCIAS])
    out = (out.withColumn('estrato_discrepancia', F.when(any_disc, 'con_discrepancias')
        .when(init_known & target_known, 'sin_discrepancias').otherwise('desconocido'))
        .withColumn('aceptado_gold', F.col('elegible_gold_convencionales') | F.col('elegible_gold_electricas')))
    return out.drop(*[c for c in out.columns if c.startswith('_') or c in ('f__t_us','f_station_id')])


def verificar_gold(df, features, tolerancia_s=30):
    """Contratos verificables antes/después de escribir, sin recopilar las filas."""
    if df.groupBy('station_id','captura_utc','horizonte_min').count().where('count > 1').limit(1).count():
        raise AssertionError('Gold no es único')
    for feature in features:
        assert feature in df.columns and not feature.startswith('f_')
    for tipo in TIPOS:
        good = df.where('elegible_gold_' + tipo)
        assert not good.where(F.col('y_' + tipo).isNull() | (F.col('y_' + tipo) < 0) |
            (F.abs('error_emparejamiento_s') > tolerancia_s) |
            (F.col('f_captura_utc') <= F.col('captura_utc')) |
            ~F.col('f_elegible_' + tipo) |
            (F.col('f_captura_utc') >= F.col('limite_objetivo_utc')) |
            (F.col('segmento_' + tipo) != F.col('f_segmento_' + tipo))).limit(1).count()
        for minutos in (5,10,15):
            assert not df.where(F.col(f'{tipo}_lag{minutos}_utc') >= F.col('captura_utc')).limit(1).count()
    assert not df.where(F.col('max_tiempo_features_utc') > F.col('captura_utc')).limit(1).count()
    return True


def grafico_svg(path, filas, etiqueta, series, titulo):
    """Barras sobre agregados pequeños; evita dependencia gráfica y pandas."""
    rows = list(filas)
    if len(rows) > 200:
        raise ValueError('Solo agregados pequeños para gráficos')
    maximum = max([float(r.get(s) or 0) for r in rows for s in series] or [1]) or 1
    width, height = 1000, 360
    colors = ('#2563eb','#d97706')
    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
             '<rect width="100%" height="100%" fill="white"/>',
             f'<text x="30" y="25" font-size="17">{escape(titulo)}</text>']
    for i, row in enumerate(rows):
        group = 900/max(len(rows),1)
        for j, name in enumerate(series):
            val = float(row.get(name) or 0)
            h = val/maximum*240
            x = 55+i*group+j*group/(len(series)+1)
            parts.append(f'<rect x="{x}" y="{290-h}" width="{group/(len(series)+1)-1}" height="{h}" fill="{colors[j%2]}"/>')
        parts.append(f'<text x="{55+i*group}" y="310" font-size="10">{escape(str(row[etiqueta]))}</text>')
    parts += [f'<text x="30" y="345" font-size="12">Máximo eje: {maximum:.2f}; azul: {escape(series[0])}; naranja: {escape(series[-1])}</text>', '</svg>']
    Path(path).write_text('\n'.join(parts), encoding='utf-8')


def explorar(silver_completo, variables):
    """Solo recopila agregados acotados, nunca el histórico completo."""
    df = silver_completo
    def rows(frame):
        return [r.asDict(recursive=True) for r in frame.collect()]
    cobertura = df.groupBy('fecha_captura_utc').agg(
        F.count('*').alias('observaciones'),F.countDistinct('archivo_origen').alias('archivos'),
        F.countDistinct('station_id').alias('estaciones'),
        F.sum(F.col('aceptado_silver').cast('long')).alias('aceptados'),
        F.sum(F.col('q_discrepancia_total').cast('long')).alias('discrepancia_total'),
        F.min('captura_utc').alias('primera_utc'),F.max('captura_utc').alias('ultima_utc'))
    por_estacion = df.groupBy('station_id').agg(F.count('*').alias('capturas'),
        F.sum(F.col('aceptado_silver').cast('long')).alias('elegibles'),
        F.min('captura_utc').alias('primera_utc'),F.max('captura_utc').alias('ultima_utc'))
    captura = df.select('captura_utc','last_updated').distinct()
    w = Window.orderBy('captura_utc')
    captura = (captura.withColumn('delta_archivo_s',F.col('captura_utc').cast('double')-
        F.lag(F.col('captura_utc').cast('double')).over(w))
        .withColumn('delta_feed_s',F.col('last_updated')-F.lag('last_updated').over(w)))
    cadencia = captura.agg(*[expr for col in ('delta_archivo_s','delta_feed_s') for expr in (
        F.min(col).alias(col+'_min'),F.max(col).alias(col+'_max'),
        F.percentile_approx(col,[0.5,0.95]).alias(col+'_p50_p95'))]).first().asDict()
    huecos = variables.agg(F.sum(F.when(F.col('intervalo_previo_s')>450,1).otherwise(0)).alias('huecos_interiores_gt450s'),
        F.max('intervalo_previo_s').alias('max_intervalo_s')).first().asDict()
    por_hora = variables.groupBy('hora_local').agg(
        F.avg('convencionales_actual').alias('convencionales_media'),F.avg('electricas_actual').alias('electricas_media'),
        *[F.avg(F.when(F.col('elegible_'+tipo), (F.col(tipo+'_actual')==0).cast('double'))).alias(tipo+'_fraccion_cero')
          for tipo in TIPOS]).orderBy('hora_local')
    por_dia = variables.groupBy('fecha_nueva_york','dia_semana_iso').agg(F.count('*').alias('observaciones'),
        F.avg('convencionales_actual').alias('convencionales_media'),F.avg('electricas_actual').alias('electricas_media'))
    distribuciones, ceros, episodios = {}, {}, {}
    for tipo in TIPOS:
        value = tipo + '_actual'
        distribuciones[tipo] = variables.agg(F.count(value).alias('n'),F.min(value).alias('min'),
            F.max(value).alias('max'),F.avg(value).alias('media'),
            F.percentile_approx(value,[0.25,0.5,0.75,0.95]).alias('p25_p50_p75_p95')).first().asDict()
        ceros[tipo] = variables.agg(F.sum(F.when(F.col(value)==0,1).otherwise(0)).alias('observaciones_cero'),
            F.countDistinct(F.when(F.col(value)==0,F.col('station_id'))).alias('estaciones_con_cero')).first().asDict()
        orden = Window.partitionBy('station_id','segmento_'+tipo).orderBy('_t_us')
        previo = F.lag(F.col(value)==0).over(orden)
        zero = variables.withColumn('_inicio_cero',F.when((F.col(value)==0) & ~F.coalesce(previo,F.lit(False)),1).otherwise(0))
        zero = zero.withColumn('_episodio',F.sum('_inicio_cero').over(orden.rowsBetween(Window.unboundedPreceding,0)))
        runs = zero.where(F.col(value)==0).groupBy('station_id','segmento_'+tipo,'_episodio').agg(
            F.min('captura_utc').alias('inicio'),F.max('captura_utc').alias('fin'),F.count('*').alias('capturas'))
        runs = runs.withColumn('duracion_observada_s',F.col('fin').cast('double')-F.col('inicio').cast('double'))
        episodios[tipo] = {'resumen':runs.agg(F.count('*').alias('episodios'),F.max('duracion_observada_s').alias('max_s'),
            F.percentile_approx('duracion_observada_s',[0.5,0.95]).alias('p50_p95_s')).first().asDict(),
            'ejemplos':rows(runs.orderBy(F.desc('duracion_observada_s'),'station_id').limit(5))}
    return {'cobertura_fecha':rows(cobertura),
        'cobertura_estaciones':por_estacion.agg(F.count('*').alias('estaciones'),F.min('capturas').alias('min_capturas'),
            F.max('capturas').alias('max_capturas'),F.min('elegibles').alias('min_elegibles'),
            F.percentile_approx('elegibles',[0.5,0.95]).alias('p50_p95_elegibles')).first().asDict(),
        'estaciones_menor_cobertura':rows(por_estacion.orderBy('capturas','station_id').limit(10)),
        'cadencia':cadencia,'huecos':huecos,'por_hora':rows(por_hora),'por_dia_local':rows(por_dia),
        'distribuciones':distribuciones,'ceros':ceros,'episodios_cero':episodios,
        'exclusiones_silver': rows(df.groupBy('tipo_registro','aceptado_silver').count())}
