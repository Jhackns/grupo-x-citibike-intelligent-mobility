"""Diagnósticos sin filtros destructivos, imputación ni deduplicación de la tabla."""

from functools import reduce
from operator import or_

from pyspark.sql import functions as F, Window


def conteos(df, condiciones):
    return df.agg(*[F.sum(F.when(c, 1).otherwise(0)).alias(k)
                    for k, c in condiciones.items()]).first().asDict()


def intervalos(df, tiempo, particiones=()):
    # Vista de instantes distintos exclusivamente para medir cadencia; no altera df.
    instantes = df.select(*particiones, tiempo).where(F.col(tiempo).isNotNull()).distinct()
    w = Window.partitionBy(*particiones).orderBy(tiempo)
    return (instantes.withColumn("anterior", F.lag(tiempo).over(w))
            .withColumn("intervalo_s", F.col(tiempo).cast("double") - F.col("anterior").cast("double"))
            .where(F.col("anterior").isNotNull()))


def resumen_intervalos(df):
    return df.agg(F.count("*").alias("intervalos"),
                  F.min("intervalo_s").alias("min_s"), F.max("intervalo_s").alias("max_s"),
                  F.avg("intervalo_s").alias("media_s"),
                  F.percentile_approx("intervalo_s", [0.5, 0.95]).alias("p50_p95_s"),
                  F.sum(F.when(F.col("intervalo_s") > 450, 1).otherwise(0)).alias("huecos_mayores_450s")
                  ).first().asDict()


def compatibilidad(status, information, vehicles):
    """Audita claves antes de cualquier enriquecimiento. No une filas de status."""
    def claves(df, key):
        counts = df.groupBy(key).count()
        return {"filas": df.count(), "ids_distintos": df.select(key).distinct().count(),
                "ids_nulos": df.where(F.col(key).isNull()).count(),
                "ids_repetidos": counts.where("count > 1").count()}
    mapping = vehicles.select("vehicle_type_id", "propulsion_type").distinct()
    type_ids = status.select(F.explode("vehicle_types_available").alias("v")).select(
        F.col("v.vehicle_type_id").alias("vehicle_type_id")).distinct()
    return {
        "station_information": claves(information, "station_id"),
        "vehicle_types": claves(vehicles, "vehicle_type_id"),
        "tipos_con_propulsion_ambigua": mapping.groupBy("vehicle_type_id").count().where("count > 1").count(),
        "estaciones_status_sin_information": status.select("station_id").distinct().join(
            information.select("station_id").distinct(), "station_id", "left_anti").count(),
        "estaciones_information_sin_status": information.select("station_id").distinct().join(
            status.select("station_id").distinct(), "station_id", "left_anti").count(),
        "tipos_status_sin_catalogo": type_ids.join(mapping, "vehicle_type_id", "left_anti").count(),
        "catalogo_observado": [r.asDict() for r in mapping.orderBy("vehicle_type_id").collect()],
    }


def cantidades_por_catalogo(status, vehicles):
    """Consulta diagnóstica: resuelve tipos reales sin join que multiplique estaciones."""
    mapping = vehicles.select("vehicle_type_id", "propulsion_type").distinct().collect()
    ids = [r.vehicle_type_id for r in mapping]
    if len(set(ids)) != len(ids):
        raise ValueError("Catálogo ambiguo: no se interpretan tipos")
    result = status
    for propulsion, name in (("human", "convencionales_catalogo"),
                             ("electric_assist", "electricas_catalogo")):
        selected = [r.vehicle_type_id for r in mapping if r.propulsion_type == propulsion]
        if not selected:
            raise ValueError(f"No hay correspondencia comprobada para {propulsion}")
        subset = F.filter(F.col("vehicle_types_available"), lambda v: v.vehicle_type_id.isin(selected))
        result = result.withColumn(name, F.when(F.size(subset) == len(selected),
            F.aggregate(subset, F.lit(0).cast("long"), lambda total, v: total + v["count"])))
    return result


def diagnosticar(feed, status, information, vehicles):
    cat = cantidades_por_catalogo(status, vehicles)
    edad = F.col("last_updated") - F.col("last_reported")
    payload = [c for c in status.columns if c not in (
        "archivo_origen", "captura_utc", "captura_nueva_york", "fecha_nueva_york",
        "last_updated", "last_updated_utc", "last_reported_utc", "ttl", "version")]
    # Arrays se canonizan SOLO para comparar payload, preservando el dato original.
    payload_cols = [F.sort_array(F.col(c)).alias(c) if c == "vehicle_types_available" else F.col(c)
                    for c in payload]
    firmas = status.select("archivo_origen", "captura_utc", "station_id", "last_reported",
                            F.to_json(F.struct(*payload_cols)).alias("payload"))
    exactos = firmas.groupBy("archivo_origen", "payload").count().where("count > 1")
    conflictos_captura = firmas.groupBy("station_id", "captura_utc").agg(
        F.count("*").alias("filas"), F.countDistinct("payload").alias("variantes"))
    conflictos_reporte = firmas.groupBy("station_id", "last_reported").agg(
        F.count("*").alias("filas"), F.countDistinct("payload").alias("variantes"))
    numeric = [c for c in payload if c.startswith("num_")]
    flags = {"cantidades_negativas": reduce(or_, [F.col(c) < 0 for c in numeric]),
             "electrica_mayor_total": F.col("num_ebikes_available") > F.col("num_bikes_available"),
             "suma_tipos_distinta_total": F.aggregate("vehicle_types_available", F.lit(0).cast("long"),
                 lambda a, v: a + v["count"]) != F.col("num_bikes_available"),
             "electrica_distinta_catalogo": F.col("electricas_catalogo") != F.col("num_ebikes_available"),
             "convencional_distinta_resta": F.col("convencionales_catalogo") !=
                 (F.col("num_bikes_available") - F.col("num_ebikes_available")),
             "conteo_tipos_nulo_negativo": F.exists("vehicle_types_available", lambda v:
                 v["count"].isNull() | (v["count"] < 0) | v.vehicle_type_id.isNull()),
             "tipos_repetidos_en_estacion": F.size("vehicle_types_available") != F.size(
                 F.array_distinct(F.transform("vehicle_types_available", lambda v: v.vehicle_type_id))),
             "tipos_no_resueltos": F.col("convencionales_catalogo").isNull() | F.col("electricas_catalogo").isNull(),
             "estado_no_binario": reduce(or_, [~F.col(c).isin(0, 1) for c in ("is_installed", "is_renting", "is_returning")]),
             "no_operativa": (F.col("is_installed") == 0) | (F.col("is_renting") == 0),
             "reporte_mas_15min": edad > 900, "reporte_mas_24h": edad > 86400,
             "reporte_posterior_feed": edad < 0,
             "feed_mas_15min_antes_archivo": F.col("captura_utc").cast("double") - F.col("last_updated") > 900,
             "feed_posterior_archivo": F.col("last_updated") > F.col("captura_utc").cast("double")}
    feed_ordenado = feed.withColumn("feed_anterior", F.lag("last_updated").over(Window.orderBy("captura_utc")))
    summary = {"filas": status.count(), "archivos_status": feed.count(),
               "estaciones": status.select("station_id").distinct().count(),
               "periodo": status.agg(F.min("captura_utc").alias("archivo_min_utc"),
                    F.max("captura_utc").alias("archivo_max_utc"), F.min("last_updated_utc").alias("feed_min_utc"),
                    F.max("last_updated_utc").alias("feed_max_utc"),
                    F.min("last_reported_utc").alias("reporte_min_utc"), F.max("last_reported_utc").alias("reporte_max_utc")).first().asDict(),
               "nulos": conteos(status, {c: F.col(c).isNull() for c in status.columns}),
               "feeds_invalidos": conteos(feed, {"json_corrupto": F.col("_corrupt_record").isNotNull(),
                    "array_nulo": F.col("data.stations").isNull(), "array_vacio": F.size("data.stations") == 0,
                    "captura_no_parseable": F.col("captura_utc").isNull()}),
               "duplicados_exactos_exceso_en_archivo": exactos.agg(F.sum(F.col("count")-1)).first()[0] or 0,
               "claves_estacion_captura_repetidas": conflictos_captura.where("filas > 1").count(),
               "conflictos_estacion_captura": conflictos_captura.where("variantes > 1").count(),
               "claves_estacion_reporte_repetidas": conflictos_reporte.where("filas > 1").count(),
               "conflictos_estacion_mismo_last_reported": conflictos_reporte.where("variantes > 1").count(),
               "indicadores": conteos(cat, flags),
               "compatibilidad": compatibilidad(status, information, vehicles),
               "cadencia_archivo": resumen_intervalos(intervalos(feed, "captura_utc")),
               "cadencia_feed": resumen_intervalos(intervalos(feed, "last_updated_utc")),
               "cadencia_reportes_estacion": resumen_intervalos(intervalos(status, "last_reported_utc", ("station_id",))),
               "cadencia_observaciones_estacion": resumen_intervalos(intervalos(status, "captura_utc", ("station_id",))),
               "retrocesos_feed_en_orden_captura": feed_ordenado.where(F.col("last_updated") < F.col("feed_anterior")).count(),
               "feeds_reutilizados": feed.count() - feed.select("last_updated").distinct().count()}
    coverage = status.groupBy("station_id").agg(F.count("*").alias("capturas"),
        F.min("captura_utc").alias("primera"), F.max("captura_utc").alias("ultima"))
    summary["cobertura_estaciones"] = coverage.agg(
        F.min("capturas").alias("min_capturas"), F.max("capturas").alias("max_capturas"),
        F.sum(F.when(F.col("capturas") < summary["archivos_status"], 1).otherwise(0)).alias("estaciones_cobertura_parcial")
        ).first().asDict()
    summary["ejemplos_discrepancias"] = [r.asDict(recursive=True) for r in cat.where(
        flags["electrica_distinta_catalogo"] | flags["suma_tipos_distinta_total"]).select(
            "station_id", "captura_utc", "num_bikes_available", "num_ebikes_available",
            "convencionales_catalogo", "electricas_catalogo", "vehicle_types_available").limit(5).collect()]
    # Sin enriquecer la tabla principal: join de diagnóstico solo si information es único.
    if summary["compatibilidad"]["station_information"]["ids_repetidos"] == 0:
        joined = status.join(information.select("station_id", "capacity"), "station_id", "left")
        summary["capacidad"] = conteos(joined, {"capacidad_ausente": F.col("capacity").isNull(),
            "bicis_mas_docks_superan_capacidad": F.col("num_bikes_available") + F.col("num_bikes_disabled")
                + F.col("num_docks_available") + F.col("num_docks_disabled") > F.col("capacity")})
    return summary, cat, conflictos_reporte
def demostrar_rdd_y_ejecucion(df, destino):
    """RDD pertinente: distribución de calidad y discrepancia, reconciliada con SQL.

    Solo se recopilan categorías (máximo seis), nunca las observaciones completas.
    statusTracker observa jobs antes/después de construir el plan y hacer count.
    """
    import json
    from pathlib import Path
    from uuid import uuid4
    from operator import add
    from pyspark.sql import functions as F
    destination=Path(destino)
    destination.mkdir(parents=True,exist_ok=True)
    sc=df.sparkSession.sparkContext
    group='u1-lazy-'+uuid4().hex
    sc.setJobGroup(group,'Demostración de ejecución diferida de disponibilidad')
    tracker=sc.statusTracker()
    before=list(tracker.getJobIdsForGroup(group))
    transformed=(df.select('station_id','captura_utc','aceptado_silver','q_discrepancia_total')
        .where(F.col('aceptado_silver')).withColumn('fecha_demo',F.to_date('captura_utc')))
    plan=transformed._jdf.queryExecution().toString()
    after_plan=list(tracker.getJobIdsForGroup(group))
    assert before==after_plan, 'Construir este plan no debe lanzar jobs'
    n=transformed.count()
    after_action=list(tracker.getJobIdsForGroup(group))
    assert len(after_action)>len(after_plan), 'La acción debe ejecutar un job'
    sc.setLocalProperty('spark.jobGroup.id',None)
    sc.setLocalProperty('spark.job.description',None)
    rdd=(df.select('aceptado_silver','q_discrepancia_total').rdd
        .map(lambda r: ((str(r.aceptado_silver),str(r.q_discrepancia_total)),1))
        .reduceByKey(add))
    result=sorted(({'aceptado_silver':key[0],'discrepancia_total':key[1],'n':count}
                   for key,count in rdd.collect()),key=lambda r:(r['aceptado_silver'],r['discrepancia_total']))
    assert len(result)<=6
    sql=sorted(({'aceptado_silver':str(r.aceptado_silver),'discrepancia_total':str(r.q_discrepancia_total),'n':r['count']}
        for r in df.groupBy('aceptado_silver','q_discrepancia_total').count().collect()),
        key=lambda r:(r['aceptado_silver'],r['discrepancia_total']))
    assert result==sql
    proof={'jobs_antes':before,'jobs_tras_plan':after_plan,'jobs_tras_accion':after_action,
        'filas_aceptadas_accion':n,'rdd_operaciones':['map','reduceByKey'],
        'categorias_rdd':result,'rdd_reconcilia_con_dataframe':True,
        'interpretacion':'select/where/withColumn crean un plan; count ejecuta. La proyección permite leer menos columnas y el filtro se optimiza antes de la acción.'}
    (destination/'plan_lazy.txt').write_text(plan)
    (destination/'rdd_y_lazy.json').write_text(json.dumps(proof,indent=2)+'\n')
    return proof

