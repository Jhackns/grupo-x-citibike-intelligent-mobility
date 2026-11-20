"""
Genera el dashboard general de Grafana (U1 + U2 + U3 en un solo tablero).

Uso (dentro del contenedor spark, desde la raíz del proyecto):
    docker compose exec spark python scripts/generar_dashboard_general.py

Fuentes:
  - U1 (batch): salidas/u1/gold_ranking_zonas (Parquet del notebook U1) y salidas/u1/estaciones_zona.json
    (tam_celda, lat0, lon0 de la cuadrícula). Estos resultados NO están en Prometheus, así que se incrustan
    como CSV en el dashboard y se sirven con el datasource TestData (uid "u1-batch"). Si se vuelve a correr
    el notebook U1, basta con volver a ejecutar este script.
  - U2 (streaming): métricas lambda26_u2esp_* publicadas por U2_02 en Pushgateway -> Prometheus.
  - U3 (integración): métricas lambda26_u3_* publicadas por U3_ejecucion_integrada.py.
  - Kafka: kafka_topic_partition_current_offset (kafka-exporter).
"""
import io
import json
from pathlib import Path

import pandas as pd

RAIZ = Path(__file__).resolve().parents[1]
SALIDA = RAIZ / "infra/grafana/provisioning/dashboards/json/dashboard_general_cristhian.json"
U1 = RAIZ / "salidas/u1"
U2 = RAIZ / "salidas/u2"

# ---------------------------------------------------------------- datos reales de U1
rank = pd.read_parquet(U1 / "gold_ranking_zonas").sort_values("actividad", ascending=False).reset_index(drop=True)
celda = json.loads((U1 / "estaciones_zona.json").read_text(encoding="utf-8"))
TAM, LAT0, LON0 = celda["tam_celda"], celda["lat0"], celda["lon0"]
gold = pd.read_parquet(U1 / "gold_zona_hora", columns=["hora_ts"])
PERIODO = (f"julio 2026 (viajes del {gold.hora_ts.min():%Y-%m-%d} al 2026-07-31; Gold incluye llegadas hasta "
           f"{gold.hora_ts.max():%Y-%m-%d %H:%M})")
TOTAL = int(rank.actividad.sum())


def centro(zona):
    # zona = Z{fila}-{col}; fila = floor((lat-LAT0)/TAM), col = floor((lon-LON0)/TAM)  (función zona_de del notebook U1)
    fila, col = (int(x) for x in zona[1:].split("-"))
    return round(LAT0 + (fila + 0.5) * TAM, 5), round(LON0 + (col + 0.5) * TAM, 5)


rank["puesto"] = rank.index + 1
rank[["lat_centro", "lon_centro"]] = [centro(z) for z in rank.zona]
rank["periodo"] = PERIODO

mapa_cols = ["zona", "estacion_ejemplo", "puesto", "actividad", "salidas", "llegadas", "participacion_pct",
             "n_estaciones", "lat_centro", "lon_centro", "lat", "lon", "periodo"]
buf = io.StringIO()
rank[mapa_cols].to_csv(buf, index=False)
CSV_MAPA = buf.getvalue()

top10 = rank.head(10).copy()
top10["zona_nombre"] = [f"{p:02d} · {z} · {e}" for p, z, e in zip(top10.puesto, top10.zona, top10.estacion_ejemplo)]
buf = io.StringIO()
top10[["zona_nombre", "actividad", "participacion_pct", "zona"]].to_csv(buf, index=False)
CSV_TOP10 = buf.getvalue()

zonas_top = json.loads((U2 / "zonas_top.json").read_text(encoding="utf-8"))["zonas"]
ZONAS_DEFECTO = [z["zona"] for z in zonas_top[:5]]
MAE_TEST = next(r["MAE"] for r in json.loads((U2 / "metricas_modelo.json").read_text(encoding="utf-8"))["resultados"]
                if r["modelo"] == json.loads((U2 / "metricas_modelo.json").read_text(encoding="utf-8"))["ganador"])

# ---------------------------------------------------------------- ayudas
PROM = {"type": "prometheus", "uid": "${ds}"}
TESTDATA = {"type": "grafana-testdata-datasource", "uid": "u1-batch"}
AZUL, VERDE, AMARILLO, ROJO = "blue", "green", "yellow", "red"
_id = [0]


def nid():
    _id[0] += 1
    return _id[0]


def q(expr, legend="", instant=False, ref="A", fmt="time_series"):
    return {"datasource": PROM, "expr": expr, "legendFormat": legend, "instant": instant, "range": not instant,
            "refId": ref, "format": fmt}


def umbrales(*pasos):
    return {"mode": "absolute", "steps": [{"color": c, "value": v} for c, v in pasos]}


def fijo(color):
    return {"mode": "absolute", "steps": [{"color": color, "value": None}]}


def row(titulo, y):
    return {"type": "row", "id": nid(), "title": titulo, "collapsed": False, "gridPos": {"h": 1, "w": 24, "x": 0, "y": y},
            "panels": []}


def stat(titulo, desc, targets, pos, unidad="none", th=None, decimales=None, texto="value", mapeos=None, color_modo="background"):
    d = {"unit": unidad, "thresholds": th or fijo(AZUL), "color": {"mode": "thresholds"}, "mappings": mapeos or []}
    if decimales is not None:
        d["decimals"] = decimales
    return {"type": "stat", "id": nid(), "title": titulo, "description": desc, "datasource": PROM, "gridPos": pos,
            "targets": targets, "fieldConfig": {"defaults": d, "overrides": []},
            "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                        "colorMode": color_modo, "graphMode": "none", "textMode": texto, "justifyMode": "center",
                        "orientation": "auto", "wideLayout": True, "showPercentChange": False}}


def serie(titulo, desc, targets, pos, unidad="none", overrides=None, interp="smooth", th=None, th_estilo="off", eje=""):
    return {"type": "timeseries", "id": nid(), "title": titulo, "description": desc, "datasource": PROM, "gridPos": pos,
            "targets": targets,
            "fieldConfig": {"defaults": {"unit": unidad, "color": {"mode": "palette-classic"},
                                         "thresholds": th or fijo(VERDE),
                                         "custom": {"drawStyle": "line", "lineInterpolation": interp, "lineWidth": 2,
                                                    "fillOpacity": 8, "pointSize": 4, "showPoints": "auto",
                                                    "axisLabel": eje, "spanNulls": True,
                                                    "thresholdsStyle": {"mode": th_estilo}}},
                            "overrides": overrides or []},
            "options": {"legend": {"displayMode": "table", "placement": "right", "calcs": ["lastNotNull"], "showLegend": True},
                        "tooltip": {"mode": "multi", "sort": "desc"}}}


def bargauge(titulo, desc, targets, pos, unidad="none", th=None, color=None, overrides=None, min_=None, max_=None):
    d = {"unit": unidad, "thresholds": th or fijo(AZUL), "color": color or {"mode": "thresholds"}}
    if min_ is not None:
        d["min"] = min_
    if max_ is not None:
        d["max"] = max_
    return {"type": "bargauge", "id": nid(), "title": titulo, "description": desc, "datasource": PROM, "gridPos": pos,
            "targets": targets, "fieldConfig": {"defaults": d, "overrides": overrides or []},
            "options": {"orientation": "horizontal", "displayMode": "gradient", "valueMode": "color", "showUnfilled": True,
                        "namePlacement": "left", "sizing": "manual", "minVizHeight": 18, "maxVizHeight": 26,
                        "minVizWidth": 8, "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}}}


paneles = []
y = 0

# ---------------------------------------------------------------- encabezado
paneles.append({
    "type": "text", "id": nid(), "title": "", "transparent": True, "gridPos": {"h": 4, "w": 24, "x": 0, "y": y},
    "options": {"mode": "html", "content": (
        "<div style='padding:4px 8px'>"
        "<div style='font-size:26px;font-weight:700;letter-spacing:.5px'>CitiBike Intelligent Mobility</div>"
        "<div style='font-size:15px;font-weight:600;color:#5794F2;margin-top:2px'>CRISTHIAN CHUQUITARQUI CHURA | ANÁLISIS ESPACIAL</div>"
        "<div style='font-size:13px;opacity:.8;margin-top:6px'>Análisis histórico, predicción espacial en tiempo real e integración "
        "de un sistema Big Data para anticipar la actividad de Citi Bike NYC.</div>"
        "<div style='font-size:13px;margin-top:6px'><b style='color:#5794F2'>ANALIZAR EL PASADO</b> (U1 · batch) &nbsp;→&nbsp; "
        "<b style='color:#FADE2A'>ANTICIPAR EL FUTURO</b> (U2 · streaming) &nbsp;→&nbsp; "
        "<b style='color:#73BF69'>COMPROBAR EL FUNCIONAMIENTO</b> (U3 · integración)</div></div>")}})
y += 4

# ================================================================ U1
paneles.append(row("01 · UNIDAD I — ANÁLISIS HISTÓRICO (BATCH) · ¿Qué zonas concentran la actividad de Citi Bike?", y))
y += 1

campos_u1 = {
    "zona": "Zona", "estacion_ejemplo": "Estación de referencia", "puesto": "Puesto en el ranking",
    "actividad": "Actividad (salidas + llegadas)", "salidas": "Salidas", "llegadas": "Llegadas",
    "participacion_pct": "Participación (%)", "n_estaciones": "Estaciones en la zona",
    "lat_centro": "Latitud centro de la celda", "lon_centro": "Longitud centro de la celda",
    "lat": "Latitud media de las estaciones", "lon": "Longitud media de las estaciones", "periodo": "Periodo analizado"}
paneles.append({
    "type": "geomap", "id": nid(), "title": "Mapa de calor de actividad por zonas (cuadrícula 0,02° × 0,02°)",
    "description": (
        "Fuente: salidas/u1/gold_ranking_zonas (notebook U1, julio 2026). Cada punto es el CENTRO EXACTO de la celda de "
        f"0,02° × 0,02° calculado con la misma fórmula del notebook (zona_de: lat0={LAT0}, lon0={LON0}). "
        "Color y tamaño = actividad histórica (salidas + llegadas, la misma definición del notebook: cada viaje cuenta en su "
        "zona de origen y en la de destino). La capa de calor resalta dónde se concentra. Pasa el cursor sobre un punto para ver "
        "zona, actividad, puesto en el ranking y periodo. Este panel no depende del selector de tiempo."),
    "datasource": TESTDATA, "gridPos": {"h": 15, "w": 14, "x": 0, "y": y},
    "targets": [{"datasource": TESTDATA, "refId": "A", "scenarioId": "csv_content", "csvContent": CSV_MAPA}],
    "fieldConfig": {
        "defaults": {"color": {"mode": "continuous-YlRd"}, "thresholds": fijo(AZUL), "unit": "none"},
        "overrides": [{"matcher": {"id": "byName", "options": k}, "properties": [{"id": "displayName", "value": v}]}
                      for k, v in campos_u1.items()]
        + [{"matcher": {"id": "byName", "options": "participacion_pct"}, "properties": [{"id": "unit", "value": "percent"}]}]},
    "options": {
        "view": {"id": "coords", "lat": 40.735, "lon": -73.955, "zoom": 11.4, "allLayers": True},
        "controls": {"showZoom": True, "mouseWheelZoom": True, "showAttribution": True, "showScale": True,
                     "showMeasure": False, "showDebug": False},
        "basemap": {"type": "xyz", "name": "Mapa base (Esri Dark Gray)", "config": {"url": "https://services.arcgisonline.com/arcgis/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}", "attribution": "Tiles © Esri"}},
        "layers": [
            {"type": "heatmap", "name": "Calor (actividad)", "tooltip": False,
             "location": {"mode": "coords", "latitude": "lat_centro", "longitude": "lon_centro"},
             "config": {"blur": 22, "radius": 14, "weight": {"field": "actividad", "fixed": 1, "min": 0, "max": 1}}},
            {"type": "markers", "name": "Zonas U1", "tooltip": True,
             "location": {"mode": "coords", "latitude": "lat_centro", "longitude": "lon_centro"},
             "config": {"showLegend": True,
                        "style": {"color": {"field": "actividad", "fixed": "dark-green"}, "opacity": 0.8,
                                  "size": {"field": "actividad", "fixed": 6, "min": 5, "max": 22},
                                  "symbol": {"mode": "fixed", "fixed": "img/icons/marker/square.svg"},
                                  "rotation": {"fixed": 0, "mode": "mod", "min": -360, "max": 360},
                                  "textConfig": {"fontSize": 12, "offsetX": 0, "offsetY": 0, "textAlign": "center", "textBaseline": "middle"}}}}],
        "tooltip": {"mode": "details"}}})

paneles.append({
    "type": "barchart", "id": nid(), "title": "Ranking histórico — top 10 zonas más activas",
    "description": (
        f"Fuente: salidas/u1/gold_ranking_zonas. Actividad = salidas + llegadas en {PERIODO}. "
        f"Las 10 zonas suman el {top10.participacion_pct.sum():.1f} % de la actividad total ({TOTAL:,} registros zona-evento). "
        "Pasa el cursor sobre una barra para ver la participación. No depende del selector de tiempo."),
    "datasource": TESTDATA, "gridPos": {"h": 15, "w": 10, "x": 14, "y": y},
    "targets": [{"datasource": TESTDATA, "refId": "A", "scenarioId": "csv_content", "csvContent": CSV_TOP10}],
    "fieldConfig": {
        "defaults": {"color": {"mode": "continuous-YlRd"}, "thresholds": fijo(AZUL), "unit": "locale", "decimals": 0,
                     "custom": {"fillOpacity": 85, "gradientMode": "none", "lineWidth": 0, "axisLabel": "Actividad (salidas + llegadas)",
                                "axisPlacement": "auto"}},
        "overrides": [
            {"matcher": {"id": "byName", "options": "actividad"}, "properties": [{"id": "displayName", "value": "Actividad (salidas + llegadas)"}]},
            {"matcher": {"id": "byName", "options": "participacion_pct"},
             "properties": [{"id": "displayName", "value": "Participación"}, {"id": "unit", "value": "percent"},
                            {"id": "custom.hideFrom", "value": {"viz": True, "legend": True, "tooltip": False}}]},
            {"matcher": {"id": "byName", "options": "zona"},
             "properties": [{"id": "custom.hideFrom", "value": {"viz": True, "legend": True, "tooltip": True}}]}]},
    "options": {"orientation": "horizontal", "xField": "zona_nombre", "showValue": "always", "colorByField": "actividad",
                "barWidth": 0.8, "groupWidth": 0.7, "stacking": "none", "xTickLabelRotation": 0, "xTickLabelMaxLength": 40,
                "legend": {"showLegend": False, "displayMode": "list", "placement": "bottom"},
                "tooltip": {"mode": "multi", "sort": "none"}, "text": {"valueSize": 13}}})
y += 15

# ================================================================ U2
paneles.append(row("02 · UNIDAD II — PREDICCIÓN EN TIEMPO REAL (STREAMING) · ¿Qué zonas concentrarán más actividad en la siguiente ventana de 15 min?", y))
y += 1

paneles.append(stat(
    "A · Actividad actual (última ventana cerrada de 15 min)",
    "lambda26_u2esp_actividad_total_ventana: salidas + llegadas de TODAS las zonas en la última ventana cerrada por el watermark. "
    "El texto indica la ventana (hora de NYC del día reproducido).",
    [q("lambda26_u2esp_actividad_total_ventana * on(job, instance) group_left(ventana) lambda26_u2esp_ventana_actual_info",
       "Ventana {{ventana}}", True)],
    {"h": 5, "w": 6, "x": 0, "y": y}, "locale", fijo(AZUL), 0, "value_and_name"))
paneles.append(stat(
    "B · Pronóstico de la siguiente ventana (15 zonas top)",
    "lambda26_u2esp_pronostico_total_siguiente_top: suma del pronóstico t+1 del modelo (LinearRegression de U2_01) para las "
    "15 zonas que modela el proyecto (65,6 % de la actividad).",
    [q("lambda26_u2esp_pronostico_total_siguiente_top", "", True)],
    {"h": 5, "w": 6, "x": 6, "y": y}, "locale", fijo(AZUL), 0))
paneles.append(stat(
    "C · Zonas en alerta (siguiente ventana)",
    "lambda26_u2esp_zonas_en_alerta_siguiente. Criterio del notebook U2_02: una zona está en alerta si su pronóstico t+1 "
    "es ≥ 1,5 × la media histórica de la misma franja. Verde = 0, amarillo = 1-2, rojo = 3 o más.",
    [q("lambda26_u2esp_zonas_en_alerta_siguiente", "", True)],
    {"h": 5, "w": 6, "x": 12, "y": y}, "none", umbrales((VERDE, None), (AMARILLO, 1), (ROJO, 3)), 0))
paneles.append(stat(
    "D · MAE en producción vs. MAE de prueba",
    f"Producción: lambda26_u2esp_mae_online (error absoluto medio de las predicciones ya evaluadas en vivo). "
    f"Prueba: lambda26_u2esp_modelo_mae_test del modelo ganador (notebook U2_01, MAE = {MAE_TEST:.2f}). "
    f"Colores: verde < 1,5 × MAE de prueba, amarillo hasta 2 ×, rojo > 2 × (misma compuerta que U3).",
    [q("lambda26_u2esp_mae_online", "Producción", True, "A"),
     q("lambda26_u2esp_modelo_mae_test and on(modelo) lambda26_u2esp_modelo_ganador_info", "Prueba ({{modelo}})", True, "B")],
    {"h": 5, "w": 6, "x": 18, "y": y}, "none",
    umbrales((VERDE, None), (AMARILLO, round(1.5 * MAE_TEST, 2)), (ROJO, round(2 * MAE_TEST, 2))), 2, "value_and_name"))
y += 5

paneles.append(serie(
    "Actividad real vs. actividad predicha por zona",
    "Real: lambda26_u2esp_actividad_observada (línea continua). Predicho: lambda26_u2esp_actividad_predicha_para_ventana "
    "(línea discontinua), es decir, lo que el modelo pronosticó para esa misma ventana una ventana antes. "
    f"Filtra con el selector Zona (por defecto las 5 zonas top: {', '.join(ZONAS_DEFECTO)}).",
    [q('lambda26_u2esp_actividad_observada{zona=~"$zona"}', "{{nombre}} · real", ref="A"),
     q('lambda26_u2esp_actividad_predicha_para_ventana{zona=~"$zona"}', "{{nombre}} · predicho", ref="B")],
    {"h": 10, "w": 14, "x": 0, "y": y}, "short",
    [{"matcher": {"id": "byRegexp", "options": ".*predicho"},
      "properties": [{"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [8, 6]}},
                     {"id": "custom.fillOpacity", "value": 0}]}],
    interp="stepAfter", eje="Viajes por ventana de 15 min"))

paneles.append(bargauge(
    "Pronóstico por zona — top 10 siguiente ventana (⚠ = en alerta)",
    "lambda26_u2esp_actividad_pronostico_siguiente, ordenado de mayor a menor. Las zonas en alerta (pronóstico ≥ 1,5 × media "
    "histórica de la franja, criterio del notebook) aparecen primero en ROJO con el prefijo ⚠.",
    [q("sort_desc(lambda26_u2esp_actividad_pronostico_siguiente >= 1.5 * clamp_min(lambda26_u2esp_actividad_media_historica_siguiente, 1))",
       "⚠ {{nombre}}", True, "A"),
     q("sort_desc(topk(10, lambda26_u2esp_actividad_pronostico_siguiente) unless on(zona) "
       "(lambda26_u2esp_actividad_pronostico_siguiente >= 1.5 * clamp_min(lambda26_u2esp_actividad_media_historica_siguiente, 1)))",
       "{{nombre}}", True, "B")],
    {"h": 10, "w": 10, "x": 14, "y": y}, "short", color={"mode": "continuous-BlPu"},
    overrides=[{"matcher": {"id": "byFrameRefID", "options": "A"},
                "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": ROJO}}]}], min_=0))
y += 10

paneles.append(serie(
    "Pronóstico t+1 vs. promedio histórico de la franja",
    "Pronóstico: lambda26_u2esp_actividad_pronostico_siguiente (línea continua). Promedio histórico: "
    "lambda26_u2esp_actividad_media_historica_siguiente (media de train para la misma zona, franja horaria y tipo de día; "
    "línea discontinua). Si el pronóstico queda muy por encima de la media, la zona se mueve más de lo habitual.",
    [q('lambda26_u2esp_actividad_pronostico_siguiente{zona=~"$zona"}', "{{nombre}} · pronóstico", ref="A"),
     q('lambda26_u2esp_actividad_media_historica_siguiente{zona=~"$zona"}', "{{nombre}} · media histórica", ref="B")],
    {"h": 10, "w": 12, "x": 0, "y": y}, "short",
    [{"matcher": {"id": "byRegexp", "options": ".*media histórica"},
      "properties": [{"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [8, 6]}},
                     {"id": "custom.fillOpacity", "value": 0}]}],
    interp="stepAfter", eje="Viajes por ventana de 15 min"))

paneles.append(bargauge(
    "Ocupación de bicicletas por zona (IoT · GBFS en vivo)",
    "lambda26_u2esp_iot_ocupacion_zona_pct = 100 × bicis disponibles / (bicis + docks libres) de las estaciones en servicio "
    "de la zona, calculado en U2_02 con el feed GBFS de Citi Bike. Ordenado de mayor a menor (desplázate para ver todas).",
    [q("sort_desc(lambda26_u2esp_iot_ocupacion_zona_pct)", "{{nombre}}", True)],
    {"h": 10, "w": 12, "x": 12, "y": y}, "percent",
    umbrales((ROJO, None), (AMARILLO, 20), (VERDE, 40), (AMARILLO, 85), (ROJO, 95)), min_=0, max_=100))
y += 10

bicis = {
    "type": "barchart", "id": nid(), "title": "Bicicletas disponibles por zona — 10 con más y 10 con menos (IoT)",
    "description": "lambda26_u2esp_iot_bicis_zona (sensores GBFS en vivo). Se muestran las 10 zonas con MÁS bicicletas "
                   "y las 10 con MENOS, ordenadas de mayor a menor.",
    "datasource": PROM, "gridPos": {"h": 11, "w": 12, "x": 0, "y": y},
    "targets": [q("topk(10, lambda26_u2esp_iot_bicis_zona) or bottomk(10, lambda26_u2esp_iot_bicis_zona)", "", True, "A", "table")],
    "transformations": [
        {"id": "organize", "options": {"excludeByName": {"Time": True, "instance": True, "job": True, "zona": True, "__name__": True},
                                       "renameByName": {"nombre": "Zona", "Value": "Bicis disponibles"}}},
        {"id": "sortBy", "options": {"sort": [{"field": "Bicis disponibles", "desc": True}]}}],
    "fieldConfig": {"defaults": {"color": {"mode": "continuous-RdYlGr"}, "thresholds": fijo(AZUL), "unit": "locale", "decimals": 0,
                                 "custom": {"fillOpacity": 85, "lineWidth": 0, "axisLabel": "Bicicletas disponibles"}},
                    "overrides": []},
    "options": {"orientation": "horizontal", "xField": "Zona", "showValue": "always", "colorByField": "Bicis disponibles",
                "barWidth": 0.8, "stacking": "none", "xTickLabelMaxLength": 36,
                "legend": {"showLegend": False, "displayMode": "list", "placement": "bottom"},
                "tooltip": {"mode": "single", "sort": "none"}}}
paneles.append(bicis)

paneles.append(bargauge(
    "Estaciones vacías por zona (sin bicicletas) — candidatas a redistribución",
    "lambda26_u2esp_iot_estaciones_vacias_zona: estaciones en servicio con 0 bicis disponibles, agrupadas por zona "
    "(GBFS en vivo). Solo zonas con al menos una estación vacía, de mayor a menor.",
    [q("sort_desc(topk(15, lambda26_u2esp_iot_estaciones_vacias_zona > 0))", "{{nombre}}", True)],
    {"h": 11, "w": 12, "x": 12, "y": y}, "none", umbrales((AMARILLO, None), (ROJO, 3)), min_=0))
y += 11

# ================================================================ U3
paneles.append(row("03 · UNIDAD III — INTEGRACIÓN Y OBSERVABILIDAD · ¿El sistema completo funciona, procesa los eventos y se puede comprobar?", y))
y += 1

paneles.append(stat(
    "Última ejecución integrada (run_id · estado)",
    "lambda26_u3_run_info{run_id, estado}, publicado por scripts/U3_ejecucion_integrada.py. Verde = OK, rojo = la "
    "corrida se detuvo (DETENIDA) en alguna compuerta.",
    [q('(lambda26_u3_run_info{estado="OK"} * 1) or (lambda26_u3_run_info * 0)', "run_id {{run_id}}", True)],
    {"h": 5, "w": 6, "x": 0, "y": y}, "none", umbrales((ROJO, None), (VERDE, 1)), 0, "value_and_name",
    [{"type": "value", "options": {"1": {"text": "OK", "color": VERDE}, "0": {"text": "ERROR / DETENIDA", "color": ROJO}}}]))
paneles.append(stat("Fecha y hora de la ejecución", "lambda26_u3_run_timestamp_seconds (hora local del navegador).",
                    [q("lambda26_u3_run_timestamp_seconds * 1000", "", True)],
                    {"h": 5, "w": 5, "x": 6, "y": y}, "dateTimeAsLocal", fijo(AZUL)))
paneles.append(stat("Duración total", "lambda26_u3_duracion_total_s / 60 (compuertas + batch + streaming + publicación).",
                    [q("lambda26_u3_duracion_total_s / 60", "", True)],
                    {"h": 5, "w": 4, "x": 11, "y": y}, "m", fijo(AZUL), 2))
paneles.append(stat("Compuertas aprobadas", "lambda26_u3_compuertas_ok", [q("lambda26_u3_compuertas_ok", "", True)],
                    {"h": 5, "w": 3, "x": 15, "y": y}, "none", fijo(VERDE), 0))
paneles.append(stat("Compuertas totales", "lambda26_u3_compuertas_total", [q("lambda26_u3_compuertas_total", "", True)],
                    {"h": 5, "w": 3, "x": 18, "y": y}, "none", fijo(AZUL), 0))
paneles.append(stat("Compuertas fallidas", "lambda26_u3_compuertas_total - lambda26_u3_compuertas_ok",
                    [q("lambda26_u3_compuertas_total - lambda26_u3_compuertas_ok", "", True)],
                    {"h": 5, "w": 3, "x": 21, "y": y}, "none", umbrales((VERDE, None), (ROJO, 1)), 0))
y += 5

paneles.append(cumpl := bargauge(
    "Cumplimiento de compuertas",
    "100 × lambda26_u3_compuertas_ok / lambda26_u3_compuertas_total. Incluye las 2 compuertas de BI: el script re-publica "
    "el conteo final después de evaluarlas, así que coincide con salidas/u3/manifiestos/manifiesto_<run_id>.json.",
    [q("100 * lambda26_u3_compuertas_ok / lambda26_u3_compuertas_total", "Compuertas aprobadas", True)],
    {"h": 5, "w": 6, "x": 0, "y": y}, "percent", umbrales((ROJO, None), (AMARILLO, 90), (VERDE, 100)), min_=0, max_=100))
cumpl["options"]["displayMode"] = "basic"
paneles.append(stat("Eventos leídos de Kafka", "lambda26_u3_eventos_leidos (topic citibike-rides durante la corrida U3).",
                    [q("lambda26_u3_eventos_leidos", "", True)], {"h": 5, "w": 4, "x": 6, "y": y}, "locale", fijo(AZUL), 0))
paneles.append(stat("Eventos válidos", "lambda26_u3_ratio_validos (eventos que cumplen el contrato / eventos leídos).",
                    [q("lambda26_u3_ratio_validos", "", True)], {"h": 5, "w": 4, "x": 10, "y": y}, "percentunit",
                    umbrales((ROJO, None), (AMARILLO, 0.95), (VERDE, 0.99)), 3))
paneles.append(stat("Ventanas cerradas", "lambda26_u3_ventanas_cerradas (ventanas de 15 min cerradas por el watermark).",
                    [q("lambda26_u3_ventanas_cerradas", "", True)], {"h": 5, "w": 4, "x": 14, "y": y}, "none",
                    umbrales((ROJO, None), (VERDE, 3)), 0))
paneles.append(stat("Descartados por watermark", "lambda26_u3_descartados_por_watermark (viaje tardío inyectado a propósito).",
                    [q("lambda26_u3_descartados_por_watermark", "", True)], {"h": 5, "w": 3, "x": 18, "y": y}, "none", fijo(AZUL), 0))
paneles.append(stat("Eventos inválidos", "lambda26_u3_eventos_invalidos (evento inválido inyectado a propósito y detectado).",
                    [q("lambda26_u3_eventos_invalidos", "", True)], {"h": 5, "w": 3, "x": 21, "y": y}, "none", fijo(AMARILLO), 0))
y += 5

paneles.append(stat("Latencia p50 (corrida U3)", "lambda26_u3_latencia_p50_ms: percentil 50 de publishedAt → Spark.",
                    [q("lambda26_u3_latencia_p50_ms", "", True)], {"h": 5, "w": 4, "x": 0, "y": y}, "ms",
                    umbrales((VERDE, None), (AMARILLO, 30000), (ROJO, 60000)), 2))
paneles.append(stat("Latencia p95 (corrida U3)", "lambda26_u3_latencia_p95_ms: percentil 95. Compuerta U3: p95 < 60 s.",
                    [q("lambda26_u3_latencia_p95_ms", "", True)], {"h": 5, "w": 4, "x": 0, "y": y + 5}, "ms",
                    umbrales((VERDE, None), (AMARILLO, 30000), (ROJO, 60000)), 2))
paneles.append(serie(
    "Latencia de ingesta en vivo (publishedAt → Spark)",
    "Streaming U2_02: lambda26_u2esp_calidad_latencia_prom_ms (promedio) y lambda26_u2esp_calidad_latencia_p95_ms (p95) por "
    "topic. El streaming en vivo publica promedio y p95; el p50 lo calcula la corrida U3 (tarjeta de la izquierda).",
    [q("lambda26_u2esp_calidad_latencia_prom_ms", "{{topic}} · promedio", ref="A"),
     q("lambda26_u2esp_calidad_latencia_p95_ms", "{{topic}} · p95", ref="B")],
    {"h": 10, "w": 8, "x": 4, "y": y}, "ms",
    [{"matcher": {"id": "byRegexp", "options": ".*p95"}, "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "orange"}}]},
     {"matcher": {"id": "byRegexp", "options": ".*promedio"}, "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "blue"}}]}],
    eje="Latencia"))
paneles.append(serie(
    "Duración del micro-lote (u2z_actividad) vs. trigger de 5 s",
    "Percentiles móviles de 5 min sobre lambda26_u2esp_spark_batch_duration_ms{query=\"u2z_actividad\"} "
    "(quantile_over_time 0,50 y 0,95). La línea/zona roja marca el trigger de 5 s: por encima, Spark no alcanza a procesar "
    "cada micro-lote dentro de su intervalo. Corrida U3: micro-lote p50/p95 = lambda26_u3_microlote_p50_ms / _p95_ms.",
    [q('quantile_over_time(0.5, lambda26_u2esp_spark_batch_duration_ms{query="u2z_actividad"}[5m])', "p50 (5 min)", ref="A"),
     q('quantile_over_time(0.95, lambda26_u2esp_spark_batch_duration_ms{query="u2z_actividad"}[5m])', "p95 (5 min)", ref="B"),
     q('lambda26_u2esp_spark_batch_duration_ms{query="u2z_actividad"}', "micro-lote", ref="C")],
    {"h": 10, "w": 12, "x": 12, "y": y}, "ms",
    [{"matcher": {"id": "byName", "options": "p50 (5 min)"}, "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "green"}}]},
     {"matcher": {"id": "byName", "options": "p95 (5 min)"}, "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "orange"}}]},
     {"matcher": {"id": "byName", "options": "micro-lote"},
      "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "blue"}}, {"id": "custom.lineWidth", "value": 1},
                     {"id": "custom.fillOpacity", "value": 0}]}],
    th=umbrales((VERDE, None), (ROJO, 5000)), th_estilo="line+area", eje="Duración"))
y += 10

thr = serie(
    "Rendimiento del pipeline: Kafka → Spark",
    "Spark: lambda26_u2esp_spark_input_rows_per_s y _processed_rows_per_s por consulta (filas/s). Kafka: "
    "rate(kafka_topic_partition_current_offset[1m]) por topic (mensajes/s, kafka-exporter). Retraso (eje derecho): "
    "lambda26_u2esp_spark_offsets_behind_latest_max, offsets que Spark aún no ha leído.",
    [q("lambda26_u2esp_spark_processed_rows_per_s", "Spark procesadas · {{query}}", ref="A"),
     q("lambda26_u2esp_spark_input_rows_per_s", "Spark entrada · {{query}}", ref="B"),
     q('sum by (topic) (rate(kafka_topic_partition_current_offset{topic=~"citibike-rides|citibike-estaciones"}[1m]))',
       "Kafka ingesta · {{topic}}", ref="C"),
     q("lambda26_u2esp_spark_offsets_behind_latest_max", "Retraso offsets · {{query}}", ref="D")],
    {"h": 10, "w": 24, "x": 0, "y": y}, "short",
    [{"matcher": {"id": "byRegexp", "options": "Retraso.*"},
      "properties": [{"id": "custom.axisPlacement", "value": "right"}, {"id": "unit", "value": "none"},
                     {"id": "custom.axisLabel", "value": "Offsets pendientes"},
                     {"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [6, 4]}}, {"id": "custom.fillOpacity", "value": 0}]},
     {"matcher": {"id": "byRegexp", "options": "Kafka.*"}, "properties": [{"id": "unit", "value": "reqps"}]}],
    eje="Filas / mensajes por segundo")
paneles.append(thr)

# ---------------------------------------------------------------- dashboard
dashboard = {
    "uid": "citibike-general-cristhian",
    "title": "CitiBike Intelligent Mobility — Cristhian Chuquitarqui · Análisis espacial (U1 · U2 · U3)",
    "description": "Análisis histórico, predicción espacial en tiempo real e integración de un sistema Big Data para anticipar "
                   "la actividad de Citi Bike NYC.",
    "tags": ["citibike", "espacial", "U1", "U2", "U3", "cristhian"],
    "editable": True, "graphTooltip": 1, "style": "dark", "schemaVersion": 39, "version": 1,
    "time": {"from": "now-30m", "to": "now"}, "refresh": "5s",
    "timepicker": {"refresh_intervals": ["5s", "10s", "30s", "1m", "5m"]},
    "templating": {"list": [
        {"name": "ds", "label": "Fuente de datos", "type": "datasource", "query": "prometheus",
         "current": {"text": "Prometheus", "value": "Prometheus"}, "hide": 0},
        {"name": "zona", "label": "Zona (U2)", "type": "query", "datasource": PROM,
         "query": {"query": "label_values(lambda26_u2esp_actividad_observada, zona)", "refId": "zona"},
         "definition": "label_values(lambda26_u2esp_actividad_observada, zona)",
         "multi": True, "includeAll": True, "allValue": ".*",
         "current": {"text": ZONAS_DEFECTO, "value": ZONAS_DEFECTO}, "refresh": 2, "sort": 1, "hide": 0}]},
    "annotations": {"list": []},
    "panels": paneles,
}
SALIDA.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"Dashboard escrito en {SALIDA.relative_to(RAIZ)} · {len(paneles)} paneles · periodo U1 {PERIODO}")
