import urllib.request
import json

GRAFANA_URL = "http://localhost:43000/api/dashboards/db"
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": "Basic YWRtaW46YWRtaW4="  # admin:admin codificado en base64
}

dashboard_json = {
  "dashboard": {
    "id": None,
    "title": "U2 - CitiBike Demanda Inteligente (Streaming & ML)",
    "tags": ["citibike", "u2", "ml"],
    "timezone": "browser",
    "refresh": "5s",
    "time": {
      "from": "now-5m",
      "to": "now"
    },
    "panels": [
      {
        "id": 1,
        "title": "Ratio de Eventos Válidos (Batch)",
        "type": "stat",
        "gridPos": {"x": 0, "y": 0, "w": 12, "h": 5},
        "targets": [
          {"expr": "lambda26_bi_eventos_validos_ratio", "refId": "A"}
        ],
        "fieldConfig": {
          "defaults": {
            "unit": "percentunit",
            "color": {"mode": "thresholds"},
            "thresholds": {
              "mode": "absolute",
              "steps": [
                {"color": "red", "value": None},
                {"color": "green", "value": 0.9}
              ]
            }
          }
        }
      },
      {
        "id": 2,
        "title": "Viajes Históricos Procesados (Batch S09/U1)",
        "type": "stat",
        "gridPos": {"x": 12, "y": 0, "w": 12, "h": 5},
        "targets": [
          {"expr": "lambda26_bi_eventos_total", "refId": "A"}
        ],
        "fieldConfig": {
          "defaults": {
            "color": {"mode": "palette-classic"}
          }
        }
      },
      {
        "id": 3,
        "title": "Demanda de Viajes en Tiempo Real vs. Predicción t+1 (Streaming ML)",
        "type": "timeseries",
        "gridPos": {"x": 0, "y": 5, "w": 24, "h": 9},
        "targets": [
          {
            "expr": "lambda26_ml_ce_real",
            "legendFormat": "Viajes Reales (t)",
            "refId": "A"
          },
          {
            "expr": "lambda26_ml_prediccion_siguiente",
            "legendFormat": "Demanda Predicha (t+1)",
            "refId": "B"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "custom": {
              "drawStyle": "line",
              "lineInterpolation": "smooth",
              "pointSize": 5,
              "showPoints": "auto"
            }
          }
        }
      }
    ],
    "schemaVersion": 39,
    "version": 1
  },
  "overwrite": True
}

req = urllib.request.Request(GRAFANA_URL, data=json.dumps(dashboard_json).encode("utf-8"), headers=HEADERS, method="POST")
try:
    with urllib.request.urlopen(req) as resp:
        resultado = json.loads(resp.read().decode())
        print("Tablero creado exitosamente en Grafana:")
        print(f"URL: http://localhost:43000{resultado.get('url')}")
except Exception as e:
    print("Error al crear el tablero en Grafana:", e)
