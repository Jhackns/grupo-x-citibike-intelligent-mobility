"""
U3 · Prueba de no regresión (S14): compara dos manifiestos de la ejecución integrada.

    python scripts/U3_comparar_manifiestos.py                 # las dos corridas más recientes
    python scripts/U3_comparar_manifiestos.py A.json B.json   # dos corridas concretas (antes, después)

Escribe la tabla en salidas/u3/no_regresion_<antes>_vs_<despues>.md (lista para pegar en el informe).
"""
import json
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
CARPETA = RAIZ / "salidas/u3/manifiestos"

FILAS = [  # (etiqueta, clave en kpis, mejor si es "mayor" / "menor" / None)
    ("Eventos leídos de citibike-rides", "eventos_leidos", None),
    ("Ratio de eventos válidos", "ratio_validos", "mayor"),
    ("Ventanas de 15 min cerradas", "ventanas_cerradas", "mayor"),
    ("Descartados por watermark (prueba)", "descartados_por_watermark", None),
    ("Latencia p50 (ms)", "latencia_p50_ms", "menor"),
    ("Latencia p95 (ms)", "latencia_p95_ms", "menor"),
    ("Micro-lote p50 (ms)", "microlote_p50_ms", "menor"),
    ("Micro-lote p95 (ms)", "microlote_p95_ms", "menor"),
    ("MAE en vivo (modelo U2)", "mae_en_vivo", "menor"),
    ("MAE en test (referencia)", "mae_test", None),
    ("Duración total (s)", "duracion_total_s", None),
]


def fmt(v):
    if v is None:
        return "—"
    if isinstance(v, float):
        return f"{v:,.5f}".rstrip("0").rstrip(".")
    return f"{v:,}" if isinstance(v, int) else str(v)


def main():
    rutas = [Path(p) for p in sys.argv[1:3]] if len(sys.argv) >= 3 else sorted(CARPETA.glob("manifiesto_*.json"))[-2:]
    if len(rutas) < 2:
        sys.exit("Hacen falta dos manifiestos: corre U3_ejecucion_integrada.py dos veces.")
    a, b = (json.loads(p.read_text(encoding="utf-8")) for p in rutas)
    lineas = [f"# No regresión: {a['run_id']} (antes) vs {b['run_id']} (después)", "",
              "| Comprobación | Antes | Después | ¿Regresión? |", "|---|---:|---:|:---:|",
              f"| Estado de la corrida | {a['estado']} | {b['estado']} | {'sí' if a['estado'] == 'OK' and b['estado'] != 'OK' else 'no'} |",
              f"| Compuertas OK | {a['resumen_compuertas']['ok']}/{a['resumen_compuertas']['total']} | "
              f"{b['resumen_compuertas']['ok']}/{b['resumen_compuertas']['total']} | "
              f"{'sí' if b['resumen_compuertas']['ok'] / b['resumen_compuertas']['total'] < a['resumen_compuertas']['ok'] / a['resumen_compuertas']['total'] else 'no'} |"]
    for etiqueta, clave, mejor in FILAS:
        va, vb = a["kpis"].get(clave), b["kpis"].get(clave)
        reg = "—"
        if mejor and va is not None and vb is not None:
            peor = vb < va * 0.9 if mejor == "mayor" else vb > va * 1.25   # tolerancia: 10 % / 25 %
            reg = "sí" if peor else "no"
        lineas.append(f"| {etiqueta} | {fmt(va)} | {fmt(vb)} | {reg} |")
    texto = "\n".join(lineas) + "\n"
    print(texto)
    salida = RAIZ / "salidas/u3" / f"no_regresion_{a['run_id']}_vs_{b['run_id']}.md"
    salida.write_text(texto, encoding="utf-8")
    print(f"Guardado en {salida.relative_to(RAIZ)}")


if __name__ == "__main__":
    main()
