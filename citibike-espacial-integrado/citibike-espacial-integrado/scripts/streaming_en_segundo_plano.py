"""
Deja el streaming de U2 corriendo en segundo plano, sin Jupyter, para que el dashboard general de Grafana muestre datos en vivo.
Ejecuta las MISMAS celdas del notebook integrado que tienen la etiqueta `u2_streaming` (U2 · Parte B, hasta "Arrancar las consultas").

Arrancar (desde la raíz del proyecto):
    docker compose exec -d spark python scripts/streaming_en_segundo_plano.py
Ver el log:
    docker compose exec spark tail -n 20 /tmp/u2_02_stream.log
Detener (antes de volver a correr el notebook o la U3):
    docker compose exec spark python scripts/streaming_en_segundo_plano.py --detener
"""
import json
import os
import signal
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
NOTEBOOK = RAIZ / "notebook" / "CitiBike_Integrado_U1_U2_U3.ipynb"
LOG = "/tmp/u2_02_stream.log"
ETIQUETA = "u2_streaming"


def detener():
    propio, n = os.getpid(), 0
    for p in os.listdir("/proc"):
        if not p.isdigit() or int(p) == propio:
            continue
        try:
            cmd = open(f"/proc/{p}/cmdline", "rb").read().split(b"\0")
        except OSError:
            continue
        if any(c.endswith(b"streaming_en_segundo_plano.py") for c in cmd) and b"--detener" not in cmd:
            os.kill(int(p), signal.SIGTERM)
            n += 1
    print(f"Streaming detenido ({n} proceso/s)." if n else "No había streaming en segundo plano.")


def main():
    if "--detener" in sys.argv:
        detener()
        return
    celdas = json.loads(NOTEBOOK.read_text(encoding="utf-8"))["cells"]
    codigo = ["".join(c["source"]) if isinstance(c["source"], list) else c["source"]
              for c in celdas if c["cell_type"] == "code" and ETIQUETA in (c.get("metadata", {}).get("tags") or [])]
    if not codigo:
        sys.exit(f"No hay celdas con la etiqueta {ETIQUETA} en {NOTEBOOK.name}")
    log = open(LOG, "w", buffering=1, encoding="utf-8")
    sys.stdout = sys.stderr = log
    os.chdir(NOTEBOOK.parent)            # el notebook usa rutas relativas (../salidas/u2)
    ns = {"__name__": "__main__"}
    for i, src in enumerate(codigo):
        exec(compile(src, f"celda_u2_streaming_{i}", "exec"), ns)
    print("STREAMING EN MARCHA", flush=True)
    ns["spark"].streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
