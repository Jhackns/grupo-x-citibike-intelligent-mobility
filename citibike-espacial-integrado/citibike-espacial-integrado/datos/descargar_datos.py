"""
Descarga los 5 CSV de Citi Bike de julio 2026 (~974 MB) en datos/ (solo con la biblioteca estándar de Python).

Uso, desde la raíz del proyecto:
    docker compose exec spark python datos/descargar_datos.py      # dentro del contenedor (no necesitas Python local)
    python datos/descargar_datos.py                                 # o con Python local
"""
import sys
import urllib.request
import zipfile
from pathlib import Path

URL = "https://s3.amazonaws.com/tripdata/202607-citibike-tripdata.zip"
DATOS = Path(__file__).resolve().parent
ESPERADOS = [f"202607-citibike-tripdata_{i}.csv" for i in range(1, 6)]


def main():
    if all((DATOS / f).exists() for f in ESPERADOS):
        print("Los 5 CSV ya están en datos/. Nada que hacer.")
        return
    destino = DATOS / "tripdata.zip"
    print(f"Descargando {URL} (~974 MB)...")

    def progreso(bloques, tam, total):
        if total > 0 and bloques % 400 == 0:
            print(f"  {min(100, 100 * bloques * tam / total):5.1f} %", flush=True)

    urllib.request.urlretrieve(URL, destino, progreso)
    print("Extrayendo CSV...")
    with zipfile.ZipFile(destino) as z:
        for info in z.infolist():
            nombre = Path(info.filename).name
            if nombre.endswith(".csv") and not info.filename.startswith("__MACOSX"):
                with z.open(info) as src, open(DATOS / nombre, "wb") as dst:
                    while bloque := src.read(1 << 20):
                        dst.write(bloque)
                print("  ", nombre)
    destino.unlink()
    faltan = [f for f in ESPERADOS if not (DATOS / f).exists()]
    if faltan:
        sys.exit(f"Faltan archivos: {faltan}")
    print("Listo: 5 CSV en datos/")


if __name__ == "__main__":
    main()
