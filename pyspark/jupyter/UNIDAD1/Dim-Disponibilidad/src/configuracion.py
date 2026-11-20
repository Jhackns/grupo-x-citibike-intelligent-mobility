"""Configuración local sin dependencias externas ni evaluación de código dotenv."""

import os
import re
import shlex
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

DIMENSION = Path(__file__).resolve().parents[1]
FUENTES = ("station_status", "station_information", "vehicle_types")


class ErrorConfiguracion(ValueError):
    """Mensaje apto para mostrar: nunca contiene valores de credenciales."""


def leer_env(path: Path) -> dict[str, str]:
    """Admite KEY=value, comillas y comentarios; no expande $VAR ni ejecuta shell."""
    try:
        lines = path.read_text(encoding="utf-8-sig").splitlines()
    except OSError:
        raise ErrorConfiguracion("No se puede leer --env-file; comprueba la ruta y permisos.") from None
    values = {}
    for number, line in enumerate(lines, 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        name, sep, value = line.partition("=")
        name = name.strip()
        if not sep or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ErrorConfiguracion(f"Formato dotenv inválido en línea {number}.")
        try:
            tokens = shlex.split(value, comments=True, posix=True)
        except ValueError:
            raise ErrorConfiguracion(f"Comillas inválidas en línea {number}.") from None
        if len(tokens) > 1:
            raise ErrorConfiguracion(f"Usa comillas para valores con espacios: línea {number}.")
        values[name] = tokens[0] if tokens else ""
    return values


@dataclass(frozen=True)
class Configuracion:
    url: str = field(repr=False)
    clave: str = field(repr=False)
    bucket: str
    raw: Path = DIMENSION / "data" / "raw"
    reports: Path = DIMENSION / "reports" / "descargas"
    timeout: float = 30.0
    intentos: int = 3
    page_size: int = 100


def cargar_configuracion(env_file=None, credencial="publicable", **options) -> Configuracion:
    if env_file is not None:
        path = Path(env_file).expanduser()
    else:
        # No se busca fuera del proyecto ni se mezclan varios archivos de entorno.
        candidates = (DIMENSION / ".env", DIMENSION.parents[2] / ".env.u2",
                      DIMENSION.parents[3] / ".env.u2")
        path = next((p for p in candidates if p.is_file()), None)
    values = leer_env(path) if path else {}
    values.update(os.environ)
    key_name = {"publicable": "NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY",
                "secreta": "SUPABASE_SECRET_KEY"}[credencial]
    missing = [name for name in ("NEXT_PUBLIC_SUPABASE_URL", key_name)
               if not values.get(name)]
    if missing:
        raise ErrorConfiguracion("Falta configuración: " + ", ".join(missing)
                                 + ". Usa --env-file /ruta/.env.u2 o variables de entorno.")
    url = values["NEXT_PUBLIC_SUPABASE_URL"].rstrip("/")
    try:
        parsed = urlsplit(url)
        valid = (parsed.scheme == "https" and parsed.hostname and not parsed.username
                 and not parsed.password and not parsed.query and not parsed.fragment
                 and parsed.path in ("", "/"))
    except ValueError:
        valid = False
    if not valid:
        raise ErrorConfiguracion("NEXT_PUBLIC_SUPABASE_URL debe ser un origen HTTPS válido.")
    bucket = values.get("SUPABASE_BUCKET", "citibike-raw")
    if not re.fullmatch(r"[A-Za-z0-9_-]+", bucket):
        raise ErrorConfiguracion("SUPABASE_BUCKET no es un identificador válido.")
    if options.get("intentos", 3) < 1 or options.get("timeout", 30) <= 0:
        raise ErrorConfiguracion("Intentos y timeout deben ser positivos.")
    if not 1 <= options.get("page_size", 100) <= 1000:
        raise ErrorConfiguracion("El tamaño de página debe estar entre 1 y 1000.")
    return Configuracion(url=url, clave=values[key_name], bucket=bucket, **options)
