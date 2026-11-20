#!/usr/bin/env python3
"""Descarga incremental de originales GBFS. Solo POST de listado y GET de lectura."""

import argparse
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
import fcntl
import gzip
import hashlib
import http.client
import json
import os
from pathlib import Path, PurePosixPath
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib

if __package__:
    from .configuracion import DIMENSION, FUENTES, ErrorConfiguracion, cargar_configuracion
else:
    from configuracion import DIMENSION, FUENTES, ErrorConfiguracion, cargar_configuracion

UTC = timezone.utc
TIMESTAMP = re.compile(
    r"^(\d{4}-\d{2}-\d{2})T(\d{2})-(\d{2})-(\d{2})-(\d{3})Z-[A-Za-z0-9-]+\.json\.gz$"
)


class ErrorDescarga(Exception):
    """Errores controlados que no incluyen URL, cabeceras o respuestas remotas."""


class ErrorRemoto(ErrorDescarga):
    def __init__(self, status=None):
        self.status = status
        super().__init__(f"HTTP {status}" if status else "Error de red o respuesta incompleta")


class ErrorIntegridad(ErrorDescarga):
    pass


class SinRedirecciones(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # Nunca reenviar credenciales a otro origen mediante un redirect.
        raise ErrorDescarga("Redirección HTTP rechazada")


def ahora():
    return datetime.now(UTC).isoformat()


def intervalo(desde, hasta):
    try:
        if not all(re.fullmatch(r"\d{4}-\d{2}-\d{2}", day) for day in (desde, hasta)):
            raise ValueError
        first, last = date.fromisoformat(desde), date.fromisoformat(hasta)
        if first > last:
            raise ValueError
        return (datetime.combine(first, datetime.min.time(), UTC),
                datetime.combine(last + timedelta(days=1), datetime.min.time(), UTC))
    except (ValueError, OverflowError):
        raise ErrorConfiguracion("Usa fechas YYYY-MM-DD, con --desde <= --hasta.") from None


def validar_ruta(path):
    parts = path.split("/")
    if (not path or any(p in ("", ".", "..") for p in parts)
            or "\\" in path or any(ord(c) < 32 for c in path)):
        raise ErrorDescarga("Ruta remota insegura o no soportada")
    return PurePosixPath(path)


def ruta_local(root, path):
    relative = validar_ruta(path)
    root = root.resolve()
    current = root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise ErrorDescarga("Enlace simbólico rechazado dentro del destino")
    return current


def fecha_captura(path):
    """Timestamp UTC del nombre observado; no sustituirlo por updated_at de Storage."""
    parts = validar_ruta(path).parts
    match = TIMESTAMP.fullmatch(parts[-1])
    if not match:
        raise ErrorDescarga("Nombre de captura sin timestamp UTC reconocido")
    day, hour, minute, second, millis = match.groups()
    try:
        stamp = datetime.fromisoformat(f"{day}T{hour}:{minute}:{second}.{millis}+00:00")
    except ValueError:
        raise ErrorDescarga("Timestamp de captura inválido") from None
    # Estructura observada: fuente/fecha/[subcarpetas]/captura.json.gz.
    if len(parts) > 2 and re.fullmatch(r"\d{4}-\d{2}-\d{2}", parts[1]) and parts[1] != day:
        raise ErrorDescarga("La fecha de carpeta no coincide con la del nombre")
    return stamp


def huella_remota(remote):
    meta = remote.get("metadata") or {}
    # last_accessed_at puede cambiar al leer: conservarlo, pero no usarlo como versión.
    return {"id": remote.get("id"), "version": remote.get("version"),
            "updated_at": remote.get("updated_at"), "etag": meta.get("eTag"),
            "size": meta.get("size"), "lastModified": meta.get("lastModified")}


def sha256_file(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def sin_cambios(path, entry, remote):
    if not entry or not path.is_file() or entry.get("remote_fingerprint") != huella_remota(remote):
        return False
    fp = huella_remota(remote)
    if not any(fp.get(k) for k in ("version", "updated_at", "etag", "lastModified")):
        return False  # Sin versión remota no asumir que un tamaño igual implica igualdad.
    return (path.stat().st_size == entry.get("local_size")
            and sha256_file(path) == entry.get("sha256"))


class Storage:
    def __init__(self, config, opener=None, sleep=time.sleep):
        self.config = config
        self.opener = opener or urllib.request.build_opener(SinRedirecciones())
        self.sleep = sleep
        self.reintentos = 0

    def abrir(self, path, body=None):
        headers = {"apikey": self.config.clave, "Accept-Encoding": "identity",
                   "Cache-Control": "no-cache", "Content-Type": "application/json"}
        if self.config.clave.startswith("eyJ"):
            headers["Authorization"] = "Bearer " + self.config.clave
        request = urllib.request.Request(
            self.config.url + "/storage/v1" + path,
            data=json.dumps(body).encode() if body is not None else None,
            headers=headers, method="POST" if body is not None else "GET")
        try:
            return self.opener.open(request, timeout=self.config.timeout)
        except urllib.error.HTTPError as exc:
            status = exc.code
            exc.close()
            raise ErrorRemoto(status) from None
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            raise ErrorRemoto() from None

    def reintentar(self, operation):
        for attempt in range(self.config.intentos):
            try:
                return operation()
            except (ErrorRemoto, ErrorIntegridad, http.client.HTTPException,
                    TimeoutError, ConnectionError, urllib.error.URLError) as exc:
                if isinstance(exc, ErrorRemoto) and exc.status is not None:
                    if exc.status not in (408, 425, 429) and exc.status < 500:
                        raise
                if attempt + 1 == self.config.intentos:
                    if isinstance(exc, ErrorDescarga):
                        raise
                    raise ErrorRemoto() from None
                self.reintentos += 1
                self.sleep(min(2 ** attempt, 8))

    def pagina(self, prefix, offset):
        def read():
            with self.abrir("/object/list/" + urllib.parse.quote(self.config.bucket, safe=""),
                           {"prefix": prefix, "limit": self.config.page_size, "offset": offset,
                            "sortBy": {"column": "name", "order": "asc"}}) as response:
                try:
                    data = json.load(response)
                except (ValueError, UnicodeError):
                    raise ErrorRemoto() from None
            if not isinstance(data, list) or any(not isinstance(x, dict) for x in data):
                raise ErrorDescarga("El listado remoto no tiene el formato esperado")
            return data
        return self.reintentar(read)

    def objetos(self, fuentes, start, end):
        pending = [""]
        visited = set()
        while pending:
            prefix = pending.pop()
            if prefix in visited:
                continue
            visited.add(prefix)
            offset = 0
            previous_page = None
            while True:
                page = self.pagina(prefix, offset)
                names = [item.get("name") for item in page]
                if page and names == previous_page:
                    raise ErrorDescarga("Paginación sin avance; se interrumpe el inventario")
                previous_page = names
                for item in page:
                    name = item.get("name")
                    if not isinstance(name, str) or "/" in name:
                        raise ErrorDescarga("Nombre de entrada inesperado en el listado")
                    path = f"{prefix}/{name}" if prefix else name
                    parts = validar_ruta(path).parts
                    if fuentes and parts[0] not in fuentes:
                        continue
                    if item.get("is_delete_marker"):
                        continue
                    if item.get("id") is None and item.get("metadata") is None:
                        if len(parts) == 2 and re.fullmatch(r"\d{4}-\d{2}-\d{2}", parts[1]):
                            try:
                                day = date.fromisoformat(parts[1])
                            except ValueError:
                                raise ErrorDescarga("Carpeta remota con fecha inválida") from None
                            if not start.date() <= day < end.date():
                                continue
                        pending.append(path)
                    elif path.endswith(".json.gz"):
                        yield path, item
                if len(page) < self.config.page_size:
                    break
                offset += len(page)

    def descargar(self, path, remote, target):
        temporary = target.with_name(target.name + ".part")
        if temporary.is_symlink():
            raise ErrorDescarga("Temporal simbólico rechazado")
        target.parent.mkdir(parents=True, exist_ok=True)

        def transfer():
            endpoint = ("/object/" + urllib.parse.quote(self.config.bucket, safe="")
                        + "/" + urllib.parse.quote(path, safe="/"))
            with self.abrir(endpoint) as response, temporary.open("wb") as output:
                headers = {k: response.headers.get(k) for k in
                           ("ETag", "Last-Modified", "Content-Length", "Content-Type")}
                digest = hashlib.sha256()
                size = 0
                while True:
                    block = response.read(1024 * 1024)
                    if not block:
                        break
                    output.write(block)
                    digest.update(block)
                    size += len(block)
                output.flush()
                os.fsync(output.fileno())
            meta = remote.get("metadata") or {}
            for expected in (headers.get("Content-Length"), meta.get("size"), meta.get("contentLength")):
                if expected is not None and size != int(expected):
                    raise ErrorIntegridad("La transferencia no coincide con el tamaño esperado")
            if meta.get("eTag") and headers.get("ETag"):
                if str(meta["eTag"]).strip('"') != headers["ETag"].strip('"'):
                    raise ErrorIntegridad("ETag distinto del listado; vuelve a inventariar el objeto")
            try:
                # Comprobar CRC y fin de gzip sin alterar ni reserializar el original.
                with gzip.open(temporary, "rb") as compressed:
                    while compressed.read(1024 * 1024):
                        pass
            except (gzip.BadGzipFile, EOFError, zlib.error):
                raise ErrorIntegridad("Captura gzip incompleta o corrupta") from None
            os.replace(temporary, target)
            return {"local_size": size, "sha256": digest.hexdigest(), "http": headers}

        # Si falla o se interrumpe, el .part no cuenta como éxito y se reinicia desde cero.
        return self.reintentar(transfer)


def guardar_json(path, value):
    temporary = path.with_name(path.name + ".tmp")
    if path.is_symlink() or temporary.is_symlink():
        raise ErrorDescarga("Archivo de control simbólico rechazado")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


@contextmanager
def bloqueo(reports):
    reports.mkdir(parents=True, exist_ok=True)
    lock = reports / "descarga.lock"
    if lock.is_symlink():
        raise ErrorDescarga("Archivo de bloqueo simbólico rechazado")
    with lock.open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ErrorDescarga("Ya hay otra descarga usando este directorio de informes") from None
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def leer_manifiesto(config):
    # Vincular al proyecto/bucket sin registrar la URL ni la clave.
    scope = hashlib.sha256((config.url + "/" + config.bucket).encode()).hexdigest()
    path = config.reports / "manifiesto.json"
    if path.is_symlink():
        raise ErrorDescarga("Manifiesto simbólico rechazado")
    if not path.exists():
        return {"schema_version": 1, "source_id": scope, "objects": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        valid = (data["schema_version"] == 1 and data["source_id"] == scope
                 and isinstance(data["objects"], dict)
                 and all(isinstance(v, dict) for v in data["objects"].values()))
    except (ValueError, KeyError, TypeError):
        valid = False
    if not valid:
        raise ErrorDescarga("Manifiesto inválido o de otro origen; no se sobrescribirá")
    return data


def ejecutar(config, start, end, fuentes=FUENTES, dry_run=False, storage=None):
    storage = storage or Storage(config)
    manifest = leer_manifiesto(config)
    summary = {"inicio_utc": ahora(), "desde_utc": start.isoformat(), "hasta_exclusivo_utc": end.isoformat(),
               "fuentes": list(fuentes) if fuentes else "todas", "dry_run": dry_run,
               "encontrados": 0, "seleccionados": 0, "omitidos": 0, "descargados": 0,
               "por_descargar": 0, "fallidos": 0, "bytes_descargados": 0,
               "registrados_ausentes": 0, "errores": []}
    for path in manifest["objects"]:
        if not ruta_local(config.raw, path).is_file():
            summary["registrados_ausentes"] += 1
    try:
        for path, remote in storage.objetos(fuentes, start, end):
            summary["encontrados"] += 1
            try:
                stamp = fecha_captura(path)
                if not start <= stamp < end:
                    continue
                summary["seleccionados"] += 1
                target = ruta_local(config.raw, path)
                previous = manifest["objects"].get(path)
                if sin_cambios(target, previous, remote):
                    summary["omitidos"] += 1
                    continue
                if dry_run:
                    summary["por_descargar"] += 1
                    continue
                details = storage.descargar(path, remote, target)
                manifest["objects"][path] = {"remote": remote, "remote_fingerprint": huella_remota(remote),
                                             "capture_time_utc": stamp.isoformat(),
                                             "downloaded_at_utc": ahora(), **details}
                # Commit del manifiesto solo después de publicar el archivo completo.
                guardar_json(config.reports / "manifiesto.json", manifest)
                summary["descargados"] += 1
                summary["bytes_descargados"] += details["local_size"]
                if summary["descargados"] % 25 == 0:
                    print(f"Progreso: {summary['descargados']} archivos completos", flush=True)
            except (ErrorDescarga, OSError, ValueError) as exc:
                summary["fallidos"] += 1
                reason = str(exc) if isinstance(exc, ErrorDescarga) else type(exc).__name__
                summary["errores"].append({"path": path, "error": reason})
                # Permisos no se arreglan repitiendo la petición para cientos de objetos.
                if isinstance(exc, ErrorRemoto) and exc.status in (401, 403):
                    break
    except ErrorDescarga as exc:
        summary["fallidos"] += 1
        summary["errores"].append({"operacion": "listado", "error": str(exc)})
    summary["fin_utc"] = ahora()
    summary["reintentos"] = storage.reintentos
    summary["estado"] = ("con_errores" if summary["fallidos"] else
                         "sin_capturas_visibles" if not summary["seleccionados"] else "ok")
    if not dry_run:
        report_name = "resumen-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ") + ".json"
        guardar_json(config.reports / report_name, summary)
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, help="Ruta a .env.u2; el entorno del proceso tiene prioridad")
    parser.add_argument("--credencial", choices=("publicable", "secreta"), default="publicable",
                        help="Selección explícita, sin elevación automática (default: publicable)")
    parser.add_argument("--desde", required=True, help="Primer día incluido, YYYY-MM-DD en UTC")
    parser.add_argument("--hasta", required=True, help="Último día incluido, YYYY-MM-DD en UTC")
    parser.add_argument("--dry-run", action="store_true", help="Solo listar/comparar; no descargar ni escribir informes")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--fuente", action="append", help="Fuente del bucket; repetible (default: las tres de disponibilidad)")
    group.add_argument("--todas-las-fuentes", action="store_true")
    parser.add_argument("--raw-dir", type=Path, default=DIMENSION / "data" / "raw")
    parser.add_argument("--reports-dir", type=Path, default=DIMENSION / "reports" / "descargas")
    parser.add_argument("--intentos", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--page-size", type=int, default=100)
    args = parser.parse_args(argv)
    try:
        start, end = intervalo(args.desde, args.hasta)
        config = cargar_configuracion(args.env_file, args.credencial, raw=args.raw_dir.resolve(),
                                      reports=args.reports_dir.resolve(), intentos=args.intentos,
                                      timeout=args.timeout, page_size=args.page_size)
        sources = None if args.todas_las_fuentes else (args.fuente or FUENTES)
        if sources and any(not re.fullmatch(r"[A-Za-z0-9_-]+", s) for s in sources):
            raise ErrorConfiguracion("Cada --fuente debe ser un nombre de carpeta raíz.")
        if args.dry_run:
            result = ejecutar(config, start, end, sources, True)
        else:
            with bloqueo(config.reports):
                result = ejecutar(config, start, end, sources)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        if result["estado"] == "sin_capturas_visibles":
            print("No hay capturas visibles en el rango/fuentes. Un listado vacío no prueba que el bucket esté vacío; revisa acceso y fechas.", file=sys.stderr)
        return 1 if result["fallidos"] else 2 if not result["seleccionados"] else 0
    except KeyboardInterrupt:
        print("Interrumpido: los archivos completos conservan su manifiesto; repite el comando para recuperar pendientes.", file=sys.stderr)
        return 130
    except (ErrorConfiguracion, ErrorDescarga) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except (OSError, ValueError, http.client.HTTPException):
        print("Error local o de transporte; comprueba permisos, disco y configuración. No se muestran detalles sensibles.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
