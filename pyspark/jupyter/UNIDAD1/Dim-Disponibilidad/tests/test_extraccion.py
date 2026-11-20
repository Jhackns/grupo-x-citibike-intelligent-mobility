"""Pruebas de descarga e integridad sin red, Spark ni credenciales reales."""

from contextlib import redirect_stdout
from dataclasses import replace
from datetime import timezone
import gzip
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import urllib.error

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.configuracion import Configuracion, ErrorConfiguracion, cargar_configuracion, leer_env
from src.extraccion import (ErrorDescarga, Storage, bloqueo, ejecutar,
                            fecha_captura, intervalo, ruta_local)

DAY = "2026-11-10"
OBJECT = "station_status/2026-11-10/2026-11-10T00-05-02-508Z-abc.json.gz"
BODY = gzip.compress(b'{"data":{"stations":[]},"last_updated":1794269100}')


class Response(io.BytesIO):
    def __init__(self, body, headers=None):
        super().__init__(body)
        self.headers = headers or {}


class Remote:
    """Simula el protocolo Storage, incluidos carpetas, páginas y cuerpo truncado."""
    def __init__(self):
        self.objects = {}
        self.gets = 0
        self.lists = []
        self.failures = 0
        self.http_status = None
        self.add(OBJECT)

    def add(self, path, body=BODY, version="v1"):
        self.objects[path] = (body, {"name": path.split("/")[-1], "id": path,
                                    "version": version, "updated_at": "2026-11-10T00:06:00Z",
                                    "metadata": {"size": len(body), "eTag": version}})

    def open(self, request, timeout):
        if self.http_status:
            raise urllib.error.HTTPError(request.full_url, self.http_status,
                                         "secret response must not leak", {}, None)
        if request.method == "POST":
            params = json.loads(request.data)
            self.lists.append(params)
            prefix = params["prefix"]
            prefix = prefix + "/" if prefix else ""
            entries = {}
            for path, (_, meta) in self.objects.items():
                if not path.startswith(prefix):
                    continue
                suffix = path[len(prefix):]
                child, sep, _ = suffix.partition("/")
                entries[child] = ({"name": child, "id": None, "metadata": None}
                                  if sep else meta)
            page = [entries[k] for k in sorted(entries)]
            offset = params["offset"]
            return Response(json.dumps(page[offset:offset + params["limit"]]).encode())
        if request.method != "GET":
            raise AssertionError("Se intentó una operación remota no permitida")
        path = request.full_url.split("/object/citibike-raw/", 1)[1]
        body, meta = self.objects[path]
        self.gets += 1
        expected = len(body)
        if self.failures:
            self.failures -= 1
            body = body[:8]
        return Response(body, {"Content-Length": str(expected), "ETag": meta["version"]})


class DescargasTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.config = Configuracion("https://test.invalid", "test-key", "citibike-raw",
                                    raw=root / "raw", reports=root / "reports", page_size=2)
        self.remote = Remote()
        self.client = Storage(self.config, self.remote, sleep=lambda _: None)
        self.start, self.end = intervalo(DAY, DAY)

    def run_download(self, dry=False, client=None):
        with redirect_stdout(io.StringIO()):
            if dry:
                return ejecutar(self.config, self.start, self.end, dry_run=True,
                                storage=client or self.client)
            with bloqueo(self.config.reports):
                return ejecutar(self.config, self.start, self.end, storage=client or self.client)

    def test_segunda_ejecucion_no_descarga_y_conserva_bytes(self):
        first = self.run_download()
        second = self.run_download()
        self.assertEqual(first["descargados"], 1)
        self.assertEqual(second["omitidos"], 1)
        self.assertEqual(self.remote.gets, 1)
        self.assertEqual((self.config.raw / OBJECT).read_bytes(), BODY)

    def test_dry_run_no_escribe_ni_descarga(self):
        result = self.run_download(dry=True)
        self.assertEqual(result["por_descargar"], 1)
        self.assertFalse(self.config.raw.exists())
        self.assertFalse(self.config.reports.exists())
        self.assertEqual(self.remote.gets, 0)

    def test_version_modificada_se_descarga(self):
        self.run_download()
        newer = gzip.compress(b'{"new":true}')
        self.remote.add(OBJECT, newer, "v2")
        self.assertEqual(self.run_download()["descargados"], 1)
        self.assertEqual((self.config.raw / OBJECT).read_bytes(), newer)

    def test_archivo_local_ausente_se_recupera(self):
        self.run_download()
        (self.config.raw / OBJECT).unlink()
        result = self.run_download()
        self.assertEqual(result["registrados_ausentes"], 1)
        self.assertEqual(result["descargados"], 1)

    def test_corrupcion_local_del_mismo_tamano_se_recupera(self):
        self.run_download()
        (self.config.raw / OBJECT).write_bytes(b"x" * len(BODY))
        self.assertEqual(self.run_download()["descargados"], 1)
        self.assertEqual((self.config.raw / OBJECT).read_bytes(), BODY)

    def test_transferencia_truncada_reintenta(self):
        self.remote.failures = 1
        result = self.run_download()
        self.assertEqual(result["descargados"], 1)
        self.assertEqual(result["reintentos"], 1)
        self.assertEqual(self.remote.gets, 2)

    def test_interrupcion_no_registra_exito_y_siguiente_ejecucion_recupera(self):
        self.remote.failures = 3
        result = self.run_download()
        self.assertEqual(result["fallidos"], 1)
        self.assertFalse((self.config.reports / "manifiesto.json").exists())
        self.assertFalse((self.config.raw / OBJECT).exists())
        self.assertTrue((self.config.raw / (OBJECT + ".part")).exists())
        recovered = self.run_download()
        self.assertEqual(recovered["descargados"], 1)
        self.assertFalse((self.config.raw / (OBJECT + ".part")).exists())
        self.assertEqual(self.run_download()["omitidos"], 1)

    def test_actualizacion_fallida_conserva_original_y_manifiesto(self):
        self.run_download()
        old = (self.config.reports / "manifiesto.json").read_bytes()
        self.remote.add(OBJECT, gzip.compress(b"changed"), "v2")
        self.remote.failures = 3
        self.assertEqual(self.run_download()["fallidos"], 1)
        self.assertEqual((self.config.raw / OBJECT).read_bytes(), BODY)
        self.assertEqual((self.config.reports / "manifiesto.json").read_bytes(), old)

    def test_gzip_invalido_no_registra_exito(self):
        self.remote.add(OBJECT, b"not gzip")
        self.assertEqual(self.run_download()["fallidos"], 1)
        self.assertFalse((self.config.raw / OBJECT).exists())

    def test_recursion_paginacion_y_limites_fecha(self):
        for i in range(6):
            self.remote.add(OBJECT.replace("-abc", f"-id{i}"))
        nested = OBJECT.replace("2026-11-10/", "2026-11-10/subcarpeta/")
        self.remote.add(nested)
        self.remote.add(OBJECT.replace("2026-11-10", "2026-11-11"))
        result = self.run_download()
        self.assertEqual(result["descargados"], 8)
        self.assertTrue((self.config.raw / nested).is_file())
        self.assertTrue(any(p["offset"] >= 2 for p in self.remote.lists))
        self.assertFalse(any("2026-11-11" in p["prefix"] for p in self.remote.lists))

    def test_last_accessed_no_fuerza_descarga(self):
        self.run_download()
        self.remote.objects[OBJECT][1]["last_accessed_at"] = "new access"
        self.assertEqual(self.run_download()["omitidos"], 1)

    def test_manifiesto_de_otro_origen_se_rechaza(self):
        self.run_download()
        self.config = replace(self.config, url="https://other.invalid")
        with self.assertRaises(ErrorDescarga):
            self.run_download()

    def test_manifiesto_corrupto_no_se_sobrescribe(self):
        self.config.reports.mkdir()
        path = self.config.reports / "manifiesto.json"
        path.write_text("{")
        with self.assertRaises(ErrorDescarga):
            self.run_download()
        self.assertEqual(path.read_text(), "{")

    def test_rutas_y_symlinks_no_escapan_destino(self):
        for path in ("../x", "/x", "a/../x", "a\\x", "a//x"):
            with self.assertRaises(ErrorDescarga):
                ruta_local(self.config.raw, path)
        self.config.raw.mkdir()
        (self.config.raw / "station_status").symlink_to(self.config.reports)
        with self.assertRaises(ErrorDescarga):
            ruta_local(self.config.raw, OBJECT)

    def test_listado_vacio_no_afirma_bucket_vacio(self):
        self.remote.objects.clear()
        self.assertEqual(self.run_download()["estado"], "sin_capturas_visibles")

    def test_error_auth_no_se_reintenta_ni_expone_respuesta(self):
        self.remote.http_status = 403
        result = self.run_download()
        self.assertEqual(result["reintentos"], 0)
        self.assertEqual(result["fallidos"], 1)
        self.assertNotIn("secret response", json.dumps(result))

    def test_error_temporal_tiene_reintentos_limitados(self):
        self.remote.http_status = 503
        result = self.run_download()
        self.assertEqual(result["reintentos"], 2)
        self.assertEqual(result["fallidos"], 1)

    def test_bloqueo_impide_ejecuciones_simultaneas(self):
        with bloqueo(self.config.reports):
            with self.assertRaises(ErrorDescarga):
                with bloqueo(self.config.reports):
                    pass


class ConfiguracionTest(unittest.TestCase):
    def test_env_privado_sin_ejecucion_shell_y_prioridad_entorno(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / ".env.u2"
            path.write_text('export NEXT_PUBLIC_SUPABASE_URL="https://test.invalid"\n'
                            'NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY="$(no-ejecutar)"\n'
                            'SUPABASE_SECRET_KEY="private"\n')
            self.assertEqual(leer_env(path)["NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY"], "$(no-ejecutar)")
            with patch.dict(os.environ, {"NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY": "override"}, clear=True):
                config = cargar_configuracion(path)
                self.assertEqual(config.clave, "override")
                self.assertNotIn("override", repr(config))
                self.assertNotIn("https://test.invalid", repr(config))
                self.assertEqual(cargar_configuracion(path, "secreta").clave, "private")

    def test_fecha_utc_y_extremo_hasta_incluido(self):
        start, end = intervalo(DAY, DAY)
        self.assertEqual((end - start).total_seconds(), 86400)
        stamp = fecha_captura(OBJECT)
        self.assertEqual(stamp.tzinfo, timezone.utc)
        self.assertTrue(start <= stamp < end)
        for path in ("source/no-date.json.gz", OBJECT.replace("00-05-02", "25-05-02"),
                     OBJECT.replace("station_status/2026-11-10", "station_status/2026-11-11")):
            with self.assertRaises(ErrorDescarga):
                fecha_captura(path)
        with self.assertRaises(ErrorConfiguracion):
            intervalo("2026-11-11", DAY)
        with self.assertRaises(ErrorConfiguracion):
            intervalo("20261110", DAY)


if __name__ == "__main__":
    unittest.main()
