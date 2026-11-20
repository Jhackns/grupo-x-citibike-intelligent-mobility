"""Descarga de un inventario: incrementalidad y confirmación segura por archivo."""
import sys
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.configuracion import Configuracion
from src.extraccion import Storage
from src.ampliacion import descargar_inventario
from test_extraccion import Remote, OBJECT, BODY


class AmpliacionTest(unittest.TestCase):
    def test_plan_incremental_conserva_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = Configuracion('https://test.invalid', 'test-key', 'citibike-raw',
                                   raw=root/'raw', reports=root/'reports')
            config.raw.mkdir()
            remote = Remote()
            inventory = {'objects': [{'path': OBJECT, 'remote': remote.objects[OBJECT][1]}]}
            with patch('src.ampliacion.Storage', lambda cfg: Storage(cfg, remote, sleep=lambda _: None)):
                self.assertEqual(descargar_inventario(config, inventory, 2)['descargados'], 1)
                self.assertEqual(descargar_inventario(config, inventory, 2)['omitidos'], 1)
            self.assertEqual((config.raw/OBJECT).read_bytes(), BODY)
            with self.assertRaises(ValueError):
                descargar_inventario(config, {'objects': inventory['objects']*2})

    def test_transferencia_fallida_no_confirma(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = Configuracion('https://test.invalid', 'test-key', 'citibike-raw',
                                   raw=root/'raw', reports=root/'reports', intentos=1)
            config.raw.mkdir()
            remote = Remote()
            remote.failures = 1
            inventory = {'objects': [{'path': OBJECT, 'remote': remote.objects[OBJECT][1]}]}
            with patch('src.ampliacion.Storage', lambda cfg: Storage(cfg, remote, sleep=lambda _: None)):
                result = descargar_inventario(config, inventory, 1)
                self.assertEqual(result['descargados'], 0)
                self.assertEqual(len(result['errores']), 1)
                self.assertEqual(descargar_inventario(config, inventory, 1)['descargados'], 1)


if __name__ == '__main__':
    unittest.main()
