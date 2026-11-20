"""Contratos de configuración, inventario e invalidación de resultados guardados."""
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.ejecucion import (artefactos, comprobar_artefactos, compatible, configuracion,
                           guardar, huella, inventario, ROOT)


class EjecucionTests(unittest.TestCase):
    def test_spark_master_precedencia_memoria_y_json_antiguo(self):
        default = configuracion()
        self.assertEqual(default['spark_master'], 'local[4]')
        self.assertNotIn('cores', default)
        self.assertEqual(configuracion(spark_master='local[2]')['driver_memory'], default['driver_memory'])
        with TemporaryDirectory() as tmp:
            path = Path(tmp)/'config.json'
            guardar(path, {'spark_master': 'local[3]'})
            self.assertEqual(configuracion(path)['spark_master'], 'local[3]')
            self.assertEqual(configuracion(path, spark_master='local[2]')['spark_master'], 'local[2]')
            guardar(path, {'cores': 2})
            before = path.read_bytes()
            self.assertEqual(configuracion(path)['spark_master'], 'local[2]')
            self.assertEqual(configuracion(path, spark_master='local[4]')['spark_master'], 'local[4]')
            self.assertEqual(path.read_bytes(), before)
            guardar(path, {'cores': 2, 'spark_master': 'local[4]'})
            with self.assertRaisesRegex(ValueError, 'contradictorios'):
                configuracion(path)
            guardar(path, {'cores': True})
            with self.assertRaises(ValueError):
                configuracion(path)
        for master in ('local[0]', 'local[-1]', 'local[2.5]', 'local[*]', 'local[2] --driver-memory 8g', '', None):
            if master is None:
                continue  # None significa que la CLI no reemplaza el JSON.
            with self.subTest(master=master), self.assertRaises(ValueError):
                configuracion(spark_master=master)
        self.assertEqual(configuracion(ROOT/'config/u1-preliminar.json')['spark_master'], 'local[2]')

    def test_lanzador_propaga_master_a_cli_y_notebook_sin_procesar(self):
        from ejecutar_u1 import main
        for args, master in (([], 'local[4]'), (['--spark-master', 'local[2]'], 'local[2]'),
                             (['--notebook', '--spark-master', 'local[2]'], 'local[2]')):
            with self.subTest(args=args), TemporaryDirectory() as tmp:
                root = Path(tmp)/'pyspark/jupyter/UNIDAD1/Dim-Disponibilidad'
                root.mkdir(parents=True)
                def output(command, **kwargs):
                    if command[0]=='docker':
                        return json.dumps([{'Destination':'/opt','Source':str(root.parents[1]),'RW':True}])
                    return 'prueba-local\n'
                proc = unittest.mock.MagicMock()
                proc.__enter__.return_value = proc
                proc.stdout = []
                def finish():
                    path = next(root.glob('data/ejecuciones/*/manifiesto.json'))
                    m = json.loads(path.read_text()); m.update(estado='completada', modo='simulada')
                    guardar(path, m)
                    return 0
                proc.wait.side_effect = finish
                with patch('ejecutar_u1.ROOT', root), \
                     patch('ejecutar_u1.subprocess.run', return_value=SimpleNamespace(stdout='true imagen-prueba\n')), \
                     patch('ejecutar_u1.subprocess.check_output', side_effect=output), \
                     patch('ejecutar_u1.subprocess.Popen', return_value=proc) as launch:
                    self.assertEqual(main(args), 0)
                command = launch.call_args.args[0]
                self.assertIn('PYSPARK_SUBMIT_ARGS=--master '+master+' --driver-memory 4g pyspark-shell', command)
                cfg = json.loads(next(root.glob('data/ejecuciones/*/configuracion.json')).read_text())
                context = json.loads((root/'data/contexto_notebook.json').read_text())
                self.assertEqual(cfg['spark_master'], master)
                self.assertEqual(cfg, context['configuracion'])
                self.assertEqual(cfg['driver_memory'], '4g')
                self.assertNotIn('cores', cfg)
                self.assertEqual('-c' in command, '--notebook' in args)

    def test_lanzador_error_deja_manifiesto_y_no_invoca_docker(self):
        from ejecutar_u1 import main
        with TemporaryDirectory() as tmp:
            with patch('ejecutar_u1.ROOT',Path(tmp)), patch('ejecutar_u1.subprocess.run') as command:
                self.assertEqual(main(['--desde','2026-11-11']),1)
                command.assert_not_called()
            paths=list(Path(tmp).glob('data/ejecuciones/*/manifiesto.json'))
            self.assertEqual(len(paths),1)
            manifest=json.loads(paths[0].read_text())
            self.assertEqual(manifest['estado'],'fallida')
            self.assertEqual(manifest['errores'][0]['tipo'],'ValueError')

    def test_configuracion_rechaza_secretos_y_cortes_incompatibles(self):
        with TemporaryDirectory() as tmp:
            path=Path(tmp)/'config.json'
            guardar(path,{'SUPABASE_SECRET_KEY':'valor-ficticio'})
            with self.assertRaises(ValueError):
                configuracion(path)
            with self.assertRaises(ValueError):
                configuracion(desde='2026-11-11')
            guardar(path,{'tolerancia_s':31})
            with self.assertRaises(ValueError):
                configuracion(path)
            self.assertEqual(configuracion()['corte_prueba_utc'],'2026-11-15 00:00:00')
            guardar(path,{'corte_validacion_utc':'2026-11-14 01:00:00'})
            with self.assertRaises(ValueError):
                configuracion(path)

    def test_no_reutiliza_parciales_config_distinta_o_artefactos_alterados(self):
        with TemporaryDirectory() as tmp:
            root=Path(tmp)/'001';root.mkdir()
            guardar(root/'salida.json',{'valor':1})
            outputs=artefactos(root)
            m={'estado':'fallida','modo':'nueva','huella_compatibilidad':'a','artefactos':outputs}
            guardar(root/'manifiesto.json',m)
            self.assertIsNone(compatible(tmp,'a'))
            m['estado']='completada';guardar(root/'manifiesto.json',m)
            self.assertIsNone(compatible(tmp,'b'))
            self.assertEqual(compatible(tmp,'a')[0],root)
            (root/'salida.json').write_text('incompleta')
            with self.assertRaises(AssertionError):
                compatible(tmp,'a')
            with self.assertRaises(AssertionError):
                comprobar_artefactos(root,[])

    def test_inventario_fija_periodo_y_detecta_cambios(self):
        with TemporaryDirectory() as tmp:
            root=Path(tmp)
            for source in ('station_status','station_information','vehicle_types'):
                folder=root/source;folder.mkdir()
                (folder/'2026-11-10T00-00-00-000Z-ejemplo.json.gz').write_bytes(b'original')
                (folder/'2026-11-11T00-00-00-000Z-ejemplo.json.gz').write_bytes(b'fuera-periodo')
            config=configuracion(ROOT/'config/u1-preliminar.json')
            first=inventario(root,config)
            self.assertEqual(len(first),3)
            (root/'station_status/2026-11-10T00-00-00-000Z-ejemplo.json.gz').write_bytes(b'modificado')
            self.assertNotEqual(huella(first),huella(inventario(root,config)))
            (root/'station_status/2026-11-10T00-00-00-000Z-ejemplo.json.gz').unlink()
            with self.assertRaises(FileNotFoundError):
                inventario(root,config)


if __name__=='__main__':
    unittest.main()
