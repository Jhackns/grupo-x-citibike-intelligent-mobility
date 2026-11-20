"""Pruebas pequeñas de contratos de telemetría y consultas, sin API ni Docker."""
from pathlib import Path
import json
import sys
import unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from disponibilidad_u2.telemetria import metrica,construir,proceso_activo
from disponibilidad_u2.grafana_api import literal,renderizar


class ObservabilidadTests(unittest.TestCase):
    def test_nulos_no_se_presentan_como_ceros(self):
        self.assertIn(' NaN',metrica('disponibilidad_sin_muestras',None))
        self.assertIn(' 0',metrica('disponibilidad_cantidad_real',0))

    def test_prohibe_etiquetas_de_alta_cardinalidad(self):
        for key in ('station_id','event_id','capture_id','run_id'):
            with self.assertRaises(ValueError):metrica('disponibilidad_test',1,{key:'x'})

    def test_proceso_desconocido_no_equivale_a_detenido(self):
        self.assertIsNone(proceso_activo({},False));self.assertFalse(proceso_activo({},True))

    def test_tipo_prometheus_no_se_repite_y_contadores_desconocidos(self):
        with patch('disponibilidad_u2.telemetria.docker_activo',return_value=False),patch('urllib.request.urlopen',side_effect=OSError):
            text,_,_=construir({'controlado':False},1000,'http://no-red')
        declarations=[line for line in text.splitlines() if line.startswith('# TYPE')]
        self.assertEqual(len(declarations),len(set(declarations)))
        self.assertNotIn('disponibilidad_productor_eventos_confirmados_total',text)
        self.assertIn('disponibilidad_componente_up{componente="productor"} NaN',text)

    def test_counter_ausente_y_reinicio_no_fabrica_cero_ni_nan(self):
        self.assertEqual(metrica('disponibilidad_test_total',None,kind='counter'),'')
        self.assertEqual(metrica('disponibilidad_test_total',float('nan'),kind='counter'),'')
        self.assertIn(' 0',metrica('disponibilidad_test_total',0,kind='counter'))
        for n in [7557,2519]:self.assertIn(str(n),metrica('disponibilidad_test_total',n,kind='counter'))
        with self.assertRaises(ValueError):metrica('disponibilidad_test_total',-1,kind='counter')

    def test_variables_sql_citadas(self):
        self.assertEqual(literal("a'b"),"'a''b'")
        out=renderizar('${estacion:sqlstring}',{'flujo':'f','estacion':"x'; DROP TABLE t;--",'tipo':'electricas','horizonte':'5'})
        self.assertTrue(out.startswith("'x''"));self.assertTrue(out.endswith("--'"))

    def test_dashboard_no_rellena_observaciones_ausentes(self):
        data=json.loads((ROOT/'observabilidad/dashboards/disponibilidad.json').read_text())
        panel=next(p for p in data['panels'] if p['title'].startswith('Observado frente'))
        self.assertFalse(panel['fieldConfig']['defaults']['custom']['spanNulls'])
        self.assertIn("CASE WHEN estado='evaluada' THEN valor_predicho",panel['targets'][0]['rawSql'])
        self.assertIn('$__timeFilter(objetivo_utc)',panel['targets'][0]['rawSql'])


if __name__=='__main__':unittest.main()
