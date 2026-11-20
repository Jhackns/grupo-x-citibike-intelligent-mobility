"""Bordes de finalización de la cohorte; no usa API, modelos o datos falsos como evidencia real."""
from pathlib import Path
import sys,unittest
from copy import deepcopy
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'))
from disponibilidad_u2.verificacion_cohorte import plazo,completa

class CohorteTests(unittest.TestCase):
    def test_plazo_preserva_horizonte_tolerancia_y_tardios(self):
        self.assertEqual(plazo('2026-11-19T00:00:00Z',15,30,600).isoformat(),'2026-11-19T00:25:30+00:00')
    def test_exige_avance_cierre_y_seis_grupos_sin_forzar_calidad(self):
        c={'reloj_maduro':True,'avance_suficiente':True,'predicciones_esperadas':6,
           'grupos':[{'n':1,'pendientes':0,'evaluadas':0,'vencidas':1} for _ in range(6)]}
        self.assertTrue(completa(c)) # Cobertura puede ser cero y la verificación seguir siendo honesta.
        for key in ('reloj_maduro','avance_suficiente'):
            other=deepcopy(c);other[key]=False;self.assertFalse(completa(other))
        other=deepcopy(c);other['grupos'][0]['pendientes']=1;self.assertFalse(completa(other))
        other=deepcopy(c);other['grupos'].pop();self.assertFalse(completa(other))

if __name__=='__main__':unittest.main()
