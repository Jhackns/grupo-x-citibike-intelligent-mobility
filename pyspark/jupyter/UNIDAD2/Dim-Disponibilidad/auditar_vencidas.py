#!/usr/bin/env python3
"""Comprueba vencimientos del ensayo conservado, sin modificar predicciones ni reglas."""
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'src'))
from disponibilidad_u2.observabilidad_sql import consultar
from disponibilidad_u2.utilidades import guardar,ahora

m=consultar((ROOT/'sql/auditar_vencidas.sql').read_text())
assert {r['motivo']:r['n'] for r in m['motivos']}=={'sin_captura_objetivo_en_tolerancia':19554,'objetivo_no_elegible':18}
assert m['falsos_faltantes']==m['sin_cercana_450s']==m['invalidas_elegibles_por_error']==0
m.update(estado='verificada',comprobado_utc=ahora(),flujo='disponibilidad-etapa3-vivo-20261119-v1',
 conclusion='Sin defecto demostrado: banda temporal sin captura o calidad futura no elegible. Se conservan tolerancia y evaluaciones originales.')
guardar(ROOT/'reports/observabilidad/vencidas.json',m)
print({k:m[k] for k in ('estado','motivos','falsos_faltantes','invalidas_elegibles_por_error')})
