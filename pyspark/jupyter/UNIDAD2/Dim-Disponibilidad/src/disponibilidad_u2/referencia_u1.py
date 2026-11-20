"""Inspección de la selección explícita U1; no carga pesos ni calcula predicciones."""
import hashlib
import json
from pathlib import Path
from .configuracion import ROOT


def comprobar_referencia(path):
    path=(ROOT/path).resolve()
    ref=json.loads(path.read_text())
    if ref.get('version_referencia')!=1:
        raise ValueError('Referencia U1 no soportada')
    contracts=ref['contratos']
    expected={(t,h) for t in ('convencionales','electricas') for h in (5,10,15)}
    if len(contracts)!=6 or {(c['tipo'],c['horizonte_min']) for c in contracts}!=expected:
        raise ValueError('Referencia U1 sin las seis combinaciones explícitas')
    root=path.parent.parent;present=0
    for item in contracts:
        original=root/item['ruta_original']
        if not original.resolve().is_relative_to(root):raise ValueError('Ruta U1 fuera de su dimensión')
        contract=item['contrato']
        if (contract['tipo'],contract['horizonte_min'])!=(item['tipo'],item['horizonte_min']):
            raise ValueError('Contrato U1 no corresponde a la combinación')
        if original.exists():
            raw=original.read_bytes()
            if hashlib.sha256(raw).hexdigest()!=item['sha256_original'] or json.loads(raw)!=contract:
                raise ValueError('Contrato U1 local distinto de la referencia seleccionada')
            present+=1
    return {'ejecucion_id':ref['ejecucion_id'],'referencia':str(path),
            'contratos_originales_verificados':present,
            'contratos':[{'tipo':c['tipo'],'horizonte_min':c['horizonte_min'],
                         'ruta_original':c['ruta_original'],'candidato':c['contrato']['candidato']} for c in contracts],
            'inferencia_implementada':False}
