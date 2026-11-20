"""IO atómico, tiempos UTC y logs de campos públicos."""
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path


def ahora():
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')


def fecha_utc(value):
    if isinstance(value, bool) or value is None:
        raise ValueError('Timestamp ausente o inválido')
    if isinstance(value, (int, float)):
        if value <= 0:
            raise ValueError('Timestamp debe ser positivo')
        dt = datetime.fromtimestamp(value, timezone.utc)
    elif isinstance(value, str):
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if dt.tzinfo is None:
            raise ValueError('Timestamp sin zona horaria')
    else:
        raise ValueError('Tipo de timestamp no admitido')
    return dt.astimezone(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')


def epoch(value):
    return datetime.fromisoformat(fecha_utc(value).replace('Z', '+00:00')).timestamp()


def serializar(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')


def digest(value):
    return hashlib.sha256(serializar(value)).hexdigest()


def guardar(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    with tmp.open('w', encoding='utf-8') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write('\n');stream.flush();os.fsync(stream.fileno())
    os.replace(tmp, path)


def log(evento, **fields):
    # No se registran configuraciones completas, variables, cabeceras ni mensajes de excepción.
    print(json.dumps({'ts_utc': ahora(), 'evento': evento, **fields}, ensure_ascii=False), flush=True)
