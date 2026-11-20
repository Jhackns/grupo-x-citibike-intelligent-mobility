#!/usr/bin/env python3
"""Crea un secreto aleatorio local solo si falta; nunca imprime su contenido."""
import os
from pathlib import Path
import secrets

root=Path(__file__).resolve().parent
secret=root/'data/secretos/postgres_password';secret.parent.mkdir(parents=True,exist_ok=True)
try:
    fd=os.open(secret,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
except FileExistsError:
    if secret.stat().st_mode&0o077:raise SystemExit('Corregir permisos del secreto local a 600')
    print('Secreto local existente conservado; no se cambió su valor.')
else:
    with os.fdopen(fd,'w') as f:f.write(secrets.token_urlsafe(48)+'\n')
    print('Secreto local creado en data/secretos/postgres_password (ignorado por Git).')
