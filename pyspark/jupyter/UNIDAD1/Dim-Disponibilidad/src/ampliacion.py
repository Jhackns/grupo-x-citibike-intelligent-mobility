"""Ejecuta un inventario ya revisado reutilizando el descargador de fase 1.

Concurrencia limitada por lotes; solo el hilo principal confirma el manifiesto.
No contiene configuración ni credenciales. Uso exclusivamente local en .venv.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice
import shutil
from collections import defaultdict
from datetime import datetime, timezone

from .extraccion import (Storage, bloqueo, leer_manifiesto, ruta_local, sin_cambios,
                        huella_remota, guardar_json, ahora, ErrorDescarga, fecha_captura)
from .configuracion import FUENTES


def inventariar_historico(config, start, end):
    """Solo listado remoto; fechas por captura, con límite exclusivo UTC."""
    manifest = leer_manifiesto(config)
    objects, days = [], defaultdict(lambda: {'archivos':0,'bytes':0,'pendientes':0,'bytes_pendientes':0})
    for path, remote in Storage(config).objetos(FUENTES,start,end):
        stamp = fecha_captura(path)
        if not start <= stamp < end:
            continue
        size = int((remote.get('metadata') or {}).get('size') or 0)
        pending = not sin_cambios(ruta_local(config.raw,path),manifest['objects'].get(path),remote)
        objects.append({'path':path,'remote':remote})
        item = days[(stamp.date().isoformat(),path.split('/')[0])]
        item['archivos'] += 1;item['bytes'] += size
        item['pendientes'] += int(pending);item['bytes_pendientes'] += size*int(pending)
        item['primera_captura'] = min(item.get('primera_captura',stamp.isoformat()),stamp.isoformat())
        item['ultima_captura'] = max(item.get('ultima_captura',stamp.isoformat()),stamp.isoformat())
    if not objects:
        raise ErrorDescarga('Listado sin capturas visibles; no se supone que el bucket esté vacío')
    total = sum(x['bytes_pendientes'] for x in days.values())
    free = shutil.disk_usage(config.raw).free
    # Considera todo el periodo que se reprocesará, no solo la descarga pendiente.
    reserve = max(5*1024**3, sum(x['bytes'] for x in days.values())*25)
    return {'inventariado_utc':ahora(),'desde_utc':start.isoformat(),'hasta_exclusivo_utc':end.isoformat(),
            'objects':objects,'por_fecha_fuente':[{'fecha_utc':day,'fuente':source,**values}
                for (day,source),values in sorted(days.items())],
            'archivos':len(objects),'bytes_remotos':sum(x['bytes'] for x in days.values()),
            'pendientes':sum(x['pendientes'] for x in days.values()),'bytes_pendientes':total,
            'libre_local_bytes':free,'reserva_spark_bytes':reserve,'espacio_suficiente':free >= total+reserve}


def descargar_inventario(config, inventario, workers=6):
    if not 1 <= workers <= 8:
        raise ValueError('Usa entre 1 y 8 transferencias simultáneas')
    paths = [x['path'] for x in inventario['objects']]
    if len(paths) != len(set(paths)):
        raise ValueError('Inventario con rutas repetidas; no se descarga')
    resumen = {'inicio_utc': ahora(), 'descargados': 0, 'omitidos': 0,
               'bytes_descargados': 0, 'reintentos': 0, 'errores': []}
    with bloqueo(config.reports):
        manifest = leer_manifiesto(config)
        pendientes = []
        for item in inventario['objects']:
            path, remote = item['path'], item['remote']
            local = ruta_local(config.raw, path)
            previous = manifest['objects'].get(path)
            if sin_cambios(local, previous, remote):
                resumen['omitidos'] += 1
            elif local.exists() and previous and previous['remote_fingerprint'] != huella_remota(remote):
                raise ValueError('Un original remoto cambió: revisar antes de sustituir raw')
            else:
                pendientes.append(item)
        total = sum(int((x['remote'].get('metadata') or {}).get('size') or 0) for x in pendientes)
        free = shutil.disk_usage(config.raw).free
        # Reserva conservadora para Spark/Silver/Gold, no solo para los gzip.
        reserva = max(5 * 1024**3, total * 25)
        if free < total + reserva:
            raise ValueError('Espacio insuficiente para descarga y reserva de procesamiento')
        resumen.update(pendientes=len(pendientes), bytes_pendientes=total, libre_antes=free, reserva=reserva)

        def transfer(item):
            client = Storage(config)  # no compartir opener/contadores entre hilos
            path = item['path']
            details = client.descargar(path, item['remote'], ruta_local(config.raw, path))
            return details, client.reintentos

        it = iter(pendientes)
        with ThreadPoolExecutor(max_workers=workers) as pool:
            while batch := list(islice(it, workers)):
                futures = {pool.submit(transfer, item): item for item in batch}
                for future in as_completed(futures):
                    item = futures[future]
                    try:
                        details, retries = future.result()
                        manifest['objects'][item['path']] = {
                            'remote': item['remote'], 'remote_fingerprint': huella_remota(item['remote']),
                            'downloaded_at_utc': ahora(), **details}
                        guardar_json(config.reports / 'manifiesto.json', manifest)
                        resumen['descargados'] += 1
                        resumen['bytes_descargados'] += details['local_size']
                        resumen['reintentos'] += retries
                    except (ErrorDescarga, OSError, ValueError) as exc:
                        resumen['errores'].append({'path': item['path'], 'error': type(exc).__name__})
                if resumen['descargados'] % 60 < workers:
                    print(f"Progreso: {resumen['descargados']} nuevos; {resumen['omitidos']} omitidos", flush=True)
    resumen['fin_utc'] = ahora()
    guardar_json(config.reports / 'resumen_ampliacion.json', resumen)
    return resumen
