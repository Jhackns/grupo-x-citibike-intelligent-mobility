"""Publicación atómica e idempotente por batchId sobre filesystem local POSIX."""
import json
import os
from pathlib import Path
from uuid import uuid4
from pyspark import StorageLevel
from .calidad_streaming import aplicar_calidad
from .utilidades import guardar,digest


def publicar_batch(df,batch_id,destino,staging,fingerprint):
    destino=Path(destino);target=destino/f'batch-{batch_id:020d}'
    if target.exists():
        m=json.loads((target/'_batch.json').read_text())
        if m['batch_id']!=batch_id or m['fingerprint']!=fingerprint:raise ValueError('Batch de otra ejecución/checkpoint')
        return m
    stage=Path(staging)/(target.name+'-'+str(uuid4()))
    df.write.mode('errorifexists').parquet(str(stage))
    meta={'batch_id':batch_id,'fingerprint':fingerprint,'filas':df.count()}
    guardar(stage/'_batch.json',meta)
    destino.mkdir(parents=True,exist_ok=True)
    os.rename(stage,target)
    fd=os.open(destino,os.O_RDONLY)
    try:os.fsync(fd)
    finally:os.close(fd)
    return meta


class CalidadSink:
    def __init__(self,u1,cfg,fingerprint):self.u1,self.cfg,self.fingerprint=u1,cfg,fingerprint
    def __call__(self,batch,batch_id):
        from .configuracion import ROOT
        batch.persist(StorageLevel.MEMORY_AND_DISK)
        quality=None
        try:
            # Consumir todo el microbatch, incluso si está vacío, para confirmar estado.
            if batch.count()==0:return
            quality=aplicar_calidad(batch,self.u1,self.cfg['max_catalogs_per_batch']).persist(StorageLevel.MEMORY_AND_DISK)
            publicar_batch(quality,batch_id,ROOT/self.cfg['output_dir']/'observaciones',
                           ROOT/self.cfg['output_dir']/'_staging',self.fingerprint)
        finally:
            if quality is not None:quality.unpersist()
            batch.unpersist()
