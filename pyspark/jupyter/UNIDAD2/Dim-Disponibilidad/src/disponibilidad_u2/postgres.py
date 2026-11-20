"""JDBC en el driver: lotes JSON acotados, transacción por microbatch y claves estables.

No necesita librerías Python nuevas. El password solo se entrega como propiedad
JDBC; no se añade a URLs, SparkConf, logs o manifiestos.
"""
import json
import time
from pathlib import Path
from uuid import uuid4
from .configuracion import ROOT
from .utilidades import digest

VERSION='sql-v1-emparejamiento-u1'


def cargar_sql(path=None):
    cfg=json.loads((ROOT/'config/postgres.json').read_text())
    extra=json.loads(Path(path).read_text()) if path else {}
    if not isinstance(extra,dict) or set(extra)-set(cfg):raise ValueError('Claves SQL desconocidas')
    cfg.update(extra)
    if not cfg['jdbc_url'].startswith('jdbc:postgresql://') or any(x in cfg['jdbc_url'].lower() for x in ('password','user=','@')):
        raise ValueError('URL JDBC pública sin credenciales')
    for k in ('batch_rows','evaluation_poll_seconds','evaluation_limit','late_seconds','tolerance_seconds','gap_seconds','retries'):
        if type(cfg[k]) is not int or cfg[k]<1:raise ValueError('Entero positivo requerido: '+k)
    if cfg['batch_rows']>1000 or cfg['retries']>5:raise ValueError('Lotes/reintentos exceden los límites')
    cp=(ROOT/cfg['checkpoint_dir']).resolve()
    if not cp.is_relative_to((ROOT/'data/sql').resolve()):raise ValueError('Checkpoint SQL fuera de la dimensión')
    if not cfg['flow_id'] or len(cfg['flow_id'])>120:raise ValueError('flow_id inválido')
    return cfg


class Base:
    def __init__(self,spark,cfg):
        self.cfg=cfg
        props=spark._jvm.java.util.Properties()
        props.setProperty('user',cfg['user'])
        secret=Path(cfg['password_file']).read_text().strip()
        if not secret:raise ValueError('Falta contraseña SQL en archivo secreto')
        props.setProperty('password',secret)
        driver=spark._jvm.java.lang.Thread.currentThread().getContextClassLoader().loadClass('org.postgresql.Driver').newInstance()
        self.conn=driver.connect(cfg['jdbc_url'],props)
        self.conn.setAutoCommit(False)
        for s in ("SET TIME ZONE 'UTC'","SET statement_timeout='90s'","SET lock_timeout='10s'"):
            self.execute(s)
        if self.json("SELECT to_json(current_database())::text")!='disponibilidad_u2':
            self.close();raise ValueError('Se exige la base aislada disponibilidad_u2')
        self.conn.commit()

    def execute(self,sql,params=()):
        statement=self.conn.prepareStatement(sql)
        try:
            for i,v in enumerate(params,1):statement.setString(i,str(v))
            statement.execute()
            return statement.getUpdateCount()
        finally:statement.close()

    def json(self,sql,params=()):
        statement=self.conn.prepareStatement(sql)
        try:
            for i,v in enumerate(params,1):statement.setString(i,str(v))
            rs=statement.executeQuery()
            try:return json.loads(rs.getString(1)) if rs.next() and rs.getString(1) is not None else None
            finally:rs.close()
        finally:statement.close()

    def close(self):self.conn.close()

    def iniciar(self):
        import hashlib
        file=ROOT/'sql/001_disponibilidad.sql';checksum=hashlib.sha256(file.read_bytes()).hexdigest()
        exists=self.json("SELECT to_json(to_regclass('disponibilidad.esquema') IS NOT NULL)::text")
        if exists:
            actual=self.json('SELECT to_json(sha256)::text FROM disponibilidad.esquema WHERE version=1')
            if actual!=checksum:raise ValueError('El esquema SQL existente tiene otra versión; se requiere migración explícita')
        else:
            self.execute(file.read_text())
            self.execute('INSERT INTO disponibilidad.esquema VALUES(1,?)',(checksum,))
        self.conn.commit()

    def registrar(self,contrato):
        text=json.dumps(contrato,sort_keys=True)
        changed=self.execute('''INSERT INTO disponibilidad.flujos(flujo,contrato) VALUES(?,?::jsonb)
          ON CONFLICT(flujo) DO UPDATE SET contrato=disponibilidad.flujos.contrato
          WHERE disponibilidad.flujos.contrato=excluded.contrato''',(self.cfg['flow_id'],text))
        if changed!=1:raise ValueError('flow_id ya vinculado a otras entradas/reglas; usar nueva configuración explícita')
        self.conn.commit()

    def confirmar_lote(self,batch):
        self.execute('''UPDATE disponibilidad.lotes SET confirmado_utc=clock_timestamp()
          WHERE flujo=? AND batch_id=?::bigint AND confirmado_utc IS NULL''',(self.cfg['flow_id'],batch))
        self.conn.commit()

    def evaluar(self,as_of=None):
        ronda=str(uuid4());f=self.cfg['flow_id']
        if not self.json('SELECT to_json(pg_try_advisory_xact_lock(hashtextextended(?,0)))::text',(f,)):
            self.conn.rollback()
            return 0  # Carga en curso: se vuelve a intentar en el siguiente poll.
        self.execute('INSERT INTO disponibilidad.rondas_evaluacion(id,flujo) VALUES(?,?)',(ronda,f))
        n=self.json('SELECT to_json(disponibilidad.evaluar(?,?::int,coalesce(?::timestamptz,clock_timestamp()),?))::text',
            (f,self.cfg['evaluation_limit'],as_of or '',ronda)) if as_of else self.json(
            'SELECT to_json(disponibilidad.evaluar(?,?::int,clock_timestamp(),?))::text',(f,self.cfg['evaluation_limit'],ronda))
        if n:
            self.conn.commit()
            self.execute('UPDATE disponibilidad.rondas_evaluacion SET confirmado_utc=clock_timestamp() WHERE id=?',(ronda,))
            self.conn.commit()
        else:self.conn.rollback()
        return n

    def confirmar_rondas(self):
        # Una caída después del commit y antes de recibir ACK deja un límite superior
        # de latencia posterior a la recuperación, nunca vuelve a evaluar la fila.
        self.execute('''UPDATE disponibilidad.rondas_evaluacion SET confirmado_utc=clock_timestamp()
          WHERE flujo=? AND confirmado_utc IS NULL''',(self.cfg['flow_id'],));self.conn.commit()


INSERT_OBS='''INSERT INTO disponibilidad.observaciones AS old
 (flujo,observacion_id,event_id,capture_id,station_id,captura_utc,last_updated_utc,last_reported_utc,
 contrato_valido,convencionales,electricas,elegible_convencionales,elegible_electricas,
 regla_version,catalogo_compatible,payload,batch_id)
 SELECT ?,x->>'_clave_dedup',x->>'event_id',x->>'capture_id',x->>'station_id',
 CASE WHEN pg_input_is_valid(x->>'captura_utc','timestamptz') THEN (x->>'captura_utc')::timestamptz END,
 CASE WHEN pg_input_is_valid(x->>'last_updated_utc_original','timestamptz') THEN (x->>'last_updated_utc_original')::timestamptz END,
 CASE WHEN pg_input_is_valid(x->>'last_reported_utc_original','timestamptz') THEN (x->>'last_reported_utc_original')::timestamptz END,
 (x->>'contrato_valido')::boolean,(x->>'objetivo_convencionales')::bigint,(x->>'objetivo_electricas')::bigint,
 (x->>'elegible_pred_convencionales')::boolean,(x->>'elegible_pred_electricas')::boolean,
 x->>'regla_version',(x->>'catalogo_modelo_compatible')::boolean,x,?::bigint
 FROM jsonb_array_elements(?::jsonb) x
 ON CONFLICT(flujo,observacion_id) DO UPDATE SET payload=old.payload WHERE old.payload=excluded.payload'''

INSERT_PRED='''INSERT INTO disponibilidad.predicciones AS old
 (flujo,prediccion_id,event_id,station_id,origen_utc,objetivo_utc,tipo,horizonte_min,valor_predicho,
 version_predictor,contrato_sha256,ejecucion_u1,payload,batch_id)
 SELECT ?,x->>'prediccion_id',x->>'event_id',x->>'station_id',(x->>'origen_utc')::timestamptz,
 (x->>'objetivo_utc')::timestamptz,x->>'tipo',(x->>'horizonte_min')::int,(x->>'valor_predicho')::double precision,
 x->>'version_predictor',x->>'contrato_sha256',x->>'ejecucion_u1',x,?::bigint
 FROM jsonb_array_elements(?::jsonb) x
 ON CONFLICT(flujo,prediccion_id) DO UPDATE SET payload=old.payload WHERE old.payload=excluded.payload'''


def escribir_json(db,kind,batch,rows):
    sql=INSERT_OBS if kind=='observaciones' else INSERT_PRED
    changed=db.execute(sql,(db.cfg['flow_id'],batch,'['+','.join(rows)+']'))
    if changed!=len(rows):raise ValueError('Identidad SQL repetida con contenido diferente; transacción cancelada')


class SinkSQL:
    def __init__(self,cfg,u1,contrato):self.cfg=cfg;self.u1=u1;self.contrato=contrato

    def __call__(self,batch,batch_id):
        from .predictor_streaming import predecir
        from .utilidades import log
        batch.persist()
        try:
            for attempt in range(self.cfg['retries']):
                db=None
                try:
                    db=Base(batch.sparkSession,self.cfg);f=self.cfg['flow_id']
                    db.execute('SELECT pg_advisory_xact_lock(hashtextextended(?,0))',(f,))
                    existing=db.json('SELECT row_to_json(t)::text FROM disponibilidad.lotes t WHERE flujo=? AND batch_id=?::bigint',(f,batch_id))
                    if existing:
                        db.conn.commit();db.confirmar_lote(batch_id)
                        log('sql_lote_reutilizado',batch_id=batch_id);return
                    counts={}
                    for kind,frame in [('observaciones',batch),('predicciones',predecir(batch,self.u1))]:
                        rows=[];count=0
                        for raw in frame.toJSON().toLocalIterator():
                            rows.append(raw)
                            if len(rows)>=self.cfg['batch_rows']:
                                escribir_json(db,kind,batch_id,rows);count+=len(rows);rows=[]
                        if rows:escribir_json(db,kind,batch_id,rows);count+=len(rows)
                        counts[kind]=count
                    rejects=db.execute('''INSERT INTO disponibilidad.rechazos
                       SELECT flujo,observacion_id,coalesce(payload->'motivos_pred_convencionales','[]'),
                         coalesce(payload->'motivos_pred_electricas','[]') FROM disponibilidad.observaciones
                       WHERE flujo=? AND batch_id=?::bigint AND (NOT elegible_convencionales OR NOT elegible_electricas)
                       ON CONFLICT DO NOTHING''',(f,batch_id))
                    db.execute('''INSERT INTO disponibilidad.evaluaciones(flujo,prediccion_id,cierre_no_antes_utc)
                       SELECT flujo,prediccion_id,objetivo_utc+make_interval(secs=>?::int)
                       FROM disponibilidad.predicciones WHERE flujo=? AND batch_id=?::bigint ON CONFLICT DO NOTHING''',
                       (self.cfg['tolerance_seconds']+self.cfg['late_seconds'],f,batch_id))
                    db.execute('INSERT INTO disponibilidad.lotes(flujo,batch_id,observaciones,rechazos,predicciones) VALUES(?,?::bigint,?::bigint,?::bigint,?::bigint)',
                        (f,batch_id,counts['observaciones'],rejects,counts['predicciones']))
                    db.conn.commit();db.confirmar_lote(batch_id)
                    log('sql_lote_confirmado',batch_id=batch_id,**counts,rechazos=rejects);return
                except Exception as exc:
                    if db is not None:db.conn.rollback()
                    if attempt+1==self.cfg['retries']:raise
                    log('sql_reintento',intento=attempt+1,error_tipo=type(exc).__name__)
                    time.sleep(min(2**attempt,4))
                finally:
                    if db is not None:db.close()
        finally:batch.unpersist()


def contrato_sql(cfg,stream,u1,stamp):
    def seconds(text):
        n,unit=text.split();return int(n)*(60 if unit=='minutes' else 1)
    ref=u1['ref']['configuracion_origen']
    if cfg['tolerance_seconds']!=ref['tolerancia_s'] or cfg['gap_seconds']!=ref['hueco_max_s']:
        raise ValueError('Tolerancia/huecos deben coincidir con U1')
    actual=stamp['configuracion_inicial']
    if stamp['u1']['referencia_sha256']!=u1['referencia_sha256']:
        raise ValueError('La salida streaming procede de otra referencia U1')
    if cfg['late_seconds']<seconds(actual['watermark']):raise ValueError('Margen SQL menor que watermark real de ingesta')
    return {'version':VERSION,'source_fingerprint':stamp['fingerprint'],
        'u1_id':u1['ref']['ejecucion_id'],'u1_reference_sha256':u1['referencia_sha256'],
        'tolerance_seconds':cfg['tolerance_seconds'],'gap_seconds':cfg['gap_seconds'],
        'late_seconds':cfg['late_seconds'],'gold_version':u1['ref']['versiones_origen']['gold'],
        'definition':'vehicle_types_available:1=bicycle/human;2=bicycle/electric_assist'}
