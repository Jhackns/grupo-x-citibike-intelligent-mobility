"""Verificación de una cohorte explícita; no modifica predicciones ni tolerancias."""
from datetime import datetime,timedelta
from .observabilidad_sql import consultar,ejecutar


def literal(value):return "'"+str(value).replace("'","''")+"'"


def plazo(origen,horizonte_min,tolerancia_s,tardios_s):
    return datetime.fromisoformat(origen.replace('Z','+00:00'))+timedelta(minutes=horizonte_min,seconds=tolerancia_s+tardios_s)


def estado_cohorte(flow,tolerancia_s=30,tardios_s=600):
    f=literal(flow)
    return consultar("SELECT coalesce(("+f'''WITH c AS MATERIALIZED (
      SELECT capture_id,min(captura_utc) origen,count(*) estaciones,
       3*sum(elegible_convencionales::int+elegible_electricas::int) esperadas
      FROM disponibilidad.observaciones WHERE flujo={f} AND contrato_valido
      GROUP BY capture_id ORDER BY min(captura_utc),capture_id LIMIT 1
    ), g AS (
      SELECT p.tipo,p.horizonte_min,count(*) n,count(*) FILTER(WHERE e.estado='pendiente') pendientes,
       count(*) FILTER(WHERE e.estado='evaluada') evaluadas,count(*) FILTER(WHERE e.estado='vencida') vencidas
      FROM disponibilidad.predicciones p JOIN disponibilidad.evaluaciones e USING(flujo,prediccion_id)
      JOIN disponibilidad.observaciones o ON o.flujo=p.flujo AND o.observacion_id=p.event_id
      JOIN c ON c.capture_id=o.capture_id WHERE p.flujo={f} GROUP BY p.tipo,p.horizonte_min
    ) SELECT jsonb_build_object('capture_id',c.capture_id,'origen_utc',c.origen,
       'estaciones',c.estaciones,'predicciones_esperadas',c.esperadas,
       'plazo_minimo_utc',c.origen+interval '{15*60+tolerancia_s+tardios_s} seconds',
       'ultima_captura_utc',(SELECT max(captura_utc) FROM disponibilidad.observaciones WHERE flujo={f} AND contrato_valido),
       'grupos',coalesce((SELECT jsonb_agg(to_jsonb(g) ORDER BY tipo,horizonte_min) FROM g),'[]'::jsonb),
       'reloj_maduro',clock_timestamp()>=c.origen+interval '{15*60+tolerancia_s+tardios_s} seconds',
       'avance_suficiente',(SELECT max(captura_utc) FROM disponibilidad.observaciones WHERE flujo={f} AND contrato_valido)>=c.origen+interval '{15*60+tolerancia_s+tardios_s} seconds') FROM c'''+"),'null'::jsonb)")


def completa(c):
    return bool(c and c['reloj_maduro'] and c['avance_suficiente'] and len(c['grupos'])==6
        and sum(g['n'] for g in c['grupos'])==c['predicciones_esperadas']
        and all(g['n']>0 and g['pendientes']==0 for g in c['grupos']))


def registrar_ejecucion(flow,inicio,fin=None,estado='activa',evidencia=''):
    end=literal(fin)+'::timestamptz' if fin else 'NULL'
    ejecutar(f'''INSERT INTO observabilidad_disponibilidad.ejecuciones(flujo,inicio_utc,fin_utc,estado,evidencia)
      VALUES({literal(flow)},{literal(inicio)}::timestamptz,{end},{literal(estado)},{literal(evidencia)})
      ON CONFLICT(flujo) DO UPDATE SET fin_utc=excluded.fin_utc,estado=excluded.estado,evidencia=excluded.evidencia''')
