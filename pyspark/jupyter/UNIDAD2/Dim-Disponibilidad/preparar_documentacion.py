#!/usr/bin/env python3
"""Genera tablas públicas desde informes verificados; no publica ni copia secretos/raw."""
from pathlib import Path
import json
ROOT=Path(__file__).resolve().parent


def table(head,rows):return '| '+' | '.join(head)+' |\n| '+' | '.join(['---']*len(head))+' |\n'+''.join('| '+' | '.join(map(str,r))+' |\n' for r in rows)
def fmt(x):return 'NULL' if x is None else (f'{x:.6f}' if isinstance(x,float) else str(x))

def main():
    r=json.loads((ROOT/'reports/correcciones/resultados.json').read_text());d=ROOT/'documentacion';e=d/'evidencias';e.mkdir(exist_ok=True)
    rows=[x for x in r['poblaciones']['cohorte_inicial'] if x['momento']=='fin_prueba']
    public={k:r[k] for k in ['fecha_auditoria_utc','flujo','cohorte_capture_id','inicio_ejecucion_utc','fin_ejecucion_utc','capturas','observaciones_sql','capturas_cercanas','motivos_cohorte','latencias']};public['cohorte']=rows
    (e/'resultados.json').write_text(json.dumps(public,indent=2,ensure_ascii=False)+'\n')
    text='# Evaluación: cohorte, cobertura y periodo\n\n'
    text+=f"Flujo explícito `{r['flujo']}`; {r['capturas']} capturas y {r['observaciones_sql']} observaciones. Cohorte `{r['cohorte_capture_id']}`, origen {r['cohorte']['origen_utc']}; plazo mínimo {r['cohorte']['plazo_minimo_utc']}. Frontier al cerrar cohorte {r['cohorte']['ultima_captura_utc']}; captura final emitida {r.get('ultima_captura_emitida_utc')}.\n\n"
    text+='Numerador: evaluadas maduras. Denominador: predicciones cuyo plazo terminó (objetivo+30s+600s). Inmaduras excluidas; sin denominador, cobertura NULL. Los errores son solo del **subconjunto evaluado**, no del conjunto completo.\n\n'
    text+=table(['Tipo','Min','n evaluada','MAE subconjunto','RMSE subconjunto','Sesgo','Numerador','Denominador','Cobertura %'],[[x['tipo'],x['horizonte_min'],x['n'],fmt(x['mae']),fmt(x['rmse']),fmt(x['sesgo']),x['numerador'],x['denominador'],fmt(x['cobertura_pct'])] for x in rows])
    text+='\nA 10 min los candidatos están a −30,084 y +34,521 segundos: fuera de banda. No hay MAE/RMSE estimable ni se cambia de cohorte o tolerancia para ocultarlo. A 5/15 min existe un rechazo por trayectoria/inactividad en cada tipo. Los grupos quedaron cerrados tras mantener captura hasta superar el horizonte máximo y margen.\n\n'
    text+='El dashboard diferencia fin_prueba/auditoria_actual, muestra periodos efectivos y causas. Las predicciones posteriores, especialmente la última captura, se separan del resultado primario: que estén inmaduras al cierre no es error cero ni falta de cobertura madura. [Datos públicos filtrados](evidencias/resultados.json).\n\n'
    text+=table(['Tipo','Min','Orígenes evaluados desde UTC','Hasta UTC','Objetivos desde UTC','Hasta UTC'],[[x['tipo'],x['horizonte_min'],x['evaluadas_origen_inicio_utc'],x['evaluadas_origen_fin_utc'],x['evaluadas_objetivo_inicio_utc'],x['evaluadas_objetivo_fin_utc']] for x in rows])
    text+='\n## Latencias\n\n'+table(['Medida','n','Mediana s','p95 s','Desde UTC','Hasta UTC'],[[x['metrica'],x['n'],fmt(x['mediana_s']),fmt(x['p95_s']),x['periodo_inicio_utc'],x['periodo_fin_utc']] for x in r['latencias']])
    text+='\nAntigüedad de fuente/reporte no es procesamiento. La marca Spark no es fin de escritura; SQL confirmado incluye espera y persistencia; evaluación incluye margen deliberado y colas. n por predicción comparte capturas: no equivale a n periodos independientes. No se infiere generalización semanal/estacional ni precisión física.\n'
    (d/'resultados.md').write_text(text)
    mib=r['almacenamiento']['incremento_mib_por_captura'];day300=mib*288/1024;day60=mib*1440/1024
    bytes_parquet=sum(f.stat().st_size for f in (ROOT/'data/streaming/correcciones_u2').rglob('*') if f.is_file())
    cost={'consulta_precios_fecha':'2026-11-19','fuente':'https://aws.amazon.com/lightsail/pricing/','escenario':'Linux/Unix General Purpose IPv4, US East N.Virginia como supuesto, sin contratación',
      'vm_usd_mes':84,'ram_gb_tarifa':16,'vcpu':4,'ssd_gb_incluidos':320,'transfer_tb_incluidos':6,
      'bloque_gb_supuestos':500,'bloque_usd_gb_mes':0.10,'snapshot_gb_supuestos':200,'snapshot_usd_gb_mes':0.05,
      'total_usd_mes_escenario':84+500*0.10+200*0.05,'gasto_cloud_nuevo_real':0,
      'incremento_sql_mib_por_captura_medido':mib,'sql_gib_dia_a_300s_estimado':day300,'sql_gib_dia_a_60s_estimado':day60,
      'sql_gib_7dias_300s_estimado':day300*7,'parquet_y_checkpoints_bytes_logicos_medido':bytes_parquet,
      'supuestos':'Carga constante y patrón físico similar; no incluye WAL en medición SQL; retención7d presupuestaria no aplicada, margen2x, tráfico supuesto100GB/mes, impuestos/operación extra. No es factura ni SLA.'}
    (ROOT/'reports/correcciones/costos_cuantificados.json').write_text(json.dumps(cost,ensure_ascii=False,indent=2)+'\n')
    text='''# Costos cuantificados y criterios de escalado

Consulta oficial **19/11/2026**: [AWS Lightsail](https://aws.amazon.com/lightsail/pricing/).
Escenario de planificación en N.Virginia: Linux/Unix General Purpose con IPv4.
Los **USD 144/mes son únicamente una estimación documental**. No se crearon,
contrataron ni desplegaron recursos cloud de pago; gasto cloud nuevo real **USD 0**.
Todo permanece local; no se activaron servicios de pago.
La ejecución fue local. No se presenta estimación como factura.

| Concepto | Precio oficial / supuesto | Estimación mensual USD |
|---|---|---:|
| VM | 16GB,4vCPU,320GB SSD y6TB transferencia incluidos | 84 |
| Bloque adicional | 500GB supuestos ×0,10 USD/GB-mes | 50 |
| Snapshot | 200GB retenidos supuestos ×0,05 USD/GB-mes | 10 |
| Total escenario continuo | Sin impuestos, trabajo operativo ni sobreconsumo | **144** |

Sin bloque adicional, el subtotal sería USD94/mes, con menor margen de disco.
No se añade PostgreSQL administrado: corre en la misma VM. No duplicar ese costo.
Se supone transferencia100GB/mes, inferior a la incluida; no se midió tráfico
facturable. Región, elegibilidad y sobreconsumos deben revisarse antes de contratar.

## Calibración con datos reales

'''
    text+=f"La prueba produjo {r['capturas']} capturas y {r['observaciones_sql']} observaciones. El aumento físico de cuatro tablas fue {r['almacenamiento']['incremento_bytes']} bytes, aproximadamente **{mib:.3f}MiB/captura**. Es una diferencia de asignación con índices/bloat, afectada por vacuum, no tamaño lógico puro por flujo ni WAL. Archivos Parquet/checkpoints del ensayo: {bytes_parquet} bytes lógicos medidos.\n\n"
    text+=table(['Frecuencia supuesta','Capturas/día','SQL GiB/día proyectados'],[[300,288,f'{day300:.3f}'],[60,1440,f'{day60:.3f}']])
    text+=f"\nSiete días a300s: aproximadamente {day300*7:.3f}GiB SQL antes de margen2x, WAL, Parquet, imágenes y backups. Para presupuestar se suponen 7d de retención, margen2x y40GB para sistema/imágenes; **no se activó purga ni se eliminaron datos**. Sin retención futura el costo/disco crecerá: no afirmar operación ilimitada con un bloque fijo.\n\n"
    text+='''La medición corta incluye muchas evaluaciones aún pendientes: cierres y mantenimiento
pueden aumentar espacio. El supuesto anterior de20KiB no se presenta ya como
medición: se sustituye por esta calibración aproximada, todavía no validada durante días.

## Local y escalado

En local se reutilizan equipo/Docker. Electricidad y amortización no se midieron.
Con potencia **supuesta** de80W, 24h consumen1,92kWh; multiplicar por la tarifa
real de la factura, no inventar gasto monetario. Hardware existente no es gratis.

Criterios propuestos: captura→SQL p95>150s durante10min (mitad del horizonte mínimo5min),
backlog>dos capturas sostenido2min, o capacidad prevista<7d/margen de disco<30%.
Investigar primero colas/SQL/IO y mediciones antes de ampliar recursos. CPU>70%
sostenida15min es un criterio futuro: falta instrumentar hardware y probar carga.
No se aumentó memoria automáticamente ni se desplegó autoscaling/HA.

Los límites no son reservas. La VM propuesta necesita benchmark: no se garantiza
que4vCPU sostengan dos Spark, broker, SQL y observabilidad24/7. El ensayo usó local[2].
Quedan precio final de cuenta/impuestos, operación, restauración/HA y política de
retención efectivamente acordada antes de un despliegue continuo.
'''
    (d/'costos.md').write_text(text);(ROOT/'costos.md').write_text(text+'\nDatos: [costos_cuantificados.json](reports/correcciones/costos_cuantificados.json).\n')
    print({'documentacion':'fuentes locales generadas','costo_estimado_usd_mes':cost['total_usd_mes_escenario'],'gasto_cloud_nuevo_real':0})

if __name__=='__main__':main()
