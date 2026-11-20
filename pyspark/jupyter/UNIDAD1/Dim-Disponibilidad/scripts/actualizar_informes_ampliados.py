#!/usr/bin/env python3
"""Formatea una ejecución U1 completa; no descarga, transforma ni entrena."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from src.ejecucion import comprobar_artefactos


def table(rows, columns):
    def value(x):
        return f'{x:.8f}' if isinstance(x,float) else str(x)
    return '\n'.join(['| '+' | '.join(columns)+' |','| '+' | '.join(['---']*len(columns))+' |']+
        ['| '+' | '.join(value(r.get(c,'')) for c in columns)+' |' for r in rows])


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('ejecucion',help='ID de la ejecución ampliada completa')
    args=p.parse_args()
    if Path(args.ejecucion).name!=args.ejecucion:
        raise ValueError('Usar solo el identificador de ejecución')
    source=ROOT/'data/ejecuciones'/args.ejecucion
    meta=json.loads((source/'manifiesto.json').read_text())
    if meta['estado']!='completada':
        raise ValueError('No se publican como completas ejecuciones pendientes/fallidas')
    if meta['modo']=='reutilizada':
        source=source.parent/meta['ejecucion_fuente']
        meta=json.loads((source/'manifiesto.json').read_text())
    comprobar_artefactos(source,meta['artefactos'])
    result=json.loads((source/'modelos/resultado.json').read_text())
    freeze=json.loads((source/'modelos/seleccion_congelada.json').read_text())
    assert len(freeze['comparativa_validacion'])==len(result['metricas_prueba'])==36
    assert freeze['congelada_utc']<result['lectura_prueba_utc']
    assert meta['conteos']['gold']['candidatos']==meta['conteos']['silver']['entrada']*3
    assert sum(meta['conteos']['gold'][k] for k in ('aceptados','excluidos'))==meta['conteos']['gold']['candidatos']
    plan=(source/'analisis/plan_gold_fecha.txt').read_text()
    assert 'InMemoryTableScan' not in plan and '2026-11-10' in plan.split('PartitionFilters:',1)[1].split('PushedFilters:',1)[0]
    notebook=ROOT/'notebooks/01_disponibilidad_batch.ipynb'
    nb=json.loads(notebook.read_text())
    code=[c for c in nb['cells'] if c['cell_type']=='code']
    assert len(code)==11 and [c['execution_count'] for c in code]==list(range(1,12))
    assert not [o for c in code for o in c.get('outputs',[]) if o.get('output_type')=='error']
    first=''.join(''.join(o.get('text',[])) for o in code[0].get('outputs',[]))
    assert args.ejecucion in first, 'El notebook pertenece a otra ejecución'
    archive=ROOT/'notebooks/archivo'/f'01_historico_ampliado_{args.ejecucion}.ipynb'
    archive.parent.mkdir(exist_ok=True)
    if archive.exists():
        assert archive.read_bytes()==notebook.read_bytes(), 'No sobrescribir el notebook archivado'
    else:
        shutil.copyfile(notebook,archive)
    dest=ROOT/'reports/ampliacion';dest.mkdir(exist_ok=True)
    rows=[]
    for r in meta['metricas']:
        base=next(b for b in result['metricas_prueba'] if b['tipo']==r['tipo'] and b['horizonte_min']==r['horizonte_min'] and b['candidato']=='persistencia')
        rows.append({**r,'MAE_persistencia':base['MAE']})
    groups=[]
    for r in meta['metricas']:
        group=[s for s in result['subgrupos_prueba'] if s['tipo']==r['tipo'] and s['horizonte_min']==r['horizonte_min'] and s['candidato']==r['candidato']]
        for dim in ('estrato_discrepancia','cambio_objetivo'):
            assert sum(s['n'] for s in group if s['dimension']==dim)==r['n']
        groups += [{**s,'categoria':s[s['dimension']]} for s in group]
    coverage=meta['analisis']['cobertura_fecha']
    observed=sum(x['discrepancia_total'] for x in coverage)
    lines=['# Resultados U1 con el histórico ampliado','',
        'Ejecución completa **`'+source.name+'`**, distinta de la prueba preliminar de un día. Fuente de verdad: su manifiesto y sus tablas locales; no se mezclan ejecuciones interrumpidas o antiguas.','',
        '## Datos, periodos y calidad','',
        'Se utilizaron 2.121 originales (149.089.863 bytes): 2.105 estados y ocho capturas de cada catálogo. Rango: 9/11/2026 desde 16:35 UTC, contexto parcial; siete días UTC de 288 capturas, 10–16/11. Último día terminado al inventariar: 16/11. Se descargaron 1.831 pendientes, se conservaron 290 y la segunda pasada omitió todos sin transferir.','',
        'Train: 10–13; validación: 14; prueba: 15–16, todos UTC, comunes a estaciones. El contexto del 9 no se usa como emisión entrenable. Los objetivos que cruzan cortes se purgan. Es un solo ciclo semanal: no demuestra estacionalidad ni repetibilidad semanal. Los horarios locales no coinciden con las fronteras UTC.','',
        'Conteos anchos reconciliados: `'+json.dumps({k:meta['conteos'][k] for k in ('silver','gold')})+'`. Gold contiene dos objetivos, no dos copias por tipo. La entrada no tenía claves estación/captura repetidas, IDs vacíos ni timestamps de captura repetidos entre archivos.','',
        f'Hay {observed} discrepancias total/tipos sobre {meta["conteos"]["silver"]["entrada"]} observaciones. La representación por tipo sigue siendo una convención de objetivo; su exactitud física y la causa de las diferencias siguen sin confirmarse. No se fuerza igualdad ni se excluyen todos los casos discrepantes.','',
        '[Detalle de cobertura/calidad](cobertura_y_calidad.md) y [protocolo fijado previamente](metodologia.md).','',
        '### Poblaciones por tipo/horizonte y partición','',
        table(sorted(meta['conteos']['por_tipo_horizonte_particion'],key=lambda r:(r['tipo'],r['horizonte_min'],r['particion_temporal'])),
              ['tipo','horizonte_min','particion_temporal','count']),'',
        '## Comparación y selección','',
        'Se realizaron **30 ajustes MLlib**: tres regularizaciones LR y dos RF por cada tipo/horizonte, además de persistencia sin entrenamiento. Son 36 comparaciones de validación y 36 de prueba, con las mismas filas por combinación. Imputación/transformaciones solo se ajustaron con train. No se ajustaron cortes ni candidatos después de consultar prueba.','',
        'El log de LR sin regularización registró covarianza singular y el reintento automático de Spark con Quasi-Newton. Se conservó ese candidato preregistrado y se exigieron predicciones finitas; no se cambiaron los hiperparámetros para favorecerlo. La redundancia de variables es una explicación posible, no un diagnóstico causal probado.','',
        'La selección se congeló a `'+freeze['congelada_utc']+'`; evaluación de prueba desde `'+result['lectura_prueba_utc']+'`. SHA-256 congelado: `'+meta['seleccion_sha256']+'`. El criterio fue MAE de validación, no las métricas de esta tabla de prueba. No se decide un horizonte definitivo.','',
        table(rows,['tipo','horizonte_min','candidato','n','MAE','RMSE','R2','MAE_persistencia']),'',
        '[Comparación completa de candidatos](comparacion_modelos.md). Se mantienen ganadores reales, sean estimadores o persistencia. Esta última no requiere fit. El mejor candidato MLlib por validación también queda identificado como referencia, sin reemplazar al ganador global.','',
        '### Estratos de los ganadores','',
        table(groups,['tipo','horizonte_min','candidato','dimension','categoria','n','MAE','RMSE','R2']),'',
        'Todos, con/sin discrepancias y cambia/igual se calculan con las mismas predicciones. Cada dimensión de subgrupos reconcilia su N global. Igual/cambia compara los extremos observados del horizonte: no demuestra ausencia de movimientos intermedios. No se redondearon predicciones a enteros; se aplicó el clipping negativo fijado antes de evaluar.','',
        '### Por estación','',
        table(result['resumen_estaciones'],['tipo','horizonte_min','candidato','estaciones','MAE_macro','p50_p95_MAE_estacion']),'',
        'Las tablas Parquet completas conservan N y errores por estación; el promedio macro no sustituye al global ponderado. Las verificaciones recargaron los resultados y reconciliaron suma de N y MAE ponderado. Las estaciones sin observaciones futuras elegibles no forman parte de esos denominadores.','',
        '## Interpretación de Random Forest','']
    importance=[]
    for path in sorted((source/'modelos/interpretacion').glob('*/*.json')):
        obj=json.loads(path.read_text());values=obj['importancias']
        assert all(r['importancia']>=0 for r in values)
        assert abs(sum(r['importancia'] for r in values)-1)<1e-8 or sum(r['importancia'] for r in values)==0
        for r in sorted(values,key=lambda r:-r['importancia'])[:3]:
            importance.append({'tipo':obj['tipo'],'horizonte_min':obj['horizonte_min'],'candidato':obj['candidato'],**r})
    assert len(importance)==36
    lines += [table(importance,['tipo','horizonte_min','candidato','variable_vector','importancia']),'',
        'Importancia por reducción de impureza sobre train. Los componentes están en el orden vectorial guardado; los indicadores de ausencia se distinguen de los conteos. La correlación entre actuales, rezagos y estadísticas puede repartir su importancia. No es efecto causal ni una validación física de las etiquetas.','',
        '## Artefactos, notebook y reproducibilidad','',
        f'Notebook principal completo desde kernel limpio: **11 celdas de código, 0 errores**, ejecución de entrenamiento nueva `{source.name}`. El mismo orquestador se ejecuta por CLI o notebook. No se usaron JSON de resultados preliminares para preparar estas entradas.','',
        'Versiones: `'+json.dumps(meta['versiones'])+'`. Duración del flujo: '+f"{meta['duracion_s']:.2f} s"+'.','',
        'Todas las recargas de ganadores y referencias MLlib:','',
        table(meta['recarga_verificada'],['tipo','horizonte_min','n','nulos','max_diferencia']),'',
        table(meta['recarga_referencias_mllib'],['tipo','horizonte_min','candidato','n','nulos','max_diferencia']),'',
        'Se guardan todos los pipelines en `modelos/candidatos/`; contratos ganadores y de referencia apuntan a esos mismos artefactos. El contrato incluye clipping y predictores; la función `predecir` aplica la transformación completa. `modelos/interpretacion/` conserva los doce RF y `modelos/resultado.json`/CSV guardan las métricas con precisión completa.','',
        'Raíz de esta única ejecución: `data/ejecuciones/'+source.name+'`. Schema/conteos/multiconjuntos, unicidad, tiempos, continuidad y purga fueron verificados. El plan de fecha se obtuvo sin caché e incluye fecha en PartitionFilters, con N comparado contra el conteo previo a escribir. Raw conserva sus hashes.','',
        'La prueba de un día se conserva como antecedente en su notebook archivado y ejecuciones originales. Sus métricas provienen de otra población y no cuantifican una mejora atribuible a ampliar el histórico. Dos ejecuciones ampliadas interrumpidas se conservan fallidas, sin reutilizar sus salidas.','',
        'Lectura del contraste por cambios: LR sin regularización tiene un MAE ligeramente menor que persistencia dentro del subconjunto donde cambia el conteo, en las seis combinaciones de prueba, pero pierde globalmente. Los conteos iguales pesan mucho y persistencia tiene error cero en ese subconjunto por definición. Este diagnóstico no cambia la selección ni autoriza afirmar utilidad operativa.','',
        '## Qué permite concluir','',
        'Permite comparar estos candidatos en los dos días posteriores reservados y describir sus errores por tipo, horizonte, discrepancias y cambio de conteo. No acredita una exactitud física ni un umbral operativo aceptable de redistribución. No demuestra generalización estacional ni un patrón semanal repetido; hay pocos días independientes y train contiene sobre todo días laborables. Los millones de pares estación/instante y sus horizontes solapados no equivalen a millones de periodos independientes. Un R² global elevado tampoco demuestra mejora frente a persistencia ni buen rendimiento cuando cambia el conteo. Se necesitan nuevas fechas prospectivas y más ciclos para evaluar estabilidad.','',
        '[Propuesta del brief y decisión Lambda/Kappa](propuesta_brief_disponibilidad.md). [Evidencias personales pendientes](evidencias_personales.md). U2 no está implementada. Sin cambios en compañeros, Dockerfile/Compose, commits o push.','']
    reuse_proof=dest/'verificacion_reutilizacion.json'
    if reuse_proof.exists():
        reuse=json.loads(reuse_proof.read_text())
        if reuse['ejecucion_fuente']==source.name:
            assert reuse['estado']=='completada' and reuse['modo']=='reutilizada'
            assert len(reuse['recarga_verificada'])==6
            assert all(r['max_diferencia']==0 and r['nulos']==0 for r in reuse['recarga_verificada'])
            lines += ['## Reutilización comprobada','',
                f"La ejecución `{reuse['id']}` reutilizó esta fuente compatible en {reuse['duracion_s']:.2f} s, sin entrenar ni descargar. "
                'Verificó hashes y reprodujo las predicciones de los seis ganadores. '
                '[Registro público sin secretos](verificacion_reutilizacion.json). El notebook principal conserva la ejecución nueva completa, no se sustituyó por esta reutilización.','']
    for key in [(r['tipo'],r['horizonte_min']) for r in rows]:
        name=f'{key[0]}_{key[1]}min_observado.svg'
        shutil.copyfile(source/'modelos'/name,dest/name)
        lines += [f'![{key[0]} {key[1]} minutos]({name})','']
    (dest/'resultados_ampliados.md').write_text('\n'.join(lines))
    comparative=['# Comparación completa — histórico ampliado','',
        'Fuente única: `'+source.name+'`. MAE/RMSE en conteos reportados; R² adicional. Predicciones sin redondear. Modelos y cortes fijados antes de prueba.','',
        '## Validación: decide el ganador por combinación','',
        table(freeze['comparativa_validacion'],['tipo','horizonte_min','candidato','n_train','n','MAE','RMSE','R2','duracion_ajuste_s']),'',
        '## Prueba posterior a la congelación: no selecciona','',
        table(result['metricas_prueba'],['tipo','horizonte_min','candidato','seleccionado','n','MAE','RMSE','R2','predicciones_negativas_raw']),'',
        'Los conteos coinciden entre candidatos dentro de cada combinación. La selección sigue siendo la de validación aunque otra métrica de prueba favorezca a un competidor. No se selecciona horizonte final con esta comparación.','']
    changes=[]
    for r in result['metricas_prueba']:
        strata=[s for s in result['subgrupos_prueba'] if s['dimension']=='cambio_objetivo' and
                all(s[k]==r[k] for k in ('tipo','horizonte_min','candidato'))]
        row={k:r[k] for k in ('tipo','horizonte_min','candidato')}
        for category in ('cambia','igual'):
            part=[s for s in strata if s['cambio_objetivo']==category]
            assert len(part)<=1
            row['n_'+category]=part[0]['n'] if part else 0
            row['MAE_'+category]=part[0]['MAE'] if part else None
        assert row['n_cambia']+row['n_igual']==r['n']
        changes.append(row)
    comparative += ['## Cambio del conteo entre los extremos: todos los candidatos','',
        table(changes,['tipo','horizonte_min','candidato','n_cambia','MAE_cambia','n_igual','MAE_igual']),'',
        'Estos estratos son diagnósticos posteriores a la selección y no eligen modelo. Igualdad entre los extremos no acredita ausencia de viajes o movimientos intermedios. Las otras métricas y los estratos de discrepancias completos están en `modelos/subgrupos_prueba.csv`.','']
    (dest/'comparacion_modelos.md').write_text('\n'.join(comparative))
    review = [
        '# Cumplimiento U1 — histórico ampliado', '',
        'Contraste realizado el 17/11/2026 con la [rúbrica oficial](https://262bigdata.github.io/lambda26/proyecto-sello/u1-producto/). '
        'Las precisiones de comparación/interpretación proceden de [S4](https://262bigdata.github.io/lambda26/sesiones/S04_ML_Distribuido_Regresion_Spark_MLlib/); '
        'las de ejecución y defensa, de [S5](https://262bigdata.github.io/lambda26/sesiones/S05_Evaluacion_Unidad_1/). '
        'No se asigna una calificación ni se sustituye la evaluación docente.', '',
        f'Ejecución técnica de referencia: `{source.name}`. Notebook completo nuevo desde kernel limpio, sin reutilizar el experimento preliminar. '
        'Los archivos de `data/` son locales e ignorados por Git; los informes publican sus resultados sin secretos.', '',
        '| Requisito | Evidencia concreta | Estado | Pendiente |',
        '|---|---|---|---|',
        '| Arquitectura justificada (12 %) | [Propuesta del brief](propuesta_brief_disponibilidad.md): diagrama, decisión Lambda/Kappa, histórico auditable, replay y riesgos de duplicar lógica. README separa batch real y streaming futuro. | Evidencia técnica disponible | Acordar con el equipo la propuesta del aporte propio; no se reescribió el brief común. |',
        '| Spark distribuido y plan (16 %) | `src/lectura.py`, `src/calidad.py`, `src/diagnostico.py`; notebook ejecutado; `analisis/rdd_y_lazy.json` reconcilia map/reduceByKey con DataFrame y registra cero jobs al construir el plan, jobs al ejecutar count. | Verificado | Demostrar y explicar personalmente transformación/acción. |',
        '| Parquet particionado y lectura de vuelta (16 %) | Silver/Gold de esta ejecución; `analisis/plan_gold_fecha.txt` sin InMemoryTableScan, con fecha en PartitionFilters; conteos, esquema, multiconjuntos, unicidad y purga reconciliados. | Verificado | Captura auténtica del plan y sus conteos. |',
        '| Notebook reproducible (12 %) | `notebooks/01_disponibilidad_batch.ipynb`: 11 celdas de código, secuencia 1–11 y cero errores; llama al mismo `src/flujo_u1.py` que la CLI. Configuración y hashes en manifiesto. | Verificado en Docker existente | El estudiante debe ejecutar las celdas pertinentes durante su demo. |',
        '| ML entrenado, comparado, evaluado y guardado (16 %) | [36 comparaciones por partición](comparacion_modelos.md), 30 ajustes MLlib, MAE/RMSE/R²; tres LR, dos RF; doce archivos de importancia; contratos ganadores y mejores referencias MLlib con recarga verificada. | Verificado técnicamente | Explicar el ganador por validación y la incertidumbre operativa; no atribuir entrenamiento a persistencia. |',
        '| Evidencias/documentación (8 %) | README, metodología, resultados, cobertura, notebook y manifiestos con versiones, entradas, métricas, tiempos y errores. | Técnica disponible; evidencia personal pendiente | Capturas propias con reloj/perfil, historial coherente y documentación publicada. No se crearon commits ni push. |',
        '| Sustentación individual (20 %) | [Guía de evidencias personales](evidencias_personales.md), preguntas y rutas para la demostración. | Pendiente del estudiante | Autoría/aporte, exposición, presentación/actitud, repositorio/topics, publicación y pitch; responder preguntas en vivo. |', '',
        '## Pendientes técnicos de la revisión anterior', '',
        '- Lambda/Kappa y propuesta del brief: documentadas sin alterar las dimensiones ajenas.',
        '- RDD y ejecución diferida: ejecutados sobre las 5.302.934 observaciones, con agregados pequeños y reconciliación.',
        '- Tercera configuración: LR sin regularización, además de ridge 0.01/0.1, fijadas antes de prueba. Son tres configuraciones acotadas; no una búsqueda exhaustiva.',
        '- Importancia RF: guardada para ambos tamaños de bosque, tipos y horizontes; top variables en notebook e informe. Es atribución por impureza, no causalidad.',
        '- Notebook limpio: ejecución completa verificada, no celdas sueltas ni salidas copiadas del día preliminar.', '',
        'El ganador global conserva su contrato y sus métricas. Cuando es persistencia, no requiere fit. '
        'El mejor candidato **entrenado MLlib** se identifica por validación en `modelos/referencias_mllib/<tipo_horizonte>/contrato.json`, '
        'con enlace al pipeline guardado en `modelos/candidatos/`; no se presenta como vencedor global si perdió. '
        'Las recargas de ambos roles reproducen todas las predicciones verificadas.', '',
        '## Límites que permanecen', '',
        'Una semana y dos días UTC de prueba no demuestran generalización estacional ni repetibilidad semanal. '
        'Los objetivos son reportes por tipo, sin verdad física confirmada. No se fijó un umbral de MAE operativamente aceptable, '
        'ni se validaron decisiones reales de redistribución. No se eligió un horizonte definitivo.', '',
        'La entrega técnica no acredita capturas personales, publicación, aprobación del cambio de brief, defensa ni autoría demostrada. '
        'Los informes de sesiones individuales, PDF, reflexión y feedback personal deben completarse si corresponden a la entrega docente. '
        'La prueba preliminar y los intentos interrumpidos permanecen identificados por separado. U2 sigue sin implementar.', ''
    ]
    (dest/'cumplimiento_actualizado.md').write_text('\n'.join(review))
    proof={'ejecucion':args.ejecucion,'fuente':source.name,'notebook_sha256':hashlib.sha256(notebook.read_bytes()).hexdigest(),
        'notebook_archivado':archive.relative_to(ROOT).as_posix(),
        'codecells':11,'errores':0,'celdas':[c['id'] for c in code],'gold_version':meta['versiones']['gold'],
        'registro_candidatos':meta['conteos']['gold']['candidatos'],'recargas_ganadores':meta['recarga_verificada'],
        'recargas_referencias':meta['recarga_referencias_mllib']}
    # Evidencia publicable sin secretos; no escribir en directorios creados por root/Docker.
    check=dest/'verificacion_notebook.json'
    check.write_text(json.dumps(proof,indent=2))
    print('Informes ampliados generados:',dest)


if __name__=='__main__':
    main()
