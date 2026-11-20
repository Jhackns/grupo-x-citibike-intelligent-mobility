# Paralelismo configurable para próximas ejecuciones

Cambio verificado el 17/11/2026, versión de integración
`u1-integrado-v3-spark-master`. No se ejecutaron lectura del histórico, Silver,
Gold ni entrenamiento para comprobarlo.

## Contrato

- Predeterminado: `local[4]` en `config/u1-local.json`.
- `--spark-master 'local[2]'` reduce el paralelismo cuando hay menos recursos.
- Precedencia: opción CLI, JSON explícito, configuración predeterminada.
- Se admite `local[N]`, con N entero positivo. Las opciones no alteran memoria:
  el driver permanece en 4 GiB y shuffle en 32 particiones.
- Se aceptan los JSON anteriores con `cores`, convirtiéndolo en memoria a
  `spark_master`. Valores contradictorios entre ambos campos se rechazan.
- CLI y notebook reciben el mismo JSON efectivo; `src/ejecucion.py::crear_spark`
  es la fábrica común usada por `src/flujo_u1.py` y el notebook.
- El lanzador propaga master y memoria mediante `PYSPARK_SUBMIT_ARGS`. La fábrica
  comprueba que el master real coincida; una sesión activa distinta exige un
  proceso/kernel nuevo. No se cambia silenciosamente una sesión existente.

Comando para una futura ejecución completa con menos recursos:

```bash
.venv/bin/python pyspark/jupyter/UNIDAD1/Dim-Disponibilidad/ejecutar_u1.py --spark-master 'local[2]' --notebook
```

Omitir `--spark-master` utiliza el valor predeterminado salvo que un JSON explícito
lo reemplace. En Run All interactivo se respeta la configuración del último
lanzamiento, incluida una selección explícita de menos hilos.

Cambiar código/configuración cambia la huella de compatibilidad. Los comandos
completos pueden iniciar una nueva preparación y entrenamiento si no existe una
ejecución compatible. No se relajó esa comprobación para reutilizar versiones distintas.

## Verificación pequeña realizada

Se ejecutó el helper real en el contenedor existente, sin leer raw ni modelos:

| Configuración | Master real | Paralelismo observado | Filas sintéticas | Resultado de map/sum |
|---|---|---:|---:|---:|
| Predeterminada | local[4] | 4 | 32 | 992 |
| Reemplazo explícito | local[2] | 2 | 32 | 992 |

`tests/test_spark_master.py`: una prueba con ambos casos, **OK**, 12,760 s.
`tests/test_ejecucion.py`: **seis pruebas OK**; incluyen precedencia, lectura
de JSON antiguo sin reescribirlo, rechazo de contradicciones/valores inválidos,
y propagación simulada a CLI/notebook sin ejecutar procesamiento.

Para repetir únicamente estas comprobaciones desde la raíz del repositorio:

```bash
PYTHONPATH=pyspark/jupyter/UNIDAD1/Dim-Disponibilidad .venv/bin/python -m unittest discover -s pyspark/jupyter/UNIDAD1/Dim-Disponibilidad/tests -p test_ejecucion.py -v
docker exec -e 'PYSPARK_SUBMIT_ARGS=--driver-memory 4g pyspark-shell' -w /opt/UNIDAD1/Dim-Disponibilidad lambda26-pyspark python -m unittest discover -s tests -p test_spark_master.py -v
```

No se repitió el notebook completo. Su código se comprobó sintácticamente y usa
la misma fábrica probada; se limpiaron sus salidas para no atribuirle resultados
de una versión anterior.

## Conservación de la evidencia ampliada

La ejecución `20261117T171917344468Z` y su reutilización
`20261117T191621979673Z` utilizaron **local[2]**. Sus métricas, tiempos, modelos y
manifiestos no se modificaron ni se atribuyen a local[4]. El notebook ejecutado
está [archivado](../../notebooks/archivo/01_historico_ampliado_20261117T171917344468Z.ipynb).
La evidencia de ejecución completa corresponde a esa versión histórica, no a
una ejecución completa de este cambio.

Se compararon SHA-256 antes/después de **16 archivos**: manifiestos y
configuraciones de ejecuciones anteriores, notebooks archivados y archivos
Dockerfile/Compose. Todos permanecieron idénticos. No se modificaron servicios,
datos originales ni dimensiones de compañeros. Sin nuevas dependencias,
descargas, commits o push.
