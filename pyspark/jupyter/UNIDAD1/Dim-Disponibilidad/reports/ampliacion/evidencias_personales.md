# Evidencias personales y sustentación pendientes

El código, los resultados y un notebook ejecutado son evidencia técnica. No
sustituyen la autoría, las capturas ni las respuestas propias. La referencia es
la [rúbrica U1](https://262bigdata.github.io/lambda26/proyecto-sello/u1-producto/)
y la [sesión S5, sección 4](https://262bigdata.github.io/lambda26/sesiones/S05_Evaluacion_Unidad_1/).

## Capturas que debe realizar el estudiante

Mostrar fecha/hora del sistema e identidad/perfil sin recortarlos; mantener
coherencia con el historial real del aporte. No mostrar archivos de entorno,
tokens, claves ni cabeceras de acceso. No se generaron capturas simuladas.

| Captura | Contenido técnico que ya prepara el proyecto | Qué debe explicar personalmente |
|---|---|---|
| Pregunta y arquitectura | README y propuesta del brief, rutas U1/futura U2 | Disponibilidad no es demanda; elección Lambda frente a Kappa |
| Datos y extracción | Cobertura por fecha, esquemas, ejemplos y catálogo del notebook | Snapshots GBFS frente a CSV de viajes; tres relojes y zonas |
| Distribución y ejecución | Ejercicio RDD, reconciliación SQL y jobs antes/después de la acción | Transformaciones/acción, evaluación diferida y plan |
| Calidad | Reconciliación Silver, exclusiones, cantidades originales y discrepancias | Inactividad frente a invalidez; ausencia frente a cero; no deduplicar por last_reported |
| Gold | Conteos por periodo/horizonte, lectura de vuelta y filtro físico | Rezagos por tiempo, huecos, tolerancia, purga y prevención de fuga |
| Comparación ML | Tres LR, dos RF y persistencia con las tres métricas | Selección con validación, mismas filas y por qué test no elige modelo/horizonte |
| Interpretación | Importancias RF y errores cambia/igual, con/sin discrepancias | Asociación no causalidad; igualdad entre extremos no demuestra ausencia de movimientos |
| Guardado y recarga | Contrato ganador y referencia MLlib, pruebas de predicciones | Persistencia no necesita entrenamiento; referencia aprendida no sustituye al ganador |

Usar la ejecución ampliada identificada en el informe de resultados. Las capturas
de la prueba preliminar de un día no se presentan como si pertenecieran a ella.
El notebook permite mostrar todo sin imprimir credenciales ni traer el dataset
completo al driver. Reloj/usuario de los logs no reemplazan una captura propia.

## Defensa individual

Preparar el pitch visual previo y después la exposición técnica, demo y preguntas
con los tiempos de S5: 8, 5 y 5 minutos. Los seis aspectos de la tabla 4 de U1
incluyen aporte, comunicación, presentación/actitud, prácticas de repositorio,
documentación publicada y pitch. Esta auditoría no los califica automáticamente.

Preguntas que debe poder resolver con sus palabras y evidencia:

1. ¿Qué variable se predice y qué incertidumbre física queda en los reportes?
2. ¿Por qué se usa el conteo por tipo y no una resta ni la extensión eléctrica?
3. ¿Qué decide el umbral de 450 s y qué no demuestra?
4. ¿Qué evita cruzar un hueco, una inactividad o un límite temporal con la etiqueta?
5. ¿Cuándo se aprende la mediana y cuál es el orden del vector de MLlib?
6. ¿Por qué se preserva el ganador por validación aunque otro tenga una métrica
   de prueba mejor en alguna combinación?
7. ¿Qué significan las importancias RF con predictores correlacionados?
8. ¿Qué permite concluir una semana y qué exigiría más semanas o temporadas?

## Repositorio y documentación

La dimensión permanece local y sin commits/push por instrucción explícita.
Corresponde incorporar después el aporte al historial real, comprobar los topics
académicos y publicar documentación navegable en MkDocs o equivalente. La propuesta
de actualización del brief requiere coordinación con el equipo; no modifica a
los compañeros ni acredita U2. No inventar commits, fechas, exposición o feedback.

Si se entregan también PDF/feedback de sesiones individuales, el estudiante debe
completar los datos y reflexiones personales solicitados por esas guías. Esos
entregables no se declaran realizados por ejecutar el pipeline.
