# Auditoría previa: 19.572 predicciones vencidas

Consulta ejecutada el 19/11/2026 a las 14:57 UTC, **antes de crear paneles**, sobre
el flujo explícito `disponibilidad-etapa3-vivo-20261119-v1`. Evidencia completa:
[vencidas.json](vencidas.json), consulta `../../sql/auditar_vencidas.sql` y
herramienta `../../auditar_vencidas.py`. Se revisaron los candidatos originales,
el catálogo/definición por tipo ya validados y la calidad de la observación futura.

| Motivo persistido | Predicciones | Resultado comprobado |
|---|---:|---|
| sin_captura_objetivo_en_tolerancia | 19.554 | No hay observación de la misma estación dentro de ±30 s de la fecha objetivo. |
| objetivo_no_elegible | 18 | La observación seleccionada existe pero incumple calidad del objetivo. |
| Total | 19.572 | Cero falsos faltantes y cero objetivos marcados inválidos que fueran elegibles. |

Las 19.554 pertenecen a cuatro cohortes de horizonte 5 minutos; son conteos de
predicciones, incluyendo ambos tipos, no de estaciones distintas:

| Origen UTC | Objetivo ideal UTC | Distancia absoluta al candidato más cercano (s) | Predicciones |
|---|---|---:|---:|
| 03:59:05.107 | 04:04:05.107 | 30,338 | 4.888 |
| 03:55:42.760 | 04:00:42.760 | 31,480 | 4.888 |
| 04:00:08.896 | 04:05:08.896 | 33,451 | 4.888 |
| 03:51:16.403 | 03:56:16.403 | 33,643 | 4.890 |

Todas son del 19/11/2026. En cada caso existe captura a menos de 450 s, pero
**ninguna dentro de la banda de emparejamiento de 30 s**. La cadencia real de la
prueba, superior a 60 s y variable, deja estos huecos respecto de algunos objetivos
ideales. Los límites estrictos explican las exclusiones; no demuestran que la
comparación temporal tenga un defecto. Ampliarlos para mejorar cobertura cambiaría
el criterio heredado de U1 y no se hizo.

De las otras 18, **16** corresponden a la estación `2098359443836787630`: instalada,
pero no alquila/devuelve, con reportes antiguos (edades observadas entre 560 y
1.340 s). Las **2** restantes corresponden a `2177015776583073844`, operativa pero
con reporte de 492 s, superior al umbral heredado de 450 s. Los flags originales
y motivos por tipo están conservados en JSON. Una estación inactiva no equivale
a disponibilidad cero. No se buscaron candidatos alternativos para eludir la
calidad del candidato más cercano.

Por separado, las **283.408 pendientes** archivadas no son estas vencidas. La
prueba de etapa 3 se detuvo sin avance suficiente de la fuente para cerrar sus
bandas con margen tardío de 600 s: el reloj puede haber madurado, pero la barrera
de progreso no. El panel las muestra como pendientes sin avance, no como
inmaduras ni vencidas automáticamente. La comprobación encontró **cero pendientes
cerrables** en ese flujo.

**Conclusión:** no se demostró un fallo de emparejamiento. No se corrigió el
algoritmo ni se alteraron tolerancias, calidad, decisiones SQL ni conteos históricos.
