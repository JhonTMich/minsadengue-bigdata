# Validación temporal del clima

Entrenamiento 2005–2014; validación 2015–2019. La climatología se calcula con años hasta 2014. Se excluyen las cuatro últimas semanas de entrenamiento para impedir que sus etiquetas crucen a validación. El panel se corta en 2019 antes de construir el objetivo; tampoco se usan desenlaces de 2020.

Es un experimento exploratorio posterior a inspeccionar el test: no es una confirmación independiente. No se volvió a evaluar ni ajustar con 2021–2024. Se mantuvieron definición de brote, pesos por clase, ventanas y umbral 0.5.

## Semanas sin brote vigente

| Algoritmo | Variables | AUC-ROC | Average precision | Precisión | Recall |
|---|---|---:|---:|---:|---:|
| logistica | casos | 0.7587 | 0.2644 | 0.2620 | 0.5239 |
| logistica | casos_anomalias | 0.7650 | 0.2798 | 0.2634 | 0.5946 |
| logistica | casos_anomalias_absolutas | 0.7730 | 0.3207 | 0.2700 | 0.6175 |
| random_forest | casos | 0.7581 | 0.2619 | 0.2514 | 0.7630 |
| random_forest | casos_anomalias | 0.7568 | 0.2810 | 0.2557 | 0.7256 |
| random_forest | casos_anomalias_absolutas | 0.7484 | 0.2344 | 0.2521 | 0.7318 |

La logística con anomalías y niveles absolutos mejora descriptivamente frente a casos solos en esta validación: AUC 0.7730 frente a 0.7587, y average precision 0.3207 frente a 0.2644. También cambia el balance de errores al umbral fijo; esto no demuestra una ventaja a igual recall o presupuesto de falsas alarmas. Las anomalías solas ofrecen una mejora menor. Random Forest con esta configuración no mejora al añadir clima.

Por tanto, no hay evidencia de que cambiar automáticamente a un modelo no lineal resuelva el problema. Los niveles absolutos son una hipótesis prometedora para la logística, pendiente de estabilidad temporal y regional. No corresponde sustituir todavía la línea base reportada en test.

## Protocolo y reproducción

Comparaciones sobre las mismas filas completas, tanto para casos solos como para clima. Las variables absolutas son medias de temperatura, humedad y precipitación semanal en las ventanas 0–3, 4–7 y 8–11 semanas. RF usa 50 árboles, profundidad 5 y semilla 42; logística usa maxIter=100. No hubo búsqueda de hiperparámetros ni calibración del umbral. Spark local[2], driver configurado en 4 GB.

```bash
docker compose exec -T pyspark-notebook python /home/jovyan/work/scripts/validar_clima_temporal.py
```

[Resultados completos](evidencias/validacion_clima/resultados.csv) y [protocolo con conteos y prevalencias](evidencias/validacion_clima/protocolo.json). Evidencia ejecutada localmente: `output/validacion_clima_temporal.ipynb`. Probabilidades locales en `data/metricas/validacion_clima/`.

El siguiente paso es comprobar los candidatos en varios cortes temporales dentro de 2005–2019, manteniendo embargo, climatología exclusiva de entrenamiento y comparaciones pareadas. Elegir variables y umbral allí. El periodo 2021–2024 ya inspeccionado no debe convertirse en un conjunto de ajuste; hará falta una evaluación independiente para confirmar generalización.
