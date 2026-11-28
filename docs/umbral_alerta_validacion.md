# Criterio del umbral de alerta

Decisión del 24 de noviembre de 2026: usar **costo simétrico 1:1**, con el umbral
que minimiza `FP + FN` en la validación completa 2021–2023. El umbral resultante del
modelo `contexto_clima` es **0.85**. Es un compromiso académico para reducir falsas
alarmas, no una valoración económica acordada con la organización ni una
certificación de utilidad sanitaria.

## Protocolo

- Se regeneraron Raw, Bronze, Silver y Gold con la carga histórica hasta 2023 en
  `data/_nube/`. Gold tiene 23 788 filas y 19 departamentos.
- Se mantuvieron los pasos y variables existentes. Entrenamiento de validación:
  2005–2020, excluyendo las cuatro semanas finales según el embargo del paso 4.
  Los años 2000–2004 solo aportan historia para construir el canal endémico.
- Validación: 2021–2023, con 2 888 filas etiquetadas y 2 051 positivas. Las últimas
  cuatro semanas por departamento no tienen etiqueta porque falta horizonte futuro.
- Sin brote vigente: 1 124 filas, de las cuales 337 son positivas.
- El objetivo sigue siendo al menos una semana de brote en las cuatro siguientes.
  No se cambió la definición para mejorar las métricas.
- La selección del candidato permanece igual: mayor average precision en
  validación sin brote actual. Se eligió `contexto_clima`. Todos los criterios se
  compararon sobre sus mismas probabilidades de validación, sin reentrenar por
  umbral ni cambiar hiperparámetros.
- La tasa de alertas es `(VP + FP) / N`; las falsas alarmas son FP, y FP/VP expresa
  falsas alarmas por semana positiva detectada. Son semanas-departamento, no
  episodios independientes de brote.
- No se agregó la carga de 2024 ni se ejecutó su puntuación o evaluación. Las
  referencias a 2024 en pruebas unitarias son datos sintéticos existentes.

## Comparación de criterios

Se contrastaron el costo 3:1 original, el costo 1:1 y un presupuesto de alertas del
20 % de las semanas-departamento de validación completa. Ese presupuesto equivale
a revisar aproximadamente cuatro departamentos por semana en promedio, sin
garantizar ese número en cada semana. Fue una propuesta académica, no una
capacidad institucional confirmada.

Los umbrales por costo se buscaron en la rejilla existente de 0.01 a 0.99, con
comparación `puntaje >= umbral`; un empate de costo favorece el umbral mayor.
Para el presupuesto se usó un corte empírico exacto sobre los puntajes, sin separar
empates. El umbral preciso se conserva en los CSV: redondearlo antes de aplicarlo
podría incumplir el presupuesto.

### Validación completa

| Criterio | Umbral | Alertas | Tasa de alertas | Recall | VP | FP | FN |
|---|---:|---:|---:|---:|---:|---:|---:|
| Costo 3:1 | 0.30 | 2418 | 83.73 % | 97.17 % | 1993 | 425 | 58 |
| **Costo 1:1** | **0.85** | **1967** | **68.11 %** | **88.15 %** | **1808** | **159** | **243** |
| Tope 20 % | 0.996108932288145 | 577 | 19.98 % | 27.94 % | 573 | 4 | 1478 |

Con costo 1:1, `FP + FN` baja de 483 a 402 frente a 3:1. La exactitud de esta
comparación describe las mismas filas usadas para escoger el umbral; no es una
evaluación independiente de generalización.

### Semanas sin brote vigente

Se aplica el mismo umbral seleccionado en la validación completa. No se elige un
segundo umbral para este subconjunto.

| Criterio | Alertas | Tasa de alertas | Recall | Precisión | VP | FP | FN | FP/VP |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Costo 3:1 | 686 | 61.03 % | 91.99 % | 45.19 % | 310 | 376 | 27 | 1.21 |
| **Costo 1:1** | **241** | **21.44 %** | **37.98 %** | **53.11 %** | **128** | **113** | **209** | **0.88** |
| Tope 20 % global | 0 | 0.00 % | 0.00 % | No definida | 0 | 0 | 337 | No definido |

El presupuesto global del 20 % concentra todas sus alertas en semanas con brote
vigente. Se descarta porque elimina la anticipación en este subconjunto. No se
modifica el presupuesto usando resultados de 2024.

El costo 1:1 reduce las falsas alarmas sin brote actual de 376 a 113, pero también
las semanas positivas detectadas de 310 a 128. Deja sin detectar 209 de las 337
positivas (62.02 %). No permite afirmar que el problema de alerta temprana esté
resuelto. La tasa global de alertas sigue siendo 68.11 %, y no debe confundirse con
la tasa del subconjunto sin brote vigente. Tampoco debe compararse directamente
con la prevalencia de brote actual: la etiqueta del modelo mira cuatro semanas
futuras. Los puntajes de la logística ponderada no se presentan como probabilidades
calibradas.

## Aplicación y reproducción

`paso_04_modelo.py` mantiene el criterio `costo` y cambia el valor predeterminado
de `--costo-fn` de 3 a 1. El umbral se selecciona de nuevo sobre la misma validación
al entrenar; no se introduce una constante 0.85 en el código. La versión final
refita el modelo con los datos permitidos hasta 2023 y conserva el umbral de
validación, como ya hacía el paso 4. Un refit o un cambio temporal puede cambiar la
distribución de puntajes; ninguna tasa de validación se garantiza en datos futuros.

El modelo y la climatología se guardan con los metadatos del criterio y el umbral.
El paso 5 ya lee ese umbral: no necesita un cambio para aplicarlo. Un modelo
anterior guardado con 3:1 no se actualiza cambiando el código; hay que regenerar el
paso 4 antes de evaluar la carga nueva.

```bash
docker compose exec -T -e PYTHONPATH=/home/jovyan/work/nube -w /home/jovyan/work pyspark-notebook spark-submit --master "local[2]" --driver-memory 3g nube/pipeline/paso_04_modelo.py --base /home/jovyan/work/data/_nube --hasta 2023 --anios-validacion 3 --criterio-umbral costo --costo-fn 1 --exportar-validacion

docker compose exec -T -e PYTHONPATH=/home/jovyan/work/nube -w /home/jovyan/work pyspark-notebook spark-submit --master "local[2]" --driver-memory 3g tests/test_modelo_nube.py
```

La exportación opcional `modelos/<version>/validacion_predicciones/part-*.txt`
contiene etiquetas y puntajes departamentales de 2021–2023 para auditar alternativas
sin volver a ajustar modelos. Permanece local, fuera de Git. Para regenerar los
CSV y el protocolo:

```bash
uv run python scripts/evaluar_umbral_validacion.py data/_nube/modelos/<version>/validacion_predicciones/part-<archivo>.txt
```

Se versionan [comparacion.csv](evidencias/umbral_validacion/comparacion.csv) y
[protocolo.json](evidencias/umbral_validacion/protocolo.json), con todos los
candidatos, los dos subconjuntos y el SHA-256 de las predicciones usadas.
Los logs de esta ejecución quedan en `output/umbral_validacion/`.
Se ejecutaron correctamente las nueve pruebas Spark del modelo (incluidas las de
fuga temporal), las tres pruebas del comparador y las 32 de infraestructura. La
versión local final es `v20261124T200610Z`, con costo 1:1 y umbral 0.85 verificados
en sus metadatos. Los archivos del modelo permanecen fuera de Git.

Para reproducir el original se puede pasar `--costo-fn 3`. La alternativa descartada
está disponible con `--criterio-umbral tope_alertas --max-tasa-alertas 0.20`.
No usar estas opciones para reajustar después de observar 2024.

## Párrafo para la bitácora

Se compararon costos de error 3:1 y 1:1 y un tope del 20 % de alertas usando
exclusivamente la validación 2021–2023, con entrenamiento hasta 2020 y el embargo
temporal del paso 4, sin cambiar el modelo ni la definición de brote. Se eligió
minimizar FP + FN con costo simétrico 1:1, supuesto académico ante la ausencia de
costos institucionales, obteniendo un umbral de 0.85. En semanas sin brote vigente,
la tasa de alertas bajó de 61.03 % a 21.44 % y las falsas alarmas de 376 a 113;
como consecuencia, el recall disminuyó de 91.99 % a 37.98 %, con 128 semanas
positivas detectadas y 209 omitidas. El tope global del 20 % se descartó porque no
detectó ninguna positiva sin brote vigente. Se deja fijado el criterio 1:1 antes de
evaluar 2024, sin reajustarlo posteriormente, y se reconoce que reducir las falsas
alarmas no demuestra por sí solo utilidad operativa ni resuelve la anticipación.