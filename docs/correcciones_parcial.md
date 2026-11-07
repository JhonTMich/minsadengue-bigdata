# Correcciones del parcial: evidencia medida

Fecha local: 6 de noviembre de 2026. Rama: `feature/correcciones-parcial`.

## Optimización: doce corridas

Entorno: Docker/WSL2, Spark 3.5.0, Python 3.11.6, `local[2]`, driver configurado en 4 GB.
Docker informa 12 CPU y unos 7.3 GiB de memoria disponible. Imagen usada:
`sha256:58377aaa152b741e244f201679f96d909a024ea337088cc276b0ee32ab3f076f`.

Cuatro configuraciones, tres repeticiones cada una, kernel nuevo por corrida y orden
aleatorio con semilla 42. Se reutiliza la descarga climática existente. Se varía la caché
de Bronze; la caché del clima se mantiene igual. La caché del SO no se vacía ni controla.

| Caché Bronze | Shuffle configurado | Mediana (s) | Mínimo (s) | Máximo (s) |
|---|---:|---:|---:|---:|
| No | 8 | 26.52 | 26.16 | 32.80 |
| No | 200 | 29.92 | 28.45 | 36.01 |
| Sí | 8 | 31.06 | 29.54 | 39.69 |
| Sí | 200 | 32.39 | 26.58 | 33.36 |

La mediana sin caché y con ocho particiones fue 11.4 % menor que sin caché y con 200.
Activar la caché aumentó la mediana 17.1 % con ocho particiones y 8.3 % con 200.
No se sostiene una mejora de la caché de Bronze en este experimento. Se deja desactivada
por defecto y se conservan ocho particiones. Son resultados locales: los rangos se solapan,
solo hay tres repeticiones y no se estima significancia ni una ventaja universal.
El valor configurado de shuffle no implica que todas las etapas ejecuten ese número de tareas.

`TOTAL` suma las siete etapas instrumentadas; excluye arranque de Spark, descarga climática,
parte de las validaciones y escalabilidad. No debe llamarse tiempo completo de extremo a extremo.
La generación de réplicas no participa en esta comparación. Las mediciones agregadas,
etapas y protocolo están en [evidencias/optimizacion](evidencias/optimizacion/).
Cada notebook ejecutado queda localmente en `output/optimizacion/`.

## Aporte incremental del clima

Se mantuvieron las variables, ponderaciones y partición existentes: entrenamiento
2005–2019 y prueba 2021–2024. Las tres logísticas se comparan sobre las mismas filas,
sin cambiar umbrales ni seleccionar modelos usando estas métricas de prueba.
No se ejecutó Random Forest en esta evaluación.

| Conjunto | Modelo | AUC-ROC | Precisión a 0.5 | Recall a 0.5 |
|---|---|---:|---:|---:|
| Completo | Casos | 0.9278 | 0.7963 | 0.9555 |
| Completo | Casos + clima | 0.9221 | 0.7822 | 0.9578 |
| Completo | Clima | 0.5769 | 0.6886 | 0.5269 |
| Sin brote actual | Casos | 0.7103 | 0.3708 | 0.7416 |
| Sin brote actual | Casos + clima | 0.6889 | 0.3524 | 0.7550 |
| Sin brote actual | Clima | 0.5182 | 0.2608 | 0.4566 |

La prueba completa tiene 3 876 filas y 67.23 % de etiquetas positivas. El subconjunto sin
brote vigente tiene 1 647 filas y 449 etiquetas positivas (27.26 %). Estas son semanas
departamentales, no 449 episodios independientes: las ventanas futuras pueden solaparse.

Sin brote actual, agregar clima detecta 339 semanas positivas frente a 333 con casos,
pero produce 623 falsas alarmas frente a 565: seis detecciones adicionales y 58 falsas
alarmas adicionales con umbral 0.5. No demuestra una mejora operativa; no se compara a
igual recall ni se han establecido costos de error con la organización.

### Variación temporal sin brote actual

| Año | AUC casos | AUC casos + clima | AUC clima |
|---|---:|---:|---:|
| 2021 | 0.7072 | 0.7130 | 0.7008 |
| 2022 | 0.8288 | 0.8317 | 0.7010 |
| 2023 | 0.6217 | 0.6019 | 0.2821 |
| 2024 | 0.7121 | 0.6877 | 0.5350 |

El aporte cambia con el año y empeora en 2023–2024. Esto justifica investigar estabilidad
temporal; no demuestra una causa concreta ni significa que deba ajustarse contra esos años.
El desglose por departamento está en [comparacion_clima_desglose.csv](evidencias/clima/comparacion_clima_desglose.csv).
`average_precision` del desglose usa sklearn y no es exactamente el área PR interpolada de Spark.

### Datos y referencias

- Las 19 descargas tienen 9 132 días por variable, sin valores de relleno `-999`.
- Hay 17 series exactamente distintas: Amazonas/Cajamarca y Lima/Callao comparten series.
  La igualdad se verificó sobre fechas y valores de las tres variables; no se deduce de
  las coordenadas devueltas por la API. Ver [auditoría](evidencias/clima/auditoria_fuente.json).
- Ninguna fila etiquetada de train o test se pierde por predictores nulos. Las 76 filas
  de prueba sin etiqueta corresponden al horizonte futuro incompleto al final de la serie.
- La mayoría en entrenamiento es la clase negativa. Se corrigió la referencia para
  calcularla en train y se conservó «siempre brote» como regla constante separada.
- Los casos sin modelo se evalúan como ranking: no se presentan métricas de clasificación
  aplicando arbitrariamente 0.5 a `log_casos`.
- Referencias y cobertura: [lineas_base.csv](evidencias/clima/lineas_base.csv) y
  [comparacion_clima_cobertura.csv](evidencias/clima/comparacion_clima_cobertura.csv).
- Evidencia ejecutada local: `output/modeloBase_aporte_clima.ipynb`. Las probabilidades
  pareadas quedan en `data/metricas/comparacion_clima_predicciones.csv`.

## Conclusión y siguiente experimento

No se demuestra aporte incremental del clima con esta representación y regresión logística.
No equivale a demostrar ausencia de relación entre clima y dengue. La representación usa
un punto por departamento y solo anomalías climáticas, sin niveles absolutos. Evaluar otra
representación o relaciones no lineales es una hipótesis futura, no una explicación comprobada.

Antes de cambiar variables o algoritmos, separar entrenamiento y validación temporal dentro
de 2005–2019. Comparar casos solos frente a casos más clima con el mismo algoritmo y filas;
seleccionar hiperparámetros y umbral únicamente en validación. El test 2021–2024 ya ha sido
inspeccionado y no debe seguir usándose para optimización; una confirmación independiente
requeriría datos no utilizados. Mantener la definición del objetivo durante la comparación.

La escala ×100 sigue desactivada. La prueba local con réplicas, compresión y caché del SO
no acredita escalabilidad multinodo. Permanecen pendientes las escalas graduales adicionales,
la actualización del informe PDF, costos/arquitectura y los respaldos del equipo/Anexo A.
