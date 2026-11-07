# Predicción de brotes de dengue en Perú con datos epidemiológicos y climáticos

Proyecto académico de Big Data y Analítica de Datos, Ingeniería de Sistemas e Inteligencia Artificial, Universidad Privada Antenor Orrego, periodo 2026-20. Grupo 13. Docente: Ing. Armando Javier Caballero Alvarado.

**Resultado actual:** el pipeline funciona y la logística obtiene AUC-ROC 0.922 con casos y clima, pero el clima no mejora al modelo de casos solos (0.928). Al evaluar únicamente semanas sin brote vigente, el AUC baja a 0.689 con clima frente a 0.710 con casos solos. El objetivo predictivo de anticipación todavía requiere mejoras; no se presenta el AUC global como prueba suficiente de alerta temprana.

## Equipo y alcance

| Integrante | Responsabilidad |
|---|---|
| Arenas Arriaga, Johan Michael | Coordinación, análisis y modelado |
| Campos Acevedo, Gianfranco | Ingeniería de datos, calidad y documentación |

Ruta A: ejecución on-premise local con Docker y Spark. Se procesa el registro nacional 2000–2024 y se modelan 19 departamentos con al menos 1 000 registros en el periodo, que concentran aproximadamente el 99.93 % de casos. El informe declara un acuerdo de alcance con el docente en Semana 2; su respaldo documental sigue pendiente de incorporar. Los roles dobles y la cantidad de commits no sustituyen evidencia de planificación, contribuciones y decisiones.

Es un prototipo académico, no un servicio oficial del MINSA ni una herramienta validada para decisiones sanitarias.

## Problema y objetivo

Estimar por departamento y semana epidemiológica si habrá al menos una semana de brote en las cuatro siguientes. La referencia de negocio del informe es la detección cuando los casos superan el canal endémico; la supuesta anticipación actual de cero semanas necesita respaldo y no constituye una medición institucional realizada por este proyecto.

La meta inicial fue AUC-ROC > 0.80. Se alcanza en prueba completa, pero no en semanas sin brote vigente, que se acercan más a la pregunta de anticipación. La evaluación debe contrastarse con referencias simples, prevalencia, falsas alarmas y estabilidad temporal; no solo con superar 0.80.

## Fuentes y granularidad

| Fuente | Periodo | Datos originales |
|---|---|---|
| Vigilancia epidemiológica de dengue, CDC Perú / MINSA | 2000–2024 | 1 029 421 registros, 14 columnas, CSV separado por `;`, aproximadamente 103.5 MB |
| NASA POWER, API diaria | 2000–2024 | 19 puntos, 9 132 días por punto; temperatura T2M, humedad RH2M y precipitación PRECTOTCORR |

Dengue: [Plataforma Nacional de Datos Abiertos](https://www.datosabiertos.gob.pe/dataset/vigilancia-epidemiol%C3%B3gica-de-dengue). Clima: `https://power.larc.nasa.gov/api/temporal/daily/point`.

La auditoría de las respuestas climáticas encontró cero valores de relleno `-999`, pero solo 17 series distintas: Amazonas/Cajamarca y Lima/Callao tienen series exactamente iguales. Un punto por departamento no representa todos sus focos; la resolución espacial limita la señal. No se demuestra que estas limitaciones expliquen por sí solas el resultado del modelo.

## Arquitectura implementada

```mermaid
flowchart LR
    A[CSV dengue MINSA] --> R[Raw local]
    B[API NASA POWER] --> R
    R --> BR[Bronze: texto y linaje]
    BR --> SD[Silver dengue: tipos y calidad]
    BR --> SC[Silver clima: agregado semanal]
    SD --> G[Gold: departamento por semana]
    SC --> G
    G --> M[Variables, exploración y modelos]
```

| Capa | Ruta bajo `data/` | Contrato |
|---|---|---|
| Raw | `raw/` | CSV dengue y respuestas originales NASA; no se modifican en el pipeline |
| Bronze | `bronze/` | Texto sin inferencia de tipos y metadatos `_archivo_origen`, `_fecha_ingesta`; Parquet |
| Silver | `silver/` | Dengue tipado, particionado por año, rechazados con motivo; clima semanal; Parquet |
| Gold | `gold/panel_dengue_clima/` | Una fila por departamento/año/semana; conteo cero en semanas sin casos; Parquet |

Gold contiene exactamente `cod_dep`, `departamento`, `ano`, `semana`, `casos`, `temp_media`, `hum_media`, `precip_total`. Su clave es departamento/año/semana. En la ejecución validada hay **24 776 filas**. El notebook `modeloBase` depende de este esquema, no de registros individuales.

El almacenamiento real es disco local montado en Docker: no se implementa HDFS, catálogo gestionado ni clúster multinodo. Spark se ejecuta en modo local. La reconstrucción desde Raw permite recuperar datos, pero no acredita alta disponibilidad. Migrar a S3/HDFS requiere adaptar rutas, credenciales, conectores y accesos; no basta con afirmar sustitución sin validación. Parquet y el esquema Gold reducen el acoplamiento entre componentes.

### Calidad y privacidad

Se normalizan textos y ubigeos, tipan años y semanas, validan diagnóstico, sexo, edad y calendario; se separan rechazados con motivo. La semana 53 solo se admite en años epidemiológicos que la tienen. Las filas idénticas se conservan porque, sin identificador de caso, pueden representar pacientes distintos.

En clima, `-999` y valores fuera de rangos físicos se convierten en nulos; se agregan semanas con siete días. Ambos conjuntos usan semanas de domingo a sábado, con semana 1 que contiene el 4 de enero. Gold verifica número de filas, conservación de casos y ausencia de clima nulo en los controles implementados.

Los datos individuales permanecen fuera de Git; Silver descarta localidad, localcod y diresa, pero conserva otros atributos individuales. Gold solo contiene agregados. No se afirma anonimización absoluta ni se ha realizado una auditoría formal de reidentificación. El informe cita la Ley N.º 29733; la conformidad legal no queda demostrada por descartar columnas. No hay controles productivos de cifrado/autorización acreditados por este repositorio.

## Instalación y reproducción

Requisitos: Docker Desktop/Engine con Compose v2, aproximadamente 8 GB de RAM disponibles para el entorno, espacio libre para originales y capas, e internet en la primera descarga climática. El crecimiento de réplicas consume espacio adicional.

```bash
git clone https://github.com/JhonTMich/geresa-bigdata.git
cd geresa-bigdata
docker compose up -d
docker compose logs pyspark-notebook
```

Jupyter: `http://localhost:8888`. Spark UI: `http://localhost:4040` mientras exista una sesión activa. La imagen declarada usa `latest`: puede cambiar; las mediciones registradas se hicieron con Spark 3.5.0 y Python 3.11.6.

1. Guardar el CSV como `data/raw/datos_abiertos_vigilancia_dengue_2000_2024.csv`.
2. Ejecutar `notebooks/cargaDatos.ipynb` completo. NASA se descarga y se reutilizan archivos existentes.
3. Ejecutar `notebooks/modeloBase.ipynb` completo después de obtener Gold.

El modelo requiere Gold; no existe actualmente lectura alternativa de una muestra CSV implementada. Las capas se sobrescriben: no ejecutar mediciones del pipeline en paralelo ni mientras se consulta la misma salida.

### Ejecuciones automáticas y evidencias

```bash
docker compose exec -T pyspark-notebook python /home/jovyan/work/scripts/medir_optimizacion.py
docker compose exec -T pyspark-notebook python /home/jovyan/work/scripts/resumir_optimizacion.py
docker compose exec -T pyspark-notebook python /home/jovyan/work/scripts/evaluar_aporte_clima.py
docker compose exec -T pyspark-notebook python /home/jovyan/work/scripts/resumir_aporte_clima.py
docker compose exec -T pyspark-notebook python /home/jovyan/work/scripts/validar_clima_temporal.py --train-end 2014 --valid-end 2019
```

Los notebooks ejecutados quedan en `output/`, ignorado por Git. Métricas y probabilidades agregadas quedan en `data/metricas/`. Los CSV de resultados y protocolos sin datos personales se conservan en `docs/evidencias/` para versionado. No hace falta consultar otro documento para interpretar los resultados principales incluidos abajo.

## Rendimiento medido: caché y particiones

Se ejecutaron cuatro configuraciones, tres repeticiones cada una, con kernel nuevo, orden aleatorio de semilla 42, Spark `local[2]` y driver configurado en 4 GB. Docker informó 12 CPU y aproximadamente 7.3 GiB de RAM. Se varió la caché de Bronze; la caché del clima se mantuvo igual. La caché del SO no se controló.

| Caché Bronze | Shuffle configurado | Mediana (s) | Mínimo (s) | Máximo (s) |
|---|---:|---:|---:|---:|
| No | 8 | 26.52 | 26.16 | 32.80 |
| No | 200 | 29.92 | 28.45 | 36.01 |
| Sí | 8 | 31.06 | 29.54 | 39.69 |
| Sí | 200 | 32.39 | 26.58 | 33.36 |

Ocho particiones sin caché produjo una mediana 11.4 % menor que 200 sin caché. La caché aumentó la mediana 17.1 % con ocho particiones y 8.3 % con 200: no se sostiene su beneficio aquí. El valor predeterminado pasó a `USAR_CACHE=False`, con `SHUFFLE_PARTITIONS=8`. Los rangos se solapan; tres corridas no prueban una ventaja universal ni significancia estadística.

`TOTAL` suma siete etapas instrumentadas, no el tiempo completo: excluye arranque, descarga climática y parte de los controles. Las particiones configuradas no equivalen necesariamente al número de tareas efectivo de todas las etapas.

## Escalabilidad: alcance y límites

`EJECUTAR_ESCALABILIDAD=False` y `FACTORES=[1]` son los valores iniciales. El interruptor ahora controla realmente el bucle. La sección 3.7 de la guía exige réplicas ×10 y ×100, ejecutar el pipeline completo en cada escala y reportar tiempos y consumo de recursos. Ambas escalas siguen pendientes con ese alcance. Las escalas ×2 y ×5 son diagnósticos intermedios, no sustituyen el requisito. Si una ejecución falla o se interrumpe por recursos, registrar factor, etapa, tiempo transcurrido, recursos y causa; no declararla completada ni dar el requisito por satisfecho sin validación del docente. El ordenamiento aleatorio está desactivado porque añade carga de shuffle y no elimina los duplicados.

La medición actual cubre transformación Silver y construcción/conteo Gold; no mide ingesta completa ni escritura Gold en la prueba replicada. Debe ampliarse antes de usarla como evidencia de cumplimiento de la sección 3.7. La validación ×1 procesó 1 029 421 registros: **5.5 s** para transformación Silver y construcción/conteo Gold, Bronze físico 4.9 MB, Silver 2.9 MB, shuffle reportado 0.1 MB, spill de memoria/disco 0 y pico de ejecución por etapa 271.6 MB. Este pico no representa RAM total del equipo o proceso. Una métrica ausente se escribe nula, no cero.

Generar y contar las réplicas ocurre antes de medir; la entrada está recién escrita/leída y la caché del SO favorece el I/O. Copias idénticas se comprimen mucho. Por tanto, los tiempos antiguos de ×100 no acreditan escalabilidad multinodo ni necesidad de clúster. El conjunto original cabe en una máquina; la necesidad de procesamiento distribuido y el punto de quiebre todavía no están demostrados.

## Modelo, etiquetas y referencias

Brote: casos superiores al Q3 de la misma semana en los cinco años previos, con mínimo de cinco casos. La semana 53 usa como referencia histórica la 52. Etiqueta positiva: al menos una semana de brote en las siguientes cuatro; sin horizonte completo, no se asigna etiqueta.

Entrenamiento original 2005–2019; prueba 2021–2024. El año 2020 no se incluye como fila de entrenamiento/prueba, aunque el protocolo original no garantiza excluirlo de todas las ventanas históricas o desenlaces próximos a los límites. Las nuevas validaciones aplican embargo explícito. Se usan logaritmo de casos actuales, rezago de una semana, brote actual y anomalías de temperatura, humedad y precipitación en ventanas 0–3, 4–7 y 8–11 semanas. Las logísticas usan escalado y ponderaciones de clase ajustadas en entrenamiento.

La prueba completa tiene 3 876 filas etiquetadas y prevalencia 67.23 %. Sin brote actual hay 1 647 filas y 449 positivas (27.26 %). Son semanas departamentales, no episodios independientes: las ventanas se solapan. Ninguna fila etiquetada se perdió por predictores nulos; las 76 filas finales sin etiqueta se excluyen por horizonte incompleto.

| Referencia/modelo, prueba completa | AUC-ROC | AUC-PR Spark | Precisión | Recall |
|---|---:|---:|---:|---:|
| Siempre brote | 0.500 | 0.672 | 0.672 | 1.000 |
| Persistencia | 0.886 | 0.942 | 0.968 | 0.828 |
| Logística: casos | 0.928 | 0.964 | 0.796 | 0.955 |
| Logística: casos + clima | 0.922 | 0.962 | 0.782 | 0.958 |
| Logística: clima | 0.577 | 0.745 | 0.689 | 0.527 |

La mayoría en entrenamiento es negativa: su regla constante acierta 32.8 % en prueba. «Siempre brote» es una referencia distinta, que acierta 67.2 %. La persistencia acierta 86.6 %, frente a 79.2 % de casos + clima al umbral 0.5. La prevalencia afecta especialmente precisión, exactitud y referencia PR; no explica automáticamente el AUC-ROC.

### Evaluación de semanas sin brote vigente

| Logística | AUC-ROC | Average precision | Precisión a 0.5 | Recall a 0.5 | VP | FP |
|---|---:|---:|---:|---:|---:|---:|
| Casos | 0.7103 | 0.4435 | 0.3708 | 0.7416 | 333 | 565 |
| Casos + clima | 0.6889 | 0.4330 | 0.3524 | 0.7550 | 339 | 623 |
| Clima | 0.5182 | 0.2667 | 0.2608 | 0.4566 | 205 | 581 |

Agregar clima detecta seis semanas positivas más, pero genera 58 falsas alarmas adicionales. No se demuestra beneficio operativo. La persistencia siempre predice cero en este subconjunto; superarla no es suficiente. El ranking de casos actuales sin modelo ya alcanza AUC 0.704. Para ese ranking no se aplican métricas de clasificación con umbral arbitrario 0.5.

`average_precision` usa sklearn y no coincide exactamente con el área PR interpolada de Spark. No deben mezclarse ambos cálculos en una misma comparación. Los umbrales siguen fijos en 0.5; no se comparó a igual recall ni se establecieron costos institucionales de error.

| Año sin brote vigente | AUC casos | AUC casos + clima |
|---|---:|---:|
| 2021 | 0.7072 | 0.7130 |
| 2022 | 0.8288 | 0.8317 |
| 2023 | 0.6217 | 0.6019 |
| 2024 | 0.7121 | 0.6877 |

La señal cambia con el tiempo. Estos resultados no demuestran que el clima carezca de relación con dengue: no aporta mejora consistente con esta representación, modelo y periodo.

## Validación exploratoria de otra representación climática

Se entrenó con 2005–2014 y validó con 2015–2019. Se recortaron datos posteriores a 2019 antes de construir etiquetas, se excluyeron cuatro semanas finales de entrenamiento y se calculó climatología solo hasta 2014. Se compararon seis modelos sobre las mismas 4 864 filas. Las variables absolutas son promedios de temperatura, humedad y precipitación semanal en las mismas tres ventanas causales.

| Algoritmo, semanas sin brote actual | Variables | AUC-ROC | Average precision |
|---|---|---:|---:|
| Logística | Casos | 0.7587 | 0.2644 |
| Logística | Casos + anomalías | 0.7650 | 0.2798 |
| Logística | Casos + anomalías + niveles absolutos | 0.7730 | 0.3207 |
| Random Forest | Casos | 0.7581 | 0.2619 |
| Random Forest | Casos + anomalías | 0.7568 | 0.2810 |
| Random Forest | Casos + anomalías + niveles absolutos | 0.7484 | 0.2344 |

RF usa 50 árboles, profundidad 5 y semilla 42; logística maxIter=100. No se buscaron hiperparámetros ni se calibró el umbral. Este primer corte sugiere investigar niveles absolutos para logística; cambiar a árboles no resolvió automáticamente el problema.

## Estabilidad en tres bloques temporales

Entrenamientos expansivos 2005–2010, 2005–2014 y 2005–2017; validaciones no solapadas 2011–2014, 2015–2017 y 2018–2019. Cada corte recalcula climatología solo con entrenamiento, aplica embargo de cuatro semanas al final de entrenamiento y excluye las cuatro semanas finales de validación sin desenlace completo. Se ejecutaron 18 modelos, sin búsqueda de hiperparámetros ni ajuste del umbral.

A continuación se comparan casos solos con casos más anomalías y niveles absolutos, únicamente en semanas sin brote vigente.

| Validación | Algoritmo | AUC casos | AUC con clima absoluto | AP casos | AP con clima absoluto |
|---|---|---:|---:|---:|---:|
| 2011–2014 | logistica | 0.8496 | 0.8416 | 0.4719 | 0.4446 |
| 2011–2014 | random_forest | 0.8472 | 0.8438 | 0.4406 | 0.4651 |
| 2015–2017 | logistica | 0.7082 | 0.7211 | 0.2678 | 0.3368 |
| 2015–2017 | random_forest | 0.7057 | 0.6885 | 0.2662 | 0.2282 |
| 2018–2019 | logistica | 0.7946 | 0.8165 | 0.2549 | 0.2498 |
| 2018–2019 | random_forest | 0.8005 | 0.7884 | 0.2482 | 0.2361 |

AP significa average precision. En logística, el clima absoluto mejora AUC en dos de tres bloques y AP en uno de tres; en 2018–2019 sube AUC pero baja AP. RF pierde AUC en los tres bloques al agregar esta representación. Con anomalías sin niveles absolutos, los AUC logísticos son 0.8479, 0.7079 y 0.8097, respectivamente; tampoco hay una mejora uniforme.

Conclusión: la mejora del primer corte amplio no es estable entre periodos ni métricas. Se mantiene la línea base y no se declara resuelto el aporte climático. El siguiente paso es examinar heterogeneidad regional y calibración de alertas dentro de validación, con criterio de falsas alarmas acordado; una evaluación independiente seguirá siendo necesaria.

```bash
docker compose exec -T pyspark-notebook python /home/jovyan/work/scripts/validar_clima_temporal.py --train-end 2010 --valid-end 2014
docker compose exec -T pyspark-notebook python /home/jovyan/work/scripts/validar_clima_temporal.py --train-end 2014 --valid-end 2017
docker compose exec -T pyspark-notebook python /home/jovyan/work/scripts/validar_clima_temporal.py --train-end 2017 --valid-end 2019
```

Los protocolos, conteos, prevalencias y métricas completas quedan en `docs/evidencias/validacion_clima/<train-end>_<valid-start>_<valid-end>/`. Las probabilidades y notebooks se conservan localmente. Las ejecuciones terminaron correctamente; Spark emitió advertencias al limpiar directorios temporales durante el cierre, sin errores en las celdas ni pérdida de resultados.

## Limitaciones, conclusiones y trabajo pendiente

No se demuestra aún una alerta temprana suficientemente útil ni un aporte climático estable. El pipeline y las comparaciones reproducibles sí constituyen resultados verificables. Persisten resolución espacial limitada, subnotificación, cambios de vigilancia, ausencia de retrasos de notificación, departamentos excluidos y selección de departamentos basada en el periodo completo.

Los experimentos posteriores son exploratorios porque 2021–2024 ya se inspeccionó. Nuevas variables, modelos y umbrales deben elegirse en validación temporal, no optimizando ese test. Hará falta una evaluación independiente para confirmar generalización. Cambiar la definición de brote para mejorar la métrica invalidaría la comparación directa y requiere justificación separada.

Pendientes: estabilidad regional y confirmación independiente; umbral elegido con validación y presupuesto explícito de falsas alarmas; ejecución completa ×10 y ×100 con recursos, límites de ejecución y punto de quiebre; costos y sostenibilidad; actualización del informe; respaldo de aprobación del equipo y planificación real frente a ejecutada.

La declaración de IA debe registrar herramientas, secciones y verificaciones efectivamente realizadas, incluidas las correcciones asistidas por Codex de esta rama. El Anexo A y las firmas requieren revisión de los integrantes; no se han completado ni firmado en su nombre. No existe presupuesto monetario productivo medido: se utiliza infraestructura local y fuentes públicas, pero no se cuantificaron hardware, electricidad ni operación.

Se conserva el trabajo en `feature/correcciones-parcial`, con Conventional Commits separados por cambio. La integración a la rama de desarrollo y posteriormente a principal corresponde al flujo GitFlow del repositorio; estos cambios no se han fusionado ni publicado.
