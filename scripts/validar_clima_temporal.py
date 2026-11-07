"""Validación temporal exploratoria: no utiliza datos posteriores a 2019.
Ejecutar dentro del contenedor con python scripts/validar_clima_temporal.py.
"""
import os
from pathlib import Path
import nbformat
from nbclient import NotebookClient

ROOT = Path(__file__).resolve().parents[1]
SPARK = Path(os.environ.get('SPARK_HOME', '/usr/local/spark'))
os.environ['PYTHONPATH'] = os.pathsep.join([str(SPARK/'python'), *map(str,(SPARK/'python/lib').glob('py4j-*-src.zip')), os.environ.get('PYTHONPATH','')])
base = nbformat.read(ROOT/'notebooks/modeloBase.ipynb', as_version=4)
# Reutilizar lectura, definición del objetivo y variables; no ejecutar entrenamiento/test originales.
cells = [c for c in base.cells[:4] if c.cell_type == 'code']
for c in cells:
    c.outputs = []; c.execution_count = None
    c.source = c.source.replace('.appName("ModeloBaseDengue")', '.master("local[2]").appName("ValidacionTemporalClima")')
    c.source = c.source.replace('ANIOS_TRAIN = (2005, 2019)', 'ANIOS_TRAIN = (2005, 2014)').replace('ANIOS_TEST = (2021, 2024)', 'ANIOS_TEST = (2015, 2019)')
    # Cortar antes del cálculo de etiquetas: tampoco usar desenlaces de 2020 para 2019.
    c.source = c.source.replace('panel = spark.read.parquet(ruta_salida("gold", "panel_dengue_clima"))', 'panel = spark.read.parquet(ruta_salida("gold", "panel_dengue_clima")).filter(F.col("ano") <= 2019)')
experimento = '''
from pyspark.ml.classification import RandomForestClassifier
from sklearn.metrics import roc_auc_score, average_precision_score, confusion_matrix
import json
import time

# Niveles absolutos además de anomalías, con las mismas ventanas causales existentes.
ABSOLUTAS = []
for variable in ["temp_media", "hum_media", "precip_total"]:
    for nombre, (desde, hasta) in VENTANAS.items():
        columna = f"{variable}_{nombre}"
        panel = panel.withColumn(columna, F.avg(variable).over(w.rowsBetween(desde, hasta)))
        ABSOLUTAS.append(columna)
variables_todas = VARS_CASOS + VARS_CLIMA + ABSOLUTAS
comparables = panel.filter(F.col("label").isNotNull()).dropna(subset=variables_todas)
# Embargo: las cuatro últimas semanas de train no pueden usar desenlaces de validación.
train = comparables.filter(F.col("ano").between(2005, 2014))
ultimas = train.select("ano", "semana").distinct().orderBy(F.desc("ano"), F.desc("semana")).limit(HORIZONTE)
train = train.join(ultimas, ["ano", "semana"], "left_anti")
valid = comparables.filter(F.col("ano").between(2015, 2019)).cache()
p = train.agg(F.avg("label")).first()[0]
train = train.withColumn("peso", F.when(F.col("label") == 1, 0.5/p).otherwise(0.5/(1-p))).cache()
assert valid.agg(F.max("ano")).first()[0] <= 2019
out = Path(ruta_salida("metricas", "validacion_clima")); out.mkdir(parents=True, exist_ok=True)
protocolo = {"train": [2005,2014], "validacion": [2015,2019], "embargo_semanas_train": HORIZONTE,
    "climatologia_hasta": 2014, "ultimo_anio_datos": 2019, "umbral_fijo": 0.5,
    "rf": {"numTrees":50,"maxDepth":5,"seed":42}, "spark":spark.version,
    "master":spark.sparkContext.master,"n_train":train.count(),"n_validacion":valid.count(),
    "prevalencia_train":p,"prevalencia_validacion":valid.agg(F.avg("label")).first()[0],
    "nota":"Experimento exploratorio posterior a inspeccionar test; no es confirmación independiente."}
(out/"protocolo.json").write_text(json.dumps(protocolo,indent=2))
filas = []
for algoritmo in ["logistica", "random_forest"]:
    for conjunto, variables in {"casos":VARS_CASOS, "casos_anomalias":VARS_CASOS+VARS_CLIMA,
                               "casos_anomalias_absolutas":variables_todas}.items():
        print(f"Entrenando {algoritmo} / {conjunto}",flush=True)
        estimador = (LogisticRegression(featuresCol="features",labelCol="label",weightCol="peso",maxIter=100)
            if algoritmo == "logistica" else RandomForestClassifier(featuresCol="features",labelCol="label",weightCol="peso",numTrees=50,maxDepth=5,seed=42))
        etapas = [VectorAssembler(inputCols=variables,outputCol="x"), StandardScaler(inputCol="x",outputCol="features"), estimador]
        inicio = time.perf_counter()
        modelo = Pipeline(stages=etapas).fit(train)
        pred = modelo.transform(valid).select("cod_dep","ano","semana","label","brote_actual",vector_to_array("probability")[1].alias("prob")).toPandas()
        pred.to_csv(out/f"pred_{algoritmo}_{conjunto}.csv",index=False)
        for subconjunto, df in {"completo":pred,"sin brote actual":pred[pred.brote_actual==0]}.items():
            y = df.label.astype(int)
            tn,fp,fn,tp = confusion_matrix(y,df.prob>=0.5,labels=[0,1]).ravel()
            filas.append({"algoritmo":algoritmo,"variables":conjunto,"subconjunto":subconjunto,
                "n":len(df),"prevalencia":y.mean(),"auc_roc":roc_auc_score(y,df.prob),
                "average_precision":average_precision_score(y,df.prob),
                "precision":tp/(tp+fp) if tp+fp else None,"recall":tp/(tp+fn),
                "vp":tp,"fp":fp,"fn":fn,"vn":tn})
        pd.DataFrame(filas).to_csv(out/"resultados.csv",index=False)
        print(f"Fin {time.perf_counter()-inicio:.1f}s",flush=True)
display(pd.DataFrame(filas))
spark.stop()
'''
nb = nbformat.v4.new_notebook(cells=cells+[nbformat.v4.new_code_cell(experimento)])
client = NotebookClient(nb, timeout=900, kernel_name='python3', resources={'metadata':{'path':str(ROOT)}})
client.on_cell_start = lambda cell,cell_index,**kw: print(f'Celda {cell_index}',flush=True)
try:
    client.execute()
    print('COMPLETADO',flush=True)
finally:
    (ROOT/'output').mkdir(exist_ok=True)
    nbformat.write(nb,ROOT/'output/validacion_clima_temporal.ipynb')
