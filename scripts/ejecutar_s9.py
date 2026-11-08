"""Ejecutor S9. Dentro del contenedor: python scripts/ejecutar_s9.py."""
import os
import sys
import json
import csv
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
SPARK=Path(os.environ.get('SPARK_HOME','/usr/local/spark'))
sys.path[:0]=[str(SPARK/'python'),*map(str,(SPARK/'python/lib').glob('py4j-*-src.zip'))]

def ejecutar(spark, cortes=((2010,2014),(2014,2017),(2017,2019))):
    import pandas as pd
    import numpy as np
    from sklearn.metrics import roc_auc_score,average_precision_score,confusion_matrix
    from pyspark.sql import functions as F
    from pyspark.ml import PipelineModel
    from pyspark.ml.functions import vector_to_array
    from ingenieria_variables import preparar,crear_pipeline,diccionario,EXPERIMENTOS
    gold=spark.read.parquet(str(ROOT/'data/gold/panel_dengue_clima'))
    out=ROOT/'docs/evidencias/s9';out.mkdir(parents=True,exist_ok=True)
    pd.DataFrame(diccionario()).to_csv(out/'diccionario_variables.csv',index=False)
    filas=[]
    for fin_train,fin_valid in cortes:
        nombre=f'{fin_train}_{fin_train+1}_{fin_valid}'
        destino=ROOT/'output/s9'/nombre;destino.mkdir(parents=True,exist_ok=True)
        train,valid,protocolo=preparar(gold,fin_train,fin_valid)
        train=train.cache();train_cache=train;valid=valid.cache()
        try:
            n_train=train.count();n_valid=valid.count()
            if not n_train or not n_valid:raise ValueError('Partición vacía')
            p=train.agg(F.avg('label')).first()[0]
            if not 0<p<1:raise ValueError('Entrenamiento requiere ambas clases')
            train=train.withColumn('peso',F.when(F.col('label')==1,0.5/p).otherwise(0.5/(1-p)))
            protocolo.update(n_train=n_train,n_validacion=n_valid,prevalencia_train=p,
                spark=spark.version,master=spark.sparkContext.master,driver_memoria=spark.conf.get('spark.driver.memory'),
                supuestos='Casos y clima de t disponibles al emitir señal; retrasos de notificación no modelados.')
            for experimento in EXPERIMENTOS:
                print(f'{nombre}: {experimento}',flush=True)
                modelo=crear_pipeline(experimento).fit(train)
                pred=modelo.transform(valid).withColumn('prob',vector_to_array('probability')[1])
                dimensiones=pred.select(F.size(vector_to_array('features')).alias('n')).distinct().collect()
                assert len(dimensiones)==1
                ruta=destino/experimento
                modelo.write().overwrite().save(str(ruta))
                # Verificar persistencia sobre exactamente las mismas filas.
                muestra=valid.orderBy('cod_dep','ano','semana').limit(12).cache()
                extraer=lambda m: m.transform(muestra).orderBy('cod_dep','ano','semana').select(vector_to_array('probability')[1]).toPandas().iloc[:,0].to_numpy()
                assert np.allclose(extraer(modelo),extraer(PipelineModel.load(str(ruta))),rtol=0,atol=1e-12)
                muestra.unpersist()
                pdf=pred.select('cod_dep','ano','semana','label','brote_actual','prob').toPandas()
                assert len(pdf)==n_valid and not pdf.duplicated(['cod_dep','ano','semana']).any()
                assert pdf.prob.notna().all() and pdf.prob.between(0,1).all()
                pdf.to_csv(destino/f'pred_{experimento}.csv',index=False)
                for subset,g in [('completo',pdf),('sin brote actual',pdf[pdf.brote_actual==0])]:
                    y=g.label.astype(int)
                    tn,fp,fn,tp=confusion_matrix(y,g.prob>=0.5,labels=[0,1]).ravel()
                    filas.append({'corte':nombre,'experimento':experimento,'subconjunto':subset,
                        'n':len(g),'positivos':int(y.sum()),'prevalencia':y.mean(),'dimension_features':dimensiones[0].n,
                        'auc_roc':roc_auc_score(y,g.prob) if y.nunique()==2 else None,
                        'average_precision':average_precision_score(y,g.prob) if y.nunique()==2 else None,
                        'precision':tp/(tp+fp) if tp+fp else None,'recall':tp/(tp+fn) if tp+fn else None,
                        'vp':int(tp),'fp':int(fp),'fn':int(fn),'vn':int(tn)})
            (out/f'protocolo_{nombre}.json').write_text(json.dumps(protocolo,ensure_ascii=False,indent=2),encoding='utf-8')
            pd.DataFrame(filas).to_csv(out/'resultados.csv',index=False)
        finally:
            train_cache.unpersist();valid.unpersist()
    return pd.DataFrame(filas)

if __name__=='__main__':
    from pyspark.sql import SparkSession
    spark=(SparkSession.builder.master('local[2]').appName('S9IngenieriaVariables')
           .config('spark.driver.memory','2g').config('spark.sql.shuffle.partitions',8).getOrCreate())
    try:print(ejecutar(spark).round(4).to_string(index=False),flush=True)
    finally:spark.stop()
