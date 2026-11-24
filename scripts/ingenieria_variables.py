"""Variables y evaluación temporal de S9; solo datos disponibles hasta 2019."""
from datetime import date, timedelta
from math import pi
from pyspark.sql import functions as F, Window
from pyspark.sql.types import NumericType, StringType
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression

CONTRATO = {'cod_dep','departamento','ano','semana','casos','temp_media','hum_media','precip_total'}
CASOS = ['log_casos','log_casos_l1','brote_actual']
ESTACION = ['estacion_sin','estacion_cos']
VENTANAS = {'0_3':(-3,0),'4_7':(-7,-4),'8_11':(-11,-8)}
CLIMA = [f'{v}_{n}' for v in ['temp_media','hum_media','precip_total','anom_temp','anom_hum','anom_prec'] for n in VENTANAS]
EXPERIMENTOS = {'casos':CASOS, 'contexto':CASOS+ESTACION, 'contexto_clima':CASOS+ESTACION+CLIMA}

def inicio_epi(ano):
    d=date(ano,1,4)
    return d-timedelta(days=(d.weekday()+1)%7)

def calendario(spark, fin):
    return spark.createDataFrame([(a,s+1,inicio_epi(a)+timedelta(weeks=s))
        for a in range(2000,fin+1)
        for s in range((inicio_epi(a+1)-inicio_epi(a)).days//7)], ['ano','semana','fecha'])

def validar_gold(panel, fin):
    if CONTRATO-set(panel.columns):raise ValueError(f'Faltan columnas: {CONTRATO-set(panel.columns)}')
    tipos={f.name:f.dataType for f in panel.schema}
    if any(not isinstance(tipos[c],StringType) for c in ['cod_dep','departamento']):raise ValueError('cod_dep y departamento deben ser texto')
    for c in CONTRATO-{'cod_dep','departamento'}:
        if not isinstance(tipos[c],NumericType):raise ValueError(f'{c} debe ser numérico')
    p=panel.filter(F.col('ano')<=fin).select(*sorted(CONTRATO))
    if p.groupBy('cod_dep','ano','semana').count().filter('count != 1').limit(1).count():
        raise ValueError('Gold tiene claves duplicadas')
    malos=F.lit(False)
    for c in CONTRATO:malos=malos|F.col(c).isNull()
    for c in ['casos','temp_media','hum_media','precip_total']:malos=malos|F.isnan(c)|F.col(c).cast('double').isin(float('inf'),float('-inf'))
    malos=malos|(~F.col('cod_dep').rlike('^[0-9]{2}$'))|(~F.col('cod_dep').between('01','25'))
    malos=malos|(F.col('casos')<0)|(F.col('casos')!=F.floor('casos'))
    malos=malos|(~F.col('temp_media').between(-10,45))|(~F.col('hum_media').between(0,100))|(F.col('precip_total')<0)
    if p.filter(malos).limit(1).count():raise ValueError('Gold contiene nulos o valores inválidos')
    cal=calendario(panel.sparkSession,fin)
    if p.join(cal,['ano','semana'],'left_anti').limit(1).count():raise ValueError('Semana inexistente')
    esperadas=cal.count()
    if p.groupBy('cod_dep').count().filter(F.col('count')!=esperadas).limit(1).count():
        raise ValueError('Hay semanas faltantes; se requiere calendario continuo desde 2000')
    return p.join(cal,['ano','semana'])

def preparar(panel, train_end=2014, valid_end=2017, min_casos=1000):
    if not 2005<=train_end<valid_end<=2019:raise ValueError('Se requiere 2005 <= train_end < valid_end <= 2019')
    p=validar_gold(panel,valid_end)
    corte=inicio_epi(train_end+1)-timedelta(weeks=4)
    # Selección con historia previa al embargo, nunca con el periodo de validación.
    deps=(p.filter(F.col('fecha')<F.lit(corte)).groupBy('cod_dep')
          .agg(F.sum('casos').alias('total')).filter(F.col('total')>=min_casos).select('cod_dep'))
    codigos=[r.cod_dep for r in deps.orderBy('cod_dep').collect()]
    if not codigos:raise ValueError('No hay departamentos elegibles')
    p=p.join(deps,'cod_dep').withColumn('sem_ref',F.least('semana',F.lit(52)))
    w=Window.partitionBy('cod_dep').orderBy('fecha')
    historia=p.filter('semana <= 52').select('cod_dep',F.col('semana').alias('sem_ref'),F.col('ano').alias('ano_h'),F.col('casos').alias('casos_h'))
    umbral=(p.select('cod_dep','ano','semana','sem_ref').join(historia,['cod_dep','sem_ref'])
        .filter((F.col('ano_h')>=F.col('ano')-5)&(F.col('ano_h')<F.col('ano')))
        .groupBy('cod_dep','ano','semana').agg(F.percentile_approx('casos_h',0.75).alias('q3'),F.count('*').alias('n_hist')))
    p=(p.join(umbral,['cod_dep','ano','semana'],'left')
       .withColumn('brote',F.when(F.col('n_hist')==5,((F.col('casos')>F.col('q3'))&(F.col('casos')>=5)).cast('double')))
       .withColumn('n_futuro',F.count('brote').over(w.rowsBetween(1,4)))
       .withColumn('label',F.when(F.col('n_futuro')==4,F.max('brote').over(w.rowsBetween(1,4))))
       .withColumn('log_casos',F.log1p('casos')).withColumn('log_casos_l1',F.lag('log_casos').over(w))
       .withColumn('brote_actual',F.col('brote')))
    # Climatología solo de filas del entrenamiento, excluido el embargo.
    clim=(p.filter((F.col('ano')>=2005)&(F.col('fecha')<F.lit(corte)))
        .groupBy('cod_dep','sem_ref').agg(*[F.avg(v).alias('base_'+v) for v in ['temp_media','hum_media','precip_total']]))
    p=p.join(clim,['cod_dep','sem_ref'],'left')
    for v,a in [('temp_media','anom_temp'),('hum_media','anom_hum'),('precip_total','anom_prec')]:p=p.withColumn(a,F.col(v)-F.col('base_'+v))
    angulo=2*pi*(F.dayofyear('fecha')-1)/365.2425
    p=p.withColumn('estacion_sin',F.sin(angulo)).withColumn('estacion_cos',F.cos(angulo))
    for v in ['temp_media','hum_media','precip_total','anom_temp','anom_hum','anom_prec']:
        for n,(a,b) in VENTANAS.items():
            ventana=w.rowsBetween(a,b)
            p=p.withColumn(f'{v}_{n}',F.when(F.count(v).over(ventana)==4,F.avg(v).over(ventana)))
    comunes=p.filter(F.col('label').isNotNull()).dropna(subset=CASOS+ESTACION+CLIMA)
    train=comunes.filter((F.col('ano')>=2005)&(F.col('fecha')<F.lit(corte)))
    valid=comunes.filter(F.col('ano').between(train_end+1,valid_end))
    protocolo={'train':[2005,train_end],'validacion':[train_end+1,valid_end],
        'corte_exclusivo_train':corte.isoformat(),'embargo_semanas':4,'horizonte':4,'historia_anios':5,
        'min_casos_brote':5,'min_casos_departamento':min_casos,'departamentos':codigos,
        'cohorte':'Seleccionada con conteos 2000 hasta corte exclusivo de train',
        'climatologia':'2005 hasta corte exclusivo de train',
        'umbral':0.5,'nota':'Exploratorio; 2021–2024 no se usa y ya fue inspeccionado en el parcial.'}
    return train,valid,protocolo

def crear_pipeline(experimento):
    numericas=EXPERIMENTOS[experimento]
    etapas=[]
    entradas=['numericas_escaladas']
    if experimento!='casos':
        etapas += [StringIndexer(inputCol='cod_dep',outputCol='departamento_idx',handleInvalid='keep'),
                    OneHotEncoder(inputCols=['departamento_idx'],outputCols=['departamento_ohe'],handleInvalid='keep')]
        entradas.append('departamento_ohe')
    etapas += [VectorAssembler(inputCols=numericas,outputCol='numericas'),
               StandardScaler(inputCol='numericas',outputCol='numericas_escaladas',withStd=True,withMean=False),
               VectorAssembler(inputCols=entradas,outputCol='features'),
               LogisticRegression(featuresCol='features',labelCol='label',weightCol='peso',maxIter=100)]
    return Pipeline(stages=etapas)

def diccionario():
    filas=[{'variable':'cod_dep','formula':'StringIndexer + OneHotEncoder; ajustados en train','disponibilidad':'Conocido en t'},
        {'variable':'log_casos','formula':'log(1+casos_t)','disponibilidad':'Casos notificados disponibles en t'},
        {'variable':'log_casos_l1','formula':'log(1+casos_{t-1})','disponibilidad':'Semana anterior'},
        {'variable':'brote_actual','formula':'casos_t > Q3 de misma semana en cinco años previos y casos_t >=5','disponibilidad':'Hasta t'},
        {'variable':'estacion_sin','formula':'sin(2*pi*(dia_del_anio(fecha)-1)/365.2425)','disponibilidad':'Calendario conocido'},
        {'variable':'estacion_cos','formula':'cos(2*pi*(dia_del_anio(fecha)-1)/365.2425)','disponibilidad':'Calendario conocido'}]
    for v in ['temp_media','hum_media','precip_total','anom_temp','anom_hum','anom_prec']:
        for n,(a,b) in VENTANAS.items():
            filas.append({'variable':f'{v}_{n}','formula':f'Media de {v} entre t{a:+d} y t{b:+d}; cuatro valores válidos. Anomalías respecto a climatología de train.',
                          'disponibilidad':'Hasta t; no incluye clima futuro'})
    return filas
