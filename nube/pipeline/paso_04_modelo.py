from datetime import datetime, timedelta, timezone

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipeline.comun import (Cronometro, argumentos, crear_spark, escribir_evidencia, escribir_json,
                            inicio_anio_epi, ruta)
from pipeline.metricas import resumen, umbral_por_costo, umbral_por_tasa
from pipeline.variables import (ANIO_MIN_ENTRENAMIENTO, EXPERIMENTOS, HISTORIA_ANIOS, HORIZONTE, TODAS,
                                climatologia, con_fecha, etiquetar, variables_base, variables_clima)

ESQUEMA_METRICAS = T.StructType([
    T.StructField("version", T.StringType()),
    T.StructField("modelo", T.StringType()),
    T.StructField("subconjunto", T.StringType()),
    T.StructField("elegido", T.BooleanType()),
    T.StructField("anios_validacion", T.StringType()),
    *[T.StructField(c, T.LongType()) for c in ("n", "positivos")],
    *[T.StructField(c, T.DoubleType()) for c in ("auc", "precision_promedio", "umbral", "precision", "recall")],
    *[T.StructField(c, T.LongType()) for c in ("vp", "fp", "fn", "vn")],
])


def crear_pipeline(experimento):
    etapas, entradas = [], ["numericas_escaladas"]
    if experimento != "casos":
        etapas += [StringIndexer(inputCol="cod_dep", outputCol="departamento_idx", handleInvalid="keep"),
                   OneHotEncoder(inputCols=["departamento_idx"], outputCols=["departamento_ohe"],
                                 handleInvalid="keep")]
        entradas.append("departamento_ohe")
    etapas += [VectorAssembler(inputCols=EXPERIMENTOS[experimento], outputCol="numericas"),
               StandardScaler(inputCol="numericas", outputCol="numericas_escaladas", withStd=True, withMean=False),
               VectorAssembler(inputCols=entradas, outputCol="features"),
               LogisticRegression(featuresCol="features", labelCol="label", weightCol="peso", maxIter=100)]
    return Pipeline(stages=etapas)


def corte(anio):
    """Primera semana excluida del entrenamiento: su etiqueta miraría semanas posteriores a `anio`."""
    return inicio_anio_epi(anio + 1) - timedelta(weeks=HORIZONTE)


def particion(base, fin_train):
    en_train = (F.col("ano") >= ANIO_MIN_ENTRENAMIENTO) & (F.col("fecha") < F.lit(corte(fin_train)))
    clima_base = climatologia(base.filter(en_train))
    completo = variables_clima(base, clima_base).filter(F.col("label").isNotNull()).dropna(subset=TODAS)
    return completo.filter(en_train), completo, clima_base


def ajustar(train, experimento):
    prevalencia = train.agg(F.avg("label")).first()[0]
    if not prevalencia or prevalencia >= 1:
        raise ValueError("El entrenamiento necesita semanas con y sin brote")
    pesado = train.withColumn("peso", F.when(F.col("label") == 1, 0.5 / prevalencia)
                              .otherwise(0.5 / (1 - prevalencia)))
    return crear_pipeline(experimento).fit(pesado), prevalencia


def probabilidades(modelo, df, *columnas):
    return (modelo.transform(df).withColumn("prob", vector_to_array("probability")[1])
            .select(*columnas, "prob"))


def evaluar(filas, puntaje, umbral):
    salida = {}
    for subconjunto, sel in (("completo", filas), ("sin_brote_actual", [f for f in filas if not f["brote_actual"]])):
        salida[subconjunto] = resumen([int(f["label"]) for f in sel], [float(f[puntaje]) for f in sel], umbral)
    return salida


def main():
    parser = argumentos("Entrena el modelo de alerta con Spark MLlib y lo guarda versionado")
    parser.add_argument("--hasta", type=int, help="Último año de entrenamiento; por defecto, el último de Gold")
    parser.add_argument("--anios-validacion", type=int, default=3)
    parser.add_argument("--costo-fn", type=float, default=1.0,
                        help="Cuántas falsas alarmas equivale un brote no detectado")
    parser.add_argument("--experimento", choices=["auto", *EXPERIMENTOS], default="auto")
    parser.add_argument("--criterio-umbral", choices=["costo", "tope_alertas"], default="costo")
    parser.add_argument("--max-tasa-alertas", type=float, default=0.20,
                        help="Presupuesto de semanas-departamento con alerta en validación completa")
    parser.add_argument("--exportar-validacion", action="store_true",
                        help="Guarda probabilidades de validación por candidato para auditar umbrales")
    args = parser.parse_args()
    if not 0 <= args.max_tasa_alertas <= 1 or args.costo_fn <= 0:
        parser.error("La tasa debe estar entre 0 y 1 y el costo debe ser positivo")
    spark = crear_spark("dengue-paso4-modelo", args.shuffle)
    crono = Cronometro()

    panel = spark.read.parquet(ruta(args.base, "gold", "panel_dengue_clima"))
    dim_semana = spark.read.parquet(ruta(args.base, "gold", "dim_semana"))
    hasta = args.hasta or panel.agg(F.max("ano")).first()[0]
    fin_train = hasta - args.anios_validacion
    if fin_train < ANIO_MIN_ENTRENAMIENTO + HISTORIA_ANIOS or args.anios_validacion < 1:
        raise ValueError(f"Con --hasta {hasta} no queda historia suficiente para entrenar y validar")

    base = variables_base(etiquetar(con_fecha(panel.filter(F.col("ano") <= hasta), dim_semana))).cache()

    with crono.etapa("validacion"):
        train_v, completo_v, _ = particion(base, fin_train)
        valid_v = completo_v.filter(F.col("ano").between(fin_train + 1, hasta))
        filas = {f"{r.cod_dep}-{r.ano}-{r.semana}": r.asDict() for r in
                 valid_v.select("cod_dep", "ano", "semana", "label", "brote_actual", "log_casos").collect()}
        candidatos = list(EXPERIMENTOS) if args.experimento == "auto" else [args.experimento]
        for experimento in candidatos:
            modelo, _ = ajustar(train_v, experimento)
            for r in probabilidades(modelo, valid_v, "cod_dep", "ano", "semana").collect():
                filas[f"{r.cod_dep}-{r.ano}-{r.semana}"][experimento] = r.prob
        filas = list(filas.values())

    etiquetas = [int(f["label"]) for f in filas]
    umbrales = {e: (umbral_por_costo(etiquetas, [f[e] for f in filas], args.costo_fn)
                    if args.criterio_umbral == "costo"
                    else umbral_por_tasa([f[e] for f in filas], args.max_tasa_alertas)) for e in candidatos}
    resultados = {e: evaluar(filas, e, umbrales[e]) for e in candidatos}
    resultados["persistencia"] = evaluar(filas, "brote_actual", 0.5)
    resultados["casos_solos"] = evaluar(filas, "log_casos", None)
    elegido = max(candidatos, key=lambda e: resultados[e]["sin_brote_actual"]["precision_promedio"] or 0)

    with crono.etapa("entrenamiento_final"):
        train_f, _, clima_f = particion(base, hasta)
        modelo, prevalencia = ajustar(train_f, elegido)

    version = f"v{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    destino = ruta(args.base, "modelos", version)
    with crono.etapa("guardado"):
        modelo.write().overwrite().save(ruta(destino, "pipeline"))
        clima_f.coalesce(1).write.mode("overwrite").parquet(ruta(destino, "climatologia"))
        muestra = train_f.orderBy("cod_dep", "ano", "semana").limit(50).cache()
        claves = ("cod_dep", "ano", "semana")
        antes = [r.prob for r in probabilidades(modelo, muestra, *claves).orderBy(*claves).collect()]
        recargado = PipelineModel.load(ruta(destino, "pipeline"))
        despues = [r.prob for r in probabilidades(recargado, muestra, *claves).orderBy(*claves).collect()]
        muestra.unpersist()
        if max(abs(a - b) for a, b in zip(antes, despues)) > 1e-12:
            raise AssertionError("El modelo recargado no reproduce sus probabilidades")

    n_train = train_f.count()
    metadatos = {
        "version": version, "experimento": elegido, "variables": EXPERIMENTOS[elegido],
        "umbral": umbrales[elegido], "costo_fn": args.costo_fn if args.criterio_umbral == "costo" else None,
        "criterio_umbral": {"tipo": args.criterio_umbral, "subconjunto": "completo",
                            "max_tasa_alertas": args.max_tasa_alertas if args.criterio_umbral == "tope_alertas" else None,
                            "busqueda": "rejilla 0.01..0.99" if args.criterio_umbral == "costo"
                            else "corte empírico exacto; >=; sin separar empates"},
        "entrenamiento": {"desde": ANIO_MIN_ENTRENAMIENTO, "hasta": hasta, "corte_exclusivo": corte(hasta),
                          "filas": n_train, "prevalencia": prevalencia},
        "validacion": {"entrenamiento_hasta": fin_train, "anios": [fin_train + 1, hasta],
                       "corte_exclusivo": corte(fin_train), "filas": len(filas)},
        "definicion_brote": f"casos > Q3 de la misma semana en {HISTORIA_ANIOS} años previos y >= 5 casos",
        "horizonte_semanas": HORIZONTE,
        "seleccion": "Mayor precisión promedio en validación, semanas sin brote vigente",
    }
    escribir_json(spark, ruta(destino, "metadatos"), metadatos)
    if args.exportar_validacion:
        escribir_json(spark, ruta(destino, "validacion_predicciones"), {"anios": [fin_train + 1, hasta],
            "entrenamiento_hasta": fin_train, "modelo_elegido": elegido, "candidatos": candidatos, "filas": filas})
    escribir_json(spark, ruta(args.base, "modelos", "vigente"), {"version": version, "ruta": destino})

    periodo = f"{fin_train + 1}-{hasta}"
    registros = [{"version": version, "modelo": m, "subconjunto": s, "elegido": m == elegido,
                  "anios_validacion": periodo, **fila}
                 for m, por_sub in resultados.items() for s, fila in por_sub.items()]
    (spark.createDataFrame(registros, ESQUEMA_METRICAS).coalesce(1)
          .write.mode("append").parquet(ruta(args.base, "gold", "modelo_metricas")))

    escribir_evidencia(spark, args.base, "paso_04_modelo", {**metadatos, "metricas_validacion": resultados,
                                                           "tiempos": crono.tiempos})
    base.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
