from statistics import median

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Window
from pyspark.sql import functions as F

from pipeline.comun import (Cronometro, argumentos, cargas_pendientes, crear_spark, escribir_evidencia,
                            existe, leer_json, registrar_en_manifiesto, ruta)
from pipeline.metricas import resumen
from pipeline.variables import ANIO_MIN_ENTRENAMIENTO, EXPERIMENTOS, HISTORIA_ANIOS, HORIZONTE, construir

CLAVES = ["cod_dep", "ano", "semana"]


def cargar_modelo(spark, base):
    puntero = ruta(base, "modelos", "vigente")
    if not existe(spark, puntero):
        raise ValueError("No hay modelo vigente: ejecutar antes paso_04_modelo.py")
    destino = leer_json(spark, puntero)["ruta"]
    return (PipelineModel.load(ruta(destino, "pipeline")), leer_json(spark, ruta(destino, "metadatos")),
            spark.read.parquet(ruta(destino, "climatologia")))


def puntuar(panel, dim_semana, modelo, meta, clima_base):
    df = construir(panel, dim_semana, clima_base).localCheckpoint()
    completas = df.dropna(subset=EXPERIMENTOS[meta["experimento"]])
    probabilidades = (modelo.transform(completas)
                      .select(*CLAVES, vector_to_array("probability")[1].alias("probabilidad")))
    return df.join(probabilidades, CLAVES, "left")


def a_alertas(df, meta, fecha_carga):
    return df.select(
        "cod_dep", "departamento", "ano", "semana", F.col("fecha").alias("fecha_inicio"), "casos",
        F.col("umbral_q3").cast("double").alias("umbral_q3"),
        F.col("brote_actual").cast("int").alias("brote_actual"),
        "probabilidad",
        (F.col("probabilidad") >= F.lit(meta["umbral"])).alias("alerta"),
        F.col("label").cast("int").alias("brote_observado"),
        F.lit(float(meta["umbral"])).alias("umbral_alerta"),
        F.lit(meta["version"]).alias("version_modelo"),
        (F.col("ano") <= F.lit(meta["entrenamiento"]["hasta"])).alias("dentro_entrenamiento"),
        F.lit(fecha_carga).alias("fecha_carga"),
        F.current_timestamp().alias("fecha_proceso"))


def completar_etiquetas(spark, destino, anio, puntuado):
    """Las últimas semanas de `anio` recién tienen etiqueta cuando llegan las 4 siguientes."""
    nuevas = puntuado.filter(F.col("ano") == anio).select("cod_dep", "semana",
                                                          F.col("label").cast("int").alias("etiqueta"))
    previas = spark.read.parquet(destino).filter(F.col("ano") == anio)
    unidas = previas.join(nuevas, ["cod_dep", "semana"], "left")
    completadas = unidas.filter(F.col("brote_observado").isNull() & F.col("etiqueta").isNotNull()).count()
    return (unidas.withColumn("brote_observado", F.coalesce("brote_observado", "etiqueta"))
            .drop("etiqueta"), completadas)


def anticipacion(alertas):
    """Semanas de alerta consecutivas justo antes del inicio de cada brote (máximo 4).
    Una alerta separada del inicio por una semana sin alerta no cuenta."""
    serie = Window.partitionBy("cod_dep").orderBy("fecha_inicio")
    df = alertas.withColumn("previo", F.lag("brote_actual").over(serie))
    for k in range(1, HORIZONTE + 1):
        df = df.withColumn(f"alerta_{k}", F.coalesce(F.lag("alerta", k).over(serie), F.lit(False)))
    semanas = F.lit(0)
    seguidas = F.lit(True)
    for k in range(1, HORIZONTE + 1):
        seguidas = seguidas & F.col(f"alerta_{k}")
        semanas = semanas + F.when(seguidas, 1).otherwise(0)
    inicios = (df.filter((F.col("brote_actual") == 1) & (F.col("previo") == 0) & ~F.col("dentro_entrenamiento"))
               .select(semanas.alias("semanas")).collect())
    valores = [r.semanas for r in inicios]
    anticipados = [v for v in valores if v > 0]
    return {"brotes_iniciados": len(valores), "con_alerta_previa": len(anticipados),
            "proporcion": len(anticipados) / len(valores) if valores else None,
            "mediana_semanas": median(anticipados) if anticipados else None,
            "distribucion": {k: valores.count(k) for k in range(HORIZONTE + 1)}}


def desempenio(alertas):
    filas = (alertas.filter(~F.col("dentro_entrenamiento") & F.col("brote_observado").isNotNull()
                            & F.col("probabilidad").isNotNull())
             .select("brote_observado", "brote_actual", "probabilidad", "umbral_alerta").collect())
    if not filas:
        return {}
    umbral = filas[0].umbral_alerta
    salida = {}
    for nombre, sel in (("completo", filas), ("sin_brote_actual", [f for f in filas if not f.brote_actual])):
        etiquetas = [f.brote_observado for f in sel]
        salida[nombre] = {"modelo": resumen(etiquetas, [f.probabilidad for f in sel], umbral),
                          "persistencia": resumen(etiquetas, [f.brote_actual for f in sel], 0.5)}
    return salida


def main():
    args = argumentos("Puntúa con el modelo vigente solo las semanas nuevas y escribe las alertas").parse_args()
    spark = crear_spark("dengue-paso5-alertas", args.shuffle)
    crono = Cronometro()

    pendientes = cargas_pendientes(spark, args.base, "gold", "alertas", ["dengue", "clima"])
    afectados = sorted({a for c in pendientes for a in c["anios_afectados"] if a >= ANIO_MIN_ENTRENAMIENTO})
    if not afectados:
        if pendientes:
            registrar_en_manifiesto(spark, args.base, [("alertas", c["fuente"], c["fecha_carga"], [], 0)
                                                       for c in pendientes])
        print("Sin semanas nuevas: las alertas no cambian.", flush=True)
        escribir_evidencia(spark, args.base, "paso_05_alertas", {"anios_escritos": [], "tiempos": []})
        spark.stop()
        return

    modelo, meta, clima_base = cargar_modelo(spark, args.base)
    destino = ruta(args.base, "gold", "alertas")
    anterior = afectados[0] - 1
    completar = (anterior >= ANIO_MIN_ENTRENAMIENTO and anterior not in afectados
                 and existe(spark, ruta(destino, f"ano={anterior}")))

    desde = afectados[0] - HISTORIA_ANIOS - 1
    panel = (spark.read.parquet(ruta(args.base, "gold", "panel_dengue_clima"))
             .filter(F.col("ano").between(desde, afectados[-1])))
    dim_semana = spark.read.parquet(ruta(args.base, "gold", "dim_semana"))

    with crono.etapa("puntuacion"):
        puntuado = puntuar(panel, dim_semana, modelo, meta, clima_base).localCheckpoint()
        fecha_carga = max(c["fecha_carga"] for c in pendientes)
        salida = a_alertas(puntuado.filter(F.col("ano").isin(afectados)), meta, fecha_carga)
        completadas = 0
        if completar:
            previas, completadas = completar_etiquetas(spark, destino, anterior, puntuado)
            salida = salida.unionByName(previas)
        salida = salida.localCheckpoint()

    with crono.etapa("escritura"):
        (salida.repartition("ano").sortWithinPartitions("cod_dep", "semana")
               .write.mode("overwrite").partitionBy("ano").parquet(destino))

    escritos = afectados + ([anterior] if completar else [])
    nuevas = spark.read.parquet(destino).filter(F.col("ano").isin(afectados))
    filas = nuevas.count()
    sin_probabilidad = nuevas.filter(F.col("probabilidad").isNull()).count()
    esperadas = panel.filter(F.col("ano").isin(afectados)).count()
    assert filas == esperadas, f"Alertas con {filas} filas; Gold tiene {esperadas}"

    todas = spark.read.parquet(destino)
    registrar_en_manifiesto(spark, args.base, [("alertas", c["fuente"], c["fecha_carga"], escritos, filas)
                                               for c in pendientes])
    escribir_evidencia(spark, args.base, "paso_05_alertas", {
        "cargas": [f"{c['fuente']} {c['fecha_carga']}" for c in pendientes],
        "version_modelo": meta["version"], "experimento": meta["experimento"], "umbral": meta["umbral"],
        "anios_puntuados": afectados, "anio_con_etiquetas_completadas": anterior if completar else None,
        "etiquetas_completadas": completadas, "filas_escritas": filas, "sin_probabilidad": sin_probabilidad,
        "alertas_emitidas": nuevas.filter(F.col("alerta")).count(),
        "fuera_de_entrenamiento": {"desempenio": desempenio(todas), "anticipacion": anticipacion(todas)},
        "tiempos": crono.tiempos})
    spark.stop()


if __name__ == "__main__":
    main()