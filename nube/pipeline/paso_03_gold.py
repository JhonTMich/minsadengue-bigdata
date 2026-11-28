from datetime import timedelta

from pyspark.sql import functions as F

from pipeline.comun import (ANIO_INICIO, COLUMNAS_GOLD, MIN_CASOS_DEPARTAMENTO, Cronometro,
                            argumentos, cargas_pendientes, crear_spark, escribir_evidencia, existe,
                            inicio_anio_epi, registrar_en_manifiesto, ruta, semanas_del_anio)


def crear_dim_departamento(spark, base):
    destino = ruta(base, "gold", "dim_departamento")
    if not existe(spark, destino):
        (spark.read.parquet(ruta(base, "silver", "dengue_nacional"))
            .groupBy("cod_dep", "departamento").agg(F.count("*").alias("casos_seleccion"))
            .filter(F.col("casos_seleccion") >= MIN_CASOS_DEPARTAMENTO)
            .withColumn("fecha_seleccion", F.current_date())
            .coalesce(1).write.mode("overwrite").parquet(destino))
    return spark.read.parquet(destino)


def escribir_dim_semana(spark, base, anio_max):
    filas = []
    for anio in range(ANIO_INICIO, anio_max + 1):
        inicio = inicio_anio_epi(anio)
        for semana in range(1, semanas_del_anio(anio) + 1):
            desde = inicio + timedelta(weeks=semana - 1)
            filas.append((anio, semana, desde, desde + timedelta(days=6), (desde + timedelta(days=3)).month))
    (spark.createDataFrame(filas, "ano int, semana int, fecha_inicio date, fecha_fin date, mes int")
          .coalesce(1).write.mode("overwrite").parquet(ruta(base, "gold", "dim_semana")))


def calendario(spark, clima, anios):
    # Un año en curso solo llega hasta la última semana con clima completo
    ultima = {r["ano"]: r["semana_max"] for r in
              clima.groupBy("ano").agg(F.max("semana").alias("semana_max")).collect()}
    filas = [(a, s) for a in anios for s in range(1, min(semanas_del_anio(a), ultima.get(a, 0)) + 1)]
    return spark.createDataFrame(filas, "ano int, semana int")


def main():
    args = argumentos("Silver a Gold, solo los años afectados").parse_args()
    spark = crear_spark("dengue-paso3-gold", args.shuffle)
    crono = Cronometro()

    pendientes = cargas_pendientes(spark, args.base, "silver", "gold", ["dengue", "clima"])
    afectados = sorted({a for c in pendientes for a in c["anios_afectados"]})
    if not afectados:
        if pendientes:
            registrar_en_manifiesto(spark, args.base, [("gold", c["fuente"], c["fecha_carga"], [], 0)
                                                       for c in pendientes])
        print("Sin años afectados: Gold no cambia.", flush=True)
        escribir_evidencia(spark, args.base, "paso_03_gold", {"anios_afectados": [], "tiempos": []})
        spark.stop()
        return

    deps = crear_dim_departamento(spark, args.base).select("cod_dep", "departamento")
    dengue = (spark.read.parquet(ruta(args.base, "silver", "dengue_nacional"))
              .filter(F.col("ano").isin(afectados)).join(deps.select("cod_dep"), "cod_dep"))
    clima = (spark.read.parquet(ruta(args.base, "silver", "clima_semanal"))
             .filter(F.col("ano").isin(afectados)).drop("n_dias"))
    semanas = calendario(spark, clima, afectados)

    casos = dengue.groupBy("cod_dep", "ano", "semana").agg(F.count("*").alias("casos"))
    panel = (deps.crossJoin(semanas)
             .join(casos, ["cod_dep", "ano", "semana"], "left")
             .fillna(0, subset=["casos"])
             .join(clima, ["cod_dep", "ano", "semana"], "left")
             .select(*COLUMNAS_GOLD))

    destino = ruta(args.base, "gold", "panel_dengue_clima")
    with crono.etapa("gold_escritura"):
        (panel.repartition("ano").sortWithinPartitions("cod_dep", "semana")
              .write.mode("overwrite").partitionBy("ano").parquet(destino))

    gold = spark.read.parquet(destino).filter(F.col("ano").isin(afectados))
    filas = gold.count()
    esperadas = deps.count() * semanas.count()
    casos_gold = gold.agg(F.sum("casos")).first()[0]
    casos_silver = dengue.join(semanas, ["ano", "semana"]).count()
    nulos_clima = gold.filter(F.col("temp_media").isNull() | F.col("hum_media").isNull()
                              | F.col("precip_total").isNull()).count()
    assert filas == esperadas, f"Gold tiene {filas} filas; se esperaban {esperadas}"
    assert casos_gold == casos_silver, f"Casos en Gold {casos_gold} y en Silver {casos_silver}"
    assert nulos_clima == 0, f"{nulos_clima} semanas sin dato climático"

    anio_max = spark.read.parquet(destino).agg(F.max("ano")).first()[0]
    escribir_dim_semana(spark, args.base, anio_max)
    registrar_en_manifiesto(spark, args.base, [("gold", c["fuente"], c["fecha_carga"], afectados, filas)
                                               for c in pendientes])
    escribir_evidencia(spark, args.base, "paso_03_gold", {
        "cargas": [f"{c['fuente']} {c['fecha_carga']}" for c in pendientes],
        "anios_afectados": afectados, "filas_escritas": filas, "casos": casos_gold,
        "tiempos": crono.tiempos})
    spark.stop()


if __name__ == "__main__":
    main()