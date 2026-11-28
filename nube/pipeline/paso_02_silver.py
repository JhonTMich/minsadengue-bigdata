from pyspark.sql import Window
from pyspark.sql import functions as F

from pipeline.comun import (ANIO_FIN, ANIO_INICIO, ANIOS_53, Cronometro, argumentos,
                            cargas_pendientes, crear_spark, escribir_evidencia, existe,
                            registrar_en_manifiesto, ruta)


def transformar_dengue(df_bronze):
    df = (
        df_bronze.select(
            F.upper(F.trim("departamento")).alias("departamento"),
            F.upper(F.trim("provincia")).alias("provincia"),
            F.upper(F.trim("distrito")).alias("distrito"),
            F.lpad(F.trim("ubigeo"), 6, "0").alias("ubigeo"),
            F.col("ano").cast("int").alias("ano"),
            F.col("semana").cast("int").alias("semana"),
            F.trim("diagnostic").alias("diagnostic"),
            F.col("edad").cast("int").alias("edad"),
            F.upper(F.trim("tipo_edad")).alias("tipo_edad"),
            F.upper(F.trim("sexo")).alias("sexo"),
            "fecha_carga",
        )
        .withColumn("cod_dep", F.substring("ubigeo", 1, 2))
        .withColumn("edad_anios",
                    F.when(F.col("tipo_edad") == "A", F.col("edad"))
                     .when(F.col("tipo_edad") == "M", F.floor(F.col("edad") / 12))
                     .when(F.col("tipo_edad") == "D", F.floor(F.col("edad") / 365)))
    )
    semana_valida = (F.col("semana").between(1, 52)
                     | ((F.col("semana") == 53) & F.col("ano").isin(ANIOS_53)))
    reglas = {
        "anio_fuera_de_rango": ~F.col("ano").between(ANIO_INICIO, ANIO_FIN),
        "semana_inexistente": ~semana_valida,
        "ubigeo_invalido": ~F.col("ubigeo").rlike("^[0-9]{6}$") | ~F.col("cod_dep").between("01", "25"),
        "sexo_invalido": ~F.col("sexo").isin("F", "M"),
        "edad_invalida": ~F.col("edad_anios").between(0, 110),
        "no_es_dengue": ~F.col("diagnostic").isin("A97.0", "A97.1", "A97.2"),
    }
    motivo = F.concat_ws(",", *[F.when(F.coalesce(cond, F.lit(True)), F.lit(nombre))
                                for nombre, cond in reglas.items()])
    return df.withColumn("motivo_rechazo", motivo)


def anios_con_cambios(spark, destino, nuevos):
    anios = sorted(r["ano"] for r in nuevos.select("ano").distinct().collect())
    if not existe(spark, destino):
        return anios
    clave = ["ano", "semana", "cod_dep"]
    nuevos_conteo = nuevos.groupBy(clave).agg(F.count("*").alias("n_nuevo"))
    vigentes_conteo = (spark.read.parquet(destino).filter(F.col("ano").isin(anios))
                       .groupBy(clave).agg(F.count("*").alias("n_vigente")))
    return sorted(r["ano"] for r in nuevos_conteo.join(vigentes_conteo, clave, "full")
                  .filter(~F.col("n_nuevo").eqNullSafe(F.col("n_vigente")))
                  .select("ano").distinct().collect())


def procesar_dengue(spark, base, fecha_carga):
    destino = ruta(base, "silver", "dengue_nacional")
    evaluado = transformar_dengue(
        spark.read.parquet(ruta(base, "bronze", "dengue")).filter(F.col("fecha_carga") == fecha_carga))
    validos = evaluado.filter(F.col("motivo_rechazo") == "").drop("motivo_rechazo").cache()
    rechazados = evaluado.filter(F.col("motivo_rechazo") != "")

    afectados = anios_con_cambios(spark, destino, validos)
    if afectados:
        (validos.filter(F.col("ano").isin(afectados)).repartition("ano")
            .write.mode("overwrite").partitionBy("ano").parquet(destino))
    (rechazados.coalesce(1).write.mode("overwrite").partitionBy("fecha_carga")
        .parquet(ruta(base, "silver", "dengue_rechazados")))

    resumen = {"validos": validos.count(),
               "rechazados_por_motivo": {r["motivo_rechazo"]: r["count"]
                                         for r in rechazados.groupBy("motivo_rechazo").count().collect()},
               "anios_afectados": afectados}
    validos.unpersist()
    return afectados, resumen


def clima_diario(df_bronze):
    variables = ["T2M", "RH2M", "PRECTOTCORR"]
    return (
        df_bronze.select(
            F.trim("cod_dep").alias("cod_dep"),
            F.to_date("fecha", "yyyyMMdd").alias("fecha"),
            "fecha_carga",
            *[F.when(F.col(v).cast("double") == -999, None).otherwise(F.col(v).cast("double")).alias(v)
              for v in variables])
        .withColumn("T2M", F.when(F.col("T2M").between(-10, 45), F.col("T2M")))
        .withColumn("RH2M", F.when(F.col("RH2M").between(0, 100), F.col("RH2M")))
        .withColumn("PRECTOTCORR", F.when(F.col("PRECTOTCORR") >= 0, F.col("PRECTOTCORR")))
        # El miércoles de la semana (domingo a sábado) define el año y la semana epidemiológica
        .withColumn("miercoles", F.expr("date_add(fecha, 4 - dayofweek(fecha))"))
        .withColumn("ano", F.year("miercoles"))
        .withColumn("semana", (F.floor((F.dayofyear("miercoles") - 1) / 7) + 1).cast("int"))
        .filter(F.col("ano").between(ANIO_INICIO, ANIO_FIN))
    )


def procesar_clima(spark, base, fecha_carga):
    bronze = spark.read.parquet(ruta(base, "bronze", "clima"))
    candidatos = sorted(r["ano"] for r in clima_diario(bronze.filter(F.col("fecha_carga") == fecha_carga))
                        .select("ano").distinct().collect())
    if not candidatos:
        return [], {"anios_afectados": []}

    # Cada día conserva el valor de la carga más reciente hasta esta carga
    ultima = Window.partitionBy("cod_dep", "fecha").orderBy(F.desc("fecha_carga"))
    diario = (clima_diario(bronze.filter(F.col("fecha_carga") <= fecha_carga))
              .filter(F.col("ano").isin(candidatos))
              .withColumn("orden", F.row_number().over(ultima))
              .filter(F.col("orden") == 1))
    semanal = (diario.groupBy("cod_dep", "ano", "semana")
               .agg(F.count("*").alias("n_dias"),
                    F.round(F.avg("T2M"), 2).alias("temp_media"),
                    F.round(F.avg("RH2M"), 2).alias("hum_media"),
                    F.round(F.sum("PRECTOTCORR"), 2).alias("precip_total"))
               .filter(F.col("n_dias") == 7)
               .cache())
    afectados = sorted(r["ano"] for r in semanal.select("ano").distinct().collect())
    if not afectados:
        semanal.unpersist()
        return [], {"anios_afectados": []}
    semanal.repartition("ano").write.mode("overwrite").partitionBy("ano").parquet(
        ruta(base, "silver", "clima_semanal"))

    escrito = spark.read.parquet(ruta(base, "silver", "clima_semanal")).filter(F.col("ano").isin(afectados))
    con_53 = {r["ano"] for r in escrito.filter("semana = 53").select("ano").distinct().collect()}
    assert con_53 <= set(ANIOS_53), f"Semana 53 en años que no la tienen: {sorted(con_53 - set(ANIOS_53))}"
    semanal.unpersist()
    return afectados, {"semanas": escrito.count(), "anios_afectados": afectados}


def main():
    args = argumentos("Bronze a Silver, solo los años afectados").parse_args()
    spark = crear_spark("dengue-paso2-silver", args.shuffle)
    crono = Cronometro()

    procesadas = []
    for carga in cargas_pendientes(spark, args.base, "bronze", "silver", ["dengue", "clima"]):
        fuente, fecha_carga = carga["fuente"], carga["fecha_carga"]
        procesar = procesar_dengue if fuente == "dengue" else procesar_clima
        with crono.etapa(f"silver_{fuente}_{fecha_carga}"):
            afectados, resumen = procesar(spark, args.base, fecha_carga)
        filas = resumen.get("validos", resumen.get("semanas", 0))
        registrar_en_manifiesto(spark, args.base, [("silver", fuente, fecha_carga, afectados, filas)])
        procesadas.append({"fuente": fuente, "fecha_carga": fecha_carga, **resumen})

    if not procesadas:
        print("Sin cargas nuevas en Bronze.", flush=True)
    escribir_evidencia(spark, args.base, "paso_02_silver",
                       {"cargas_procesadas": procesadas, "tiempos": crono.tiempos})
    spark.stop()


if __name__ == "__main__":
    main()