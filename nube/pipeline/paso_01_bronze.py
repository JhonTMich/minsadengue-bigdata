from pyspark.sql import functions as F

from pipeline.comun import (Cronometro, argumentos, cargas_en_raw, crear_spark, escribir_evidencia,
                            leer_manifiesto, registrar_en_manifiesto, ruta)

FUENTES = {
    "dengue": ["departamento", "provincia", "distrito", "ubigeo", "ano", "semana",
               "diagnostic", "edad", "tipo_edad", "sexo"],
    "clima": ["cod_dep", "departamento", "fecha", "T2M", "RH2M", "PRECTOTCORR"],
}


def separador(cabecera):
    # El MINSA publicó hasta 2024 con ";" y desde el corte 2025 con "," y comillas.
    return ";" if cabecera.count(";") > cabecera.count(",") else ","


def ingestar_carga(spark, base, fuente, fecha_carga):
    requeridas = FUENTES[fuente]
    carpeta = ruta(base, "raw", fuente, f"fecha_carga={fecha_carga}")
    cabecera = spark.read.option("pathGlobFilter", "*.csv").text(carpeta).first()[0]
    sep = separador(cabecera.lstrip("﻿"))
    df = (spark.read.option("pathGlobFilter", "*.csv")
          .csv(carpeta, header=True, inferSchema=False, sep=sep, quote='"', encoding="UTF-8"))
    df = df.toDF(*[c.lstrip("﻿").strip() for c in df.columns])
    faltantes = set(requeridas) - set(df.columns)
    if faltantes:
        raise ValueError(f"{fuente} {fecha_carga}: faltan columnas {sorted(faltantes)}")
    (df.withColumn("_archivo_origen", F.element_at(F.split(F.input_file_name(), "/"), -1))
       .withColumn("_fecha_ingesta", F.current_timestamp())
       .withColumn("fecha_carga", F.lit(fecha_carga))
       .write.mode("overwrite").partitionBy("fecha_carga").parquet(ruta(base, "bronze", fuente)))
    return (spark.read.parquet(ruta(base, "bronze", fuente))
            .filter(F.col("fecha_carga") == fecha_carga).count())


def main():
    args = argumentos("Raw a Bronze, solo cargas nuevas").parse_args()
    spark = crear_spark("dengue-paso1-bronze", args.shuffle)
    crono = Cronometro()

    manifiesto = leer_manifiesto(spark, args.base).filter(F.col("etapa") == "bronze")
    hechas = {(r["fuente"], r["fecha_carga"]) for r in manifiesto.collect()}

    procesadas = []
    for fuente in FUENTES:
        for fecha_carga in cargas_en_raw(spark, args.base, fuente):
            if (fuente, fecha_carga) in hechas:
                continue
            with crono.etapa(f"bronze_{fuente}_{fecha_carga}"):
                filas = ingestar_carga(spark, args.base, fuente, fecha_carga)
            registrar_en_manifiesto(spark, args.base, [("bronze", fuente, fecha_carga, [], filas)])
            procesadas.append({"fuente": fuente, "fecha_carga": fecha_carga, "filas": filas})

    if not procesadas:
        print("Sin cargas nuevas en Raw.", flush=True)
    escribir_evidencia(spark, args.base, "paso_01_bronze",
                       {"cargas_procesadas": procesadas, "tiempos": crono.tiempos})
    spark.stop()


if __name__ == "__main__":
    main()