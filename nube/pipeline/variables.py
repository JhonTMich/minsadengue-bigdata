"""Etiqueta de brote y variables del modelo, compartidas por el entrenamiento y la puntuación."""

from math import pi

from pyspark.sql import Window
from pyspark.sql import functions as F

HORIZONTE = 4
HISTORIA_ANIOS = 5
MIN_CASOS_BROTE = 5
ANIO_MIN_ENTRENAMIENTO = 2005
SEMANAS_PREVIAS = 11

VENTANAS = {"0_3": (-3, 0), "4_7": (-7, -4), "8_11": (-11, -8)}
ANOMALIAS = {"temp_media": "anom_temp", "hum_media": "anom_hum", "precip_total": "anom_prec"}
CASOS = ["log_casos", "log_casos_l1", "brote_actual"]
ESTACION = ["estacion_sin", "estacion_cos"]
CLIMA = [f"{v}_{n}" for v in [*ANOMALIAS, *ANOMALIAS.values()] for n in VENTANAS]
EXPERIMENTOS = {"casos": CASOS, "contexto": CASOS + ESTACION, "contexto_clima": CASOS + ESTACION + CLIMA}
TODAS = CASOS + ESTACION + CLIMA


def _serie():
    return Window.partitionBy("cod_dep").orderBy("fecha")


def con_fecha(panel, dim_semana):
    return panel.join(dim_semana.select("ano", "semana", F.col("fecha_inicio").alias("fecha")),
                      ["ano", "semana"])


def etiquetar(panel):
    """Brote: casos > Q3 de la misma semana en los 5 años previos y al menos 5 casos.
    La etiqueta es 1 si hay brote en alguna de las 4 semanas siguientes; queda nula si
    esas 4 semanas todavía no existen en el panel recibido."""
    p = panel.withColumn("sem_ref", F.least("semana", F.lit(52)))
    historia = (p.filter(F.col("semana") <= 52)
                .select("cod_dep", "sem_ref", F.col("ano").alias("ano_h"), F.col("casos").alias("casos_h")))
    umbral = (p.select("cod_dep", "ano", "semana", "sem_ref").join(historia, ["cod_dep", "sem_ref"])
              .filter((F.col("ano_h") >= F.col("ano") - HISTORIA_ANIOS) & (F.col("ano_h") < F.col("ano")))
              .groupBy("cod_dep", "ano", "semana")
              .agg(F.percentile_approx("casos_h", 0.75).alias("umbral_q3"), F.count("*").alias("n_hist")))
    futuro = _serie().rowsBetween(1, HORIZONTE)
    return (p.join(umbral, ["cod_dep", "ano", "semana"], "left")
            .withColumn("brote", F.when(F.col("n_hist") == HISTORIA_ANIOS,
                                        ((F.col("casos") > F.col("umbral_q3"))
                                         & (F.col("casos") >= MIN_CASOS_BROTE)).cast("double")))
            .withColumn("label", F.when(F.count("brote").over(futuro) == HORIZONTE, F.max("brote").over(futuro)))
            .drop("n_hist"))


def variables_base(df):
    angulo = 2 * pi * (F.dayofyear("fecha") - 1) / 365.2425
    return (df.withColumn("log_casos", F.log1p("casos"))
            .withColumn("log_casos_l1", F.lag("log_casos").over(_serie()))
            .withColumn("brote_actual", F.col("brote"))
            .withColumn("estacion_sin", F.sin(angulo))
            .withColumn("estacion_cos", F.cos(angulo)))


def climatologia(df):
    """Media semanal de cada departamento; se calcula solo con filas de entrenamiento."""
    return df.groupBy("cod_dep", "sem_ref").agg(*[F.avg(v).alias(f"base_{v}") for v in ANOMALIAS])


def variables_clima(df, clima_base):
    df = df.join(clima_base, ["cod_dep", "sem_ref"], "left")
    for v, anomalia in ANOMALIAS.items():
        df = df.withColumn(anomalia, F.col(v) - F.col(f"base_{v}"))
    for v in [*ANOMALIAS, *ANOMALIAS.values()]:
        for nombre, (desde, hasta) in VENTANAS.items():
            ventana = _serie().rowsBetween(desde, hasta)
            df = df.withColumn(f"{v}_{nombre}", F.when(F.count(v).over(ventana) == 4, F.avg(v).over(ventana)))
    return df.drop(*[f"base_{v}" for v in ANOMALIAS])


def construir(panel, dim_semana, clima_base):
    return variables_clima(variables_base(etiquetar(con_fecha(panel, dim_semana))), clima_base)