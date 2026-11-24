import argparse
import json
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

ANIO_INICIO = 2000
ANIO_FIN = date.today().year
MIN_CASOS_DEPARTAMENTO = 1000

COLUMNAS_GOLD = ["cod_dep", "departamento", "ano", "semana", "casos",
                 "temp_media", "hum_media", "precip_total"]

ESQUEMA_MANIFIESTO = T.StructType([
    T.StructField("etapa", T.StringType()),
    T.StructField("fuente", T.StringType()),
    T.StructField("fecha_carga", T.StringType()),
    T.StructField("anios_afectados", T.ArrayType(T.IntegerType())),
    T.StructField("filas", T.LongType()),
    T.StructField("fecha_proceso", T.TimestampType()),
])


def argumentos(descripcion):
    parser = argparse.ArgumentParser(description=descripcion)
    parser.add_argument("--base", required=True)
    parser.add_argument("--shuffle", type=int, default=8)
    return parser


def crear_spark(nombre, shuffle):
    return (SparkSession.builder.appName(nombre)
            .config("spark.sql.shuffle.partitions", shuffle)
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .getOrCreate())


def ruta(base, *partes):
    return "/".join([base.rstrip("/"), *partes])


def _sistema_archivos(spark, destino):
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(destino)
    return path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), path


def existe(spark, destino):
    fs, path = _sistema_archivos(spark, destino)
    return fs.exists(path)


def cargas_en_raw(spark, base, fuente):
    fs, path = _sistema_archivos(spark, ruta(base, "raw", fuente, "fecha_carga=*"))
    estados = fs.globStatus(path) or []
    return sorted(e.getPath().getName().split("=", 1)[1] for e in estados if e.isDirectory())


def inicio_anio_epi(anio):
    d = date(anio, 1, 4)
    return d - timedelta(days=(d.weekday() + 1) % 7)


def semanas_del_anio(anio):
    return (inicio_anio_epi(anio + 1) - inicio_anio_epi(anio)).days // 7


ANIOS_53 = [a for a in range(ANIO_INICIO, ANIO_FIN + 1) if semanas_del_anio(a) == 53]


def leer_manifiesto(spark, base):
    destino = ruta(base, "control", "manifiesto")
    if not existe(spark, destino):
        return spark.createDataFrame([], ESQUEMA_MANIFIESTO)
    return spark.read.parquet(destino)


def registrar_en_manifiesto(spark, base, filas):
    registros = [(etapa, fuente, fecha_carga, sorted(anios), int(n), datetime.now(timezone.utc))
                 for etapa, fuente, fecha_carga, anios, n in filas]
    (spark.createDataFrame(registros, ESQUEMA_MANIFIESTO)
          .coalesce(1).write.mode("append").parquet(ruta(base, "control", "manifiesto")))


def cargas_pendientes(spark, base, etapa_previa, etapa, fuentes):
    manifiesto = leer_manifiesto(spark, base)
    hechas = {(r["fuente"], r["fecha_carga"])
              for r in manifiesto.filter(F.col("etapa") == etapa).collect()}
    previas = (manifiesto.filter((F.col("etapa") == etapa_previa) & F.col("fuente").isin(fuentes))
               .select("fuente", "fecha_carga", "anios_afectados").collect())
    return sorted([r for r in previas if (r["fuente"], r["fecha_carga"]) not in hechas],
                  key=lambda r: (r["fecha_carga"], r["fuente"]))


class Cronometro:
    def __init__(self):
        self.tiempos = []

    @contextmanager
    def etapa(self, nombre):
        inicio = time.perf_counter()
        yield
        segundos = round(time.perf_counter() - inicio, 2)
        self.tiempos.append({"etapa": nombre, "segundos": segundos})
        print(f"[{nombre}] {segundos:.2f} s", flush=True)


def escribir_json(spark, destino, datos):
    texto = json.dumps(datos, ensure_ascii=False, default=str)
    spark.createDataFrame([(texto,)], ["value"]).coalesce(1).write.mode("overwrite").text(destino)


def leer_json(spark, destino):
    return json.loads(spark.read.text(destino).first()["value"])


def escribir_evidencia(spark, base, paso, datos):
    ahora = datetime.now(timezone.utc)
    registro = {"paso": paso, "fecha_utc": ahora.isoformat(timespec="seconds"),
                "spark": spark.version, "master": spark.sparkContext.master, **datos}
    escribir_json(spark, ruta(base, "evidencias", paso, f"ejecucion={ahora:%Y%m%dT%H%M%SZ}"), registro)
    print(json.dumps(registro, ensure_ascii=False, indent=2, default=str), flush=True)
