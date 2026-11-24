"""Pruebas del modelo de la Ruta B. Dentro del contenedor:
PYTHONPATH=nube spark-submit --master local[2] tests/test_modelo_nube.py"""
import random
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "nube"))

from pyspark.sql import SparkSession, functions as F
from sklearn.metrics import average_precision_score, roc_auc_score

from pipeline.metricas import auc, precision_promedio, umbral_por_costo
from pipeline.paso_04_modelo import particion
from pipeline.paso_05_alertas import anticipacion, completar_etiquetas
from pipeline.variables import TODAS, con_fecha, etiquetar, variables_base

GOLD = ROOT / "data/_nube/gold"


class Metricas(unittest.TestCase):
    def test_igual_a_sklearn_con_empates(self):
        azar = random.Random(7)
        etiquetas = [azar.randint(0, 1) for _ in range(500)]
        puntajes = [round(azar.random(), 1) for _ in etiquetas]
        self.assertAlmostEqual(auc(etiquetas, puntajes), roc_auc_score(etiquetas, puntajes), places=12)
        self.assertAlmostEqual(precision_promedio(etiquetas, puntajes),
                               average_precision_score(etiquetas, puntajes), places=12)

    def test_umbral_por_costo(self):
        etiquetas, puntajes = [0, 0, 1, 1], [0.1, 0.6, 0.4, 0.9]
        self.assertEqual(umbral_por_costo(etiquetas, puntajes, 10), 0.4)
        self.assertEqual(umbral_por_costo(etiquetas, puntajes, 0.5), 0.9)


@unittest.skipUnless((GOLD / "panel_dengue_clima").exists(), "Requiere Gold local en data/_nube")
class SinFugaTemporal(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[2]").config("spark.sql.shuffle.partitions", 8).getOrCreate()
        cls.panel = cls.spark.read.parquet(str(GOLD / "panel_dengue_clima")).cache()
        cls.semanas = cls.spark.read.parquet(str(GOLD / "dim_semana"))

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def entrenamiento(self, panel, fin_train):
        base = variables_base(etiquetar(con_fecha(panel, self.semanas)))
        train, _, _ = particion(base, fin_train)
        return (train.select("cod_dep", "ano", "semana", "label", *TODAS)
                .orderBy("cod_dep", "ano", "semana").collect())

    def test_futuro_no_modifica_entrenamiento(self):
        alterado = (self.panel
                    .withColumn("casos", F.when(F.col("ano") > 2015, F.col("casos") * 10 + 50)
                                .otherwise(F.col("casos")))
                    .withColumn("temp_media", F.when(F.col("ano") > 2015, F.lit(35.0))
                                .otherwise(F.col("temp_media"))))
        original = self.entrenamiento(self.panel, 2015)
        self.assertTrue(original)
        self.assertEqual(original, self.entrenamiento(alterado, 2015))

    def test_ultimas_semanas_sin_etiqueta(self):
        base = etiquetar(con_fecha(self.panel.filter("ano <= 2015"), self.semanas))
        ultimas = base.filter("ano = 2015 and semana > 48").select("label").distinct().collect()
        self.assertEqual([r.label for r in ultimas], [None])


class Alertas(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[2]").config("spark.sql.shuffle.partitions", 2).getOrCreate()

    def test_anticipacion(self):
        from datetime import date, timedelta
        brote = [0, 0, 0, 0, 0, 1, 1, 0, 0, 1]
        alerta = [True, False, False, True, True, True, True, True, True, False]
        filas = [("01", date(2024, 1, 7) + timedelta(weeks=i), b, a, False)
                 for i, (b, a) in enumerate(zip(brote, alerta))]
        df = self.spark.createDataFrame(filas, "cod_dep string, fecha_inicio date, brote_actual int, "
                                               "alerta boolean, dentro_entrenamiento boolean")
        resultado = anticipacion(df)
        self.assertEqual(resultado["brotes_iniciados"], 2)
        self.assertEqual(resultado["distribucion"], {0: 0, 1: 0, 2: 1, 3: 0, 4: 1})
        self.assertEqual(resultado["con_alerta_previa"], 2)
        self.assertEqual(resultado["mediana_semanas"], 3)

    def test_completar_etiquetas_sin_pisar_las_conocidas(self):
        with tempfile.TemporaryDirectory() as tmp:
            destino = f"{tmp}/alertas"
            (self.spark.createDataFrame([("01", 2023, 51, 1), ("01", 2023, 52, None)],
                                        "cod_dep string, ano int, semana int, brote_observado int")
                 .write.partitionBy("ano").parquet(destino))
            puntuado = self.spark.createDataFrame([("01", 2023, 51, 0.0), ("01", 2023, 52, 0.0)],
                                                  "cod_dep string, ano int, semana int, label double")
            df, completadas = completar_etiquetas(self.spark, destino, 2023, puntuado)
            valores = {r.semana: r.brote_observado for r in df.collect()}
        self.assertEqual(completadas, 1)
        self.assertEqual(valores, {51: 1, 52: 0})


if __name__ == "__main__":
    unittest.main(verbosity=2)
