"""Pruebas de contrato y no interferencia temporal; no generan datos de análisis."""
import os
import sys
import unittest
from pathlib import Path
from datetime import date,timedelta
ROOT=Path(__file__).resolve().parents[1]
SPARK=Path(os.environ.get('SPARK_HOME','/usr/local/spark'))
sys.path[:0]=[str(ROOT/'scripts'),str(SPARK/'python'),*map(str,(SPARK/'python/lib').glob('py4j-*-src.zip'))]
from pyspark.sql import SparkSession,functions as F
from ingenieria_variables import inicio_epi,calendario,validar_gold,preparar,CASOS,ESTACION,CLIMA

class IngenieriaTemporal(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark=(SparkSession.builder.master('local[2]').appName('PruebasS9')
                   .config('spark.driver.memory','2g').config('spark.sql.shuffle.partitions',8).getOrCreate())
        cls.gold=cls.spark.read.parquet(str(ROOT/'data/gold/panel_dengue_clima')).cache()
    @classmethod
    def tearDownClass(cls):
        cls.gold.unpersist();cls.spark.stop()
    def test_calendario_en_limites_de_anio(self):
        self.assertEqual(inicio_epi(2003),date(2002,12,29))
        c=calendario(self.spark,2004)
        self.assertEqual(c.filter('ano=2003 and semana=53').first().fecha,date(2003,12,28))
        self.assertEqual(inicio_epi(2004),date(2004,1,4))
    def test_rechaza_duplicados_nulos_y_semanas_invalidas(self):
        with self.assertRaisesRegex(ValueError,'duplicadas'):
            validar_gold(self.gold.unionByName(self.gold.filter('ano=2010').limit(1)),2017)
        with self.assertRaisesRegex(ValueError,'nulos'):
            validar_gold(self.gold.withColumn('hum_media',F.lit(None).cast('double')),2017)
        with self.assertRaisesRegex(ValueError,'Semana inexistente'):
            validar_gold(self.gold.withColumn('semana',F.when((F.col('ano')==2015)&(F.col('semana')==52),53).otherwise(F.col('semana'))),2017)
        with self.assertRaisesRegex(ValueError,'faltantes'):
            validar_gold(self.gold.filter(~((F.col('ano')==2010)&(F.col('semana')==1))),2017)
        with self.assertRaisesRegex(ValueError,'valores inválidos'):
            validar_gold(self.gold.withColumn('precip_total',F.lit(float('inf'))),2017)
    def test_futuro_no_modifica_entrenamiento(self):
        import pandas as pd
        a,_,pa=preparar(self.gold,2014,2017)
        futuro=(self.gold.withColumn('casos',F.when(F.col('ano')>=2015,F.col('casos')*10).otherwise(F.col('casos')))
                .withColumn('temp_media',F.when(F.col('ano')>=2015,F.lit(30.0)).otherwise(F.col('temp_media'))))
        b,_,pb=preparar(futuro,2014,2017)
        self.assertEqual(pa['departamentos'],pb['departamentos'])
        cols=['cod_dep','ano','semana','fecha','label']+CASOS+ESTACION+CLIMA
        x=a.select(*cols).orderBy('cod_dep','ano','semana').toPandas()
        y=b.select(*cols).orderBy('cod_dep','ano','semana').toPandas()
        pd.testing.assert_frame_equal(x,y,check_exact=False,atol=1e-9,rtol=0)
        self.assertLess(x.fecha.max()+timedelta(weeks=4),inicio_epi(2015))

if __name__=='__main__':unittest.main(verbosity=2)
