"""Pruebas sin credenciales: python -m unittest discover -s nube/infra."""

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import catalogo as c
import desplegar as d
from test_desplegar import CONFIG


class PruebasCatalogo(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.deploy = d.Despliegue(CONFIG.copy(), Path(self.temp.name) / "estado")
        salida = contextlib.redirect_stdout(io.StringIO())
        salida.__enter__()
        self.addCleanup(salida.__exit__, None, None, None)

    def leer(self, archivo):
        return c.sentencias((c.SQL / archivo).read_text(encoding="utf-8"),
                            {"BASE_DATOS": "gold_prueba", "BUCKET": "minsadengue-prueba"})

    def test_tablas_apuntan_a_gold_del_bucket(self):
        tablas = self.leer("01_tablas.sql")
        self.assertTrue(tablas[0].startswith("CREATE DATABASE IF NOT EXISTS gold_prueba"))
        self.assertEqual(len(tablas), 1 + len(c.TABLAS))
        for nombre, sql in zip(c.TABLAS, tablas[1:]):
            self.assertIn(f"gold_prueba.{nombre} (", sql)
            self.assertIn(f"s3://minsadengue-prueba/datalake/gold/{nombre}/", sql)

    def test_particiones_por_anio_con_proyeccion(self):
        for sql in self.leer("01_tablas.sql")[1:3]:
            self.assertIn("PARTITIONED BY (ano int)", sql)
            self.assertIn("'projection.enabled' = 'true'", sql)

    def test_vistas_de_los_cuatro_indicadores(self):
        vistas = self.leer("02_indicadores.sql")
        self.assertEqual([v.split("VIEW gold_prueba.")[1].split()[0] for v in vistas], c.VISTAS)

    def test_marcador_sin_reemplazar_se_rechaza(self):
        with self.assertRaises(ValueError):
            c.sentencias("SELECT 1 FROM {{OTRO}}.t;", {"BASE_DATOS": "x"})

    def test_nombre_de_base_invalido(self):
        with self.assertRaises(ValueError):
            c.base_datos({"GLUE_DATABASE": "Gold-Prueba"})
        self.assertEqual(c.base_datos({}), "minsadengue_gold")

    def test_consulta_fallida_detiene_el_catalogo(self):
        respuestas = [{"QueryExecutionId": "q1"},
                      {"QueryExecution": {"Status": {"State": "FAILED", "StateChangeReason": "sintaxis"}}}]
        with patch.object(self.deploy, "aws", side_effect=respuestas):
            with self.assertRaisesRegex(ValueError, "sintaxis"):
                c.consultar(self.deploy, "SELECT 1", espera=0)

    def test_resultados_cifrados_en_el_bucket_del_proyecto(self):
        with patch.object(self.deploy, "aws", side_effect=[{"QueryExecutionId": "q1"},
                          {"QueryExecution": {"QueryExecutionId": "q1", "Status": {"State": "SUCCEEDED"}}}]) as aws:
            c.consultar(self.deploy, "SELECT 1", espera=0)
        args = aws.call_args_list[0].args
        config = json.loads(args[args.index("--result-configuration") + 1])
        self.assertEqual(config, {"OutputLocation": "s3://minsadengue-prueba/athena/resultados/",
                                  "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}})

    def test_simulacion_no_escribe_evidencia(self):
        deploy = d.Despliegue(CONFIG.copy(), Path(self.temp.name) / "simulacion", dry_run=True)
        c.catalogo(deploy)
        self.assertFalse((Path(self.temp.name) / "simulacion").exists())


if __name__ == "__main__":
    unittest.main()