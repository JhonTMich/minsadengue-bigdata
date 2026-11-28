import importlib.util
import json
import re
import unittest
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
CARPETA = RAIZ / "nube" / "tablero" / "grafana"
PERMITIDAS = {"v_departamentos_en_alerta", "v_ranking_riesgo", "v_casos_vs_canal",
              "v_desempeno_alertas", "dim_departamento"}


def cargar_generador():
    spec = importlib.util.spec_from_file_location("generar_tablero", CARPETA / "generar_tablero.py")
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


class TableroGrafanaTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.gen = cargar_generador()
        cls.json = json.loads((CARPETA / "tablero_dengue.json").read_text(encoding="utf-8"))

    def test_json_al_dia_con_el_generador(self):
        self.assertEqual(self.json, json.loads(json.dumps(self.gen.tablero(), ensure_ascii=False)))

    def test_solo_lee_vistas_de_indicadores(self):
        sqls = list(self.gen.CONSULTAS.values()) + list(self.gen.VARIABLES_SQL.values())
        for sql in sqls:
            objetos = set(re.findall(r"minsadengue_gold\.(\w+)", sql))
            self.assertTrue(objetos, sql)
            self.assertLessEqual(objetos, PERMITIDAS, sql)

    def test_paneles_usan_el_origen_importado(self):
        paneles = [p for p in self.json["panels"] if p["type"] != "row"]
        self.assertEqual(len(paneles), 8)
        for panel in paneles:
            self.assertEqual(panel["datasource"]["uid"], "${DS_ATHENA}")
            for objetivo in panel["targets"]:
                self.assertEqual(objetivo["datasource"]["uid"], "${DS_ATHENA}")
                self.assertIn("${ano}", objetivo["rawSQL"])

    def test_ids_y_posiciones_unicos(self):
        ids = [p["id"] for p in self.json["panels"]]
        self.assertEqual(len(ids), len(set(ids)))
        for panel in self.json["panels"]:
            pos = panel["gridPos"]
            self.assertLessEqual(pos["x"] + pos["w"], 24)

    def test_sin_credenciales(self):
        texto = (CARPETA / "tablero_dengue.json").read_text(encoding="utf-8")
        self.assertNotRegex(texto, r"AKIA[0-9A-Z]{16}|secretKey|accessKey")


if __name__ == "__main__":
    unittest.main()
