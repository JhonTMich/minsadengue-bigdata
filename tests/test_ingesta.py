"""Pruebas sin Spark ni red: python -m unittest discover -s tests."""

import contextlib
import io
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "nube" / "demo"))
sys.path.insert(0, str(RAIZ / "nube" / "infra"))

import descargar_clima as clima
import desplegar as d
import dividir_cargas as dc

CABECERA_2024 = "﻿departamento;provincia;ano;semana\n"
CABECERA_2025 = "departamento,provincia,ano,semana\n"


class PruebasIngesta(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)

    def test_separador_de_ambos_formatos_del_minsa(self):
        self.assertEqual(dc.separador(CABECERA_2024), ";")
        self.assertEqual(dc.separador(CABECERA_2025), ",")

    def test_dividir_con_comillas_y_comas_dentro_del_campo(self):
        origen = self.base / "minsa.csv"
        origen.write_text(CABECERA_2025 + '"LA LIBERTAD","TRUJILLO, CENTRO",2024,52\n'
                          '"SAN MARTIN",MOYOBAMBA,2025,1\n', encoding="utf-8")
        final, anios = dc.dividir(origen, self.base / "raw", "dengue", lambda a: a > 2024)
        self.assertEqual((final.name, anios), ("vigilancia_dengue_2025_2025.csv", 1))
        self.assertEqual(final.read_text(encoding="utf-8"),
                         CABECERA_2025 + '"SAN MARTIN",MOYOBAMBA,2025,1\n')

    def test_dividir_formato_2024_quita_bom(self):
        origen = self.base / "minsa.csv"
        origen.write_text(CABECERA_2024 + "LORETO;MAYNAS;2023;5\nLORETO;MAYNAS;2024;1\n", encoding="utf-8")
        final, _ = dc.dividir(origen, self.base / "raw", "dengue", lambda a: a <= 2023)
        self.assertEqual(final.read_text(encoding="utf-8"), CABECERA_2024.lstrip("﻿") + "LORETO;MAYNAS;2023;5\n")

    def test_anio_epidemiologico_2025_tiene_53_semanas(self):
        desde, hasta = clima.rango_anio_epi(2025)
        self.assertEqual((desde, hasta), (date(2024, 12, 29), date(2026, 1, 3)))
        self.assertEqual(((hasta - desde).days + 1) / 7, 53)

    def test_clima_incompleto_se_rechaza(self):
        desde, hasta = date(2025, 1, 1), date(2025, 1, 3)
        serie = {v: {"20250101": 1.0, "20250102": 2.0} for v in clima.VARIABLES}
        with self.assertRaises(ValueError):
            clima.filas("01", "AMAZONAS", {"properties": {"parameter": serie}}, desde, hasta)
        serie = {v: {"20250101": 1.0, "20250102": 2.0, "20250103": -999} for v in clima.VARIABLES}
        filas = clima.filas("01", "AMAZONAS", {"properties": {"parameter": serie}}, desde, hasta)
        self.assertEqual(filas[-1], ["01", "AMAZONAS", "20250103", -999, -999, -999])

    def test_simulacion_con_estado_de_la_corrida_anterior(self):
        from test_desplegar import CONFIG
        deploy = d.Despliegue(CONFIG.copy(), self.base / "e", dry_run=True)
        deploy.state.update({"cluster_id": "j-viejo", "step_ids": ["s-1"], "release": "abc"})
        with patch.object(deploy, "identidad"), contextlib.redirect_stdout(io.StringIO()):
            deploy.lanzar("carga")
        real = d.Despliegue(CONFIG.copy(), self.base / "r")
        real.state.update({"cluster_id": "j-viejo", "step_ids": ["s-1"], "release": "abc"})
        activo = {"Status": {"State": "WAITING"}}
        with patch.object(real, "identidad"), patch.object(real, "cluster", return_value=("j-viejo", activo)), \
             self.assertRaises(ValueError):
            real.lanzar("carga")

    def test_raw_sube_tambien_los_json_originales(self):
        for fuente in ("dengue", "clima"):
            carpeta = self.base / f"raw/{fuente}/fecha_carga=2026-11-28"
            carpeta.mkdir(parents=True)
            (carpeta / "a.csv").write_text("x\n1\n")
        (self.base / "raw/clima/fecha_carga=2026-11-28/nasa_power_01.json").write_text("{}")
        claves = [k for _, k in d.archivos_raw(self.base, "2026-11-28")]
        self.assertIn("datalake/raw/clima/fecha_carga=2026-11-28/nasa_power_01.json", claves)
        self.assertEqual(len(claves), 3)


if __name__ == "__main__":
    unittest.main()
