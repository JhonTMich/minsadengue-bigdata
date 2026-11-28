"""Pruebas sin credenciales: python -m unittest discover -s tests."""

import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "nube" / "infra"))

import benchmark as b
import costos
import desplegar as d
from test_desplegar import CONFIG


def paso(nombre, inicio, fin):
    return {"Step": {"Id": f"s-{nombre}", "Name": nombre, "Status": {
        "State": "COMPLETED", "Timeline": {"StartDateTime": inicio, "EndDateTime": fin}}}}


class PruebasBenchmark(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        salida = contextlib.redirect_stdout(io.StringIO())
        salida.__enter__()
        self.addCleanup(salida.__exit__, None, None, None)

    def test_prefijo_aislado_en_pasos_raw_y_evidencias(self):
        deploy = d.Despliegue(CONFIG.copy(), self.base / "e", prefijo="datalake/benchmark/x/n4")
        args = d.pasos("bkt", "abc", 8, "completa", deploy.prefijo)[0]["Args"]
        self.assertEqual(args[args.index("--base") + 1], "s3://bkt/datalake/benchmark/x/n4")
        for fuente in ("dengue", "clima"):
            carpeta = self.base / f"raw/{fuente}/fecha_carga=2026-11-17"
            carpeta.mkdir(parents=True)
            (carpeta / "a.csv").write_text("x\n1\n")
        claves = [k for _, k in d.archivos_raw(self.base, "2026-11-17", deploy.prefijo)]
        self.assertTrue(all(k.startswith("datalake/benchmark/x/n4/raw/") for k in claves))
        with patch.object(deploy, "identidad"), patch.object(deploy, "bucket_propio"),              patch.object(deploy, "aws") as aws:
            deploy.evidencias()
        self.assertIn("s3://minsadengue-prueba/datalake/benchmark/x/n4/evidencias/", aws.call_args_list[-1].args)

    def test_prefijo_fuera_de_datalake_se_rechaza(self):
        for malo in ("benchmark/n2", "datalake/../codigo", "datalake/N2", "datalake/"):
            with self.assertRaises(ValueError):
                d.Despliegue(CONFIG.copy(), self.base / "e", prefijo=malo)

    def test_medicion_de_tiempos_y_costo(self):
        deploy = d.Despliegue(dict(CONFIG, NODOS_CORE="3"), self.base / "e", prefijo="datalake/benchmark/x/n4")
        deploy.state["estimacion_al_crear"] = {"escenarios": [
            {"nodos_totales": 2, "horas_sesion": 1.0, "total_parcial_usd": "0.5"},
            {"nodos_totales": 4, "horas_sesion": 1.0, "total_parcial_usd": "1.0"}]}
        cluster = {"Id": "j-x", "Status": {"State": "TERMINATED", "Timeline": {
            "CreationDateTime": "2026-11-27T10:00:00-05:00", "ReadyDateTime": "2026-11-27T10:04:00-05:00",
            "EndDateTime": "2026-11-27T10:30:00-05:00"}}}
        steps = [paso("paso_01_bronze", "2026-11-27T10:04:00-05:00", "2026-11-27T10:05:00-05:00"),
                 paso("paso_02_silver", "2026-11-27T10:05:00-05:00", "2026-11-27T10:05:30-05:00")]
        r = b.medir(deploy, cluster, steps, 4)
        self.assertEqual(r["arranque_s"], 240.0)
        self.assertEqual(r["pasos_s"], {"paso_01_bronze": 60.0, "paso_02_silver": 30.0})
        self.assertEqual(r["total_pasos_s"], 90.0)
        self.assertEqual(r["vida_cluster_h"], 0.5)
        self.assertEqual(r["usd_estimado"], 0.5)
        self.assertIn("| Total pasos (s) | 90.0 |", b.tabla([r]))

    def test_configuraciones_en_secuencia_y_bases_separadas(self):
        orden = []

        class Falso:
            def __init__(self, config, salida, dry_run, prefijo):
                self.c, self.prefijo, self.dry_run = config, prefijo, dry_run
                self.state = {"cluster_id": f"j-{config['NODOS_CORE']}"}
            def subir(self, *a): orden.append(("subir", self.prefijo))
            def crear_cluster(self, *a): orden.append(("cluster", self.c["NODOS_CORE"]))
            def lanzar(self, corrida): orden.append(("lanzar", corrida))
            def esperar(self, *a): orden.append(("esperar", self.c["NODOS_CORE"]))
            def estado(self): return []

        with patch.object(b, "Despliegue", Falso), \
             patch.object(b, "esperar_apagado", side_effect=lambda dep, cid: orden.append(("apagado", cid))), \
             patch.object(b, "medir", return_value={"nodos_totales": 2, "pasos_s": {}, "arranque_s": 0,
                          "total_pasos_s": 0, "vida_cluster_h": 0, "usd_estimado": 0}):
            b.benchmark(CONFIG.copy(), self.base, "2026-11-17", salida=self.base / "bench")
        cores = [x[1] for x in orden if x[0] == "cluster"]
        self.assertEqual(cores, ["1", "3"])
        self.assertLess(orden.index(("apagado", "j-1")), orden.index(("cluster", "3")))
        prefijos = [x[1] for x in orden if x[0] == "subir"]
        self.assertEqual(len(set(prefijos)), 2)
        self.assertTrue(all(p.startswith("datalake/benchmark/") for p in prefijos))
        self.assertEqual([x[1] for x in orden if x[0] == "lanzar"], ["completa", "completa"])
        self.assertTrue(list((self.base / "bench").glob("*/resultados.json")))

    def test_fallo_al_lanzar_apaga_el_cluster(self):
        apagado = []

        class Falso:
            def __init__(self, config, salida, dry_run, prefijo):
                self.c, self.prefijo = config, prefijo
            def subir(self, *a): pass
            def crear_cluster(self, *a): pass
            def lanzar(self, corrida): raise ValueError("fallo")
            def apagar(self): apagado.append(True)

        with patch.object(b, "Despliegue", Falso), self.assertRaises(ValueError):
            b.benchmark(CONFIG.copy(), self.base, "2026-11-17", salida=self.base / "bench")
        self.assertEqual(apagado, [True])

    def test_costo_real_suma_por_servicio(self):
        deploy = d.Despliegue(CONFIG.copy(), self.base / "e")
        respuesta = {"ResultsByTime": [
            {"TimePeriod": {"Start": "2026-11-26"}, "Groups": [
                {"Keys": ["Amazon Elastic MapReduce"], "Metrics": {"UnblendedCost": {"Amount": "0.05"}}},
                {"Keys": ["Amazon Elastic Compute Cloud - Compute"], "Metrics": {"UnblendedCost": {"Amount": "0.20"}}}]},
            {"TimePeriod": {"Start": "2026-11-27"}, "Groups": [
                {"Keys": ["Amazon Elastic MapReduce"], "Metrics": {"UnblendedCost": {"Amount": "0.03"}}}]}]}
        with patch.object(deploy, "identidad"), patch.object(deploy, "aws", return_value=respuesta) as aws:
            informe = costos.costo_real(deploy, "2026-11-26", "2026-11-28")
        self.assertEqual(aws.call_args.kwargs["region"], "us-east-1")
        self.assertEqual(informe["por_servicio"]["Amazon Elastic MapReduce"], "0.0800")
        self.assertEqual(informe["por_dia"], {"2026-11-26": "0.2500", "2026-11-27": "0.0300"})
        self.assertEqual(informe["total"], "0.2800")
        self.assertTrue(json.loads((self.base / "e" / "costo_real.json").read_text()))


if __name__ == "__main__":
    unittest.main()
