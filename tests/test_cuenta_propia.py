import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "nube" / "infra"))

import costos
import cuenta_propia as cuenta
import desplegar
from test_desplegar import CONFIG


def precios_ejemplo():
    # Tarifas sintéticas para probar cálculos; no son una cotización AWS.
    return {"region": CONFIG["AWS_REGION"], "instancia": CONFIG["INSTANCIA"],
            "tarifas": {k: {"usd": v, "unidad": u} for k, v, u in [
                ("ec2_hora", "0.2", "Hrs"), ("emr_hora", "0.05", "Hrs"),
                ("ebs_gib_mes", "0.1", "GB-Mo"), ("ipv4_hora", "0.005", "Hrs")]}}

class PruebasCuentaPropia(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.deploy = desplegar.Despliegue(CONFIG.copy(), self.base)
        self.stdout = contextlib.redirect_stdout(io.StringIO())
        self.stdout.__enter__()
        self.addCleanup(self.stdout.__exit__, None, None, None)

    def test_estimar_compara_dos_y_cuatro_nodos(self):
        p = precios_ejemplo()
        dos = costos.calcular(p, CONFIG["AWS_REGION"], CONFIG["INSTANCIA"], 2, 1)
        cuatro = costos.calcular(p, CONFIG["AWS_REGION"], CONFIG["INSTANCIA"], 4, 1)
        self.assertAlmostEqual(float(cuatro["total_parcial_usd"]), float(dos["total_parcial_usd"]) * 2, places=5)
        self.assertGreater(float(dos["total_parcial_usd"]), 0.51)
        self.assertEqual(dos["gib_por_nodo"], 47)
        self.assertIn("S3 y sus versiones/solicitudes", dos["excluye"])

    def test_tarifas_no_mezclan_region_instancia_o_unidades(self):
        p = precios_ejemplo()
        with self.assertRaisesRegex(ValueError, "región"):
            costos.calcular(p, "us-west-2", CONFIG["INSTANCIA"], 2, 1)
        p["tarifas"]["ebs_gib_mes"]["unidad"] = "Hrs"
        with self.assertRaisesRegex(ValueError, "Tarifa inválida"):
            costos.calcular(p, CONFIG["AWS_REGION"], CONFIG["INSTANCIA"], 2, 1)

    def test_no_usa_tarifa_ausente_o_ambigua_como_cero(self):
        with self.assertRaisesRegex(ValueError, "ausente"):
            costos.tarifa([], "Hrs")
        producto = {"product": {"sku": "ejemplo"}, "terms": {"OnDemand": {"t": {
            "priceDimensions": {"p": {"unit": "Hrs", "pricePerUnit": {"USD": "0.2"}}}}}}}
        self.assertEqual(costos.tarifa([json.dumps(producto)], "Hrs")["usd"], "0.2")
        with self.assertRaisesRegex(ValueError, "ambigua"):
            costos.tarifa([producto, producto], "Hrs")

    def test_politica_de_datos_limitada_a_bucket_del_proyecto(self):
        policy = cuenta.politica_ec2(CONFIG["BUCKET"])
        for statement in policy["Statement"]:
            if any(a.startswith("s3:") for a in statement["Action"]):
                resources = statement["Resource"]
                for resource in [resources] if isinstance(resources, str) else resources:
                    self.assertTrue(resource.startswith("arn:aws:s3:::minsadengue-prueba"))
        self.assertNotIn("iam:PassRole", json.dumps(policy))

    def test_no_modifica_roles_ajenos(self):
        with patch.object(self.deploy, "aws", return_value={"Role": {"Tags": []}}) as aws:
            with self.assertRaisesRegex(ValueError, "no pertenece"):
                cuenta._rol(self.deploy, "existente", "ec2.amazonaws.com")
        self.assertEqual(aws.call_count, 1)

    def test_no_silencia_denegacion_de_iam(self):
        with patch.object(self.deploy, "aws", side_effect=desplegar.ErrorAWS("AccessDenied")) as aws:
            with self.assertRaises(desplegar.ErrorAWS):
                cuenta._rol(self.deploy, "existente", "ec2.amazonaws.com")
        self.assertEqual(aws.call_count, 1)

    def test_crea_rol_ausente_con_confianza_y_etiquetas(self):
        with patch.object(self.deploy, "aws", side_effect=[desplegar.ErrorAWS("NoSuchEntity"), {}]) as aws:
            cuenta._rol(self.deploy, "nuevo", "ec2.amazonaws.com")
        args = aws.call_args.args
        self.assertEqual(args[:2], ("iam", "create-role"))
        policy = json.loads(args[args.index("--assume-role-policy-document") + 1])
        self.assertEqual(policy["Statement"][0]["Principal"], {"Service": "ec2.amazonaws.com"})

    def test_preparar_no_modifica_subred_proporcionada(self):
        with patch.object(self.deploy, "identidad"), patch.object(self.deploy, "aws") as aws:
            with self.assertRaisesRegex(ValueError, "redes ajenas"):
                cuenta.preparar(self.deploy)
        aws.assert_not_called()

    def test_preparacion_dry_run_no_crea_nat_ec2_o_cluster(self):
        self.deploy.dry_run = True
        self.deploy.c["SUBNET_ID"] = ""
        calls = []
        def aws(*args, **kwargs):
            calls.append(args)
            return {}
        with patch.object(self.deploy, "aws", side_effect=aws):
            cuenta.preparar(self.deploy)
        operaciones = [c[:2] for c in calls]
        self.assertIn(("ec2", "create-vpc"), operaciones)
        self.assertIn(("ec2", "create-internet-gateway"), operaciones)
        self.assertNotIn(("ec2", "create-nat-gateway"), operaciones)
        self.assertNotIn(("ec2", "run-instances"), operaciones)
        self.assertNotIn(("emr", "create-cluster"), operaciones)
        self.assertFalse(self.deploy.state_path.exists())

    def test_red_retoma_recursos_existentes_sin_duplicarlos(self):
        self.deploy.dry_run = True
        cuenta.preparar_red(self.deploy)
        with patch.object(self.deploy, "aws", return_value={}) as aws:
            cuenta.preparar_red(self.deploy)
        self.assertFalse(any(c.args[1].startswith("create-") and c.args[1] != "create-route"
                             for c in aws.call_args_list))

    def test_config_inicial_permite_subred_vacia_solo_en_preparacion(self):
        archivo = self.base / "entorno.env"
        config = dict(CONFIG, SUBNET_ID="")
        archivo.write_text("\n".join(f"{k}={v}" for k, v in config.items()))
        self.assertEqual(desplegar.cargar_config(archivo, requiere_infra=False)["SUBNET_ID"], "")
        with self.assertRaisesRegex(ValueError, "SUBNET_ID"):
            desplegar.cargar_config(archivo)


if __name__ == "__main__":
    unittest.main()