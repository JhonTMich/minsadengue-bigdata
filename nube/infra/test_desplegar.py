"""Pruebas sin credenciales ni recursos cloud: python -m unittest discover -s nube/infra."""

import contextlib
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import desplegar as d

CONFIG = {"AWS_REGION": "us-east-1", "EXPECTED_ACCOUNT_ID": "123456789012",
          "BUCKET": "minsadengue-prueba", "EMR_RELEASE": "emr-7.1.0", "INSTANCIA": "m5.xlarge",
          "NODOS_CORE": "1", "ROL_SERVICIO_EMR": "rol-lab", "PERFIL_INSTANCIA_EMR": "perfil-lab",
          "SUBNET_ID": "subnet-abc123", "IDLE_TIMEOUT": "900", "SHUFFLE_PARTITIONS": "8"}


class PruebasInfra(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.deploy = d.Despliegue(CONFIG.copy(), self.base / "estado")
        self.stdout = contextlib.redirect_stdout(io.StringIO())
        self.stdout.__enter__()
        self.addCleanup(self.stdout.__exit__, None, None, None)

    def test_config_no_ejecuta_shell_y_rechaza_credenciales(self):
        archivo = self.base / "entorno.env"
        archivo.write_text("AWS_SECRET_ACCESS_KEY=no-es-una-clave-real\n")
        with self.assertRaisesRegex(ValueError, "no guardar credenciales"):
            d.cargar_config(archivo)
        archivo.write_text("\n".join(f"{k}={v}" for k, v in CONFIG.items()))
        self.assertEqual(d.cargar_config(archivo), CONFIG)

    def test_otra_cuenta_no_crea_bucket(self):
        with patch.object(self.deploy, "aws", return_value={"Account": "999999999999"}) as aws:
            with self.assertRaisesRegex(ValueError, "otra cuenta"):
                self.deploy.bucket()
            self.assertEqual(aws.call_args_list, [unittest.mock.call("sts", "get-caller-identity")])

    def test_estado_no_se_reutiliza_en_otra_cuenta(self):
        self.deploy.guardar()
        otro = dict(CONFIG, EXPECTED_ACCOUNT_ID="999999999999")
        with self.assertRaisesRegex(ValueError, "otra cuenta"):
            d.Despliegue(otro, self.deploy.salida)

    def test_zip_importa_el_paquete_y_no_incluye_datos(self):
        datos, sha = d.paquete()
        self.assertEqual(d.paquete(), (datos, sha))
        with zipfile.ZipFile(io.BytesIO(datos)) as archivo:
            self.assertEqual(set(archivo.namelist()), {f"pipeline/{f}" for f in d.FILES})
            self.assertIn(b"def crear_spark", archivo.read("pipeline/comun.py"))

    def test_pasos_son_secuenciales_y_yarn_con_dependencias(self):
        pasos = d.pasos(CONFIG["BUCKET"], "abc", 8, "completa")
        self.assertEqual([p["Name"] for p in pasos], [f.removesuffix(".py") for f in d.PASOS])
        for paso in pasos:
            self.assertEqual(paso["ActionOnFailure"], "TERMINATE_CLUSTER")
            args = paso["Args"]
            self.assertEqual(paso["Type"], "CUSTOM_JAR")
            self.assertLessEqual(set(paso), {"Type", "Name", "ActionOnFailure", "Jar", "Args", "MainClass", "Properties", "LogUri", "EncryptionKeyArn"})
            self.assertEqual(args[args.index("--master") + 1], "yarn")
            self.assertEqual(args[args.index("--py-files") + 1], "s3://minsadengue-prueba/codigo/abc/pipeline.zip")

    def test_corrida_de_carga_no_reentrena(self):
        nombres = [p["Name"] for p in d.pasos(CONFIG["BUCKET"], "abc", 8, "carga")]
        self.assertEqual(nombres, ["paso_01_bronze", "paso_02_silver", "paso_03_gold", "paso_05_alertas"])

    def test_zip_incluye_modulos_del_modelo(self):
        with zipfile.ZipFile(io.BytesIO(d.paquete()[0])) as archivo:
            self.assertTrue({"pipeline/variables.py", "pipeline/metricas.py"} <= set(archivo.namelist()))

    def test_lanzar_registra_la_corrida(self):
        self.deploy.state["release"] = "abc"
        with patch.object(self.deploy, "identidad"), \
             patch.object(self.deploy, "cluster", return_value=("j-a", {"Status": {"State": "WAITING"}})), \
             patch.object(self.deploy, "aws", return_value={"StepIds": ["s-1", "s-2", "s-3", "s-5"]}) as aws:
            self.deploy.lanzar("carga")
        args = aws.call_args.args
        self.assertEqual(len(json.loads(args[args.index("--steps") + 1])), 4)
        self.assertEqual(self.deploy.state["corrida"], "carga")

    def test_creacion_registra_id_y_configura_dos_nodos_con_logs(self):
        self.deploy.state["release"] = "abc"
        self.deploy.state["bootstrap"] = "codigo/bootstrap/def.sh"
        with patch.object(self.deploy, "comprobar"), patch.object(self.deploy, "bucket_propio"), \
             patch.object(d, "estimar", return_value={"escenarios": [{"nodos_totales": 2, "total_parcial_usd": "0.5"}]}), \
             patch.object(self.deploy, "aws", return_value={"ClusterId": "j-prueba"}) as aws:
            self.deploy.crear_cluster()
        args = aws.call_args.args
        grupos = json.loads(args[args.index("--instance-groups") + 1])
        self.assertEqual(sum(g["InstanceCount"] for g in grupos), 2)
        self.assertEqual([g["InstanceGroupType"] for g in grupos], ["MASTER", "CORE"])
        permitidas = {"Name", "InstanceGroupType", "BidPrice", "InstanceType", "InstanceCount",
                      "CustomAmiId", "EbsConfiguration", "AutoScalingPolicy", "Configurations"}
        self.assertTrue(all(set(g) <= permitidas for g in grupos))
        self.assertEqual(args[args.index("--step-concurrency-level") + 1], "1")
        self.assertIn("s3://minsadengue-prueba/logs/emr/", args)
        self.assertEqual(args[args.index("--ebs-root-volume-size") + 1], "15")
        self.assertEqual(grupos[0]["EbsConfiguration"]["EbsBlockDeviceConfigs"][0]["VolumeSpecification"],
                         {"VolumeType": "gp3", "SizeInGB": 32})
        self.assertEqual(json.loads(self.deploy.state_path.read_text())["cluster_id"], "j-prueba")
        acciones = json.loads(args[args.index("--bootstrap-actions") + 1])
        self.assertEqual(acciones, [{"Name": "instalar-numpy", "Path": "s3://minsadengue-prueba/codigo/bootstrap/def.sh"}])

    def test_no_crea_cluster_sin_bootstrap_de_numpy(self):
        self.deploy.state["release"] = "abc"
        with patch.object(self.deploy, "comprobar"), patch.object(self.deploy, "bucket_propio"), \
             patch.object(self.deploy, "aws") as aws:
            with self.assertRaisesRegex(ValueError, "numpy"):
                self.deploy.crear_cluster()
        aws.assert_not_called()

    def test_no_duplica_cluster_activo(self):
        self.deploy.state.update(release="abc", bootstrap="codigo/bootstrap/def.sh", cluster_id="j-prueba")
        with patch.object(self.deploy, "comprobar"), patch.object(self.deploy, "bucket_propio"), \
             patch.object(self.deploy, "cluster", return_value=("j-prueba", {"Status": {"State": "WAITING"}})), \
             patch.object(self.deploy, "aws") as aws:
            self.deploy.crear_cluster()
        aws.assert_not_called()

    def test_no_crea_cluster_si_estimacion_supera_umbral(self):
        self.deploy.state["release"] = "abc"
        self.deploy.state["bootstrap"] = "codigo/bootstrap/def.sh"
        with patch.object(self.deploy, "comprobar"), patch.object(self.deploy, "bucket_propio"), \
             patch.object(d, "estimar", return_value={"escenarios": [{"nodos_totales": 2, "total_parcial_usd": "5"}]}), \
             patch.object(self.deploy, "aws") as aws:
            with self.assertRaisesRegex(ValueError, "supera"):
                self.deploy.crear_cluster(max_usd=2)
        aws.assert_not_called()

    def test_no_sube_raw_si_falta_una_fuente(self):
        carpeta = self.base / "raw/dengue/fecha_carga=2026-11-17"
        carpeta.mkdir(parents=True)
        (carpeta / "dengue.csv").write_text("cabecera\nregistro\n")
        with self.assertRaisesRegex(ValueError, "clima"):
            d.archivos_raw(self.base, "2026-11-17")

    def test_raw_distinto_no_se_sobrescribe(self):
        archivo = self.base / "casos.csv"
        archivo.write_text("version corregida")
        calls = []
        def aws(*args):
            calls.append(args)
            if args[:2] == ("s3api", "head-object"):
                return {"Metadata": {"sha256": "otro-hash"}}
            return {}
        with patch.object(self.deploy, "aws", side_effect=aws):
            with self.assertRaisesRegex(ValueError, "Contenido distinto"):
                self.deploy.subir_inmutables([(archivo, "datalake/raw/dengue/fecha_carga=2026-11-17/a.csv")])
        self.assertFalse(any(c[:2] == ("s3", "cp") for c in calls))

    def test_raw_identico_no_se_vuelve_a_subir(self):
        archivo = self.base / "casos.csv"
        archivo.write_text("contenido")
        with patch.object(self.deploy, "aws", return_value={"Metadata": {"sha256": d.sha_archivo(archivo)}}) as aws:
            self.deploy.subir_inmutables([(archivo, "raw/casos.csv")])
        self.assertFalse(any(c.args[:2] == ("s3", "cp") for c in aws.call_args_list))

    def test_error_403_no_intenta_crear_bucket(self):
        def aws(*args):
            if args[0] == "sts":
                return {"Account": CONFIG["EXPECTED_ACCOUNT_ID"]}
            raise d.ErrorAWS("An error occurred (403) Forbidden")
        with patch.object(self.deploy, "aws", side_effect=aws) as llamada:
            with self.assertRaises(d.ErrorAWS):
                self.deploy.bucket()
        self.assertFalse(any(c.args[:2] == ("s3api", "create-bucket") for c in llamada.call_args_list))

    def test_fallo_de_step_solicita_apagado(self):
        self.deploy.state["step_ids"] = ["s-a", "s-b", "s-c"]
        with patch.object(self.deploy, "estado", return_value=[{"Step": {"Status": {"State": "FAILED"}}}]), \
             patch.object(self.deploy, "apagar") as apagar:
            with self.assertRaisesRegex(ValueError, "Falló"):
                self.deploy.esperar(100)
            apagar.assert_called_once()

    def test_exito_de_todos_los_steps_solicita_apagado(self):
        self.deploy.state["step_ids"] = ["s-1", "s-2", "s-3", "s-4", "s-5"]
        with patch.object(self.deploy, "estado", return_value=[{"Step": {"Status": {"State": "COMPLETED"}}}] * 5), \
             patch.object(self.deploy, "evidencias") as evidencias, \
             patch.object(self.deploy, "apagar") as apagar:
            self.deploy.esperar(100)
            evidencias.assert_called_once()
            apagar.assert_called_once()

    def test_timeout_solicita_apagado(self):
        self.deploy.state["step_ids"] = ["s-a", "s-b", "s-c"]
        with patch.object(self.deploy, "estado", return_value=[{"Step": {"Status": {"State": "RUNNING"}}}]), \
             patch.object(d.time, "monotonic", side_effect=[0, 100]), \
             patch.object(self.deploy, "apagar") as apagar:
            with self.assertRaises(TimeoutError):
                self.deploy.esperar(1)
            apagar.assert_called_once()

    def test_lanzamiento_no_se_duplica(self):
        self.deploy.state["step_ids"] = ["s-a", "s-b", "s-c"]
        with patch.object(self.deploy, "identidad"), \
             patch.object(self.deploy, "cluster", return_value=("j-a", {"Status": {"State": "WAITING"}})), \
             patch.object(self.deploy, "aws") as aws:
            with self.assertRaisesRegex(ValueError, "Ya se enviaron"):
                self.deploy.lanzar("carga")
            aws.assert_not_called()

    def test_dry_run_no_invoca_aws_ni_guarda_estado(self):
        deployment = d.Despliegue(CONFIG.copy(), self.base / "dry", dry_run=True)
        with patch.object(d.subprocess, "run", side_effect=AssertionError("No debe invocar AWS")), \
             patch.dict(d.os.environ, {}, clear=True):
            deployment.bucket()
            deployment.crear_cluster()
            deployment.lanzar("completa")
            deployment.apagar()
        self.assertFalse(deployment.state_path.exists())

    def test_emulador_no_se_presenta_como_cloud(self):
        with patch.dict(d.os.environ, {"AWS_ENDPOINT_URL": "https://127.0.0.1:9297"}):
            with self.assertRaisesRegex(ValueError, "AWS real"):
                self.deploy.identidad()


if __name__ == "__main__":
    unittest.main()
