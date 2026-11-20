"""Punto 2: S3 y EMR para los pasos 1 a 3. Solo requiere Python y AWS CLI v2."""

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from datetime import date, datetime, timezone
import zipfile

ROOT = Path(__file__).resolve().parents[2]
PIPELINE = ROOT / "nube/pipeline"
FILES = ["__init__.py", "comun.py", "paso_01_bronze.py", "paso_02_silver.py", "paso_03_gold.py"]
TERMINALES = {"TERMINATED", "TERMINATED_WITH_ERRORS"}


class ErrorAWS(RuntimeError):
    pass


def cargar_config(path):
    config = {}
    permitidas = {"AWS_REGION", "EXPECTED_ACCOUNT_ID", "BUCKET", "EMR_RELEASE", "INSTANCIA",
                  "NODOS_CORE", "ROL_SERVICIO_EMR", "PERFIL_INSTANCIA_EMR", "SUBNET_ID",
                  "IDLE_TIMEOUT", "SHUFFLE_PARTITIONS", "GLUE_DATABASE"}
    for numero, linea in enumerate(path.read_text(encoding="utf-8-sig").splitlines(), 1):
        linea = linea.strip()
        if not linea or linea.startswith("#"):
            continue
        clave, sep, valor = linea.partition("=")
        if not sep or clave not in permitidas:
            raise ValueError(f"Configuración inválida en línea {numero}; no guardar credenciales aquí.")
        config[clave] = valor.strip().strip('"\'')
    for clave in permitidas - {"GLUE_DATABASE"}:
        if not config.get(clave):
            raise ValueError(f"Completar {clave} en {path}.")
    if not re.fullmatch(r"\d{12}", config["EXPECTED_ACCOUNT_ID"]) or config["EXPECTED_ACCOUNT_ID"] == "000000000000":
        raise ValueError("EXPECTED_ACCOUNT_ID debe tener 12 dígitos.")
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]", config["BUCKET"]):
        raise ValueError("Usar un nombre de bucket de 3 a 63 caracteres, sin puntos.")
    if not re.fullmatch(r"emr-\d+\.\d+\.\d+", config["EMR_RELEASE"]):
        raise ValueError("EMR_RELEASE debe ser una versión explícita, por ejemplo emr-7.1.0.")
    if not re.fullmatch(r"subnet-[a-f0-9]+", config["SUBNET_ID"]):
        raise ValueError("SUBNET_ID inválida.")
    limites = {"NODOS_CORE": (1, 3), "IDLE_TIMEOUT": (60, 3600), "SHUFFLE_PARTITIONS": (1, 200)}
    for clave, (minimo, maximo) in limites.items():
        if not minimo <= int(config[clave]) <= maximo:
            raise ValueError(f"{clave} debe estar entre {minimo} y {maximo}.")
    return config


def paquete():
    """ZIP reproducible; distribuye pipeline.comun tanto al driver como a ejecutores."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archivo:
        for nombre in FILES:
            info = zipfile.ZipInfo(f"pipeline/{nombre}")
            info.compress_type = zipfile.ZIP_DEFLATED
            archivo.writestr(info, (PIPELINE / nombre).read_bytes())
    datos = buffer.getvalue()
    return datos, hashlib.sha256(datos).hexdigest()


def pasos(bucket, release, shuffle):
    codigo = f"s3://{bucket}/codigo/{release}"
    return [
        {"Name": nombre.removesuffix(".py"), "ActionOnFailure": "TERMINATE_CLUSTER",
         "HadoopJarStep": {"Jar": "command-runner.jar", "Args": [
             "spark-submit", "--master", "yarn", "--deploy-mode", "cluster",
             "--conf", "spark.yarn.submit.waitAppCompletion=true",
             "--py-files", f"{codigo}/pipeline.zip", f"{codigo}/{nombre}",
             "--base", f"s3://{bucket}/datalake", "--shuffle", str(shuffle)]}}
        for nombre in FILES[2:]
    ]


def archivos_raw(base, fecha):
    if date.fromisoformat(fecha).isoformat() != fecha:
        raise ValueError("Usar fecha de carga AAAA-MM-DD.")
    archivos = []
    for fuente in ("dengue", "clima"):
        carpeta = base / "raw" / fuente / f"fecha_carga={fecha}"
        candidatos = sorted(carpeta.glob("*.csv"))
        if not candidatos or any(p.stat().st_size == 0 or p.is_symlink() for p in candidatos):
            raise ValueError(f"Se requieren CSV no vacíos y sin enlaces en {carpeta}.")
        archivos.extend((p, f"datalake/raw/{fuente}/fecha_carga={fecha}/{p.name}") for p in candidatos)
    return archivos


def sha_archivo(path):
    digest = hashlib.sha256()
    with path.open("rb") as entrada:
        for bloque in iter(lambda: entrada.read(1024 * 1024), b""):
            digest.update(bloque)
    return digest.hexdigest()


class Despliegue:
    def __init__(self, config, salida, dry_run=False):
        self.c = config
        self.salida = salida
        self.dry_run = dry_run
        self.state_path = salida / "estado.json"
        self.contexto = {k: config[k] for k in ("AWS_REGION", "EXPECTED_ACCOUNT_ID", "BUCKET")}
        self.state = json.loads(self.state_path.read_text()) if self.state_path.exists() else {}
        if self.state and self.state.get("contexto") != self.contexto:
            raise ValueError("El estado pertenece a otra cuenta/región/bucket. Usar otra --salida.")
        self.state["contexto"] = self.contexto

    def guardar(self):
        if self.dry_run:
            return
        self.salida.mkdir(parents=True, exist_ok=True)
        temp = self.state_path.with_suffix(".tmp")
        temp.write_text(json.dumps(self.state, indent=2), encoding="utf-8")
        temp.replace(self.state_path)

    def aws(self, *args):
        comando = ["aws", *args, "--region", self.c["AWS_REGION"], "--output", "json", "--no-cli-pager"]
        if self.dry_run:
            print(json.dumps(comando, ensure_ascii=False))
            return {}
        entorno = dict(os.environ, AWS_PAGER="", AWS_CLI_AUTO_PROMPT="off")
        resultado = subprocess.run(comando, capture_output=True, text=True, encoding="utf-8",
                                   env=entorno, timeout=180)
        if resultado.returncode:
            raise ErrorAWS(resultado.stderr.strip())
        return json.loads(resultado.stdout) if resultado.stdout.strip() else {}

    def identidad(self):
        # No aceptar endpoints de emulador como evidencia de ejecución cloud.
        if any(k.startswith("AWS_ENDPOINT_URL") and v for k, v in os.environ.items()):
            raise ValueError("Quitar AWS_ENDPOINT_URL y sus variantes para el despliegue AWS real.")
        identity = self.aws("sts", "get-caller-identity")
        if not self.dry_run and identity["Account"] != self.c["EXPECTED_ACCOUNT_ID"]:
            raise ValueError("La sesión AWS pertenece a otra cuenta; no se crearán recursos.")

    def bucket_propio(self):
        self.aws("s3api", "head-bucket", "--bucket", self.c["BUCKET"],
                 "--expected-bucket-owner", self.c["EXPECTED_ACCOUNT_ID"])
        location = self.aws("s3api", "get-bucket-location", "--bucket", self.c["BUCKET"],
                            "--expected-bucket-owner", self.c["EXPECTED_ACCOUNT_ID"])
        if not self.dry_run and (location.get("LocationConstraint") or "us-east-1") != self.c["AWS_REGION"]:
            raise ValueError("El bucket está en otra región; usar la misma región para S3 y EMR.")

    def comprobar(self):
        self.identidad()
        subnets = self.aws("ec2", "describe-subnets", "--subnet-ids", self.c["SUBNET_ID"])
        if not self.dry_run and subnets["Subnets"][0]["State"] != "available":
            raise ValueError("La subred no está disponible.")
        self.aws("emr", "describe-release-label", "--release-label", self.c["EMR_RELEASE"])
        print("Comprobación terminada. Roles, salida de red y tipos de instancia deben estar habilitados en Learner Lab.")

    def bucket(self):
        self.identidad()
        try:
            self.bucket_propio()
        except ErrorAWS as error:
            # 403 nunca se interpreta como permiso para crear o tomar un bucket ajeno.
            if "(404)" not in str(error) and "NoSuchBucket" not in str(error):
                raise
            args = ["s3api", "create-bucket", "--bucket", self.c["BUCKET"]]
            if self.c["AWS_REGION"] != "us-east-1":
                args += ["--create-bucket-configuration", f"LocationConstraint={self.c['AWS_REGION']}"]
            self.aws(*args)
        if self.dry_run:
            print("Si el bucket no existe: create-bucket (en ejecución real se verifica propietario).")
        owner = ["--bucket", self.c["BUCKET"], "--expected-bucket-owner", self.c["EXPECTED_ACCOUNT_ID"]]
        bloqueo = {k: True for k in ("BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")}
        self.aws("s3api", "put-public-access-block", *owner, "--public-access-block-configuration", json.dumps(bloqueo))
        self.aws("s3api", "put-bucket-versioning", *owner, "--versioning-configuration", "Status=Enabled")
        cifrado = {"Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]}
        self.aws("s3api", "put-bucket-encryption", *owner, "--server-side-encryption-configuration", json.dumps(cifrado))
        self.state["bucket_configurado"] = True
        self.guardar()

    def subir_inmutables(self, archivos):
        self.bucket_propio()
        pendientes = []
        # Validar todos antes de subir: no reemplazar silenciosamente cargas ya ingeridas.
        for archivo, key in archivos:
            sha = sha_archivo(archivo)
            try:
                actual = self.aws("s3api", "head-object", "--bucket", self.c["BUCKET"], "--key", key,
                                  "--expected-bucket-owner", self.c["EXPECTED_ACCOUNT_ID"])
            except ErrorAWS as error:
                if "(404)" not in str(error) and "NoSuchKey" not in str(error):
                    raise
                actual = None
            if self.dry_run or actual is None:
                pendientes.append((archivo, key, sha))
            elif actual.get("Metadata", {}).get("sha256") != sha:
                raise ValueError(f"Contenido distinto en {key}. Usar una nueva fecha de carga; no se sobrescribió.")
        for archivo, key, sha in pendientes:
            self.aws("s3", "cp", str(archivo), f"s3://{self.c['BUCKET']}/{key}", "--only-show-errors",
                     "--sse", "AES256", "--metadata", f"sha256={sha}")

    def subir(self, base, fecha):
        self.identidad()
        archivos = archivos_raw(base, fecha)
        datos, release = paquete()
        build = self.salida / "paquetes" / release
        build.mkdir(parents=True, exist_ok=True)
        (build / "pipeline.zip").write_bytes(datos)
        codigo = [(build / "pipeline.zip", f"codigo/{release}/pipeline.zip")]
        codigo += [(PIPELINE / nombre, f"codigo/{release}/{nombre}") for nombre in FILES[2:]]
        self.subir_inmutables(archivos + codigo)
        self.state["release"] = release
        self.state.setdefault("cargas", {})[fecha] = {key: sha_archivo(p) for p, key in archivos}
        self.guardar()
        print(f"Carga {fecha}; paquete {release}; base s3://{self.c['BUCKET']}/datalake")

    def cluster(self):
        cluster_id = self.state.get("cluster_id") or ("j-SIMULACION" if self.dry_run else None)
        if not cluster_id:
            raise ValueError("Primero crear el clúster con este archivo de estado.")
        cluster = self.aws("emr", "describe-cluster", "--cluster-id", cluster_id).get("Cluster", {})
        if not self.dry_run:
            tags = {x["Key"]: x["Value"] for x in cluster.get("Tags", [])}
            if tags.get("Proyecto") != "minsadengue-bigdata" or tags.get("Bucket") != self.c["BUCKET"]:
                raise ValueError("El clúster no corresponde al proyecto y bucket del estado.")
        return cluster_id, cluster

    def crear_cluster(self):
        self.comprobar()
        self.bucket_propio()
        if not self.state.get("release") and not self.dry_run:
            raise ValueError("Subir primero datos y código; así se evita crear un clúster sin entradas.")
        if self.state.get("cluster_id"):
            actual, cluster = self.cluster()
            if self.dry_run or cluster["Status"]["State"] not in TERMINALES:
                print(f"Clúster existente: {actual}; no se crea otro.")
                return
        grupos = [{"Name": tipo, "InstanceRole": tipo, "InstanceType": self.c["INSTANCIA"],
                   "InstanceCount": cantidad, "Market": "ON_DEMAND"}
                  for tipo, cantidad in (("MASTER", 1), ("CORE", int(self.c["NODOS_CORE"])))]
        resultado = self.aws("emr", "create-cluster", "--name", "minsadengue-pasos-1-3",
            "--release-label", self.c["EMR_RELEASE"], "--applications", "Name=Spark",
            "--service-role", self.c["ROL_SERVICIO_EMR"],
            "--ec2-attributes", json.dumps({"InstanceProfile": self.c["PERFIL_INSTANCIA_EMR"], "SubnetId": self.c["SUBNET_ID"]}),
            "--instance-groups", json.dumps(grupos), "--step-concurrency-level", "1",
            "--log-uri", f"s3://{self.c['BUCKET']}/logs/emr/", "--no-termination-protected",
            "--auto-termination-policy", json.dumps({"IdleTimeout": int(self.c["IDLE_TIMEOUT"])}),
            "--tags", "Proyecto=minsadengue-bigdata", f"Bucket={self.c['BUCKET']}",
            "for-use-with-amazon-emr-managed-policies=true")
        if not self.dry_run:
            self.state["cluster_id"] = resultado["ClusterId"]
            self.state.pop("step_ids", None)
            self.state["creado_utc"] = datetime.now(timezone.utc).isoformat()
            self.guardar()
        print(resultado or "Simulación: create-cluster; no se creó infraestructura.")

    def lanzar(self):
        self.identidad()
        cluster_id, cluster = self.cluster()
        if not self.dry_run and cluster["Status"]["State"] not in {"STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING"}:
            raise ValueError("El clúster ya está terminando o terminó. Crear uno nuevo.")
        if self.state.get("step_ids"):
            raise ValueError("Ya se enviaron pasos a este clúster. Consultar estado/esperar; no reenviar.")
        release = self.state.get("release") or (paquete()[1] if self.dry_run else None)
        if not release:
            raise ValueError("No se registró código subido.")
        resultado = self.aws("emr", "add-steps", "--cluster-id", cluster_id, "--steps",
                            json.dumps(pasos(self.c["BUCKET"], release, self.c["SHUFFLE_PARTITIONS"])))
        if not self.dry_run:
            self.state["step_ids"] = resultado["StepIds"]
            self.guardar()
        print(resultado or "Simulación: tres pasos secuenciales con Spark en YARN.")

    def estado(self):
        self.identidad()
        cluster_id, cluster = self.cluster()
        steps = [self.aws("emr", "describe-step", "--cluster-id", cluster_id, "--step-id", sid)
                 for sid in self.state.get("step_ids", [])]
        evidencia = {"fecha_utc": datetime.now(timezone.utc).isoformat(), "contexto": self.contexto,
                     "release": self.state.get("release"), "cluster": cluster, "steps": steps}
        if not self.dry_run:
            self.salida.mkdir(parents=True, exist_ok=True)
            (self.salida / "ejecucion.json").write_text(json.dumps(evidencia, indent=2), encoding="utf-8")
            historial = self.salida / "ejecuciones" / cluster_id
            historial.mkdir(parents=True, exist_ok=True)
            (historial / "ejecucion.json").write_text(json.dumps(evidencia, indent=2), encoding="utf-8")
        print(json.dumps({"cluster_id": cluster_id, "estado": cluster.get("Status"),
                          "pasos": [{"id": s["Step"]["Id"], "nombre": s["Step"]["Name"],
                                      "estado": s["Step"]["Status"]} for s in steps]}, indent=2))
        return steps

    def evidencias(self):
        self.identidad()
        self.bucket_propio()
        self.aws("s3", "sync", f"s3://{self.c['BUCKET']}/datalake/evidencias/",
                 str(self.salida / "pipeline"), "--only-show-errors")
        print("Evidencias agregadas del pipeline descargadas; originales y casos individuales no se descargan.")

    def esperar(self, timeout):
        if not self.state.get("step_ids"):
            raise ValueError("No hay pasos enviados para supervisar.")
        limite = time.monotonic() + timeout
        try:
            while True:
                steps = self.estado()
                estados = [s["Step"]["Status"]["State"] for s in steps]
                if self.dry_run or (len(estados) == 3 and all(e == "COMPLETED" for e in estados)):
                    self.evidencias()
                    return
                if any(e in {"FAILED", "CANCELLED", "INTERRUPTED"} for e in estados):
                    raise ValueError("Falló un paso; consultar ejecucion.json y logs en S3.")
                if time.monotonic() >= limite:
                    raise TimeoutError("Se agotó el tiempo de supervisión.")
                time.sleep(20)
        finally:
            habia_error = sys.exc_info()[0] is not None
            try:
                self.apagar()
            except (ErrorAWS, OSError, ValueError, subprocess.SubprocessError) as error:
                if not habia_error:
                    raise
                print(f"No se pudo solicitar el apagado: {error}. Revisar EMR en la consola.", file=sys.stderr)

    def apagar(self):
        self.identidad()
        cluster_id, cluster = self.cluster()
        if not self.dry_run and cluster["Status"]["State"] in TERMINALES | {"TERMINATING"}:
            print("Clúster terminado o terminando.")
            return
        self.aws("emr", "terminate-clusters", "--cluster-ids", cluster_id)
        print("Apagado solicitado; verificar estado TERMINATED. S3 y evidencias se conservan.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("accion", choices=["comprobar", "bucket", "subir", "cluster", "lanzar", "estado", "esperar", "evidencias", "apagar"])
    parser.add_argument("--config", type=Path, default=ROOT / "nube/config/entorno.env")
    parser.add_argument("--salida", type=Path, default=ROOT / "output/aws")
    parser.add_argument("--dry-run", action="store_true", help="No invoca AWS; puede crear el ZIP local.")
    parser.add_argument("--base-local", type=Path, default=ROOT / "data/_nube")
    parser.add_argument("--fecha-carga")
    parser.add_argument("--timeout", type=int, default=3600)
    args = parser.parse_args()
    if args.accion == "subir" and not args.fecha_carga:
        parser.error("subir requiere --fecha-carga")
    if args.timeout <= 0:
        parser.error("--timeout debe ser positivo")
    try:
        deployment = Despliegue(cargar_config(args.config), args.salida, args.dry_run)
        if args.accion == "subir":
            deployment.subir(args.base_local, args.fecha_carga)
        elif args.accion == "cluster":
            deployment.crear_cluster()
        elif args.accion == "esperar":
            deployment.esperar(args.timeout)
        else:
            getattr(deployment, args.accion)()
    except (ValueError, OSError, ErrorAWS, subprocess.SubprocessError, TimeoutError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
