"""S3 y EMR para el pipeline (pasos 1 a 5). Solo requiere Python y AWS CLI v2."""

import argparse
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import time
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from costos import DATA_GIB, ROOT_GIB, estimar
from errores import ErrorAWS

ROOT = Path(__file__).resolve().parents[2]
PIPELINE = ROOT / "nube/pipeline"
BOOTSTRAP = Path(__file__).resolve().parent / "bootstrap_python.sh"
MODULOS = ["__init__.py", "comun.py", "variables.py", "metricas.py"]
PASOS = ["paso_01_bronze.py", "paso_02_silver.py", "paso_03_gold.py", "paso_04_modelo.py", "paso_05_alertas.py"]
FILES = MODULOS + PASOS
# completa: primera carga o reentrenamiento. carga: datos nuevos con el modelo vigente.
CORRIDAS = {"completa": PASOS, "carga": [p for p in PASOS if p != "paso_04_modelo.py"]}
TERMINALES = {"TERMINATED", "TERMINATED_WITH_ERRORS"}


def cargar_config(path, requiere_infra=True):
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
    opcionales = {"GLUE_DATABASE"} if requiere_infra else {"GLUE_DATABASE", "SUBNET_ID"}
    for clave in permitidas - opcionales:
        if not config.get(clave):
            raise ValueError(f"Completar {clave} en {path}.")
    if not re.fullmatch(r"\d{12}", config["EXPECTED_ACCOUNT_ID"]) or config["EXPECTED_ACCOUNT_ID"] == "000000000000":
        raise ValueError("EXPECTED_ACCOUNT_ID debe tener 12 dígitos.")
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]", config["BUCKET"]):
        raise ValueError("Usar un nombre de bucket de 3 a 63 caracteres, sin puntos.")
    if not re.fullmatch(r"emr-\d+\.\d+\.\d+", config["EMR_RELEASE"]):
        raise ValueError("EMR_RELEASE debe ser una versión explícita, por ejemplo emr-7.1.0.")
    if config.get("SUBNET_ID") and not re.fullmatch(r"subnet-[a-f0-9]+", config["SUBNET_ID"]):
        raise ValueError("SUBNET_ID inválida.")
    for clave in ("ROL_SERVICIO_EMR", "PERFIL_INSTANCIA_EMR"):
        if not re.fullmatch(r"[\w+=,.@-]{1,64}", config[clave]):
            raise ValueError(f"{clave} debe contener el nombre del rol/perfil, sin ARN ni ruta.")
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


def pasos(bucket, release, shuffle, corrida, prefijo="datalake"):
    codigo = f"s3://{bucket}/codigo/{release}"
    return [
        {"Type": "CUSTOM_JAR", "Name": nombre.removesuffix(".py"), "ActionOnFailure": "TERMINATE_CLUSTER",
         "Jar": "command-runner.jar", "Args": [
             "spark-submit", "--master", "yarn", "--deploy-mode", "cluster",
             "--conf", "spark.yarn.submit.waitAppCompletion=true",
             "--py-files", f"{codigo}/pipeline.zip", f"{codigo}/{nombre}",
             "--base", f"s3://{bucket}/{prefijo}", "--shuffle", str(shuffle)]}
        for nombre in CORRIDAS[corrida]
    ]


def archivos_raw(base, fecha, prefijo="datalake"):
    if date.fromisoformat(fecha).isoformat() != fecha:
        raise ValueError("Usar fecha de carga AAAA-MM-DD.")
    archivos = []
    for fuente in ("dengue", "clima"):
        carpeta = base / "raw" / fuente / f"fecha_carga={fecha}"
        candidatos = sorted(carpeta.glob("*.csv"))
        if not candidatos or any(p.stat().st_size == 0 or p.is_symlink() for p in candidatos):
            raise ValueError(f"Se requieren CSV no vacíos y sin enlaces en {carpeta}.")
        archivos.extend((p, f"{prefijo}/raw/{fuente}/fecha_carga={fecha}/{p.name}") for p in candidatos)
    return archivos


def decodificar(datos):
    """En Windows el AWS CLI escribe con la página de códigos de la consola (cp1252), no en UTF-8."""
    try:
        return datos.decode("utf-8")
    except UnicodeDecodeError:
        return datos.decode("cp1252", errors="replace")


def sha_archivo(path):
    digest = hashlib.sha256()
    with path.open("rb") as entrada:
        for bloque in iter(lambda: entrada.read(1024 * 1024), b""):
            digest.update(bloque)
    return digest.hexdigest()


def validar_prefijo(prefijo):
    # El rol EC2 de EMR solo puede escribir bajo datalake/.
    if prefijo != "datalake" and not re.fullmatch(r"datalake(/[a-z0-9][a-z0-9_-]*)+", prefijo):
        raise ValueError("El prefijo debe ser datalake o datalake/<carpeta>, en minúsculas.")
    return prefijo


class Despliegue:
    def __init__(self, config, salida, dry_run=False, prefijo="datalake"):
        self.c = config
        self.prefijo = validar_prefijo(prefijo)
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

    def aws(self, *args, region=None):
        comando = ["aws", *args, "--region", region or self.c["AWS_REGION"], "--output", "json", "--no-cli-pager"]
        if self.dry_run:
            print(json.dumps(comando, ensure_ascii=False))
            return {}
        entorno = dict(os.environ, AWS_PAGER="", AWS_CLI_AUTO_PROMPT="off")
        resultado = subprocess.run(comando, capture_output=True, env=entorno, timeout=180)
        if resultado.returncode:
            raise ErrorAWS(decodificar(resultado.stderr).strip())
        salida = decodificar(resultado.stdout)
        return json.loads(salida) if salida.strip() else {}

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
        if not self.c.get("SUBNET_ID"):
            raise ValueError("Ejecutar preparar y usar su entorno.preparado.env, o configurar SUBNET_ID.")
        subnets = self.aws("ec2", "describe-subnets", "--subnet-ids", self.c["SUBNET_ID"])
        if not self.dry_run:
            subnet = subnets["Subnets"][0]
            tags = {t["Key"]: t["Value"] for t in subnet.get("Tags", [])}
            if subnet["State"] != "available" or not subnet.get("MapPublicIpOnLaunch"):
                raise ValueError("Se requiere una subred disponible con IPv4 pública automática, sin NAT.")
            if tags.get("for-use-with-amazon-emr-managed-policies") != "true":
                raise ValueError("La subred no tiene la etiqueta requerida por AmazonEMRServicePolicy_v2.")
        rutas = self.aws("ec2", "describe-route-tables", "--filters",
                         f"Name=association.subnet-id,Values={self.c['SUBNET_ID']}")
        if not self.dry_run:
            internet = any(r.get("DestinationCidrBlock") == "0.0.0.0/0" and
                          r.get("GatewayId", "").startswith("igw-") and r.get("State") == "active"
                          for t in rutas["RouteTables"] for r in t["Routes"])
            if not internet:
                raise ValueError("Falta ruta pública activa hacia Internet Gateway; no se creará EMR.")
        self.aws("iam", "get-role", "--role-name", self.c["ROL_SERVICIO_EMR"])
        perfil = self.aws("iam", "get-instance-profile", "--instance-profile-name", self.c["PERFIL_INSTANCIA_EMR"])
        if not self.dry_run and [r["RoleName"] for r in perfil["InstanceProfile"]["Roles"]] != [self.c["PERFIL_INSTANCIA_EMR"]]:
            raise ValueError("EMR requiere un rol EC2 del mismo nombre dentro del perfil de instancia.")
        self.aws("emr", "describe-release-label", "--release-label", self.c["EMR_RELEASE"])
        print("Comprobación terminada: identidad, subred pública, roles y release. Cuotas, políticas efectivas y arranque se confirman en AWS.")

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
        archivos = archivos_raw(base, fecha, self.prefijo)
        datos, release = paquete()
        build = self.salida / "paquetes" / release
        build.mkdir(parents=True, exist_ok=True)
        (build / "pipeline.zip").write_bytes(datos)
        codigo = [(build / "pipeline.zip", f"codigo/{release}/pipeline.zip")]
        codigo += [(PIPELINE / nombre, f"codigo/{release}/{nombre}") for nombre in PASOS]
        bootstrap = f"codigo/bootstrap/{sha_archivo(BOOTSTRAP)}.sh"
        self.subir_inmutables(archivos + codigo + [(BOOTSTRAP, bootstrap)])
        self.state["release"] = release
        self.state["bootstrap"] = bootstrap
        self.state.setdefault("cargas", {})[fecha] = {key: sha_archivo(p) for p, key in archivos}
        self.guardar()
        print(f"Carga {fecha}; paquete {release}; base s3://{self.c['BUCKET']}/{self.prefijo}")

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

    def crear_cluster(self, horas=1, max_usd=2, tarifas=None):
        self.comprobar()
        self.bucket_propio()
        if not self.state.get("release") and not self.dry_run:
            raise ValueError("Subir primero datos y código; así se evita crear un clúster sin entradas.")
        if not self.state.get("bootstrap") and not self.dry_run:
            raise ValueError("Volver a ejecutar subir: falta el script que instala numpy y pandas para MLlib.")
        if self.state.get("cluster_id"):
            actual, cluster = self.cluster()
            if self.dry_run or cluster["Status"]["State"] not in TERMINALES:
                print(f"Clúster existente: {actual}; no se crea otro.")
                return
        informe = estimar(self, horas, tarifas)
        if informe:
            escenario = next(e for e in informe["escenarios"] if e["nodos_totales"] == 1 + int(self.c["NODOS_CORE"]))
            if float(escenario["total_parcial_usd"]) > max_usd:
                raise ValueError("La estimación parcial supera --max-usd. Revisar duración/nodos antes de crear EMR.")
        grupos = [{"Name": tipo, "InstanceGroupType": tipo, "InstanceType": self.c["INSTANCIA"],
                   "InstanceCount": cantidad, "EbsConfiguration": {
                       "EbsBlockDeviceConfigs": [{"VolumeSpecification": {"VolumeType": "gp3", "SizeInGB": DATA_GIB},
                                                  "VolumesPerInstance": 1}]}}
                  for tipo, cantidad in (("MASTER", 1), ("CORE", int(self.c["NODOS_CORE"])))]
        resultado = self.aws("emr", "create-cluster", "--name", "minsadengue-pipeline",
            "--release-label", self.c["EMR_RELEASE"], "--applications", "Name=Spark",
            "--service-role", self.c["ROL_SERVICIO_EMR"],
            "--ec2-attributes", json.dumps({"InstanceProfile": self.c["PERFIL_INSTANCIA_EMR"], "SubnetId": self.c["SUBNET_ID"]}),
            "--instance-groups", json.dumps(grupos), "--step-concurrency-level", "1",
            "--ebs-root-volume-size", str(ROOT_GIB),
            "--bootstrap-actions", json.dumps([{"Name": "instalar-librerias-python",
                "Path": f"s3://{self.c['BUCKET']}/{self.state.get('bootstrap', 'codigo/bootstrap/SIMULACION.sh')}"}]),
            "--log-uri", f"s3://{self.c['BUCKET']}/logs/emr/", "--no-termination-protected",
            "--auto-termination-policy", json.dumps({"IdleTimeout": int(self.c["IDLE_TIMEOUT"])}),
            "--tags", "Proyecto=minsadengue-bigdata", f"Bucket={self.c['BUCKET']}",
            "for-use-with-amazon-emr-managed-policies=true")
        if not self.dry_run:
            self.state["cluster_id"] = resultado["ClusterId"]
            self.state.pop("step_ids", None)
            self.state["creado_utc"] = datetime.now(timezone.utc).isoformat()
            self.state["estimacion_al_crear"] = informe
            self.guardar()
        print(resultado or "Simulación: create-cluster; no se creó infraestructura.")

    def lanzar(self, corrida):
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
                            json.dumps(pasos(self.c["BUCKET"], release, self.c["SHUFFLE_PARTITIONS"], corrida, self.prefijo)))
        if not self.dry_run:
            self.state["step_ids"] = resultado["StepIds"]
            self.state["corrida"] = corrida
            self.guardar()
        nombres = ", ".join(n.removesuffix(".py") for n in CORRIDAS[corrida])
        print(resultado or f"Simulación de la corrida {corrida}: {nombres}, en orden y con Spark en YARN.")

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
        self.aws("s3", "sync", f"s3://{self.c['BUCKET']}/{self.prefijo}/evidencias/",
                 str(self.salida / "pipeline"), "--only-show-errors")
        print("Evidencias agregadas del pipeline descargadas; originales y casos individuales no se descargan.")

    def esperar(self, timeout):
        if not self.state.get("step_ids") and not self.dry_run:
            raise ValueError("No hay pasos enviados para supervisar.")
        limite = time.monotonic() + timeout
        try:
            while True:
                steps = self.estado()
                estados = [s["Step"]["Status"]["State"] for s in steps]
                completos = len(estados) == len(self.state.get("step_ids", [])) and all(e == "COMPLETED" for e in estados)
                if self.dry_run or completos:
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

    def todo(self, base, fecha, corrida, horas=1, max_usd=2, tarifas=None, timeout=3600):
        """bucket, subir, cluster, lanzar, esperar y catalogo en orden. Se puede repetir:
        reutiliza el clúster activo y no reenvía pasos ya enviados. Siempre solicita el apagado."""
        self.bucket()
        self.subir(base, fecha)
        self.crear_cluster(horas, max_usd, tarifas)
        try:
            if self.state.get("step_ids") and not self.dry_run:
                print("Pasos ya enviados a este clúster; se retoma la supervisión.")
            else:
                self.lanzar(corrida)
        except BaseException:
            try:
                self.apagar()
            except (ErrorAWS, OSError, ValueError, subprocess.SubprocessError) as error:
                print(f"No se pudo solicitar el apagado: {error}. Revisar EMR en la consola.", file=sys.stderr)
            raise
        self.esperar(timeout)  # apaga el clúster al terminar, con o sin error
        from catalogo import catalogo
        catalogo(self)

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
    parser.add_argument("accion", choices=["preparar", "estimar", "comprobar", "bucket", "subir", "cluster", "lanzar", "estado", "esperar", "evidencias", "apagar", "catalogo", "todo", "benchmark", "costos"],
                        help="todo: bucket, subir, cluster, lanzar, esperar (apaga) y catalogo; benchmark: histórico con 2 y 4 nodos en bases separadas; costos: costo real en Cost Explorer")
    parser.add_argument("--config", type=Path, default=ROOT / "nube/config/entorno.env")
    parser.add_argument("--salida", type=Path, default=ROOT / "output/aws")
    parser.add_argument("--dry-run", action="store_true", help="No invoca AWS; puede crear el ZIP local.")
    parser.add_argument("--base-local", type=Path, default=ROOT / "data/_nube")
    parser.add_argument("--fecha-carga")
    parser.add_argument("--corrida", choices=list(CORRIDAS),
                        help="lanzar: completa (pasos 1 a 5, entrena) o carga (1, 2, 3 y 5, modelo vigente)")
    parser.add_argument("--timeout", type=int, default=3600)
    parser.add_argument("--horas", type=float, default=1, help="Duración total prevista, incluido arranque y apagado.")
    parser.add_argument("--max-usd", type=float, default=2, help="Umbral para la estimación parcial; NO es un límite de facturación.")
    parser.add_argument("--tarifas", type=Path, help="JSON de tarifas revisadas para calcular sin consultar Pricing API.")
    parser.add_argument("--nodos", type=int, nargs="+", default=[2, 4], help="benchmark: nodos totales por configuración")
    parser.add_argument("--desde", help="costos: primer día AAAA-MM-DD")
    parser.add_argument("--hasta", help="costos: día siguiente al último, AAAA-MM-DD; por defecto mañana")
    args = parser.parse_args()
    if args.accion in {"subir", "todo"} and not args.fecha_carga:
        parser.error(f"{args.accion} requiere --fecha-carga")
    if args.accion in {"lanzar", "todo"} and not args.corrida:
        parser.error(f"{args.accion} requiere --corrida completa o --corrida carga")
    if args.accion == "costos" and not args.desde:
        parser.error("costos requiere --desde AAAA-MM-DD")
    if args.timeout <= 0:
        parser.error("--timeout debe ser positivo")
    if not 0 < args.horas <= 24 or not 0 < args.max_usd < float("inf"):
        parser.error("Usar 0 < --horas <= 24 y --max-usd positivo y finito")
    try:
        requiere_infra = args.accion not in {"preparar", "estimar", "bucket", "subir", "catalogo", "costos"}
        deployment = Despliegue(cargar_config(args.config, requiere_infra), args.salida, args.dry_run)
        if args.accion == "subir":
            deployment.subir(args.base_local, args.fecha_carga)
        elif args.accion == "cluster":
            deployment.crear_cluster(args.horas, args.max_usd, args.tarifas)
        elif args.accion == "preparar":
            from cuenta_propia import preparar
            preparar(deployment)
        elif args.accion == "catalogo":
            from catalogo import catalogo
            catalogo(deployment)
        elif args.accion == "estimar":
            estimar(deployment, args.horas, args.tarifas)
        elif args.accion == "lanzar":
            deployment.lanzar(args.corrida)
        elif args.accion == "esperar":
            deployment.esperar(args.timeout)
        elif args.accion == "benchmark":
            from benchmark import benchmark
            benchmark(deployment.c, args.base_local, args.fecha_carga or "2026-11-17", args.nodos,
                      salida=args.salida / "benchmark", dry_run=args.dry_run, horas=args.horas, max_usd=args.max_usd, timeout=args.timeout)
        elif args.accion == "costos":
            from costos import costo_real
            hasta = args.hasta or (date.today() + timedelta(days=1)).isoformat()
            costo_real(deployment, date.fromisoformat(args.desde).isoformat(), date.fromisoformat(hasta).isoformat())
        elif args.accion == "todo":
            deployment.todo(args.base_local, args.fecha_carga, args.corrida, args.horas,
                            args.max_usd, args.tarifas, args.timeout)
        else:
            getattr(deployment, args.accion)()
    except (ValueError, OSError, ErrorAWS, subprocess.SubprocessError, TimeoutError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())