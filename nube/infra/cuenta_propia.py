"""Prepara IAM y una VPC pública dedicada; no crea EC2, NAT ni clústeres."""

import json

from errores import ErrorAWS

PROYECTO = "minsadengue-bigdata"
TAG_EMR = "for-use-with-amazon-emr-managed-policies"


def etiquetas(bucket):
    return [{"Key": "Proyecto", "Value": PROYECTO}, {"Key": "Bucket", "Value": bucket},
            {"Key": TAG_EMR, "Value": "true"}]


def politica_ec2(bucket):
    arn = f"arn:aws:s3:::{bucket}"
    return {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetBucketLocation", "s3:GetBucketVersioning",
            "s3:ListBucketMultipartUploads"], "Resource": arn},
        {"Effect": "Allow", "Action": ["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject",
            "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
            "Resource": [f"{arn}/{p}/*" for p in ("codigo", "datalake", "logs")]},
        {"Effect": "Allow", "Action": ["cloudwatch:PutMetricData"], "Resource": "*"},
        {"Effect": "Allow", "Action": ["ec2:Describe*", "elasticmapreduce:DescribeCluster",
            "elasticmapreduce:ListInstances", "elasticmapreduce:ListSteps"], "Resource": "*"}]}


def confianza(servicio):
    return {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {
        "Service": servicio}, "Action": "sts:AssumeRole"}]}


def _rol(deploy, nombre, servicio):
    # Reutilizar solo roles de este proyecto y bucket; no modificar roles ajenos.
    try:
        rol = deploy.aws("iam", "get-role", "--role-name", nombre).get("Role", {})
    except ErrorAWS as error:
        if "NoSuchEntity" not in str(error):
            raise
        rol = None
    if deploy.dry_run or rol is None:
        deploy.aws("iam", "create-role", "--role-name", nombre, "--assume-role-policy-document",
                   json.dumps(confianza(servicio)), "--tags", json.dumps(etiquetas(deploy.c["BUCKET"])))
    else:
        tags = {t["Key"]: t["Value"] for t in rol.get("Tags", [])}
        if tags.get("Proyecto") != PROYECTO or tags.get("Bucket") != deploy.c["BUCKET"]:
            raise ValueError(f"El rol {nombre} ya existe y no pertenece a este proyecto/bucket.")
        if rol["AssumeRolePolicyDocument"] != confianza(servicio):
            raise ValueError(f"El rol {nombre} tiene una confianza distinta; revisar IAM.")


def preparar_roles(deploy):
    servicio = deploy.c["ROL_SERVICIO_EMR"]
    # EMR requiere que rol EC2 y perfil de instancia tengan el mismo nombre.
    ec2 = deploy.c["PERFIL_INSTANCIA_EMR"]
    _rol(deploy, servicio, "elasticmapreduce.amazonaws.com")
    _rol(deploy, ec2, "ec2.amazonaws.com")
    deploy.aws("iam", "attach-role-policy", "--role-name", servicio, "--policy-arn",
               "arn:aws:iam::aws:policy/service-role/AmazonEMRServicePolicy_v2")
    passrole = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "iam:PassRole",
        "Resource": f"arn:aws:iam::{deploy.c['EXPECTED_ACCOUNT_ID']}:role/{ec2}",
        "Condition": {"StringEquals": {"iam:PassedToService": "ec2.amazonaws.com"}}}]}
    deploy.aws("iam", "put-role-policy", "--role-name", servicio, "--policy-name", "MinsaDenguePassEC2",
               "--policy-document", json.dumps(passrole))
    deploy.aws("iam", "put-role-policy", "--role-name", ec2, "--policy-name", "MinsaDengueDatos",
               "--policy-document", json.dumps(politica_ec2(deploy.c["BUCKET"])))
    try:
        perfil = deploy.aws("iam", "get-instance-profile", "--instance-profile-name", ec2).get("InstanceProfile", {})
    except ErrorAWS as error:
        if "NoSuchEntity" not in str(error):
            raise
        perfil = None
    if deploy.dry_run or perfil is None:
        deploy.aws("iam", "create-instance-profile", "--instance-profile-name", ec2,
                   "--tags", json.dumps(etiquetas(deploy.c["BUCKET"])))
        roles = []
    else:
        tags = {t["Key"]: t["Value"] for t in perfil.get("Tags", [])}
        if tags.get("Proyecto") != PROYECTO or tags.get("Bucket") != deploy.c["BUCKET"]:
            raise ValueError("El perfil de instancia no pertenece al proyecto y bucket.")
        roles = [r["RoleName"] for r in perfil["Roles"]]
    if roles and roles != [ec2]:
        raise ValueError("El perfil ya contiene otro rol.")
    if not roles:
        deploy.aws("iam", "add-role-to-instance-profile", "--instance-profile-name", ec2, "--role-name", ec2)


def preparar_red(deploy):
    def recurso(clave, operacion, opciones, respuesta, atributo, simulado):
        if clave not in deploy.state:
            result = deploy.aws("ec2", operacion, *opciones)
            deploy.state[clave] = simulado if deploy.dry_run else result[respuesta][atributo]
            deploy.guardar()
        return deploy.state[clave]

    tags = etiquetas(deploy.c["BUCKET"])
    def tag(tipo):
        return ["--tag-specifications", json.dumps([{"ResourceType": tipo, "Tags": tags}])]

    vpc = recurso("vpc_id", "create-vpc", ["--cidr-block", "10.83.0.0/16", *tag("vpc")], "Vpc", "VpcId", "vpc-SIMULACION")
    deploy.aws("ec2", "wait", "vpc-available", "--vpc-ids", vpc)
    deploy.aws("ec2", "modify-vpc-attribute", "--vpc-id", vpc, "--enable-dns-support", "Value=true")
    deploy.aws("ec2", "modify-vpc-attribute", "--vpc-id", vpc, "--enable-dns-hostnames", "Value=true")
    subnet = recurso("subnet_id", "create-subnet", ["--vpc-id", vpc, "--cidr-block", "10.83.1.0/24",
        *tag("subnet")], "Subnet", "SubnetId", "subnet-abc123")
    deploy.aws("ec2", "wait", "subnet-available", "--subnet-ids", subnet)
    deploy.aws("ec2", "modify-subnet-attribute", "--subnet-id", subnet, "--map-public-ip-on-launch")
    igw = recurso("igw_id", "create-internet-gateway", tag("internet-gateway"),
                  "InternetGateway", "InternetGatewayId", "igw-SIMULACION")
    if not deploy.state.get("igw_adjuntado"):
        adjunto = deploy.aws("ec2", "describe-internet-gateways", "--internet-gateway-ids", igw)
        attachments = [] if deploy.dry_run else adjunto["InternetGateways"][0]["Attachments"]
        if not any(a["VpcId"] == vpc for a in attachments):
            deploy.aws("ec2", "attach-internet-gateway", "--vpc-id", vpc, "--internet-gateway-id", igw)
        deploy.state["igw_adjuntado"] = True
        deploy.guardar()
    tabla = recurso("route_table_id", "create-route-table", ["--vpc-id", vpc, *tag("route-table")],
                    "RouteTable", "RouteTableId", "rtb-SIMULACION")
    # replace-route si ya existía permite retomar después de un fallo de la terminal.
    consulta = deploy.aws("ec2", "describe-route-tables", "--route-table-ids", tabla)
    rutas = [] if deploy.dry_run else consulta["RouteTables"][0]["Routes"]
    operacion = "replace-route" if any(r.get("DestinationCidrBlock") == "0.0.0.0/0" for r in rutas) else "create-route"
    deploy.aws("ec2", operacion, "--route-table-id", tabla, "--destination-cidr-block", "0.0.0.0/0", "--gateway-id", igw)
    if not deploy.state.get("route_association_id"):
        asociaciones = [] if deploy.dry_run else consulta["RouteTables"][0]["Associations"]
        asociada = next((a["RouteTableAssociationId"] for a in asociaciones if a.get("SubnetId") == subnet), None)
        if not asociada:
            res = deploy.aws("ec2", "associate-route-table", "--route-table-id", tabla, "--subnet-id", subnet)
            asociada = "rtbassoc-SIMULACION" if deploy.dry_run else res["AssociationId"]
        deploy.state["route_association_id"] = asociada
        deploy.guardar()
    deploy.c["SUBNET_ID"] = subnet


def preparar(deploy):
    deploy.identidad()
    if deploy.c.get("SUBNET_ID") and deploy.c["SUBNET_ID"] != deploy.state.get("subnet_id"):
        raise ValueError("preparar requiere SUBNET_ID vacío o la subred ya creada en este estado; no modifica redes ajenas.")
    preparar_roles(deploy)
    preparar_red(deploy)
    if not deploy.dry_run:
        salida = deploy.salida / "entorno.preparado.env"
        salida.write_text("\n".join(f"{k}={v}" for k, v in deploy.c.items()) + "\n", encoding="utf-8")
        print(f"Cuenta preparada. Usar --config {salida} en los comandos siguientes.")
    else:
        print("Simulación: roles y VPC pública sin NAT; no se crearon recursos.")