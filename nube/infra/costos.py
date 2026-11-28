"""Estimación de una sesión EMR a tarifas On-Demand; no es un límite de gasto."""

import json
from datetime import datetime, timezone
from decimal import Decimal

ROOT_GIB = 15
DATA_GIB = 32


def tarifa(productos, unidad):
    encontrados = []
    for texto in productos:
        producto = json.loads(texto) if isinstance(texto, str) else texto
        for termino in producto.get("terms", {}).get("OnDemand", {}).values():
            for dimension in termino["priceDimensions"].values():
                if dimension["unit"] == unidad and dimension.get("beginRange", "0") == "0":
                    encontrados.append((Decimal(dimension["pricePerUnit"]["USD"]), producto["product"]["sku"]))
    if len(encontrados) != 1 or encontrados[0][0] <= 0:
        raise ValueError(f"Tarifa {unidad} ausente o ambigua; no se estimará como cero.")
    precio, sku = encontrados[0]
    return {"usd": str(precio), "unidad": unidad, "sku": sku}


def consultar_precios(deploy):
    region, instancia = deploy.c["AWS_REGION"], deploy.c["INSTANCIA"]
    consultas = {
        "ec2_hora": ("AmazonEC2", "Hrs", {"instanceType": instancia, "operatingSystem": "Linux",
            "tenancy": "Shared", "preInstalledSw": "NA", "capacitystatus": "Used", "operation": "RunInstances"}),
        "emr_hora": ("ElasticMapReduce", "Hrs", {"instanceType": instancia, "softwareType": "EMR"}),
        "ebs_gib_mes": ("AmazonEC2", "GB-Mo", {"volumeApiName": "gp3", "productFamily": "Storage"}),
        "ipv4_hora": ("AmazonVPC", "Hrs", {"group": "VPCPublicIPv4Address",
            "groupDescription": "Hourly charge for In-use Public IPv4 Addresses"}),
    }
    resultado = {"region": region, "instancia": instancia,
                 "fecha_utc": datetime.now(timezone.utc).isoformat(), "fuente": "AWS Price List Query API", "tarifas": {}}
    for nombre, (servicio, unidad, filtros) in consultas.items():
        filtros["regionCode"] = region
        respuesta = deploy.aws("pricing", "get-products", "--service-code", servicio, "--filters",
            json.dumps([{"Type": "TERM_MATCH", "Field": k, "Value": v} for k, v in filtros.items()]),
            region="us-east-1")
        if deploy.dry_run:
            continue
        resultado["tarifas"][nombre] = tarifa(respuesta.get("PriceList", []), unidad)
    return resultado


def calcular(precios, region, instancia, nodos, horas):
    if precios["region"] != region or precios["instancia"] != instancia:
        raise ValueError("Las tarifas no corresponden a la región e instancia configuradas.")
    if not 0 < horas <= 24:
        raise ValueError("--horas debe ser mayor que cero y no superar 24.")
    unidades = {"ec2_hora": "Hrs", "emr_hora": "Hrs", "ebs_gib_mes": "GB-Mo", "ipv4_hora": "Hrs"}
    tasas = {}
    for clave, unidad in unidades.items():
        dato = precios["tarifas"][clave]
        tasas[clave] = Decimal(dato["usd"])
        if dato["unidad"] != unidad or not tasas[clave].is_finite() or tasas[clave] <= 0:
            raise ValueError(f"Tarifa inválida: {clave}")
    nh = Decimal(nodos) * Decimal(str(horas))
    partes = {"ec2": nh * tasas["ec2_hora"], "emr": nh * tasas["emr_hora"],
              "ipv4": nh * tasas["ipv4_hora"],
              "ebs_gp3": nh * Decimal(ROOT_GIB + DATA_GIB) * tasas["ebs_gib_mes"] / Decimal(730)}
    return {"nodos_totales": nodos, "horas_sesion": horas, "gib_por_nodo": ROOT_GIB + DATA_GIB,
            "desglose_usd": {k: str(v.quantize(Decimal("0.000001"))) for k, v in partes.items()},
            "total_parcial_usd": str(sum(partes.values()).quantize(Decimal("0.000001"))),
            "excluye": ["S3 y sus versiones/solicitudes", "CloudWatch", "Athena/Glue", "transferencias", "impuestos"],
            "supuestos": "Horas incluyen arranque, procesamiento, espera y apagado. EBS prorrateado con 730 h/mes; sin créditos ni descuentos."}


def estimar(deploy, horas, archivo=None):
    if archivo:
        contenido = json.loads(archivo.read_text(encoding="utf-8"))
        precios = contenido.get("precios", contenido)
    else:
        if not deploy.dry_run:
            deploy.identidad()
        precios = consultar_precios(deploy)
    if deploy.dry_run and not archivo:
        print("Simulación: se consultarían tarifas; no hay estimación monetaria sin precios.")
        return None
    try:
        informe = {"precios": precios, "escenarios": [calcular(precios, deploy.c["AWS_REGION"],
            deploy.c["INSTANCIA"], n, horas) for n in sorted({2, 4, 1 + int(deploy.c["NODOS_CORE"])})]}
    except (KeyError, TypeError, ArithmeticError) as error:
        raise ValueError("Archivo de tarifas incompleto o inválido; revisar región, instancia, unidades y precios.") from error
    if not deploy.dry_run:
        deploy.salida.mkdir(parents=True, exist_ok=True)
        (deploy.salida / "estimacion.json").write_text(json.dumps(informe, indent=2), encoding="utf-8")
    print(json.dumps(informe, indent=2, ensure_ascii=False))
    return informe


def costo_real(deploy, desde, hasta):
    """Costo facturado por servicio y día según Cost Explorer (se actualiza con 24 a 48 h de retraso).
    Incluye todo lo de la cuenta, no solo este proyecto."""
    deploy.identidad()
    respuesta = deploy.aws("ce", "get-cost-and-usage", "--time-period", f"Start={desde},End={hasta}",
                           "--granularity", "DAILY", "--metrics", "UnblendedCost",
                           "--group-by", "Type=DIMENSION,Key=SERVICE", region="us-east-1")
    if deploy.dry_run:
        return None
    por_servicio, por_dia = {}, {}
    for dia in respuesta["ResultsByTime"]:
        fecha = dia["TimePeriod"]["Start"]
        for grupo in dia["Groups"]:
            monto = Decimal(grupo["Metrics"]["UnblendedCost"]["Amount"])
            servicio = grupo["Keys"][0]
            por_servicio[servicio] = por_servicio.get(servicio, Decimal(0)) + monto
            por_dia[fecha] = por_dia.get(fecha, Decimal(0)) + monto
    informe = {"fecha_utc": datetime.now(timezone.utc).isoformat(), "fuente": "AWS Cost Explorer",
               "periodo": {"desde": desde, "hasta_exclusivo": hasta}, "moneda": "USD",
               "por_servicio": {k: f"{v:.4f}" for k, v in sorted(por_servicio.items(), key=lambda x: -x[1])},
               "por_dia": {k: f"{v:.4f}" for k, v in sorted(por_dia.items())},
               "total": f"{sum(por_servicio.values(), Decimal(0)):.4f}",
               "nota": "Incluye todos los servicios de la cuenta; impuestos y créditos según la factura."}
    deploy.salida.mkdir(parents=True, exist_ok=True)
    (deploy.salida / "costo_real.json").write_text(json.dumps(informe, indent=2), encoding="utf-8")
    print(json.dumps(informe, indent=2, ensure_ascii=False))
    return informe
