"""Registra Gold en Glue Data Catalog y crea las vistas de indicadores con Athena."""

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

SQL = Path(__file__).resolve().parents[1] / "sql"
ARCHIVOS = ["01_tablas.sql", "02_indicadores.sql"]
TABLAS = ["panel_dengue_clima", "alertas", "dim_departamento", "dim_semana", "modelo_metricas"]
VISTAS = ["v_hechos_semanales", "v_departamentos_en_alerta", "v_ranking_riesgo", "v_casos_vs_canal",
          "v_desempeno_alertas"]
BASE_PREDETERMINADA = "minsadengue_gold"
TERMINALES = {"SUCCEEDED", "FAILED", "CANCELLED"}


def base_datos(config):
    nombre = config.get("GLUE_DATABASE") or BASE_PREDETERMINADA
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,254}", nombre):
        raise ValueError("GLUE_DATABASE: usar minúsculas, dígitos y guiones bajos.")
    return nombre


def sentencias(texto, valores):
    for clave, valor in valores.items():
        texto = texto.replace("{{" + clave + "}}", valor)
    if "{{" in texto:
        raise ValueError("Quedó un marcador sin reemplazar en el SQL.")
    sin_comentarios = "\n".join(l for l in texto.splitlines() if not l.lstrip().startswith("--"))
    return [s.strip() for s in sin_comentarios.split(";") if s.strip()]


def resultados(deploy):
    return f"s3://{deploy.c['BUCKET']}/athena/resultados/"


def consultar(deploy, sql, espera=2, limite=300):
    inicio = deploy.aws("athena", "start-query-execution", "--query-string", sql, "--work-group", "primary",
                        "--result-configuration", json.dumps({
                            "OutputLocation": resultados(deploy),
                            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}}))
    if deploy.dry_run:
        return {"QueryExecutionId": "SIMULACION", "Status": {"State": "SUCCEEDED"}}
    qid = inicio["QueryExecutionId"]
    fin = time.monotonic() + limite
    while True:
        ejecucion = deploy.aws("athena", "get-query-execution", "--query-execution-id", qid)["QueryExecution"]
        estado = ejecucion["Status"]["State"]
        if estado in TERMINALES:
            break
        if time.monotonic() >= fin:
            raise TimeoutError(f"Athena no terminó la consulta {qid}.")
        time.sleep(espera)
    if estado != "SUCCEEDED":
        motivo = ejecucion["Status"].get("StateChangeReason", "sin detalle")
        raise ValueError(f"Athena {estado} en {qid}: {motivo}\n{sql[:300]}")
    return ejecucion


def valor_unico(deploy, ejecucion):
    if deploy.dry_run:
        return None
    filas = deploy.aws("athena", "get-query-results", "--query-execution-id",
                       ejecucion["QueryExecutionId"])["ResultSet"]["Rows"]
    return filas[1]["Data"][0].get("VarCharValue")


def catalogo(deploy):
    deploy.identidad()
    deploy.bucket_propio()
    base = base_datos(deploy.c)
    valores = {"BASE_DATOS": base, "BUCKET": deploy.c["BUCKET"]}
    for archivo in ARCHIVOS:
        for sql in sentencias((SQL / archivo).read_text(encoding="utf-8"), valores):
            consultar(deploy, sql)
    conteos = {}
    for objeto in TABLAS + VISTAS:
        ejecucion = consultar(deploy, f"SELECT COUNT(*) FROM {base}.{objeto}")
        conteos[objeto] = {"filas": valor_unico(deploy, ejecucion),
                           "bytes_escaneados": ejecucion.get("Statistics", {}).get("DataScannedInBytes"),
                           "query_id": ejecucion["QueryExecutionId"]}
    evidencia = {"fecha_utc": datetime.now(timezone.utc).isoformat(), "base_datos": base,
                 "resultados_athena": resultados(deploy), "objetos": conteos}
    if not deploy.dry_run:
        destino = deploy.salida / "athena"
        destino.mkdir(parents=True, exist_ok=True)
        (destino / "catalogo.json").write_text(json.dumps(evidencia, indent=2), encoding="utf-8")
    print(json.dumps(evidencia, indent=2, ensure_ascii=False))