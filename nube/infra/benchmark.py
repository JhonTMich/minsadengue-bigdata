"""Benchmark: el histórico completo con 2 y 4 nodos, cada configuración en su propia base S3.

Cada corrida parte de Raw vacío bajo datalake/benchmark/<id>/n<nodos>, así que procesa
todo desde cero y no toca el data lake principal ni su manifiesto. Las configuraciones
corren una tras otra: la cuota de 16 vCPU no alcanza para tener ambas encendidas.
"""

import json
import sys
import time
from datetime import datetime, timezone

from desplegar import ROOT, TERMINALES, Despliegue

SALIDA = ROOT / "output/aws/benchmark"


def segundos(inicio, fin):
    return round((datetime.fromisoformat(fin) - datetime.fromisoformat(inicio)).total_seconds(), 1)


def esperar_apagado(deploy, cluster_id, limite=1800, espera=20):
    fin = time.monotonic() + limite
    while True:
        cluster = deploy.aws("emr", "describe-cluster", "--cluster-id", cluster_id)["Cluster"]
        if cluster["Status"]["State"] in TERMINALES:
            return cluster
        if time.monotonic() >= fin:
            raise TimeoutError(f"{cluster_id} no terminó; revisar EMR en la consola antes de seguir.")
        time.sleep(espera)


def medir(deploy, cluster, steps, nodos):
    linea = cluster["Status"]["Timeline"]
    pasos = {}
    for s in steps:
        t = s["Step"]["Status"]["Timeline"]
        pasos[s["Step"]["Name"]] = segundos(t["StartDateTime"], t["EndDateTime"])
    horas = segundos(linea["CreationDateTime"], linea["EndDateTime"]) / 3600
    escenario = next(e for e in deploy.state["estimacion_al_crear"]["escenarios"] if e["nodos_totales"] == nodos)
    usd_hora = float(escenario["total_parcial_usd"]) / float(escenario["horas_sesion"])
    return {"nodos_totales": nodos, "nodos_core": nodos - 1, "cluster_id": cluster["Id"],
            "base": f"s3://{deploy.c['BUCKET']}/{deploy.prefijo}",
            "arranque_s": segundos(linea["CreationDateTime"], linea["ReadyDateTime"]),
            "pasos_s": pasos, "total_pasos_s": round(sum(pasos.values()), 1),
            "vida_cluster_h": round(horas, 4), "usd_hora_estimado": round(usd_hora, 4),
            "usd_estimado": round(usd_hora * horas, 4)}


def tabla(resultados):
    nombres = list(resultados[0]["pasos_s"])
    filas = ["| Medida | " + " | ".join(f"{r['nodos_totales']} nodos" for r in resultados) + " |",
             "|---|" + "---:|" * len(resultados),
             "| Arranque (s) | " + " | ".join(str(r["arranque_s"]) for r in resultados) + " |"]
    filas += [f"| {n} (s) | " + " | ".join(str(r["pasos_s"].get(n, "—")) for r in resultados) + " |" for n in nombres]
    filas += ["| Total pasos (s) | " + " | ".join(str(r["total_pasos_s"]) for r in resultados) + " |",
              "| Vida del clúster (h) | " + " | ".join(str(r["vida_cluster_h"]) for r in resultados) + " |",
              "| Costo estimado (USD) | " + " | ".join(str(r["usd_estimado"]) for r in resultados) + " |"]
    return "\n".join(filas)


def benchmark(config, base_local, fecha, nodos=(2, 4), salida=SALIDA, dry_run=False,
              horas=1, max_usd=2, timeout=3600):
    corrida = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    resultados = []
    for n in nodos:
        if not 2 <= n <= 4:
            raise ValueError("Cada configuración debe tener entre 2 y 4 nodos en total.")
        deploy = Despliegue(dict(config, NODOS_CORE=str(n - 1)), salida / corrida / f"n{n}", dry_run,
                            f"datalake/benchmark/{corrida}/n{n}")
        print(f"== {n} nodos: base s3://{config['BUCKET']}/{deploy.prefijo}", flush=True)
        deploy.subir(base_local, fecha)
        deploy.crear_cluster(horas, max_usd)
        try:
            deploy.lanzar("completa")
        except BaseException:
            deploy.apagar()
            raise
        deploy.esperar(timeout)
        if dry_run:
            continue
        cluster = esperar_apagado(deploy, deploy.state["cluster_id"])
        resultados.append(medir(deploy, cluster, deploy.estado(), n))
        guardar(salida / corrida, corrida, fecha, config, resultados)
    if dry_run:
        print(f"Simulación del benchmark con {', '.join(map(str, nodos))} nodos; no se creó infraestructura.")
        return []
    print(tabla(resultados))
    print(f"Resultados en {salida / corrida}", file=sys.stderr)
    return resultados


def guardar(destino, corrida, fecha, config, resultados):
    informe = {"corrida": corrida, "fecha_carga": fecha, "instancia": config["INSTANCIA"],
               "emr_release": config["EMR_RELEASE"], "shuffle_partitions": config["SHUFFLE_PARTITIONS"],
               "resultados": resultados}
    destino.mkdir(parents=True, exist_ok=True)
    (destino / "resultados.json").write_text(json.dumps(informe, indent=2), encoding="utf-8")
    (destino / "resultados.md").write_text(tabla(resultados) + "\n", encoding="utf-8")