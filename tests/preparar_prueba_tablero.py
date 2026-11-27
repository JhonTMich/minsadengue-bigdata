"""Crea una COPIA PBIP con ocho observaciones sintéticas, sin acceder a AWS.

Abrir output/prueba_tablero/indicadores.pbip y actualizar para probar el DAX
en Power BI Desktop. Nunca usar esta copia como evidencia del proyecto MINSA.
"""

import json
import shutil
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DESTINO = ROOT / "output/prueba_tablero"


def preparar():
    shutil.copytree(ROOT / "nube/tablero", DESTINO, dirs_exist_ok=True,
                    ignore=shutil.ignore_patterns(".pbi", "__pycache__"))
    modelo = DESTINO / "indicadores.SemanticModel/model.bim"
    datos = json.loads(modelo.read_text(encoding="utf-8"))
    hechos = datos["model"]["tables"][0]
    tipos = {"string": "text", "int64": "Int64.Type", "double": "number",
             "boolean": "logical", "dateTime": "datetime"}
    campos = ", ".join(f'{c["name"]} = {tipos[c["dataType"]]}' for c in hechos["columns"])
    # TP, FP, FN, TN; luego brote vigente, entrenamiento, sin desenlace, sin puntaje.
    casos = [(True, 1, 0, False, .9), (True, 0, 0, False, .9),
             (False, 1, 0, False, .4), (False, 0, 0, False, .4),
             (True, 1, 1, False, .9), (True, 1, 0, True, .9),
             (True, None, 0, False, .9), (False, 1, 0, False, None)]
    filas = []
    for i, (alerta, desenlace, vigente, entrenamiento, puntaje) in enumerate(casos):
        inicio = date(2024, 1, 7) + timedelta(weeks=i)
        fin = inicio + timedelta(days=6)
        valores = {"cod_dep": '"01"', "departamento": '"PRUEBA SINTÉTICA"',
                   "version_modelo": '"PRUEBA_NO_REAL"', "fecha_carga": '"2026-11-27"',
                   "ano": "2024", "semana": str(i + 1), "mes": str(inicio.month),
                   "casos": "10", "brote_actual": str(vigente),
                   "brote_observado": "null" if desenlace is None else str(desenlace),
                   "temp_media": "25", "hum_media": "70", "precip_total": "2",
                   "umbral_q3": "20", "probabilidad": "null" if puntaje is None else str(puntaje),
                   "umbral_alerta": ".85", "alerta": str(alerta).lower(),
                   "dentro_entrenamiento": str(entrenamiento).lower(),
                   "fecha_inicio": f"#datetime({inicio.year}, {inicio.month}, {inicio.day}, 0, 0, 0)",
                   "fecha_fin": f"#datetime({fin.year}, {fin.month}, {fin.day}, 0, 0, 0)",
                   "actualizado_utc": "#datetime(2026, 11, 27, 0, 0, 0)"}
        filas.append("{" + ", ".join(valores[c["name"]] for c in hechos["columns"]) + "}")
    hechos["partitions"][0]["source"]["expression"] = (
        "#table(type table [" + campos + "], {" + ",\n".join(filas) + "})")
    modelo.write_text(json.dumps(datos, ensure_ascii=False, indent=2), encoding="utf-8")
    for visual in (DESTINO / "indicadores.Report").rglob("visual.json"):
        contenido = json.loads(visual.read_text(encoding="utf-8"))
        obj = contenido["visual"].get("objects", {})
        if contenido["visual"]["visualType"] == "textbox":
            for parrafo in obj["general"][0]["properties"]["paragraphs"]:
                for texto in parrafo["textRuns"]:
                    texto["value"] = texto["value"].replace("VIGILANCIA", "PRUEBA SINTÉTICA")
        visual.write_text(json.dumps(contenido, ensure_ascii=False, indent=2), encoding="utf-8")
    print(DESTINO / "indicadores.pbip")
    print("Esperado sin brote: N=4, TP=1, FP=1, FN=1, TN=1; recall=precisión=50%.")
    print("Todas las semanas: N=5, TP=2, FP=1, FN=1, TN=1. Sin desenlace=1.")


if __name__ == "__main__":
    preparar()
