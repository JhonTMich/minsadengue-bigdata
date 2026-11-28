"""Compara reglas de alerta solo sobre la exportación 2021–2023 del paso 4."""

import argparse
import csv
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "nube"))
from pipeline.metricas import resumen, umbral_por_costo, umbral_por_tasa  # noqa: E402


def comparar(datos, max_tasa=0.20):
    if datos["anios"] != [2021, 2023] or datos["entrenamiento_hasta"] != 2020:
        raise ValueError("Esta evaluación requiere train hasta 2020 y validación 2021–2023.")
    filas = datos["filas"]
    if not filas or any(not 2021 <= f["ano"] <= 2023 for f in filas):
        raise ValueError("La exportación está vacía o contiene años fuera de validación.")
    resultado = []
    for modelo in datos["candidatos"]:
        etiquetas = [int(f["label"]) for f in filas]
        puntajes = [f[modelo] for f in filas]
        reglas = {"costo_3_a_1": umbral_por_costo(etiquetas, puntajes, 3),
                  "costo_1_a_1": umbral_por_costo(etiquetas, puntajes, 1),
                  "tope_alertas_20pct": umbral_por_tasa(puntajes, max_tasa)}
        for regla, umbral in reglas.items():
            for grupo, seleccion in [("completo", filas),
                                      ("sin_brote_actual", [f for f in filas if not f["brote_actual"]])]:
                m = resumen([int(f["label"]) for f in seleccion], [f[modelo] for f in seleccion], umbral)
                alertas = m["vp"] + m["fp"]
                resultado.append({"modelo": modelo, "elegido": modelo == datos["modelo_elegido"],
                    "criterio": regla, "subconjunto": grupo, **m, "alertas": alertas,
                    "tasa_alertas": alertas / m["n"] if m["n"] else None,
                    "falsas_alarmas_por_acierto": m["fp"] / m["vp"] if m["vp"] else None})
    return resultado


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("predicciones", type=Path, help="Archivo part-*.txt de validacion_predicciones")
    parser.add_argument("--salida", type=Path, default=ROOT / "docs/evidencias/umbral_validacion")
    args = parser.parse_args()
    contenido = args.predicciones.read_bytes()
    datos = json.loads(contenido)
    filas = comparar(datos)
    args.salida.mkdir(parents=True, exist_ok=True)
    with (args.salida / "comparacion.csv").open("w", newline="", encoding="utf-8") as destino:
        escritor = csv.DictWriter(destino, fieldnames=list(filas[0]))
        escritor.writeheader()
        escritor.writerows(filas)
    protocolo = {"train_hasta": 2020, "validacion": [2021, 2023], "hasta": 2023,
                 "modelo_elegido": datos["modelo_elegido"], "seleccion_modelo": "Sin cambios: AP sin brote actual",
                 "n_validacion": len(datos["filas"]), "max_tasa_propuesta": 0.20,
                 "busqueda_costo": "rejilla 0.01..0.99; >=; empate de costo favorece umbral alto",
                 "busqueda_tope": "corte empírico exacto; >=; empates completos; posibilidad de no emitir",
                 "criterio_elegido": "costo_1_a_1",
                 "version_modelo_local": args.predicciones.parent.parent.name,
                 "codigo_sha256": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                                    for p in [ROOT / "nube/pipeline/variables.py", ROOT / "nube/pipeline/metricas.py",
                                              ROOT / "nube/pipeline/paso_04_modelo.py"]},
                 "sha256_predicciones": hashlib.sha256(contenido).hexdigest(),
                 "2024": "No se ejecutó ni se utilizó para seleccionar el umbral"}
    (args.salida / "protocolo.json").write_text(json.dumps(protocolo, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps([f for f in filas if f["elegido"]], indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()