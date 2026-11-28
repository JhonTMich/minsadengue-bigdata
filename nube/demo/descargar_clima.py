"""Descarga de NASA POWER el clima diario de un año epidemiológico y lo deja como carga en Raw.

Guarda la respuesta JSON de cada punto sin cambios y un CSV con el mismo esquema que
clima_departamentos.csv. Los puntos son los mismos del notebook cargaDatos.ipynb.
"""

import argparse
import csv
import json
import time
import urllib.request
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlencode

URL_BASE = "https://power.larc.nasa.gov/api/temporal/daily/point"
VARIABLES = ["T2M", "RH2M", "PRECTOTCORR"]
RELLENO = -999

PUNTOS = {
    "01": ("AMAZONAS", -5.64, -78.53),
    "02": ("ANCASH", -9.07, -78.59),
    "05": ("AYACUCHO", -11.91, -73.285),
    "06": ("CAJAMARCA", -5.71, -78.81),
    "07": ("CALLAO", -11.87, -77.755),
    "08": ("CUSCO", -12.36, -73.315),
    "10": ("HUANUCO", -8.80, -75.375),
    "11": ("ICA", -13.57, -76.355),
    "12": ("JUNIN", -11.05, -74.695),
    "13": ("LA LIBERTAD", -8.11, -79.655),
    "14": ("LAMBAYEQUE", -6.77, -79.84),
    "15": ("LIMA", -11.97, -77.655),
    "16": ("LORETO", -3.75, -73.25),
    "17": ("MADRE DE DIOS", -12.59, -69.19),
    "19": ("PASCO", -9.86, -75.01),
    "20": ("PIURA", -5.19, -80.63),
    "22": ("SAN MARTIN", -6.49, -76.36),
    "24": ("TUMBES", -3.57, -80.45),
    "25": ("UCAYALI", -8.38, -74.55),
}


def inicio_anio_epi(anio):
    d = date(anio, 1, 4)
    return d - timedelta(days=(d.weekday() + 1) % 7)


def rango_anio_epi(anio):
    return inicio_anio_epi(anio), inicio_anio_epi(anio + 1) - timedelta(days=1)


def descargar(lat, lon, desde, hasta, intentos=3):
    consulta = urlencode({"parameters": ",".join(VARIABLES), "community": "AG",
                          "latitude": lat, "longitude": lon,
                          "start": desde.strftime("%Y%m%d"), "end": hasta.strftime("%Y%m%d"),
                          "format": "JSON"})
    for intento in range(1, intentos + 1):
        try:
            with urllib.request.urlopen(f"{URL_BASE}?{consulta}", timeout=180) as resp:
                return resp.read()
        except OSError:
            if intento == intentos:
                raise
            time.sleep(10 * intento)


def filas(cod, departamento, datos, desde, hasta):
    serie = datos["properties"]["parameter"]
    dias = [(desde + timedelta(days=i)).strftime("%Y%m%d") for i in range((hasta - desde).days + 1)]
    faltan = [d for d in dias if any(d not in serie[v] for v in VARIABLES)]
    if faltan:
        raise ValueError(f"{cod}: NASA no devolvió {len(faltan)} días, desde {faltan[0]}")
    return [[cod, departamento, d, *[serie[v][d] for v in VARIABLES]] for d in dias]


def main():
    parser = argparse.ArgumentParser(description="Clima NASA POWER de un año epidemiológico, como carga Raw")
    parser.add_argument("--base", required=True)
    parser.add_argument("--fecha-carga", required=True)
    parser.add_argument("--anio", type=int, required=True)
    args = parser.parse_args()

    date.fromisoformat(args.fecha_carga)
    desde, hasta = rango_anio_epi(args.anio)
    destino = Path(args.base) / "raw" / "clima" / f"fecha_carga={args.fecha_carga}"
    destino.mkdir(parents=True, exist_ok=True)

    todas, relleno = [], 0
    for cod, (departamento, lat, lon) in PUNTOS.items():
        json_punto = destino / f"nasa_power_{cod}_{lat}_{lon}.json"
        if not json_punto.exists():
            json_punto.write_bytes(descargar(lat, lon, desde, hasta))
            time.sleep(2)
        nuevas = filas(cod, departamento, json.loads(json_punto.read_text(encoding="utf-8")), desde, hasta)
        relleno += sum(v == RELLENO for f in nuevas for v in f[3:])
        todas += nuevas
        print(f"{cod} {departamento:<14} {len(nuevas)} días", flush=True)

    temporal = destino / "clima.parcial"
    with temporal.open("w", encoding="utf-8", newline="") as salida:
        escritor = csv.writer(salida, lineterminator="\n")
        escritor.writerow(["cod_dep", "departamento", "fecha", *VARIABLES])
        escritor.writerows(todas)
    final = destino / f"clima_nasa_power_{desde:%Y%m%d}_{hasta:%Y%m%d}.csv"
    temporal.replace(final)
    print(f"{final} ({len(todas)} filas, {relleno} valores {RELLENO})")


if __name__ == "__main__":
    main()
