import argparse
import csv
from datetime import date, timedelta
from pathlib import Path


def anio_dengue(campos, indice):
    return int(campos[indice])


def anio_clima(campos, indice):
    texto = campos[indice]
    dia = date(int(texto[:4]), int(texto[4:6]), int(texto[6:8]))
    # El miércoles de la semana (domingo a sábado) define el año epidemiológico
    miercoles = dia + timedelta(days=3 - (dia.weekday() + 1) % 7)
    return miercoles.year


FUENTES = {
    "dengue": ("vigilancia_dengue", "ano", anio_dengue),
    "clima": ("clima_nasa_power", "fecha", anio_clima),
}


def separador(cabecera):
    # El MINSA publicó hasta 2024 con ";" y desde el corte 2025 con "," y comillas.
    return ";" if cabecera.count(";") > cabecera.count(",") else ","


def dividir(origen, destino_dir, fuente, incluir):
    prefijo, columna, obtener_anio = FUENTES[fuente]
    destino_dir.mkdir(parents=True, exist_ok=True)
    temporal = destino_dir / f"{prefijo}.parcial"
    anios = set()
    # Las líneas se copian sin cambios: Raw conserva el formato de la fuente.
    with origen.open(encoding="utf-8-sig", newline="") as entrada, \
         temporal.open("w", encoding="utf-8", newline="") as salida:
        cabecera = entrada.readline()
        sep = separador(cabecera)
        indice = next(csv.reader([cabecera], delimiter=sep)).index(columna)
        salida.write(cabecera)
        for linea in entrada:
            anio = obtener_anio(next(csv.reader([linea], delimiter=sep)), indice)
            if incluir(anio):
                salida.write(linea)
                anios.add(anio)
    if not anios:
        temporal.unlink()
        raise SystemExit(f"{origen.name}: ninguna fila para esta parte")
    final = destino_dir / f"{prefijo}_{min(anios)}_{max(anios)}.csv"
    temporal.replace(final)
    return final, len(anios)


def main():
    parser = argparse.ArgumentParser(description="Divide las fuentes en una carga histórica y una carga nueva")
    parser.add_argument("--base", required=True)
    parser.add_argument("--parte", choices=["historico", "nueva"], required=True)
    parser.add_argument("--fecha-carga", required=True)
    parser.add_argument("--anio-corte", type=int, default=2023)
    parser.add_argument("--dengue")
    parser.add_argument("--clima", help="Omitir si el clima de la carga lo escribe descargar_clima.py")
    args = parser.parse_args()

    date.fromisoformat(args.fecha_carga)
    corte = args.anio_corte
    incluir = (lambda a: a <= corte) if args.parte == "historico" else (lambda a: a > corte)
    for fuente, origen in [("dengue", args.dengue), ("clima", args.clima)]:
        if not origen:
            continue
        destino = Path(args.base) / "raw" / fuente / f"fecha_carga={args.fecha_carga}"
        archivo, n_anios = dividir(Path(origen), destino, fuente, incluir)
        print(f"{fuente}: {archivo} ({n_anios} años)")


if __name__ == "__main__":
    main()