"""Genera data/muestra: un Raw pequeño con la misma estructura que data/_nube/raw.

Toma pocos departamentos y las dos fechas de carga, con el mismo formato y separador,
para probar el pipeline incremental sin EMR. Dengue conserva los conteos por semana pero
no los datos de cada caso (lugar, edad y sexo son ficticios); clima se copia tal cual:

    python scripts/crear_muestra.py
    spark-submit nube/pipeline/paso_01_bronze.py --base data/muestra   (luego pasos 2 a 5)

HUANUCO y PASCO superan el mínimo de 1 000 registros; PUNO queda por debajo y debe
excluirse en Gold.
"""

import argparse
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEPARTAMENTOS = {"HUANUCO", "PASCO", "PUNO"}
SEPARADORES = {"dengue": ";", "clima": ","}


def normalizar(texto):
    return texto.strip().upper().replace("Á", "A").replace("É", "E").replace("Í", "I") \
        .replace("Ó", "O").replace("Ú", "U")


def anonimizar(campos, columnas, numero):
    """Conserva departamento, año, semana y diagnóstico (lo que llega a Gold) y reemplaza
    lugar, edad y sexo por valores ficticios: la muestra no contiene casos individuales."""
    fila = dict(zip(columnas, campos))
    fila.update(provincia="MUESTRA", distrito="MUESTRA", localidad="MUESTRA", localcod="",
                ubigeo=fila["ubigeo"][:2] + "0101", edad="30", tipo_edad="A",
                sexo="F" if numero % 2 else "M")
    return [fila[c] for c in columnas]


def filtrar(origen, destino, sep, departamentos, anonimo=False):
    with origen.open(encoding="utf-8-sig", newline="") as entrada:
        encabezado = entrada.readline()
        columnas = [c.strip().lower() for c in encabezado.rstrip("\r\n").split(sep)]
        indice = columnas.index("departamento")
        filas = []
        for linea in entrada:
            campos = linea.rstrip("\r\n").split(sep)
            if normalizar(campos[indice]) not in departamentos:
                continue
            if anonimo:
                campos = anonimizar(campos, columnas, len(filas))
            filas.append(sep.join(campos) + "\n")
    destino.parent.mkdir(parents=True, exist_ok=True)
    with destino.open("w", encoding="utf-8", newline="") as salida:
        salida.write(encabezado)
        salida.writelines(filas)
    return len(filas)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--origen", type=Path, default=ROOT / "data/_nube/raw")
    parser.add_argument("--destino", type=Path, default=ROOT / "data/muestra/raw")
    args = parser.parse_args()
    if args.destino.exists():
        shutil.rmtree(args.destino)
    for fuente, sep in SEPARADORES.items():
        for archivo in sorted((args.origen / fuente).glob("fecha_carga=*/*.csv")):
            destino = args.destino / fuente / archivo.parent.name / archivo.name
            filas = filtrar(archivo, destino, sep, DEPARTAMENTOS, anonimo=fuente == "dengue")
            if not filas:
                raise ValueError(f"{archivo} no tiene filas de {sorted(DEPARTAMENTOS)}")
            print(f"{destino.relative_to(ROOT)}: {filas} filas")


if __name__ == "__main__":
    main()