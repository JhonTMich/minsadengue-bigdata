"""La auditoría de umbrales nunca debe aceptar años externos a la validación."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.evaluar_umbral_validacion import comparar  # noqa: E402

class PeriodoValidacion(unittest.TestCase):
    def datos(self):
        return {"anios": [2021, 2023], "entrenamiento_hasta": 2020,
                "modelo_elegido": "casos", "candidatos": ["casos"],
                "filas": [{"ano": 2021, "label": 1, "casos": 0.8, "brote_actual": 0},
                          {"ano": 2023, "label": 0, "casos": 0.2, "brote_actual": 0}]}

    def test_rechaza_filas_fuera_de_validacion(self):
        datos = self.datos()
        # Año artificial de test: no se lee ni se utiliza la carga real de 2024.
        datos["filas"][0]["ano"] = 2024
        with self.assertRaisesRegex(ValueError, "fuera"):
            comparar(datos)

    def test_rechaza_otro_corte_de_entrenamiento(self):
        datos = self.datos()
        datos["entrenamiento_hasta"] = 2021
        with self.assertRaisesRegex(ValueError, "2020"):
            comparar(datos)

    def test_compara_sobre_las_mismas_filas(self):
        filas = comparar(self.datos())
        self.assertEqual(len(filas), 6)
        self.assertTrue(all(f["n"] == 2 for f in filas))


if __name__ == "__main__":
    unittest.main()