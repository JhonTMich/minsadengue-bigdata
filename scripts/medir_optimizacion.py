"""Ejecutar dentro del contenedor: python scripts/medir_optimizacion.py.
Cada corrida usa un kernel nuevo; la caché del SO no se controla.
"""
import json
import os
import platform
import random
import re
import time
from pathlib import Path

import nbformat
from nbclient import NotebookClient

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'output' / 'optimizacion'
OUT.mkdir(parents=True, exist_ok=True)
# Spark se distribuye con la imagen, sin paquete pip independiente.
spark_home = Path(os.environ.get('SPARK_HOME', '/usr/local/spark'))
os.environ['PYTHONPATH'] = os.pathsep.join([str(spark_home / 'python'), *map(str, (spark_home / 'python/lib').glob('py4j-*-src.zip')), os.environ.get('PYTHONPATH', '')])
orden = [(cache, sp, r) for cache in (False, True) for sp in (8, 200) for r in range(1, 4)]
random.Random(42).shuffle(orden)
(OUT / 'protocolo.json').write_text(json.dumps({'orden': orden, 'master': 'local[2]', 'driver_memory': '4g', 'cache_so': 'no_controlada', 'python': platform.python_version(), 'plataforma': platform.platform(), 'semilla': 42}, indent=2))
for cache, sp, r in orden:
    nombre = f'cache{cache}_sp{sp}_r{r}'
    destino = OUT / f'{nombre}.ipynb'
    print(f'INICIO {nombre}', flush=True)
    nb = nbformat.read(ROOT / 'notebooks/cargaDatos.ipynb', as_version=4)
    # Excluir escalabilidad y su código de resumen: no forman parte del benchmark.
    nb.cells = nb.cells[:next(i for i,c in enumerate(nb.cells) if c.cell_type == 'markdown' and '## 7. Prueba de escalabilidad' in c.source)]
    for c in nb.cells:
        if c.cell_type != 'code': continue
        c.outputs = []; c.execution_count = None
        c.source = c.source.replace('SHUFFLE_PARTITIONS = 8', f'SHUFFLE_PARTITIONS = {sp}').replace('CORRIDA = 1', f'CORRIDA = {r}')
        c.source = re.sub(r'^USAR_CACHE = (?:True|False).*$', f'USAR_CACHE = {cache}', c.source, flags=re.MULTILINE)
        c.source = c.source.replace('.appName("ModeloPredictivoDenguePeru")', '.master("local[2]").appName("ModeloPredictivoDenguePeru")')
        c.source = c.source.replace('dir_metricas = Path(ruta_salida("metricas", ""))', 'dir_metricas = Path(ruta_salida("metricas", "optimizacion_controlada"))')
    inicio = time.perf_counter()
    try:
        NotebookClient(nb, timeout=600, kernel_name='python3', resources={'metadata': {'path': str(ROOT)}}).execute()
    finally:
        nbformat.write(nb, destino)
    print(f'FIN {nombre}: {time.perf_counter()-inicio:.1f}s de pared (incluye kernel y descarga/reuso de clima)', flush=True)
print('COMPLETADO: 12 corridas', flush=True)
