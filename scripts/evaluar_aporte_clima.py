from pathlib import Path
import os
import nbformat
from nbclient import NotebookClient
root = Path(__file__).resolve().parents[1]
spark = Path(os.environ.get('SPARK_HOME', '/usr/local/spark'))
os.environ['PYTHONPATH'] = os.pathsep.join([str(spark/'python'), *map(str,(spark/'python/lib').glob('py4j-*-src.zip')), os.environ.get('PYTHONPATH','')])
nb = nbformat.read(root/'notebooks/modeloBase.ipynb', as_version=4)
# Esta ejecución contrasta las logísticas ya definidas; RF se evalúa en otro experimento.
nb.cells = [c for c in nb.cells if not (c.cell_type == 'code' and 'from pyspark.ml.classification import RandomForestClassifier' in c.source)]
for c in nb.cells:
    if c.cell_type != 'code': continue
    c.outputs=[]; c.execution_count=None
    c.source=c.source.replace('.appName("ModeloBaseDengue")', '.master("local[2]").appName("ModeloBaseDengue")')
client=NotebookClient(nb, timeout=600, kernel_name='python3', resources={'metadata':{'path':str(root)}})
client.on_cell_start=lambda cell,cell_index,**kw: print(f'Celda {cell_index}',flush=True)
try:
    client.execute()
    print('COMPLETADO',flush=True)
finally:
    (root/'output').mkdir(exist_ok=True)
    nbformat.write(nb,root/'output/modeloBase_aporte_clima.ipynb')
