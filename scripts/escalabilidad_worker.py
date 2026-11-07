"""Worker del experimento completo. Ejecutar con ejecutar_escalabilidad.py."""
import argparse
import json
import os
import shutil
import threading
import time
import urllib.request
from pathlib import Path

parser=argparse.ArgumentParser()
parser.add_argument('--factor',type=int,required=True,choices=[1,10,100])
args=parser.parse_args()
ROOT=Path(__file__).resolve().parents[1]
BASE=ROOT/'data/escalabilidad_completa'/f'x{args.factor}'
BASE.mkdir(parents=True,exist_ok=True)
STATE=BASE/'estado.json'
state={'factor':args.factor,'estado':'iniciado','etapa':'preparacion','cache_so':'no_controlada','clima_replicado':False}
inicio=time.perf_counter()
def guardar():
    temporal=STATE.with_suffix('.tmp')
    temporal.write_text(json.dumps(state,indent=2))
    temporal.replace(STATE)
def tamano(path):
    return sum(p.stat().st_size for p in path.rglob('*') if p.is_file()) if path.exists() else 0
stop=threading.Event()
def vigilar():
    while not stop.wait(1):
        def leer(*rutas):
            for ruta in rutas:
                p=Path(ruta)
                if p.exists():return int(p.read_text())
            return None
        muestra={'segundos':time.perf_counter()-inicio,
            'memoria_pico_bytes':leer('/sys/fs/cgroup/memory.peak','/sys/fs/cgroup/memory/memory.max_usage_in_bytes'),
            'memoria_actual_bytes':leer('/sys/fs/cgroup/memory.current','/sys/fs/cgroup/memory/memory.usage_in_bytes'),
            'cpu_ns_cgroup_v1':leer('/sys/fs/cgroup/cpuacct/cpuacct.usage'),
            'disco_libre_bytes':shutil.disk_usage(BASE).free}
        temporal=BASE/'recursos.tmp'
        temporal.write_text(json.dumps(muestra,indent=2))
        temporal.replace(BASE/'recursos.json')
        if shutil.disk_usage(BASE).free < 8*1024**3:
            state.update(estado='interrumpido_disco',segundos_transcurridos=time.perf_counter()-inicio)
            guardar()
            os._exit(75)
threading.Thread(target=vigilar,daemon=True).start()
guardar()
try:
    original=ROOT/'data/raw/datos_abiertos_vigilancia_dengue_2000_2024.csv'
    esperado=original.stat().st_size*args.factor
    if shutil.disk_usage(BASE).free < esperado*3+8*1024**3:
        raise RuntimeError('Espacio insuficiente para Raw y capas con reserva de 8 GiB')
    raw=BASE/'raw/dengue.csv';raw.parent.mkdir(exist_ok=True)
    state['etapa']='generacion_raw';guardar()
    with original.open('rb') as f:
        cabecera=f.readline();offset=f.tell()
        with raw.open('wb') as dst:
            dst.write(cabecera)
            for _ in range(args.factor):
                f.seek(offset);shutil.copyfileobj(f,dst,length=1024*1024)
                # Evitar concatenar dos registros si el CSV original no termina en salto.
                f.seek(-1,2)
                if f.read(1)!=b'\n':dst.write(b'\n')
    state.update(generacion_s=time.perf_counter()-inicio,raw_bytes=raw.stat().st_size)
    spark_home=Path(os.environ.get('SPARK_HOME','/usr/local/spark'))
    import sys
    sys.path[:0]=[str(spark_home/'python'),*map(str,(spark_home/'python/lib').glob('py4j-*-src.zip'))]
    nb=json.loads((ROOT/'notebooks/cargaDatos.ipynb').read_text())
    env={}
    config=''.join(nb['cells'][1]['source']).replace('.appName("ModeloPredictivoDenguePeru")','.master("local[2]").appName("EscalabilidadCompleta")').replace('"4g"','"2g"')
    exec(compile(config,'configuracion','exec'),env)
    spark=env['spark']
    state.update(spark=spark.version,master=spark.sparkContext.master,driver_memoria='2g',limite_contenedor_gib=4,shuffle_partitions=env['SHUFFLE_PARTITIONS'])
    env['ruta_salida']=lambda capa,dataset:str(BASE/capa/dataset)
    env['resolver_ruta_datos']=lambda nombre:str(raw)
    env['dir_clima']=ROOT/'data/raw/clima_nasa'
    inicio_pipeline=time.perf_counter()
    for indice,nombre in [(5,'ingesta_bronze'),(7,'silver_dengue'),(9,'silver_clima'),(11,'gold_y_controles')]:
        state.update(etapa=nombre,segundos_transcurridos=time.perf_counter()-inicio);guardar()
        source=''.join(nb['cells'][indice]['source'])
        if indice==11:source=source.replace('MIN_CASOS_DEPARTAMENTO = 1000',f'MIN_CASOS_DEPARTAMENTO = {1000*args.factor}')
        exec(compile(source,f'cargaDatos_celda_{indice}','exec'),env)
    pipeline_s=time.perf_counter()-inicio_pipeline
    assert env['total']==1029421*args.factor,'Conteo Raw/Bronze inesperado'
    assert env['filas']==24776,'Cambió la geometría del panel'
    reference=spark.read.parquet(str(ROOT/'data/gold/panel_dengue_clima')).agg(env['F'].sum('casos')).first()[0]
    assert env['casos_gold']==reference*args.factor,'La réplica no conserva casos escalados'
    api=f'{spark.sparkContext.uiWebUrl}/api/v1/applications/{spark.sparkContext.applicationId}/stages'
    stages=json.load(urllib.request.urlopen(api))
    completas=[s for s in stages if s['status']=='COMPLETE']
    def suma(campo):
        return sum(s[campo] for s in completas) if completas and all(campo in s for s in completas) else None
    def memoria(campo):
        p=Path('/sys/fs/cgroup')/campo
        if p.exists():return int(p.read_text())
        # Docker sobre WSL2 puede usar cgroup v1.
        alternativos={'memory.peak':'memory.max_usage_in_bytes','memory.current':'memory.usage_in_bytes'}
        p=Path('/sys/fs/cgroup/memory')/alternativos[campo]
        return int(p.read_text()) if p.exists() else None
    state.update(estado='completado',etapa='finalizado',pipeline_s=pipeline_s,total_s=time.perf_counter()-inicio,
        registros=env['total'],filas_gold=env['filas'],casos_gold=env['casos_gold'],
        tiempos=env['tiempos'],spill_memoria_bytes=suma('memoryBytesSpilled'),spill_disco_bytes=suma('diskBytesSpilled'),
        shuffle_write_bytes=suma('shuffleWriteBytes'),shuffle_read_bytes=suma('shuffleReadBytes'),
        memoria_pico_contenedor_bytes=memoria('memory.peak'),memoria_actual_contenedor_bytes=memoria('memory.current'),
        tamanos_bytes={c:tamano(BASE/c) for c in ['raw','bronze','silver','gold']})
    guardar();spark.stop()
except Exception as error:
    state.update(estado='fallido',error_tipo=type(error).__name__,error=str(error),total_s=time.perf_counter()-inicio)
    guardar();raise
finally:
    stop.set()
