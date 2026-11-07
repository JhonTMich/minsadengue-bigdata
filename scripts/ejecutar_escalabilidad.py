"""Supervisar ×1, ×10 y ×100 en contenedores con recursos limitados.
Ejecutar en el host: uv run python scripts/ejecutar_escalabilidad.py.
"""
import argparse
import json
import shutil
import os
import time
import subprocess
from datetime import datetime,timezone
from pathlib import Path

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--factores',type=int,nargs='+',default=[1,10,100],choices=[1,10,100])
parser.add_argument('--timeout',type=int,default=1200,help='Segundos máximos por factor, incluida generación')
parser.add_argument('--reuse-raw',action='store_true',help='Reusar CSV previamente generado si coinciden tamaño y cabecera; validar conteos al terminar')
args=parser.parse_args()
if args.timeout<1:parser.error('timeout debe ser positivo')
ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'output/escalabilidad_completa';OUT.mkdir(parents=True,exist_ok=True)
compose=json.loads(subprocess.check_output(['docker','compose','--file',str(ROOT/'docker-compose.yml'),'config','--format','json'],text=True))
referencia=compose['services']['pyspark-notebook']['image']
image=subprocess.check_output(['docker','image','inspect',referencia,'--format','{{.Id}}'],text=True).strip()
for factor in args.factores:
    nombre=f'geresa-escala-x{factor}'
    existe=subprocess.run(['docker','inspect',nombre],capture_output=True)
    if existe.returncode==0:raise RuntimeError(f'El contenedor {nombre} ya existe; no se modificará')
    base=ROOT/f'data/escalabilidad_completa/x{factor}';base.mkdir(parents=True,exist_ok=True)
    if (base/'supervision.json').exists():
        archivo=base/'intentos'/datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        archivo.mkdir(parents=True,exist_ok=True)
        for item in ['estado.json','supervision.json','recursos.json']:
            if (base/item).exists():shutil.copyfile(base/item,archivo/item)
    (base/'estado.json').write_text(json.dumps({'factor':factor,'estado':'pendiente','etapa':'crear_contenedor'}))
    protocolo={'factor':factor,'image':image,'timeout_s':args.timeout,'memoria_limite':'4g','cpus':2,'inicio_utc':datetime.now(timezone.utc).isoformat()}
    (base/'supervision.json').write_text(json.dumps(protocolo,indent=2))
    cmd=['docker','run','--name',nombre,'--memory','4g','--memory-swap','4g','--cpus','2','--pids-limit','512',
         '-e','SPARK_LOCAL_IP=127.0.0.1','-v',f'{ROOT}:/home/jovyan/work','-w','/home/jovyan/work',
         '--entrypoint','python',image,'scripts/escalabilidad_worker.py','--factor',str(factor)]
    if args.reuse_raw:cmd.append('--reuse-raw')
    print(f'INICIO x{factor}; límite {args.timeout}s / 4 GiB / 2 CPU',flush=True)
    with (OUT/f'x{factor}.log').open('w',encoding='utf-8') as log:
        proc=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT)
        try:
            limite=time.monotonic()+args.timeout
            while proc.poll() is None:
                if os.name=='nt' and shutil.disk_usage(os.environ.get('SystemDrive','C:')+'\\').free < 2*1024**3:
                    protocolo['interrupcion_disco_host']=True
                    subprocess.run(['docker','stop','--timeout','10',nombre],check=False,stdout=subprocess.DEVNULL)
                    break
                restante=limite-time.monotonic()
                if restante<=0:raise subprocess.TimeoutExpired(cmd,args.timeout)
                try:proc.wait(timeout=min(3,restante))
                except subprocess.TimeoutExpired:pass
            protocolo['exit_code']=proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            protocolo['timeout']=True
            subprocess.run(['docker','stop','--timeout','10',nombre],check=False,stdout=subprocess.DEVNULL)
            protocolo['exit_code']=proc.wait(timeout=30)
    inspeccion=subprocess.run(['docker','inspect',nombre,'--format','{{json .State}}'],text=True,capture_output=True)
    if inspeccion.returncode==0:
        protocolo['container_state']=json.loads(inspeccion.stdout)
        subprocess.run(['docker','rm',nombre],check=True,stdout=subprocess.DEVNULL)
    protocolo['fin_utc']=datetime.now(timezone.utc).isoformat()
    (base/'supervision.json').write_text(json.dumps(protocolo,indent=2))
    print(f'FIN x{factor}: exit={protocolo["exit_code"]}, timeout={protocolo.get("timeout",False)}, OOM={protocolo.get("container_state",{}).get("OOMKilled")}',flush=True)
    if protocolo.get('container_state',{}).get('OOMKilled') or protocolo.get('timeout') or protocolo['exit_code']!=0:
        print('Se conserva el fallo; no se escalan más factores en esta ejecución.',flush=True)
        break
