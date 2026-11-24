"""Consolidar las 12 mediciones y copiar evidencia agregada sin datos personales."""
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
MET=ROOT/'data/metricas/optimizacion_controlada'
OUT=ROOT/'docs/evidencias/optimizacion'
OUT.mkdir(parents=True,exist_ok=True)
grupos=defaultdict(list)
etapas=defaultdict(list)
for cache in ('False','True'):
    for sp in (8,200):
        for r in (1,2,3):
            p=MET/f'tiempos_cache{cache}_sp{sp}_r{r}.csv'
            with p.open(newline='') as f:
                filas=list(csv.DictReader(f))
            total=next(float(x['segundos']) for x in filas if x['etapa']=='TOTAL')
            grupos[(cache,sp)].append(total)
            for x in filas:
                etapas[(cache,sp,x['etapa'])].append(float(x['segundos']))
            (OUT/p.name).write_bytes(p.read_bytes())
resumen=[]
for (cache,sp), vals in grupos.items():
    resumen.append({'cache':cache,'shuffle_partitions':sp,'corridas':len(vals),'mediana_s':round(statistics.median(vals),2),'minimo_s':round(min(vals),2),'maximo_s':round(max(vals),2)})
with (OUT/'resumen.csv').open('w',newline='') as f:
    w=csv.DictWriter(f,fieldnames=list(resumen[0]));w.writeheader();w.writerows(resumen)
medianas=[{'cache':c,'shuffle_partitions':sp,'etapa':e,'mediana_s':round(statistics.median(vals),2)} for (c,sp,e),vals in etapas.items()]
with (OUT/'etapas.csv').open('w',newline='') as f:
    w=csv.DictWriter(f,fieldnames=list(medianas[0]));w.writeheader();w.writerows(medianas)
(OUT/'protocolo.json').write_bytes((ROOT/'output/optimizacion/protocolo.json').read_bytes())
for fila in resumen:print(fila)
for sp in (8,200):
    sin=statistics.median(grupos[('False',sp)]); con=statistics.median(grupos[('True',sp)])
    print(f'Cache con shuffle {sp}: reduccion de tiempo {(1-con/sin)*100:.1f}%, razon sin/con {sin/con:.2f}')
print('TOTAL suma etapas instrumentadas; no incluye descarga, arranque, todas las validaciones ni escalabilidad.')
