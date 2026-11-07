"""Consolidar resultados del pipeline completo sin copiar datos individuales."""
import csv
import json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'docs/evidencias/escalabilidad_completa';OUT.mkdir(parents=True,exist_ok=True)
rows=[]
for factor in [1,10,100]:
    base=ROOT/f'data/escalabilidad_completa/x{factor}'
    estado=json.loads((base/'estado.json').read_text())
    supervisor=json.loads((base/'supervision.json').read_text())
    if 'fin_utc' not in supervisor:raise RuntimeError(f'x{factor} todavía está en ejecución')
    destino=OUT/f'x{factor}';destino.mkdir(exist_ok=True)
    for name in ['estado.json','supervision.json','recursos.json']:
        if (base/name).exists():(destino/name).write_bytes((base/name).read_bytes())
    status=estado['estado']
    if supervisor.get('interrupcion_disco_host'):status='interrumpido_disco_host'
    elif supervisor.get('timeout'):status='timeout'
    elif supervisor.get('container_state',{}).get('OOMKilled'):status='oom'
    elif supervisor['exit_code']!=0:status='fallido'
    row={'factor':factor,'estado':status,'etapa_final':estado['etapa'],
         'registros':estado.get('registros'),'generacion_s':estado.get('generacion_s'),
         'pipeline_s':estado.get('pipeline_s') if status=='completado' else None,
         'total_s':estado.get('total_s'),'filas_gold':estado.get('filas_gold'),
         'memoria_pico_contenedor_bytes':estado.get('memoria_pico_contenedor_bytes'),
         'spill_memoria_bytes':estado.get('spill_memoria_bytes'),
         'spill_disco_bytes':estado.get('spill_disco_bytes'),
         'shuffle_write_bytes':estado.get('shuffle_write_bytes')}
    for capa in ['raw','bronze','silver','gold']:row[f'{capa}_bytes']=estado.get('tamanos_bytes',{}).get(capa)
    rows.append(row)
with (OUT/'resumen.csv').open('w',newline='') as f:
    w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
for row in rows:print(row)
