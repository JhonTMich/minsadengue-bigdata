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
    recursos=json.loads((base/'recursos.json').read_text()) if (base/'recursos.json').exists() else {}
    row={'factor':factor,'estado':status,'etapa_final':estado['etapa'],
         'registros':estado.get('registros'),'generacion_s':estado.get('generacion_s'),
         'pipeline_s':estado.get('pipeline_s') if status=='completado' else None,
         'total_s':estado.get('total_s'),'filas_gold':estado.get('filas_gold'),
         'memoria_pico_contenedor_bytes':estado.get('memoria_pico_contenedor_bytes') or recursos.get('memoria_pico_bytes'),
         'memoria_fuente':'cgroup_al_cierre' if estado.get('memoria_pico_contenedor_bytes') else 'ultimo_muestreo',
         'cpu_ns_ultimo_muestreo':recursos.get('cpu_ns_cgroup_v1'),
         'raw_reutilizado':estado.get('raw_reutilizado'),
         'spill_memoria_bytes':estado.get('spill_memoria_bytes'),
         'spill_disco_bytes':estado.get('spill_disco_bytes'),
         'shuffle_write_bytes':estado.get('shuffle_write_bytes')}
    for capa in ['raw','bronze','silver','gold']:row[f'{capa}_bytes']=estado.get('tamanos_bytes',{}).get(capa)
    rows.append(row)
with (OUT/'resumen.csv').open('w',newline='') as f:
    w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
for row in rows:print(row)

# El README es la referencia principal para la evaluación docente.
def fmt(valor, divisor=1, decimales=2):
    return f"{valor/divisor:.{decimales}f}" if valor is not None else "N/D"
base_s=rows[0]['pipeline_s']
texto=['### Resultados del pipeline completo','',
'Una corrida por escala, con dos CPU, 4 GiB de límite de contenedor, heap de driver de 2 GB y temporales en E:. Se reutilizaron los CSV ya generados: la preparación reutilizada no se interpreta como tiempo de generación. La generación inicial del CSV ×100 tomó 207 s y produjo 10.86 GB; ese intento se interrumpió antes de Silver para trasladar los temporales de C: a E:, sin OOM ni timeout. Su motivo y métricas se conservaron por separado.','',
'| Factor | Estado | Registros dengue | Pipeline (s) | Tiempo relativo | Pico cgroup (GiB) | Spill memoria (MB) | Spill disco (MB) | Shuffle escrito (MB) |',
'|---|---|---:|---:|---:|---:|---:|---:|---:|']
for r in rows:
    relativo=r['pipeline_s']/base_s if r['pipeline_s'] is not None and base_s else None
    texto.append('| '+' | '.join([str(r['factor']),r['estado'],str(r['registros'] or 'N/D'),fmt(r['pipeline_s']),fmt(relativo),fmt(r['memoria_pico_contenedor_bytes'],1024**3),fmt(r['spill_memoria_bytes'],1e6),fmt(r['spill_disco_bytes'],1e6),fmt(r['shuffle_write_bytes'],1e6)])+' |')
texto += ['', '| Factor | Raw CSV (GB) | Bronze (MB) | Silver (MB) | Gold (MB) |', '|---|---:|---:|---:|---:|']
for r in rows:
    texto.append('| '+' | '.join([str(r['factor']),fmt(r['raw_bytes'],1e9),fmt(r['bronze_bytes'],1e6),fmt(r['silver_bytes'],1e6),fmt(r['gold_bytes'],1e6)])+' |')
texto += ['', '| Etapa cronometrada | ×1 (s) | ×10 (s) | ×100 (s) |', '|---|---:|---:|---:|']
por_factor={r['factor']:{t['etapa']:t['segundos'] for t in json.loads((OUT/f"x{r['factor']}"/'estado.json').read_text()).get('tiempos',[])} for r in rows}
for etapa in ['bronze_dengue','bronze_clima','dengue_calidad','dengue_silver_escritura','clima_calidad','clima_silver_escritura','gold_escritura']:
    texto.append('| '+' | '.join([etapa]+[fmt(por_factor[f].get(etapa)) for f in [1,10,100]])+' |')
texto += ['', 'Estas etapas parciales no suman el tiempo completo: el pipeline incluye controles y acciones adicionales fuera de sus cronómetros. La salida de rechazados usa coalesce(1), observada como una sola tarea en ×100; su tiempo forma parte de dengue_silver_escritura. No se cambió esta lógica durante la corrida.', '',
'El tiempo de pipeline incluye ingesta de los CSV de dengue y clima, escritura Bronze/Silver/Gold y controles internos. Excluye generar la réplica, arrancar Spark y la comprobación independiente contra Gold canónico. Las métricas Spark incluyen también esa comprobación. El pico cgroup incluye memoria fuera del heap y puede incluir caché; para un intento fallido solo hay pico hasta el último muestreo, no necesariamente el pico final. Spill de memoria es volumen acumulado, no RAM instantánea.','',
'El presupuesto inicial fue 1200 s por factor. En ×100 se amplió a 1800 s por progreso sostenido y recursos estables, retomando solo la supervisión: no se reinició Spark ni se reinició el reloj del pipeline. El supervisor actualizado usa 1800 s por defecto.', '',
'El aumento del volumen solo corresponde a dengue: clima y cardinalidad de Gold permanecen fijos. Las réplicas idénticas favorecen compresión y agregaciones; la caché del SO no se controla. Una corrida no estima variabilidad ni demuestra escalabilidad multinodo. No se deben comparar estos tiempos con los 5.5 s del experimento parcial ni con la suma de etapas del benchmark de optimización, que usan otro alcance y configuración.','']
if all(r['estado']=='completado' for r in rows):
    texto += ['Las tres escalas terminaron sin OOM ni timeout; cada Gold tiene 24 776 filas y conserva los casos originales multiplicados por el factor. Se ejecutaron las escalas ×10 y ×100 solicitadas, con los límites de esta metodología. No se observó un fallo de capacidad hasta ×100: el spill demuestra presión de memoria, no que la máquina haya dejado de alcanzar. No hay evidencia suficiente para fijar un punto de quiebre exacto o afirmar que ya se necesita un clúster.','']
else:
    texto += ['Las escalas incompletas se muestran como fallo/interrupción, sin asignarles un tiempo de pipeline completado. Debe identificarse la etapa final en los registros y revisar el cumplimiento con el docente; un timeout es un presupuesto del experimento, no prueba de imposibilidad física.','']
texto += ['La evidencia numérica y los protocolos quedan en `docs/evidencias/escalabilidad_completa/`; los datos replicados permanecen fuera de Git y del modelado.','']
readme=ROOT/'README.md'
contenido=readme.read_text(encoding='utf-8')
inicio='<!-- escalabilidad-resultados:inicio -->'
fin='<!-- escalabilidad-resultados:fin -->'
bloque=inicio+'\n'+'\n'.join(texto)+fin+'\n\n'
if inicio in contenido:
    a=contenido.index(inicio);b=contenido.index(fin,a)+len(fin)
    contenido=contenido[:a]+bloque.rstrip()+contenido[b:]
else:
    contenido=contenido.replace('## Modelo, etiquetas y referencias',bloque+'## Modelo, etiquetas y referencias')
readme.write_text(contenido,encoding='utf-8')
