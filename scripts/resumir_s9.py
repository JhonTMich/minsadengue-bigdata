"""Resumen del experimento S9 en el README, sin volver a entrenar."""
import csv,json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'docs/evidencias/s9'
rows=list(csv.DictReader((OUT/'resultados.csv').open()))
assert len(rows)==18 and len({(r['corte'],r['experimento']) for r in rows})==9
strict=[r for r in rows if r['subconjunto']=='sin brote actual']
text=['## Ingeniería de variables S9','',
'El módulo de variables compara tres logísticas: casos; casos más departamento y estacionalidad; y ese mismo contexto más clima. Departamento se transforma con StringIndexer y OneHotEncoder. Se escalan solo las variables numéricas y se ensamblan con la codificación categórica. La comparación de contexto frente a contexto más clima aísla el aporte climático sin confundirlo con añadir geografía o calendario.','',
'Los niveles climáticos absolutos y sus anomalías se resumen en ventanas de cuatro semanas: 0–3, 4–7 y 8–11. La estacionalidad usa seno/coseno del día del año de la fecha de inicio de semana, con periodo 365.2425 días. La etiqueta conserva la definición del parcial: Q3 de cinco años previos, mínimo cinco casos y horizonte futuro de cuatro semanas.','',
'Cada corte valida un calendario completo desde 2000, claves únicas, tipos, valores válidos y ausencia de nulos en clima. La cohorte se selecciona por al menos 1000 casos históricos anteriores al corte de entrenamiento con embargo; la climatología utiliza únicamente las filas de entrenamiento de 2005 en adelante. Las ventanas requieren cuatro observaciones válidas. El indexado, escalado y modelo se ajustan exclusivamente con train.','',
'La selección histórica produce 13, 14 y 17 departamentos en los tres cortes, respectivamente. Las cohortes son distintas de los 19 departamentos del parcial: estos números no deben compararse directamente con las métricas anteriores. Dentro de cada corte, los tres experimentos usan exactamente las mismas filas.','',
'### Resultados sin brote vigente','',
'| Validación | N | AUC casos | AUC contexto | AUC contexto + clima | AP contexto | AP contexto + clima |',
'|---|---:|---:|---:|---:|---:|---:|']
auc=ap=0
for corte in sorted({r['corte'] for r in strict}):
    g={r['experimento']:r for r in strict if r['corte']==corte}
    a,b,c=g['casos'],g['contexto'],g['contexto_clima']
    assert a['n']==b['n']==c['n']
    auc+=float(c['auc_roc'])>float(b['auc_roc']);ap+=float(c['average_precision'])>float(b['average_precision'])
    _,ini,fin=corte.split('_')
    text.append('| '+' | '.join([f'{ini}–{fin}',a['n']]+[f'{float(r[k]):.4f}' for r,k in [(a,'auc_roc'),(b,'auc_roc'),(c,'auc_roc'),(b,'average_precision'),(c,'average_precision')]])+' |')
text += ['',f'Agregar clima al contexto mejora AUC en {auc} de tres cortes y average precision en {ap} de tres. No hay una mejora uniforme; incorporar departamento y estacionalidad tampoco garantiza superar casos solos. Las probabilidades proceden de una logística ponderada y no se presentan como calibradas para uso operativo. El umbral se mantuvo fijo en 0.5, sin búsqueda de hiperparámetros ni elección con 2021–2024.','',
'Las validaciones son exploratorias; el test 2021–2024 ya se inspeccionó durante el parcial y no se utiliza en este avance. Se supone que casos y clima de t están disponibles al emitir la señal; el retraso de notificación no se modela.','',
'### Reproducción y artefactos','',
'El notebook `notebooks/ingenieriaVariables.ipynb` utiliza funciones compartidas de `scripts/ingenieria_variables.py`. Ejecutar en un kernel nuevo con Gold disponible. También se puede ejecutar desde terminal:','',
'```bash',
'docker compose exec -T pyspark-notebook python /home/jovyan/work/scripts/ejecutar_s9.py',
'docker compose exec -T pyspark-notebook python /home/jovyan/work/tests/test_ingenieria_variables.py',
'uv run python scripts/resumir_s9.py',
'```','',
'El ejecutor usa Spark local[2], driver de 2 GB y ocho particiones de shuffle. Las métricas, protocolos y diccionario se versionan en `docs/evidencias/s9/`; los nueve PipelineModel y las probabilidades quedan en `output/s9/`. Los modelos guardados incluyen indexado, escalado, ensamblado y logística; requieren las variables temporales ya construidas por el módulo, no aceptan directamente una fila Gold sin preparar. No son todavía un servicio de predicción autónomo.','',
'En cada entrenamiento se guardó y recargó el PipelineModel y se verificó igualdad de probabilidades con tolerancia 1e-12. Las pruebas de contrato rechazan claves duplicadas, humedad nula y semanas inexistentes; verifican límites del calendario epidemiológico y comprueban que modificar casos y temperatura de validación no altera las variables ni etiquetas de entrenamiento. Las pruebas usan copias de Gold y no modifican datos ni evidencias del análisis.','']
p=ROOT/'README.md';s=p.read_text(encoding='utf-8')
a='<!-- s9:inicio -->';b='<!-- s9:fin -->';block=a+'\n'+'\n'.join(text)+b+'\n\n'
if a in s:
    i=s.index(a);j=s.index(b,i)+len(b);s=s[:i]+block.rstrip()+s[j:]
else:s=s.replace('## Limitaciones, conclusiones y trabajo pendiente',block+'## Limitaciones, conclusiones y trabajo pendiente')
p.write_text(s,encoding='utf-8')
print('README actualizado con resultados S9')
