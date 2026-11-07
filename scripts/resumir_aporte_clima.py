"""Resumen descriptivo pareado; no ajusta modelos ni elige umbrales con test."""
from pathlib import Path
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score, confusion_matrix

ROOT = Path(__file__).resolve().parents[1]
MET = ROOT / 'data/metricas'
df = pd.read_csv(MET / 'comparacion_clima_predicciones.csv', dtype={'cod_dep': str})
filas = []
for subconjunto, datos in [('completo', df), ('sin brote actual', df[df.brote_actual == 0])]:
    grupos = [('total', 'todos', datos)]
    grupos += [('ano', str(k), g) for k,g in datos.groupby('ano')]
    grupos += [('departamento', str(k), g) for k,g in datos.groupby('departamento')]
    for dimension, valor, g in grupos:
        y = g.label.astype(int)
        for modelo in ['prob_casos', 'prob_casos_clima', 'prob_clima']:
            tn, fp, fn, tp = confusion_matrix(y, g[modelo] >= 0.5, labels=[0,1]).ravel()
            filas.append({'subconjunto': subconjunto, 'dimension': dimension, 'grupo': valor,
                'modelo': modelo, 'n': len(g), 'positivos': int(y.sum()), 'prevalencia': y.mean(),
                'auc_roc': roc_auc_score(y, g[modelo]) if y.nunique() == 2 else None,
                'average_precision': average_precision_score(y, g[modelo]) if y.nunique() == 2 else None,
                'precision': tp/(tp+fp) if tp+fp else None,
                'recall': tp/(tp+fn) if tp+fn else None,
                'vp': tp, 'fp': fp, 'fn': fn, 'vn': tn})
r = pd.DataFrame(filas)
r.to_csv(MET / 'comparacion_clima_desglose.csv', index=False)
print(r[r.dimension == 'total'].round(4).to_string(index=False))
print('\nAUC por año, semanas sin brote actual:')
print(r[(r.dimension == 'ano') & (r.subconjunto == 'sin brote actual')].pivot(index='grupo',columns='modelo',values='auc_roc').round(4).to_string())
print('\nAverage precision usa sklearn; no equivale exactamente al área PR interpolada de Spark.')
