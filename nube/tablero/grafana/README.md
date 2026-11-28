# Tablero de dengue en Grafana

Reemplaza al tablero de Power BI Desktop: Grafana Cloud corre en la nube y
consulta Gold con Athena, sin nada instalado en la laptop.

`tablero_dengue.json` es el tablero importable. Lo genera
`generar_tablero.py`; para cambiarlo, editar el generador y volver a correrlo:

```powershell
python nube/tablero/grafana/generar_tablero.py
```

## Qué muestra

Se elige **Año**, **Departamento** (uno, varios o todos) y **Evaluación**
(sin brote vigente o todas las semanas). La **semana de corte** es la última
con puntaje del modelo en el año elegido.

| Sección | Vista de Athena | Contenido |
| --- | --- | --- |
| Panorama | `v_departamentos_en_alerta` | Departamentos en alerta y en brote en la semana de corte y por semana (los 19) |
| Ranking de riesgo | `v_ranking_riesgo` | Departamentos ordenados por puntaje, con alerta, brote vigente, casos y Q3 |
| Canal endémico | `v_casos_vs_canal` | Casos frente a la suma de los umbrales Q3; razón casos / Q3 por departamento |
| Desempeño | `v_desempeno_alertas` | Observaciones, VP, FP, FN, VN, recall y precisión del año, y por mes (los 19) |

Panorama y Desempeño son de los 19 departamentos: sus vistas no tienen
departamento. El puntaje no es una probabilidad calibrada; el tablero no elige
ni ajusta el umbral de alerta. Desempeño excluye los años usados para entrenar.

## Conciliación con Athena (28/11/2026, modelo `v20261127T015304Z`)

Todas las consultas del tablero se ejecutaron en Athena con el usuario de solo
lectura:

| Año · evaluación | N | VP | FP | FN | VN | Recall | Precisión |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2024 · sin brote vigente | 534 | 34 | 70 | 69 | 361 | 33,0 % | 32,7 % |
| 2025 · sin brote vigente | 834 | 12 | 71 | 32 | 719 | 27,3 % | 14,5 % |
| 2025 · todas las semanas | 931 | 93 | 84 | 34 | 720 | 73,2 % | 52,5 % |

2024 cambió frente al informe anterior (483 observaciones, FP=65) porque la
carga de 2025 completó las etiquetas de las últimas semanas de 2024.

## Conectar Grafana Cloud a Athena

1. En Grafana Cloud: **Connections → Add new connection → Amazon Athena** e
   instalar el plugin.
2. **Add new data source** con:

| Campo | Valor |
| --- | --- |
| Authentication Provider | Access & secret key del usuario de solo lectura |
| Default Region | `us-east-1` |
| Data source | `AwsDataCatalog` |
| Database | `minsadengue_gold` |
| Workgroup | `primary` |
| Output Location | `s3://minsadengue-g13-datalake/athena/resultados/` |

3. **Save & test**.
4. **Dashboards → New → Import**, subir `tablero_dengue.json` y elegir el
   origen Athena del paso 2.

El usuario de Grafana tiene la misma política de solo lectura que Power BI
(`MinsaDenguePowerBISoloLectura`): consulta Athena, lee Glue y Gold y escribe
solo en `athena/resultados/`. Su clave queda guardada en Grafana Cloud, no en
el repositorio ni en este JSON.

Cada panel lanza una consulta a Athena al abrir el tablero o cambiar un filtro.
Gold pesa menos de 1 MB: Athena cobra el mínimo de 10 MB por consulta, del
orden de USD 0,00005.
