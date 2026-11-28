"""Genera el tablero de Grafana (JSON importable) sobre las vistas de Athena.

Uso: python nube/tablero/grafana/generar_tablero.py
Escribe nube/tablero/grafana/tablero_dengue.json. Las consultas leen solo las
cuatro vistas de indicadores de nube/sql/02_indicadores.sql.
"""

import json
from pathlib import Path

BASE = "minsadengue_gold"
SALIDA = Path(__file__).resolve().parent / "tablero_dengue.json"
DS = {"type": "grafana-athena-datasource", "uid": "${DS_ATHENA}"}
CONEXION = {"region": "__default", "catalog": "__default", "database": "__default",
            "resultReuseEnabled": False, "resultReuseMaxAgeInMinutes": 60}
TABLA, SERIE = 1, 0
DEPTOS = "${departamento:singlequote}"
MESES = "ARRAY['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Set','Oct','Nov','Dic']"

ULTIMA_SEMANA = f"(SELECT max(semana) FROM {BASE}.v_ranking_riesgo WHERE ano = ${{ano}})"

CONSULTAS = {
    "corte": f"""SELECT ano AS "Año", max(semana) AS "Semana de corte"
FROM {BASE}.v_ranking_riesgo WHERE ano = ${{ano}} GROUP BY ano""",

    "situacion": f"""SELECT departamentos_en_alerta AS "En alerta",
       departamentos_en_brote AS "En brote",
       departamentos_evaluados AS "Evaluados"
FROM {BASE}.v_departamentos_en_alerta
WHERE ano = ${{ano}} AND semana = {ULTIMA_SEMANA}""",

    "evolucion": f"""SELECT semana, departamentos_en_alerta AS "En alerta",
       departamentos_en_brote AS "En brote"
FROM {BASE}.v_departamentos_en_alerta
WHERE ano = ${{ano}} ORDER BY semana""",

    "ranking": f"""SELECT puesto AS "Puesto", departamento AS "Departamento",
       probabilidad AS "Puntaje",
       CASE WHEN alerta THEN 'Sí' ELSE 'No' END AS "Alerta",
       CASE WHEN brote_actual = 1 THEN 'Sí' ELSE 'No' END AS "Brote vigente",
       casos AS "Casos", umbral_q3 AS "Umbral Q3"
FROM {BASE}.v_ranking_riesgo
WHERE ano = ${{ano}} AND semana = {ULTIMA_SEMANA}
  AND departamento IN ({DEPTOS})
ORDER BY puesto""",

    "canal_serie": f"""SELECT semana, sum(casos) AS "Casos", sum(umbral_q3) AS "Umbral Q3"
FROM {BASE}.v_casos_vs_canal
WHERE ano = ${{ano}} AND departamento IN ({DEPTOS})
GROUP BY semana ORDER BY semana""",

    "canal_tabla": f"""SELECT departamento AS "Departamento", casos AS "Casos",
       umbral_q3 AS "Umbral Q3", razon_canal AS "Casos / Q3", zona AS "Zona"
FROM {BASE}.v_casos_vs_canal
WHERE ano = ${{ano}} AND semana = {ULTIMA_SEMANA}
  AND departamento IN ({DEPTOS})
ORDER BY razon_canal DESC NULLS LAST""",

    "desempeno": f"""SELECT sum(semanas) AS "Observaciones",
       sum(aciertos) AS "VP", sum(falsas_alarmas) AS "FP",
       sum(brotes_no_detectados) AS "FN",
       sum(semanas) - sum(aciertos) - sum(falsas_alarmas) - sum(brotes_no_detectados) AS "VN",
       CAST(sum(aciertos) AS double) / NULLIF(sum(brotes), 0) AS "Recall",
       CAST(sum(aciertos) AS double) / NULLIF(sum(alertas), 0) AS "Precisión"
FROM {BASE}.v_desempeno_alertas
WHERE ano = ${{ano}} AND subconjunto = '${{evaluacion}}' AND NOT dentro_entrenamiento""",

    "desempeno_mes": f"""SELECT element_at({MESES}, m) AS "Mes", recall AS "Recall", precision AS "Precisión"
FROM (SELECT mes AS m,
             CAST(sum(aciertos) AS double) / NULLIF(sum(brotes), 0) AS recall,
             CAST(sum(aciertos) AS double) / NULLIF(sum(alertas), 0) AS precision
      FROM {BASE}.v_desempeno_alertas
      WHERE ano = ${{ano}} AND subconjunto = '${{evaluacion}}' AND NOT dentro_entrenamiento
      GROUP BY mes)
ORDER BY m""",
}

VARIABLES_SQL = {
    "ano": f"SELECT DISTINCT CAST(ano AS varchar) FROM {BASE}.v_ranking_riesgo ORDER BY 1 DESC",
    "departamento": f"SELECT departamento FROM {BASE}.dim_departamento ORDER BY 1",
}


def objetivo(clave, formato=TABLA):
    return {"refId": "A", "datasource": DS, "format": formato,
            "rawSQL": CONSULTAS[clave], "connectionArgs": CONEXION}


def panel(id_, tipo, titulo, clave, pos, descripcion="", opciones=None, campos=None, formato=TABLA):
    return {"id": id_, "type": tipo, "title": titulo, "description": descripcion,
            "datasource": DS, "gridPos": dict(zip("xywh", pos)),
            "targets": [objetivo(clave, formato)],
            "options": opciones or {}, "fieldConfig": campos or {"defaults": {}, "overrides": []}}


def fila(id_, titulo, y):
    return {"id": id_, "type": "row", "title": titulo, "collapsed": False,
            "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []}


def stat(color="blue", unidad="none", decimales=0, umbrales=None):
    pasos = umbrales or [{"color": color, "value": None}]
    return {"defaults": {"unit": unidad, "decimals": decimales, "color": {"mode": "thresholds"},
                         "thresholds": {"mode": "absolute", "steps": pasos}}, "overrides": []}


def opciones_stat(campos=""):
    return {"reduceOptions": {"calcs": ["lastNotNull"], "fields": campos, "values": False},
            "colorMode": "background", "graphMode": "none", "textMode": "value_and_name",
            "justifyMode": "center", "orientation": "horizontal"}


def color_texto(campo, colores):
    mapeo = {"type": "value", "options": {v: {"color": c, "index": i}
                                          for i, (v, c) in enumerate(colores.items())}}
    return {"matcher": {"id": "byName", "options": campo},
            "properties": [{"id": "mappings", "value": [mapeo]},
                           {"id": "custom.cellOptions", "value": {"type": "color-background"}}]}


def lineas(x):
    return {"xField": x, "legend": {"showLegend": True, "displayMode": "list", "placement": "bottom"},
            "tooltip": {"mode": "multi", "sort": "none"}}


def panel_trend(id_, titulo, clave, pos, descripcion, colores, eje):
    overrides = [{"matcher": {"id": "byName", "options": n},
                  "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": c}}]}
                 for n, c in colores.items()]
    campos = {"defaults": {"custom": {"lineWidth": 2, "fillOpacity": 10, "showPoints": "never",
                                      "axisLabel": eje}, "min": 0}, "overrides": overrides}
    return panel(id_, "trend", titulo, clave, pos, descripcion, lineas("semana"), campos)


def paneles():
    p = []
    p.append(fila(1, "Panorama de la semana de corte", 0))
    p.append(panel(2, "stat", "Semana de corte", "corte", (0, 1, 6, 5),
                   "Última semana epidemiológica con puntaje del modelo en el año elegido.",
                   opciones_stat(), stat("blue")))
    p.append(panel(3, "stat", "Departamentos en la semana de corte", "situacion", (6, 1, 18, 5),
                   "Los 19 departamentos del panel. Alerta: puntaje sobre el umbral del modelo "
                   "(brote en las 4 semanas siguientes). Brote vigente: casos sobre el canal "
                   "endémico Q3 y al menos 5 casos.", opciones_stat(),
                   {"defaults": {"color": {"mode": "fixed"}}, "overrides": [
                       {"matcher": {"id": "byName", "options": n},
                        "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": c}}]}
                       for n, c in {"En alerta": "orange", "En brote": "red", "Evaluados": "blue"}.items()]}))
    p.append(panel_trend(4, "Departamentos en alerta y en brote por semana", "evolucion", (0, 6, 24, 8),
                         "Conteo semanal sobre los 19 departamentos.",
                         {"En alerta": "orange", "En brote": "red"}, "Departamentos"))

    p.append(fila(5, "Ranking de riesgo", 14))
    ranking_campos = {"defaults": {"custom": {"align": "auto"}}, "overrides": [
        {"matcher": {"id": "byName", "options": "Puntaje"},
         "properties": [{"id": "custom.cellOptions", "value": {"type": "gauge", "mode": "basic"}},
                        {"id": "min", "value": 0}, {"id": "max", "value": 1}, {"id": "decimals", "value": 2},
                        {"id": "color", "value": {"mode": "continuous-YlRd"}}]},
        {"matcher": {"id": "byName", "options": "Umbral Q3"},
         "properties": [{"id": "decimals", "value": 1}]},
        color_texto("Alerta", {"Sí": "orange", "No": "transparent"}),
        color_texto("Brote vigente", {"Sí": "red", "No": "transparent"}),
    ]}
    p.append(panel(6, "table", "Ranking de riesgo en la semana de corte", "ranking", (0, 15, 24, 11),
                   "Departamentos ordenados por el puntaje del modelo. El puntaje viene de una "
                   "regresión logística ponderada: ordena el riesgo, pero no es una probabilidad "
                   "calibrada.", {"showHeader": True, "cellHeight": "sm"}, ranking_campos))

    p.append(fila(7, "Casos frente al canal endémico", 26))
    p.append(panel_trend(8, "Casos y umbral Q3 por semana", "canal_serie", (0, 27, 14, 10),
                         "Suma de los departamentos elegidos. Con varios departamentos, la línea Q3 "
                         "es la suma de sus umbrales, no un canal nacional.",
                         {"Casos": "red", "Umbral Q3": "blue"}, "Casos"))
    canal_campos = {"defaults": {}, "overrides": [
        {"matcher": {"id": "byName", "options": "Casos / Q3"},
         "properties": [{"id": "decimals", "value": 2},
                        {"id": "custom.cellOptions", "value": {"type": "color-text"}},
                        {"id": "thresholds", "value": {"mode": "absolute", "steps": [
                            {"color": "green", "value": None}, {"color": "red", "value": 1}]}}]},
        {"matcher": {"id": "byName", "options": "Umbral Q3"},
         "properties": [{"id": "decimals", "value": 1}]},
        color_texto("Zona", {"Brote": "red", "Sobre el canal, menos de 5 casos": "orange",
                             "Dentro del canal": "transparent"}),
    ]}
    p.append(panel(9, "table", "Razón casos / Q3 en la semana de corte", "canal_tabla", (14, 27, 10, 10),
                   "Mayor que 1: casos sobre el canal endémico. Vacío si el umbral es 0.",
                   {"showHeader": True, "cellHeight": "sm"}, canal_campos))

    p.append(fila(10, "Desempeño de las alertas", 37))
    pct = [{"id": "unit", "value": "percentunit"}, {"id": "decimals", "value": 1},
           {"id": "color", "value": {"mode": "fixed", "fixedColor": "purple"}}]
    desempeno_campos = {"defaults": {"color": {"mode": "fixed", "fixedColor": "blue"}, "decimals": 0},
                        "overrides": [{"matcher": {"id": "byName", "options": n}, "properties": pct}
                                      for n in ("Recall", "Precisión")]}
    p.append(panel(11, "stat", "Desempeño en semanas cerradas", "desempeno", (0, 38, 24, 5),
                   "Semanas-departamento fuera del entrenamiento con desenlace observado. "
                   "Recall = VP / (VP + FN); precisión = VP / (VP + FP). Vacío en años que el "
                   "modelo usó para entrenar.", opciones_stat(), desempeno_campos))
    mes_campos = {"defaults": {"unit": "percentunit", "min": 0, "max": 1, "decimals": 0},
                  "overrides": [{"matcher": {"id": "byName", "options": n},
                                 "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": c}}]}
                                for n, c in {"Recall": "green", "Precisión": "purple"}.items()]}
    p.append(panel(12, "barchart", "Recall y precisión por mes", "desempeno_mes", (0, 43, 24, 9),
                   "Calculados con los conteos de cada mes, no promediando porcentajes.",
                   {"xField": "Mes", "orientation": "vertical", "showValue": "auto",
                    "legend": {"showLegend": True, "displayMode": "list", "placement": "bottom"},
                    "tooltip": {"mode": "multi"}}, mes_campos))
    return p


def variable_sql(nombre, etiqueta, multi=False, todos=False, actual=None):
    sql = VARIABLES_SQL[nombre]
    return {"name": nombre, "label": etiqueta, "type": "query", "datasource": DS,
            "query": {"refId": nombre, "rawSQL": sql, "format": TABLA, "connectionArgs": CONEXION},
            "definition": sql, "refresh": 1, "sort": 0, "multi": multi, "includeAll": todos,
            "current": actual or {}, "options": [], "hide": 0}


def tablero():
    evaluacion = {"Sin brote vigente": "sin_brote_actual", "Todas las semanas": "completo"}
    return {
        "__inputs": [{"name": "DS_ATHENA", "label": "Amazon Athena", "type": "datasource",
                      "pluginId": "grafana-athena-datasource", "pluginName": "Amazon Athena"}],
        "__requires": [{"type": "datasource", "id": "grafana-athena-datasource", "name": "Amazon Athena"}],
        "uid": "minsadengue-alertas",
        "title": "Alerta de brotes de dengue · Perú",
        "description": "Grupo 13 · Gold en S3 consultado con Athena (minsadengue_gold).",
        "tags": ["dengue", "athena", "minsadengue"],
        "timezone": "utc", "editable": True, "graphTooltip": 1, "refresh": "",
        "time": {"from": "now-1y", "to": "now"}, "timepicker": {"hidden": True},
        "schemaVersion": 39, "version": 1,
        "templating": {"list": [
            variable_sql("ano", "Año", actual={"text": "2025", "value": "2025"}),
            variable_sql("departamento", "Departamento", multi=True, todos=True,
                         actual={"text": ["All"], "value": ["$__all"]}),
            {"name": "evaluacion", "label": "Evaluación", "type": "custom", "hide": 0,
             "query": ",".join(f"{k} : {v}" for k, v in evaluacion.items()),
             "current": {"text": "Sin brote vigente", "value": "sin_brote_actual"},
             "options": [{"text": k, "value": v, "selected": i == 0}
                         for i, (k, v) in enumerate(evaluacion.items())],
             "multi": False, "includeAll": False},
        ]},
        "panels": paneles(),
    }


if __name__ == "__main__":
    SALIDA.write_text(json.dumps(tablero(), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Escrito {SALIDA}")
