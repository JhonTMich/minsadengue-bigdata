"""Genera el modelo y las cuatro páginas PBIP; no conecta a AWS ni incrusta datos."""

import hashlib
import json
from pathlib import Path

BASE = Path(__file__).resolve().parent
REPORT = BASE / "indicadores.Report"
MODEL = BASE / "indicadores.SemanticModel"
SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/"
NAVY, TEAL, RED, AMBER, MUTED = "#162E43", "#07857F", "#B34245", "#B27623", "#61778A"
MEASURES = []


def guardar(path, datos):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(datos, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def medida(nombre, expresion, formato="#,0", carpeta="Operación", descripcion=None):
    MEASURES.append({"name": nombre, "expression": expresion, "formatString": formato,
                     "displayFolder": carpeta, "description": descripcion or nombre})


def crear_modelo():
    MEASURES.clear()
    medida("Semana de corte", "CALCULATE(MAX(Hechos[fecha_inicio]), ALLSELECTED(Departamento), KEEPFILTERS(NOT ISBLANK(Hechos[probabilidad])))", "dd MMM yyyy")
    for nombre, expr in [
        ("Departamentos evaluados", "DISTINCTCOUNT(Hechos[cod_dep])"),
        ("Departamentos en alerta", "CALCULATE(DISTINCTCOUNT(Hechos[cod_dep]), KEEPFILTERS(Hechos[alerta] = TRUE()))"),
        ("Departamentos en brote", "CALCULATE(DISTINCTCOUNT(Hechos[cod_dep]), KEEPFILTERS(Hechos[brote_actual] = 1))"),
        ("Casos última semana", "SUM(Hechos[casos])"),
        ("Suma Q3 última semana", "SUM(Hechos[umbral_q3])")]:
        medida(nombre, f"VAR Fecha = [Semana de corte] RETURN IF(NOT ISBLANK(Fecha), COALESCE(CALCULATE({expr}, KEEPFILTERS(Semana[fecha_inicio] = Fecha), KEEPFILTERS(NOT ISBLANK(Hechos[probabilidad]))), 0))")
    medida("Proporción en alerta", "DIVIDE([Departamentos en alerta], [Departamentos evaluados])", "0.0%")
    medida("Puntaje última semana", "VAR Fecha = [Semana de corte] RETURN IF(NOT ISBLANK(Fecha), CALCULATE(MAX(Hechos[probabilidad]), KEEPFILTERS(Semana[fecha_inicio] = Fecha)))", "0.000", descripcion="Puntaje de logística ponderada; no es una probabilidad calibrada.")
    medida("Umbral de alerta", "VAR Fecha = [Semana de corte] RETURN CALCULATE(MAX(Hechos[umbral_alerta]), KEEPFILTERS(Semana[fecha_inicio] = Fecha))", "0.00")
    medida("Puesto de riesgo", "IF(NOT ISBLANK([Puntaje última semana]), RANKX(FILTER(ALLSELECTED(Departamento[departamento]), NOT ISBLANK([Puntaje última semana])), [Puntaje última semana], , DESC, DENSE))")
    medida("Estado de alerta", 'IF(ISBLANK([Puntaje última semana]), "Sin puntaje", IF([Departamentos en alerta] > 0, "Alerta", "Sin alerta"))', "", "Operación")
    medida("Casos notificados", "SUM(Hechos[casos])", carpeta="Casos y canal")
    medida("Suma de canales Q3", "SUM(Hechos[umbral_q3])", carpeta="Casos y canal", descripcion="Suma de umbrales departamentales; no constituye un canal nacional estimado.")
    medida("Razón casos sobre Q3", "DIVIDE([Casos notificados], [Suma de canales Q3])", "0.00", "Casos y canal")
    medida("Dptos. con alerta semanal", "IF(CALCULATE(COUNTROWS(Hechos), KEEPFILTERS(NOT ISBLANK(Hechos[probabilidad]))) > 0, COALESCE(CALCULATE(DISTINCTCOUNT(Hechos[cod_dep]), KEEPFILTERS(Hechos[alerta] = TRUE())), 0))")
    medida("Dptos. en brote semanal", "IF(CALCULATE(COUNTROWS(Hechos), KEEPFILTERS(NOT ISBLANK(Hechos[brote_actual]))) > 0, COALESCE(CALCULATE(DISTINCTCOUNT(Hechos[cod_dep]), KEEPFILTERS(Hechos[brote_actual] = 1)), 0))")

    # Cada razón se construye sobre conteos; no se promedian porcentajes mensuales.
    filtro = '''VAR SinBrote = SELECTEDVALUE(Ambito[ambito], "Sin brote vigente") = "Sin brote vigente"
RETURN COUNTROWS(FILTER(Hechos,
    NOT ISBLANK(Hechos[probabilidad]) && NOT ISBLANK(Hechos[brote_observado]) &&
    Hechos[dentro_entrenamiento] == FALSE() &&
    (NOT SinBrote || Hechos[brote_actual] == 0) {extra}))'''
    for nombre, extra in [
        ("Semanas evaluadas", ""),
        ("Positivas observadas", "&& Hechos[brote_observado] == 1"),
        ("Alertas evaluadas", "&& Hechos[alerta] == TRUE()"),
        ("Aciertos", "&& Hechos[alerta] == TRUE() && Hechos[brote_observado] == 1"),
        ("Falsas alarmas", "&& Hechos[alerta] == TRUE() && Hechos[brote_observado] == 0"),
        ("Positivas no detectadas", "&& Hechos[alerta] == FALSE() && Hechos[brote_observado] == 1"),
        ("Negativas correctas", "&& Hechos[alerta] == FALSE() && Hechos[brote_observado] == 0")]:
        medida(nombre, filtro.replace("{extra}", extra), carpeta="Desempeño")
    for nombre, expr, formato in [
        ("Recall", "DIVIDE([Aciertos], [Positivas observadas])", "0.0%"),
        ("Precisión", "DIVIDE([Aciertos], [Alertas evaluadas])", "0.0%"),
        ("Alertas falsas", "DIVIDE([Falsas alarmas], [Alertas evaluadas])", "0.0%"),
        ("Tasa de alertas evaluadas", "DIVIDE([Alertas evaluadas], [Semanas evaluadas])", "0.0%"),
        ("FP por acierto", "DIVIDE([Falsas alarmas], [Aciertos])", "0.00")]:
        medida(nombre, expr, formato, "Desempeño")
    medida("Sin desenlace (total)", "COUNTROWS(FILTER(Hechos, Hechos[dentro_entrenamiento] == FALSE() && NOT ISBLANK(Hechos[probabilidad]) && ISBLANK(Hechos[brote_observado])))", carpeta="Control")
    medida("Ámbito evaluado", 'SELECTEDVALUE(Ambito[ambito], "Sin brote vigente")', "", "Control")
    medida("Actualización UTC", "MAX(Hechos[actualizado_utc])", "dd/MM/yyyy HH:mm", "Control")
    medida("Versión del modelo", 'CONCATENATEX(FILTER(VALUES(Hechos[version_modelo]), NOT ISBLANK(Hechos[version_modelo])), Hechos[version_modelo], ", ")', "", "Control")

    columnas = {"string": ["cod_dep", "departamento", "version_modelo", "fecha_carga"],
                "int64": ["ano", "semana", "mes", "casos", "brote_actual", "brote_observado"],
                "double": ["temp_media", "hum_media", "precip_total", "umbral_q3", "probabilidad", "umbral_alerta"],
                "boolean": ["alerta", "dentro_entrenamiento"],
                "dateTime": ["fecha_inicio", "fecha_fin", "actualizado_utc"]}
    columns = [{"name": c, "dataType": t, "sourceColumn": c, "summarizeBy": "none", "isHidden": True}
               for t, campos in columnas.items() for c in campos]
    for c in columns:
        if c["dataType"] == "dateTime":
            c["formatString"] = "dd/MM/yyyy"
    hechos = {"name": "Hechos", "columns": columns, "measures": MEASURES,
              "partitions": [{"name": "Hechos", "mode": "import", "source": {
                  "type": "m", "expression": (BASE / "consultas/Hechos.m").read_text(encoding="utf-8")}}]}

    def tabla_calculada(nombre, expression, tipos):
        return {"name": nombre, "columns": [{"name": c, "dataType": t, "type": "calculatedTableColumn",
                   "sourceColumn": f"[{c}]", "summarizeBy": "none"} for c, t in tipos],
                "partitions": [{"name": nombre, "mode": "import", "source": {"type": "calculated", "expression": expression}}]}

    dept = tabla_calculada("Departamento", 'DISTINCT(SELECTCOLUMNS(Hechos, "cod_dep", Hechos[cod_dep], "departamento", Hechos[departamento]))', [("cod_dep", "string"), ("departamento", "string")])
    dept["columns"][0]["isHidden"] = True
    semana = tabla_calculada("Semana", '''DISTINCT(SELECTCOLUMNS(Hechos,
        "fecha_inicio", Hechos[fecha_inicio], "ano", Hechos[ano], "semana", Hechos[semana],
        "mes", Hechos[mes], "AñoMes", FORMAT(Hechos[ano], "0000") & "-" & FORMAT(Hechos[mes], "00"),
        "AñoSemana", FORMAT(Hechos[ano], "0000") & "-S" & FORMAT(Hechos[semana], "00")))''',
        [("fecha_inicio", "dateTime"), ("ano", "int64"), ("semana", "int64"), ("mes", "int64"), ("AñoMes", "string"), ("AñoSemana", "string")])
    semana["columns"][0]["formatString"] = "dd/MM/yyyy"
    semana["columns"][-1]["sortByColumn"] = "fecha_inicio"
    ambito = tabla_calculada("Ambito", 'DATATABLE("ambito", STRING, {{"Sin brote vigente"}, {"Todas las semanas"}})', [("ambito", "string")])
    model = {"name": "indicadores", "compatibilityLevel": 1601, "model": {
        "culture": "es-PE", "sourceQueryCulture": "es-PE", "defaultPowerBIDataSourceVersion": "powerBI_V3",
        "dataAccessOptions": {"legacyRedirects": True, "returnErrorValuesAsNull": True},
        "tables": [hechos, dept, semana, ambito], "relationships": [
            {"name": "Hechos_Departamento", "fromTable": "Hechos", "fromColumn": "cod_dep",
             "toTable": "Departamento", "toColumn": "cod_dep", "fromCardinality": "many", "toCardinality": "one", "crossFilteringBehavior": "oneDirection"},
            {"name": "Hechos_Semana", "fromTable": "Hechos", "fromColumn": "fecha_inicio",
             "toTable": "Semana", "toColumn": "fecha_inicio", "fromCardinality": "many", "toCardinality": "one", "crossFilteringBehavior": "oneDirection"}],
        "expressions": [{"name": k, "kind": "m", "expression": json.dumps(v) + ' meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]'}
                        for k, v in [("pDSN", "minsadengue_athena"), ("pBaseDatos", "minsadengue_gold")]],
        "annotations": [{"name": "PBI_QueryOrder", "value": '["pDSN","pBaseDatos","Hechos"]'}]}}
    guardar(MODEL / "definition.pbism", {"$schema": "https://developer.microsoft.com/json-schemas/fabric/item/semanticModel/definitionProperties/1.0.0/schema.json", "version": "1.0", "settings": {}})
    guardar(MODEL / "model.bim", model)


def ident(nombre):
    return hashlib.sha256(nombre.encode()).hexdigest()[:20]


def literal(value):
    if isinstance(value, bool):
        value = "true" if value else "false"
    elif isinstance(value, (float, int)):
        value = f"{value}D"
    else:
        value = "'" + str(value).replace("'", "''") + "'"
    return {"expr": {"Literal": {"Value": value}}}


def color(value):
    return {"solid": {"color": literal(value)}}


def objeto(**properties):
    return [{"properties": properties}]


def campo(tabla, nombre, measure=False):
    return {"Measure" if measure else "Column": {"Expression": {"SourceRef": {"Entity": tabla}}, "Property": nombre}}


def proyeccion(tabla, nombre, measure=False):
    return {"field": campo(tabla, nombre, measure), "queryRef": f"{tabla}.{nombre}", "nativeQueryRef": nombre}


def contenedor(titulo=None, fondo="#FFFFFF"):
    vco = {"background": objeto(show=literal(True), color=color(fondo), transparency=literal(0)),
           "border": objeto(show=literal(False)),
           "padding": objeto(top=literal(12), bottom=literal(12), left=literal(14), right=literal(14)),
           "visualHeader": objeto(show=literal(False))}
    if titulo:
        vco["title"] = objeto(show=literal(True), text=literal(titulo), fontColor=color(NAVY),
                              fontSize=literal(14), fontFamily=literal("Segoe UI Semibold"))
    return vco


class Pagina:
    def __init__(self, nombre, titulo, subtitulo, first_id=None):
        self.id = first_id or ident(nombre)
        self.dir = REPORT / "definition/pages" / self.id
        self.n = 0
        guardar(self.dir / "page.json", {"$schema": SCHEMA + "page/2.1.0/schema.json",
            "name": self.id, "displayName": nombre, "displayOption": "FitToPage", "height": 900, "width": 1440,
            "objects": {"background": objeto(color=color("#F2F5F8"), transparency=literal(0))}})
        self.texto("VIGILANCIA\nDE DENGUE", 24, 24, 184, 72, 24, NAVY)
        self.texto("PERÚ · GRUPO 13", 24, 100, 184, 32, 12, MUTED)
        self.visual("pageNavigator", 24, 152, 184, 192, objects={"layout": objeto(orientation=literal(1), columnCount=literal(1), rowCount=literal(4))})
        self.slicer("Semana", "ano", "Año", 24, 380, 184)
        self.slicer("Departamento", "departamento", "Departamento", 24, 476, 184)
        self.texto("FUENTE\nGold · Amazon Athena\nImportación por ODBC", 24, 728, 184, 92, 12, MUTED)
        self.texto(titulo, 248, 24, 1160, 48, 28, NAVY)
        self.texto(subtitulo, 248, 80, 1160, 48, 13, MUTED)
        self.texto("Prototipo académico · Puntajes sin calibrar · No es un servicio oficial del MINSA/CDC", 248, 850, 1160, 30, 12, MUTED)

    def visual(self, tipo, x, y, w, h, query=None, objects=None, vco=None, titulo=None, sorts=None, sync=None):
        self.n += 1
        nombre = ident(f"{self.id}:{self.n}")
        visual = {"visualType": tipo, "visualContainerObjects": vco or contenedor(titulo)}
        if query:
            visual["query"] = {"queryState": query}
            if sorts:
                visual["query"]["sortDefinition"] = {"sort": sorts, "isDefaultSort": True}
        if objects:
            visual["objects"] = objects
        if sync:
            visual["syncGroup"] = {"groupName": sync, "fieldChanges": True, "filterChanges": True}
        guardar(self.dir / "visuals" / nombre / "visual.json", {"$schema": SCHEMA + "visualContainer/2.9.0/schema.json",
            "name": nombre, "position": {"x": x, "y": y, "z": self.n * 1000, "width": w, "height": h, "tabOrder": self.n * 1000},
            "visual": visual})

    def texto(self, texto, x, y, w, h, size=14, tono=NAVY):
        paragraphs = [{"textRuns": [{"value": linea, "textStyle": {"fontFamily": "Segoe UI", "fontSize": f"{size}px", "color": tono}}]} for linea in texto.split("\n")]
        self.visual("textbox", x, y, w, h, objects={"general": objeto(paragraphs=paragraphs)},
            vco={"background": objeto(show=literal(False)), "border": objeto(show=literal(False)),
                 "padding": objeto(top=literal(0), bottom=literal(0), left=literal(0), right=literal(0))})

    def slicer(self, tabla, columna, label, x, y, w=184):
        objects = {"data": objeto(mode=literal("Dropdown")),
                   "header": objeto(show=literal(True), text=literal(label), fontColor=color(NAVY), textSize=literal(11)),
                   "selection": objeto(singleSelect=literal(tabla == "Ambito"))}
        valor = "2024L" if columna == "ano" else "'Sin brote vigente'" if tabla == "Ambito" else None
        if valor:
            objects["general"] = objeto(filter={"filter": {"Version": 2, "From": [{"Name": "s", "Entity": tabla, "Type": 0}],
                "Where": [{"Condition": {"In": {"Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": columna}}],
                                                     "Values": [[{"Literal": {"Value": valor}}]]}}}]}})
        self.visual("slicer", x, y, w, 88, {"Values": {"projections": [proyeccion(tabla, columna)]}},
            objects, sync=f"Filtro_{tabla}_{columna}" if tabla != "Ambito" and columna != "semana" else None)

    def tarjetas(self, medidas, y=152):
        self.visual("cardVisual", 248, y, 1160, 120, {"Data": {"projections": [proyeccion("Hechos", m, True) for m in medidas]}},
            {"value": [{"selector": {"id": "default"}, "properties": {"fontSize": literal(30), "fontColor": color(TEAL), "labelDisplayUnits": literal("0")}}],
             "label": [{"selector": {"id": "default"}, "properties": {"fontSize": literal(11), "fontColor": color(MUTED)}}]})

    def tabla(self, campos, x, y, w, h, titulo, orden=None, totales=True):
        self.visual("tableEx", x, y, w, h, {"Values": {"projections": [proyeccion(*c) for c in campos]}},
            {"columnHeaders": objeto(autoSizeColumnWidth=literal(True), columnAdjustment=literal("growToFit"), fontSize=literal(11), fontColor=color(NAVY), backColor=color("#E8EFF4")),
             "values": objeto(fontSize=literal(11)), "total": objeto(totals=literal(totales))}, titulo=titulo,
            sorts=[{"field": campo(*orden), "direction": "Ascending"}] if orden else None)

    def grafico(self, tipo, categoria, medidas, x, y, w, h, titulo, ordenar=None):
        sorts = [{"field": campo("Hechos", ordenar, True), "direction": "Descending"}] if ordenar else None
        self.visual(tipo, x, y, w, h,
            {"Category": {"projections": [proyeccion(*categoria)]},
             "Y": {"projections": [proyeccion("Hechos", m, True) for m in medidas]}},
            objects={"lineStyles": objeto(lineChartType=literal("linear"))} if tipo == "lineChart" else None,
            titulo=titulo, sorts=sorts)


def crear_paginas():
    meta = REPORT / "definition/pages/pages.json"
    first = json.loads(meta.read_text())["pageOrder"][0]
    p1 = Pagina("01 Panorama", "Dengue en perspectiva", "Estado de la última semana disponible dentro de los filtros. Diferenciar alerta y brote vigente.", first)
    p1.slicer("Semana", "semana", "Semana", 24, 572)
    p1.tarjetas(["Departamentos evaluados", "Departamentos en alerta", "Departamentos en brote", "Proporción en alerta"])
    p1.grafico("lineChart", ("Semana", "fecha_inicio"), ["Dptos. con alerta semanal", "Dptos. en brote semanal"], 248, 296, 1160, 330, "Departamentos por semana · alerta del modelo y brote vigente")
    p1.tabla([("Hechos", "Semana de corte", True), ("Hechos", "Umbral de alerta", True), ("Hechos", "Versión del modelo", True), ("Hechos", "Actualización UTC", True)], 248, 650, 1160, 164, "Corte, trazabilidad y última actualización")

    p2 = Pagina("02 Riesgo", "Priorizar la revisión", "Orden por puntaje del modelo. Un puntaje alto no equivale a una probabilidad calibrada.")
    p2.slicer("Semana", "semana", "Semana", 24, 572)
    p2.tarjetas(["Semana de corte", "Umbral de alerta", "Departamentos en alerta"])
    p2.grafico("clusteredBarChart", ("Departamento", "departamento"), ["Puntaje última semana"], 248, 296, 440, 518, "Puntaje por departamento", "Puntaje última semana")
    p2.tabla([("Hechos", "Puesto de riesgo", True), ("Departamento", "departamento", False),
        ("Hechos", "Puntaje última semana", True), ("Hechos", "Casos última semana", True),
        ("Hechos", "Estado de alerta", True)], 712, 296, 696, 518, "Orden de revisión · semana de corte", ("Hechos", "Puesto de riesgo", True), totales=False)

    p3 = Pagina("03 Canal", "Casos frente al canal endémico", "Selecciona un departamento para interpretar su canal. Con varios, se muestra la suma de sus umbrales Q3.")
    p3.tarjetas(["Casos notificados", "Suma de canales Q3", "Razón casos sobre Q3"])
    p3.grafico("lineChart", ("Semana", "fecha_inicio"), ["Casos notificados", "Suma de canales Q3"], 248, 296, 1160, 416, "Evolución semanal · casos y suma de canales Q3")
    p3.texto("Brote vigente: casos > Q3 de la misma semana en los cinco años previos y al menos cinco casos.\nUna razón vacía puede indicar ausencia de historia o un Q3 de cero; no se sustituye por cero.", 264, 744, 1120, 64, 14, MUTED)

    p4 = Pagina("04 Desempeño", "Qué detecta y qué omite", "Solo fuera del entrenamiento, con puntaje y desenlace conocido. Horizonte de cuatro semanas.")
    p4.slicer("Ambito", "ambito", "Evaluación", 24, 572)
    p4.tarjetas(["Recall", "Precisión", "Falsas alarmas", "Positivas no detectadas"])
    p4.grafico("clusteredColumnChart", ("Semana", "AñoMes"), ["Aciertos", "Falsas alarmas", "Positivas no detectadas"], 248, 296, 1160, 194, "Resultados por mes · conteos, no promedios de porcentajes")
    p4.tabla([("Semana", "AñoMes", False), ("Hechos", "Semanas evaluadas", True), ("Hechos", "Aciertos", True),
        ("Hechos", "Falsas alarmas", True), ("Hechos", "Positivas no detectadas", True),
        ("Hechos", "Recall", True), ("Hechos", "Precisión", True)], 248, 510, 1160, 204, "Desglose del periodo seleccionado")
    p4.tarjetas(["Ámbito evaluado", "Sin desenlace (total)", "Alertas falsas"], y=724)
    guardar(meta, {"$schema": SCHEMA + "pagesMetadata/1.1.0/schema.json", "pageOrder": [p.id for p in (p1, p2, p3, p4)], "activePageName": p1.id})

    report = json.loads((REPORT / "definition/report.json").read_text())
    theme = {"name": "tema_minsadengue.json", "dataColors": [TEAL, AMBER, RED, "#4778A8", "#8E79A8"],
             "background": "#F2F5F8", "foreground": NAVY, "tableAccent": TEAL,
             "textClasses": {"label": {"fontFace": "Segoe UI", "fontSize": 11, "color": MUTED},
                             "title": {"fontFace": "Segoe UI Semibold", "fontSize": 14, "color": NAVY}}}
    guardar(REPORT / "StaticResources/RegisteredResources/tema_minsadengue.json", theme)
    report["themeCollection"]["customTheme"] = {"name": "tema_minsadengue.json", "type": "RegisteredResources",
        "reportVersionAtImport": {"visual": "2.9.0", "report": "3.3.0", "page": "2.1.0"}}
    report["resourcePackages"] = [p for p in report["resourcePackages"] if p["name"] != "RegisteredResources"]
    report["resourcePackages"].append({"name": "RegisteredResources", "type": "RegisteredResources", "items": [
        {"name": "tema_minsadengue.json", "path": "tema_minsadengue.json", "type": "CustomTheme"}]})
    guardar(REPORT / "definition/report.json", report)


if __name__ == "__main__":
    crear_modelo()
    crear_paginas()
    print("Proyecto indicadores.pbip generado; no incluye credenciales ni datos.")