-- Vistas de indicadores para Power BI (Athena / Trino).
-- {{BASE_DATOS}} lo reemplaza `desplegar.py catalogo`. Power BI lee solo estas vistas.

CREATE OR REPLACE VIEW {{BASE_DATOS}}.v_hechos_semanales AS
SELECT
    p.cod_dep,
    COALESCE(d.departamento, p.departamento) AS departamento,
    p.ano,
    p.semana,
    s.fecha_inicio,
    s.fecha_fin,
    s.mes,
    p.casos,
    p.temp_media,
    p.hum_media,
    p.precip_total,
    a.umbral_q3,
    a.brote_actual,
    a.probabilidad,
    a.alerta,
    a.brote_observado,
    a.umbral_alerta,
    a.version_modelo,
    a.dentro_entrenamiento,
    a.fecha_carga
FROM {{BASE_DATOS}}.panel_dengue_clima p
JOIN {{BASE_DATOS}}.dim_semana s
  ON s.ano = p.ano AND s.semana = p.semana
LEFT JOIN {{BASE_DATOS}}.dim_departamento d
  ON d.cod_dep = p.cod_dep
LEFT JOIN {{BASE_DATOS}}.alertas a
  ON a.cod_dep = p.cod_dep AND a.ano = p.ano AND a.semana = p.semana;

CREATE OR REPLACE VIEW {{BASE_DATOS}}.v_departamentos_en_alerta AS
SELECT
    ano,
    semana,
    fecha_inicio,
    COUNT(*) AS departamentos_evaluados,
    SUM(CASE WHEN alerta THEN 1 ELSE 0 END) AS departamentos_en_alerta,
    SUM(brote_actual) AS departamentos_en_brote,
    CAST(SUM(CASE WHEN alerta THEN 1 ELSE 0 END) AS double) / COUNT(*) AS proporcion_en_alerta,
    array_join(array_sort(array_agg(departamento) FILTER (WHERE alerta)), ', ') AS departamentos
FROM {{BASE_DATOS}}.v_hechos_semanales
WHERE probabilidad IS NOT NULL
GROUP BY ano, semana, fecha_inicio;

CREATE OR REPLACE VIEW {{BASE_DATOS}}.v_ranking_riesgo AS
SELECT
    ano,
    semana,
    fecha_inicio,
    cod_dep,
    departamento,
    probabilidad,
    RANK() OVER (PARTITION BY ano, semana ORDER BY probabilidad DESC) AS puesto,
    alerta,
    casos,
    umbral_q3,
    brote_actual,
    brote_observado,
    dentro_entrenamiento
FROM {{BASE_DATOS}}.v_hechos_semanales
WHERE probabilidad IS NOT NULL;

CREATE OR REPLACE VIEW {{BASE_DATOS}}.v_casos_vs_canal AS
SELECT
    ano,
    semana,
    fecha_inicio,
    cod_dep,
    departamento,
    casos,
    umbral_q3,
    CAST(casos AS double) / NULLIF(umbral_q3, 0) AS razon_canal,
    CASE
        WHEN brote_actual = 1 THEN 'Brote'
        WHEN casos > umbral_q3 THEN 'Sobre el canal, menos de 5 casos'
        ELSE 'Dentro del canal'
    END AS zona,
    alerta
FROM {{BASE_DATOS}}.v_hechos_semanales
WHERE umbral_q3 IS NOT NULL;

CREATE OR REPLACE VIEW {{BASE_DATOS}}.v_desempeno_alertas AS
WITH cerradas AS (
    SELECT h.ano, h.mes, h.dentro_entrenamiento, t.subconjunto,
           CASE WHEN h.alerta THEN 1 ELSE 0 END AS alerta,
           h.brote_observado AS brote
    FROM {{BASE_DATOS}}.v_hechos_semanales h
    CROSS JOIN (VALUES ('completo'), ('sin_brote_actual')) AS t (subconjunto)
    WHERE h.brote_observado IS NOT NULL
      AND h.probabilidad IS NOT NULL
      AND (t.subconjunto = 'completo' OR h.brote_actual = 0)
)
SELECT
    ano,
    mes,
    dentro_entrenamiento,
    subconjunto,
    COUNT(*) AS semanas,
    SUM(alerta) AS alertas,
    SUM(brote) AS brotes,
    SUM(alerta * brote) AS aciertos,
    SUM(alerta * (1 - brote)) AS falsas_alarmas,
    SUM((1 - alerta) * brote) AS brotes_no_detectados,
    CAST(SUM(alerta * brote) AS double) / NULLIF(SUM(brote), 0) AS tasa_deteccion,
    CAST(SUM(alerta * (1 - brote)) AS double) / NULLIF(SUM(alerta), 0) AS tasa_falsas_alarmas
FROM cerradas
GROUP BY ano, mes, dentro_entrenamiento, subconjunto;