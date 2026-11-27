-- Tablas externas sobre la capa Gold (Glue Data Catalog + Athena).
-- {{BASE_DATOS}} y {{BUCKET}} los reemplaza `desplegar.py catalogo`.
-- La partición `ano` usa partition projection: cada carga nueva queda visible sin MSCK REPAIR.

CREATE DATABASE IF NOT EXISTS {{BASE_DATOS}}
COMMENT 'Capa Gold del pipeline de alerta temprana de dengue';

CREATE EXTERNAL TABLE IF NOT EXISTS {{BASE_DATOS}}.panel_dengue_clima (
    cod_dep       string  COMMENT 'Código del departamento',
    departamento  string,
    semana        int     COMMENT 'Semana epidemiológica',
    casos         bigint  COMMENT 'Casos notificados en la semana',
    temp_media    double  COMMENT 'Temperatura media semanal (°C)',
    hum_media     double  COMMENT 'Humedad relativa media semanal (%)',
    precip_total  double  COMMENT 'Precipitación acumulada semanal (mm)'
)
COMMENT 'Hechos: un departamento en una semana epidemiológica'
PARTITIONED BY (ano int)
STORED AS PARQUET
LOCATION 's3://{{BUCKET}}/datalake/gold/panel_dengue_clima/'
TBLPROPERTIES (
    'projection.enabled' = 'true',
    'projection.ano.type' = 'integer',
    'projection.ano.range' = '2000,2035'
);

CREATE EXTERNAL TABLE IF NOT EXISTS {{BASE_DATOS}}.alertas (
    cod_dep               string,
    departamento          string,
    semana                int,
    fecha_inicio          date,
    casos                 bigint,
    umbral_q3             double   COMMENT 'Canal endémico: Q3 de la misma semana en los 5 años previos',
    brote_actual          int      COMMENT '1 si casos > umbral_q3 y casos >= 5',
    probabilidad          double   COMMENT 'Probabilidad de brote en las 4 semanas siguientes (sin calibrar)',
    alerta                boolean  COMMENT 'probabilidad >= umbral_alerta',
    brote_observado       int      COMMENT 'Etiqueta real, nula hasta que pasan 4 semanas',
    umbral_alerta         double,
    version_modelo        string,
    dentro_entrenamiento  boolean  COMMENT 'true si el año lo vio el modelo al entrenar',
    fecha_carga           string,
    fecha_proceso         timestamp
)
COMMENT 'Hechos: alerta semanal por departamento'
PARTITIONED BY (ano int)
STORED AS PARQUET
LOCATION 's3://{{BUCKET}}/datalake/gold/alertas/'
TBLPROPERTIES (
    'projection.enabled' = 'true',
    'projection.ano.type' = 'integer',
    'projection.ano.range' = '2000,2035'
);

CREATE EXTERNAL TABLE IF NOT EXISTS {{BASE_DATOS}}.dim_departamento (
    cod_dep          string,
    departamento     string,
    casos_seleccion  bigint  COMMENT 'Casos con los que se eligió el departamento',
    fecha_seleccion  date
)
COMMENT 'Dimensión: los 19 departamentos fijos del panel'
STORED AS PARQUET
LOCATION 's3://{{BUCKET}}/datalake/gold/dim_departamento/';

CREATE EXTERNAL TABLE IF NOT EXISTS {{BASE_DATOS}}.dim_semana (
    ano           int,
    semana        int,
    fecha_inicio  date,
    fecha_fin     date,
    mes           int
)
COMMENT 'Dimensión: calendario de semanas epidemiológicas'
STORED AS PARQUET
LOCATION 's3://{{BUCKET}}/datalake/gold/dim_semana/';

CREATE EXTERNAL TABLE IF NOT EXISTS {{BASE_DATOS}}.modelo_metricas (
    version             string,
    modelo              string,
    subconjunto         string   COMMENT 'completo o sin_brote_actual',
    elegido             boolean,
    anios_validacion    string,
    n                   bigint,
    positivos           bigint,
    auc                 double,
    precision_promedio  double,
    umbral              double,
    `precision`         double,
    recall              double,
    vp                  bigint,
    fp                  bigint,
    fn                  bigint,
    vn                  bigint
)
COMMENT 'Métricas de validación de cada versión del modelo'
STORED AS PARQUET
LOCATION 's3://{{BUCKET}}/datalake/gold/modelo_metricas/';