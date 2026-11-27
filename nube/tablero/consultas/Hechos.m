let
    Base = if Text.Select(pBaseDatos, {"a".."z", "0".."9", "_"}) = pBaseDatos
        then pBaseDatos else error "Nombre de base de datos no válido",
    Origen = Odbc.Query("dsn=" & pDSN,
        "SELECT cod_dep, departamento, ano, semana, fecha_inicio, fecha_fin, mes, " &
        "casos, temp_media, hum_media, precip_total, umbral_q3, brote_actual, " &
        "probabilidad, alerta, brote_observado, umbral_alerta, version_modelo, " &
        "dentro_entrenamiento, fecha_carga FROM " & Base & ".v_hechos_semanales"),
    Tipos = Table.TransformColumnTypes(Origen, {
        {"cod_dep", type text}, {"departamento", type text}, {"ano", Int64.Type},
        {"semana", Int64.Type}, {"fecha_inicio", type date}, {"fecha_fin", type date},
        {"mes", Int64.Type}, {"casos", Int64.Type}, {"temp_media", type number},
        {"hum_media", type number}, {"precip_total", type number}, {"umbral_q3", type number},
        {"brote_actual", Int64.Type}, {"probabilidad", type number}, {"alerta", type logical},
        {"brote_observado", Int64.Type}, {"umbral_alerta", type number},
        {"version_modelo", type text}, {"dentro_entrenamiento", type logical}, {"fecha_carga", type text}
    }),
    ConActualizacion = Table.AddColumn(Tipos, "actualizado_utc",
        each DateTimeZone.RemoveZone(DateTimeZone.FixedUtcNow()), type datetime)
in
    ConActualizacion
