# Tablero de dengue en Power BI

Abrir `indicadores.pbip` en Power BI Desktop actualizado. El proyecto incluye
el informe editable PBIR, el modelo semántico y las medidas DAX; no incluye datos
importados, credenciales ni un archivo PBIX con resultados. Se mantiene en texto
para revisar los cambios en Git.

## Conexión pendiente a Athena

Depende de las vistas de `feature/athena-catalogo`, en particular
`nube/sql/02_indicadores.sql`. No se incorporó esa rama a esta implementación.
El responsable de AWS debe confirmar que el catálogo desplegado corresponde a
esa versión y proporcionar acceso de consulta por un canal privado.

Instalar el driver **Amazon Athena ODBC 2.x de 64 bits** y crear un DSN de usuario
o sistema de 64 bits con estos valores del entorno del grupo:

| Parámetro | Valor |
| --- | --- |
| DSN | `minsadengue_athena` |
| Región | `us-east-1` |
| Catálogo | `AwsDataCatalog` |
| Base / Schema | `minsadengue_gold` |
| Workgroup | `primary` |
| Resultados S3 | `s3://minsadengue-g13-datalake/athena/resultados/` |
| Autenticación | `IAM Profile`, perfil local exclusivo para BI |

Configurar el perfil local con el acceso que entregue el responsable de AWS.
No utilizar el perfil administrador ni escribir claves en el código M, PBIP,
repositorio o capturas. Si el workgroup impone otra ubicación de resultados,
confirmar y utilizar esa ubicación. Si se utiliza otro DSN o base, cambiar los
parámetros `pDSN` y `pBaseDatos` en **Transformar datos → Editar parámetros**.

El acceso necesita consultar Athena y su catálogo Glue, leer los objetos Gold
que respaldan la vista y leer/escribir resultados de consultas en el prefijo
anterior. No necesita crear clústeres EMR ni modificar los datos Gold. Aunque
sea un usuario de consulta, Athena necesita escribir los resultados de sus
consultas. Si hay cifrado SSE-KMS o Lake Formation, AWS debe habilitar los
permisos correspondientes a ese entorno.

En el administrador ODBC, ejecutar **Test**. En Power BI abrir el proyecto,
revisar **Configuración de origen de datos** para `dsn=minsadengue_athena` y
usar las credenciales del DSN si Power BI lo solicita. Pulsar **Actualizar**.
La consulta de `consultas/Hechos.m` importa únicamente
`minsadengue_gold.v_hechos_semanales` con columnas explícitas.

La importación evita lanzar una consulta Athena por cada interacción con los
gráficos. Cada actualización vuelve a consultar la vista completa: **la
actualización del tablero no es una carga incremental de Power BI**. La
incrementalidad demostrada pertenece al pipeline de S3/EMR. No hace falta dejar
un clúster encendido para consultar sus salidas con Athena.

## Páginas y reglas de lectura

| Página | Contenido |
| --- | --- |
| 01 Panorama | Departamentos evaluados, en alerta y en brote; evolución semanal; corte, modelo y actualización UTC |
| 02 Riesgo | Ranking y puntaje de la última semana del periodo seleccionado |
| 03 Canal | Casos y Q3 por semana; razón de casos frente al umbral |
| 04 Desempeño | Recall, precisión, falsas alarmas y positivos no detectados; desglose mensual y cobertura |

Los filtros de año y departamento se sincronizan entre páginas. El año inicial
es 2024. El filtro de semana se utiliza en Panorama y Riesgo; cada página tiene
su selección de semana. Las tarjetas operativas usan una **semana de corte
común**: la última con puntajes dentro del periodo y departamentos seleccionados.
El ranking no mezcla semanas distintas para completar departamentos faltantes.

El modelo utiliza la vista de hechos con grano departamento-año-semana. Las
dimensiones calculadas Departamento y Semana permiten filtrar todas las
medidas. No se consumen agregados nacionales para intentar filtrarlos después
por departamento. La vista debe conservar claves únicas de ese grano.

Los puntajes de la logística ponderada **no son probabilidades calibradas**.
El umbral se lee de `umbral_alerta`; el tablero no elige ni ajusta umbrales.
El brote vigente se distingue de la alerta sobre las cuatro semanas futuras.
Al seleccionar varios departamentos, la línea Q3 representa la suma de sus
umbrales; no un canal nacional estimado. Una razón con denominador cero se
muestra vacía.

Desempeño excluye filas de entrenamiento, sin puntaje o sin desenlace observado.
Por defecto también excluye semanas con brote vigente; el selector Evaluación
permite consultar todas las semanas elegibles. Cada observación es una
semana-departamento, no un episodio independiente de brote.

| Medida | Cálculo |
| --- | --- |
| Recall | TP / (TP + FN) |
| Precisión | TP / (TP + FP) |
| Alertas falsas | FP / (TP + FP), proporción de alertas falsas; no FP / negativos |
| Tasa de alertas evaluadas | (TP + FP) / observaciones evaluables |
| Sin desenlace (total) | Filas fuera de entrenamiento con puntaje pero sin etiqueta, antes de filtrar el ámbito de brote |

Los totales se recalculan con conteos, sin promediar porcentajes mensuales.
Los cocientes sin denominador quedan vacíos. Los campos fuente de Hechos
están ocultos para evitar sumar puntajes o etiquetas por accidente.

## Comprobación con datos reales y entrega

Después de conectar, confirmar claves únicas y conteos contra Athena. Para el
despliegue reportado por el grupo, se esperan 24 776 filas del panel y 988 de
2024; confirmar esos números con AWS antes de darlos por verificados aquí.
En Desempeño, año 2024 y Sin brote vigente, la referencia comunicada es:
483 observaciones, TP=34, FP=65, FN=69 y TN=315; recall 33,01 % y precisión
34,34 %. El total sin desenlace comunicado es 76. Estos valores son referencias
externas de conciliación, **no datos incrustados ni pruebas realizadas contra AWS**.
Si difieren, revisar fuente, filtros y versión sin volver a ajustar el modelo.

Para demostrar la nueva carga: capturar el tablero después del histórico,
ejecutar la carga 2024 en el pipeline, actualizar Power BI y mostrar 2024 en
los mismos visuales. Conservar capturas de las cuatro páginas, filtros, fecha
de actualización y versión del modelo; no incluir credenciales.

Al finalizar la importación y revisión, usar **Archivo → Guardar como** en
Power BI Desktop y seleccionar **Power BI (.pbix)** para entregar
`indicadores.pbix`. Conservar también el proyecto PBIP y sus dos carpetas.
Publicar en Power BI Service o programar refrescos requiere una configuración
adicional; esta entrega prepara el informe para Desktop con actualización manual.

## Reproducir y verificar el desarrollo

`generar_proyecto.py` es la fuente de generación del modelo y las páginas.
Ejecutarlo sobrescribe esas definiciones: guardar cambios manuales del informe
antes y trasladarlos al generador si se desea conservarlos.

```powershell
python nube/tablero/generar_proyecto.py
npx --yes @microsoft/powerbi-report-authoring-cli@latest validate nube/tablero/indicadores.pbip --format text
python tests/preparar_prueba_tablero.py
```

La última instrucción crea una copia independiente en `output/prueba_tablero`.
Abrir su PBIP y actualizar en Desktop. Está marcada PRUEBA SINTÉTICA y su origen
es una tabla M local, sin AWS. Las ocho observaciones incluyen TP/FP/FN/TN,
brote vigente, entrenamiento, falta de desenlace y falta de puntaje. Esperado:
Sin brote vigente N=4, TP=FP=FN=TN=1 y recall=precisión=50 %; Todas las semanas
N=5, TP=2, FP=FN=TN=1 y recall=precisión=66,67 %. Sin desenlace=1 en ambos.

`tests/test_tablero_dax.ps1` consulta y verifica estas medidas en el motor
local de la copia abierta. Requiere PowerShell 7, la librería oficial
`Microsoft.AnalysisServices.AdomdClient.dll` (NuGet
`Microsoft.AnalysisServices.AdomdClient.NetCore.retail.amd64` 19.84.1) y el
puerto de **esa copia**, disponible en su workspace Analysis Services de
Desktop (`Data/msmdsrv.port.txt`, UTF-16). No modifica el modelo y rechaza
una versión diferente de `PRUEBA_NO_REAL`.

```powershell
./tests/test_tablero_dax.ps1 -Puerto <puerto-de-la-copia> -RutaDll <ruta-a-la-dll>
```

Verificado durante el desarrollo: apertura nativa en Desktop, validación PBIR
sin errores ni advertencias y pruebas DAX anteriores en el motor local de
Power BI. Pendiente: importación real por ODBC, conciliación de resultados y
guardado final del PBIX con datos.

Referencias: [proyectos PBIP de Microsoft](https://learn.microsoft.com/en-us/power-bi/developer/projects/projects-overview),
[parámetros ODBC de AWS](https://docs.aws.amazon.com/athena/latest/ug/odbc-v2-driver-main-connection-parameters.html)
y [autenticación del driver](https://docs.aws.amazon.com/athena/latest/ug/odbc-v2-driver-authentication-options.html).
