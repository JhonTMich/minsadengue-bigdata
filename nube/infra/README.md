# Ejecutar el pipeline en una cuenta AWS propia

Estos comandos cubren el punto 2 del proyecto: bucket S3 privado, cifrado y
versionado; clúster EMR; Bronze, Silver, Gold, modelo MLlib y alertas en AWS.
Reutilizan el pipeline local sin modificar sus transformaciones. Athena y Power BI
se configuran aparte.

Estado: implementación probada sin recursos AWS. Para cerrar el punto 2 faltan
configurar la cuenta, ejecutar y conservar evidencias reales. No se dispone de
Learner Lab universitario; el proyecto usará una cuenta propia.

## 1. Acceso y configuración

Se desarrolla y prueba con Spark local. EMR se crea solo durante las ejecuciones
reales y se termina al finalizar. S3 conserva las capas, modelos y evidencias;
Athena/Power BI podrán consultar Gold sin mantener el clúster encendido. AWS
Emulator es opcional para probar APIs; no acredita ejecución cloud del pipeline.

1. Crear o elegir una sola cuenta AWS para el proyecto. Revisar créditos, plan de
   cuenta y acceso a EMR; no asumir que los servicios están cubiertos por Free Tier.
2. Configurar alertas de AWS Budgets y revisar cuotas de EC2/EMR. Una alerta no
   bloquea automáticamente los cargos. La cuenta necesita un medio de pago válido
   si así lo exige su plan y debe poder utilizar EMR.
3. Configurar un perfil de AWS CLI v2 siguiendo la [guía oficial de autenticación](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-authentication.html).
   Usar una identidad con permisos para IAM, VPC, S3, EMR, `iam:PassRole` y
   `pricing:GetProducts`. Guardar las credenciales fuera del repositorio y no
   compartirlas por chat. El perfil se selecciona con `AWS_PROFILE` en la terminal.
4. Instalar Python 3.9+ y AWS CLI v2 en el equipo que ejecutará los comandos. En
   Windows se puede usar `uv run python` si no se dispone de Python en el PATH.
5. Copiar `nube/config/entorno.example.env` a `nube/config/entorno.env` y completar:

| Variable | Cómo elegirla |
|---|---|
| `EXPECTED_ACCOUNT_ID` | Los 12 dígitos de la cuenta propia |
| `AWS_REGION` | Misma para bucket y clúster; comenzar con `us-east-1` |
| `BUCKET` | Nombre único de un bucket dedicado al proyecto |
| `ROL_SERVICIO_EMR` | `MinsaDengueEMRService`, creado por `preparar` |
| `PERFIL_INSTANCIA_EMR` | `MinsaDengueEMREC2`, rol y perfil del mismo nombre |
| `SUBNET_ID` | Vacío al inicio; `preparar` crea una subred dedicada |
| `EMR_RELEASE` | La plantilla propone `emr-7.1.0`, con Spark 3.5.0 |
| `INSTANCIA` | `m5.xlarge`; comprobar disponibilidad y cuotas |
| `NODOS_CORE` | `1`: un primario y un core, dos nodos totales |
| `IDLE_TIMEOUT` | Segundos de inactividad para autoapagado; predeterminado `900` |

No se necesita una clave SSH. El comando `preparar` crea dos roles IAM y el perfil
EC2, una VPC dedicada con DNS, una subred pública, una tabla de rutas y un Internet
Gateway. No crea NAT Gateway, instancias ni clústeres. La VPC y subred llevan las
etiquetas requeridas por `AmazonEMRServicePolicy_v2`. Los permisos de datos del rol
EC2 se limitan al bucket del proyecto y no incluyen permisos administrativos.

Revisar primero la simulación y después ejecutar con las credenciales configuradas:

```bash
python nube/infra/desplegar.py preparar --dry-run
python nube/infra/desplegar.py preparar
```

Se genera `output/aws/entorno.preparado.env`, sin credenciales, con `SUBNET_ID`.
Copiarlo a `nube/config/entorno.env` para continuar (PowerShell:
`Copy-Item output/aws/entorno.preparado.env nube/config/entorno.env`). Ambos archivos
están fuera de Git. Conservar `output/aws/estado.json`: permite retomar la preparación
y reutilizar sus recursos. No ejecutar preparaciones en paralelo.

Los roles existentes solo se reutilizan si pertenecen al mismo proyecto y bucket.
La preparación rechaza una subred ajena proporcionada en el archivo inicial.
Si falla una llamada de creación de forma ambigua, comprobar la consola antes de
reintentar. IAM puede tardar en propagar el perfil; reintentar la comprobación tras
unos minutos si AWS todavía no lo reconoce. No crear otro clúster para resolverlo.

### Estimar antes de crear el clúster

```bash
python nube/infra/desplegar.py estimar --horas 1
```

Consulta AWS Price List Query API y guarda `output/aws/estimacion.json` con fecha,
región, instancia y SKU de las tarifas. Compara 2 y 4 nodos totales para la misma
duración. Incluye EC2 Linux On-Demand, recargo EMR, una IPv4 por nodo y 47 GiB gp3
por nodo (15 raíz + 32 datos). El despliegue configura explícitamente esos discos.
EBS se aproxima con 730 horas/mes. Las horas deben incluir arranque, trabajo, espera
y apagado. S3, solicitudes/versiones, CloudWatch, consultas, transferencias e
impuestos se muestran como excluidos. Créditos y descuentos tampoco se descuentan.

`cluster --horas 1 --max-usd 2` vuelve a estimar y no crea EMR si el total parcial
supera USD 2. Este valor es un umbral de revisión configurable, **no un tope de
facturación** ni una garantía de que la sesión dure una hora. La supervisión y el
autoapagado continúan siendo necesarios. No se sustituyen precios faltantes por
cero. Si la cuenta no permite consultar precios, revisar ese permiso antes de crear
el clúster o usar una cotización revisada con `--tarifas archivo.json`.

Para recalcular sin llamadas AWS se puede reutilizar una cotización guardada:
`python nube/infra/desplegar.py estimar --tarifas output/aws/estimacion.json --dry-run`.
Verificar su fecha antes de usarla para crear recursos. La simulación sin tarifas
solo imprime las consultas previstas; no inventa un importe.

## 2. Preparar una carga local

Ejecutar desde la raíz del repositorio. Si el compañero ya generó la demostración,
reutilizar sus carpetas; de lo contrario, con los originales disponibles:

```bash
python nube/demo/dividir_cargas.py --base data/_nube --parte historico --fecha-carga 2026-11-17 --dengue data/raw/datos_abiertos_vigilancia_dengue_2000_2024.csv --clima data/raw/clima_nasa/clima_departamentos.csv
```

Se esperan archivos CSV en `data/_nube/raw/dengue/fecha_carga=2026-11-17/` y
`data/_nube/raw/clima/fecha_carga=2026-11-17/`. La fecha identifica el lote de la
demostración; no representa la fecha de publicación de la fuente.

## 3. Revisar y ejecutar

En Windows, sustituir `python` por `uv run python` en estos comandos si corresponde.
En una terminal Linux puede usarse `python3`. Primero revisar las
operaciones sin invocar AWS:

```bash
python nube/infra/desplegar.py bucket --dry-run
python nube/infra/desplegar.py subir --fecha-carga 2026-11-17 --dry-run
python nube/infra/desplegar.py cluster --dry-run
python nube/infra/desplegar.py lanzar --corrida completa --dry-run
```

`--dry-run` no necesita credenciales ni AWS CLI. Valida la configuración; `subir`
también verifica archivos y genera un ZIP local. No registra estado ni demuestra
que los permisos AWS sean suficientes.

Con la cuenta propia preparada y las credenciales configuradas:

```bash
python nube/infra/desplegar.py comprobar
python nube/infra/desplegar.py estimar --horas 1
python nube/infra/desplegar.py bucket
python nube/infra/desplegar.py subir --fecha-carga 2026-11-17
python nube/infra/desplegar.py cluster --horas 1 --max-usd 2
python nube/infra/desplegar.py lanzar --corrida completa
python nube/infra/desplegar.py esperar --timeout 3600
python nube/infra/desplegar.py estado
```

Hay dos corridas:

| Corrida | Pasos | Cuándo |
|---|---|---|
| `completa` | Bronze, Silver, Gold, entrenamiento y alertas | Primera carga (histórico) o cuando se decide reentrenar |
| `carga` | Bronze, Silver, Gold y alertas | Cada carga nueva; usa el modelo vigente sin reentrenar |

`lanzar` exige `--corrida` a propósito: si la carga de 2024 se lanzara como
`completa`, el modelo se reentrenaría con 2024 y ese año dejaría de servir para
evaluarlo. La corrida `carga` falla si todavía no hay un modelo vigente.

Para no ir paso por paso, `todo` encadena `bucket`, `subir`, `cluster`, `lanzar`,
`esperar` y `catalogo`:

```bash
python nube/infra/desplegar.py todo --fecha-carga 2026-11-17 --corrida completa --horas 1 --max-usd 2
```

Si se corta o falla, se puede repetir el mismo comando: reutiliza el clúster activo
registrado en `estado.json`, no vuelve a subir archivos idénticos y, si los pasos ya
se enviaron, solo retoma la supervisión. Si falla el envío de pasos o la espera,
solicita el apagado del clúster. `comprobar` y `estimar` siguen siendo recomendables
antes de la primera ejecución.

Los pasos se envían juntos y en orden, con concurrencia 1. Cada paso usa
`spark-submit --master yarn --deploy-mode cluster`, código versionado por SHA-256
y `--py-files` para distribuir el paquete `pipeline`. Las capas se escriben en
`s3://<bucket>/datalake/`, sin rutas de Docker ni master local.

Ejecutar los comandos de forma secuencial desde un solo equipo. No ejecutar otro
pipeline contra la misma base ni crear otro archivo de estado para el mismo
clúster. El estado local queda en `output/aws/estado.json`, ignorado por Git. Para
otro bucket/cuenta/región usar una `--salida` diferente en todos los comandos.

## 4. Supervisión, apagado y reintentos

`esperar` consulta todos los pasos enviados, conserva sus estados y, al completarse, descarga
las evidencias agregadas del pipeline. Solicita apagar el clúster tanto al terminar
correctamente como ante fallo, timeout o interrupción de la terminal. Los propios
pasos también solicitan terminar EMR ante un fallo.

La política de autoapagado por inactividad sirve como respaldo si se cierra el
equipo. No es un límite total de duración ni un tope monetario. Si caducan las
credenciales, renovar la sesión para poder consultar y apagar. Si se pierde toda
conectividad, comprobar el clúster en la consola de AWS.

```bash
python nube/infra/desplegar.py estado
python nube/infra/desplegar.py apagar
```

El apagado es asíncrono: confirmar estado `TERMINATED` (o revisar
`TERMINATED_WITH_ERRORS`) en la consola o con `estado`. No interpretar la solicitud
como confirmación de apagado. No se eliminan bucket, datos ni logs; S3 puede seguir
generando cargos. Guardar la estimación y revisar el costo real en Billing/Cost
Explorer cuando se actualice. Revisar volúmenes huérfanos y versiones S3 tras las
pruebas; no borrar las evidencias necesarias para el informe. Los roles y la red se
conservan para reutilizarlos en la siguiente sesión.

Repetir `cluster` reutiliza el clúster activo del estado y evita crear otro.
Repetir `lanzar` después de un envío se rechaza para evitar doble ejecución; usar
`estado` o `esperar`. Si el envío o la creación devuelve un error de red ambiguo,
verificar la consola antes de reintentar: AWS pudo haber recibido la petición.

Repetir la subida del mismo contenido es una operación sin cambios. Si un objeto
de Raw ya existe con otro hash o sin el metadato esperado, se rechaza su reemplazo.
Una carga incompleta debe terminar de subirse antes de enviar los pasos. La carga
queda congelada una vez que se empieza a procesar; no agregar archivos a su fecha.

## 5. Demostrar la llegada de 2024

Después de completar el histórico y confirmar su apagado:

```bash
python nube/demo/dividir_cargas.py --base data/_nube --parte nueva --fecha-carga 2026-11-18 --dengue data/raw/datos_abiertos_vigilancia_dengue_2000_2024.csv --clima data/raw/clima_nasa/clima_departamentos.csv
python nube/infra/desplegar.py subir --fecha-carga 2026-11-18
python nube/infra/desplegar.py cluster --horas 1 --max-usd 2
python nube/infra/desplegar.py lanzar --corrida carga
python nube/infra/desplegar.py esperar --timeout 3600
python nube/infra/desplegar.py estado
```

La nueva ejecución comparte el data lake y su manifiesto; procesa las cargas
pendientes. Se crea un clúster nuevo si el anterior ya terminó. Conservar el mismo
archivo de estado en todo el recorrido.

La implementación actual de dengue espera años completos dentro de cada carga
afectada. Esta demostración agrega 2024 completo. No usar lotes de semanas sueltas
del mismo año ni presentar este alcance como soporte de cualquier corrección
histórica. La generalización del contrato incremental queda fuera del punto 2.

## Catálogo de Gold en Athena

Con Gold ya escrito en S3, sin clúster encendido:

```bash
python nube/infra/desplegar.py catalogo --dry-run
python nube/infra/desplegar.py catalogo
```

Ejecuta `nube/sql/01_tablas.sql` (base de datos de Glue `GLUE_DATABASE`, por defecto
`minsadengue_gold`, y cinco tablas externas de Gold) y `nube/sql/02_indicadores.sql`
(la vista de hechos semanal y las cuatro vistas de indicadores). `panel_dengue_clima` y
`alertas` usan *partition projection* por `ano`: una carga nueva queda visible sin
`MSCK REPAIR`. Los resultados de Athena se guardan cifrados en
`s3://<bucket>/athena/resultados/`. Al final cuenta las filas de cada tabla y vista y deja
`output/aws/athena/catalogo.json`. Se puede repetir: las tablas usan `IF NOT EXISTS` y
las vistas `CREATE OR REPLACE`.

## 6. Evidencias del entregable

- `output/aws/ejecuciones/<cluster-id>/ejecucion.json`: configuración observada,
  estados y tiempos reportados por EMR, conservados por clúster.
- `output/aws/pipeline/`: JSON agregados descargados de las evidencias del pipeline.
  Se pueden recuperar de nuevo con `python nube/infra/desplegar.py evidencias`.
- S3 `logs/emr/`: logs de EMR/YARN; en deploy mode cluster, revisar el contenedor
  YARN del driver si falla una aplicación.
- Consola S3: comprobar bloqueo de acceso público, cifrado, versionado y las capas.
- Consola EMR: comprobar dos nodos, master YARN en la evidencia del pipeline, cinco (completa) o cuatro (carga)
  pasos `COMPLETED` y estado final de apagado.
- Comparar conteos del histórico y carga nueva con la referencia local antes de
  declarar paridad. No reutilizar los tiempos locales como mediciones AWS.

Las evidencias y originales quedan fuera de Git por defecto. Incorporar al informe
solo resultados agregados revisados, sin credenciales ni registros personales.

## Referencias y pruebas

- [Crear un clúster con AWS CLI](https://docs.aws.amazon.com/cli/latest/reference/emr/create-cluster.html).
- [Pasos Spark en EMR](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-submit-step.html).
- [Autoapagado y sus requisitos de permisos/red](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-auto-termination-policy.html).
- [EMR 7.1.0 y versiones de aplicaciones](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-710-release.html).
- [Rol EC2 de EMR y perfil del mismo nombre](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-iam-role-for-ec2.html).
- [Permisos y etiquetas del rol de servicio](https://docs.aws.amazon.com/aws-managed-policy/latest/reference/AmazonEMRServicePolicy_v2.html).
- [AWS Price List Query API](https://docs.aws.amazon.com/cli/latest/reference/pricing/get-products.html).

```bash
python -m unittest discover -s tests -v
```

Estas pruebas usan respuestas simuladas y archivos temporales. No acreditan
permisos efectivos, cuotas, conectividad ni ejecución real en una cuenta AWS.