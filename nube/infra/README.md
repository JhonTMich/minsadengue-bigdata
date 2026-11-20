# Ejecutar los pasos 1 a 3 en AWS Academy Learner Lab

Estos comandos cubren el punto 2 del proyecto: bucket S3 privado, cifrado y
versionado; clúster EMR; Bronze, Silver y Gold en AWS. Reutilizan el pipeline local
sin modificar sus transformaciones. No implementan modelo, Athena ni Power BI.

Estado: implementación probada sin recursos AWS. Para cerrar el punto 2 faltan
configurar el laboratorio, ejecutar y conservar evidencias reales.

## 1. Acceso y configuración

Learner Lab es el laboratorio de AWS Academy que habilita la universidad. Usa AWS
real, consume el saldo educativo disponible y tiene restricciones propias de
servicios, roles, regiones e instancias. AWS Emulator es para pruebas locales y no
acredita la ejecución de EMR en la nube exigida por este entregable.

1. Entrar al curso de AWS Academy, abrir Learner Lab y usar **Start Lab**.
2. Revisar saldo, tiempo de sesión y servicios permitidos en las instrucciones del
   laboratorio. Confirmar EMR, tipos de instancia, región y roles habilitados.
3. Configurar AWS CLI v2 con las credenciales temporales de **AWS Details**,
   incluido el token de sesión. Guardarlas fuera del repositorio, en el perfil
   local de AWS o en las variables de la terminal. No compartirlas por chat ni
   agregarlas a `entorno.env`. Renovarlas cuando caduque la sesión.
4. Instalar Python 3.9+ y AWS CLI v2 en el equipo que ejecutará los comandos. En
   Windows se puede usar `uv run python` si no se dispone de Python en el PATH.
5. Copiar `nube/config/entorno.example.env` a `nube/config/entorno.env` y completar:

| Variable | Cómo elegirla |
|---|---|
| `EXPECTED_ACCOUNT_ID` | Los 12 dígitos de la cuenta del laboratorio |
| `AWS_REGION` | Región habilitada, misma para bucket y clúster |
| `BUCKET` | Nombre único de un bucket dedicado al proyecto |
| `ROL_SERVICIO_EMR` | Rol de servicio EMR ya permitido por el laboratorio |
| `PERFIL_INSTANCIA_EMR` | Perfil de instancia EC2 para EMR ya permitido |
| `SUBNET_ID` | Subred disponible con conectividad para EMR y S3 |
| `EMR_RELEASE` | Versión habilitada; la plantilla propone `emr-7.1.0`, con Spark 3.5.0 |
| `INSTANCIA` | Tipo permitido; verificar `m5.xlarge` antes de crear |
| `NODOS_CORE` | `1`: un primario y un core, dos nodos totales |
| `IDLE_TIMEOUT` | Segundos de inactividad para autoapagado; predeterminado `900` |

Los scripts no crean roles IAM ni redes, ni necesitan una clave SSH. Usar los
recursos autorizados del laboratorio. La comprobación inicial valida identidad,
subred disponible y release; no garantiza permisos efectivos de los roles ni
conectividad de la subred. Si falla, conservar el error y revisar la configuración
del laboratorio antes de crear recursos adicionales.

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
En la terminal Linux del laboratorio puede usarse `python3`. Primero revisar las
operaciones sin invocar AWS:

```bash
python nube/infra/desplegar.py bucket --dry-run
python nube/infra/desplegar.py subir --fecha-carga 2026-11-17 --dry-run
python nube/infra/desplegar.py cluster --dry-run
python nube/infra/desplegar.py lanzar --dry-run
```

`--dry-run` no necesita credenciales ni AWS CLI. Valida la configuración; `subir`
también verifica archivos y genera un ZIP local. No registra estado ni demuestra
que los permisos AWS sean suficientes.

Con el laboratorio activo y las credenciales configuradas:

```bash
python nube/infra/desplegar.py comprobar
python nube/infra/desplegar.py bucket
python nube/infra/desplegar.py subir --fecha-carga 2026-11-17
python nube/infra/desplegar.py cluster
python nube/infra/desplegar.py lanzar
python nube/infra/desplegar.py esperar --timeout 3600
python nube/infra/desplegar.py estado
```

Los cinco wrappers `01_...sh` a `04_...sh` y `99_apagar.sh` delegan al mismo
ejecutor; `00_comprobar.sh` y `06_esperar.sh` completan el flujo. Son opcionales.

Los pasos se envían juntos y en orden, con concurrencia 1. Cada paso usa
`spark-submit --master yarn --deploy-mode cluster`, código versionado por SHA-256
y `--py-files` para distribuir el paquete `pipeline`. Las capas se escriben en
`s3://<bucket>/datalake/`, sin rutas de Docker ni master local.

Ejecutar los comandos de forma secuencial desde un solo equipo. No ejecutar otro
pipeline contra la misma base ni crear otro archivo de estado para el mismo
clúster. El estado local queda en `output/aws/estado.json`, ignorado por Git. Para
otro bucket/cuenta/región usar una `--salida` diferente en todos los comandos.

## 4. Supervisión, apagado y reintentos

`esperar` consulta los tres pasos, conserva sus estados y, al completarse, descarga
las evidencias agregadas del pipeline. Solicita apagar el clúster tanto al terminar
correctamente como ante fallo, timeout o interrupción de la terminal. Los propios
pasos también solicitan terminar EMR ante un fallo.

La política de autoapagado por inactividad sirve como respaldo si se cierra el
equipo. No es un límite total de duración ni un tope monetario. Si caducan las
credenciales, renovar la sesión para poder consultar y apagar. Si se pierde toda
conectividad, comprobar el clúster en la consola del laboratorio.

```bash
python nube/infra/desplegar.py estado
python nube/infra/desplegar.py apagar
```

El apagado es asíncrono: confirmar estado `TERMINATED` (o revisar
`TERMINATED_WITH_ERRORS`) en la consola o con `estado`. No interpretar la solicitud
como confirmación de apagado. No se eliminan bucket, datos ni logs; S3 puede seguir
consumiendo saldo. Guardar capturas del saldo antes y después y consultar el costo
real en las herramientas habilitadas por el laboratorio.

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
python nube/infra/desplegar.py cluster
python nube/infra/desplegar.py lanzar
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

## 6. Evidencias del entregable

- `output/aws/ejecuciones/<cluster-id>/ejecucion.json`: configuración observada,
  estados y tiempos reportados por EMR, conservados por clúster.
- `output/aws/pipeline/`: JSON agregados descargados de las evidencias del pipeline.
  Se pueden recuperar de nuevo con `python nube/infra/desplegar.py evidencias`.
- S3 `logs/emr/`: logs de EMR/YARN; en deploy mode cluster, revisar el contenedor
  YARN del driver si falla una aplicación.
- Consola S3: comprobar bloqueo de acceso público, cifrado, versionado y las capas.
- Consola EMR: comprobar dos nodos, master YARN en la evidencia del pipeline, tres
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

```bash
python -m unittest discover -s nube/infra -v
```

Estas pruebas usan respuestas simuladas y archivos temporales. No acreditan
compatibilidad efectiva con las restricciones de una cuenta Learner Lab concreta.
