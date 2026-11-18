# Despliegue en AWS

Guía para desplegar la captura en una cuenta personal de AWS desde macOS.
Todo se crea con una sola plantilla (`template.yaml`, AWS SAM) en **us-east-1**.

## Qué se crea

| Recurso | Para qué |
|---|---|
| Bucket S3 (cifrado SSE-S3, sin acceso público, solo HTTPS) | Capas `bronze/` y `silver/`. Se conserva aunque se borre el stack |
| Lambda `IngestFunction` (256 MB, sin dependencias) | Guarda en bronze el estado de estaciones cada 2 min y el clima cada hora |
| Job de Glue `ecobici-cloud-curate` (Python shell, 0.0625 DPU) + trigger diario | A las 00:15 UTC convierte el raw del día anterior en un Parquet por tabla (curated), aplica las reglas de calidad y archiva el raw |
| 2 horarios de EventBridge Scheduler | Disparan la Lambda de ingesta |
| Base `ecobici` en Glue + 3 tablas con *partition projection* | Consultar silver con SQL en Athena sin crawlers |
| Workgroup de Athena con tope de 1 GB por consulta | Evita consultas caras por error |
| Tema SNS + 2 alarmas de CloudWatch + regla de EventBridge | Correo si la ingesta falla o se detiene, o si falla el Job de Glue |
| Ciclo de vida del bucket | Expira raw individual (30 d) y resultados de Athena (7 d); archivo a Glacier; curated a Standard-IA |
| Presupuesto mensual (AWS Budgets) | Correo al 80% del gasto real o si el pronóstico supera el presupuesto |
| Logs con retención de 14 días | Evita que los logs se acumulen y cuesten |

**Costo esperado:** menos de 1 USD al mes, sin descontar la capa gratuita (S3 ~0.25, Lambda ~0.15, el resto centavos).

## 0. Antes de empezar (una sola vez)

1. **Protege la cuenta raíz:** en la consola, menú de tu nombre → *Security credentials* → activa **MFA**.
2. **No uses la cuenta raíz para trabajar.** Crea un usuario IAM:
   - Consola → **IAM** → *Users* → *Create user* → nombre `ecobici-dev`.
   - *Attach policies directly* → `AdministratorAccess` (aceptable para un proyecto personal; en una empresa se usaría IAM Identity Center y permisos acotados).
   - Abre el usuario → *Security credentials* → *Create access key* → caso de uso **CLI** → guarda el *Access key* y el *Secret* (el secreto solo se muestra una vez).
3. **Instala las herramientas:**
   ```bash
   brew install awscli aws-sam-cli
   aws --version && sam --version
   ```
4. **Configura las credenciales:**
   ```bash
   aws configure
   # AWS Access Key ID:     (la del paso 2)
   # AWS Secret Access Key: (la del paso 2)
   # Default region name:   us-east-1
   # Default output format: json
   aws sts get-caller-identity   # debe mostrar el usuario ecobici-dev
   ```

## 2. Construir y desplegar

Desde la carpeta del proyecto:

```bash
sam validate --lint
sam build
sam deploy --guided
```

Respuestas para `--guided` (solo la primera vez; después basta `sam deploy`):

| Pregunta | Respuesta |
|---|---|
| Stack Name | `ecobici-cloud` (Enter) |
| AWS Region | `us-east-1` (Enter) |
| Parameter AlertEmail | tu correo |
| Parameter MonthlyBudgetUSD | `5` (Enter) |
| Parameter DataStartDate | Enter |
| Confirm changes before deploy | `Y` |
| Allow SAM CLI IAM role creation | `Y` |
| Disable rollback | `N` |
| Save arguments to configuration file | `Y` |

SAM muestra la lista de recursos (*changeset*); confirma con `y`. Tarda 2–4 minutos.

**Al terminar:** llega un correo *AWS Notification - Subscription Confirmation*. Abre el link para confirmar; sin eso no llegan las alarmas.
Es normal recibir una alarma "La ingesta no se ejecutó en 15 minutos" justo después de desplegar: se resuelve sola con las primeras ejecuciones y llega un correo de *OK*.

## 3. Verificar que está capturando

Después de 5 minutos:

```bash
# Nombre del bucket
BUCKET=$(aws cloudformation describe-stacks --stack-name ecobici-cloud \
  --query "Stacks[0].Outputs[?OutputKey=='BucketName'].OutputValue" --output text)
echo $BUCKET

# Últimos snapshots guardados (debe haber uno cada 2 minutos)
aws s3 ls s3://$BUCKET/bronze/station_status/ --recursive | tail -5

# Logs de la ingesta en vivo (Ctrl + C para salir)
sam logs -n IngestFunction --stack-name ecobici-cloud --tail
```

Cada línea del log muestra algo como `{"task": "status", "estaciones_status": 677, ...}`.

## 4. Subir el código del Job de Glue y probarlo

Después de cada `sam deploy` que cambie el código del paquete:

```bash
./glue/upload.sh
```

Correr el job para un día con datos (es idempotente):

```bash
aws glue start-job-run --job-name ecobici-cloud-curate --arguments '{"--DATE":"2026-11-12"}'
aws glue get-job-runs --job-name ecobici-cloud-curate --max-items 1 \
  --query "JobRuns[0].[JobRunState,ExecutionTime,ErrorMessage]"
```

Repetir el segundo comando hasta que diga `SUCCEEDED` (~1–2 min). La salida detallada está en CloudWatch → *Log groups* → `/aws-glue/python-jobs/output`.

## 5. Consultar en Athena

Consola → **Athena** → *Query editor* → arriba a la derecha elige el workgroup **ecobici-cloud** → base de datos **ecobici**:

```sql
-- Calidad por hora (hora local de CDMX) del día compactado
SELECT hour(ingested_at AT TIME ZONE 'America/Mexico_City') AS hora_cdmx,
       count(DISTINCT ingested_at)                          AS snapshots,
       round(avg(IF(dq_ok, 1.0, 0.0)) * 100, 1)             AS pct_validas
FROM ecobici.fact_station_status
WHERE dt = '2026-11-10'
GROUP BY 1
ORDER BY 1;

-- Estaciones que pasaron más tiempo vacías en horario de operación
SELECT s.short_name, s.name,
       round(avg(IF(f.num_bikes_available = 0, 1.0, 0.0)) * 100, 1) AS pct_tiempo_vacia
FROM ecobici.fact_station_status f
JOIN ecobici.dim_station s ON s.station_id = f.station_id AND s.dt = f.dt
WHERE f.dt = '2026-11-10' AND f.en_horario AND f.dq_ok
GROUP BY 1, 2
ORDER BY pct_tiempo_vacia DESC
LIMIT 15;
```

Las particiones (`dt`) están en fecha UTC; filtra siempre por `dt` para que Athena lea solo ese día.

## 6. Pausar o eliminar

- **Pausar la captura** (sin borrar nada): consola → **EventBridge** → *Scheduler* → *Schedules* → selecciona los de `ecobici-cloud` → *Disable*.
- **Eliminar todo:** `sam delete --stack-name ecobici-cloud`. El bucket se conserva a propósito para no perder datos; para borrarlo también: `aws s3 rb s3://$BUCKET --force`.

## Problemas comunes

| Síntoma | Causa probable | Solución |
|---|---|---|
| El Job de Glue falla con `NoSuchKey` | No se subió el código | `./glue/upload.sh` y volver a correrlo |
| El Job de Glue falla con `ModuleNotFoundError: pandas` | El job no tomó las librerías de análisis | Revisar que el parámetro `library-set` = `analytics` esté en el job |
| No llegan correos de alarmas | No se confirmó la suscripción SNS | Revisa spam y abre el link de confirmación |
| `AccessDenied` al correr comandos | Credenciales de otro usuario o región distinta | `aws sts get-caller-identity` y `aws configure get region` |
| La compactación falla con "No hay snapshots" | Se pidió un día sin captura | Usa una fecha UTC con datos (`aws s3 ls .../bronze/station_status/`) |
