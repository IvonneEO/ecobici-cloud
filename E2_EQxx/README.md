# E2 · Ecobici Cloud: predicción de disponibilidad por estación

**Equipo:** EQxx · **Perfil técnico:** Ivonne Enciso · **Perfil funcional:** Emiliano
**Repositorio:** código en la raíz del repo; este folder contiene el entregable y sus evidencias.

## Resumen

Capturamos cada 2 minutos el estado de las ~677 estaciones de Ecobici (feed público GBFS) y el clima por hora (Open-Meteo). Los datos llegan crudos a S3. Un Job de Glue los convierte cada noche en Parquet particionado por fecha y se consultan con SQL en Athena. Todo está desplegado en AWS (us-east-1) con infraestructura como código (`template.yaml`, AWS SAM), monitoreo con alertas por correo y un presupuesto mensual. Costo real: **menos de 1 USD al mes**.

```
EventBridge Scheduler ─(cada 2 min)─► Lambda ingest ─► S3 bronze/  (raw: JSON gzip por snapshot)
                      ─(cada hora)──► Lambda ingest ─► S3 bronze/weather/
Glue Trigger ─(00:15 UTC)─► Glue Job curate ─► S3 silver/  (curated: 1 Parquet por tabla y día)
                                          └──► S3 archive/ (raw del día en 1 archivo, para Glacier)
Glue Data Catalog (partition projection) ─► Athena (workgroup con tope) ─► S3 athena-results/
CloudWatch + EventBridge ─► SNS ─► correo          AWS Budgets ─► correo
```

---

## Ruta 1 · Técnica (construido en AWS)

### 1. Lago con 3 capas

| Capa del enunciado | Prefijo en S3 | Contenido | Formato |
|---|---|---|---|
| raw | `bronze/` | Un archivo por snapshot (estado de estaciones), por consulta de clima y catálogo diario, tal cual llegó | JSON gzip |
| raw (archivo) | `archive/` | El raw de cada día en un solo archivo, para conservarlo barato | JSONL gzip |
| curated | `silver/` | `fact_station_status`, `dim_station`, `fact_weather_hourly`: un Parquet por tabla y día | Parquet + Snappy |
| resultados | `athena-results/` | Resultados de las consultas de Athena | CSV |

Todas las capas se particionan por fecha UTC (`dt=YYYY-MM-DD/`). Evidencia: [`evidencias/capas_s3.md`](evidencias/capas_s3.md).

**Tres consultas en Athena con los bytes anotados:** [`evidencias/athena_consultas.md`](evidencias/athena_consultas.md).

| Consulta | Bytes escaneados | Lectura |
|---|---|---|
| Q1. Calidad por hora de un día (con filtro de partición) | PENDIENTE (ver evidencia) | Lee 1 día y 2 columnas |
| Q2. La misma, sin filtro de partición | PENDIENTE | Lee todos los días: muestra el ahorro del particionado |
| Q3. Estaciones que más tiempo pasaron vacías (JOIN) | PENDIENTE | Consulta de negocio |

### 2. Tablas con datos estructurados

Tres tablas en el **Glue Data Catalog**, consultables en **Athena** (base `ecobici`). Usan *partition projection*, así que no requieren crawlers ni `MSCK REPAIR`.

| Tabla | Grano | Columnas clave |
|---|---|---|
| `fact_station_status` | estación × snapshot (cada 2 min) | bicis y anclajes disponibles, estado, 7 banderas de calidad `dq_*`, `en_horario` |
| `dim_station` | estación × día | `short_name` (llave con los viajes), nombre, coordenadas, capacidad |
| `fact_weather_hourly` | hora | temperatura, lluvia, viento, código de clima |

**DynamoDB no se usa en esta etapa.** Hoy todas las consultas son por lotes (análisis y entrenamiento). DynamoDB entra cuando exista la consulta en tiempo real "predicción de la estación X". Ver el ADR de almacenamiento en la Ruta 2.

### 3. Job de Glue: raw → curated en Parquet particionado

- **Job:** `ecobici-cloud-curate`, Glue **Python shell**, 0.0625 DPU (la capacidad mínima), timeout de 30 min, 1 reintento. Código: [`glue/curate_job.py`](../glue/curate_job.py).
- **Lógica:** reutiliza el módulo probado `ecobici.aws.compact` (32 pruebas automáticas, incluido un día completo simulado en S3).
  1. Lee los ~720 snapshots del día en `bronze/`.
  2. Aplica las reglas de calidad snapshot por snapshot y marca el horario de operación.
  3. Escribe un Parquet por tabla en `silver/<tabla>/dt=<día>/part-00000.parquet`.
  4. Archiva el raw del día en `archive/`.
  - Es idempotente: se puede reprocesar cualquier día con `--DATE 2026-11-12`.
- **Disparo:** Glue Trigger programado, `cron(15 0 * * ? *)` (00:15 UTC = 18:15 en CDMX).
- **Aviso de fallas:** una regla de EventBridge (`Glue Job State Change` = FAILED/TIMEOUT/ERROR) publica en SNS y llega por correo.
- **Por qué Python shell y no Spark:** el volumen diario es de ~5 MB. Spark exige mínimo 2 DPU y costaría ~10.70 USD al año, contra ~0.33 USD de Python shell. Ver el ADR de procesamiento.

Evidencia: [`evidencias/glue_corridas.md`](evidencias/glue_corridas.md).

### 4. Políticas para optimizar costos

**Almacenamiento**

| Política | Dónde | Por qué |
|---|---|---|
| Expiración a 30 días | `bronze/` | Los JSON individuales pesan ~7 KB y S3 no transiciona a Glacier objetos de menos de 128 KB. El raw no se pierde: queda en `archive/` |
| Glacier Instant Retrieval a 30 días; Deep Archive a 365 | `archive/` | El raw solo se relee para reprocesar; un archivo por día supera los 128 KB |
| Standard-IA a 90 días | `silver/` | Lo reciente se consulta seguido; lo viejo, casi nunca |
| Expiración a 7 días | `athena-results/` | Los resultados se pueden regenerar |
| Abortar cargas incompletas a 1 día | todo el bucket | Evita pagar por fragmentos huérfanos |
| Parquet + Snappy, particionado por `dt` | `silver/` | Athena lee solo los días y columnas que pide la consulta |
| Retención de logs de 14 días | CloudWatch | Los logs no se acumulan indefinidamente |

Reglas vigentes: [`evidencias/ciclo_de_vida.json`](evidencias/ciclo_de_vida.json).

**Procesamiento**

| Política | Por qué |
|---|---|
| La Lambda de ingesta no usa pandas (256 MB, solo librería estándar) | Corre 744 veces al día: un paquete de pocos KB arranca rápido y cuesta menos |
| Un solo Job de Glue al día, en Python shell a 0.0625 DPU | El volumen no justifica Spark |
| Workgroup de Athena con tope de 1 GB por consulta | Una consulta mal escrita cuesta como máximo ~0.005 USD |
| Partition projection en lugar de crawlers | Sin costo de crawler ni pasos manuales para registrar particiones |
| AWS Budgets a 5 USD al mes, sin descontar créditos | Aviso al 80% del gasto real y si el pronóstico supera el presupuesto |

---

## Eventos (triggers) de ingesta

**A. Evento que dispara la llegada del dato.** Una **programación (cron)**. El feed GBFS solo publica el estado actual: no avisa cuando cambia, ni deja archivos, ni ofrece un flujo continuo. Hay que ir a buscarlo periódicamente.

| Flujo | Evento | Servicio |
|---|---|---|
| Estado de estaciones | `rate(2 minutes)` | EventBridge Scheduler → Lambda `ingest` (`{"task": "status"}`) |
| Clima | `rate(1 hour)` | EventBridge Scheduler → Lambda `ingest` (`{"task": "weather"}`) |
| raw → curated | `cron(15 0 * * ? *)` | Glue Trigger → Job `curate` |

**B. Clasificación.**
- **Invocación asíncrona:** el Scheduler dispara la Lambda y no espera su respuesta. Lambda gestiona el reintento (1 reintento, descartado si el evento tiene más de 120 s).
- **Consulta a la fuente síncrona:** dentro de la Lambda, la petición HTTP al feed espera la respuesta (timeout de 15 s, 3 intentos con backoff de 1 y 2 s).
- **Job de Glue asíncrono:** el trigger lo arranca y su resultado se notifica por evento (EventBridge → SNS).

**C. Justificación con cifras.**

| | Estado de estaciones | Clima |
|---|---|---|
| Frecuencia de cambio en la fuente | el feed se regenera cada ~60 s (`ttl`) | horaria |
| Frecuencia elegida | cada 2 min (720 al día) | cada hora (24 al día) |
| Volumen | ~7 KB comprimido por snapshot; ~5 MB/día | <1 KB por consulta |
| Tiempo de respuesta esperado | dato en S3 en <5 s tras el disparo; curated al día siguiente (T+1) | igual |
| Por qué no más seguido | cada 1 min duplica costo y volumen; la duración mediana de un viaje es de 12 min, así que 2 min captura los cambios relevantes | el clima de Open-Meteo es horario |

**¿Qué pasa si la fuente falla?**
1. La Lambda reintenta 3 veces la petición HTTP y, si sigue fallando, termina en error. Lambda la reintenta una vez más.
2. **Alarma 1:** si hay 3 o más errores en 10 minutos, llega un correo.
3. **Alarma 2:** si no hay ninguna ejecución en 15 minutos (el scheduler o la función están detenidos), llega un correo.
4. **Un snapshot perdido no se puede recuperar,** porque el feed solo da el estado actual. Por eso el Job de Glue mide y reporta la completitud del día (snapshots recibidos / 720) y los huecos de más de 5 minutos, y el modelo lo tolera.
5. El clima sí se puede recuperar después desde la API histórica de Open-Meteo.

**D. Construcción y prueba.** Implementado en `template.yaml` y `src/ecobici/aws/`. Pruebas:
- Automáticas (`python -m pytest`, 32 pruebas): un día completo de ingesta (720 snapshots + 24 consultas de clima) contra S3 simulado, compactación, idempotencia, esquema Parquet = tablas de Athena, ciclo de vida y compatibilidad del job con Python 3.9.
- En la nube: completitud medida en [`evidencias/glue_corridas.md`](evidencias/glue_corridas.md) y triggers activos en [`evidencias/ingesta.md`](evidencias/ingesta.md).

---

## Ruta 2 · Funcional (decide y documenta)

> **[EMILIANO]** Secciones a cargo del perfil funcional. Las cifras técnicas que necesitas están en `docs/E2_kit_funcional.md`.

### 1. Zonas y regiones
### 2. Caso de uso de la nube: tres razones frente a un centro de datos propio
### 3. ADR de entrada y almacenamiento
### 4. ADR de procesamiento (costo total a 12 meses, x1 y x3)
### 5. Política de acceso y tabla RACI
### 6. Costos de almacenamiento y procesamiento

---

## Parte funcional · Roles y estructura de datos

### Roles
> **[EMILIANO]** Definir al menos tres roles (qué puede hacer, qué no y su responsabilidad). Hay una propuesta en el kit.

### Estructura de datos

**Usabilidad**
- Parquet con compresión Snappy en curated: columnar, solo se leen las columnas que pide la consulta.
- Partición por fecha UTC (`dt=YYYY-MM-DD`) en todas las capas.
- Nombres en `snake_case`: prefijo `fact_`/`dim_` por tipo de tabla, `dq_` para banderas de calidad, `_ts` para fechas y horas, `num_` para conteos.
- Fechas y horas en UTC, con precisión de milisegundos para que Athena las lea.

**Seguridad**
- Bucket cifrado (SSE-S3), con acceso público bloqueado, solo HTTPS y *ownership* del dueño del bucket.
- La ingesta solo puede escribir en `bronze/`. El Job de Glue solo lee `bronze/` y escribe en `silver/` y `archive/`. Las consultas pasan por un workgroup con tope de lectura.
- **Datos personales:** ninguno en el lago. El feed y el clima no identifican personas. Los viajes históricos (con edad y género) **no se suben a la nube**: se procesan en local y el código descarta edad y género al limpiar.

**Costo y retención**

| Zona | Vive | Regla |
|---|---|---|
| `bronze/` | 30 días | Expira (el raw queda en `archive/`) |
| `archive/` | indefinido | Standard → Glacier IR a 30 días → Deep Archive a 365 |
| `silver/` | indefinido | Standard → Standard-IA a 90 días |
| `athena-results/` | 7 días | Expira |
| Logs | 14 días | Retención de CloudWatch |

**Trazabilidad (linaje)**

| Dato fuente | Zona | Tabla / ruta | Job / proceso | Rol que escribe | Dueño | Desde |
|---|---|---|---|---|---|---|
| GBFS `station_status` (Ecobici) | raw | `bronze/station_status/` | Lambda `ingest` (cada 2 min) | Rol de ejecución de la Lambda de ingesta | [EMILIANO: dueño de datos] | 2026-11-10 |
| GBFS `station_information` | raw | `bronze/station_information/` | Lambda `ingest` (1 vez al día) | Rol de ejecución de la Lambda de ingesta | [EMILIANO] | 2026-11-10 |
| Open-Meteo pronóstico | raw | `bronze/weather/` | Lambda `ingest` (cada hora) | Rol de ejecución de la Lambda de ingesta | [EMILIANO] | 2026-11-10 |
| `bronze/station_status/` | archive | `archive/station_status/` | Glue `curate` (diario) | `GlueJobRole` | [EMILIANO] | 2026-11-13 |
| `bronze/station_status/` + catálogo | curated | `fact_station_status` | Glue `curate` (diario) | `GlueJobRole` | [EMILIANO] | 2026-11-13 |
| `bronze/station_information/` | curated | `dim_station` | Glue `curate` (diario) | `GlueJobRole` | [EMILIANO] | 2026-11-13 |
| `bronze/weather/` | curated | `fact_weather_hourly` | Glue `curate` (diario) | `GlueJobRole` | [EMILIANO] | 2026-11-13 |

---

## Evidencias

| Archivo | Qué demuestra |
|---|---|
| [`evidencias/athena_consultas.md`](evidencias/athena_consultas.md) | 3 consultas con bytes escaneados, tiempo y costo |
| [`evidencias/capas_s3.md`](evidencias/capas_s3.md) | Objetos y tamaño por capa y clase de almacenamiento |
| [`evidencias/ciclo_de_vida.json`](evidencias/ciclo_de_vida.json) | Reglas de ciclo de vida activas en el bucket |
| [`evidencias/glue_corridas.md`](evidencias/glue_corridas.md) | Corridas del Job de Glue |
| [`evidencias/ingesta.md`](evidencias/ingesta.md) | Triggers de ingesta y procesamiento activos |

Se generan con `python scripts/evidencias.py --salida E2_EQxx/evidencias --dia <fecha>`.
