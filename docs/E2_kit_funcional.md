# Kit para la Ruta 2 (perfil funcional) · E2

Este documento reúne las cifras y hechos técnicos que necesita el perfil funcional para escribir su parte del entregable. **Las decisiones y la redacción son del perfil funcional**; aquí hay datos, alternativas ya costeadas y propuestas para discutir. Todo lo que escribas va en `E2/README.md`, en las secciones marcadas `[EMILIANO]`.

Precios: lista de AWS en **us-east-1**, en **USD**, sin capa gratuita, consultados el 13/11/2026.

---

## Hechos del sistema

| Concepto | Valor |
|---|---|
| Estaciones | ~677 |
| Frecuencia de captura | estado cada 2 min (720/día), clima cada hora (24/día) |
| Volumen raw | ~7 KB por snapshot comprimido; ~5 MB/día |
| Volumen curated (estimado; confirmar con `evidencias/capas_s3.md`) | ~4 MB/día en Parquet |
| Región | us-east-1 (Norte de Virginia) |
| ¿Hay datos personales en la nube? | **No.** Solo estado de estaciones (público) y clima. Los viajes con edad y género se quedan en local |
| Fuente recuperable si se pierde | **No** para el estado de estaciones (el feed solo da "ahora"); **sí** para el clima (API histórica) |
| Costo total actual | ~0.50 USD/mes (x1) |

---

## 1. Zonas y regiones: datos para decidir

- **Una región (us-east-1).** Es la más barata y tiene todos los servicios que usamos. La alternativa es **Mexico (Central) mx-central-1**, que tiene precios más altos y sirve si se exige que los datos residan en México. No es nuestro caso: los datos son públicos y no personales.
- **Varias zonas (AZ) sin pagar extra.** S3 Standard guarda cada objeto en al menos 3 zonas de disponibilidad (durabilidad de 99.999999999%). Lambda, Glue y Athena son servicios regionales que corren en varias AZ.
- **¿Replicar a otra región (CRR)?**
  - Costo: duplicar el almacenamiento (~0.2 GB al año) más la transferencia (0.02 USD/GB), menos de 0.10 USD al año.
  - Argumento a favor: el estado de estaciones es irrecuperable si se pierde.
  - Argumento en contra: la ingesta tampoco funcionaría si cae la región; habría que duplicar todo el stack.
  - Hay que decidir y poner una condición de cambio, por ejemplo: *"si el histórico se vuelve un producto con usuarios externos o un SLA, activamos CRR a us-west-2"*.

## 2. Nube vs. centro de datos propio: ejemplos del proyecto

| Razón | Ejemplo concreto del proyecto |
|---|---|
| Pago por uso, sin inversión inicial | Todo el sistema cuesta ~0.50 USD/mes. Un servidor propio encendido 24/7 cuesta más solo en luz, sin contar el hardware |
| Servicios administrados | No hay servidores que parchar: Lambda corre la ingesta, Glue el procesamiento y Athena las consultas. El equipo es de 2 personas |
| Elasticidad | Pasar a 3 o 10 ciudades (x3, x10) no requiere comprar hardware: el costo crece proporcional (ver la sección 6) |
| Durabilidad y disponibilidad | S3 replica en 3 zonas automáticamente; en un centro propio habría que montar respaldos |
| Velocidad y repetibilidad | Toda la infraestructura se crea con un comando (`sam deploy`) desde un archivo versionado en Git |

## 3. ADR de entrada y almacenamiento: alternativas costeadas

**Entrada (cómo llega el dato):**

| Alternativa | Costo/mes (x1) | Riesgo |
|---|---|---|
| **EventBridge Scheduler + Lambda (actual)** | ~0.15 USD | Límite de 15 min por ejecución (sobrado: hoy tarda ~1–2 s) |
| Servidor EC2 t4g.nano con cron | ~3 USD + mantenimiento | Hay que parcharlo; si se cae, la captura para |
| Kinesis Data Streams (flujo continuo) | ~29 USD (modo on-demand) | Sobredimensionado: la fuente no es un flujo, se consulta cada 2 min |

**Almacenamiento:**

| Alternativa | Costo/mes (x1) | Riesgo |
|---|---|---|
| **S3 + Parquet + Athena (actual)** | ~0.25 USD (almacenamiento + solicitudes) | Consultas en segundos, no en milisegundos |
| DynamoDB (escribir cada estación en cada snapshot) | ~9 USD (14.6 M escrituras/mes) | Caro para análisis por lotes; no sirve para SQL analítico |
| RDS PostgreSQL db.t4g.micro | ~12 USD (encendida 24/7) | Hay que administrarla; paga aunque no se use |

Condiciones de cambio sugeridas: *"si la frecuencia baja a menos de 1 minuto o se agregan más de 10 fuentes, evaluar Kinesis"*; *"si hay más de 1 millón de consultas por llave al día (app para usuarios), agregar DynamoDB para la última predicción por estación"*.

## 4. ADR de procesamiento: costo total a 12 meses

Supuesto x3: tres veces el volumen (3 sistemas o 3 veces las estaciones).

| Alternativa para raw → curated | 12 meses x1 | 12 meses x3 | Riesgo |
|---|---|---|---|
| **Glue Python shell, 0.0625 DPU (actual)** | **0.33 USD** | **1.00 USD** | Un solo nodo: no escala a volúmenes grandes |
| Glue Spark (mínimo 2 DPU) | 10.70 USD | 32.10 USD | ~32 veces más caro sin necesidad a este volumen |
| Lambda (1 GB, ~60 s) | 0.37 USD | 1.09 USD | Límite de 15 min y 10 GB de memoria |
| EMR | >100 USD | >100 USD | Clúster sobredimensionado |

**Costo total del sistema a 12 meses** (ingesta, procesamiento, almacenamiento, solicitudes y consultas): **~5.92 USD (x1)** y **~17.76 USD (x3)**.

Condición de cambio sugerida: *"si el raw diario supera 1 GB o el job tarda más de 10 minutos, migramos a Glue Spark"*.

## 5. Roles y RACI: propuesta para discutir

| Rol | Puede | No puede | Responsabilidad |
|---|---|---|---|
| Dueño de datos (negocio) | Aprobar accesos a cada zona; definir retención | Ejecutar jobs ni modificar infraestructura | Que el dato sirva al negocio y cumpla las reglas |
| Ingeniero de datos | Escribir en `bronze/`, `silver/` y `archive/`; desplegar y ejecutar el job | Cambiar permisos ni el presupuesto | Que la ingesta y el job funcionen y tengan calidad |
| Científico/analista de datos | Leer `silver/` con Athena; leer resultados | Escribir en S3; leer `bronze/` | Modelos y análisis |
| Auditor de seguridad | Leer CloudTrail, roles, políticas y costos | Cambiar cualquier recurso | Revisar accesos y gasto |

RACI de ejemplo (R = responsable, A = aprueba, C = consultado, I = informado):

| Actividad | Dueño de datos | Ingeniero de datos | Analista | Auditor |
|---|---|---|---|---|
| Definir retención y accesos por zona | A | C | C | I |
| Operar ingesta y Job de Glue | I | R/A | I | I |
| Analizar datos y entrenar el modelo | A | C | R | I |
| Revisar permisos y costos | I | C | I | R/A |

## 6. Costos de almacenamiento y procesamiento

| Rubro | 12 meses x1 | 12 meses x3 |
|---|---|---|
| Almacenamiento (con las políticas de ciclo de vida) | 0.21 USD | 0.64 USD |
| Solicitudes a S3 (PUT/LIST/GET) | 2.74 USD | 8.21 USD |
| Procesamiento (Lambda ingesta, Glue, Athena, Scheduler, logs) | 2.97 USD | 8.91 USD |
| **Total** | **5.92 USD** | **17.76 USD** |

Lecturas útiles:
- **El costo lo dominan las solicitudes y la ingesta, no el almacenamiento.** Las políticas de ciclo de vida ahorran poco en pesos porque los datos pesan poco, pero dejan el sistema listo para escalar.
- La mayor palanca de costo sería la frecuencia de captura: pasar de 2 a 1 minuto duplica ingesta y solicitudes.
- Si se agrega el dashboard en QuickSight, suma ~21 USD/mes (1 autor y 3 lectores), más que todo lo demás junto.

---

## Pendientes que solo puede resolver el perfil funcional

- Número de equipo (para renombrar `E2`).
- Nombre del dueño de datos en la tabla de linaje.
- Decisión sobre replicación entre regiones, con su condición de cambio.
- Redacción de los dos ADR, con fecha y responsable.
- Roles y RACI finales.
