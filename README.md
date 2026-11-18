# Ecobici Cloud: predicción de disponibilidad por estación

Proyecto de la materia de Cómputo en la Nube (Maestría en Ciencia de Datos, Universidad Panamericana).

**Objetivo:** predecir cuántas bicis y anclajes libres habrá en cada estación de Ecobici en los próximos 30 a 60 minutos, y señalar qué estaciones necesitan rebalanceo antes de que se vacíen o se saturen.

Este repositorio contiene la ingesta de datos (en local y desplegada en AWS con infraestructura como código), la compactación diaria, las reglas de calidad, las pruebas y la exploración de viajes históricos.

---

## Estructura

```
ecobici-cloud/
├── src/ecobici/
│   ├── config.py         # URLs, zona horaria, horario de operación y almacenamiento (variables de entorno)
│   ├── http.py           # cliente HTTP con reintentos (solo librería estándar)
│   ├── feeds.py          # descarga de GBFS y Open-Meteo, sin pandas
│   ├── gbfs.py           # tipado del feed GBFS a tablas
│   ├── weather.py        # tipado del clima horario
│   ├── quality.py        # reglas de calidad (banderas dq_*) y en_horario
│   ├── storage.py        # bronze/silver particionado por fecha, en local o en S3
│   ├── run_snapshot.py   # captura local para desarrollo
│   ├── trips.py          # viajes históricos: carga, limpieza (sin datos personales) y malla estación × hora
│   ├── features.py       # calendario, festivos, rezagos, perfil histórico y clima (sin fuga de información)
│   ├── modeling.py       # baselines, gradient boosting (Poisson) y métricas por segmento
│   └── aws/
│       ├── ingest.py     # Lambda: bronze cada 2 min (estado) y cada hora (clima)
│       └── compact.py    # silver diario (1 Parquet por tabla) + archivo del raw; lo corre el Job de Glue
├── glue/                 # Job de Glue (curate_job.py) y script para subirlo (upload.sh)
├── scripts/evidencias.py # genera las evidencias del entregable desde la cuenta de AWS
├── E2_EQxx/              # entregable E2: README y evidencias
├── template.yaml         # infraestructura (AWS SAM): S3, Lambda, Glue, horarios, Athena, alarmas, presupuesto
├── samconfig.toml        # configuración de despliegue
├── docs/DESPLIEGUE.md    # guía paso a paso para desplegar en AWS
├── tests/                # pruebas offline, incluido S3 simulado (moto)
├── notebooks/
│   ├── 01_exploracion_viajes.ipynb   # exploración de viajes históricos
│   └── 02_modelo_demanda.ipynb       # pronóstico de retiros y arribos por estación y hora
├── medir.py              # resumen de una captura: pesos, huecos y calidad
└── pyproject.toml
```

## Cómo correrlo

Requiere Python 3.10 o mayor.

```bash
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -e ".[dev]"

python -m pytest                            # pruebas offline (incluye un día simulado en S3)
python -m ecobici.run_snapshot              # un snapshot
python -m ecobici.run_snapshot --loop 120   # un snapshot cada 2 minutos
python medir.py                             # resumen de lo capturado
```

En macOS, para que la captura no se detenga si la computadora entra en reposo:

```bash
caffeinate -i python -m ecobici.run_snapshot --loop 120
```

Los datos se guardan en `data/` (excluida de Git) con la misma estructura de carpetas que tendrá el bucket de S3:

```
data/bronze/<fuente>/dt=YYYY-MM-DD/hour=HH/<epoch>.json.gz   # crudo, tal cual llegó
data/silver/<tabla>/dt=YYYY-MM-DD/<epoch>.parquet            # limpio y tipado
```

Las particiones `dt=` usan la fecha **UTC**: lo capturado en CDMX después de las 18:00 queda en la partición del día siguiente.

---

## Fuentes de datos

| Fuente | Qué trae | Consumo | Frecuencia de consulta |
|---|---|---|---|
| Ecobici GBFS: `station_status` | Bicis y anclajes libres, estado de cada estación | API pública, JSON, sin registro | Cada 2 minutos |
| Ecobici GBFS: `station_information` | Catálogo: nombre, coordenadas, capacidad | API pública, JSON, sin registro | Se consulta en cada corrida para validar; se guarda una vez al día |
| Open-Meteo | Temperatura, lluvia y viento por hora en CDMX | API pública, JSON, sin registro (uso no comercial, < 10,000 llamadas/día, atribución CC BY 4.0) | Cada hora |
| Viajes históricos de Ecobici | Un registro por viaje | CSV mensual del portal de datos abiertos | Mensual |

El feed solo entrega el estado **actual**: el histórico de disponibilidad por estación no existe en ningún lado y se construye capturando snapshots.

---

## Resultados de la primera captura (4 al 5 de noviembre de 2026)

**Captura:** del 4 de noviembre a las 17:39 al 5 de noviembre a las 10:04 (hora CDMX). 489 snapshots en 16.4 horas, 99.2% de completitud y 677 estaciones por snapshot. Un cierre accidental del proceso dejó un hueco menor a 5 minutos.

### Pesos medidos

| Capa | Por snapshot | Por día | Por mes |
|---|---|---|---|
| Bronze `station_status` (JSON gzip) | 6.9 KB | ~5 MB | ~0.15 GB |
| Silver `fact_station_status` (Parquet) | 24.2 KB | ~17 MB | ~0.52 GB |
| Catálogo de estaciones | ~40 KB por copia | — | — |

- El total medido fue de **80.6 MB/día**, pero la mitad era el catálogo, que se guardaba en cada snapshot. **Ya se corrigió:** ahora se guarda una vez al día, lo que deja el total en ~23 MB/día (~0.7 GB/mes).
- El Parquet pesa más que el JSON porque cada archivo es muy pequeño y carga sus propios metadatos. En la nube se compactará en un archivo por día.
- Viajes históricos: ~100 MB y ~1.5 millones de viajes por mes en CSV.

### Calidad del feed

- En plena operación (7:00 a 21:00), entre **86% y 93%** de las estaciones pasan todas las reglas.
- En la apertura y el cierre (5–6 h y 22–23 h) baja a 51–78%, porque las estaciones se encienden y se apagan de forma escalonada. De 1:00 a 4:59 es casi 0%, porque el sistema está cerrado.
- Solo 1 estación estuvo caída todo el periodo; el resto de los reportes atrasados son intermitentes.
- Hubo 2 casos aislados de estaciones que reportaron más bicis y anclajes que su capacidad.

### Calidad de los viajes históricos (septiembre y octubre de 2026, 3.1 millones de viajes)

- ~99.98% de los viajes pasa las reglas críticas. Ninguna fecha resultó ilegible.
- Género: además de M y F existe `O` (otro, 1.8%) y `?` (no reportado, 2.5%), que se trata como dato faltante.
- 61 viajes duran más de 24 horas (bicis perdidas o no ancladas). En los casos extremos, la bici salió entre febrero y junio y regresó en septiembre u octubre: **los archivos mensuales se organizan por fecha de llegada**, así que la partición debe usar la fecha de retiro.
- 447 viajes de menos de 1 minuto, 106 edades atípicas y 59 edades vacías.
- **Unión entre fuentes:** el 100% de las 676 estaciones de los viajes coincide con `short_name` del feed. Unir por `station_id` daría un falso 79% de coincidencia que asignaría viajes a estaciones equivocadas. La llave de unión es siempre `short_name`, como texto (para no perder ceros a la izquierda).

### Hallazgos de demanda

- Pico entre semana a las 8:00 y entre 17:00 y 18:00. Los fines de semana la demanda es más baja y se reparte entre 10:00 y 15:00.
- En las mañanas, pocas estaciones concentran la pérdida de bicis (la principal pierde ~9,700 netas en dos meses) mientras otras se saturan. Son las prioritarias para el modelo y el rebalanceo.
- Duración mediana del viaje: 12 minutos.

---

## Reglas de calidad (`quality.py`)

Las filas no se borran: se marcan con banderas, para que el modelo decida qué excluir y para medir la calidad del feed en el tiempo.

| Bandera | Regla |
|---|---|
| `dq_reporte_viejo` | La estación no reporta desde hace más de 30 minutos |
| `dq_excede_capacidad` | Bicis + anclajes > capacidad |
| `dq_negativo` | Conteos negativos |
| `dq_sin_catalogo` | La estación no existe en el catálogo |
| `dq_fuera_servicio` | Instalada pero sin prestar bicis |
| `dq_duplicado` | Mismo `station_id` y `last_reported` |
| `dq_ok` | Ninguna bandera activa |
| `en_horario` | La hora local está dentro del horario de operación (fuera de 1:00 a 4:59). No es una bandera de error: indica qué filas evaluar y usar en el modelo |

---

## Modelo de datos

Esquema estrella:

| Tabla origen | Relación | Tabla destino | Campo de unión |
|---|---|---|---|
| `dim_station` | 1 a muchos | `fact_station_status_snapshot` | `station_id` |
| `dim_station` | 1 a muchos | `fact_trip` (retiro y llegada) | `short_name` |
| `dim_station` | 1 a muchos | `fact_prediction` | `station_id` |
| `fact_station_status_snapshot` | muchos a 1 | `fact_weather_hourly` | hora (UTC) |
| `fact_station_status_snapshot` | muchos a 1 | `dim_calendar` | fecha |

**Datos personales:** solo `user_gender` y la edad de `fact_trip`. La edad se guarda en rangos y los viajes se usan solo agregados por estación y hora. Lo que alimenta la predicción en tiempo real (feed, clima, calendario) no contiene datos personales.

---

## Arquitectura en AWS

**Implementado (`template.yaml`, guía en [docs/DESPLIEGUE.md](docs/DESPLIEGUE.md)):**

```
EventBridge Scheduler ──(cada 2 min)──► Lambda ingest ──► S3 bronze/station_status   (JSON gzip)
                      ──(cada hora)───► Lambda ingest ──► S3 bronze/weather
                                                     └─► S3 bronze/station_information (1 vez al día)
Glue Trigger ────────(00:15 UTC)───► Glue Job curate ─► S3 silver/<tabla>/dt=YYYY-MM-DD/part-00000.parquet (+ archive/)
                                                              │
                                            Glue Data Catalog (partition projection) ──► Athena
CloudWatch (logs 14 días + 3 alarmas) ──► SNS ──► correo          AWS Budgets ──► correo
```

Decisiones de diseño:
- **Ingesta sin pandas.** Corre 720 veces al día: un paquete de pocos KB arranca rápido y cuesta menos. La limpieza se hace una vez al día.
- **Compactación diaria.** Resuelve el problema de archivos pequeños medido en la primera captura y aplica las reglas de calidad al día completo. Es idempotente: se puede volver a correr para cualquier fecha.
- **Mínimo privilegio.** La ingesta solo puede escribir en `bronze/`; la compactación solo lee `bronze/` y escribe `silver/`.
- **Seguridad del bucket.** Cifrado SSE-S3, acceso público bloqueado y solo conexiones HTTPS. El bucket se conserva si se borra el stack.
- **Sin ciclo de vida a Glacier.** Los objetos pesan ~7 KB y S3 no transiciona objetos de menos de 128 KB; el ahorro calculado era menor a 1%.
- **Control de costos.** Tope de 1 GB por consulta en Athena, logs con retención de 14 días y presupuesto mensual que mide el gasto real sin descontar créditos.

**Modelo de demanda (en desarrollo):** notebook `02_modelo_demanda.ipynb`, con división temporal (prueba en las últimas dos semanas), baselines de perfil histórico y semana pasada, y gradient boosting con pérdida Poisson.

**Siguiente fase (propuesta):** SageMaker para entrenamiento semanal → Lambda de inferencia (cada 10 min) → DynamoDB con la última predicción por estación → dashboard.

**¿Por qué S3 y no una base de datos relacional?** Lo crudo llega como JSON y se guarda tal cual para poder reprocesarlo. El proyecto es analítico, no transaccional: los snapshots solo se agregan, nunca se actualizan. Las tablas en Parquet se consultan con SQL desde Athena, y el costo es menor a 1 USD al mes en el escenario actual. DynamoDB se usa solo para la consulta rápida de la predicción por estación.

**Costo estimado** (us-east-1, precios de lista en USD, noviembre de 2026): ~26 USD al mes en el escenario actual, ~160 a 10x y ~1,419 a 100x. El dashboard representa ~81% del costo actual; el almacenamiento es casi irrelevante.

---

## Próximos pasos

- Correr el notebook 02 con los viajes reales y documentar los resultados contra los baselines.
- Convertir la demanda pronosticada en disponibilidad (`bicis en 1 h ≈ bicis ahora + neto pronosticado`) y validarla con los snapshots de AWS cuando haya dos semanas de captura.
- Cruzar viajes con clima histórico para medir el efecto de la lluvia en la demanda.
- Entrenar el modelo base de pronóstico por estación.

## Fuera de alcance

- Optimización de rutas de los camiones de rebalanceo.
- Datos o perfiles de usuarios individuales.
- App móvil propia.

---

Datos meteorológicos: [Open-Meteo.com](https://open-meteo.com/) (CC BY 4.0). Datos de estaciones y viajes: [Ecobici, datos abiertos](https://ecobici.cdmx.gob.mx/en/open-data/).
