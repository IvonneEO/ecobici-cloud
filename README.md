# Ecobici Cloud: predicción de disponibilidad por estación

Proyecto de Cómputo en la Nube. Perfil técnico.
Objetivo: predecir bicis y anclajes libres por estación a 30–60 minutos y señalar estaciones que necesitan rebalanceo.

---

## 1. Cómo correrlo en local

```bash
python -m venv .venv && source .venv/bin/activate   
pip install -e ".[dev]"
pytest                                   # pruebas offline (no usan internet)
python -m ecobici.run_snapshot           # un snapshot real
python -m ecobici.run_snapshot --loop 120  # un snapshot cada 2 minutos
```

Cada corrida imprime cuántas estaciones llegaron y el resumen de calidad. Los datos quedan en `data/` con la misma estructura de carpetas que tendrá el bucket de S3.

---

## 2. Origen del dato, forma de consumo y frecuencia

| Fuente | Qué trae | Consumo | Frecuencia de cambio | Frecuencia de consulta |
|---|---|---|---|---|
| GBFS `station_status` | Bicis y anclajes libres, estado de cada estación | API REST pública, JSON, sin llave | El feed declara un `ttl` (típicamente 60 s) | Cada 2 minutos |
| GBFS `station_information` | Catálogo: nombre, coordenadas, capacidad | API REST pública, JSON, sin llave | Rara vez (altas, bajas, cambios de capacidad) | Junto con cada snapshot (es pequeño) o 1 vez al día |
| Open-Meteo | Temperatura, lluvia, viento por hora | API REST, JSON, sin llave (uso no comercial, < 10,000 llamadas/día) | Cada hora | 1 vez por hora |
| Viajes históricos Ecobici | Un registro por viaje (retiro, arribo, estaciones, bici, edad, género) | Archivo CSV mensual en el portal de datos abiertos | Mensual | 1 vez al mes |
| Calendario (festivos, quincenas) | Variables de calendario | Lo generamos nosotros | Anual | Se regenera 1 vez al año |

Punto clave: el feed solo entrega el **estado actual**. El histórico de disponibilidad no existe en ningún lado; lo construimos nosotros capturando snapshots desde el día uno. Por eso la ingesta debe empezar ya, aunque el modelo llegue después.

---

## 3. Tipo de dato, peso y almacenamiento

| Fuente | Tipo | Peso aproximado | Dónde vive |
|---|---|---|---|
| `station_status` crudo | Semiestructurado (JSON) | ~190 KB por snapshot (≈687 estaciones × ~280 B). A 720 snapshots/día: ~135 MB/día sin comprimir, ~15 MB/día en gzip, ~0.45 GB/mes | S3 bronze (gzip) |
| `station_status` limpio | Estructurado (Parquet) | ~500 mil filas/día; ~5 MB/día en Parquet+snappy, ~150 MB/mes | S3 silver |
| `station_information` | Semiestructurado → estructurado | ~150 KB | S3 (bronze y silver) |
| Clima | Semiestructurado → estructurado | < 10 KB por hora | S3 |
| Viajes históricos | Estructurado (CSV) | ~97 MB por mes, ~1 millón de viajes | S3 bronze; Parquet en silver |

> Los pesos son estimaciones. Validarlos con las primeras 24 h de captura (basta con `du -sh data/`) y actualizar esta tabla y la calculadora de costos.

**¿Sirve S3?** Sí, como almacenamiento principal (data lake):

- **S3 bronze:** JSON crudo comprimido. Permite auditar y reprocesar si cambia la lógica.
- **S3 silver:** Parquet particionado por fecha (`dt=YYYY-MM-DD`), consultable con **Athena** usando el catálogo de **Glue**. Athena cobra por datos leídos, y Parquet + particiones lo abaratan mucho.
- **Lo que S3 no resuelve:** consultas de baja latencia para la app ("¿cuántas bicis habrá en la estación X en 30 min?"). Para eso, una tabla pequeña en **DynamoDB** con la última predicción por estación.
- **Orquestación:** EventBridge (cada 2 min) → Lambda con este mismo código. Reentrenamiento semanal en SageMaker o en una Lambda/contenedor si el modelo es ligero.

---

## 4. Modelo de datos

Esquema estrella. Las llaves de unión son `station_id` y la hora.

```
dim_station (1) ───< (N) fact_station_status_snapshot
     │                         │ (por hora)
     │                         └──> fact_weather_hourly
     │                         └──> dim_calendar (por fecha)
     └───< (N) fact_trip  (estación de retiro y de arribo)

fact_prediction: salida del modelo, también ligada a dim_station
```

### dim_station
| Campo | Tipo | Nulo | Descripción |
|---|---|---|---|
| station_id | string (PK) | No | ID del feed GBFS |
| name | string | No | Nombre de la estación |
| short_name | string | No | Código corto ("001", "271-272"). **Llave de unión con fact_trip** (validado: 100% de coincidencia) |
| lat, lon | float64 | No | Coordenadas WGS84 |
| capacity | int16 | Sí | Total de anclajes |
| valid_from, valid_to | timestamp | valid_to sí | Vigencia del registro, para cuando cambie capacidad o ubicación (SCD tipo 2) |

### fact_station_status_snapshot (tabla principal)
| Campo | Tipo | Nulo | Descripción |
|---|---|---|---|
| station_id | string (FK) | No | |
| num_bikes_available | int16 | No | Bicis disponibles (variable objetivo) |
| num_bikes_disabled | int16 | Sí | Bicis descompuestas en el anclaje |
| num_docks_available | int16 | No | Anclajes libres (segunda variable objetivo) |
| num_docks_disabled | int16 | Sí | Anclajes fuera de servicio |
| is_installed, is_renting, is_returning | boolean | No | Estado operativo |
| last_reported_ts | timestamp UTC | No | Cuándo reportó la estación |
| feed_last_updated_ts | timestamp UTC | No | Cuándo se generó el feed |
| ingested_at | timestamp UTC | No | Cuándo lo capturamos |
| dq_* | boolean | No | Banderas de calidad (sección 5) |

Llave natural: `(station_id, last_reported_ts)`. Partición: `dt`.

### fact_weather_hourly
| Campo | Tipo | Descripción |
|---|---|---|
| ts_hour (PK) | timestamp UTC | Hora |
| temperature_2m | float | °C |
| precipitation, rain | float | mm |
| wind_speed_10m | float | km/h |
| weather_code | int | Código WMO |

### dim_calendar
`date (PK)`, `day_of_week`, `is_weekend`, `is_holiday`, `is_quincena`, `is_school_vacation`.

### fact_trip (histórico mensual)
| Campo | Tipo | Sensibilidad | Descripción |
|---|---|---|---|
| trip_id | string (PK) | Baja | Generado: año-mes-índice |
| bike_id | string | Baja | ID de la bici |
| station_out_id, station_in_id | string (FK) | Baja | Estación de retiro y de arribo |
| ts_out, ts_in | timestamp | Media | Fecha y hora; combinadas con estación pueden revelar rutinas |
| duration_min | float | Baja | Derivado |
| user_gender | string | **Alta** | Dato personal |
| user_age_band | string | **Alta** | Guardar en rangos (18–24, 25–34…), nunca la edad exacta |

### fact_prediction (salida)
`station_id`, `predicted_at`, `horizon_min` (30/60), `pred_bikes`, `pred_docks`, `p_empty`, `p_full`, `model_version`.

---

## 5. Problemas de calidad esperados

Implementados como banderas en `src/ecobici/quality.py` (no se borran filas, se marcan):

| Problema | Regla | Bandera |
|---|---|---|
| Estación que no reporta (caída o sin señal) | `last_reported` con más de 30 min de antigüedad | `dq_reporte_viejo` |
| Estado imposible | bicis + anclajes > capacidad | `dq_excede_capacidad` |
| Valores negativos | conteos < 0 | `dq_negativo` |
| Estación en el status pero no en el catálogo | sin `capacity` tras el join | `dq_sin_catalogo` |
| Estación instalada pero sin servicio | `is_installed` o `is_renting` falso | `dq_fuera_servicio` |
| Snapshot repetido (el feed no se actualizó) | mismo `station_id` y `last_reported` | `dq_duplicado` |

Pendientes para siguientes iteraciones:

- - **Llaves distintas entre fuentes:** el feed en vivo identifica estaciones con `station_id` (ID interno) y los CSV de viajes con un código corto. Se validó que el 100% de las 676 estaciones de los viajes coincide con `short_name` del feed, incluidas las estaciones dobles (p. ej. `271-272`). Unir por `station_id` daría un falso 79% de coincidencia que asignaría viajes a estaciones equivocadas sin generar error. **Regla:** la unión siempre es `fact_trip.station_out/in = dim_station.short_name`, conservando los IDs como texto para no perder ceros a la izquierda.
- **Huecos de captura:** si la Lambda o el feed fallan, faltan snapshots. Medir completitud por hora.
- **Zona horaria:** el feed usa epoch UTC; los CSV de viajes usan hora local. Todo se normaliza a UTC y se convierte a `America/Mexico_City` solo para features (hora del día).
- **CSV de viajes inconsistentes:** fechas `dd/mm/aaaa`, IDs con ceros a la izquierda (`003`), nombres de columna que cambian entre años (p. ej. `Ciclo_EstacionArribo` sin guion bajo), edades atípicas, arribo anterior al retiro.
- **Cambios de esquema o URL del feed:** las pruebas detectan si falta un campo obligatorio.

- **Calidad:** la fuente es muy limpia; el ~99.98% de los viajes pasa todas las reglas críticas. Hallazgos:
  - **Género:** además de M y F existe `O` (otro, 1.8%), que es válido, y `?` (no reportado, 2.5%), que se trata como dato faltante y no invalida el viaje.
  - **Viajes que cruzan de mes:** 98 viajes con retiro el 31 de agosto aparecen en el archivo de septiembre porque llegaron ya en septiembre. Son legítimos; la partición por mes debe usar la fecha de retiro, no el archivo de origen.Es consistente con que los archivos se organizan por fecha de arribo.
  - **Bicis perdidas o no ancladas:** 61 viajes duran más de 24 horas; los casos extremos (5) salieron entre febrero y junio y regresaron hasta septiembre u octubre, con más de 200 días fuera. No son errores de fecha: indican que los **archivos mensuales se organizan por fecha de arribo**. Por eso la ingesta debe particionar por fecha de retiro y no por archivo de origen. Estos viajes se excluyen del modelo de demanda, pero son un indicador útil de bicis perdidas para operación.
  - **Otros:** viajes de menos de 1 minuto (447), edades atípicas (106), viajes de más de 24 horas (61) y edad nula (59).
  - Ninguna fecha resultó ilegible.

---

## 6. Qué decisiones de negocio habilita

1. **Rebalanceo:** a qué estaciones mandar camiones y a qué hora, antes de que se vacíen o se llenen.
2. **Planeación de capacidad:** qué estaciones pasan más tiempo vacías o llenas y necesitan más anclajes, o dónde abrir estaciones nuevas.
3. **Experiencia del usuario:** avisar "esta estación probablemente estará vacía en 20 min, la más cercana con bicis es X".
4. **Mantenimiento:** estaciones con muchas bicis o anclajes deshabilitados de forma recurrente.

---

## 7. Regulación (avance para el perfil funcional)

| Dato | ¿Personal? | Marco aplicable |
|---|---|---|
| Estado de estaciones, catálogo, clima | No | Términos de uso de cada fuente; Open-Meteo exige atribución y uso no comercial |
| Viajes (edad, género, horarios, estaciones) | Sí, publicado como dato abierto pero potencialmente reidentificable | Ecobici es un sistema del gobierno de la CDMX, así que quien publica los datos se rige por la Ley de Protección de Datos Personales en Posesión de Sujetos Obligados de la Ciudad de México (vigilada por el INFO CDMX). Nosotros, como particulares que los tratamos, caeríamos en la LFPDPPP. Validar con el perfil funcional la versión vigente y qué autoridad la vigila, porque el marco federal cambió en 2025 |

Medidas: usar viajes solo agregados por estación y hora, edad en rangos, cifrado en reposo en S3 (SSE-S3 o KMS) y acceso por IAM con mínimo privilegio.

---

## 8. Fuera de alcance (por ahora)

- Optimización de rutas de los camiones de rebalanceo.
- Datos de usuarios individuales o perfiles de uso.
- App móvil propia (la salida es dashboard + endpoint).
