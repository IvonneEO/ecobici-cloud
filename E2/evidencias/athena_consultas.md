# Consultas en Athena

Generado: 2026-11-18 02:15 UTC · Workgroup `ecobici-cloud` (tope de 1 GB por consulta) · Día: `2026-11-16`

Costo = max(bytes escaneados, 10 MB) × 5 USD/TB, como cobra Athena.

| Consulta | Bytes escaneados | Tiempo | Costo (USD) | Query ID |
|---|---|---|---|---|
| Q1. Calidad por hora de un día (con filtro de partición) | 59,695 (58.3 KB) | 0.9 s | 0.00004768 | `75e6d6bc-44df-49dd-a65e-6288bebe77d4` |
| Q2. La misma consulta sin filtro de partición | 403,778 (394.3 KB) | 0.8 s | 0.00004768 | `c9851494-03d7-423b-be28-ef08fa31b602` |
| Q3. Estaciones que más tiempo pasaron vacías (negocio, con JOIN) | 534,129 (521.6 KB) | 0.9 s | 0.00004768 | `654f0ce0-2f75-4780-b646-24440d664be7` |

## Q1. Calidad por hora de un día (con filtro de partición)

Lee solo la partición del día y 2 columnas: el caso eficiente.

```sql
SELECT hour(ingested_at AT TIME ZONE 'America/Mexico_City') AS hora_cdmx,
       count(DISTINCT ingested_at) AS snapshots,
       round(avg(IF(dq_ok, 1.0, 0.0)) * 100, 1) AS pct_validas
FROM ecobici.fact_station_status
WHERE dt = '2026-11-16'
GROUP BY 1
ORDER BY 1
```

**Bytes escaneados:** 59,695 · **Tiempo:** 0.9 s · **Costo:** 0.00004768 USD

Primeras filas:

| hora_cdmx | snapshots | pct_validas |
|---|---|---|
| 0 | 30 | 20.9 |
| 1 | 30 | 0.2 |
| 2 | 30 | 0.0 |
| 3 | 30 | 0.0 |
| 4 | 30 | 0.1 |
| 5 | 30 | 32.3 |
| 6 | 30 | 31.8 |
| 7 | 30 | 50.1 |
| 8 | 30 | 65.9 |
| 9 | 30 | 72.1 |

## Q2. La misma consulta sin filtro de partición

Recorre todos los días capturados: muestra cuánto ahorra particionar por fecha.

```sql
SELECT hour(ingested_at AT TIME ZONE 'America/Mexico_City') AS hora_cdmx,
       count(DISTINCT ingested_at) AS snapshots,
       round(avg(IF(dq_ok, 1.0, 0.0)) * 100, 1) AS pct_validas
FROM ecobici.fact_station_status
GROUP BY 1
ORDER BY 1
```

**Bytes escaneados:** 403,778 · **Tiempo:** 0.8 s · **Costo:** 0.00004768 USD

Primeras filas:

| hora_cdmx | snapshots | pct_validas |
|---|---|---|
| 0 | 210 | 23.6 |
| 1 | 210 | 0.3 |
| 2 | 210 | 0.1 |
| 3 | 210 | 0.1 |
| 4 | 210 | 0.1 |
| 5 | 210 | 37.7 |
| 6 | 210 | 52.1 |
| 7 | 210 | 71.9 |
| 8 | 210 | 82.4 |
| 9 | 210 | 84.7 |

## Q3. Estaciones que más tiempo pasaron vacías (negocio, con JOIN)

Une hechos con el catálogo; solo horario de operación y registros válidos.

```sql
SELECT s.short_name, s.name,
       round(avg(IF(f.num_bikes_available = 0, 1.0, 0.0)) * 100, 1) AS pct_tiempo_vacia
FROM ecobici.fact_station_status f
JOIN ecobici.dim_station s ON s.station_id = f.station_id AND s.dt = f.dt
WHERE f.dt = '2026-11-16' AND f.en_horario AND f.dq_ok
GROUP BY 1, 2
ORDER BY pct_tiempo_vacia DESC
LIMIT 10
```

**Bytes escaneados:** 534,129 · **Tiempo:** 0.9 s · **Costo:** 0.00004768 USD

Primeras filas:

| short_name | name | pct_tiempo_vacia |
|---|---|---|
| 332 | CE-332 Calle 27-Avenida 3 | 50.8 |
| 349 | CE-349 Augusto Rodin-Avenida San Antonio | 49.3 |
| 681 | CE-681 Guty Cárdenas - Ricardo Castro | 49.2 |
| 619 | CE-619 Kiff - Rafael Solana Verduguillo | 47.7 |
| 348 | CE-348 Indiana-Avenida San Antonio | 45.7 |
| 682 | CE-682 Manuel M. Ponce - Fernando M. Villalpando | 45.6 |
| 352 | CE-352 Tintoreto-Giorgione | 44.4 |
| 331 | CE-331 Avenida 2-Calle 17 | 43.7 |
| 177 | CE-177 Benjamín Hill - Carlos Zetina | 43.0 |
| 710 | CE-710 Molino del Rey - Glorieta de la Lealtad | 41.4 |
