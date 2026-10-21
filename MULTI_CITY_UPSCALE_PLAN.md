# Implementation Plan — Multi-City Mobility Observatory

Upscale the existing single-city (NYC Citi Bike) pipeline into a multi-city **Global Urban Mobility & Transit Resilience Observatory**, monitoring real-time micro-mobility health across major metropolitan networks: **New York City (Citi Bike)**, **Chicago (Divvy)**, **Washington, D.C. (Capital Bikeshare)**, and **San Francisco Bay Area (Bay Wheels)** (with extensible configuration for Paris and London).

---

## User Review Required

> [!IMPORTANT]
> **Schema & Primary Key Evolution:**
> Upstream `station_id` values are unique within a city, but can collide across cities (e.g., both NYC and Chicago might have a station `101`). We introduce `city` across all raw, staging, and mart layers, and generate a global surrogate identifier:
> `global_station_id = city || '-' || station_id`
> This preserves 100% data integrity without breaking existing single-city analytics.

> [!NOTE]
> **Initial City Selection:**
> We will configure the pipeline with 4 premier North American GBFS systems totaling **over 6,100 stations**:
> 1. **New York City** (`nyc` — Citi Bike, ~2,519 stations)
> 2. **Chicago** (`chicago` — Divvy, ~2,074 stations)
> 3. **Washington, D.C.** (`dc` — Capital Bikeshare, ~868 stations)
> 4. **San Francisco Bay Area** (`sf` — Bay Wheels, ~646 stations)
> All 4 use standard GBFS feeds with 100% field parity and zero API authentication required.

---

## Proposed Changes

```
GBFS Endpoints (NYC, Chicago, DC, SF)
   │
   ▼
ingest.py ──► data/raw/<feed>/city=<city>/batch_id=<ts>/data.parquet
   │          + warehouse.duckdb (raw tables with `city` column)
   ▼
dbt-duckdb ──► Staging (extracts `city`, creates `global_station_id`)
   │           Marts (dim_stations, fct_station_snapshots, agg_hourly_metrics)
   │           Schema Tests (global coordinate & utilization validations)
   ▼
export_marts.py ──► data/marts/*.parquet
   │
   ▼
app.py (Streamlit)
   ├── Multi-City Selector + Comparative KPIs
   ├── Dynamic Map Auto-Centering per Selected City
   └── Cross-City Starvation & Bottleneck Comparison
```

---

### Ingestion Layer

#### [MODIFY] `ingest.py`

1. Replace static single-city `FEEDS` with a structured `CITIES` registry mapping city codes to their `station_information` and `station_status` URLs.
2. Update `write_raw_partition`:
   - Store Parquet under Hive-partitioned directories: `data/raw/<feed>/city=<city>/batch_id=<batch_id>/data.parquet`.
   - Add `city` column to each raw record.
3. Update `RAW_TABLE_DDL`:
   - Primary key becomes `(city, station_id, _batch_id)`.
4. Update `load_raw_table`:
   - Insert rows with `(station_id, city, payload, batch_id, loaded_at, source)`.
5. Update `prune_retention`:
   - Safely prune expired batch directories across all city folders.

---

### dbt Modeling & Data Quality Layer

#### [MODIFY] `dbt/models/staging/schema.yml`
- Update source definition to recognize Hive partitioning:
  `external_location: "read_parquet('../data/raw/{name}/**/*.parquet', hive_partitioning=true)"`
  *(DuckDB automatically injects `city` and `batch_id` as virtual columns from the file path!)*
- Relax NYC-specific coordinate tests (`40.5 - 41.0`, `-74.3 - -73.7`) to global valid coordinates:
  - Latitude: `-90.0` to `90.0`
  - Longitude: `-180.0` to `180.0`

#### [MODIFY] `dbt/models/staging/stg_station_information.sql`
- Include `city` in the SELECT.
- Generate `global_station_id = city || '-' || cast(station_id as varchar)`.
- Update deduplication window: `partition by city, station_id, batch_id`.

#### [MODIFY] `dbt/models/staging/stg_station_status.sql`
- Include `city` in the SELECT.
- Generate `global_station_id = city || '-' || cast(station_id as varchar)`.
- Update deduplication window: `partition by city, station_id, batch_id`.

#### [MODIFY] `dbt/models/marts/dim_stations.sql`
- Include `city` and `global_station_id`.
- Deduplicate on `global_station_id` (or `city, station_id`).

#### [MODIFY] `dbt/models/marts/fct_station_snapshots.sql`
- Join `stg_station_status` to `dim_stations` on `global_station_id`.
- Retain `city` column in fact records.

#### [MODIFY] `dbt/models/marts/agg_hourly_metrics.sql`
- Group by `metric_hour, city, station_id, global_station_id`.
- Retain `city` column for cross-city aggregations.

#### [MODIFY] `dbt/models/marts/schema.yml`
- Update uniqueness tests:
  - `dim_stations`: `unique` on `global_station_id`.
  - `fct_station_snapshots`: `unique` on composite `global_station_id || '-' || coalesce(cast(batch_id as varchar), '')`.
- Relax lat/lon range constraints to global coordinates.

---

### Visualization Layer

#### [MODIFY] `app.py`

1. **City Switcher & Global Filter:**
   - Add a top-level dropdown: `["All Cities (Global Overview)", "New York (Citi Bike)", "Chicago (Divvy)", "Washington D.C. (Capital Bikeshare)", "San Francisco (Bay Wheels)"]`.
2. **City Benchmark View (when "All Cities" is selected):**
   - High-level comparison table/cards: Fleet Size, Total Active Bikes, Average Availability %, Starvation Alert Count across each city.
   - Comparative bar chart of dock starvation rates by city.
3. **Dynamic Geospatial Auto-Centering:**
   - Pre-configure viewport centers per city:
     - NYC: `lat=40.75, lon=-73.97, zoom=10.5`
     - Chicago: `lat=41.88, lon=-87.63, zoom=10.5`
     - DC: `lat=38.90, lon=-77.03, zoom=11.0`
     - SF: `lat=37.77, lon=-122.42, zoom=11.0`
     - Global: Zoomed out or auto-calculated from coordinates.
4. **Enhanced KPIs & Trends:**
   - Filter time series and starvation alerts based on selected city or show cross-city comparative curves.

---

### Documentation & Automation Layer

#### [MODIFY] `README.md`
- Update title, description, and architectural diagrams to reflect the multi-city platform.
- Document the new cities, data volume (~6,100+ stations), and city-partitioned data lake layout.

#### [VERIFY] `.github/workflows/pipeline.yml`
- Ensure workflow runs cleanly on multi-city ingestion without timeout or permission issues.

---

## Verification Plan

### Automated Tests
1. **Multi-City Ingestion Verification:**
   ```powershell
   .\.venv\Scripts\python.exe ingest.py
   ```
   *Assert:* Ingests NYC, Chicago, DC, and SF; verifies Hive partitions created under `data/raw/` for each city.

2. **Idempotency Check:**
   Run `ingest.py` a second time immediately.
   *Assert:* Zero duplicate rows in `warehouse.duckdb` and raw row counts remain unchanged.

3. **dbt Transformation & Quality Suite:**
   ```powershell
   cd dbt
   ..\.venv\Scripts\dbt.exe build --profiles-dir .
   cd ..
   ```
   *Assert:* All staging models, dimensional tables, facts, aggregations, and schema assertions pass with 0 errors.

4. **Mart Export:**
   ```powershell
   .\.venv\Scripts\python.exe export_marts.py
   ```
   *Assert:* Export completes and Parquet files in `data/marts/` reflect multi-city schema.

### Manual Verification
1. Launch the Streamlit dashboard:
   ```powershell
   .\.venv\Scripts\streamlit.exe run app.py
   ```
2. Verify:
   - Selecting "All Cities" displays comparative KPIs across NYC, Chicago, DC, and SF.
   - Selecting each individual city recenters the PyDeck map smoothly to that city's geographic bounds.
   - Station cards, starvation lists, and capacity indicators populate correctly for every city.
