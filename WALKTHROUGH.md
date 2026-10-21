# Walkthrough — Multi-City Mobility Observatory

We have upscaled the pipeline from a single-city tracker into a multi-city **Global Real-Time Mobility Observatory** monitoring **over 6,100 stations** across 4 major metropolitan networks:
1. **New York City** (`nyc` — Citi Bike, ~2,519 stations)
2. **Chicago** (`chicago` — Divvy, ~2,074 stations)
3. **Washington, D.C.** (`dc` — Capital Bikeshare, ~868 stations)
4. **San Francisco Bay Area** (`sf` — Bay Wheels, ~646 stations)

---

## What Changes Were Made

### 1. Ingestion Layer ([`ingest.py`](file:///c:/low%20latency%20tracker/ingest.py))
- **Fault-Tolerant Isolation:** Network fetching and database loading are now isolated per city in a structured loop. A transient failure or 5xx error in one feed logs an isolated error and allows the other city feeds to complete successfully.
- **Hive-Partitioned Lake Storage:** Ingested Parquet files are stored under `data/raw/<feed>/city=<city>/batch_id=<batch_id>/data.parquet`.
- **DuckDB Schema Migration & Idempotency:** The DuckDB raw table schema was upgraded with `(city, station_id, _batch_id)` primary key, and automatic table migration logic was added to handle legacy single-city tables seamlessly.
- **Dynamic Partition Pruning:** Retention cleaner automatically globs `city=*/batch_id=*` partitions.

### 2. dbt Staging & Modeling Layer ([`dbt/`](file:///c:/low%20latency%20tracker/dbt))
- **Global Surrogate Key:** Both [`stg_station_information.sql`](file:///c:/low%20latency%20tracker/dbt/models/staging/stg_station_information.sql) and [`stg_station_status.sql`](file:///c:/low%20latency%20tracker/dbt/models/staging/stg_station_status.sql) now generate `global_station_id = city || '-' || station_id` to eliminate ID collisions across cities.
- **Dimension & Fact Models:** [`dim_stations.sql`](file:///c:/low%20latency%20tracker/dbt/models/marts/dim_stations.sql) and [`fct_station_snapshots.sql`](file:///c:/low%20latency%20tracker/dbt/models/marts/fct_station_snapshots.sql) join on `global_station_id` and maintain `city` as a first-class dimension.
- **Local Timezone Commute Alignment:**
  [`fct_station_snapshots.sql`](file:///c:/low%20latency%20tracker/dbt/models/marts/fct_station_snapshots.sql) and [`agg_hourly_metrics.sql`](file:///c:/low%20latency%20tracker/dbt/models/marts/agg_hourly_metrics.sql) map city timezones (`America/New_York`, `America/Chicago`, `America/Los_Angeles`) and calculate `local_snapshot_at`, `local_metric_hour`, and integer `local_hour` ($0..23$). This enables comparing rush hour commute curves across Eastern, Central, and Pacific time zones.

### 3. Data Quality & Assertion Hardening ([`schema.yml`](file:///c:/low%20latency%20tracker/dbt/models/marts/schema.yml), [`assert_unique_snapshot_grain.sql`](file:///c:/low%20latency%20tracker/dbt/tests/assert_unique_snapshot_grain.sql))
- **CONUS Geo Bounds:** Latitude and longitude validations are tightened to realistic Continental US bounds:
  - Latitude: `[35.0, 45.0]`
  - Longitude: `[-125.0, -70.0]`
- **Uniqueness & Non-Null Guarantees:** 40 schema tests assert `not_null` and `unique` on `global_station_id`, composite grain `(global_station_id, batch_id)`, and `local_hour` values ($0..23$).

### 4. Interactive Analytics Dashboard ([`app.py`](file:///c:/low%20latency%20tracker/app.py))
- **Scope & City Switcher:** Top-level dropdown selector with 5 views:
  - 🌐 *All Cities (Comparative Overview)*
  - 🍎 *New York City (Citi Bike)*
  - 🏙️ *Chicago (Divvy)*
  - 🏛️ *Washington, D.C. (Capital Bikeshare)*
  - 🌉 *San Francisco Bay Area (Bay Wheels)*
- **Cross-City Operational Benchmarks:** Comparative summary table when *All Cities* is selected, contrasting system size, available bikes, fleet availability %, and starvation rates across all 4 networks.
- **Dynamic Viewports:** Viewport automatically recenters per city:
  - SF Bay Area is set to `(lat: 37.65, lon: -122.25)` with `zoom: 9.5`, cleanly capturing San Francisco, Oakland, and San Jose stations.
- **Local Hour Commute Curves:** When *All Cities* is selected, Plotly charts starvation volume indexed by local commute hour ($0..23$), directly comparing morning and evening rush hour bottlenecks across timezones.

---

## Verification Results

### 1. Ingestion Execution
```
[NYC] fetched station_information: 2,519 stations
[CHICAGO] fetched station_information: 2,074 stations
[DC] fetched station_information: 868 stations
[SF] fetched station_information: 646 stations
Total stations: 6,107 stations across 4 networks
Result: 4 succeeded, 0 failed.
```

### 2. Idempotency Check
Immediately re-ran `ingest.py`:
```
[NYC] loaded 0 new rows into raw_station_status (2519 duplicate/skipped)
[CHICAGO] loaded 0 new rows into raw_station_status (2074 duplicate/skipped)
[DC] loaded 0 new rows into raw_station_status (868 duplicate/skipped)
[SF] loaded 0 new rows into raw_station_status (646 duplicate/skipped)
```

### 3. dbt Test & Build Suite
```
dbt build --profiles-dir .
Finished running 3 table models, 40 data tests, 2 view models in 2.42s.
Done. PASS=43 WARN=2 ERROR=0 SKIP=0
```
*(The 2 warnings are the intended warning-level tests for valet overflow stations where utilization exceeds 1.0).*

### 4. Parquet Mart Export
```
exported dim_stations -> data\marts\dim_stations.parquet (6,107 rows)
exported fct_station_snapshots -> data\marts\fct_station_snapshots.parquet
exported agg_hourly_metrics -> data\marts\agg_hourly_metrics.parquet (6,107 rows)
```
