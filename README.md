# Global Real-Time Mobility Observatory
<img width="1920" height="836" alt="image" src="https://github.com/user-attachments/assets/e0ccd9ef-f745-4733-a597-00be08f337d0" />
<img width="1920" height="836" alt="image" src="https://github.com/user-attachments/assets/dd01b9ee-3f23-40a6-af6a-914f33c607c1" />
<img width="1920" height="836" alt="image" src="https://github.com/user-attachments/assets/2fccd1e1-3486-459b-bede-11facb5323cc" />
<img width="1920" height="836" alt="image" src="https://github.com/user-attachments/assets/6d2926a9-9f01-4b1e-b6bb-3648e94baf31" />



End-to-end, **zero-cost** production data pipeline and analytics dashboard tracking real-time
public micro-mobility networks across major metropolitan areas using open **GBFS** feeds:
- **New York City** (Citi Bike — ~2,500+ stations)
- **Chicago** (Divvy — ~2,000+ stations)
- **Washington, D.C.** (Capital Bikeshare — ~850+ stations)
- **San Francisco Bay Area** (Bay Wheels — ~650+ stations)

Total footprint: **over 6,000 stations monitored in real time**.

Demonstrates production data engineering: **fault-tolerant idempotent ingestion**, **Hive-partitioned lakehouse storage**, **modular dimensional ELT with dbt**, **timezone-aligned commute metrics**, **automated schema & CONUS coordinate testing**, and **interactive multi-city geospatial analytics**.

```
GBFS Public REST APIs (NYC, Chicago, DC, SF)
   │  httpx (exponential backoff + jitter, isolated per city)
   ▼
ingest.py ──► data/raw/<feed>/city=<city>/batch_id=<ts>/data.parquet
   │          + warehouse.duckdb raw tables (city, station_id, _batch_id)
   ▼
dbt-duckdb ──► Staging (JSON flattening, surrogate key: city-station_id)
   │          ├── dim_stations (latest station dimensions)
   │          ├── fct_station_snapshots (real-time utilization, starvation, overflow)
   │          └── agg_hourly_metrics (local_hour commute alignment across ET, CT, PT)
   ▼
export_marts.py ──► data/marts/*.parquet
   │
   ▼
Streamlit (app.py) ──► Cross-city benchmarks + dynamic PyDeck map + local commute curves
```

## Supported Metropolitan Networks

| City | Network | Timezone | Stations |
|---|---|---|---|
| **New York City** (`nyc`) | Citi Bike | `America/New_York` (ET) | ~2,519 |
| **Chicago** (`chicago`) | Divvy | `America/Chicago` (CT) | ~2,074 |
| **Washington, D.C.** (`dc`) | Capital Bikeshare | `America/New_York` (ET) | ~868 |
| **San Francisco** (`sf`) | Bay Wheels (SF, Oakland, San Jose) | `America/Los_Angeles` (PT) | ~646 |

## Architecture

| Layer | Tool | Notes |
|---|---|---|
| Ingestion | `ingest.py` (`httpx`, `duckdb`) | Fault-tolerant per-city fetch; retry w/ backoff & jitter |
| Storage | Parquet + `warehouse.duckdb` | Hive partitioning (`city=<city>/batch_id=<batch_id>/`) |
| Transform | `dbt-duckdb` | Kimball dimensional modeling, surrogate keys, timezone mapping |
| Visualization | Streamlit + PyDeck + Plotly | City selector, CONUS overview, local commute rush hour alignment |
| Orchestration | GitHub Actions | Scheduled cron; commits Parquet artifacts to `data` branch |

## Idempotency & Fault Isolation

- **Fault-Tolerant Fetching**: Each metropolitan feed is isolated. A transient network failure or downtime in one city will not block or abort ingestion for the others.
- **Idempotency**: Upstream `last_updated` epoch serves as `batch_id`. Re-running ingestion in the same publication cycle safely overwrites `city=<city>/batch_id=<n>/` without duplicating data.
- **DuckDB Raw Storage**: Rows are inserted with `INSERT OR IGNORE` on `(city, station_id, _batch_id)`.

## Local Quickstart

Requires **Python 3.12**.

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

# 1. Ingest multi-city snapshot
.\.venv\Scripts\python.exe ingest.py

# 2. Build and test dbt models
cd dbt
..\.venv\Scripts\dbt.exe build --profiles-dir .

# 3. Export marts to Parquet
cd ..
.\.venv\Scripts\python.exe export_marts.py

# 4. Launch multi-city dashboard
.\.venv\Scripts\streamlit.exe run app.py
```

## Models & Metrics

| Model | Grain | Description |
|---|---|---|
| `stg_station_information` | city × station × batch | Typed/flattened static attributes with `global_station_id` |
| `stg_station_status` | city × station × batch | Typed/flattened availability snapshots |
| `dim_stations` | global_station_id | Latest static attributes per station across all cities |
| `fct_station_snapshots` | global_station_id × batch | Utilization, dock starvation, overflow, and `local_snapshot_at` |
| `agg_hourly_metrics` | city × station × local_hour | Hourly rollups aligned by local commute hour across timezones |

## Data Quality & Test Suite

- `unique` and `not_null` on `global_station_id` (`city || '-' || station_id`).
- `accepted_range` on CONUS coordinates (lat `35.0–45.0`, lon `-125.0– -70.0`).
- `accepted_range` on capacity (`>= 0`) and available bikes/docks (`0–200`).
- Warning-level test on `utilization_rate` (`0–1`) accommodating valet overflow stations.

## CI / Automation

`.github/workflows/pipeline.yml` runs every 30 minutes:
1. Restores `data` branch (raw + marts Parquet).
2. `python ingest.py` across all configured metropolitan feeds.
3. `dbt build` — passing data tests gate deployment.
4. `python export_marts.py` and commit updated artifacts to `data` branch.
