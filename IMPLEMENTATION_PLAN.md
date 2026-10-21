# Implementation Plan — Zero-Cost GBFS Mobility Pipeline

End-to-end, zero-cost ELT pipeline and analytics dashboard for public GBFS bikeshare data
(Citi Bike NYC: `station_information.json`, `station_status.json`).

Stack: Python (`httpx` + `duckdb`) → dbt-duckdb → Streamlit → GitHub Actions.

---

## Confirmed Decisions

- **Raw layer**: per-station **JSON blob** + metadata; flattening happens in dbt (handles the
  heterogeneous `num_scooters_*` fields present on only some records).
- **Artifacts**: **Parquet committed to an orphan `data` branch** (raw partitions + mart exports).
- **Python**: **3.12** locally and in CI (only 3.14 is currently installed; 3.12 will be installed).
- **Git**: `git init` + `.gitignore`.

## Verified Feed Facts (drive schema + tests)

- 2,519 stations per feed; `station_id` unique within each feed.
- Root has `last_updated`, `ttl` (60), `version`; `last_updated` is identical across both feeds
  → used as the **batch id**.
- `station_information` fields: `name, short_name, lat, lon, region_id (13 null), capacity,
  has_kiosk, station_type, eightd_has_key_dispenser`.
- `station_status` fields: `num_bikes_available, num_bikes_disabled, num_docks_available,
  num_docks_disabled, num_ebikes_available, (num_scooters_available — optional),
  is_installed, is_renting, is_returning, last_reported (epoch)`.
- Coordinate ranges: lat `40.611–40.912`, lon `−74.098 – −73.832`.

## Idempotency Design

`_batch_id = root["last_updated"]` (int). Raw is stored as **one Parquet partition per batch**:

```
data/raw/<feed>/batch_id=<ts>/data.parquet
```

- Re-run within the same 60s publication window overwrites the same partition → no duplicates.
- New publication → new partition.
- Staging additionally dedups on natural keys (defense in depth).

---

## 1. Repo Scaffold

```
├─ .gitignore                 # .venv, warehouse.duckdb, __pycache__, dbt target/ logs/
├─ requirements.txt           # pinned: httpx, duckdb, dbt-duckdb, dbt-core, streamlit, pandas, pyarrow, plotly, pydeck
├─ README.md                  # quickstart + architecture diagram
├─ ingest.py
├─ app.py
├─ data/                      # raw + marts parquet (tracked on `data` branch)
└─ dbt/
   ├─ dbt_project.yml
   ├─ profiles.yml            # duckdb target -> ../../warehouse.duckdb
   ├─ models/staging/{stg_station_information.sql, stg_station_status.sql, schema.yml}
   └─ models/marts/{dim_stations.sql, fct_station_snapshots.sql, agg_hourly_metrics.sql, schema.yml}
```

## 2. `ingest.py` (httpx + duckdb)

- `httpx.Client` with a manual retry loop (exponential backoff + jitter, ~4 tries, 1s→8s) on
  transport errors, timeouts, and 5xx responses.
- Fetch both endpoints; extract `last_updated` (batch id) and `data.stations`.
- Write raw Parquet per batch via DuckDB `COPY (... FORMAT PARQUET)`, deleting any existing
  `batch_id=<ts>` directory first (clean idempotent overwrite).
  Columns: `station_id, payload (JSON), _batch_id, _loaded_at (TIMESTAMPTZ), _source`.
- Also load into `warehouse.duckdb` raw tables (`INSERT OR IGNORE`, PK `(station_id, _batch_id)`)
  for local ad-hoc queries.
- Prune raw partitions older than a retention window (default 14 days).
- Structured logging; non-zero exit on failure so CI can alert.

## 3. dbt Project

- `profiles.yml`: `type: duckdb`, `path: ../warehouse.duckdb` (+ dev target). Sources point at the
  raw Parquet glob so CI does not depend on a persisted `.duckdb`:
  `external_location: "read_parquet('data/raw/station_status/**/*.parquet', hive_partitioning=true)"`.

### Staging (flatten via `json_extract_string` + casts)

- `stg_station_information.sql` → typed dim source (lat/lon double, capacity/region int, bools).
- `stg_station_status.sql` → `num_*` ints, bools from 0/1, `last_reported_at = to_timestamp(last_reported)`;
  NULL-safe for optional scooter fields. Dedup `row_number()` on `(station_id, _batch_id)`.

### Marts

- `dim_stations.sql` — latest info per `station_id` (name, capacity, lat, lon, region, has_kiosk, station_type).
- `fct_station_snapshots.sql` — grain `station_id × _batch_id`, joined to dim; adds `capacity`,
  `utilization_rate = bikes / NULLIF(capacity, 0)`,
  `is_starved = bikes=0 AND is_renting AND is_installed`,
  `is_overflow = docks=0 AND is_returning AND is_installed`, `snapshot_at`.
- `agg_hourly_metrics.sql` — `date_trunc('hour', snapshot_at) × station_id`: avg utilization,
  starvation snapshots (→ minutes), overflow events, avg bikes.

### Quality & Tests (`schema.yml`)

- `unique` / `not_null` on keys (`station_id`, composite `station_id + _batch_id`).
- `accepted_range`: lat `40.5–41.0`, lon `−74.3 – −73.7`, `capacity >= 0`,
  `num_bikes_available 0–200`, `num_docks 0–200`, `utilization_rate 0–1` (where capacity > 0).
- One singular test asserting no duplicate `(station_id, _batch_id)`.

## 4. `app.py` (Streamlit, reads marts)

- Reads `warehouse.duckdb` if present, else marts Parquet (`data/marts/*.parquet`).
- **KPIs**: monitored stations, system availability rate `Σ bikes / Σ capacity`, active starvation
  alerts (latest snapshot `is_starved`).
- **Geo view**: pydeck scatter colored by current availability (capacity-normalized).
- **Time series**: plotly of starved-station count over the last 24h from `agg_hourly_metrics`.

## 5. Automation — `.github/workflows/pipeline.yml`

- Triggers: `schedule` every 30 min + `workflow_dispatch`; `concurrency` guard;
  `permissions: contents: write`.
- Steps: checkout main → setup-python **3.12** → pip install → checkout orphan `data` branch into
  `data/` → `python ingest.py` → `dbt build --project-dir dbt` → export marts to
  `data/marts/*.parquet` → commit + push `data` branch (bot) → on failure create/comment a GitHub
  issue (alert).

## 6. Verification

- Local: `py -3.12 -m venv .venv`, install deps, run ingest twice (assert row counts unchanged →
  idempotent), `dbt build` (all tests pass), `streamlit run app.py`.
- CI: `workflow_dispatch` dry run; confirm `data` branch updates and tests gate the commit.

## Open Risks

- **Python 3.12 not installed** — first implementation step installs it (python.org / winget).
  Fallback: attempt 3.14 in the venv if 3.12 install is blocked.
- **`data` branch vs Streamlit Cloud** — Streamlit Community Cloud deploys from the default branch.
  App supports reading marts from a local path or a remote Parquet URL so it works locally and when
  pointed at the `data` branch.
- **Dependency pins** — lock `dbt-duckdb` / `dbt-core` to known-good 3.12 versions at install time
  (latest 1.11.0 / 1.12.x verified to have pure-Python deps).
