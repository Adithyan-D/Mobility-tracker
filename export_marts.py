"""Export dbt mart tables from warehouse.duckdb to Parquet for the ``data`` branch.

The DuckDB warehouse is a build artifact and is not committed; the compact Parquet
extracts here are what the Streamlit app (and the git history) consume.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent
WAREHOUSE = ROOT / "warehouse.duckdb"
MARTS_DIR = ROOT / "data" / "marts"
MARTS = ["dim_stations", "fct_station_snapshots", "agg_hourly_metrics"]


def main() -> int:
    if not WAREHOUSE.exists():
        raise FileNotFoundError(f"warehouse not found: {WAREHOUSE}. Run ingest + dbt build first.")

    MARTS_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    try:
        for name in MARTS:
            out = MARTS_DIR / f"{name}.parquet"
            con.execute(
                f"COPY (SELECT * FROM {name}) TO '{out.as_posix()}' (FORMAT PARQUET)"
            )
            print(f"exported {name} -> {out.relative_to(ROOT)}")
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
