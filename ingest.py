"""Idempotent multi-city GBFS ingestion into Parquet partitions + a local DuckDB warehouse.

Pulls GBFS ``station_information`` and ``station_status`` feeds for multiple metropolitan
networks (NYC Citi Bike, Chicago Divvy, Washington D.C. Capital Bikeshare, SF Bay Wheels).
Isolates network fetches per city so a failure in one feed does not halt ingestion for others.

Idempotency
-----------
Every feed publication carries a root-level ``last_updated`` epoch which acts as a
``batch_id``. Each batch is written to its own Hive-partitioned Parquet directory:
``data/raw/<feed>/city=<city>/batch_id=<n>/data.parquet``.
Re-running ingestion during the same publication window cleanly overwrites that partition,
preventing duplicates. Raw records are also loaded into ``warehouse.duckdb`` using
``INSERT OR IGNORE`` against a ``(city, station_id, _batch_id)`` primary key.
"""

from __future__ import annotations

import json
import logging
import os
import random
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("ingest")

CITIES: dict[str, dict[str, Any]] = {
    "nyc": {
        "name": "New York City (Citi Bike)",
        "timezone": "America/New_York",
        "feeds": {
            "station_information": "https://gbfs.citibikenyc.com/gbfs/en/station_information.json",
            "station_status": "https://gbfs.citibikenyc.com/gbfs/en/station_status.json",
        },
    },
    "chicago": {
        "name": "Chicago (Divvy)",
        "timezone": "America/Chicago",
        "feeds": {
            "station_information": "https://gbfs.divvybikes.com/gbfs/en/station_information.json",
            "station_status": "https://gbfs.divvybikes.com/gbfs/en/station_status.json",
        },
    },
    "dc": {
        "name": "Washington, D.C. (Capital Bikeshare)",
        "timezone": "America/New_York",
        "feeds": {
            "station_information": "https://gbfs.capitalbikeshare.com/gbfs/en/station_information.json",
            "station_status": "https://gbfs.capitalbikeshare.com/gbfs/en/station_status.json",
        },
    },
    "sf": {
        "name": "San Francisco Bay Area (Bay Wheels)",
        "timezone": "America/Los_Angeles",
        "feeds": {
            "station_information": "https://gbfs.baywheels.com/gbfs/en/station_information.json",
            "station_status": "https://gbfs.baywheels.com/gbfs/en/station_status.json",
        },
    },
}

FEED_NAMES = ["station_information", "station_status"]

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
MARTS_DIR = DATA_DIR / "marts"
WAREHOUSE = ROOT / "warehouse.duckdb"

RAW_RETENTION_DAYS = int(os.getenv("RAW_RETENTION_DAYS", "14"))
MAX_ATTEMPTS = int(os.getenv("INGEST_MAX_ATTEMPTS", "5"))
BASE_DELAY = float(os.getenv("INGEST_BASE_DELAY", "1.0"))
MAX_DELAY = float(os.getenv("INGEST_MAX_DELAY", "16.0"))

RAW_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    station_id VARCHAR NOT NULL,
    city       VARCHAR NOT NULL,
    payload    JSON    NOT NULL,
    _batch_id  BIGINT  NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL,
    _source    VARCHAR NOT NULL,
    PRIMARY KEY (city, station_id, _batch_id)
)
"""


def request_json(client: httpx.Client, url: str) -> dict[str, Any]:
    """GET ``url`` and decode JSON, retrying transient failures with backoff."""
    last_exc: Exception | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            resp = client.get(url)
            if resp.status_code >= 500:
                resp.raise_for_status()
            resp.raise_for_status()
            return resp.json()
        except (httpx.HTTPError, json.JSONDecodeError) as exc:
            last_exc = exc
            if attempt == MAX_ATTEMPTS:
                break
            delay = min(MAX_DELAY, BASE_DELAY * 2 ** (attempt - 1))
            delay += random.uniform(0, delay * 0.25)
            log.warning(
                "attempt %d/%d failed for %s: %s -- retrying in %.1fs",
                attempt, MAX_ATTEMPTS, url, exc, delay,
            )
            time.sleep(delay)
    raise RuntimeError(f"giving up on {url}: {last_exc}")


def write_raw_partition(
    con: duckdb.DuckDBPyConnection,
    feed: str,
    city: str,
    batch_id: int,
    stations: list[dict[str, Any]],
    loaded_at: datetime,
) -> Path:
    """Write one Parquet partition for a batch under city Hive-partitioning."""
    batch_dir = RAW_DIR / feed / f"city={city}" / f"batch_id={batch_id}"
    if batch_dir.exists():
        shutil.rmtree(batch_dir)
    batch_dir.mkdir(parents=True, exist_ok=True)
    parquet_file = batch_dir / "data.parquet"

    con.execute(
        "CREATE OR REPLACE TEMP TABLE _batch("
        "station_id VARCHAR, city VARCHAR, payload VARCHAR, _loaded_at TIMESTAMPTZ)"
    )
    rows = [
        (str(st.get("station_id")), city, json.dumps(st), loaded_at)
        for st in stations
        if st.get("station_id") is not None
    ]
    con.executemany("INSERT INTO _batch VALUES (?, ?, ?, ?)", rows)
    con.execute(f"COPY _batch TO '{parquet_file.as_posix()}' (FORMAT PARQUET)")

    log.info("[%s] wrote %d %s records -> %s", city.upper(), len(rows), feed, parquet_file.relative_to(ROOT))
    return batch_dir


def load_raw_table(
    con: duckdb.DuckDBPyConnection,
    feed: str,
    city: str,
    batch_id: int,
    stations: list[dict[str, Any]],
    loaded_at: datetime,
) -> int:
    """Idempotently load a batch into the DuckDB raw table."""
    table = f"raw_{feed}"
    con.execute(RAW_TABLE_DDL.format(table=table))
    cols = [r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()]
    if "city" not in cols:
        log.info("Migrating legacy table %s to include 'city' column...", table)
        con.execute(f"DROP TABLE {table}")
        con.execute(RAW_TABLE_DDL.format(table=table))

    con.execute(
        "CREATE OR REPLACE TEMP TABLE _batch_table("
        "station_id VARCHAR, city VARCHAR, payload JSON, _loaded_at TIMESTAMPTZ)"
    )
    rows = [
        (str(st.get("station_id")), city, json.dumps(st), loaded_at)
        for st in stations
        if st.get("station_id") is not None
    ]
    con.executemany("INSERT INTO _batch_table VALUES (?, ?, ?, ?)", rows)
    before = con.execute(f"SELECT count(*) FROM {table} WHERE city = ?", [city]).fetchone()[0]
    con.execute(
        f"INSERT OR IGNORE INTO {table} "
        f"SELECT station_id, city, payload, ?, _loaded_at, ? FROM _batch_table",
        [batch_id, feed],
    )
    after = con.execute(f"SELECT count(*) FROM {table} WHERE city = ?", [city]).fetchone()[0]
    inserted = after - before
    log.info("[%s] loaded %d new rows into %s (%d duplicate/skipped)", city.upper(), inserted, table, len(rows) - inserted)
    return inserted


def prune_retention(con: duckdb.DuckDBPyConnection, feeds: list[str]) -> None:
    """Drop raw partitions and rows older than the retention window."""
    if RAW_RETENTION_DAYS <= 0:
        return
    cutoff_epoch = int(time.time()) - RAW_RETENTION_DAYS * 86_400

    for feed in feeds:
        feed_dir = RAW_DIR / feed
        if feed_dir.exists():
            for part in feed_dir.glob("city=*/batch_id=*"):
                try:
                    batch_id = int(part.name.split("=", 1)[1])
                except (IndexError, ValueError):
                    continue
                if batch_id < cutoff_epoch:
                    shutil.rmtree(part, ignore_errors=True)
                    log.info("pruned expired partition %s", part.relative_to(ROOT))

        table = f"raw_{feed}"
        exists = con.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema='main' AND table_name=?",
            [table],
        ).fetchone()[0]
        if exists:
            con.execute(f"DELETE FROM {table} WHERE _batch_id < ?", [cutoff_epoch])


def main() -> int:
    loaded_at = datetime.now(timezone.utc)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    MARTS_DIR.mkdir(parents=True, exist_ok=True)

    success_count = 0
    failure_count = 0

    con = duckdb.connect(str(WAREHOUSE))
    try:
        with httpx.Client(
            timeout=httpx.Timeout(30.0),
            follow_redirects=True,
            headers={"User-Agent": "gbfs-mobility-pipeline/2.0"},
        ) as client:
            for city, config in CITIES.items():
                city_name = config["name"]
                feeds = config["feeds"]
                log.info("Starting ingestion for %s (%s)...", city_name, city.upper())
                try:
                    for feed, url in feeds.items():
                        payload = request_json(client, url)
                        data = payload.get("data") or {}
                        stations = data.get("stations") or []
                        batch_id = int(payload.get("last_updated") or time.time())
                        if not stations:
                            raise RuntimeError(f"[{city}] {feed} returned no stations")

                        log.info(
                            "[%s] fetched %s: batch_id=%s stations=%d ttl=%s",
                            city.upper(), feed, batch_id, len(stations), payload.get("ttl"),
                        )
                        write_raw_partition(con, feed, city, batch_id, stations, loaded_at)
                        load_raw_table(con, feed, city, batch_id, stations, loaded_at)

                    success_count += 1
                except Exception as exc:
                    failure_count += 1
                    log.error("[%s] Ingestion failed: %s (isolated from other cities)", city.upper(), exc)

        prune_retention(con, FEED_NAMES)
    finally:
        con.close()

    log.info(
        "Ingestion completed: %d succeeded, %d failed out of %d configured cities",
        success_count, failure_count, len(CITIES),
    )
    if success_count == 0:
        log.error("All city ingestions failed.")
        return 1
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:  # noqa: BLE001
        log.exception("Fatal ingestion runner error")
        sys.exit(1)
