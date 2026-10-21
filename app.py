"""Streamlit dashboard for the Multi-City GBFS mobility pipeline.

Reads the dbt mart tables from ``warehouse.duckdb`` when present (local dev) or
from the committed Parquet extracts under ``data/marts`` (CI / deployment).
Provides cross-city comparative analytics and granular per-city operational views.
"""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import plotly.express as px
import pydeck as pdk
import streamlit as st

ROOT = Path(__file__).resolve().parent
WAREHOUSE = ROOT / "warehouse.duckdb"
MARTS_DIR = ROOT / "data" / "marts"
DATA_SOURCE = os.getenv("DATA_SOURCE", "auto")

CITY_CONFIG: dict[str, dict[str, Any]] = {
    "all": {
        "label": "🌐 All Cities (Comparative Overview)",
        "lat": 39.5,
        "lon": -98.35,
        "zoom": 3.8,
    },
    "nyc": {
        "label": "🍎 New York City (Citi Bike)",
        "lat": 40.75,
        "lon": -73.97,
        "zoom": 10.5,
    },
    "chicago": {
        "label": "🏙️ Chicago (Divvy)",
        "lat": 41.88,
        "lon": -87.63,
        "zoom": 10.5,
    },
    "dc": {
        "label": "🏛️ Washington, D.C. (Capital Bikeshare)",
        "lat": 38.90,
        "lon": -77.03,
        "zoom": 11.0,
    },
    "sf": {
        "label": "🌉 San Francisco Bay Area (Bay Wheels)",
        "lat": 37.65,
        "lon": -122.25,
        "zoom": 9.5,  # Encompasses SF, Oakland, and San Jose
    },
}

st.set_page_config(
    page_title="Global Bikeshare Mobility Observatory",
    page_icon="🚲",
    layout="wide",
)


@st.cache_data(ttl=300)
def load_table(name: str) -> pd.DataFrame:
    """Load a mart table from DuckDB (preferred) or the Parquet extract."""
    use_duckdb = DATA_SOURCE == "duckdb" or (DATA_SOURCE == "auto" and WAREHOUSE.exists())
    if use_duckdb:
        con = duckdb.connect(str(WAREHOUSE), read_only=True)
        try:
            return con.execute(f"SELECT * FROM {name}").df()
        finally:
            con.close()

    parquet = MARTS_DIR / f"{name}.parquet"
    if not parquet.exists():
        raise FileNotFoundError(
            f"No data source found for '{name}'. Run `python ingest.py`, `dbt build`, "
            f"and `python export_marts.py`, or check out the data branch under data/."
        )
    return pd.read_parquet(parquet)


def availability_color(ratio: float) -> list[int]:
    """Red -> amber -> green scale for a 0..1 availability ratio."""
    if ratio is None or pd.isna(ratio):
        return [120, 120, 120]
    ratio = max(0.0, min(1.0, float(ratio)))
    if ratio < 0.5:
        r, g = 220, int(220 * (ratio / 0.5))
    else:
        r, g = int(220 * ((1.0 - ratio) / 0.5)), 200
    return [r, g, 40, 180]


@st.cache_data(ttl=300)
def latest_snapshot(fct: pd.DataFrame) -> pd.DataFrame:
    key_col = "global_station_id" if "global_station_id" in fct.columns else "station_id"
    sort_cols = [key_col, "snapshot_at"]
    if "ingested_at" in fct.columns:
        sort_cols.append("ingested_at")
    return (
        fct.sort_values(sort_cols)
        .groupby(key_col, as_index=False)
        .tail(1)
    )


def main() -> None:
    st.title("🚲 Global Real-time Bikeshare Mobility Observatory")
    st.caption("Multi-city GBFS data pipeline — automated ingestion → dbt dimensional models → comparative analytics")

    try:
        dim = load_table("dim_stations")
        fct = load_table("fct_station_snapshots")
        agg = load_table("agg_hourly_metrics")
    except FileNotFoundError as exc:
        st.error(str(exc))
        st.stop()

    dim["latitude"] = pd.to_numeric(dim["latitude"], errors="coerce")
    dim["longitude"] = pd.to_numeric(dim["longitude"], errors="coerce")
    dim["capacity"] = pd.to_numeric(dim["capacity"], errors="coerce")

    # City filter selector
    city_options = list(CITY_CONFIG.keys())
    selected_city = st.selectbox(
        "Select Scope / Metropolitan Network",
        options=city_options,
        format_func=lambda c: CITY_CONFIG[c]["label"],
        index=0,
    )

    if selected_city != "all":
        dim_filtered = dim[dim["city"] == selected_city].copy()
        fct_filtered = fct[fct["city"] == selected_city].copy()
        agg_filtered = agg[agg["city"] == selected_city].copy()
    else:
        dim_filtered = dim.copy()
        fct_filtered = fct.copy()
        agg_filtered = agg.copy()

    snapshots = latest_snapshot(fct_filtered)
    active = snapshots[snapshots["is_installed"]].copy()

    # KPI Metrics
    key_col = "global_station_id" if "global_station_id" in dim_filtered.columns else "station_id"
    total_stations = int(dim_filtered[key_col].nunique())
    total_bikes = int(active["num_bikes_available"].sum())
    total_capacity = int(active["capacity"].fillna(0).sum())
    system_rate = (total_bikes / total_capacity) if total_capacity else 0.0
    starved_now = int(active["is_starved"].sum())
    overflow_now = int(active["is_overflow"].sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Monitored Stations", f"{total_stations:,}")
    c2.metric("Fleet Availability", f"{system_rate:.1%}")
    c3.metric("Active Starvation Alerts", f"{starved_now:,}", help="Stations with 0 available bikes")
    c4.metric("Active Overflow Alerts", f"{overflow_now:,}", help="Stations with 0 available docks")

    st.divider()

    # If All Cities: show cross-city comparison table and benchmark charts
    if selected_city == "all":
        st.subheader("Cross-City Operational Benchmarks")
        city_groups = []
        for c in ["nyc", "chicago", "dc", "sf"]:
            c_dim = dim[dim["city"] == c]
            c_act = active[active["city"] == c]
            c_bikes = int(c_act["num_bikes_available"].sum())
            c_cap = int(c_act["capacity"].fillna(0).sum())
            c_rate = (c_bikes / c_cap) if c_cap else 0.0
            c_starved = int(c_act["is_starved"].sum())
            c_overflow = int(c_act["is_overflow"].sum())
            starve_rate = (c_starved / len(c_act)) if len(c_act) else 0.0
            city_groups.append({
                "City": CITY_CONFIG[c]["label"],
                "Total Stations": len(c_dim),
                "Available Bikes": c_bikes,
                "Total Capacity": c_cap,
                "System Availability": f"{c_rate:.1%}",
                "Starvation Events": c_starved,
                "Starvation %": f"{starve_rate:.1%}",
                "Overflow Alerts": c_overflow,
            })
        st.dataframe(pd.DataFrame(city_groups), use_container_width=True, hide_index=True)
        st.divider()

    # Visual Layout: Map (Left) and Time Series / Local Commute (Right)
    left, right = st.columns([3, 2])

    with left:
        st.subheader(f"Station Availability Map — {CITY_CONFIG[selected_city]['label']}")
        map_df = active.dropna(subset=["latitude", "longitude"]).copy()
        map_df["availability"] = (
            map_df["num_bikes_available"] / map_df["capacity"].replace(0, pd.NA)
        )
        map_df["color"] = map_df["availability"].apply(availability_color)
        map_df["radius"] = map_df["capacity"].fillna(5).clip(lower=3) * (
            1.2 if selected_city != "all" else 4.0
        )

        layer = pdk.Layer(
            "ScatterplotLayer",
            map_df,
            get_position="[longitude, latitude]",
            get_fill_color="color",
            get_radius="radius",
            pickable=True,
            opacity=0.8,
            stroked=False,
        )
        cfg = CITY_CONFIG[selected_city]
        view = pdk.ViewState(
            latitude=cfg["lat"],
            longitude=cfg["lon"],
            zoom=cfg["zoom"],
            pitch=0,
        )
        st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view))

    with right:
        if selected_city == "all" and "local_hour" in agg.columns:
            st.subheader("Commute Alignment (Local Hour Comparison)")
            st.caption("Normalized by local timezones (ET, CT, PT) to compare rush hour patterns.")
            local_curve = (
                agg.groupby(["local_hour", "city"], as_index=False)["starvation_events"]
                .sum()
                .sort_values("local_hour")
            )
            fig = px.line(
                local_curve,
                x="local_hour",
                y="starvation_events",
                color="city",
                markers=True,
                labels={"local_hour": "Local Hour (0–23)", "starvation_events": "Starved Snapshots", "city": "City"},
            )
            fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=420)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.subheader("Starvation Spikes (Last 24h)")
            if not agg_filtered.empty:
                agg_filtered["metric_hour"] = pd.to_datetime(agg_filtered["metric_hour"], utc=True)
                cutoff = pd.Timestamp.now(tz="UTC") - timedelta(hours=24)
                recent = agg_filtered[agg_filtered["metric_hour"] >= cutoff]
                if recent.empty:
                    recent = agg_filtered
                series = (
                    recent.groupby("metric_hour", as_index=False)["starvation_events"]
                    .sum()
                    .sort_values("metric_hour")
                )
                fig = px.area(
                    series,
                    x="metric_hour",
                    y="starvation_events",
                    labels={"metric_hour": "Hour (UTC)", "starvation_events": "Starved snapshots"},
                )
                fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=420)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No hourly metrics available yet.")

    st.divider()
    st.subheader(f"Most Starved Stations ({CITY_CONFIG[selected_city]['label']})")
    cols_to_show = ["station_name", "city", "num_bikes_available", "capacity", "snapshot_at"]
    if "local_snapshot_at" in active.columns:
        cols_to_show.append("local_snapshot_at")

    cols_present = [c for c in cols_to_show if c in active.columns]
    worst = (
        active[active["is_starved"]]
        .sort_values("snapshot_at", ascending=False)
        .loc[:, cols_present]
        .head(20)
    )
    if worst.empty:
        st.success("No stations currently starved. 🎉")
    else:
        st.dataframe(worst, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
