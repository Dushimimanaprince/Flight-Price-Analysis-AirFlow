"""Task 4: compute the key performance indicators (KPIs).

Four KPI tables are calculated from the enriched flights and stored in MySQL:

* average fare by airline
* seasonal fare variation (peak vs. non-peak)
* booking count by airline
* most popular routes
"""

import logging
from typing import Dict

import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook

from flight_etl.config import (
    ENRICHED_TABLE,
    MYSQL_CHUNKSIZE,
    MYSQL_CONN_ID,
    TOP_ROUTES_LIMIT,
)
from flight_etl.db import write_dataframe


logger = logging.getLogger(__name__)


def calculate_kpis(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Calculate every KPI table from the enriched flights.

    Each booking is one row, so a "booking count" is a row count. Kept free of
    database code so it can be unit tested with a small DataFrame.

    Args:
        df: Enriched flights with at least the columns ``airline``, ``route``,
            ``source``, ``destination``, ``seasonality``, ``season_type`` and
            ``total_fare``.

    Returns:
        A mapping of table name to KPI DataFrame. The names must match
        ``KPI_TABLES`` in the config module, which the load task relies on:

        * ``kpi_avg_fare_by_airline``: mean total fare per airline, highest
          first.
        * ``kpi_seasonal_fare_variation``: mean fare and booking count per
          season, plus ``pct_vs_non_peak``, the percentage difference from the
          average fare of all non-peak bookings.
        * ``kpi_booking_count_by_airline``: bookings per airline, most first.
        * ``kpi_popular_routes``: the ``TOP_ROUTES_LIMIT`` busiest routes,
          with a ``rank`` column starting at 1.
    """
    df = df.copy()
    df["total_fare"] = df["total_fare"].astype("float64")

    # KPI 1: average fare by airline
    avg_fare = (
        df.groupby("airline", as_index=False)["total_fare"].mean()
        .rename(columns={"total_fare": "avg_total_fare"})
    )
    avg_fare["avg_total_fare"] = avg_fare["avg_total_fare"].round(2)
    avg_fare = avg_fare.sort_values("avg_total_fare", ascending=False)

    # KPI 2: seasonal fare variation (peak vs non-peak)
    seasonal = df.groupby(["season_type", "seasonality"], as_index=False).agg(
        avg_total_fare=("total_fare", "mean"),
        booking_count=("total_fare", "size"),
    )
    baseline = df.loc[df["season_type"] == "Non-Peak", "total_fare"].mean()
    seasonal["pct_vs_non_peak"] = (
        (seasonal["avg_total_fare"] - baseline) / baseline * 100
    ).round(2)
    seasonal["avg_total_fare"] = seasonal["avg_total_fare"].round(2)

    # KPI 3: booking count by airline
    bookings = (
        df.groupby("airline", as_index=False).size()
        .rename(columns={"size": "booking_count"})
        .sort_values("booking_count", ascending=False)
    )

    # KPI 4: most popular routes
    routes = (
        df.groupby(["route", "source", "destination"], as_index=False).size()
        .rename(columns={"size": "booking_count"})
        .sort_values("booking_count", ascending=False)
        .head(TOP_ROUTES_LIMIT)
    )
    routes.insert(0, "rank", range(1, len(routes) + 1))

    return {
        "kpi_avg_fare_by_airline": avg_fare,
        "kpi_seasonal_fare_variation": seasonal,
        "kpi_booking_count_by_airline": bookings,
        "kpi_popular_routes": routes,
    }


def compute_kpis() -> dict:
    """Airflow task: calculate the KPIs and store them in MySQL.

    Each KPI table is dropped and recreated on every run.

    Returns:
        Mapping of KPI table name to the number of rows saved (pushed to
        XCom).

    Raises:
        ValueError: If a KPI table's saved row count is wrong.
    """
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    df = hook.get_df(
        "SELECT airline, route, source, destination, seasonality, "
        f"season_type, total_fare FROM {ENRICHED_TABLE}"
    )

    engine = hook.get_sqlalchemy_engine()
    report = {
        name: write_dataframe(
            frame, name, engine, MYSQL_CHUNKSIZE, if_exists="replace"
        )
        for name, frame in calculate_kpis(df).items()
    }

    logger.info("KPI report: %s", report)
    return report