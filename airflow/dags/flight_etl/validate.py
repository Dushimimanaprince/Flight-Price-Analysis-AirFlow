# Task 2: validate and clean the staged data.
# Reads stg_flights, drops unusable rows, flags total fare mismatches,
# and writes the result to validated_flights. The staging table is never changed.

import logging
from typing import Tuple

import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook

from flight_etl.config import (
    FARE_COLS,
    MYSQL_CHUNKSIZE,
    MYSQL_CONN_ID,
    REQUIRED_COLUMNS,
    STAGING_TABLE,
    TEXT_COLS,
    VALIDATED_TABLE,
)
from flight_etl.db import write_dataframe
from flight_etl.schema import ADD_VALIDATED_FLAG_SQL_TEMPLATE

# Allowed gap (in BDT) between total_fare and base_fare + tax_surcharge.
# MySQL rounding alone can cause up to 0.015, so 0.05 leaves a safe margin.
TOTAL_FARE_TOLERANCE = 0.05


logger = logging.getLogger(__name__)


def clean_flights(df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
    # Takes the raw rows, returns (good_rows, report).
    # No database code here, so it is easy to test with a small DataFrame.
    report = {"rows_in": len(df)}

    # 1. required columns must exist
    missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # 2. fix types and clean text
    df = df.copy()
    for c in FARE_COLS + ["duration_hrs"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")
    for c in TEXT_COLS:
        df[c] = df[c].str.strip()
    df["source"] = df["source"].str.upper()
    df["destination"] = df["destination"].str.upper()

    # 3. find bad rows (a row failing several checks is counted under each)
    missing_mask = (
        df[TEXT_COLS + FARE_COLS].isna().any(axis=1) | (df[TEXT_COLS] == "").any(axis=1)
    )
    negative_mask = (df[FARE_COLS] < 0).any(axis=1)
    city_mask = (
        ~df["source"].str.fullmatch(r"[A-Z]{3}", na=False)
        | ~df["destination"].str.fullmatch(r"[A-Z]{3}", na=False)
        | (df["source"] == df["destination"])
    )
    bad_mask = missing_mask | negative_mask | city_mask
    report["dropped_missing"] = int(missing_mask.sum())
    report["dropped_negative_fare"] = int(negative_mask.sum())
    report["dropped_invalid_city"] = int(city_mask.sum())

    good = df[~bad_mask].copy()

    # 4. flag (do not overwrite) totals that differ from base fare + tax
    # a gap can be a real extra charge, so we keep the original value
    expected = good["base_fare"] + good["tax_surcharge"]
    mismatch = (good["total_fare"] - expected).abs() > TOTAL_FARE_TOLERANCE
    good["total_fare_mismatch"] = mismatch.astype("int64")
    report["total_fare_flagged"] = int(mismatch.sum())

    return good, report


def validate_data() -> dict:
    # Airflow task: clean stg_flights into validated_flights.
    # The report is returned so Airflow stores it in XCom.
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    df = hook.get_df(f"SELECT * FROM {STAGING_TABLE}")

    good, report = clean_flights(df)

    # drop and recreate so re-running always gives the same result
    hook.run(f"DROP TABLE IF EXISTS {VALIDATED_TABLE}")
    hook.run(f"CREATE TABLE {VALIDATED_TABLE} LIKE {STAGING_TABLE}")
    hook.run(ADD_VALIDATED_FLAG_SQL_TEMPLATE.format(table=VALIDATED_TABLE))
    report["rows_out"] = write_dataframe(
        good, VALIDATED_TABLE, hook.get_sqlalchemy_engine(), MYSQL_CHUNKSIZE
    )

    logger.info("Validation report: %s", report)
    return report