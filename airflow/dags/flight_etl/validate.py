"""Task 2: validate and clean the staged data.

Reads the raw staging table, drops rows that are unusable, corrects the total
fare where it disagrees with base fare + tax, and writes the result to a
separate table. The staging table is never modified, so a bad rule here can be
fixed and re-run without re-ingesting the CSV.
"""

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

# A stored total fare may differ from base + tax by up to this many BDT before
# it is treated as wrong.
TOTAL_FARE_TOLERANCE = 0.01


def clean_flights(df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
    """Validate a staged flights DataFrame and return only the good rows.

    Steps, in order:
        1. Check that every required column exists.
        2. Convert fares and duration to numbers, trim text, upper-case the
           airport codes.
        3. Drop rows with missing values, negative fares, or invalid airport
           codes (not three letters, or source equal to destination).
        4. Recompute ``total_fare`` as base fare + tax wherever the stored
           value is further than ``TOTAL_FARE_TOLERANCE`` away from it.

    Kept free of database code so it can be unit tested with a small DataFrame.

    Args:
        df: Raw rows from the staging table.

    Returns:
        A tuple ``(good_rows, report)``. ``report`` counts the rows read, the
        rows dropped for each reason, and the total fares corrected. A row
        that fails several checks is counted under each of them.

    Raises:
        ValueError: If a required column is missing.
    """
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

    # 3. find bad rows
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

    # 4. correct total fare where it does not equal base fare + tax
    expected = (good["base_fare"] + good["tax_surcharge"]).round(2)
    mismatch = (good["total_fare"] - expected).abs() > TOTAL_FARE_TOLERANCE
    report["total_fare_corrected"] = int(mismatch.sum())
    good.loc[mismatch, "total_fare"] = expected[mismatch]

    return good, report


def validate_data() -> dict:
    """Airflow task: clean the staging table into ``validated_flights``.

    The destination table is dropped and recreated on every run, with the same
    structure as the staging table.

    Returns:
        The validation report from ``clean_flights`` plus ``rows_out``, the
        number of rows saved (pushed to XCom).

    Raises:
        ValueError: If a required column is missing, or the saved row count
            differs from the number of good rows.
    """
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    df = hook.get_df(f"SELECT * FROM {STAGING_TABLE}")

    good, report = clean_flights(df)

    hook.run(f"DROP TABLE IF EXISTS {VALIDATED_TABLE}")
    hook.run(f"CREATE TABLE {VALIDATED_TABLE} LIKE {STAGING_TABLE}")
    report["rows_out"] = write_dataframe(
        good, VALIDATED_TABLE, hook.get_sqlalchemy_engine(), MYSQL_CHUNKSIZE
    )

    print(report)
    return report