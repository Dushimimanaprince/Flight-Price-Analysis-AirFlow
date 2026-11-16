# Task 1: load the raw CSV into the MySQL staging table.
# The staging table is a faithful copy of the file. Nothing is cleaned here;
# that is the job of the validation task.

import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook

from flight_etl.config import (
    COLUMN_MAP,
    CSV_PATH,
    MYSQL_CHUNKSIZE,
    MYSQL_CONN_ID,
    STAGING_TABLE,
)
from flight_etl.db import write_dataframe
from flight_etl.schema import CREATE_STAGING_SQL


def read_flight_csv(path: str) -> pd.DataFrame:
    # Reads the CSV and renames its columns for the database.
    # Kept separate from the database code so it can be tested without MySQL.
    df = pd.read_csv(path)

    # remove stray spaces around the header names
    df.columns = df.columns.str.strip()

    # stop early with a clear message if the file does not have what we expect
    missing = [c for c in COLUMN_MAP if c not in df.columns]
    if missing:
        raise ValueError(f"CSV is missing required columns: {missing}")

    # keep only the expected columns, in the expected order, with database names
    df = df[list(COLUMN_MAP)].rename(columns=COLUMN_MAP)
    df["departure_datetime"] = pd.to_datetime(df["departure_datetime"])
    df["arrival_datetime"] = pd.to_datetime(df["arrival_datetime"])
    return df


def ingest_csv_to_mysql() -> int:
    # Airflow task: rebuild stg_flights from the CSV file.
    # The table is dropped and recreated each run, so re-running never
    # duplicates rows. Returns the number of rows loaded (stored in XCom).
    df = read_flight_csv(CSV_PATH)

    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    hook.run(f"DROP TABLE IF EXISTS {STAGING_TABLE}")
    hook.run(CREATE_STAGING_SQL)

    return write_dataframe(
        df, STAGING_TABLE, hook.get_sqlalchemy_engine(), MYSQL_CHUNKSIZE
    )
