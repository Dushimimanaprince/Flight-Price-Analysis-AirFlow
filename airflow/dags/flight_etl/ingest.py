"""Task 1: load the raw CSV into the MySQL staging table.

The staging table is a faithful copy of the source file. Nothing is cleaned or
filtered here; that is the job of the validation task.
"""

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
    """Read the flight price CSV and rename its columns for the database.

    Kept separate from the database code so it can be tested without a
    running MySQL server.

    Args:
        path: Location of ``Flight_Price_Dataset_of_Bangladesh.csv``.

    Returns:
        DataFrame with snake_case column names (see ``COLUMN_MAP``) and the
        departure and arrival columns parsed as datetimes.
    """
    df = pd.read_csv(path)
    df = df.rename(columns=COLUMN_MAP)
    df["departure_datetime"] = pd.to_datetime(df["departure_datetime"])
    df["arrival_datetime"] = pd.to_datetime(df["arrival_datetime"])
    return df


def ingest_csv_to_mysql() -> int:
    """Airflow task: rebuild the staging table from the CSV file.

    The table is dropped and recreated on every run, so re-running the task
    never duplicates rows.

    Returns:
        Number of rows loaded into the staging table (pushed to XCom).

    Raises:
        ValueError: If the row count in MySQL differs from the CSV.
    """
    df = read_flight_csv(CSV_PATH)

    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    hook.run(f"DROP TABLE IF EXISTS {STAGING_TABLE}")
    hook.run(CREATE_STAGING_SQL)

    return write_dataframe(
        df, STAGING_TABLE, hook.get_sqlalchemy_engine(), MYSQL_CHUNKSIZE
    )