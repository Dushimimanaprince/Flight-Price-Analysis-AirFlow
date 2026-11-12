import os
from datetime import datetime

import pandas as pd
from airflow.sdk import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.standard.operators.python import PythonOperator

CSV_PATH = os.environ.get(
    "FLIGHT_CSV_PATH",
    os.path.expanduser("~/flight-pipeline/data/Flight_Price_Dataset_of_Bangladesh.csv"),
)
MYSQL_CONN_ID = "mysql_staging"
STAGING_TABLE = "stg_flights"

COLUMN_MAP = {
    "Airline": "airline",
    "Source": "source",
    "Source Name": "source_name",
    "Destination": "destination",
    "Destination Name": "destination_name",
    "Departure Date & Time": "departure_datetime",
    "Arrival Date & Time": "arrival_datetime",
    "Duration (hrs)": "duration_hrs",
    "Stopovers": "stopovers",
    "Aircraft Type": "aircraft_type",
    "Class": "travel_class",
    "Booking Source": "booking_source",
    "Base Fare (BDT)": "base_fare",
    "Tax & Surcharge (BDT)": "tax_surcharge",
    "Total Fare (BDT)": "total_fare",
    "Seasonality": "seasonality",
    "Days Before Departure": "days_before_departure",
}

CREATE_TABLE_SQL = f"""
CREATE TABLE {STAGING_TABLE} (
    id INT AUTO_INCREMENT PRIMARY KEY,
    airline VARCHAR(100),
    source VARCHAR(10),
    source_name VARCHAR(150),
    destination VARCHAR(10),
    destination_name VARCHAR(150),
    departure_datetime DATETIME,
    arrival_datetime DATETIME,
    duration_hrs DECIMAL(6,2),
    stopovers VARCHAR(30),
    aircraft_type VARCHAR(50),
    travel_class VARCHAR(30),
    booking_source VARCHAR(50),
    base_fare DECIMAL(12,2),
    tax_surcharge DECIMAL(12,2),
    total_fare DECIMAL(12,2),
    seasonality VARCHAR(30),
    days_before_departure INT
)
"""
VALIDATED_TABLE = "validated_flights"
REQUIRED_COLUMNS = ["airline", "source", "destination", "base_fare", "tax_surcharge", "total_fare"]
TEXT_COLS = ["airline", "source", "destination"]
FARE_COLS = ["base_fare", "tax_surcharge", "total_fare"]


def ingest_csv_to_mysql():
    df = pd.read_csv(CSV_PATH)
    df = df.rename(columns=COLUMN_MAP)
    df["departure_datetime"] = pd.to_datetime(df["departure_datetime"])
    df["arrival_datetime"] = pd.to_datetime(df["arrival_datetime"])

    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    hook.run(f"DROP TABLE IF EXISTS {STAGING_TABLE}")
    hook.run(CREATE_TABLE_SQL)

    engine = hook.get_sqlalchemy_engine()
    df.to_sql(
        STAGING_TABLE, engine, if_exists="append",
        index=False, chunksize=2000, method="multi",
    )

    loaded = hook.get_first(f"SELECT COUNT(*) FROM {STAGING_TABLE}")[0]
    if loaded != len(df):
        raise ValueError(f"Row mismatch: CSV has {len(df)}, MySQL has {loaded}")
    return loaded


def validate_data():
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    df = hook.get_df(f"SELECT * FROM {STAGING_TABLE}")
    report = {"rows_in": len(df)}

    # 1. required columns must exist
    missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # 2. fix types and clean text
    for c in FARE_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")
    df["duration_hrs"] = pd.to_numeric(df["duration_hrs"], errors="coerce").astype("float64")
    for c in TEXT_COLS:
        df[c] = df[c].str.strip()
    df["source"] = df["source"].str.upper()
    df["destination"] = df["destination"].str.upper()

    # 3. find bad rows
    missing_mask = df[TEXT_COLS + FARE_COLS].isna().any(axis=1) | (df[TEXT_COLS] == "").any(axis=1)
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
    mismatch = (good["total_fare"] - expected).abs() > 0.01
    report["total_fare_corrected"] = int(mismatch.sum())
    good.loc[mismatch, "total_fare"] = expected[mismatch]

    # 5. write to a separate table, keep staging untouched
    hook.run(f"DROP TABLE IF EXISTS {VALIDATED_TABLE}")
    hook.run(f"CREATE TABLE {VALIDATED_TABLE} LIKE {STAGING_TABLE}")
    engine = hook.get_sqlalchemy_engine()
    good.to_sql(
        VALIDATED_TABLE, engine, if_exists="append",
        index=False, chunksize=2000, method="multi",
    )

    saved = hook.get_first(f"SELECT COUNT(*) FROM {VALIDATED_TABLE}")[0]
    if saved != len(good):
        raise ValueError(f"Row mismatch: expected {len(good)}, saved {saved}")
    report["rows_out"] = int(saved)
    print(report)
    return report

with DAG(
    dag_id="flight_price_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    ingest = PythonOperator(
        task_id="ingest_csv_to_mysql",
        python_callable=ingest_csv_to_mysql,
    )   
    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    ingest >> validate