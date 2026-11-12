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


with DAG(
    dag_id="flight_price_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    ingest = PythonOperator(
        task_id="ingest_csv_to_mysql",
        python_callable=ingest_csv_to_mysql,
    )   