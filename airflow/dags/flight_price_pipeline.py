"""Airflow DAG: flight price analysis for Bangladesh.

This file only describes the workflow: which tasks exist and in what order
they run. The work done by each task lives in the ``flight_etl`` package.

Execution flow::

    ingest_csv_to_mysql -> validate_data -> transform_data
                        -> compute_kpis -> load_to_postgres

1. ingest_csv_to_mysql: load the raw CSV into the MySQL staging table.
2. validate_data:       drop unusable rows, correct fares, save to a new table.
3. transform_data:      add the route and season_type columns.
4. compute_kpis:        calculate the four KPI tables.
5. load_to_postgres:    copy the final tables to the PostgreSQL analytics DB.
"""

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from flight_etl.ingest import ingest_csv_to_mysql
from flight_etl.kpis import compute_kpis
from flight_etl.load import load_to_postgres
from flight_etl.transform import transform_data
from flight_etl.validate import validate_data

with DAG(
    dag_id="flight_price_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,      # run on demand (manual trigger)
    catchup=False,      # no backfill of past intervals
    max_active_runs=1,  # never let two runs overwrite the same tables at once
) as dag:

    ingest = PythonOperator(
        task_id="ingest_csv_to_mysql",
        python_callable=ingest_csv_to_mysql,
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    kpis = PythonOperator(
        task_id="compute_kpis",
        python_callable=compute_kpis,
    )

    load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    ingest >> validate >> transform >> kpis >> load