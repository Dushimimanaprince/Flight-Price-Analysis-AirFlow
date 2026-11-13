"""Central configuration for the flight price pipeline.

Every value that more than one task needs (table names, column names,
connection IDs, file paths, chunk sizes) is defined here, so a change is made
in exactly one place.

This module contains constants only. It has no Airflow imports and no logic,
so it can be imported anywhere (including unit tests) without side effects.
"""

import os

from sqlalchemy.types import Numeric

# ---------------------------------------------------------------------------
# Source file
# ---------------------------------------------------------------------------

# Path to the raw Kaggle CSV. Override with the FLIGHT_CSV_PATH environment
# variable when the project lives somewhere other than ~/flight-pipeline.
CSV_PATH = os.environ.get(
    "FLIGHT_CSV_PATH",
    os.path.expanduser("~/flight-pipeline/data/Flight_Price_Dataset_of_Bangladesh.csv"),
)

# ---------------------------------------------------------------------------
# Airflow connection IDs (created in the Airflow UI / CLI, never hardcoded)
# ---------------------------------------------------------------------------

MYSQL_CONN_ID = "mysql_staging"
POSTGRES_CONN_ID = "postgres_analytics"

# ---------------------------------------------------------------------------
# MySQL table names, one per pipeline stage
# ---------------------------------------------------------------------------

STAGING_TABLE = "stg_flights"        # raw copy of the CSV
VALIDATED_TABLE = "validated_flights"  # rows that passed validation
ENRICHED_TABLE = "enriched_flights"    # validated rows + route and season_type

# KPI tables produced by the compute_kpis task.
KPI_TABLES = [
    "kpi_avg_fare_by_airline",
    "kpi_seasonal_fare_variation",
    "kpi_booking_count_by_airline",
    "kpi_popular_routes",
]

# Every table copied from MySQL to PostgreSQL by the load task.
TABLES_TO_LOAD = [ENRICHED_TABLE, *KPI_TABLES]

# ---------------------------------------------------------------------------
# Column handling
# ---------------------------------------------------------------------------

# CSV header -> snake_case column name used in every database table.
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

# Columns the project brief says must exist (database names).
REQUIRED_COLUMNS = [
    "airline", "source", "destination",
    "base_fare", "tax_surcharge", "total_fare",
]
TEXT_COLS = ["airline", "source", "destination"]  # must be non-empty strings
FARE_COLS = ["base_fare", "tax_surcharge", "total_fare"]  # must be numeric, >= 0

# ---------------------------------------------------------------------------
# Business rules
# ---------------------------------------------------------------------------

# Values of the `seasonality` column that count as peak season.
PEAK_SEASONS = ["Eid", "Hajj", "Winter Holidays"]

# How many routes the "most popular routes" KPI keeps.
TOP_ROUTES_LIMIT = 20

# ---------------------------------------------------------------------------
# Loading behaviour
# ---------------------------------------------------------------------------

# Rows per INSERT when writing to MySQL.
MYSQL_CHUNKSIZE = 2000

# Rows per INSERT when writing to PostgreSQL.
POSTGRES_CHUNKSIZE = 5000

# Explicit PostgreSQL types, so fares become NUMERIC instead of float.
NUMERIC_TYPES = {
    "base_fare": Numeric(12, 2),
    "tax_surcharge": Numeric(12, 2),
    "total_fare": Numeric(12, 2),
    "duration_hrs": Numeric(6, 2),
}