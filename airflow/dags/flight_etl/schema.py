"""SQL table definitions (DDL) for the MySQL staging database.

Only the staging table is declared from scratch. The validated and enriched
tables are cloned from it with ``CREATE TABLE ... LIKE`` in their own tasks,
so they always inherit the same column types and can never drift apart.
"""

from flight_etl.config import STAGING_TABLE

# Staging table: one column per CSV column, typed to match the source data.
# DECIMAL is used for money so no floating-point error is stored; ``id`` is a
# surrogate key added by MySQL because the CSV has no unique identifier.
CREATE_STAGING_SQL = f"""
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

ADD_VALIDATED_FLAG_SQL_TEMPLATE = (
    "ALTER TABLE {table} "
    "ADD COLUMN total_fare_mismatch TINYINT(1) NOT NULL DEFAULT 0"
)

ADD_ENRICHED_COLUMNS_SQL_TEMPLATE = (
    "ALTER TABLE {table} "
    "ADD COLUMN route VARCHAR(25), ADD COLUMN season_type VARCHAR(15)"
)