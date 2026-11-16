# Flight Price Analysis Pipeline: Project Report

## 1. Overview

This project is an end-to-end data pipeline, orchestrated with Apache Airflow, that processes the *Flight Price Dataset of Bangladesh* (Kaggle). The pipeline ingests the raw CSV file into a MySQL staging database, validates and cleans it, enriches it with the fields needed for analysis, computes four key performance indicators (KPIs), and loads the final tables into a PostgreSQL analytics database.

The source file contains 57,000 bookings and 17 columns: airline, source and destination airports (code and name), departure and arrival time, duration, number of stopovers, aircraft type, travel class, booking source, base fare, tax and surcharge, total fare (all in BDT), a seasonality label, and the number of days between booking and departure. It covers 24 airlines, 8 source airports and 152 distinct routes.

## 2. Architecture

### 2.1 Technologies

| Component | Role |
|---|---|
| Apache Airflow 3 | Workflow orchestration, scheduling, retries, run history |
| MySQL (`flight_staging`) | Staging database: raw, validated and enriched data, plus KPI tables |
| PostgreSQL (`flight_analytics`) | Analytics database: the final tables for analysis |
| PostgreSQL (`airflow_db`) | Airflow's own metadata database |
| Python, pandas, SQLAlchemy | Data processing and database access |

Airflow runs directly on Ubuntu-based Linux (Zorin OS) with `airflow standalone` and the LocalExecutor. Docker was not used: the project was first developed on WSL limited to 4 GB of memory, where the usual Docker-based setup did not fit, and the same direct installation was kept on the final machine. Using PostgreSQL for the Airflow metadata, instead of the default SQLite, is what allows tasks to run under the LocalExecutor.

### 2.2 Data flow

```
 CSV file ──► MySQL: stg_flights ──► validated_flights ──► enriched_flights
                    (raw copy)        (cleaned + flagged)   (+ route, season_type)
                                                                   │
                                                                   ▼
                                                  MySQL: four kpi_* tables
                                                                   │
                                                                   ▼
                                       PostgreSQL: enriched_flights + four kpi_* tables
```

Every stage writes to its own table instead of modifying the previous one. The staging table is therefore always an untouched copy of the CSV, and any later stage can be fixed and re-run without re-reading the file.

### 2.3 Tables

| Database | Table | Content |
|---|---|---|
| MySQL | `stg_flights` | The CSV exactly as received, with typed columns |
| MySQL | `validated_flights` | Rows that passed validation, plus the `total_fare_mismatch` flag |
| MySQL | `enriched_flights` | Validated rows plus `route` and `season_type` |
| MySQL, PostgreSQL | `kpi_avg_fare_by_airline`, `kpi_seasonal_fare_variation`, `kpi_booking_count_by_airline`, `kpi_popular_routes` | The four KPI tables |
| PostgreSQL | `enriched_flights` | Final copy of the enriched data |

## 3. Pipeline Execution Flow

The DAG is named `flight_price_pipeline` and has five tasks that run strictly one after the other:

```
ingest_csv_to_mysql ─► validate_data ─► transform_data ─► compute_kpis ─► load_to_postgres
```

### 3.1 DAG settings

The DAG has no schedule and is triggered manually (`schedule=None`), because the source is a static file. `catchup=False` prevents Airflow from creating runs for past dates. `max_active_runs=1` guarantees that two runs never rewrite the same tables at the same time. Every task is retried twice, one minute apart, and is stopped if it runs for more than 15 minutes. Retrying is safe because each task rebuilds its output tables from scratch, so a second attempt cannot create duplicate rows.

### 3.2 Tasks

**`ingest_csv_to_mysql`** reads the CSV, checks that every expected column is present in the header, renames the columns to `snake_case`, parses the two timestamp columns, and recreates the `stg_flights` table with explicit column types (for example `DECIMAL(12,2)` for fares, `DATETIME` for timestamps and `INT` for the days before departure). It then inserts the rows in batches of 2,000 and compares the table's row count with the number of rows read from the file. A mismatch fails the task.

**`validate_data`** reads `stg_flights` and applies the rules in section 4. It writes the surviving rows to `validated_flights`, leaving the staging table untouched, and returns a report with the number of rows read, dropped (by reason), flagged and saved.

**`transform_data`** reads `validated_flights`, converts the decimal columns to floating-point numbers, fills `total_fare` with base fare plus tax only where it is missing, and adds the `route` and `season_type` columns. It writes the result to `enriched_flights` and returns the number of rows, peak rows and non-peak rows.

**`compute_kpis`** reads the columns it needs from `enriched_flights`, computes the four KPI tables with pandas (section 5) and stores each of them in MySQL, verifying the row count of every table.

**`load_to_postgres`** copies `enriched_flights` and the four KPI tables from MySQL to PostgreSQL inside one database transaction (section 6).

Each task returns a small report dictionary, which Airflow stores in XCom and the task log, so a run can be audited without querying the databases.

## 4. Data Validation

The validation task implements the checks required by the project brief.

| Check | Rule | Action |
|---|---|---|
| Required columns | `airline`, `source`, `destination`, `base_fare`, `tax_surcharge` and `total_fare` exist | Fail the task |
| Missing values | None of the required text or fare fields is null, empty or only spaces | Drop the row |
| Data types | Fares and duration are numeric; non-numeric text becomes null and the row is dropped | Drop the row |
| Negative fares | No fare column is below zero | Drop the row |
| Airport codes | Source and destination are exactly three letters after trimming and upper-casing, and are not the same airport | Drop the row |
| Total fare consistency | `total_fare` equals `base_fare + tax_surcharge` within 0.05 BDT | Flag the row, keep the value |

Text fields are trimmed, and airport codes are converted to upper case before checking. A row that fails several checks is counted once under each of them.

On this dataset no row was dropped. The only finding is 2,522 rows (4.4%) whose total fare is higher than base fare plus tax, by 445 to 93,165 BDT (about 15,400 on average). Those rows are marked with `total_fare_mismatch = 1` and their total fare is kept unchanged; the reasoning is in section 8.

## 5. KPIs

### 5.1 Peak and non-peak seasons

The dataset's `seasonality` column has four values. *Eid*, *Hajj* and *Winter Holidays* are classified as **peak** seasons, and *Regular* as **non-peak**. This is stored in `enriched_flights.season_type`. The classification is defined in one place (`PEAK_SEASONS` in the configuration module), so it can be changed without touching the KPI code.

### 5.2 Definitions and computation

Each booking is one row, so a booking count is a row count. All fare KPIs use `total_fare`, the amount the passenger actually pays.

| KPI | Table | Definition and logic |
|---|---|---|
| Average fare by airline | `kpi_avg_fare_by_airline` | Mean `total_fare` per airline, rounded to 2 decimals, highest first. Columns: `airline`, `avg_total_fare`. |
| Seasonal fare variation | `kpi_seasonal_fare_variation` | Mean `total_fare` and booking count for each season, grouped by `season_type` and `seasonality`. `pct_vs_non_peak` is the percentage difference between that season's mean and the mean of all non-peak bookings: `(season_avg − non_peak_avg) / non_peak_avg × 100`. |
| Booking count by airline | `kpi_booking_count_by_airline` | Number of rows per airline, most bookings first. |
| Most popular routes | `kpi_popular_routes` | Rows grouped by `route` (`SOURCE - DESTINATION`), sorted by booking count, top 20 kept, with a `rank` column starting at 1. |

### 5.3 Results

Average fare by airline ranges from 68,108 BDT (Vistara) to 75,547 BDT (Turkish Airlines); AirAsia (74,534) and Cathay Pacific (73,325) follow Turkish Airlines.

US-Bangla Airlines has by far the most bookings (4,496). The other 23 airlines have between 2,201 and 2,368 bookings each.

The seasonal comparison shows a clear peak-season premium:

| Season | Type | Average fare (BDT) | Bookings | Difference vs non-peak |
|---|---|---|---|---|
| Regular | Non-peak | 68,077.27 | 44,525 | 0.00% |
| Winter Holidays | Peak | 79,676.74 | 10,930 | +17.04% |
| Eid | Peak | 91,560.02 | 603 | +34.49% |
| Hajj | Peak | 97,144.47 | 942 | +42.70% |

Taken together, peak-season bookings average 81,570 BDT against 68,077 BDT for non-peak, about 19.8% higher. The busiest route is Rajshahi to Singapore (RJH - SIN) with 417 bookings, followed by DAC - DXB (413) and BZL - YYZ (410). The top routes are close: the 4th and 5th are tied at 408. In total the pipeline processes fares worth 4,048,728,023.05 BDT.

## 6. Loading into PostgreSQL

The goal is minimal latency and data consistency during the transfer.

**Consistency.** The five tables are written inside a single transaction. After each table, the row count in PostgreSQL is compared with the count read from MySQL, and a mismatch raises an error. If any table fails, the transaction is rolled back and PostgreSQL keeps the previous complete data. The analytics database therefore never holds a mix of old and new tables. 

**Latency.** Rows are sent as multi-row INSERT statements. The batch size is computed per table so that a statement never exceeds the 65,535 bound-parameter limit of the PostgreSQL driver: the 20-column enriched table uses 3,276 rows per statement and the narrow KPI tables use 5,000. In the latest run on the final machine, the whole `load_to_postgres` task, which writes all five tables, took about 50 seconds (see `docs/evidence/task_durations.txt`). Fare columns are created as `NUMERIC(12,2)` so that money values are not stored as floating-point numbers.

After a run, the same row counts and the same sum of `total_fare` (4,048,728,023.05) were verified in both MySQL and PostgreSQL.

## 7. Project Structure

```
flight-pipeline/
├── airflow/dags/
│   ├── flight_price_pipeline.py   DAG definition: tasks and their order only
│   └── flight_etl/                task logic, one module per concern
│       ├── config.py              table names, column maps, connection IDs, constants
│       ├── schema.py              CREATE TABLE and ALTER TABLE statements
│       ├── db.py                  shared "write a DataFrame and verify the row count"
│       ├── ingest.py              task 1
│       ├── validate.py            task 2
│       ├── transform.py           task 3
│       ├── kpis.py                task 4
│       └── load.py                task 5
├── data/                          the CSV (not committed)
├── docs/report.md                 this report
└── requirements.txt
```

Each task module separates the pure data logic (`read_flight_csv`, `clean_flights`, `enrich_flights`, `calculate_kpis`) from the function that talks to the database and Airflow. The logic can therefore be read and tried on a small DataFrame without any running database. Every module and function has a comment or docstring explaining what it does. Credentials are never in the code: databases are reached through the Airflow connections `mysql_staging` and `postgres_analytics`, and `airflow.cfg` is excluded from version control.

## 8. Challenges and How They Were Resolved

**Limited memory, no Docker.** The usual Docker-based Airflow setup did not fit in the memory of the first development machine (WSL, capped at 4 GB). Airflow was installed directly instead, using `airflow standalone` with the LocalExecutor and a PostgreSQL metadata database, and the project later moved to a Linux PC with the same setup. The dataset is small enough (57,000 rows) for the tasks to work in memory with pandas.

**PostgreSQL parameter limit.** The final task failed with `number of parameters must be between 0 and 65535`. The enriched table has 20 columns, and a 5,000-row INSERT binds 100,000 values, more than the driver allows in one statement. The fix is a small function that divides the limit by the number of columns of each table to choose a safe batch size. It also keeps working if columns are added later.

**False total-fare corrections.** The first version of the validation recomputed the total fare and reported 7,551 corrections. Investigation showed two different things mixed together. About 5,000 of those rows differed only by rounding: MySQL stores each fare with two decimals, so base fare plus tax can differ from the total by up to 0.015 BDT, which the original 0.01 tolerance did not allow. The other 2,522 rows are real differences, all of them between 445 and 93,165 BDT, with nothing between 0.02 and 445. Because they are always higher than base plus tax and appear in all three travel classes, they look like a genuine extra charge, not data errors. Overwriting them would have changed the data and lowered every airline's average fare by 0.7% to 1.2%. The solution was to raise the tolerance to 0.05 BDT, keep the original totals, and mark the 2,522 rows with a `total_fare_mismatch` flag, so analysts can include or exclude them.

**A validation check that could never fire.** The "required columns exist" check ran on the staging table, which always has the full set of columns, so a CSV with a missing column would have crashed earlier with an unclear key error. A header check was added to the ingestion step, which now stops with a message listing the missing columns before anything is written to MySQL. Extra columns in the file are ignored.

**A single 300-line DAG file.** Configuration, SQL, the five tasks and the DAG definition were all in one file, which made it hard to read and impossible to try parts of it in isolation. The code was split into the `flight_etl` package, and the copy-pasted "write and verify the row count" block, which existed in four tasks, was replaced by one shared function. The DAG file shrank to about 70 lines.

**Overlapping runs and temporary failures.** Two simultaneous runs would drop and recreate the same tables. `max_active_runs=1` prevents this. Retries with a delay, and a timeout per task, handle short database interruptions without manual re-runs.

## 9. How to Run

1. Create the two databases (`flight_staging` in MySQL, `flight_analytics` in PostgreSQL) and install the dependencies from `requirements.txt`.
2. In Airflow, create the connections `mysql_staging` and `postgres_analytics`.
3. Place `Flight_Price_Dataset_of_Bangladesh.csv` in `data/`, or set the `FLIGHT_CSV_PATH` environment variable to its location.
4. Start Airflow with `airflow standalone`, open the DAG `flight_price_pipeline` in the web interface and trigger it.
5. Check the result in PostgreSQL: `enriched_flights` should have 57,000 rows, and the four KPI tables 24, 4, 24 and 20 rows.

## 10. Limitations and Possible Improvements

The load into PostgreSQL uses batched INSERT statements. PostgreSQL's native `COPY` command would be considerably faster and is the first step if the data grows. The KPIs are computed in pandas after reading the whole enriched table, which is fine for 57,000 rows; for much larger data they could be computed with SQL `GROUP BY` queries inside the database. The pipeline has 16 unit tests, which cover the validation rules, the CSV header check and the PostgreSQL batch size rule but not the transform, KPI or load logic. It also has no failure alerts (for example an e-mail on a failed task), and it runs only when triggered; a schedule can be added if new files arrive regularly. Finally, each run rebuilds the tables completely; keeping history across runs would need a run identifier column and an append strategy.