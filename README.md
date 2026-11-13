# Flight Price Analysis Pipeline

An Apache Airflow pipeline that ingests the *Flight Price Dataset of Bangladesh*, validates and enriches it, computes four KPIs, and loads the results into PostgreSQL for analysis.

```
CSV ─► MySQL staging ─► validate ─► transform ─► KPIs ─► PostgreSQL analytics
```

For the full design, task descriptions, KPI logic and the problems solved along the way, see [docs/report.md](docs/report.md).

## What it does

| Step | Airflow task | Result |
|---|---|---|
| 1 | `ingest_csv_to_mysql` | Loads the CSV into the MySQL table `stg_flights`, with typed columns and a row-count check |
| 2 | `validate_data` | Drops unusable rows, flags inconsistent total fares, writes `validated_flights` |
| 3 | `transform_data` | Adds `route` and `season_type`, writes `enriched_flights` |
| 4 | `compute_kpis` | Builds the four KPI tables in MySQL |
| 5 | `load_to_postgres` | Copies the enriched data and KPI tables to PostgreSQL in a single transaction |

### KPIs

| KPI | Table |
|---|---|
| Average fare by airline | `kpi_avg_fare_by_airline` |
| Seasonal fare variation (peak vs. non-peak) | `kpi_seasonal_fare_variation` |
| Booking count by airline | `kpi_booking_count_by_airline` |
| Most popular routes (top 20) | `kpi_popular_routes` |

*Eid*, *Hajj* and *Winter Holidays* count as peak seasons; *Regular* counts as non-peak.

## Tech stack

Apache Airflow 3 (LocalExecutor), MySQL (staging), PostgreSQL (analytics and Airflow metadata), Python with pandas and SQLAlchemy. Developed on WSL with Airflow run through `airflow standalone`, without Docker.

## Project structure

```
flight-pipeline/
├── airflow/
│   └── dags/
│       ├── flight_price_pipeline.py   DAG: tasks and their order
│       └── flight_etl/                task logic
│           ├── config.py              table names, columns, connection IDs, constants
│           ├── schema.py              CREATE / ALTER TABLE statements
│           ├── db.py                  write a DataFrame and verify the row count
│           ├── ingest.py              task 1
│           ├── validate.py            task 2
│           ├── transform.py           task 3
│           ├── kpis.py                task 4
│           └── load.py                task 5
├── data/                              put the CSV here (not committed)
├── docs/report.md                     full project report
├── requirements.txt
└── README.md
```

## Prerequisites

- Linux or WSL with Python 3.10 or newer
- A running MySQL server and a running PostgreSQL server
- The MySQL client library headers, needed to build the `mysqlclient` package: `sudo apt install libmysqlclient-dev pkg-config build-essential`
- The dataset *Flight Price Dataset of Bangladesh* from Kaggle (file `Flight_Price_Dataset_of_Bangladesh.csv`)

## Setup

### 1. Install the dependencies

```bash
cd flight-pipeline
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

`requirements.txt` explains how to install Airflow with its official constraint file, which keeps all of Airflow's own dependencies on tested versions.

### 2. Create the databases

In MySQL:

```sql
CREATE DATABASE flight_staging;
```

In PostgreSQL (one database for the analytics tables, one for Airflow's metadata):

```sql
CREATE DATABASE flight_analytics;
CREATE DATABASE airflow_db;
```

### 3. Configure Airflow

Point Airflow at this project and at the PostgreSQL metadata database. Replace `USER` and `PASSWORD` with your own PostgreSQL credentials.

```bash
export AIRFLOW_HOME="$(pwd)/airflow"
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://USER:PASSWORD@127.0.0.1:5432/airflow_db"
```

Airflow writes its generated `airflow.cfg` into `airflow/`. That file can contain your database password, so it is excluded from git and must never be committed.

### 4. Create the connections

The code never contains credentials. It reaches the databases through two Airflow connections:

```bash
airflow connections add mysql_staging \
  --conn-type mysql --conn-host 127.0.0.1 --conn-port 3306 \
  --conn-login USER --conn-password PASSWORD --conn-schema flight_staging

airflow connections add postgres_analytics \
  --conn-type postgres --conn-host 127.0.0.1 --conn-port 5432 \
  --conn-login USER --conn-password PASSWORD --conn-schema flight_analytics
```

You can also create them in the web interface under **Admin → Connections**. The connection IDs must be exactly `mysql_staging` and `postgres_analytics`.

### 5. Add the data

Copy the CSV to `data/Flight_Price_Dataset_of_Bangladesh.csv`. If the project is not in `~/flight-pipeline`, tell the pipeline where the file is:

```bash
export FLIGHT_CSV_PATH="/full/path/to/Flight_Price_Dataset_of_Bangladesh.csv"
```

## Running the pipeline

Start Airflow (with the environment variables from step 3 set in the same terminal):

```bash
source venv/bin/activate
airflow standalone
```

`airflow standalone` prints the admin username and password on first start; they are also saved in `airflow/simple_auth_manager_passwords.json.generated`. Open the address it shows (normally http://localhost:8080), find the DAG **flight_price_pipeline**, and press the trigger button. A full run takes a few minutes. The DAG has no schedule; it runs only when triggered.

## Verifying the results

Open `psql` on the analytics database (`psql -h 127.0.0.1 -U USER -d flight_analytics`) and run:

```sql
SELECT COUNT(*) FROM enriched_flights;
SELECT * FROM kpi_seasonal_fare_variation ORDER BY avg_total_fare DESC;
SELECT * FROM kpi_popular_routes ORDER BY rank LIMIT 5;
```

With the original dataset you should see:

| Check | Expected |
|---|---|
| Rows in `enriched_flights` | 57,000 |
| Rows in the four KPI tables | 24 airlines, 4 seasons, 24 airlines, 20 routes |
| `SUM(total_fare)` in `enriched_flights` | 4,048,728,023.05 (identical in MySQL) |
| Rows with `total_fare_mismatch = 1` | 2,522 |
| Peak premium over regular fares | Eid about +34%, Hajj about +43%, Winter Holidays about +17% |

## Design notes

- **One table per stage.** Staging, validated and enriched data live in separate tables, so any step can be fixed and re-run without re-reading the CSV.
- **Idempotent tasks.** Each task drops and recreates its output tables, so re-running or retrying never duplicates rows. Tasks are retried twice, and `max_active_runs=1` stops two runs from overwriting the same tables.
- **Verified loads.** Every write compares the table's row count with the data that was sent, and the PostgreSQL load is a single all-or-nothing transaction.
- **Flag, don't overwrite.** 2,522 bookings have a total fare higher than base fare plus tax (by 445 to 93,000 BDT). These look like real extra charges, so they are kept as they are and marked with `total_fare_mismatch = 1` instead of being "corrected".
- **Docstrings everywhere.** Every module and function documents its purpose, arguments, return value and errors.

## Troubleshooting

| Problem | Likely cause and fix |
|---|---|
| Import error banner saying `No module named 'flight_etl'` | The `flight_etl/` folder must sit directly inside `airflow/dags/` and contain `__init__.py`. Restart `airflow standalone` after fixing. |
| `pip install` fails building `mysqlclient` | Install the system packages listed under Prerequisites. |
| A task fails with a connection error | Check that the MySQL and PostgreSQL servers are running, and that the connection IDs and credentials in **Admin → Connections** are correct. |
| `CSV is missing required columns` | The CSV does not match the expected header; use the original Kaggle file. |
| `FileNotFoundError` in the ingest task | The CSV is not at the expected path; place it in `data/` or set `FLIGHT_CSV_PATH`. |
| WSL runs out of memory | Limit and tune WSL in `%UserProfile%\.wslconfig` (for example `memory=4GB` under `[wsl2]`), then run `wsl --shutdown` and restart. |

## Security

Never commit `airflow/airflow.cfg`, the `*.generated` password file, the `venv/` folder or the dataset. They are all listed in `.gitignore`.