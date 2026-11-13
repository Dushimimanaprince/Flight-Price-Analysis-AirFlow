"""Task 5: copy the final tables from MySQL to PostgreSQL.

Loads the enriched flights and the four KPI tables into the analytics
database. All tables are written inside one transaction, so PostgreSQL ends up
with either the complete new data or the complete old data, never a mix.
"""

from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from flight_etl.config import (
    MYSQL_CONN_ID,
    NUMERIC_TYPES,
    POSTGRES_CHUNKSIZE,
    POSTGRES_CONN_ID,
    TABLES_TO_LOAD,
)
from flight_etl.db import write_dataframe

# The PostgreSQL driver (psycopg 3) rejects a single statement that carries
# more than this many bound values.
POSTGRES_MAX_PARAMETERS = 65535


def safe_chunksize(n_columns: int, max_rows: int = POSTGRES_CHUNKSIZE) -> int:
    """Pick a rows-per-INSERT value that stays under PostgreSQL's parameter limit.

    A multi-row INSERT binds one parameter per cell, so its size is
    ``rows * columns``. A table with many columns therefore needs fewer rows
    per statement than a narrow one.

    Args:
        n_columns: Number of columns in the table being written.
        max_rows: Upper bound on rows per INSERT, even for narrow tables.

    Returns:
        The number of rows to send per INSERT, always at least 1.
    """
    return max(1, min(max_rows, POSTGRES_MAX_PARAMETERS // n_columns))


def load_to_postgres() -> dict:
    """Airflow task: replace the analytics tables in PostgreSQL.

    Every table in ``TABLES_TO_LOAD`` is read from MySQL and written to
    PostgreSQL inside a single transaction. If any table fails (including its
    row-count check), the whole transaction rolls back and the previous
    PostgreSQL data is left as it was.

    Returns:
        Mapping of table name to the number of rows loaded (pushed to XCom).

    Raises:
        ValueError: If a table's row count in PostgreSQL differs from MySQL.
    """
    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    pg_engine = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_sqlalchemy_engine()

    report = {}
    with pg_engine.begin() as conn:
        for table in TABLES_TO_LOAD:
            df = mysql_hook.get_df(f"SELECT * FROM {table}")
            dtype = {c: t for c, t in NUMERIC_TYPES.items() if c in df.columns}
            report[table] = write_dataframe(
                df,
                table,
                conn,
                chunksize=safe_chunksize(len(df.columns)),
                if_exists="replace",
                dtype=dtype,
            )

    print(report)
    return report