"""Database helpers shared by every task of the pipeline.

Each task used to end with the same steps: write a DataFrame to a table, count
the rows that arrived, and fail if the count is wrong. That logic lives here
once so the checks cannot differ between tasks.

Nothing in this module imports Airflow; it works with any SQLAlchemy engine or
connection, which keeps it easy to unit test.
"""

from typing import Optional, Union

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

# Either a pooled engine (each call opens and commits its own connection) or an
# already-open connection (used when several writes must share one transaction).
Connectable = Union[Engine, Connection]


def count_rows(connectable: Connectable, table: str) -> int:
    """Count the rows currently in a table.

    The table name is quoted with the rules of the target database (backticks
    for MySQL, double quotes for PostgreSQL), so the same function works on
    both.

    Args:
        connectable: SQLAlchemy engine or open connection to query.
        table: Name of the table to count.

    Returns:
        Number of rows in the table.
    """
    quoted = connectable.dialect.identifier_preparer.quote(table)
    query = text(f"SELECT COUNT(*) FROM {quoted}")

    if isinstance(connectable, Connection):
        # Reuse the caller's connection so uncommitted rows are counted too.
        return int(connectable.execute(query).scalar())

    with connectable.connect() as conn:
        return int(conn.execute(query).scalar())


def write_dataframe(
    df: pd.DataFrame,
    table: str,
    connectable: Connectable,
    chunksize: int,
    if_exists: str = "append",
    dtype: Optional[dict] = None,
) -> int:
    """Write a DataFrame to a table and verify that every row arrived.

    Args:
        df: Data to write. The index is never written.
        table: Destination table name.
        connectable: SQLAlchemy engine or open connection to write through.
        chunksize: Number of rows sent per INSERT statement.
        if_exists: What to do if the table exists: ``"append"`` (default,
            for tables created beforehand) or ``"replace"`` (drop and
            recreate it from the DataFrame).
        dtype: Optional mapping of column name to SQLAlchemy type, to override
            the types pandas would otherwise guess.

    Returns:
        Number of rows now in the table.

    Raises:
        ValueError: If the table's row count differs from ``len(df)``.
    """
    df.to_sql(
        table,
        connectable,
        if_exists=if_exists,
        index=False,
        chunksize=chunksize,
        method="multi",
        dtype=dtype,
    )

    loaded = count_rows(connectable, table)
    if loaded != len(df):
        raise ValueError(f"{table}: expected {len(df)} rows, found {loaded}")
    return loaded