"""Task 3: enrich the validated data with the columns the KPIs need.

Adds a ``route`` column (``SOURCE - DESTINATION``) and a ``season_type``
column (``Peak`` or ``Non-Peak``), and fills in ``total_fare`` if it is
missing. The result is written to its own table so the validated data stays
untouched.
"""

import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook

from flight_etl.config import (
    ENRICHED_TABLE,
    FARE_COLS,
    MYSQL_CHUNKSIZE,
    MYSQL_CONN_ID,
    PEAK_SEASONS,
    VALIDATED_TABLE,
)
from flight_etl.db import write_dataframe
from flight_etl.schema import ADD_ENRICHED_COLUMNS_SQL_TEMPLATE


def enrich_flights(df: pd.DataFrame) -> pd.DataFrame:
    """Add ``route`` and ``season_type`` to validated flights.

    ``total_fare`` is only calculated (base fare + tax) for rows where it is
    missing; an existing value is never overwritten. Kept free of database
    code so it can be unit tested with a small DataFrame.

    Args:
        df: Rows from the validated table.

    Returns:
        A new DataFrame with numeric fare columns, plus:

        * ``route``: for example ``"DAC - CXB"``.
        * ``season_type``: ``"Peak"`` when ``seasonality`` is one of
          ``PEAK_SEASONS``, otherwise ``"Non-Peak"``.
    """
    df = df.copy()

    # MySQL DECIMAL columns arrive as Decimal objects; convert to float.
    for c in FARE_COLS + ["duration_hrs"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")

    # total fare = base fare + tax, only if it is missing
    df["total_fare"] = df["total_fare"].fillna(df["base_fare"] + df["tax_surcharge"])

    df["route"] = df["source"] + " - " + df["destination"]
    df["season_type"] = df["seasonality"].isin(PEAK_SEASONS).map(
        {True: "Peak", False: "Non-Peak"}
    )
    return df


def transform_data() -> dict:
    """Airflow task: build ``enriched_flights`` from ``validated_flights``.

    The destination table is dropped and recreated on every run. It is cloned
    from the validated table, then the two derived columns are added.

    Returns:
        Row counts: ``rows`` saved, and how many are ``peak`` and ``non_peak``
        (pushed to XCom).

    Raises:
        ValueError: If the saved row count differs from the number of rows
            transformed.
    """
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    df = enrich_flights(hook.get_df(f"SELECT * FROM {VALIDATED_TABLE}"))

    hook.run(f"DROP TABLE IF EXISTS {ENRICHED_TABLE}")
    hook.run(f"CREATE TABLE {ENRICHED_TABLE} LIKE {VALIDATED_TABLE}")
    hook.run(ADD_ENRICHED_COLUMNS_SQL_TEMPLATE.format(table=ENRICHED_TABLE))

    saved = write_dataframe(
        df, ENRICHED_TABLE, hook.get_sqlalchemy_engine(), MYSQL_CHUNKSIZE
    )

    report = {
        "rows": saved,
        "peak": int((df["season_type"] == "Peak").sum()),
        "non_peak": int((df["season_type"] == "Non-Peak").sum()),
    }
    print(report)
    return report