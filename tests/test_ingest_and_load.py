# Tests for the CSV header check (ingest) and the Postgres chunk size rule (load).
# No database is needed: the CSV is built in a temporary folder.

import pandas as pd
import pytest

from flight_etl.config import COLUMN_MAP, POSTGRES_CHUNKSIZE
from flight_etl.ingest import read_flight_csv
from flight_etl.load import POSTGRES_MAX_PARAMETERS, safe_chunksize


def write_csv(tmp_path, drop=None, rename=None):
    # write a one-row CSV that has every expected column
    row = {name: "x" for name in COLUMN_MAP}
    row["Departure Date & Time"] = "2026-01-01 10:00:00"
    row["Arrival Date & Time"] = "2026-01-01 11:00:00"
    df = pd.DataFrame([row])
    if drop:
        df = df.drop(columns=[drop])
    if rename:
        df = df.rename(columns=rename)
    path = tmp_path / "flights.csv"
    df.to_csv(path, index=False)
    return str(path)


def test_read_flight_csv_renames_columns_in_expected_order(tmp_path):
    df = read_flight_csv(write_csv(tmp_path))
    assert list(df.columns) == list(COLUMN_MAP.values())


def test_read_flight_csv_parses_datetimes(tmp_path):
    df = read_flight_csv(write_csv(tmp_path))
    assert pd.api.types.is_datetime64_any_dtype(df["departure_datetime"])
    assert pd.api.types.is_datetime64_any_dtype(df["arrival_datetime"])


def test_read_flight_csv_ignores_spaces_around_header_names(tmp_path):
    path = write_csv(tmp_path, rename={"Airline": "  Airline "})
    df = read_flight_csv(path)
    assert "airline" in df.columns


def test_read_flight_csv_raises_when_a_column_is_missing(tmp_path):
    path = write_csv(tmp_path, drop="Airline")
    with pytest.raises(ValueError, match="missing required columns"):
        read_flight_csv(path)


def test_safe_chunksize_stays_under_postgres_limit():
    # a chunk of rows x columns must never go over 65,535 bound parameters
    for n_columns in (1, 3, 8, 20, 50):
        rows = safe_chunksize(n_columns)
        assert rows >= 1
        assert rows * n_columns <= POSTGRES_MAX_PARAMETERS


def test_safe_chunksize_for_20_columns_is_3276():
    # the case that failed before the fix: 65535 // 20 = 3276
    assert safe_chunksize(20) == 3276


def test_safe_chunksize_never_exceeds_the_configured_maximum():
    assert safe_chunksize(2) == POSTGRES_CHUNKSIZE
