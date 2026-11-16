# Unit tests for the validation rules in flight_etl.validate.clean_flights.
# They use tiny DataFrames, so no database or Airflow run is needed.

import pandas as pd
import pytest

from flight_etl.validate import clean_flights

# one valid row; each test copies it and changes one thing
GOOD_ROW = {
    "airline": "Biman Bangladesh",
    "source": "DAC",
    "destination": "CXB",
    "base_fare": 5000.0,
    "tax_surcharge": 500.0,
    "total_fare": 5500.0,
    "duration_hrs": 1.0,
}


def make_df(*changes):
    # build a DataFrame with the good row plus one modified copy per change
    rows = [GOOD_ROW]
    for change in changes:
        rows.append({**GOOD_ROW, **change})
    return pd.DataFrame(rows)


def test_valid_row_is_kept_and_not_flagged():
    good, report = clean_flights(make_df())
    assert len(good) == 1
    assert good["total_fare_mismatch"].iloc[0] == 0
    assert report["total_fare_flagged"] == 0


def test_total_fare_mismatch_is_flagged_not_overwritten():
    # total 6000 does not equal 5000 + 500, so it is flagged but kept as is
    good, report = clean_flights(make_df({"total_fare": 6000.0}))
    flagged = good[good["total_fare_mismatch"] == 1]
    assert len(flagged) == 1
    assert flagged["total_fare"].iloc[0] == 6000.0
    assert report["total_fare_flagged"] == 1


def test_rounding_noise_is_not_flagged():
    # a 0.01 gap is within the 0.05 tolerance
    good, report = clean_flights(make_df({"total_fare": 5500.01}))
    assert report["total_fare_flagged"] == 0
    assert len(good) == 2


def test_negative_fare_is_dropped():
    good, report = clean_flights(make_df({"base_fare": -1.0}))
    assert len(good) == 1
    assert report["dropped_negative_fare"] == 1


def test_missing_airline_is_dropped():
    good, report = clean_flights(make_df({"airline": None}))
    assert len(good) == 1
    assert report["dropped_missing"] == 1


def test_invalid_airport_code_is_dropped():
    # "DH" is not three letters
    good, report = clean_flights(make_df({"source": "DH"}))
    assert len(good) == 1
    assert report["dropped_invalid_city"] == 1


def test_same_source_and_destination_is_dropped():
    good, report = clean_flights(make_df({"destination": "DAC"}))
    assert len(good) == 1
    assert report["dropped_invalid_city"] == 1


def test_airport_codes_are_uppercased():
    good, _ = clean_flights(make_df({"source": " dac ", "destination": "cxb"}))
    assert set(good["source"]) == {"DAC"}
    assert set(good["destination"]) == {"CXB"}


def test_missing_required_column_raises():
    df = make_df().drop(columns=["total_fare"])
    with pytest.raises(ValueError, match="Missing required columns"):
        clean_flights(df)
