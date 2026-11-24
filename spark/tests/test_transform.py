"""Unit tests for the Spark cleaning rules (run: pytest spark/tests)."""
import os
import sys
from datetime import datetime, timedelta

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "jobs"))
from transform import clean_trips, month_bounds  # noqa: E402

COLUMNS = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
           "trip_distance", "PULocationID", "DOLocationID", "payment_type",
           "fare_amount", "tip_amount", "tolls_amount", "total_amount", "extra_column"]


def trip(pickup="2024-01-10 08:00:00", minutes=15, passengers=1.0, distance=3.0, fare=15.0):
    """Build one raw TLC-like row as a SQL VALUES tuple (raw files store passengers as double)."""
    p = datetime.fromisoformat(pickup)
    d = p + timedelta(minutes=minutes)
    return (f"(TIMESTAMP '{p}', TIMESTAMP '{d}', CAST({passengers} AS DOUBLE), "
            f"CAST({distance} AS DOUBLE), 132L, 236L, 1L, CAST({fare} AS DOUBLE), "
            f"CAST(2.0 AS DOUBLE), CAST(0.0 AS DOUBLE), CAST({fare + 2.0} AS DOUBLE), 'x')")


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("tests")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    yield s
    s.stop()


def run(spark, rows):
    # Built with Spark SQL so no Python worker is needed (portable across Python versions)
    df = spark.sql(f"SELECT * FROM VALUES {', '.join(rows)} AS t({', '.join(COLUMNS)})")
    return clean_trips(df, 2024, 1)


def test_month_bounds_handles_december():
    assert month_bounds(2024, 12) == (datetime(2024, 12, 1), datetime(2025, 1, 1))


def test_valid_trip_is_kept_with_features(spark):
    df = run(spark, [trip()])
    out = df.collect()
    assert len(out) == 1
    row = out[0]
    assert row.trip_duration_minutes == 15.0
    assert row.pickup_hour == 8
    assert str(row.pickup_date) == "2024-01-10"
    assert row.cost_per_mile == round(17.0 / 3.0, 2)
    assert "extra_column" not in df.columns


@pytest.mark.parametrize("bad", [
    trip(distance=0.0),                  # zero distance
    trip(fare=2.5),                      # below NYC minimum fare
    trip(passengers=0.0),                # no passengers
    trip(passengers=7.0),                # above legal limit
    trip(minutes=0),                     # duration < 1 min
    trip(minutes=200),                   # duration > 180 min
    trip(pickup="2023-12-31 23:00:00"),  # outside the processed month
    trip(pickup="2024-02-01 00:10:00"),  # outside the processed month
])
def test_invalid_trips_are_removed(spark, bad):
    assert run(spark, [bad]).count() == 0


def test_only_valid_rows_survive_a_mixed_batch(spark):
    rows = [trip(), trip(pickup="2024-01-20 12:00:00"), trip(fare=1.0), trip(distance=0.0)]
    assert run(spark, rows).count() == 2


def test_types_are_cast(spark):
    schema = dict(run(spark, [trip()]).dtypes)
    assert schema["passenger_count"] == "bigint"
    assert schema["PULocationID"] == "bigint"
    assert schema["tpep_pickup_datetime"] == "timestamp"
    assert schema["fare_amount"] == "double"
