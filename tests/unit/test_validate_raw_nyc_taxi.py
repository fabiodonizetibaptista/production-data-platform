import pytest

pytestmark = pytest.mark.unit

from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq

from src.quality.validate_raw_nyc_taxi import validate_dataset


def build_raw_table(
    *,
    passenger_count=1,
    trip_distance=2.5,
    fare_amount=15.0,
    total_amount=18.0,
):
    """
    Cria um dataset RAW mínimo e sintético com o contrato
    necessário para os testes de qualidade.
    """
    return pa.table(
        {
            "VendorID": [1],
            "tpep_pickup_datetime": pa.array(
                [datetime(2026, 1, 10, 10, 0)],
                type=pa.timestamp("us"),
            ),
            "tpep_dropoff_datetime": pa.array(
                [datetime(2026, 1, 10, 10, 20)],
                type=pa.timestamp("us"),
            ),
            "passenger_count": [passenger_count],
            "trip_distance": [trip_distance],
            "PULocationID": [100],
            "DOLocationID": [200],
            "payment_type": [1],
            "fare_amount": [fare_amount],
            "total_amount": [total_amount],
        }
    )


def test_validate_dataset_returns_true_for_valid_raw(tmp_path):
    file_path = tmp_path / "valid_raw.parquet"

    pq.write_table(
        build_raw_table(),
        file_path,
    )

    assert validate_dataset(file_path) is True


def test_validate_dataset_warns_but_does_not_fail_for_semantic_anomalies(
    tmp_path,
):
    file_path = tmp_path / "raw_with_warnings.parquet"

    table = build_raw_table(
        passenger_count=None,
        trip_distance=0.0,
        fare_amount=-10.0,
        total_amount=-12.0,
    )

    pq.write_table(
        table,
        file_path,
    )

    assert validate_dataset(file_path) is True


def test_validate_dataset_returns_false_when_required_column_is_missing(
    tmp_path,
):
    file_path = tmp_path / "invalid_schema.parquet"

    table = build_raw_table()

    # Remove uma coluna que faz parte do contrato técnico RAW.
    table = table.drop(["VendorID"])

    pq.write_table(
        table,
        file_path,
    )

    assert validate_dataset(file_path) is False
