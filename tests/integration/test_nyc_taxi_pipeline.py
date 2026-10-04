from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import src.ingestion.download_nyc_taxi as ingestion
import src.pipeline.run_nyc_taxi_pipeline as pipeline
import src.quality.validate_silver_nyc_taxi as silver_quality
import src.transformation.raw_to_silver_nyc_taxi as transformation


pytestmark = pytest.mark.integration


def build_raw_table():
    """
    Dataset sintético para validar a integração entre:

    ingestão
        -> validação RAW
        -> transformação Silver
        -> quarantine
        -> validação Silver

    Registros:
    1. válido;
    2. pickup fora de janeiro;
    3. dropoff anterior ao pickup.
    """
    return pa.table(
        {
            "VendorID": [1, 1, 2],
            "tpep_pickup_datetime": pa.array(
                [
                    datetime(2026, 1, 10, 10, 0),
                    datetime(2026, 2, 1, 0, 0),
                    datetime(2026, 1, 15, 12, 0),
                ],
                type=pa.timestamp("us"),
            ),
            "tpep_dropoff_datetime": pa.array(
                [
                    datetime(2026, 1, 10, 10, 20),
                    datetime(2026, 2, 1, 0, 30),
                    datetime(2026, 1, 15, 11, 50),
                ],
                type=pa.timestamp("us"),
            ),
            "passenger_count": [1, 2, None],
            "trip_distance": [2.5, 4.0, 0.0],
            "PULocationID": [100, 101, 102],
            "DOLocationID": [200, 201, 202],
            "payment_type": [1, 1, 2],
            "fare_amount": [15.0, 20.0, -5.0],
            "total_amount": [18.0, 24.0, -6.0],
        }
    )


def test_pipeline_runs_end_to_end_with_synthetic_data(
    tmp_path,
    monkeypatch,
):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    raw_file = raw_dir / "yellow_tripdata_2026-01.parquet"

    silver_file = (
        tmp_path
        / "silver"
        / "yellow_taxi"
        / "year=2026"
        / "month=01"
        / "yellow_taxi_2026-01.parquet"
    )

    quarantine_file = (
        tmp_path
        / "quarantine"
        / "yellow_taxi"
        / "year=2026"
        / "month=01"
        / "yellow_taxi_2026-01_quarantine.parquet"
    )

    pq.write_table(
        build_raw_table(),
        raw_file,
    )

    # ---------------------------------------------------------
    # Ingestion
    # ---------------------------------------------------------

    monkeypatch.setattr(
        ingestion,
        "RAW_DIR",
        raw_dir,
    )

    monkeypatch.setattr(
        ingestion,
        "OUTPUT_FILE",
        raw_file,
    )

    monkeypatch.setattr(
        ingestion,
        "TEMP_FILE",
        raw_dir / "yellow_tripdata_2026-01.parquet.part",
    )

    # O RAW já existe. Se houver tentativa de rede,
    # a integração deve falhar.
    def fail_if_network_is_called(*args, **kwargs):
        raise AssertionError(
            "A ingestão não deveria acessar a rede "
            "quando o RAW válido já existe."
        )

    monkeypatch.setattr(
        ingestion,
        "urlopen",
        fail_if_network_is_called,
    )

    # ---------------------------------------------------------
    # Pipeline / RAW validation
    # ---------------------------------------------------------

    monkeypatch.setattr(
        pipeline,
        "DATA_FILE",
        raw_file,
    )

    monkeypatch.setattr(
        pipeline,
        "create_run_id",
        lambda: "integration-run-001",
    )

    # ---------------------------------------------------------
    # Transformation
    # ---------------------------------------------------------

    monkeypatch.setattr(
        transformation,
        "RAW_FILE",
        raw_file,
    )

    monkeypatch.setattr(
        transformation,
        "SILVER_FILE",
        silver_file,
    )

    monkeypatch.setattr(
        transformation,
        "QUARANTINE_FILE",
        quarantine_file,
    )

    audit_records = []

    monkeypatch.setattr(
        transformation,
        "write_audit_record",
        audit_records.append,
    )

    # ---------------------------------------------------------
    # Silver validation
    # ---------------------------------------------------------

    monkeypatch.setattr(
        silver_quality,
        "RAW_FILE",
        raw_file,
    )

    monkeypatch.setattr(
        silver_quality,
        "SILVER_FILE",
        silver_file,
    )

    monkeypatch.setattr(
        silver_quality,
        "QUARANTINE_FILE",
        quarantine_file,
    )

    # ---------------------------------------------------------
    # Execução end-to-end
    # ---------------------------------------------------------

    pipeline.run_pipeline()

    # ---------------------------------------------------------
    # Evidências
    # ---------------------------------------------------------

    assert silver_file.exists()
    assert quarantine_file.exists()

    silver_count = (
        pq.ParquetFile(silver_file)
        .metadata.num_rows
    )

    quarantine_count = (
        pq.ParquetFile(quarantine_file)
        .metadata.num_rows
    )

    assert silver_count == 1
    assert quarantine_count == 2

    assert silver_count + quarantine_count == 3

    assert len(audit_records) == 1

    audit = audit_records[0]

    assert audit["run_id"] == "integration-run-001"
    assert audit["status"] == "SUCCESS"
    assert audit["raw_count"] == 3
    assert audit["silver_count"] == 1
    assert audit["quarantine_count"] == 2
