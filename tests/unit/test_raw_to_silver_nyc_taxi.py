import pytest

pytestmark = pytest.mark.unit

from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq

import src.transformation.raw_to_silver_nyc_taxi as transformation


def build_raw_table():
    """
    Cria três registros:

    1. válido;
    2. pickup fora de janeiro;
    3. dropoff anterior ao pickup.

    Esperado:
    SILVER = 1
    QUARANTINE = 2
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


def test_transform_raw_to_silver_separates_invalid_rows_and_audits(
    tmp_path,
    monkeypatch,
):
    raw_file = tmp_path / "raw.parquet"

    silver_file = (
        tmp_path
        / "silver"
        / "year=2026"
        / "month=01"
        / "silver.parquet"
    )

    quarantine_file = (
        tmp_path
        / "quarantine"
        / "year=2026"
        / "month=01"
        / "quarantine.parquet"
    )

    pq.write_table(
        build_raw_table(),
        raw_file,
    )

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

    # Evita escrever no arquivo real de auditoria durante o teste.
    monkeypatch.setattr(
        transformation,
        "write_audit_record",
        audit_records.append,
    )

    run_id = "test-run-001"

    transformation.transform_raw_to_silver(
        run_id=run_id,
    )

    assert silver_file.exists()
    assert quarantine_file.exists()

    silver = pq.read_table(silver_file)
    quarantine = pq.read_table(quarantine_file)

    assert silver.num_rows == 1
    assert quarantine.num_rows == 2

    assert (
        silver.num_rows
        + quarantine.num_rows
        == 3
    )

    assert "trip_duration_minutes" in silver.column_names
    assert "is_zero_distance" in silver.column_names
    assert "has_negative_fare" in silver.column_names
    assert "has_negative_total" in silver.column_names
    assert "is_passenger_count_null" in silver.column_names
    assert "_source_file" in silver.column_names

    # Colunas da Silver devem estar normalizadas.
    assert "VendorID" not in silver.column_names
    assert "vendorid" in silver.column_names

    assert silver["_source_file"][0].as_py() == raw_file.name

    assert len(audit_records) == 1

    audit = audit_records[0]

    assert audit["run_id"] == run_id
    assert audit["status"] == "SUCCESS"
    assert audit["raw_count"] == 3
    assert audit["silver_count"] == 1
    assert audit["quarantine_count"] == 2


def test_transform_raw_to_silver_fails_when_raw_does_not_exist(
    tmp_path,
    monkeypatch,
):
    raw_file = tmp_path / "missing_raw.parquet"
    silver_file = tmp_path / "silver.parquet"
    quarantine_file = tmp_path / "quarantine.parquet"

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

    try:
        transformation.transform_raw_to_silver(
            run_id="test-run-missing-raw",
        )
        assert False, "Era esperado FileNotFoundError"
    except FileNotFoundError:
        pass

    assert len(audit_records) == 1
    assert audit_records[0]["status"] == "FAILED"
    assert audit_records[0]["run_id"] == "test-run-missing-raw"


def test_transform_raw_to_silver_removes_stale_quarantine(
    tmp_path,
    monkeypatch,
):
    raw_file = tmp_path / "raw.parquet"
    silver_file = tmp_path / "silver.parquet"
    quarantine_file = tmp_path / "quarantine.parquet"

    valid_table = pa.table(
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
            "passenger_count": [1],
            "trip_distance": [2.5],
            "PULocationID": [100],
            "DOLocationID": [200],
            "payment_type": [1],
            "fare_amount": [15.0],
            "total_amount": [18.0],
        }
    )

    pq.write_table(
        valid_table,
        raw_file,
    )

    # Simula uma quarantine deixada por execução anterior.
    pq.write_table(
        pa.table({"id": [999]}),
        quarantine_file,
    )

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

    transformation.transform_raw_to_silver(
        run_id="test-run-no-quarantine",
    )

    assert silver_file.exists()

    # Como a nova execução não gerou registros inválidos,
    # a quarantine antiga deve ser removida.
    assert not quarantine_file.exists()

    assert len(audit_records) == 1

    audit = audit_records[0]

    assert audit["status"] == "SUCCESS"
    assert audit["raw_count"] == 1
    assert audit["silver_count"] == 1
    assert audit["quarantine_count"] == 0


def test_transform_raw_to_silver_fails_when_raw_does_not_exist(
    tmp_path,
    monkeypatch,
):
    raw_file = tmp_path / "missing_raw.parquet"
    silver_file = tmp_path / "silver.parquet"
    quarantine_file = tmp_path / "quarantine.parquet"

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

    try:
        transformation.transform_raw_to_silver(
            run_id="test-run-missing-raw",
        )
        assert False, "Era esperado FileNotFoundError"
    except FileNotFoundError:
        pass

    assert len(audit_records) == 1
    assert audit_records[0]["status"] == "FAILED"
    assert audit_records[0]["run_id"] == "test-run-missing-raw"


def test_transform_raw_to_silver_removes_stale_quarantine(
    tmp_path,
    monkeypatch,
):
    raw_file = tmp_path / "raw.parquet"
    silver_file = tmp_path / "silver.parquet"
    quarantine_file = tmp_path / "quarantine.parquet"

    valid_table = pa.table(
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
            "passenger_count": [1],
            "trip_distance": [2.5],
            "PULocationID": [100],
            "DOLocationID": [200],
            "payment_type": [1],
            "fare_amount": [15.0],
            "total_amount": [18.0],
        }
    )

    pq.write_table(
        valid_table,
        raw_file,
    )

    # Simula uma quarantine deixada por execução anterior.
    pq.write_table(
        pa.table({"id": [999]}),
        quarantine_file,
    )

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

    transformation.transform_raw_to_silver(
        run_id="test-run-no-quarantine",
    )

    assert silver_file.exists()

    # Como a nova execução não gerou registros inválidos,
    # a quarantine antiga deve ser removida.
    assert not quarantine_file.exists()

    assert len(audit_records) == 1

    audit = audit_records[0]

    assert audit["status"] == "SUCCESS"
    assert audit["raw_count"] == 1
    assert audit["silver_count"] == 1
    assert audit["quarantine_count"] == 0
