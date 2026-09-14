from datetime import datetime
from pathlib import Path
import sys

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


RAW_FILE = Path(
    "data/raw/yellow_tripdata_2026-01.parquet"
)

SILVER_FILE = Path(
    "data/silver/yellow_taxi/year=2026/month=01/"
    "yellow_taxi_2026-01.parquet"
)

QUARANTINE_FILE = Path(
    "data/quarantine/yellow_taxi/year=2026/month=01/"
    "yellow_taxi_2026-01_quarantine.parquet"
)


JANUARY_START = pa.scalar(
    datetime(2026, 1, 1),
    type=pa.timestamp("us"),
)

FEBRUARY_START = pa.scalar(
    datetime(2026, 2, 1),
    type=pa.timestamp("us"),
)


EXPECTED_SILVER_COLUMNS = {
    "vendorid",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "pulocationid",
    "dolocationid",
    "payment_type",
    "fare_amount",
    "total_amount",
    "trip_duration_minutes",
    "is_zero_distance",
    "has_negative_fare",
    "has_negative_total",
    "is_passenger_count_null",
    "_source_file",
}


def count_true(mask) -> int:
    result = pc.sum(
        pc.cast(mask, "int64")
    ).as_py()

    return int(result or 0)


def validate_silver() -> None:

    for file_path in [
        RAW_FILE,
        SILVER_FILE,
        QUARANTINE_FILE,
    ]:
        if not file_path.exists():
            raise FileNotFoundError(
                f"Arquivo não encontrado: {file_path}"
            )

    raw_metadata = pq.ParquetFile(
        RAW_FILE
    ).metadata

    silver_metadata = pq.ParquetFile(
        SILVER_FILE
    ).metadata

    quarantine_metadata = pq.ParquetFile(
        QUARANTINE_FILE
    ).metadata

    raw_count = raw_metadata.num_rows
    silver_count = silver_metadata.num_rows
    quarantine_count = quarantine_metadata.num_rows

    print("\n=== SILVER VALIDATION ===\n")

    # ---------------------------------------------------------
    # Reconciliação
    # ---------------------------------------------------------

    reconciled = (
        silver_count + quarantine_count
    )

    reconciliation_ok = (
        raw_count == reconciled
    )

    print(
        f"[{'PASS' if reconciliation_ok else 'FAIL'}] "
        f"reconciliation "
        f"RAW={raw_count:,} "
        f"SILVER={silver_count:,} "
        f"QUARANTINE={quarantine_count:,}"
    )

    # ---------------------------------------------------------
    # Schema Silver
    # ---------------------------------------------------------

    silver_schema = pq.ParquetFile(
        SILVER_FILE
    ).schema_arrow

    actual_columns = set(
        silver_schema.names
    )

    missing_columns = (
        EXPECTED_SILVER_COLUMNS
        - actual_columns
    )

    schema_ok = len(missing_columns) == 0

    print(
        f"[{'PASS' if schema_ok else 'FAIL'}] "
        f"silver_schema "
        f"missing={sorted(missing_columns)}"
    )

    # ---------------------------------------------------------
    # Regras que a Silver prometeu cumprir
    # ---------------------------------------------------------

    silver = pq.read_table(
        SILVER_FILE,
        columns=[
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
        ],
    )

    pickup = silver[
        "tpep_pickup_datetime"
    ]

    dropoff = silver[
        "tpep_dropoff_datetime"
    ]

    invalid_duration = count_true(
        pc.less(
            dropoff,
            pickup,
        )
    )

    duration_ok = (
        invalid_duration == 0
    )

    print(
        f"[{'PASS' if duration_ok else 'FAIL'}] "
        f"dropoff_before_pickup "
        f"observado={invalid_duration:,}"
    )

    pickup_outside_month = count_true(
        pc.or_(
            pc.less(
                pickup,
                JANUARY_START,
            ),
            pc.greater_equal(
                pickup,
                FEBRUARY_START,
            ),
        )
    )

    month_ok = (
        pickup_outside_month == 0
    )

    print(
        f"[{'PASS' if month_ok else 'FAIL'}] "
        f"pickup_outside_january "
        f"observado={pickup_outside_month:,}"
    )

    # ---------------------------------------------------------
    # Resultado final
    # ---------------------------------------------------------

    validations = [
        reconciliation_ok,
        schema_ok,
        duration_ok,
        month_ok,
    ]

    print()

    if not all(validations):
        print(
            "SILVER VALIDATION: FAILED"
        )

        sys.exit(1)

    print(
        "SILVER VALIDATION: PASSED"
    )


if __name__ == "__main__":
    validate_silver()