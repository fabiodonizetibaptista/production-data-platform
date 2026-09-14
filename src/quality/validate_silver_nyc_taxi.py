from datetime import datetime
from pathlib import Path

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
    """
    Conta quantos valores True existem em uma expressão
    booleana do PyArrow.
    """

    result = pc.sum(
        pc.cast(mask, "int64")
    ).as_py()

    return int(result or 0)


def get_row_count(
    file_path: Path,
    required: bool = True,
) -> int:
    """
    Retorna a quantidade de linhas de um Parquet.

    Para arquivos opcionais, como quarantine, a ausência
    representa zero registros e não uma falha.
    """

    if not file_path.exists():

        if required:
            raise FileNotFoundError(
                f"Arquivo não encontrado: {file_path}"
            )

        return 0

    return pq.ParquetFile(
        file_path
    ).metadata.num_rows


def validate_silver() -> bool:
    """
    Valida o contrato da camada Silver.

    Verifica:
    - reconciliação entre RAW, Silver e quarantine;
    - presença das colunas obrigatórias;
    - consistência temporal;
    - registros fora do mês esperado.
    """

    print("\n=== SILVER VALIDATION ===\n")

    # ---------------------------------------------------------
    # 1. Reconciliação
    # ---------------------------------------------------------

    raw_count = get_row_count(
        RAW_FILE
    )

    silver_count = get_row_count(
        SILVER_FILE
    )

    quarantine_count = get_row_count(
        QUARANTINE_FILE,
        required=False,
    )

    reconciliation_ok = (
        raw_count
        == silver_count + quarantine_count
    )

    print(
        f"[{'PASS' if reconciliation_ok else 'FAIL'}] "
        f"reconciliation "
        f"RAW={raw_count:,} "
        f"SILVER={silver_count:,} "
        f"QUARANTINE={quarantine_count:,}"
    )

    # ---------------------------------------------------------
    # 2. Schema
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

    schema_ok = (
        len(missing_columns) == 0
    )

    print(
        f"[{'PASS' if schema_ok else 'FAIL'}] "
        f"silver_schema "
        f"missing={sorted(missing_columns)}"
    )

    # ---------------------------------------------------------
    # 3. Ler somente as colunas necessárias
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

    # ---------------------------------------------------------
    # 4. Dropoff não pode acontecer antes do pickup
    # ---------------------------------------------------------

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

    # ---------------------------------------------------------
    # 5. Pickup precisa pertencer à partição mensal
    # ---------------------------------------------------------

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

        return False

    print(
        "SILVER VALIDATION: PASSED"
    )

    return True


if __name__ == "__main__":

    success = validate_silver()

    raise SystemExit(
        0 if success else 1
    )