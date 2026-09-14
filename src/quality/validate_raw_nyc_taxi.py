from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import sys

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


DATA_FILE = Path("data/raw/yellow_tripdata_2026-01.parquet")


# Colunas que consideramos obrigatórias para esta versão da pipeline.
REQUIRED_COLUMNS = {
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "total_amount",
}


# Janeiro é tratado como intervalo:
#
# [2026-01-01, 2026-02-01)
#
# ou seja:
# inclui 01/01
# exclui 01/02.
JANUARY_START = pa.scalar(
    datetime(2026, 1, 1),
    type=pa.timestamp("us"),
)

FEBRUARY_START = pa.scalar(
    datetime(2026, 2, 1),
    type=pa.timestamp("us"),
)


@dataclass
class CheckResult:
    """
    Representa o resultado de uma regra de qualidade.
    """

    name: str
    severity: str
    passed: bool
    observed: str
    expectation: str


def count_true(mask) -> int:
    """
    Conta quantos valores True existem em uma expressão booleana
    do PyArrow.
    """

    result = pc.sum(
        pc.cast(mask, "int64")
    ).as_py()

    return int(result or 0)


def print_results(results: list[CheckResult]) -> None:
    """
    Exibe o relatório das regras de qualidade.
    """

    print("\n=== DATA QUALITY REPORT ===\n")

    for result in results:

        if result.passed:
            status = "PASS"

        elif result.severity == "FAIL":
            status = "FAIL"

        else:
            status = "WARN"

        print(
            f"[{status:<4}] "
            f"{result.name:<35} "
            f"observado={result.observed}"
        )

        if not result.passed:
            print(
                f"       esperado: {result.expectation}"
            )


def validate_dataset(file_path: Path) -> None:

    if not file_path.exists():
        raise FileNotFoundError(
            f"Arquivo não encontrado: {file_path}"
        )

    results: list[CheckResult] = []

    # ---------------------------------------------------------
    # 1. Schema
    # ---------------------------------------------------------

    parquet_file = pq.ParquetFile(file_path)

    schema_columns = set(
        parquet_file.schema_arrow.names
    )

    missing_columns = (
        REQUIRED_COLUMNS - schema_columns
    )

    results.append(
        CheckResult(
            name="required_columns",
            severity="FAIL",
            passed=len(missing_columns) == 0,
            observed=str(
                sorted(missing_columns)
            ),
            expectation="nenhuma coluna obrigatória ausente",
        )
    )

    # Se o schema já está quebrado, não podemos executar
    # as regras que dependem dessas colunas.
    if missing_columns:
        print_results(results)
        sys.exit(1)

    # ---------------------------------------------------------
    # 2. Ler somente as colunas necessárias
    # ---------------------------------------------------------

    columns = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "total_amount",
    ]

    table = pq.read_table(
        file_path,
        columns=columns,
    )

    pickup = table[
        "tpep_pickup_datetime"
    ]

    dropoff = table[
        "tpep_dropoff_datetime"
    ]

    # ---------------------------------------------------------
    # 3. Campos críticos não podem ser nulos
    # ---------------------------------------------------------

    critical_columns = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "total_amount",
    ]

    for column_name in critical_columns:

        null_count = (
            table[column_name].null_count
        )

        results.append(
            CheckResult(
                name=f"null_{column_name}",
                severity="FAIL",
                passed=null_count == 0,
                observed=str(null_count),
                expectation="0",
            )
        )

    # ---------------------------------------------------------
    # 4. passenger_count
    # ---------------------------------------------------------

    passenger_nulls = (
        table["passenger_count"].null_count
    )

    results.append(
        CheckResult(
            name="passenger_count_null",
            severity="WARN",
            passed=passenger_nulls == 0,
            observed=f"{passenger_nulls:,}",
            expectation="investigar valores nulos",
        )
    )

    # ---------------------------------------------------------
    # 5. Distância zero
    # ---------------------------------------------------------

    zero_distance = count_true(
        pc.equal(
            table["trip_distance"],
            0,
        )
    )

    results.append(
        CheckResult(
            name="trip_distance_zero",
            severity="WARN",
            passed=zero_distance == 0,
            observed=f"{zero_distance:,}",
            expectation="investigar corridas com distância zero",
        )
    )

    # ---------------------------------------------------------
    # 6. Valores monetários negativos
    # ---------------------------------------------------------

    negative_fare = count_true(
        pc.less(
            table["fare_amount"],
            0,
        )
    )

    results.append(
        CheckResult(
            name="negative_fare_amount",
            severity="WARN",
            passed=negative_fare == 0,
            observed=f"{negative_fare:,}",
            expectation="investigar valores negativos",
        )
    )

    negative_total = count_true(
        pc.less(
            table["total_amount"],
            0,
        )
    )

    results.append(
        CheckResult(
            name="negative_total_amount",
            severity="WARN",
            passed=negative_total == 0,
            observed=f"{negative_total:,}",
            expectation="investigar valores negativos",
        )
    )

    # ---------------------------------------------------------
    # 7. Consistência temporal
    # ---------------------------------------------------------

    invalid_duration = count_true(
        pc.less(
            dropoff,
            pickup,
        )
    )

    results.append(
    CheckResult(
        name="dropoff_before_pickup",
        severity="WARN",
        passed=invalid_duration == 0,
        observed=f"{invalid_duration:,}",
        expectation="investigar na transformação Silver",
        )
    )

    # ---------------------------------------------------------
    # 8. Pickup fora do mês esperado
    # ---------------------------------------------------------

    before_january = pc.less(
        pickup,
        JANUARY_START,
    )

    after_january = pc.greater_equal(
        pickup,
        FEBRUARY_START,
    )

    outside_january = count_true(
        pc.or_(
            before_january,
            after_january,
        )
    )

    results.append(
        CheckResult(
            name="pickup_outside_january",
            severity="WARN",
            passed=outside_january == 0,
            observed=f"{outside_january:,}",
            expectation=(
                "investigar registros fora "
                "da janela mensal"
            ),
        )
    )

    # ---------------------------------------------------------
    # Resultado
    # ---------------------------------------------------------

    print_results(results)

    blocking_failures = [
        result
        for result in results
        if (
            not result.passed
            and result.severity == "FAIL"
        )
    ]

    print()

    if blocking_failures:
        print(
            f"DATA QUALITY: FAILED "
            f"({len(blocking_failures)} "
            f"regra(s) bloqueante(s))"
        )

        sys.exit(1)

    print(
        "DATA QUALITY: PASSED"
    )


if __name__ == "__main__":
    validate_dataset(DATA_FILE)