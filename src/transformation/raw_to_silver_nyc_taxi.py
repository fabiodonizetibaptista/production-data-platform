from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


RAW_FILE = Path(
    "data/raw/yellow_tripdata_2026-01.parquet"
)

SILVER_DIR = Path(
    "data/silver/yellow_taxi/year=2026/month=01"
)

QUARANTINE_DIR = Path(
    "data/quarantine/yellow_taxi/year=2026/month=01"
)

SILVER_FILE = SILVER_DIR / "yellow_taxi_2026-01.parquet"

QUARANTINE_FILE = (
    QUARANTINE_DIR
    / "yellow_taxi_2026-01_quarantine.parquet"
)


JANUARY_START = pa.scalar(
    datetime(2026, 1, 1),
    type=pa.timestamp("us"),
)

FEBRUARY_START = pa.scalar(
    datetime(2026, 2, 1),
    type=pa.timestamp("us"),
)


def transform_raw_to_silver() -> None:
    """
    Transforma o arquivo RAW do NYC Yellow Taxi em:

    1. Silver:
       registros utilizáveis e padronizados.

    2. Quarantine:
       registros que violam regras bloqueantes
       definidas para esta versão da pipeline.

    A camada RAW nunca é alterada.
    """

    if not RAW_FILE.exists():
        raise FileNotFoundError(
            f"Arquivo RAW não encontrado: {RAW_FILE}"
        )

    print("Lendo RAW...")

    table = pq.read_table(RAW_FILE)

    raw_count = table.num_rows

    print(f"Registros RAW: {raw_count:,}")

    pickup = table["tpep_pickup_datetime"]
    dropoff = table["tpep_dropoff_datetime"]

    # ---------------------------------------------------------
    # Regras bloqueantes da Silver V1
    # ---------------------------------------------------------

    pickup_before_month = pc.less(
        pickup,
        JANUARY_START,
    )

    pickup_after_month = pc.greater_equal(
        pickup,
        FEBRUARY_START,
    )

    pickup_outside_month = pc.or_(
        pickup_before_month,
        pickup_after_month,
    )

    invalid_duration = pc.less(
        dropoff,
        pickup,
    )

    # Um registro vai para quarantine se violar
    # pelo menos uma regra bloqueante.
    quarantine_mask = pc.or_(
        pickup_outside_month,
        invalid_duration,
    )

    valid_mask = pc.invert(quarantine_mask)

    silver = table.filter(valid_mask)

    quarantine = table.filter(quarantine_mask)

    # ---------------------------------------------------------
    # Padronização de nomes
    # ---------------------------------------------------------

    normalized_names = [
        column.lower()
        for column in silver.column_names
    ]

    silver = silver.rename_columns(
        normalized_names
    )

    # ---------------------------------------------------------
    # Colunas derivadas
    # ---------------------------------------------------------

    duration_seconds = pc.divide(
        pc.subtract(
            silver["tpep_dropoff_datetime"],
            silver["tpep_pickup_datetime"],
        ),
        1_000_000,
    )

    duration_minutes = pc.divide(
        duration_seconds,
        60,
    )

    silver = silver.append_column(
        "trip_duration_minutes",
        duration_minutes,
    )

    # ---------------------------------------------------------
    # Flags de qualidade não bloqueantes
    # ---------------------------------------------------------

    silver = silver.append_column(
        "is_zero_distance",
        pc.equal(
            silver["trip_distance"],
            0,
        ),
    )

    silver = silver.append_column(
        "has_negative_fare",
        pc.less(
            silver["fare_amount"],
            0,
        ),
    )

    silver = silver.append_column(
        "has_negative_total",
        pc.less(
            silver["total_amount"],
            0,
        ),
    )

    silver = silver.append_column(
        "is_passenger_count_null",
        pc.is_null(
            silver["passenger_count"]
        ),
    )

    # ---------------------------------------------------------
    # Metadata de processamento
    # ---------------------------------------------------------

    processed_at = datetime.now(timezone.utc)

    processed_at_array = pa.array(
        [processed_at] * silver.num_rows,
        type=pa.timestamp("us", tz="UTC"),
    )

    source_file_array = pa.array(
        [RAW_FILE.name] * silver.num_rows
    )

    silver = silver.append_column(
        "_processed_at",
        processed_at_array,
    )

    silver = silver.append_column(
        "_source_file",
        source_file_array,
    )

    # ---------------------------------------------------------
    # Escrita
    # ---------------------------------------------------------

    SILVER_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    QUARANTINE_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    pq.write_table(
        silver,
        SILVER_FILE,
        compression="snappy",
    )

    if quarantine.num_rows > 0:
        pq.write_table(
            quarantine,
            QUARANTINE_FILE,
            compression="snappy",
        )

    # ---------------------------------------------------------
    # Reconciliação
    # ---------------------------------------------------------

    silver_count = silver.num_rows
    quarantine_count = quarantine.num_rows

    reconciled_count = (
        silver_count + quarantine_count
    )

    print()
    print("=== TRANSFORMATION RESULT ===")
    print(f"RAW:        {raw_count:,}")
    print(f"SILVER:     {silver_count:,}")
    print(f"QUARANTINE: {quarantine_count:,}")
    print(f"RECONCILED: {reconciled_count:,}")

    if reconciled_count != raw_count:
        raise RuntimeError(
            "Falha de reconciliação: "
            "RAW != SILVER + QUARANTINE"
        )

    print()
    print("RAW → SILVER concluído com sucesso.")


if __name__ == "__main__":
    transform_raw_to_silver()