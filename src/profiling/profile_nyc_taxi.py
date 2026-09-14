from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq


DATA_FILE = Path("data/raw/yellow_tripdata_2026-01.parquet")


def profile_dataset(file_path: Path) -> None:
    """
    Executa um profiling inicial do dataset NYC Yellow Taxi.

    O objetivo aqui não é corrigir os dados.
    Queremos descobrir o que realmente existe na camada RAW.
    """

    if not file_path.exists():
        raise FileNotFoundError(
            f"Arquivo não encontrado: {file_path}"
        )

    # Nesta primeira versão vamos carregar somente as colunas
    # necessárias para nossas verificações iniciais.
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
        "tip_amount",
        "total_amount",
    ]

    table = pq.read_table(
        file_path,
        columns=columns,
    )

    total_rows = table.num_rows

    print("\n=== DATASET ===")
    print(f"Total de linhas: {total_rows:,}")

    print("\n=== INTERVALO DE DATAS ===")

    pickup = table["tpep_pickup_datetime"]
    dropoff = table["tpep_dropoff_datetime"]

    print(
        "Pickup mínimo:",
        pc.min(pickup).as_py(),
    )

    print(
        "Pickup máximo:",
        pc.max(pickup).as_py(),
    )

    print(
        "Dropoff mínimo:",
        pc.min(dropoff).as_py(),
    )

    print(
        "Dropoff máximo:",
        pc.max(dropoff).as_py(),
    )

    print("\n=== NULLS ===")

    for column_name in columns:
        column = table[column_name]

        null_count = column.null_count

        null_percentage = (
            null_count / total_rows * 100
            if total_rows > 0
            else 0
        )

        print(
            f"{column_name:<30}"
            f"{null_count:>12,} "
            f"({null_percentage:6.2f}%)"
        )

    print("\n=== REGRAS DE QUALIDADE ===")

    negative_distance = pc.sum(
        pc.cast(
            pc.less(
                table["trip_distance"],
                0,
            ),
            "int64",
        )
    ).as_py()

    negative_fare = pc.sum(
        pc.cast(
            pc.less(
                table["fare_amount"],
                0,
            ),
            "int64",
        )
    ).as_py()

    negative_total = pc.sum(
        pc.cast(
            pc.less(
                table["total_amount"],
                0,
            ),
            "int64",
        )
    ).as_py()

    invalid_duration = pc.sum(
        pc.cast(
            pc.less(
                table["tpep_dropoff_datetime"],
                table["tpep_pickup_datetime"],
            ),
            "int64",
        )
    ).as_py()

    zero_distance = pc.sum(
        pc.cast(
            pc.equal(
                table["trip_distance"],
                0,
            ),
            "int64",
        )
    ).as_py()

    print(
        f"trip_distance < 0: "
        f"{negative_distance:,}"
    )

    print(
        f"trip_distance = 0: "
        f"{zero_distance:,}"
    )

    print(
        f"fare_amount < 0: "
        f"{negative_fare:,}"
    )

    print(
        f"total_amount < 0: "
        f"{negative_total:,}"
    )

    print(
        f"dropoff < pickup: "
        f"{invalid_duration:,}"
    )


if __name__ == "__main__":
    profile_dataset(DATA_FILE)