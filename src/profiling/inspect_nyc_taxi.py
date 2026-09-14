from pathlib import Path

import pyarrow.parquet as pq


DATA_FILE = Path("data/raw/yellow_tripdata_2026-01.parquet")


def inspect_parquet(file_path: Path) -> None:
    """
    Exibe metadados básicos de um arquivo Parquet sem carregar
    todo o dataset na memória.
    """

    if not file_path.exists():
        raise FileNotFoundError(
            f"Arquivo não encontrado: {file_path}"
        )

    parquet_file = pq.ParquetFile(file_path)

    metadata = parquet_file.metadata
    schema = parquet_file.schema_arrow

    print("\n=== ARQUIVO ===")
    print(file_path)

    print("\n=== METADADOS ===")
    print(f"Linhas: {metadata.num_rows:,}")
    print(f"Colunas: {metadata.num_columns}")
    print(f"Row groups: {metadata.num_row_groups}")

    print("\n=== SCHEMA ===")

    for field in schema:
        print(
            f"{field.name:<30} "
            f"{str(field.type):<25} "
            f"nullable={field.nullable}"
        )


if __name__ == "__main__":
    inspect_parquet(DATA_FILE)