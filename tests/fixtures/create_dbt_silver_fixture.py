from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


OUTPUT_FILE = Path(
    "data/silver/yellow_taxi/"
    "year=2026/month=01/"
    "yellow_taxi_2026-01.parquet"
)


def main() -> None:
    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    table = pa.table(
        {
            "vendorid": [1, 1, 2],
            "tpep_pickup_datetime": pa.array(
                [
                    datetime(2026, 1, 10, 10, 0),
                    datetime(2026, 1, 10, 11, 0),
                    datetime(2026, 1, 11, 12, 0),
                ],
                type=pa.timestamp("us"),
            ),
            "tpep_dropoff_datetime": pa.array(
                [
                    datetime(2026, 1, 10, 10, 20),
                    datetime(2026, 1, 10, 11, 30),
                    datetime(2026, 1, 11, 12, 15),
                ],
                type=pa.timestamp("us"),
            ),
            "passenger_count": [1, 2, None],
            "trip_distance": [2.5, 4.0, 0.0],
            "pulocationid": [100, 101, 102],
            "dolocationid": [200, 201, 202],
            "payment_type": [1, 1, 2],
            "fare_amount": [15.0, 20.0, 8.0],
            "total_amount": [18.0, 24.0, 10.0],
            "trip_duration_minutes": [20.0, 30.0, 15.0],
            "is_zero_distance": [False, False, True],
            "has_negative_fare": [False, False, False],
            "has_negative_total": [False, False, False],
            "is_passenger_count_null": [False, False, True],
            "_source_file": [
                "ci_fixture.parquet",
                "ci_fixture.parquet",
                "ci_fixture.parquet",
            ],
        }
    )

    pq.write_table(
        table,
        OUTPUT_FILE,
        compression="snappy",
    )

    print(
        f"CI Silver fixture criada: "
        f"{OUTPUT_FILE} "
        f"({table.num_rows} rows)"
    )


if __name__ == "__main__":
    main()
