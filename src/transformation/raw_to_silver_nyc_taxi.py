from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from src.common.observability import (
    configure_logger,
    create_run_id,
    utc_now,
    write_audit_record,
)


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


logger = configure_logger(
    "nyc_taxi_raw_to_silver"
)


def write_parquet_atomically(
    table: pa.Table,
    destination: Path,
) -> None:
    """
    Escreve primeiro em um arquivo temporário.

    O arquivo final só é substituído depois que a
    escrita termina com sucesso.

    Isso reduz o risco de deixar um Parquet final
    incompleto caso a execução falhe durante a escrita.
    """

    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_file = destination.with_suffix(
        destination.suffix + ".tmp"
    )

    # Pode existir caso uma execução anterior tenha
    # terminado inesperadamente.
    if temp_file.exists():
        logger.warning(
            "Removendo arquivo temporário antigo: %s",
            temp_file,
        )

        temp_file.unlink()

    pq.write_table(
        table,
        temp_file,
        compression="snappy",
    )

    # Path.replace usa uma substituição do arquivo final.
    # Assim só publicamos o resultado depois que a escrita
    # temporária foi concluída.
    temp_file.replace(destination)


def transform_raw_to_silver(
    run_id: str | None = None,
) -> None:
    """
    Processa NYC Yellow Taxi da RAW para Silver.

    Registros que violam regras bloqueantes são enviados
    para quarantine.

    Também registra logs operacionais e uma linha de
    auditoria para cada execução.
    """

    if run_id is None:
        run_id = create_run_id()

    started_at = utc_now()

    raw_count = None
    silver_count = None
    quarantine_count = None

    logger.info(
        "run_id=%s | Iniciando RAW -> SILVER",
        run_id,
    )

    try:
        if not RAW_FILE.exists():
            raise FileNotFoundError(
                f"Arquivo RAW não encontrado: {RAW_FILE}"
            )

        logger.info(
            "run_id=%s | Lendo RAW: %s",
            run_id,
            RAW_FILE,
        )

        table = pq.read_table(
            RAW_FILE
        )

        raw_count = table.num_rows

        logger.info(
            "run_id=%s | RAW rows=%s",
            run_id,
            f"{raw_count:,}",
        )

        pickup = table[
            "tpep_pickup_datetime"
        ]

        dropoff = table[
            "tpep_dropoff_datetime"
        ]

        # -----------------------------------------------------
        # Regras bloqueantes da Silver
        # -----------------------------------------------------

        pickup_outside_month = pc.or_(
            pc.less(
                pickup,
                JANUARY_START,
            ),
            pc.greater_equal(
                pickup,
                FEBRUARY_START,
            ),
        )

        invalid_duration = pc.less(
            dropoff,
            pickup,
        )

        quarantine_mask = pc.or_(
            pickup_outside_month,
            invalid_duration,
        )

        valid_mask = pc.invert(
            quarantine_mask
        )

        silver = table.filter(
            valid_mask
        )

        quarantine = table.filter(
            quarantine_mask
        )

        # -----------------------------------------------------
        # Normalização
        # -----------------------------------------------------

        silver = silver.rename_columns(
            [
                column.lower()
                for column in silver.column_names
            ]
        )

        # -----------------------------------------------------
        # Colunas derivadas
        # -----------------------------------------------------

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

        # -----------------------------------------------------
        # Quality flags
        # -----------------------------------------------------

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

        # -----------------------------------------------------
        # Lineage
        # -----------------------------------------------------

        # _source_file pertence ao registro porque permite
        # rastrear de qual arquivo RAW ele veio.
        silver = silver.append_column(
            "_source_file",
            pa.array(
                [RAW_FILE.name] * silver.num_rows
            ),
        )

        silver_count = silver.num_rows
        quarantine_count = quarantine.num_rows

        # -----------------------------------------------------
        # Reconciliação ANTES de publicar
        # -----------------------------------------------------

        reconciled_count = (
            silver_count
            + quarantine_count
        )

        if reconciled_count != raw_count:
            raise RuntimeError(
                "Falha de reconciliação: "
                f"RAW={raw_count}, "
                f"SILVER={silver_count}, "
                f"QUARANTINE={quarantine_count}"
            )

        logger.info(
            (
                "run_id=%s | Reconciliação OK | "
                "raw=%s silver=%s quarantine=%s"
            ),
            run_id,
            f"{raw_count:,}",
            f"{silver_count:,}",
            f"{quarantine_count:,}",
        )

        # -----------------------------------------------------
        # Publicação Silver
        # -----------------------------------------------------

        write_parquet_atomically(
            silver,
            SILVER_FILE,
        )

        logger.info(
            "run_id=%s | Silver publicada: %s",
            run_id,
            SILVER_FILE,
        )

        # -----------------------------------------------------
        # Publicação Quarantine
        # -----------------------------------------------------

        if quarantine_count > 0:

            write_parquet_atomically(
                quarantine,
                QUARANTINE_FILE,
            )

            logger.warning(
                (
                    "run_id=%s | "
                    "Quarantine publicada | rows=%s"
                ),
                run_id,
                f"{quarantine_count:,}",
            )

        else:
            # Importante:
            # se uma execução anterior gerou quarantine
            # mas esta execução não gerou, removemos o
            # resultado antigo para evitar dado obsoleto.
            if QUARANTINE_FILE.exists():

                logger.info(
                    (
                        "run_id=%s | "
                        "Removendo quarantine obsoleta"
                    ),
                    run_id,
                )

                QUARANTINE_FILE.unlink()

        ended_at = utc_now()

        duration_seconds = (
            ended_at - started_at
        ).total_seconds()

        # -----------------------------------------------------
        # Auditoria da execução
        # -----------------------------------------------------

        write_audit_record(
            {
                "run_id": run_id,
                "pipeline": "nyc_taxi_raw_to_silver",
                "status": "SUCCESS",
                "started_at": started_at.isoformat(),
                "ended_at": ended_at.isoformat(),
                "duration_seconds": duration_seconds,
                "source_file": RAW_FILE.name,
                "raw_count": raw_count,
                "silver_count": silver_count,
                "quarantine_count": quarantine_count,
            }
        )

        logger.info(
            (
                "run_id=%s | Pipeline concluída "
                "com sucesso em %.2fs"
            ),
            run_id,
            duration_seconds,
        )

    except Exception as error:

        ended_at = utc_now()

        duration_seconds = (
            ended_at - started_at
        ).total_seconds()

        write_audit_record(
            {
                "run_id": run_id,
                "pipeline": "nyc_taxi_raw_to_silver",
                "status": "FAILED",
                "started_at": started_at.isoformat(),
                "ended_at": ended_at.isoformat(),
                "duration_seconds": duration_seconds,
                "source_file": RAW_FILE.name,
                "raw_count": raw_count,
                "silver_count": silver_count,
                "quarantine_count": quarantine_count,
                "error_type": type(error).__name__,
                "error_message": str(error),
            }
        )

        # logger.exception também registra o stack trace.
        logger.exception(
            "run_id=%s | Pipeline FAILED",
            run_id,
        )

        raise


if __name__ == "__main__":
    transform_raw_to_silver()