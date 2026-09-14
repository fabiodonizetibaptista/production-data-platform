from src.common.observability import (
    configure_logger,
    create_run_id,
    utc_now,
)

from src.ingestion.download_nyc_taxi import (
    download_file,
)

from src.quality.validate_raw_nyc_taxi import (
    DATA_FILE,
    validate_dataset,
)

from src.transformation.raw_to_silver_nyc_taxi import (
    transform_raw_to_silver,
)

from src.quality.validate_silver_nyc_taxi import (
    validate_silver,
)


logger = configure_logger(
    "nyc_taxi_end_to_end_pipeline"
)


class PipelineStepError(RuntimeError):
    """
    Erro utilizado quando uma etapa da pipeline
    termina sem atender ao contrato esperado.
    """

    pass


def run_pipeline() -> None:
    """
    Executa a pipeline NYC Taxi de ponta a ponta.

    Fluxo:

    ingestion
        ↓
    raw validation
        ↓
    raw -> silver
        ↓
    silver validation
    """

    run_id = create_run_id()
    started_at = utc_now()

    logger.info(
        "pipeline_run_id=%s | Pipeline iniciada",
        run_id,
    )

    try:
        # -----------------------------------------------------
        # 1. INGESTION
        # -----------------------------------------------------

        logger.info(
            "pipeline_run_id=%s | "
            "STEP 1/4 | INGESTION | START",
            run_id,
        )

        download_file()

        logger.info(
            "pipeline_run_id=%s | "
            "STEP 1/4 | INGESTION | SUCCESS",
            run_id,
        )

        # -----------------------------------------------------
        # 2. RAW VALIDATION
        # -----------------------------------------------------

        logger.info(
            "pipeline_run_id=%s | "
            "STEP 2/4 | RAW VALIDATION | START",
            run_id,
        )

        raw_is_valid = validate_dataset(
            DATA_FILE
        )

        if not raw_is_valid:
            raise PipelineStepError(
                "RAW validation failed"
            )

        logger.info(
            "pipeline_run_id=%s | "
            "STEP 2/4 | RAW VALIDATION | SUCCESS",
            run_id,
        )

        # -----------------------------------------------------
        # 3. RAW -> SILVER
        # -----------------------------------------------------

        logger.info(
            "pipeline_run_id=%s | "
            "STEP 3/4 | RAW -> SILVER | START",
            run_id,
        )

        transform_raw_to_silver(
            run_id=run_id
        )

        logger.info(
            "pipeline_run_id=%s | "
            "STEP 3/4 | RAW -> SILVER | SUCCESS",
            run_id,
        )

        # -----------------------------------------------------
        # 4. SILVER VALIDATION
        # -----------------------------------------------------

        logger.info(
            "pipeline_run_id=%s | "
            "STEP 4/4 | SILVER VALIDATION | START",
            run_id,
        )

        silver_is_valid = validate_silver()

        if not silver_is_valid:
            raise PipelineStepError(
                "Silver validation failed"
            )

        logger.info(
            "pipeline_run_id=%s | "
            "STEP 4/4 | SILVER VALIDATION | SUCCESS",
            run_id,
        )

    except Exception:

        ended_at = utc_now()

        duration_seconds = (
            ended_at - started_at
        ).total_seconds()

        logger.exception(
            (
                "pipeline_run_id=%s | "
                "PIPELINE FAILED | duration=%.2fs"
            ),
            run_id,
            duration_seconds,
        )

        raise

    ended_at = utc_now()

    duration_seconds = (
        ended_at - started_at
    ).total_seconds()

    logger.info(
        (
            "pipeline_run_id=%s | "
            "PIPELINE SUCCESS | duration=%.2fs"
        ),
        run_id,
        duration_seconds,
    )


if __name__ == "__main__":
    run_pipeline()