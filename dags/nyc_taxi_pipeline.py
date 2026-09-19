from datetime import timedelta

from airflow.sdk import dag, get_current_context, task

from src.ingestion.download_nyc_taxi import download_file
from src.quality.validate_raw_nyc_taxi import DATA_FILE, validate_dataset
from src.transformation.raw_to_silver_nyc_taxi import (
    transform_raw_to_silver,
)

from src.quality.validate_silver_nyc_taxi import (
    validate_silver as validate_silver_dataset,
)


@dag(
    dag_id="nyc_taxi_pipeline",
    schedule=None,
    catchup=False,
    tags=["portfolio", "nyc-taxi"],
)
def nyc_taxi_pipeline():
    """
    Pipeline de Engenharia de Dados para NYC Yellow Taxi.

    Fluxo atual:
    1. Ingestão do arquivo Parquet para a camada RAW.
    2. Validação técnica e observacional da camada RAW.
    3. Transformação RAW -> Silver com quarantine e auditoria.
    """

    @task(
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    def ingest_nyc_taxi():
        """
        Executa a ingestão idempotente do arquivo Parquet
        para a camada RAW.
        """
        download_file()

    @task
    def validate_raw():
        """
        Valida o contrato técnico da camada RAW.

        Regras classificadas como WARN são observacionais.
        Uma regra bloqueante com FAIL interrompe a pipeline.
        """
        success = validate_dataset(DATA_FILE)

        if not success:
            raise RuntimeError(
                "A validação bloqueante da camada RAW falhou."
            )

    @task
    def raw_to_silver():
        """
        Transforma os dados RAW em Silver e direciona
        registros inválidos para quarantine.
        """

        # Reutilizamos o run_id do Airflow para correlacionar
        # a execução da DAG com nossos logs e registros de auditoria.
        context = get_current_context()
        airflow_run_id = context["ti"].run_id

        transform_raw_to_silver(
            run_id=airflow_run_id,
        )

    @task
    def validate_silver():
        """
        Valida o contrato e a integridade da camada Silver.

        Qualquer falha bloqueante interrompe a pipeline.
        """
        success = validate_silver_dataset()

        if not success:
            raise RuntimeError(
                "A validação da camada Silver falhou."
            )

    ingestion = ingest_nyc_taxi()
    raw_validation = validate_raw()
    silver_transformation = raw_to_silver()
    silver_validation = validate_silver()

    ingestion >> raw_validation >> silver_transformation >> silver_validation


nyc_taxi_pipeline()