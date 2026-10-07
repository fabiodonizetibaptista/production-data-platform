"""Transformações PySpark do pipeline NYC Yellow Taxi.

Este módulo contém somente lógica de transformação de DataFrames.

Decisões intencionais:
- não conhece caminhos de arquivos;
- não conhece nomes de tabelas do Unity Catalog;
- não cria SparkSession;
- não executa writes;
- não depende diretamente do Databricks.

Isso permite reutilizar e testar as regras independentemente
do ambiente responsável pela execução.
"""

from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_bronze_lineage(
    source_df: DataFrame,
    source_file: str,
) -> DataFrame:
    """Adiciona metadado mínimo de linhagem à camada Bronze."""

    if not source_file or not source_file.strip():
        raise ValueError("source_file não pode ser vazio.")

    return source_df.withColumn(
        "_source_file",
        F.lit(source_file),
    )


def _lowercase_columns(df: DataFrame) -> DataFrame:
    """Padroniza os nomes das colunas sem alterar seus valores."""

    return df.select(
        *[
            F.col(f"`{column}`").alias(column.lower())
            for column in df.columns
        ]
    )


def transform_silver(
    bronze_df: DataFrame,
    period_start: datetime,
    period_end: datetime,
) -> tuple[DataFrame, DataFrame]:
    """Transforma Bronze e separa registros válidos e quarantine.

    Regras bloqueantes:
    - pickup fora do período esperado;
    - dropoff anterior ao pickup.

    Anomalias semânticas não são removidas. Elas são preservadas
    na Silver por meio de flags para análise posterior.

    Returns:
        tuple[DataFrame, DataFrame]:
            silver_df, quarantine_df.
    """

    if period_start >= period_end:
        raise ValueError(
            "period_start deve ser anterior a period_end."
        )

    # Timestamps são campos estruturais da viagem. Ausência de pickup ou
    # dropoff impede validar período e duração de forma confiável.
    missing_pickup_datetime = F.col("tpep_pickup_datetime").isNull()
    missing_dropoff_datetime = F.col("tpep_dropoff_datetime").isNull()

    pickup_outside_period = (
        (~missing_pickup_datetime)
        & (
            (F.col("tpep_pickup_datetime") < F.lit(period_start))
            | (F.col("tpep_pickup_datetime") >= F.lit(period_end))
        )
    )

    dropoff_before_pickup = (
        (~missing_pickup_datetime)
        & (~missing_dropoff_datetime)
        & (
            F.col("tpep_dropoff_datetime")
            < F.col("tpep_pickup_datetime")
        )
    )

    prepared_df = (
        bronze_df
        .withColumn(
            "_is_missing_pickup_datetime",
            missing_pickup_datetime,
        )
        .withColumn(
            "_is_missing_dropoff_datetime",
            missing_dropoff_datetime,
        )
        .withColumn(
            "_is_pickup_outside_period",
            F.coalesce(
                pickup_outside_period,
                F.lit(False),
            ),
        )
        .withColumn(
            "_is_dropoff_before_pickup",
            F.coalesce(
                dropoff_before_pickup,
                F.lit(False),
            ),
        )
        .withColumn(
            "trip_duration_minutes",
            F.expr(
                """
                timestampdiff(
                    SECOND,
                    tpep_pickup_datetime,
                    tpep_dropoff_datetime
                ) / 60.0
                """
            ),
        )
        .withColumn(
            "is_zero_distance",
            F.col("trip_distance") == 0,
        )
        .withColumn(
            "has_negative_fare",
            F.col("fare_amount") < 0,
        )
        .withColumn(
            "has_negative_total",
            F.col("total_amount") < 0,
        )
        .withColumn(
            "is_passenger_count_null",
            F.col("passenger_count").isNull(),
        )
    )

    quarantine_condition = (
        F.col("_is_missing_pickup_datetime")
        | F.col("_is_missing_dropoff_datetime")
        | F.col("_is_pickup_outside_period")
        | F.col("_is_dropoff_before_pickup")
    )

    internal_rule_columns = [
        "_is_missing_pickup_datetime",
        "_is_missing_dropoff_datetime",
        "_is_pickup_outside_period",
        "_is_dropoff_before_pickup",
    ]

    silver_df = (
        prepared_df
        .filter(~quarantine_condition)
        .drop(*internal_rule_columns)
    )

    quarantine_df = (
        prepared_df
        .filter(quarantine_condition)
        .withColumn(
            "_quarantine_reason",
            F.concat_ws(
                ";",
                F.when(
                    F.col("_is_missing_pickup_datetime"),
                    F.lit("missing_pickup_datetime"),
                ),
                F.when(
                    F.col("_is_missing_dropoff_datetime"),
                    F.lit("missing_dropoff_datetime"),
                ),
                F.when(
                    F.col("_is_pickup_outside_period"),
                    F.lit("pickup_outside_period"),
                ),
                F.when(
                    F.col("_is_dropoff_before_pickup"),
                    F.lit("dropoff_before_pickup"),
                ),
            ),
        )
        .drop(*internal_rule_columns)
    )

    return (
        _lowercase_columns(silver_df),
        _lowercase_columns(quarantine_df),
    )
