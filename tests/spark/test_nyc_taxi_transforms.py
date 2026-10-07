from datetime import datetime

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.spark.nyc_taxi_transforms import (
    add_bronze_lineage,
    transform_silver,
)


SCHEMA = StructType(
    [
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", LongType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("_source_file", StringType(), True),
    ]
)


def _create_source_df(spark):
    rows = [
        # Registro válido.
        (
            1,
            datetime(2026, 1, 10, 10, 0, 0),
            datetime(2026, 1, 10, 10, 15, 0),
            1,
            3.5,
            15.0,
            18.0,
            "yellow_tripdata_2026-01.parquet",
        ),

        # Pickup fora do período esperado -> quarantine.
        (
            1,
            datetime(2025, 12, 31, 23, 59, 0),
            datetime(2026, 1, 1, 0, 10, 0),
            1,
            2.0,
            10.0,
            12.0,
            "yellow_tripdata_2026-01.parquet",
        ),

        # Dropoff anterior ao pickup -> quarantine.
        (
            2,
            datetime(2026, 1, 17, 19, 30, 0),
            datetime(2026, 1, 17, 19, 18, 0),
            1,
            4.0,
            20.0,
            24.0,
            "yellow_tripdata_2026-01.parquet",
        ),

        # Anomalias semânticas não bloqueantes -> permanece na Silver.
        (
            2,
            datetime(2026, 1, 20, 8, 0, 0),
            datetime(2026, 1, 20, 8, 5, 0),
            None,
            0.0,
            -5.0,
            -7.0,
            "yellow_tripdata_2026-01.parquet",
        ),
    ]

    return spark.createDataFrame(rows, schema=SCHEMA)


def test_add_bronze_lineage_adds_source_file(spark):
    source = spark.createDataFrame(
        [(1,), (2,)],
        ["id"],
    )

    result = add_bronze_lineage(
        source,
        "yellow_tripdata_2026-01.parquet",
    )

    source_files = {
        row["_source_file"]
        for row in result.select("_source_file").distinct().collect()
    }

    assert source_files == {"yellow_tripdata_2026-01.parquet"}


def test_add_bronze_lineage_rejects_empty_source_file(spark):
    source = spark.createDataFrame([(1,)], ["id"])

    with pytest.raises(ValueError):
        add_bronze_lineage(source, "   ")


def test_transform_silver_reconciles_valid_and_quarantine_rows(spark):
    bronze = _create_source_df(spark)

    silver, quarantine = transform_silver(
        bronze,
        period_start=datetime(2026, 1, 1),
        period_end=datetime(2026, 2, 1),
    )

    bronze_count = bronze.count()
    silver_count = silver.count()
    quarantine_count = quarantine.count()

    assert bronze_count == 4
    assert silver_count == 2
    assert quarantine_count == 2
    assert bronze_count == silver_count + quarantine_count


def test_transform_silver_assigns_expected_quarantine_reasons(spark):
    bronze = _create_source_df(spark)

    _, quarantine = transform_silver(
        bronze,
        period_start=datetime(2026, 1, 1),
        period_end=datetime(2026, 2, 1),
    )

    reasons = {
        row["_quarantine_reason"]
        for row in quarantine.select("_quarantine_reason").collect()
    }

    assert reasons == {
        "pickup_outside_period",
        "dropoff_before_pickup",
    }


def test_transform_silver_preserves_semantic_anomalies_as_flags(spark):
    bronze = _create_source_df(spark)

    silver, _ = transform_silver(
        bronze,
        period_start=datetime(2026, 1, 1),
        period_end=datetime(2026, 2, 1),
    )

    anomalous_row = (
        silver
        .filter("is_zero_distance = true")
        .select(
            "is_zero_distance",
            "has_negative_fare",
            "has_negative_total",
            "is_passenger_count_null",
            "trip_duration_minutes",
        )
        .first()
    )

    assert anomalous_row["is_zero_distance"] is True
    assert anomalous_row["has_negative_fare"] is True
    assert anomalous_row["has_negative_total"] is True
    assert anomalous_row["is_passenger_count_null"] is True
    assert anomalous_row["trip_duration_minutes"] == pytest.approx(5.0)


def test_transform_silver_normalizes_column_names(spark):
    bronze = _create_source_df(spark)

    silver, quarantine = transform_silver(
        bronze,
        period_start=datetime(2026, 1, 1),
        period_end=datetime(2026, 2, 1),
    )

    assert "vendorid" in silver.columns
    assert "VendorID" not in silver.columns

    assert "_quarantine_reason" in quarantine.columns


def test_transform_silver_rejects_invalid_period(spark):
    bronze = _create_source_df(spark)

    with pytest.raises(ValueError):
        transform_silver(
            bronze,
            period_start=datetime(2026, 2, 1),
            period_end=datetime(2026, 1, 1),
        )


def test_transform_silver_quarantines_missing_timestamps(spark):
    rows = [
        (
            1,
            None,
            datetime(2026, 1, 10, 10, 15, 0),
            1,
            3.5,
            15.0,
            18.0,
            "yellow_tripdata_2026-01.parquet",
        ),
        (
            1,
            datetime(2026, 1, 10, 10, 0, 0),
            None,
            1,
            3.5,
            15.0,
            18.0,
            "yellow_tripdata_2026-01.parquet",
        ),
    ]

    bronze = spark.createDataFrame(rows, schema=SCHEMA)

    silver, quarantine = transform_silver(
        bronze,
        period_start=datetime(2026, 1, 1),
        period_end=datetime(2026, 2, 1),
    )

    reasons = {
        row["_quarantine_reason"]
        for row in quarantine.select("_quarantine_reason").collect()
    }

    assert silver.count() == 0
    assert quarantine.count() == 2
    assert reasons == {
        "missing_pickup_datetime",
        "missing_dropoff_datetime",
    }


def test_transform_silver_does_not_expose_internal_rule_columns(spark):
    bronze = _create_source_df(spark)

    silver, quarantine = transform_silver(
        bronze,
        period_start=datetime(2026, 1, 1),
        period_end=datetime(2026, 2, 1),
    )

    internal_columns = {
        "_is_missing_pickup_datetime",
        "_is_missing_dropoff_datetime",
        "_is_pickup_outside_period",
        "_is_dropoff_before_pickup",
    }

    assert internal_columns.isdisjoint(silver.columns)
    assert internal_columns.isdisjoint(quarantine.columns)
    assert "_quarantine_reason" in quarantine.columns


def test_transform_silver_rejects_case_insensitive_column_collisions(spark):
    # select permite criar explicitamente duas colunas que diferem
    # apenas por capitalização. withColumn não serve para este teste
    # porque a resolução padrão do Spark é case-insensitive e substituiria
    # VendorID ao adicionar vendorid.
    bronze = (
        _create_source_df(spark)
        .select(
            "*",
            F.lit(999).alias("vendorid"),
        )
    )

    # Garante que o próprio fixture realmente representa a colisão
    # que queremos testar.
    assert "VendorID" in bronze.columns
    assert "vendorid" in bronze.columns

    with pytest.raises(
        ValueError,
        match="colunas duplicadas: vendorid",
    ):
        transform_silver(
            bronze,
            period_start=datetime(2026, 1, 1),
            period_end=datetime(2026, 2, 1),
        )
