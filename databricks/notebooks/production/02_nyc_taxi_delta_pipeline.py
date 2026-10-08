# Databricks notebook source
# Configuração da pipeline

from datetime import datetime
import sys

from pyspark.sql import functions as F


RAW_PATH = (
    "/Volumes/workspace/nyc_taxi_bronze/landing/"
    "yellow_tripdata_2026-01.parquet"
)

BRONZE_TABLE = "workspace.nyc_taxi_bronze.yellow_taxi_trips"
SILVER_TABLE = "workspace.nyc_taxi_silver.yellow_taxi_trips"
QUARANTINE_TABLE = (
    "workspace.nyc_taxi_silver.yellow_taxi_trips_quarantine"
)

SOURCE_FILE = "yellow_tripdata_2026-01.parquet"

PERIOD_START = datetime(2026, 1, 1)
PERIOD_END = datetime(2026, 2, 1)


# Ambiente de desenvolvimento:
# o código local é sincronizado pelo Databricks CLI para a pasta
# pessoal do usuário no Workspace.
#
# O usuário é resolvido em runtime para evitar hardcode de e-mail.
workspace_user = spark.sql(
    "SELECT current_user() AS workspace_user"
).first()["workspace_user"]

PROJECT_ROOT = (
    f"/Workspace/Users/{workspace_user}/production-data-platform"
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# As regras de transformação ficam no módulo Python versionado e testado.
from src.spark.nyc_taxi_transforms import (
    add_bronze_lineage,
    transform_silver,
)


print(f"Source: {RAW_PATH}")
print(f"Bronze target: {BRONZE_TABLE}")
print(f"Silver target: {SILVER_TABLE}")
print(f"Quarantine target: {QUARANTINE_TABLE}")

# COMMAND ----------

df_source = spark.read.parquet(RAW_PATH)

df_bronze = add_bronze_lineage(
    source_df=df_source,
    source_file=SOURCE_FILE,
)

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)

print(f"Bronze criada com sucesso: {BRONZE_TABLE}")

# COMMAND ----------

spark.sql(f"""
    DESCRIBE DETAIL {BRONZE_TABLE}
""").select(
    "format",
    "name",
    "location",
    "numFiles",
    "sizeInBytes"
).show(truncate=False)


spark.sql(f"""
    SELECT
        COUNT(*) AS row_count,
        MIN(tpep_pickup_datetime) AS min_pickup,
        MAX(tpep_pickup_datetime) AS max_pickup,
        COUNT(DISTINCT _source_file) AS source_files
    FROM {BRONZE_TABLE}
""").show(truncate=False)

# COMMAND ----------

df_bronze_source = spark.table(BRONZE_TABLE)

df_silver, df_quarantine = transform_silver(
    bronze_df=df_bronze_source,
    period_start=PERIOD_START,
    period_end=PERIOD_END,
)

print("Transformação Silver/Quarantine concluída.")

# COMMAND ----------

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

(
    df_quarantine.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(QUARANTINE_TABLE)
)

print("Silver e Quarantine criadas.")

# COMMAND ----------

bronze_count = spark.table(BRONZE_TABLE).count()
silver_count = spark.table(SILVER_TABLE).count()
quarantine_count = spark.table(QUARANTINE_TABLE).count()

print(f"Bronze:     {bronze_count:,}")
print(f"Silver:     {silver_count:,}")
print(f"Quarantine: {quarantine_count:,}")
print(f"Reconciled: {bronze_count == silver_count + quarantine_count}")


spark.table(QUARANTINE_TABLE).select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "_quarantine_reason",
).orderBy(
    "tpep_pickup_datetime"
).show(20, truncate=False)

# COMMAND ----------

spark.sql(f"""
    SELECT
        COUNT(*) AS row_count,

        SUM(
            CASE
                WHEN tpep_pickup_datetime <
                     CAST('2026-01-01 00:00:00' AS TIMESTAMP_NTZ)
                  OR tpep_pickup_datetime >=
                     CAST('2026-02-01 00:00:00' AS TIMESTAMP_NTZ)
                THEN 1 ELSE 0
            END
        ) AS pickup_outside_period,

        SUM(
            CASE
                WHEN tpep_dropoff_datetime < tpep_pickup_datetime
                THEN 1 ELSE 0
            END
        ) AS dropoff_before_pickup,

        SUM(CASE WHEN is_zero_distance THEN 1 ELSE 0 END)
            AS zero_distance_rows,

        SUM(CASE WHEN has_negative_fare THEN 1 ELSE 0 END)
            AS negative_fare_rows,

        SUM(CASE WHEN has_negative_total THEN 1 ELSE 0 END)
            AS negative_total_rows,

        SUM(CASE WHEN is_passenger_count_null THEN 1 ELSE 0 END)
            AS passenger_count_null_rows

    FROM {SILVER_TABLE}
""").show(truncate=False)

# COMMAND ----------

spark.sql(f"""
    DESCRIBE HISTORY {SILVER_TABLE}
""").select(
    "version",
    "timestamp",
    "operation",
    "operationParameters",
    "operationMetrics"
).show(truncate=False)

# COMMAND ----------

spark.sql(f"""
    DESCRIBE HISTORY {QUARANTINE_TABLE}
""").select(
    "version",
    "timestamp",
    "operation",
    "operationMetrics"
).show(truncate=False)

# COMMAND ----------

# ============================================================
# CHECKPOINT DE VALIDAÇÃO — BRONZE -> SILVER -> QUARANTINE
# ============================================================

BRONZE_TABLE = "workspace.nyc_taxi_bronze.yellow_taxi_trips"
SILVER_TABLE = "workspace.nyc_taxi_silver.yellow_taxi_trips"
QUARANTINE_TABLE = "workspace.nyc_taxi_silver.yellow_taxi_trips_quarantine"


print("=" * 80)
print("1. RECONCILIAÇÃO E QUALIDADE")
print("=" * 80)

spark.sql(f"""
    SELECT
        b.bronze_count,
        s.silver_count,
        q.quarantine_count,

        b.bronze_count = s.silver_count + q.quarantine_count
            AS reconciled,

        s.pickup_outside_period,
        s.dropoff_before_pickup,

        s.zero_distance_rows,
        s.negative_fare_rows,
        s.negative_total_rows,
        s.passenger_count_null_rows

    FROM
        (
            SELECT COUNT(*) AS bronze_count
            FROM {BRONZE_TABLE}
        ) b

    CROSS JOIN
        (
            SELECT
                COUNT(*) AS silver_count,

                SUM(
                    CASE
                        WHEN tpep_pickup_datetime <
                             CAST('2026-01-01 00:00:00' AS TIMESTAMP_NTZ)
                          OR tpep_pickup_datetime >=
                             CAST('2026-02-01 00:00:00' AS TIMESTAMP_NTZ)
                        THEN 1 ELSE 0
                    END
                ) AS pickup_outside_period,

                SUM(
                    CASE
                        WHEN tpep_dropoff_datetime < tpep_pickup_datetime
                        THEN 1 ELSE 0
                    END
                ) AS dropoff_before_pickup,

                SUM(CASE WHEN is_zero_distance THEN 1 ELSE 0 END)
                    AS zero_distance_rows,

                SUM(CASE WHEN has_negative_fare THEN 1 ELSE 0 END)
                    AS negative_fare_rows,

                SUM(CASE WHEN has_negative_total THEN 1 ELSE 0 END)
                    AS negative_total_rows,

                SUM(CASE WHEN is_passenger_count_null THEN 1 ELSE 0 END)
                    AS passenger_count_null_rows

            FROM {SILVER_TABLE}
        ) s

    CROSS JOIN
        (
            SELECT COUNT(*) AS quarantine_count
            FROM {QUARANTINE_TABLE}
        ) q
""").show(truncate=False)


print("=" * 80)
print("2. MOTIVOS DE QUARANTINE")
print("=" * 80)

spark.sql(f"""
    SELECT
        _quarantine_reason,
        COUNT(*) AS row_count
    FROM {QUARANTINE_TABLE}
    GROUP BY _quarantine_reason
    ORDER BY _quarantine_reason
""").show(truncate=False)


print("=" * 80)
print("3. DELTA HISTORY — BRONZE")
print("=" * 80)

spark.sql(f"""
    DESCRIBE HISTORY {BRONZE_TABLE}
""").select(
    "version",
    "operation",
    "operationMetrics"
).show(truncate=False)


print("=" * 80)
print("4. DELTA HISTORY — SILVER")
print("=" * 80)

spark.sql(f"""
    DESCRIBE HISTORY {SILVER_TABLE}
""").select(
    "version",
    "operation",
    "operationMetrics"
).show(truncate=False)


print("=" * 80)
print("5. DELTA HISTORY — QUARANTINE")
print("=" * 80)

spark.sql(f"""
    DESCRIBE HISTORY {QUARANTINE_TABLE}
""").select(
    "version",
    "operation",
    "operationMetrics"
).show(truncate=False)
