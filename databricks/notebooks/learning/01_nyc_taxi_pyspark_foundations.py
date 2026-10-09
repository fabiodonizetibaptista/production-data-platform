# Databricks notebook source
print(f"Spark version: {spark.version}")

spark.sql("""
    SELECT
        current_catalog() AS current_catalog,
        current_schema() AS current_schema
""").show(truncate=False)

df_test = spark.range(5)

df_test.show()
df_test.printSchema()

# COMMAND ----------

schemas = [
    "nyc_taxi_bronze",
    "nyc_taxi_silver",
    "nyc_taxi_gold",
]

for schema in schemas:
    spark.sql(
        f"""
        CREATE SCHEMA IF NOT EXISTS workspace.{schema}
        COMMENT 'Schema da arquitetura Medallion do projeto NYC Taxi'
        """
    )

print("Schemas criados.")


# COMMAND ----------

spark.sql("""
    CREATE VOLUME IF NOT EXISTS
    workspace.nyc_taxi_bronze.landing

    COMMENT 'Landing zone para arquivos brutos do NYC Taxi'
""")

print("Volume criado.")

# COMMAND ----------

spark.sql("""
    SHOW SCHEMAS IN workspace
""").filter(
    "databaseName LIKE 'nyc_taxi_%'"
).show(truncate=False)

spark.sql("""
    SHOW VOLUMES IN workspace.nyc_taxi_bronze
""").show(truncate=False)

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC curl -L \
# MAGIC   "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet" \
# MAGIC   -o "/Volumes/workspace/nyc_taxi_bronze/landing/yellow_tripdata_2026-01.parquet"
# MAGIC
# MAGIC ls -lh /Volumes/workspace/nyc_taxi_bronze/landing/

# COMMAND ----------

raw_path = (
    "/Volumes/workspace/nyc_taxi_bronze/landing/"
    "yellow_tripdata_2026-01.parquet"
)

df_raw = spark.read.parquet(raw_path)

print(f"Partitions: {df_raw.rdd.getNumPartitions() if hasattr(df_raw, 'rdd') else 'Spark Connect'}")

df_raw.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

df_raw.select(
    F.count("*").alias("row_count"),
    F.min("tpep_pickup_datetime").alias("min_pickup"),
    F.max("tpep_pickup_datetime").alias("max_pickup"),
).show(truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F


df_january = (
    df_raw
    .filter(
        (F.col("tpep_pickup_datetime") >= F.lit("2026-01-01 00:00:00"))
        & (F.col("tpep_pickup_datetime") < F.lit("2026-02-01 00:00:00"))
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
        )
    )
    .withColumn(
        "is_zero_distance",
        F.col("trip_distance") == 0
    )
    .withColumn(
        "has_negative_fare",
        F.col("fare_amount") < 0
    )
    .withColumn(
        "has_negative_total",
        F.col("total_amount") < 0
    )
)

# COMMAND ----------

df_january.explain("formatted")

# COMMAND ----------

df_january.agg(
    F.count("*").alias("january_rows"),
    F.sum(
        F.when(F.col("is_zero_distance"), 1).otherwise(0)
    ).alias("zero_distance_rows"),
    F.sum(
        F.when(F.col("has_negative_fare"), 1).otherwise(0)
    ).alias("negative_fare_rows"),
    F.sum(
        F.when(F.col("has_negative_total"), 1).otherwise(0)
    ).alias("negative_total_rows"),
).show()

# COMMAND ----------

df_metrics = (
    df_raw
    .select(
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "fare_amount",
        "total_amount",
    )
    .filter(
        (F.col("tpep_pickup_datetime") >= F.lit("2026-01-01 00:00:00"))
        & (F.col("tpep_pickup_datetime") < F.lit("2026-02-01 00:00:00"))
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
        )
    )
    .withColumn(
        "is_zero_distance",
        F.col("trip_distance") == 0
    )
    .withColumn(
        "has_negative_fare",
        F.col("fare_amount") < 0
    )
    .withColumn(
        "has_negative_total",
        F.col("total_amount") < 0
    )
)

df_metrics.explain("formatted")

# COMMAND ----------

df_metrics.groupBy(
    F.to_date("tpep_pickup_datetime").alias("pickup_date")
).agg(
    F.count("*").alias("trip_count"),
    F.sum("trip_distance").alias("total_distance"),
    F.avg("trip_duration_minutes").alias("avg_duration_minutes"),
).explain("formatted")

# COMMAND ----------

df_daily = (
    df_metrics
    .groupBy(
        F.to_date("tpep_pickup_datetime").alias("pickup_date")
    )
    .agg(
        F.count("*").alias("trip_count"),
        F.sum("trip_distance").alias("total_distance"),
        F.avg("trip_duration_minutes").alias("avg_duration_minutes"),
    )
)

# COMMAND ----------

df_daily.orderBy("pickup_date").show(31, truncate=False)

df_daily.explain("formatted")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast


# Fato: milhões de corridas.
df_trips = (
    df_raw
    .filter(
        (F.col("tpep_pickup_datetime") >= F.lit("2026-01-01 00:00:00"))
        & (F.col("tpep_pickup_datetime") < F.lit("2026-02-01 00:00:00"))
    )
    .select(
        F.col("PULocationID").alias("pickup_location_id"),
        "total_amount",
    )
)


# Pequena dimensão técnica apenas para estudarmos o comportamento do join.
df_locations = (
    spark.range(1, 266)
    .select(
        F.col("id").cast("int").alias("location_id"),
        F.concat(
            F.lit("zone_"),
            F.col("id")
        ).alias("zone_label"),
    )
)

print("Trips:")
df_trips.printSchema()

print("Locations:")
df_locations.show(5, truncate=False)

# COMMAND ----------

df_shuffle_join = (
    df_trips.hint("merge")
    .join(
        df_locations.hint("merge"),
        df_trips.pickup_location_id == df_locations.location_id,
        "left",
    )
)

df_shuffle_join.explain("formatted")

# COMMAND ----------

df_broadcast_join = (
    df_trips
    .join(
        broadcast(df_locations),
        df_trips.pickup_location_id == df_locations.location_id,
        "left",
    )
)

df_broadcast_join.explain("formatted")