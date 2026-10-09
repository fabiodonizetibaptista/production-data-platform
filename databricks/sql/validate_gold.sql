-- Validação consolidada da camada Gold do NYC Taxi.
-- Este SQL comprova o contrato analítico produzido pelo dbt no Databricks.

SELECT
    COUNT(*) AS day_count,
    SUM(trip_count) AS total_trip_count,

    MIN(pickup_date) AS min_pickup_date,
    MAX(pickup_date) AS max_pickup_date,

    SUM(zero_distance_trip_count) AS zero_distance_trip_count,
    SUM(negative_fare_trip_count) AS negative_fare_trip_count,
    SUM(negative_total_trip_count) AS negative_total_trip_count,
    SUM(missing_passenger_count_trip_count) AS missing_passenger_count_trip_count

FROM workspace.nyc_taxi_gold.mart_daily_taxi_metrics
