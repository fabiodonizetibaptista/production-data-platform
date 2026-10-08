-- Databricks AI/BI consumption queries
--
-- Design principle:
-- Business and data-quality semantics are modeled in dbt Gold.
-- The BI layer should consume those marts with minimal logic.
--
-- Physical Gold objects:
--   workspace.nyc_taxi_gold.mart_monthly_taxi_kpis
--   workspace.nyc_taxi_gold.mart_daily_taxi_metrics
--   workspace.nyc_taxi_gold.mart_data_quality_summary


-- ============================================================
-- EXECUTIVE MONTHLY KPIs
-- Grain: one row per pickup month.
-- ============================================================

SELECT
    pickup_month,
    total_trips,
    total_amount,
    total_distance_miles,
    average_amount_per_trip
FROM workspace.nyc_taxi_gold.mart_monthly_taxi_kpis
ORDER BY pickup_month;


-- ============================================================
-- DAILY OPERATING TRENDS
-- Grain: one row per pickup date.
-- ============================================================

SELECT
    pickup_date,
    trip_count,
    average_amount_per_trip
FROM workspace.nyc_taxi_gold.mart_daily_taxi_metrics
ORDER BY pickup_date;


-- ============================================================
-- DATA QUALITY SUMMARY
-- Grain: one row per pickup month and quality issue.
-- ============================================================

SELECT
    pickup_month,
    quality_issue,
    sort_order,
    issue_count
FROM workspace.nyc_taxi_gold.mart_data_quality_summary
ORDER BY
    pickup_month,
    issue_count DESC;
