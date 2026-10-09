# NYC Taxi Operations - January 2026

Published Databricks AI/BI dashboard consuming the Gold analytical layer:

- `workspace.nyc_taxi_gold.mart_daily_taxi_metrics`
- `workspace.nyc_taxi_gold.mart_monthly_taxi_kpis`
- `workspace.nyc_taxi_gold.mart_data_quality_summary`

## Dashboard scope

The dashboard is intentionally scoped to January 2026.

KPIs:
- Total Trips
- Total Amount
- Total Distance (miles)
- Average Amount per Trip

Business trends:
- Daily Trips
- Daily Average Amount per Trip

Data quality:
- Missing passenger count
- Zero distance
- Negative total amount
- Negative fare

## Gold model grains

### `mart_daily_taxi_metrics`

Grain: one row per `pickup_date`.

Provides the daily metrics used by the trend visualizations.

### `mart_monthly_taxi_kpis`

Grain: one row per `pickup_month`.

Provides the monthly executive KPIs, including the already-calculated
`average_amount_per_trip`.

The `Average Amount per Trip` counter uses
`MAX(average_amount_per_trip)` only as the visualization aggregation required
by Databricks AI/BI. Because this dashboard is scoped to January 2026 and the
Gold mart contains one row per month, the calculation does not redefine the
business metric in the BI layer.

### `mart_data_quality_summary`

Grain: one row per `pickup_month` and `quality_issue`.

Provides the monthly data-quality counts displayed in the dashboard.

## Semantic-layer principle

Business metric definitions are implemented in the dbt Gold layer.

The Databricks AI/BI dashboard is intentionally kept as a thin consumption
layer and does not redefine business calculations that already exist in Gold.

## Databricks catalog binding

The dashboard currently references the `workspace` Unity Catalog explicitly.

This is intentional for the current Databricks environment used by this
project. Promotion to multiple isolated Databricks environments would require
catalog rebinding or parameterization as part of the deployment strategy.

## Published dashboard

https://dbc-d7715295-e2d4.cloud.databricks.com/dashboardsv3/01f1c283e83a1c73a7bf01a8350ae767/published?o=7474646281512184

> The dashboard may require authentication to the Databricks workspace.
