select *
from {{ ref('mart_daily_taxi_metrics') }}
where pickup_date < date '2026-01-01'
   or pickup_date >= date '2026-02-01'
