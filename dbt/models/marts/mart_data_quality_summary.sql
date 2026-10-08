with daily as (

    select *
    from {{ ref('mart_daily_taxi_metrics') }}

),

quality_summary as (

    select
        cast(date_trunc('month', pickup_date) as date) as pickup_month,
        'Missing passenger count' as quality_issue,
        1 as sort_order,
        sum(missing_passenger_count_trip_count) as issue_count
    from daily
    group by cast(date_trunc('month', pickup_date) as date)

    union all

    select
        cast(date_trunc('month', pickup_date) as date) as pickup_month,
        'Zero distance' as quality_issue,
        2 as sort_order,
        sum(zero_distance_trip_count) as issue_count
    from daily
    group by cast(date_trunc('month', pickup_date) as date)

    union all

    select
        cast(date_trunc('month', pickup_date) as date) as pickup_month,
        'Negative total amount' as quality_issue,
        3 as sort_order,
        sum(negative_total_trip_count) as issue_count
    from daily
    group by cast(date_trunc('month', pickup_date) as date)

    union all

    select
        cast(date_trunc('month', pickup_date) as date) as pickup_month,
        'Negative fare' as quality_issue,
        4 as sort_order,
        sum(negative_fare_trip_count) as issue_count
    from daily
    group by cast(date_trunc('month', pickup_date) as date)

)

select *
from quality_summary
