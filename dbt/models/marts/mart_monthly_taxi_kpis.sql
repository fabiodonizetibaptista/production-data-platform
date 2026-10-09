with daily as (

    select *
    from {{ ref('mart_daily_taxi_metrics') }}

),

monthly as (

    select
        cast(date_trunc('month', pickup_date) as date) as pickup_month,

        sum(trip_count) as total_trips,

        sum(total_amount) as total_amount,

        sum(total_distance_miles) as total_distance_miles,

        case
            when sum(trip_count) > 0
            then sum(total_amount) / sum(trip_count)
            else null
        end as average_amount_per_trip

    from daily

    group by
        cast(date_trunc('month', pickup_date) as date)

)

select *
from monthly
