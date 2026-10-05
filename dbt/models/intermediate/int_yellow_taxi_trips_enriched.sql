with trips as (

    select *
    from {{ ref('stg_yellow_taxi') }}

),

enriched as (

    select
        *,

        cast(pickup_at as date) as pickup_date,

        extract(hour from pickup_at) as pickup_hour,

        case
            when trip_duration_minutes > 0
                 and trip_distance_miles > 0
            then
                trip_distance_miles
                / (trip_duration_minutes / 60.0)
            else null
        end as average_speed_mph

    from trips

)

select *
from enriched
