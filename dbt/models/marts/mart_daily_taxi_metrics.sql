with trips as (

    select *
    from {{ ref('int_yellow_taxi_trips_enriched') }}

),

daily as (

    select
        pickup_date,

        count(*) as trip_count,

        count(passenger_count) as trips_with_passenger_count,

        sum(passenger_count) as known_passenger_count,

        sum(trip_distance_miles) as total_distance_miles,

        avg(trip_distance_miles) as average_trip_distance_miles,

        avg(trip_duration_minutes) as average_trip_duration_minutes,

        sum(fare_amount) as total_fare_amount,

        sum(total_amount) as total_amount,

        sum(
            case when is_zero_distance then 1 else 0 end
        ) as zero_distance_trip_count,

        sum(
            case when has_negative_fare then 1 else 0 end
        ) as negative_fare_trip_count,

        sum(
            case when has_negative_total then 1 else 0 end
        ) as negative_total_trip_count,

        sum(
            case when is_passenger_count_null then 1 else 0 end
        ) as missing_passenger_count_trip_count

    from trips

    group by pickup_date

)

select *
from daily
