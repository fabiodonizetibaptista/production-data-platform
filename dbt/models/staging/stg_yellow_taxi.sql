with source as (

    select *
    from {{ source('silver', 'yellow_taxi') }}

),

renamed as (

    select
        cast(vendorid as integer) as vendor_id,

        tpep_pickup_datetime as pickup_at,
        tpep_dropoff_datetime as dropoff_at,

        cast(passenger_count as integer) as passenger_count,
        cast(trip_distance as double) as trip_distance_miles,

        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,

        cast(payment_type as integer) as payment_type,

        cast(fare_amount as double) as fare_amount,
        cast(total_amount as double) as total_amount,

        cast(trip_duration_minutes as double) as trip_duration_minutes,

        cast(is_zero_distance as boolean) as is_zero_distance,
        cast(has_negative_fare as boolean) as has_negative_fare,
        cast(has_negative_total as boolean) as has_negative_total,
        cast(is_passenger_count_null as boolean) as is_passenger_count_null,

        _source_file

    from source

)

select *
from renamed
