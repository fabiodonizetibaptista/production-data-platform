with staging as (

    select count(*) as row_count
    from {{ ref('stg_yellow_taxi') }}

),

mart as (

    select sum(trip_count) as row_count
    from {{ ref('mart_daily_taxi_metrics') }}

)

select
    staging.row_count as staging_row_count,
    mart.row_count as mart_row_count

from staging
cross join mart

where staging.row_count <> mart.row_count
