with raw_data as (
    select * from {{ source('raw_taxi', 'yellow_trips') }}
)

select
    vendorid,
    tpep_pickup_datetime as pickup_datetime,
    tpep_dropoff_datetime as dropoff_datetime,

    -- Extract the date for grouping
    cast(tpep_pickup_datetime as date) as pickup_date,

    -- Get the day of the week (Trino syntax)
    day_of_week(cast(tpep_pickup_datetime as date)) as pickup_day_of_week,

    -- Calculate duration in seconds (Trino syntax)
    date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) as trip_duration_seconds,

    passenger_count,
    trip_distance,
    payment_type,
    fare_amount,
    tip_amount,
    total_amount
from raw_data
where trip_distance > 0
  and fare_amount > 0