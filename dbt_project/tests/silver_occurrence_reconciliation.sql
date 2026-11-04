-- Compare multisets: legitimate equal-valued occurrences retain their multiplicity.
with expected as (
    select vendorid,
        tpep_pickup_datetime as pickup_datetime,
        tpep_dropoff_datetime as dropoff_datetime,
        cast(tpep_pickup_datetime as date) as pickup_date,
        day_of_week(cast(tpep_pickup_datetime as date)) as pickup_day_of_week,
        date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) as trip_duration_seconds,
        passenger_count, trip_distance, payment_type, fare_amount, tip_amount, total_amount
    from {{ source('raw_taxi', 'yellow_trips') }}
    where trip_distance > 0 and fare_amount > 0
), signed_occurrences as (
    select *, 1 as occurrence_delta from expected
    union all
    select vendorid, pickup_datetime, dropoff_datetime, pickup_date, pickup_day_of_week, trip_duration_seconds, passenger_count, trip_distance, payment_type, fare_amount, tip_amount, total_amount, -1 as occurrence_delta
    from {{ ref('stg_yellow_trips') }}
)
select vendorid, pickup_datetime, dropoff_datetime, pickup_date, pickup_day_of_week, trip_duration_seconds, passenger_count, trip_distance, payment_type, fare_amount, tip_amount, total_amount,
       sum(occurrence_delta) as missing_minus_extra_occurrences
from signed_occurrences
group by vendorid, pickup_datetime, dropoff_datetime, pickup_date, pickup_day_of_week, trip_duration_seconds, passenger_count, trip_distance, payment_type, fare_amount, tip_amount, total_amount
having sum(occurrence_delta) <> 0
