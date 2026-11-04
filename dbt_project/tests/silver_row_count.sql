with expected as (
    select count(*) as eligible_raw_rows
    from {{ source('raw_taxi', 'yellow_trips') }}
    where trip_distance > 0 and fare_amount > 0
), actual as (
    select count(*) as silver_rows from {{ ref('stg_yellow_trips') }}
)
select * from expected cross join actual where eligible_raw_rows <> silver_rows
