-- Rounded floating results allow only sub-cent numerical noise (1e-6).
-- NULL is preserved: SUM/AVG over all-NULL inputs is not zero.
with expected as (
    select pickup_date, pickup_day_of_week,
        count(*) as total_trips,
        sum(passenger_count) as total_passengers,
        round(avg(trip_distance), 2) as avg_trip_distance,
        round(avg(trip_duration_seconds / 60.0), 2) as avg_trip_duration_minutes,
        round(sum(fare_amount), 2) as total_fare,
        round(sum(tip_amount), 2) as total_tips,
        round(sum(total_amount), 2) as total_revenue,
        round(avg(total_amount), 2) as avg_revenue_per_trip,
        1 as expected_present
    from {{ ref('stg_yellow_trips') }}
    group by pickup_date, pickup_day_of_week
), actual as (
    select *, 1 as actual_present from {{ ref('fct_daily_trips') }}
), compared as (
    select e.*, a.pickup_date as actual_pickup_date,
        a.pickup_day_of_week as actual_pickup_day_of_week, a.actual_present,
        a.total_trips as actual_total_trips, a.total_passengers as actual_total_passengers, a.avg_trip_distance as actual_avg_trip_distance, a.avg_trip_duration_minutes as actual_avg_trip_duration_minutes, a.total_fare as actual_total_fare, a.total_tips as actual_total_tips, a.total_revenue as actual_total_revenue, a.avg_revenue_per_trip as actual_avg_revenue_per_trip
    from expected e full outer join actual a
      on e.pickup_date is not distinct from a.pickup_date
     and e.pickup_day_of_week is not distinct from a.pickup_day_of_week
)
select pickup_date, pickup_day_of_week, actual_pickup_date, actual_pickup_day_of_week, 'group_presence' as mismatched_metric, cast(expected_present as varchar) as expected_value, cast(actual_present as varchar) as actual_value from compared where expected_present is null or actual_present is null
union all
select pickup_date, pickup_day_of_week, actual_pickup_date, actual_pickup_day_of_week, 'total_trips', cast(total_trips as varchar), cast(actual_total_trips as varchar) from compared where expected_present = 1 and actual_present = 1 and (total_trips is distinct from actual_total_trips)
union all
select pickup_date, pickup_day_of_week, actual_pickup_date, actual_pickup_day_of_week, 'total_passengers', cast(total_passengers as varchar), cast(actual_total_passengers as varchar) from compared where expected_present = 1 and actual_present = 1 and (total_passengers is distinct from actual_total_passengers)
union all
select pickup_date, pickup_day_of_week, actual_pickup_date, actual_pickup_day_of_week, 'avg_trip_distance', cast(avg_trip_distance as varchar), cast(actual_avg_trip_distance as varchar) from compared where expected_present = 1 and actual_present = 1 and (not coalesce((avg_trip_distance is null and actual_avg_trip_distance is null) or (avg_trip_distance is not null and actual_avg_trip_distance is not null and (avg_trip_distance = actual_avg_trip_distance or abs(avg_trip_distance - actual_avg_trip_distance) <= 0.000001)), false))
union all
select pickup_date, pickup_day_of_week, actual_pickup_date, actual_pickup_day_of_week, 'avg_trip_duration_minutes', cast(avg_trip_duration_minutes as varchar), cast(actual_avg_trip_duration_minutes as varchar) from compared where expected_present = 1 and actual_present = 1 and (not coalesce((avg_trip_duration_minutes is null and actual_avg_trip_duration_minutes is null) or (avg_trip_duration_minutes is not null and actual_avg_trip_duration_minutes is not null and (avg_trip_duration_minutes = actual_avg_trip_duration_minutes or abs(avg_trip_duration_minutes - actual_avg_trip_duration_minutes) <= 0.000001)), false))
union all
select pickup_date, pickup_day_of_week, actual_pickup_date, actual_pickup_day_of_week, 'total_fare', cast(total_fare as varchar), cast(actual_total_fare as varchar) from compared where expected_present = 1 and actual_present = 1 and (not coalesce((total_fare is null and actual_total_fare is null) or (total_fare is not null and actual_total_fare is not null and (total_fare = actual_total_fare or abs(total_fare - actual_total_fare) <= 0.000001)), false))
union all
select pickup_date, pickup_day_of_week, actual_pickup_date, actual_pickup_day_of_week, 'total_tips', cast(total_tips as varchar), cast(actual_total_tips as varchar) from compared where expected_present = 1 and actual_present = 1 and (not coalesce((total_tips is null and actual_total_tips is null) or (total_tips is not null and actual_total_tips is not null and (total_tips = actual_total_tips or abs(total_tips - actual_total_tips) <= 0.000001)), false))
union all
select pickup_date, pickup_day_of_week, actual_pickup_date, actual_pickup_day_of_week, 'total_revenue', cast(total_revenue as varchar), cast(actual_total_revenue as varchar) from compared where expected_present = 1 and actual_present = 1 and (not coalesce((total_revenue is null and actual_total_revenue is null) or (total_revenue is not null and actual_total_revenue is not null and (total_revenue = actual_total_revenue or abs(total_revenue - actual_total_revenue) <= 0.000001)), false))
union all
select pickup_date, pickup_day_of_week, actual_pickup_date, actual_pickup_day_of_week, 'avg_revenue_per_trip', cast(avg_revenue_per_trip as varchar), cast(actual_avg_revenue_per_trip as varchar) from compared where expected_present = 1 and actual_present = 1 and (not coalesce((avg_revenue_per_trip is null and actual_avg_revenue_per_trip is null) or (avg_revenue_per_trip is not null and actual_avg_revenue_per_trip is not null and (avg_revenue_per_trip = actual_avg_revenue_per_trip or abs(avg_revenue_per_trip - actual_avg_revenue_per_trip) <= 0.000001)), false))
