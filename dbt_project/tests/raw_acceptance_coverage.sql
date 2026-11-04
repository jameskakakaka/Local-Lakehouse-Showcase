{{ config(enabled=(target.name == 'local_acceptance' and var('dq_local_acceptance', false)), tags=['local_acceptance_only']) }}
-- Only for the single-file, zero-based first-10,000-occurrences prototype.
with coverage as (
    select count(*) as row_count,
           count(distinct source_file_sha256) as source_files,
           count(distinct source_row_number) as distinct_positions,
           min(source_row_number) as first_position,
           max(source_row_number) as last_position
    from {{ source('raw_taxi', 'yellow_trips') }}
)
select * from coverage
where row_count <> 10000 or source_files <> 1 or distinct_positions <> 10000
   or first_position is distinct from 0 or last_position is distinct from 9999
