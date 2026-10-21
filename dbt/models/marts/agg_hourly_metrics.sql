{{ config(materialized='table') }}

with snapshots as (

    select * from {{ ref('fct_station_snapshots') }}

),

hourly as (

    select
        date_trunc('hour', snapshot_at)                          as metric_hour,
        date_trunc('hour', local_snapshot_at)                    as local_metric_hour,
        cast(extract(hour from local_snapshot_at) as integer)    as local_hour,
        global_station_id,
        city,
        station_id,
        any_value(timezone_name)                                 as timezone_name,
        any_value(station_name)                                  as station_name,
        any_value(region_id)                                     as region_id,
        any_value(latitude)                                      as latitude,
        any_value(longitude)                                     as longitude,
        any_value(capacity)                                      as capacity,
        count(*)                                                 as snapshot_count,
        avg(utilization_rate)                                    as avg_utilization_rate,
        avg(num_bikes_available)                                 as avg_bikes_available,
        avg(num_docks_available)                                 as avg_docks_available,
        min(num_bikes_available)                                 as min_bikes_available,
        max(num_bikes_available)                                 as max_bikes_available,
        sum(case when is_starved then 1 else 0 end)              as starvation_events,
        sum(case when is_overflow then 1 else 0 end)             as overflow_events
    from snapshots
    group by 1, 2, 3, 4, 5, 6

)

select * from hourly
