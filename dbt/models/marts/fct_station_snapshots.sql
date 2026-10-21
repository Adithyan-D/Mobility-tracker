{{ config(materialized='table') }}

with status as (

    select * from {{ ref('stg_station_status') }}

),

stations as (

    select * from {{ ref('dim_stations') }}

),

joined as (

    select
        s.global_station_id,
        s.city,
        s.station_id,
        s.batch_id,
        s.last_reported_at                                   as snapshot_at,
        case
            when s.city in ('nyc', 'dc') then timezone('America/New_York', s.last_reported_at)
            when s.city = 'chicago' then timezone('America/Chicago', s.last_reported_at)
            when s.city = 'sf' then timezone('America/Los_Angeles', s.last_reported_at)
            else s.last_reported_at
        end                                                  as local_snapshot_at,
        case
            when s.city in ('nyc', 'dc') then 'America/New_York'
            when s.city = 'chicago' then 'America/Chicago'
            when s.city = 'sf' then 'America/Los_Angeles'
            else 'UTC'
        end                                                  as timezone_name,
        s._loaded_at                                         as ingested_at,
        st.station_name                                      as station_name,
        st.region_id,
        st.latitude,
        st.longitude,
        st.capacity,
        s.num_bikes_available,
        s.num_bikes_disabled,
        s.num_docks_available,
        s.num_docks_disabled,
        s.num_ebikes_available,
        s.num_scooters_available,
        coalesce(s.is_installed, false)                      as is_installed,
        coalesce(s.is_renting, false)                        as is_renting,
        coalesce(s.is_returning, false)                      as is_returning
    from status s
    left join stations st on s.global_station_id = st.global_station_id

)

select
    *,
    case
        when capacity > 0 then num_bikes_available::double / capacity
    end as utilization_rate,
    (is_installed and is_renting and num_bikes_available = 0)   as is_starved,
    (is_installed and is_returning and num_docks_available = 0) as is_overflow
from joined
