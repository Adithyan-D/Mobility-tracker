{{ config(materialized='view') }}

with source as (

    select * from {{ source('raw', 'station_status') }}

),

flattened as (

    select
        cast(city as varchar)                                                  as city,
        cast(station_id as varchar)                                            as station_id,
        cast(city as varchar) || '-' || cast(station_id as varchar)           as global_station_id,
        try_cast(json_extract_string(payload, '$.num_bikes_available') as integer)  as num_bikes_available,
        try_cast(json_extract_string(payload, '$.num_bikes_disabled') as integer)   as num_bikes_disabled,
        try_cast(json_extract_string(payload, '$.num_docks_available') as integer)  as num_docks_available,
        try_cast(json_extract_string(payload, '$.num_docks_disabled') as integer)   as num_docks_disabled,
        try_cast(json_extract_string(payload, '$.num_ebikes_available') as integer) as num_ebikes_available,
        try_cast(json_extract_string(payload, '$.num_scooters_available') as integer) as num_scooters_available,
        try_cast(json_extract_string(payload, '$.is_installed') as boolean)    as is_installed,
        try_cast(json_extract_string(payload, '$.is_renting') as boolean)      as is_renting,
        try_cast(json_extract_string(payload, '$.is_returning') as boolean)    as is_returning,
        try_cast(json_extract_string(payload, '$.last_reported') as bigint)    as last_reported_epoch,
        cast(batch_id as bigint)                                               as batch_id,
        _loaded_at
    from source

),

typed as (

    select
        *,
        coalesce(to_timestamp(last_reported_epoch), _loaded_at) as last_reported_at
    from flattened

),

deduplicated as (

    select *
    from typed
    qualify row_number() over (
        partition by global_station_id, batch_id
        order by _loaded_at desc
    ) = 1

)

select * from deduplicated
