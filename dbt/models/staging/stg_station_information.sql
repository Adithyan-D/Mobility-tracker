{{ config(materialized='view') }}

with source as (

    select * from {{ source('raw', 'station_information') }}

),

flattened as (

    select
        cast(city as varchar)                                           as city,
        cast(station_id as varchar)                                     as station_id,
        cast(city as varchar) || '-' || cast(station_id as varchar)     as global_station_id,
        json_extract_string(payload, '$.name')                          as station_name,
        json_extract_string(payload, '$.short_name')                    as short_name,
        cast(json_extract_string(payload, '$.lat') as double)           as latitude,
        cast(json_extract_string(payload, '$.lon') as double)           as longitude,
        try_cast(json_extract_string(payload, '$.region_id') as integer) as region_id,
        try_cast(json_extract_string(payload, '$.capacity') as integer) as capacity,
        try_cast(json_extract_string(payload, '$.has_kiosk') as boolean) as has_kiosk,
        json_extract_string(payload, '$.station_type')                  as station_type,
        try_cast(json_extract_string(payload, '$.eightd_has_key_dispenser') as boolean) as has_key_dispenser,
        cast(batch_id as bigint)                                        as batch_id,
        _loaded_at
    from source

),

deduplicated as (

    select *
    from flattened
    qualify row_number() over (
        partition by global_station_id, batch_id
        order by _loaded_at desc
    ) = 1

)

select * from deduplicated
