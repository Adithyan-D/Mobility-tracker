{{ config(materialized='table') }}

with stations as (

    select * from {{ ref('stg_station_information') }}

),

latest as (

    select
        global_station_id,
        city,
        station_id,
        station_name,
        short_name,
        latitude,
        longitude,
        region_id,
        capacity,
        has_kiosk,
        station_type,
        has_key_dispenser,
        batch_id    as last_info_batch_id,
        _loaded_at  as last_info_loaded_at
    from stations
    qualify row_number() over (
        partition by global_station_id
        order by batch_id desc, _loaded_at desc
    ) = 1

)

select * from latest
