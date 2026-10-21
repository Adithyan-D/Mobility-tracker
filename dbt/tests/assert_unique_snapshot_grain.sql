-- Fails if any (global_station_id, batch_id) pair is duplicated in the snapshot fact.
select
    global_station_id,
    batch_id,
    count(*) as row_count
from {{ ref('fct_station_snapshots') }}
group by 1, 2
having count(*) > 1
