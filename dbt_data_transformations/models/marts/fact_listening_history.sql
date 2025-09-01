{{ config(materialized='table') }}


select
    r.played_at,
    extract(hour from r.played_at) as played_hour,
    extract(dow from r.played_at) as played_day,
    extract(week from r.played_at) as played_week,
    case when extract(dow from r.played_at) in (0,6) then true else false end as is_weekend,
    r.track__id as track_id,
    r.track__album__id as album_id,
    r.context__type as context_type,
    r._dlt_load_id,
    r._dlt_id
from {{ source('spotify_data', 'recently_played_tracks') }} r
