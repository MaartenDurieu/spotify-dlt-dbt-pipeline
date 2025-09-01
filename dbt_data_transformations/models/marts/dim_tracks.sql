{{ config(materialized='table') }}

select distinct
    track__id as track_id,
    track__name as track_name,
    track__track_number as track_number,
    track__duration_ms as duration_ms,
    track__duration_ms / 60000.0 as track_duration_minutes,
    track__explicit as is_explicit,
    track__external_ids__isrc as isrc,
    track__popularity as popularity,
    case 
        when track__popularity >= 75 then 'High'
        when track__popularity >= 50 then 'Medium'
        else 'Low'
    end as track_popularity_bucket,
    track__album__id as album_id,
from {{ source('spotify_data', 'recently_played_tracks') }}
where track__id is not null
