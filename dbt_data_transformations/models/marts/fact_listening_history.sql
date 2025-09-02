{{
  config(
    materialized='table'
  )
}}

select
    s._dlt_id as dlt_pk,
    s.played_at,
    s.track__id as track_id,
    s.track__album__id as album_id,
    s.context__type as context__type
from {{ source('spotify_data', 'recently_played_tracks') }} s