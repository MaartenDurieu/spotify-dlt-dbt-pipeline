{{
    config(
        materialized='table'
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['track__id']) }} AS track_key,
    track__id as track_id,
    track__name as track_name,
    track__track_number as track_number,
    track__duration_ms as duration_ms,
    track__duration_ms / 60000.0 AS track_duration_minutes,
    track__explicit as is_explicit,
    track__external_ids__isrc as isrc,
    track__popularity as popularity,
    {{ spotify_popularity_label('track__popularity', 'track_popularity_bucket') }},
    track__album__id as album_id
FROM {{ source('spotify_data', 'recently_played_tracks') }}
WHERE track__id IS NOT NULL