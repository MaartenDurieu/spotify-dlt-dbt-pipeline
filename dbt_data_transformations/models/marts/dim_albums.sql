{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['id', '_dlt_valid_from']) }} AS album_pk,
    id as album_id,
    name as album_name,
    album_type,
    release_date,
    total_tracks,
    label,
    popularity as popularity_score,
    {{ spotify_popularity_label('popularity_score', 'popularity_label') }},
    _dlt_valid_from as version_start,
    _dlt_valid_to as version_end
from {{ source('spotify_data', 'albums') }}
