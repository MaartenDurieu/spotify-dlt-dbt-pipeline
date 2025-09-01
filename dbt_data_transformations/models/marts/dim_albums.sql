{{ config(materialized='table') }}

select distinct
    id as album_id,
    name as album_name,
    album_type,
    release_date,
    total_tracks,
    label,
    popularity as popularity_score,
    case 
        when popularity between 0 and 24 then 'Hidden Gem'
        when popularity between 25 and 49 then 'Rising Star'
        when popularity between 50 and 74 then 'Known Favorite'
        when popularity between 75 and 100 then 'Chart Topper'
        else 'Unknown'
    end as popularity_label,
    _dlt_valid_from as version_start,
    _dlt_valid_to as version_end
from {{ source('spotify_data', 'albums') }}
