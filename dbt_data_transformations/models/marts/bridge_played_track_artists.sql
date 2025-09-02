{{ config(materialized='table') }}

select distinct
    r.dlt_pk as listening_history_id,
    a.id as artist_id
from {{ source('spotify_data', 'recently_played_tracks__track__artists') }} a
join {{ ref('fact_listening_history') }} r
    on a._dlt_parent_id = r.dlt_pk
