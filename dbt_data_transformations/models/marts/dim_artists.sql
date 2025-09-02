{{ config(materialized='table') }}

WITH artist_genres AS (
    SELECT
        a.id AS artist_id,
        a.followers__total AS followers,
        a.name AS artist_name,
        a.type AS artist_type,
        a.popularity AS popularity_score,
        a.external_urls__spotify AS spotify_url,
        a._dlt_valid_from AS version_start,
        a._dlt_valid_to AS version_end,
        g.value AS genre
    FROM {{ source('spotify_data', 'artists') }} a
    LEFT JOIN {{ source('spotify_data', 'artists__genres') }} g
        ON a._dlt_id = g._dlt_parent_id
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['artist_id', 'version_start']) }} AS artist_pk,
    artist_id,
    followers,
    artist_name,
    artist_type,
    popularity_score,
    {{ spotify_popularity_label('popularity_score', 'popularity_label') }},
    spotify_url,
    version_start,
    version_end,
    STRING_AGG(DISTINCT genre, ', ') AS genres
FROM artist_genres
GROUP BY
    artist_id,
    followers,
    artist_name,
    artist_type,
    popularity_score,
    spotify_url,
    version_start,
    version_end
