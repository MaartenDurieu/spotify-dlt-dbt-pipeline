{{ config(materialized='view') }}

WITH played_genres AS (
    SELECT
        UNNEST(STRING_SPLIT(a.genres, ', ')) AS genre,
        b.listening_history_id
    FROM analytics.bridge_played_track_artists b
    JOIN analytics.dim_artists a
        ON b.artist_id = a.artist_id
)

SELECT
    'Top Genres' AS category,
    genre AS item,
    COUNT(DISTINCT listening_history_id) AS play_count
FROM played_genres
GROUP BY genre
ORDER BY play_count DESC
LIMIT 10
