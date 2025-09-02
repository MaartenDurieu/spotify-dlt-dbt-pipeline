{{ config(materialized='view') }}
SELECT
        'Top Artists' AS category,
        a.artist_name AS item,
        COUNT(*) AS play_count
    FROM analytics.bridge_played_track_artists b
    JOIN analytics.dim_artists a
        ON b.artist_id = a.artist_id
    GROUP BY a.artist_name
    ORDER BY play_count DESC
    LIMIT 10