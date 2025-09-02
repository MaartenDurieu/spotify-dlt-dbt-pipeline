{{ config(materialized='view') }}

SELECT
    'Top Tracks' AS category,
    t.track_name AS item,
    COUNT(*) AS play_count
FROM analytics.fact_listening_history f
JOIN analytics.dim_tracks t
    ON f.track_id = t.track_id
GROUP BY t.track_name
ORDER BY play_count DESC
LIMIT 10
