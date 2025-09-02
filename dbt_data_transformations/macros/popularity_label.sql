{% macro spotify_popularity_label(column, new_column_name='popularity_label') %}
CASE
    WHEN {{ column }} BETWEEN 0 AND 24 THEN 'Hidden Gem'
    WHEN {{ column }} BETWEEN 25 AND 49 THEN 'Rising Star'
    WHEN {{ column }} BETWEEN 50 AND 74 THEN 'Known Favorite'
    WHEN {{ column }} BETWEEN 75 AND 100 THEN 'Chart Topper'
    ELSE 'Unknown'
END AS {{ new_column_name }}
{% endmacro %}
