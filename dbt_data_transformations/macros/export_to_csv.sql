{% macro export_to_csv(table_name, file_name) %}
{% set folder = 'csv_dumps' %}
{% set full_path = folder ~ '/' ~ file_name %}

{{ log("Exporting table " ~ table_name ~ " to " ~ full_path, info=True) }}

copy (select * from {{ table_name }})
to '{{ full_path }}'
with (header, delimiter ',');
{% endmacro %}
