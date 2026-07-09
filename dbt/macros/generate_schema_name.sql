{#
    Override default dbt: kalau model punya custom schema (+schema: xxx di dbt_project.yml
    atau config(schema='xxx') di model), pakai PERSIS nama itu sebagai nama database ClickHouse
    -- BUKAN digabung jadi '<target_schema>_<custom_schema>' (mis. 'silver_gold') seperti
    perilaku default dbt.

    Dengan macro ini:
    - Model tanpa override schema -> pakai schema default di profiles.yml (CLICKHOUSE_DATABASE, saat ini 'silver')
    - Model dengan +schema: gold  -> masuk ke database 'gold' apa adanya
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
