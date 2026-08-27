{#
    Use the configured schema verbatim.

    dbt's default concatenates the profile's schema with the model's, producing
    `gold_stg` from target `gold` plus config `stg` — which happens to be right
    once and wrong everywhere else (`gold_marts` instead of `gold`). CONTRACTS.md
    §1 freezes these three names, so they are stated rather than derived.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif custom_schema_name == 'stg' -%}
        gold_stg
    {%- elif custom_schema_name == 'int' -%}
        gold_int
    {%- elif custom_schema_name == 'marts' -%}
        gold
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
