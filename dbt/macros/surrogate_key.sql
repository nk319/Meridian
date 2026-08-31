{#
    A surrogate key from one or more natural columns.

    md5 over the columns joined by a separator, with NULLs coalesced to a
    sentinel first — the same reasoning as the Bronze record hash: without it
    ('a', NULL) and (NULL, 'a') collide, and two different dimension members
    would share a key.

    Written out rather than pulled from dbt_utils. One macro is not worth a
    package dependency that has to be fetched over the network before the
    project will compile.
#}
{% macro surrogate_key(columns) -%}
    md5(
        {%- for column in columns %}
        coalesce(cast({{ column }} as varchar), '<null>')
        {%- if not loop.last %} || '|' || {% endif %}
        {%- endfor %}
    )
{%- endmacro %}
