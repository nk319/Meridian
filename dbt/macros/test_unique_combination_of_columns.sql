{#
    A composite-key uniqueness test.

    dbt ships `unique` for a single column and nothing for a set of them, which
    leaves the grain of every mart in this project untested — and the grain is
    the one property a mart has to get right. `mart_daily_sales` claiming to be
    one row per (day, channel) and quietly being two is the bug that doubles
    every chart on the dashboard.

    dbt_utils has this test. Taking the dependency would mean `dbt deps` — a
    network fetch — before the project will compile, which is a real cost for a
    demo someone is meant to be able to clone and run. Fourteen lines is
    cheaper, and this way the whole project builds offline.

    Written to fail loudly rather than tidily: the failing rows carry the key
    values and the count, so `dbt test` output says which day and channel are
    duplicated rather than only that something is.
#}

{% test unique_combination_of_columns(model, combination_of_columns) %}

{%- set columns_csv = combination_of_columns | join(', ') -%}

select
    {{ columns_csv }},
    count(*) as n_rows
from {{ model }}
group by {{ columns_csv }}
having count(*) > 1

{% endtest %}
