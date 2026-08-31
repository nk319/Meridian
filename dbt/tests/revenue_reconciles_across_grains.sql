{#
    The two order fact grains agree on the money.

    This is the test the grain split needs. `fact_orders` and
    `fact_order_items` are built from different sources and joined to the SCD2
    dimension independently, so either one can fan out or lose rows without the
    other noticing — and a fan-out on the line fact looks exactly like a
    successful product launch.

    Compared against `gross_amount`, not `total_amount`: the header carries
    discount, shipping and tax, which have no line to belong to. Merchandise
    value is the quantity both grains actually claim to measure.

    A cent of tolerance, because both sides are DECIMAL(12,2) sums of
    DECIMAL(12,2) values and exact equality across a re-association is a
    stricter claim than the data model makes.
#}

with header as (
    select order_id, gross_amount from {{ ref('fact_orders') }}
),

lines as (
    select order_id, sum(line_amount) as line_total
    from {{ ref('fact_order_items') }}
    group by order_id
)

select
    h.order_id,
    h.gross_amount,
    l.line_total,
    (h.gross_amount - coalesce(l.line_total, 0)) as difference
from header h
left join lines l on l.order_id = h.order_id
where abs(h.gross_amount - coalesce(l.line_total, 0)) > 0.01
