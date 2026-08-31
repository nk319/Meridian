{#
    Recency, frequency and monetary value per customer. CONTRACTS.md §8.

    Grain is the customer, not the customer *version* — RFM describes a person's
    behaviour over their whole history, so it keys on `customer_id` and joins
    the current dimension row for attributes. Using `customer_sk` here would
    give a platinum customer three RFM rows, one per tier they have held, each
    describing a slice of the same behaviour.

    Recency is measured from `var('anchor_date')`, not `now()`. Against a fixed
    seed, now() makes every customer look progressively more lapsed each day
    until the whole book is "churned" and the segmentation says nothing.
#}

with orders as (
    select
        customer_id,
        count(*) filter (where is_revenue_recognised) as frequency,
        sum(revenue_amount)                           as monetary,
        max(order_date) filter (where is_revenue_recognised) as last_order_date,
        min(order_date) filter (where is_revenue_recognised) as first_order_date,
        sum(unit_count)                               as units
    from {{ ref('fact_orders') }}
    group by customer_id
),

scored as (
    select
        o.*,
        ('{{ var("anchor_date") }}'::date - o.last_order_date) as recency_days,

        -- Quintiles over the customer base. ntile is the honest tool here: RFM
        -- scores are ranks within a population, not absolute thresholds, so a
        -- hardcoded "spent over £500 = a 5" stops meaning anything the moment
        -- the business changes size.
        --
        -- Recency inverts: fewer days since the last order is a better score,
        -- so the ntile is taken descending.
        6 - ntile(5) over (order by o.last_order_date) as r_score,
        ntile(5) over (order by o.frequency)           as f_score,
        ntile(5) over (order by o.monetary)            as m_score
    from orders o
    where o.frequency > 0
)

select
    c.customer_sk,
    s.customer_id,
    c.loyalty_tier,
    c.segment,
    c.city,
    c.country,
    c.signup_date,
    c.is_deleted,

    s.first_order_date,
    s.last_order_date,
    s.recency_days,
    s.frequency,
    s.monetary,
    s.units,

    s.r_score,
    s.f_score,
    s.m_score,
    (s.r_score + s.f_score + s.m_score) as rfm_score,

    -- The standard coarse buckets. Deliberately derived from the scores rather
    -- than from `silver.customers.segment`: that column is what the source
    -- system asserts, this is what the orders say, and a dashboard that can
    -- show both is more interesting than one that can only echo the CRM.
    case
        when s.r_score >= 4 and s.f_score >= 4 and s.m_score >= 4 then 'champion'
        when s.r_score >= 4 and s.f_score >= 3                    then 'loyal'
        when s.r_score >= 4                                       then 'promising'
        when s.r_score = 3  and s.f_score >= 3                    then 'needs_attention'
        when s.r_score <= 2 and s.f_score >= 4                    then 'at_risk'
        when s.r_score <= 2 and s.m_score >= 4                    then 'cant_lose'
        when s.r_score <= 2                                       then 'hibernating'
        else 'other'
    end as rfm_segment,

    case when s.frequency > 0
         then round(s.monetary / s.frequency, 2)
    end as nadd_avg_order_value

from scored s
-- The current version, for attributes only. `is_current` is true for the
-- hard-deleted customer's tombstone too, which is why `is_deleted` is carried
-- through rather than assumed false.
join {{ ref('dim_customer') }} c
  on c.customer_id = s.customer_id and c.is_current
