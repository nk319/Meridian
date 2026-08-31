{#
    One row per ticket, carrying both the source system's labels and the
    model's. CONTRACTS.md §8.

    `intent` and `sentiment` are the source system's own — ground truth,
    generated with the ticket and never shown to the model
    (db/init/05_oltp_ddl.sql). `ai_intent` and `ai_sentiment` are what
    `meridian.rag.enrich` predicted from the *masked* text. Keeping both on one
    row is what lets `mart_support_health` report agreement, which is a real
    quality measure rather than a claim.

    Every `ai_*` column is null when no ANTHROPIC_API_KEY is configured. That is
    a supported state, documented in docs/governance/owners.yml, and the
    agreement flags below are null with it rather than false — "we did not ask"
    and "the model was wrong" are different facts and a boolean cannot hold
    both.
#}

select
    {{ surrogate_key(['t.ticket_id']) }} as ticket_sk,

    t.ticket_id,
    t.order_id,
    {{ surrogate_key(['t.order_id']) }} as order_sk,

    c.customer_sk,
    t.customer_id,
    (to_char(t.created_date, 'YYYYMMDD'))::int as date_sk,

    t.created_ts,
    t.created_date,
    t.resolved_ts,
    t.status,
    t.contact_channel,
    t.priority,
    t.subject,

    t.intent,
    t.sentiment,

    e.ai_intent,
    e.ai_sentiment,
    e.ai_confidence,
    e.ai_model,
    e.ai_error,

    (e.ai_intent    is not null) as is_ai_enriched,
    case when e.ai_intent    is not null then e.ai_intent    = t.intent    end as ai_intent_agrees,
    case when e.ai_sentiment is not null then e.ai_sentiment = t.sentiment end as ai_sentiment_agrees,

    t.resolution_hours,
    (t.status = 'resolved') as is_resolved

from {{ ref('stg_support_tickets') }} t
left join {{ ref('stg_ticket_enrichment') }} e on e.ticket_id = t.ticket_id
left join {{ ref('dim_customer') }} c
       on c.customer_id = t.customer_id
      and t.created_ts >= c.valid_from
      and t.created_ts <  c.valid_to
