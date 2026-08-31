{#
    Ticket volume, resolution and the AI layer's accuracy. CONTRACTS.md §8.

    This mart is the reason the AI layer cannot become a side attachment
    nothing consumes: it reads `ai_intent` and `ai_sentiment` off
    `fact_support_tickets` structurally, so removing the enrichment breaks a
    dashboard rather than going unnoticed.

    Every ai_* measure is null-safe in the specific sense that matters: the
    accuracy percentages divide by the number of *enriched* tickets, not by the
    number of tickets. With no ANTHROPIC_API_KEY nothing is enriched,
    `ai_enriched` is 0, and the accuracies are null — which is what "we do not
    know" looks like. Dividing by the ticket count would report 0% accuracy,
    which is a claim about the model rather than about the configuration.
#}

select
    t.date_sk,
    d.date_day,
    d.month_start_date,
    t.intent,
    t.priority,
    t.contact_channel,

    count(*)                                          as tickets,
    count(*) filter (where t.is_resolved)              as resolved_tickets,
    count(*) filter (where t.status = 'open')          as open_tickets,
    count(*) filter (where t.status = 'pending')       as pending_tickets,
    count(distinct t.customer_id)                      as customers,

    count(*) filter (where t.sentiment = 'negative')   as negative_tickets,
    count(*) filter (where t.sentiment = 'neutral')    as neutral_tickets,
    count(*) filter (where t.sentiment = 'positive')   as positive_tickets,

    -- Only over resolved tickets. Averaging in the unresolved ones as zero
    -- would make a backlog look like fast service.
    round(avg(t.resolution_hours) filter (where t.is_resolved)::numeric, 2)
        as nadd_avg_resolution_hours,
    round((percentile_cont(0.5) within group (order by t.resolution_hours)
           filter (where t.is_resolved))::numeric, 2)
        as nadd_median_resolution_hours,

    round(100.0 * count(*) filter (where t.is_resolved) / count(*), 2)
        as nadd_resolution_rate_pct,
    round(100.0 * count(*) filter (where t.sentiment = 'negative') / count(*), 2)
        as nadd_negative_pct,

    -- The AI half.
    count(*) filter (where t.is_ai_enriched)           as ai_enriched,
    count(*) filter (where t.ai_error is not null)     as ai_errors,
    count(*) filter (where t.ai_intent_agrees)         as ai_intent_agreements,
    count(*) filter (where t.ai_sentiment_agrees)      as ai_sentiment_agreements,

    round(100.0 * count(*) filter (where t.is_ai_enriched) / count(*), 2)
        as nadd_ai_coverage_pct,

    case when count(*) filter (where t.is_ai_enriched) > 0
         then round(100.0 * count(*) filter (where t.ai_intent_agrees)
                          / count(*) filter (where t.is_ai_enriched), 2)
    end as nadd_ai_intent_accuracy_pct,
    case when count(*) filter (where t.ai_sentiment is not null) > 0
         then round(100.0 * count(*) filter (where t.ai_sentiment_agrees)
                          / count(*) filter (where t.ai_sentiment is not null), 2)
    end as nadd_ai_sentiment_accuracy_pct,

    round(avg(t.ai_confidence) filter (where t.is_ai_enriched), 3)
        as nadd_avg_ai_confidence

from {{ ref('fact_support_tickets') }} t
join {{ ref('dim_date') }} d on d.date_sk = t.date_sk
group by t.date_sk, d.date_day, d.month_start_date,
         t.intent, t.priority, t.contact_channel
