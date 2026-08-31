{#
    The AI layer's output, staged like any other source.

    Deliberately not filtered to rows where the model succeeded. A row with
    null labels and a populated `error` means "we asked and got nothing
    usable", which is a different state from a ticket that was never enriched
    at all — and only the first has a row here. Dropping them would erase that
    distinction exactly where the mart needs it to compute coverage.
#}

select
    ticket_id,
    predicted_intent    as ai_intent,
    predicted_sentiment as ai_sentiment,
    confidence          as ai_confidence,
    model               as ai_model,
    enriched_at         as ai_enriched_at,
    error               as ai_error
from {{ source('rag', 'ticket_enrichment') }}
