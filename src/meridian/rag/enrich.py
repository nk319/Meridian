"""Classify support tickets with the LLM, from masked text only.

    python -m meridian.rag.enrich --limit 200

The last of CONTRACTS.md §5's entrypoints. Reads `rag.chunks`, asks the model
for an intent and a sentiment, and writes `rag.ticket_enrichment`, which
`gold.fact_support_tickets` joins and `gold.mart_support_health` scores.

Three properties are the point of this module, and each is a deliberate choice
rather than a convenience:

**It reads `rag.chunks`, never `silver.support_tickets`.** The model sees the
masked text and nothing else — the same text retrieval sees, with names, emails
and phone numbers already replaced by placeholders. The role it connects as
(`rag_indexer`) has no grant on `secure`, so this is enforced by the cluster and
not by this file being careful. That is the whole argument of CONTRACTS.md §10
applied to the enrichment path: PII does not leave the warehouse for a vendor,
because the process that talks to the vendor cannot read it.

**It never sees the ground truth.** `silver.support_tickets.intent` and
`.sentiment` are generated with the ticket and are what the prediction is scored
against in `mart_support_health`. Feeding them in — even as "context" — would
turn the accuracy number into a measurement of nothing. The SQL below selects
from `rag.chunks` alone, which does not have those columns to leak.

**Skipping is keyed on the input.** `content_hash` over the masked text is the
same skip key the indexer uses, for the same reason: it makes "the masking
changed" and "re-ask the model" the same event, so a masking fix cannot leave
stale predictions derived from text that no longer exists.

With no ANTHROPIC_API_KEY this writes nothing and exits 0. That is a supported
state, not a degraded one — docs/governance/owners.yml states it as the contract
for this table's consumers, and every `ai_*` column downstream is null.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import uuid
from dataclasses import dataclass

from ..db import UpstreamUnavailable, connect

# The frozen CONTRACTS.md §9 vocabularies, taken from the module that already
# owns them rather than copied. The copy in `silver_spec` exists because the
# lake must not import the seed generator; there is no such constraint here, and
# a third copy is a third thing to drift.
from ..lake.silver_spec import SENTIMENT as SENTIMENTS
from ..lake.silver_spec import TICKET_INTENT as TICKET_INTENTS
from ..runlog import EXIT_ERROR, EXIT_OK, EXIT_UPSTREAM_UNAVAILABLE, RunLogger
from ..settings import settings

SYSTEM_PROMPT = """\
You classify e-commerce support tickets.

Return ONLY a JSON object, no prose and no code fence, with exactly these keys:

  "intent":     one of {intents}
  "sentiment":  one of {sentiments}
  "confidence": a number between 0 and 1

The ticket text has had personal data replaced with placeholders such as
[CUSTOMER_NAME] and [EMAIL]. That is expected; classify the remaining text and
do not comment on the redaction.

Judge sentiment by the customer's tone, not by whether their problem is
serious. A calm, polite report of a broken product is neutral.\
"""


@dataclass(frozen=True)
class Prediction:
    ticket_id: str
    content_hash: str
    intent: str | None
    sentiment: str | None
    confidence: float | None
    error: str | None


# One row per ticket: the chunks of a ticket are joined back together, because
# intent is a property of the whole ticket and the chunker splits mid-argument.
# `min(content_hash)` over the chunks is a stable digest of the ticket's masked
# text — it changes whenever any chunk's masked text does, which is the only
# property the skip key needs.
PENDING_SQL = """
SELECT c.ticket_id,
       min(c.content_hash)                                  AS content_hash,
       string_agg(c.content, E'\\n' ORDER BY c.chunk_seq)   AS content
FROM rag.chunks c
LEFT JOIN rag.ticket_enrichment e ON e.ticket_id = c.ticket_id
GROUP BY c.ticket_id
HAVING max(e.content_hash) IS DISTINCT FROM min(c.content_hash)
    OR max(e.model) IS DISTINCT FROM %(model)s
ORDER BY c.ticket_id
LIMIT %(limit)s
"""

UPSERT_SQL = """
INSERT INTO rag.ticket_enrichment
    (ticket_id, predicted_intent, predicted_sentiment, confidence,
     content_hash, model, enriched_at, error)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (ticket_id) DO UPDATE SET
    predicted_intent    = EXCLUDED.predicted_intent,
    predicted_sentiment = EXCLUDED.predicted_sentiment,
    confidence          = EXCLUDED.confidence,
    content_hash        = EXCLUDED.content_hash,
    model               = EXCLUDED.model,
    enriched_at         = EXCLUDED.enriched_at,
    error               = EXCLUDED.error
"""


def parse_response(text: str) -> tuple[str | None, str | None, float | None, str | None]:
    """Turn the model's reply into validated labels.

    Returns (intent, sentiment, confidence, error). A label outside the frozen
    vocabulary is discarded rather than stored: `rag.ticket_enrichment` has a
    CHECK constraint on both columns, so storing one would fail the insert and
    take the whole batch with it — and a warehouse whose enum columns hold
    whatever a model said this week is not a warehouse.

    The failure is still recorded, with the offending value in `error`. "The
    model answered something invalid" and "the model was not asked" are
    different facts and the table is able to hold both.
    """
    cleaned = text.strip()
    # Models sometimes fence JSON despite being asked not to. Cheaper to strip
    # than to make a second call about.
    if cleaned.startswith("```"):
        cleaned = cleaned.split("\n", 1)[-1].rsplit("```", 1)[0].strip()

    try:
        payload = json.loads(cleaned)
    except json.JSONDecodeError:
        return None, None, None, f"unparseable response: {cleaned[:120]!r}"

    if not isinstance(payload, dict):
        return None, None, None, f"expected an object, got {type(payload).__name__}"

    intent = payload.get("intent")
    sentiment = payload.get("sentiment")
    confidence = payload.get("confidence")

    problems = []
    if intent not in TICKET_INTENTS:
        problems.append(f"intent={intent!r}")
        intent = None
    if sentiment not in SENTIMENTS:
        problems.append(f"sentiment={sentiment!r}")
        sentiment = None
    try:
        confidence = float(confidence)
        if not 0.0 <= confidence <= 1.0:
            problems.append(f"confidence={confidence!r}")
            confidence = None
    except (TypeError, ValueError):
        problems.append(f"confidence={confidence!r}")
        confidence = None

    error = ("outside vocabulary: " + ", ".join(problems)) if problems else None
    return intent, sentiment, confidence, error


def classify(client, model: str, ticket_id: str, content: str, content_hash: str) -> Prediction:
    import anthropic

    prompt = SYSTEM_PROMPT.format(
        intents=", ".join(TICKET_INTENTS),
        sentiments=", ".join(SENTIMENTS),
    )

    try:
        response = client.messages.create(
            model=model,
            max_tokens=1000,
            system=prompt,
            # No extended thinking. This is a short classification against a
            # seven-item vocabulary, run over the whole corpus; thinking here
            # would multiply the cost of the batch to sharpen a decision the
            # model makes correctly in one pass.
            messages=[{"role": "user", "content": f"Ticket:\n\n{content}"}],
        )
    except anthropic.APIStatusError as exc:
        return Prediction(
            ticket_id,
            content_hash,
            None,
            None,
            None,
            f"{type(exc).__name__}: HTTP {exc.status_code}",
        )
    except anthropic.APIConnectionError as exc:
        return Prediction(ticket_id, content_hash, None, None, None, f"APIConnectionError: {exc}")

    if response.stop_reason == "refusal":
        category = getattr(response.stop_details, "category", None)
        return Prediction(ticket_id, content_hash, None, None, None, f"refusal: {category}")

    text = "\n".join(b.text for b in response.content if b.type == "text")
    intent, sentiment, confidence, error = parse_response(text)
    return Prediction(ticket_id, content_hash, intent, sentiment, confidence, error)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help=(
            "Maximum tickets to enrich in one run. Bounded by default because "
            "this is the only step in the platform that costs money per row."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-ask for every ticket, ignoring the content-hash skip.",
    )
    args = parser.parse_args(argv)

    log = RunLogger("rag.enrich")
    cfg = settings()

    if not cfg.has_anthropic_key:
        # Exit 0, deliberately. The platform is specified to run end to end
        # without an API key, and a non-zero exit here would fail the Airflow
        # task and take the DAG red for a configuration that is supported.
        log.emit(
            "skipped",
            reason="no ANTHROPIC_API_KEY — ai_* columns stay null",
            status="SUCCESS",
        )
        return EXIT_OK

    try:
        import anthropic
    except ImportError:
        log.emit("failed", error="anthropic not installed — `pip install -e '.[rag]'`")
        return EXIT_ERROR

    client = anthropic.Anthropic(api_key=cfg.anthropic_api_key)
    model = cfg.anthropic_model

    try:
        with connect("rag_indexer", vectors=False) as conn:
            with conn.cursor() as cur:
                if args.force:
                    cur.execute(
                        "SELECT ticket_id, min(content_hash), "
                        "       string_agg(content, E'\\n' ORDER BY chunk_seq) "
                        "FROM rag.chunks GROUP BY ticket_id ORDER BY ticket_id LIMIT %s",
                        (args.limit,),
                    )
                else:
                    cur.execute(PENDING_SQL, {"model": model, "limit": args.limit})
                pending = cur.fetchall()

            log.emit("pending", tickets=len(pending), model=model)

            predictions = [
                classify(client, model, ticket_id, content, content_hash)
                for ticket_id, content_hash, content in pending
            ]

            now = dt.datetime.now(dt.UTC)
            with conn.cursor() as cur:
                cur.executemany(
                    UPSERT_SQL,
                    [
                        (
                            p.ticket_id,
                            p.intent,
                            p.sentiment,
                            p.confidence,
                            p.content_hash,
                            model,
                            now,
                            p.error,
                        )
                        for p in predictions
                    ],
                )

            run_id = uuid.UUID(log.run_id)
            conn.commit()
    except UpstreamUnavailable as exc:
        log.emit("failed", error=str(exc))
        return EXIT_UPSTREAM_UNAVAILABLE

    errors = sum(1 for p in predictions if p.error)
    log.emit(
        "done",
        run_id=str(run_id),
        rows_in=len(pending),
        rows_out=len(predictions),
        errors=errors,
        model=model,
        status="SUCCESS",
    )
    return EXIT_OK


def cli() -> int:
    return main()


if __name__ == "__main__":
    raise SystemExit(cli())
