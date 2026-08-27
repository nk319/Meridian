"""Values the golden eval set needs to name, discovered rather than assumed.

`eval/golden_questions.yml` contains one question that names an order reference
directly — "What is the problem reported on order O0000019?" — and that question
carries the whole eval. Every other question paraphrases a template the intent
grammar emits dozens of times, so all three retrieval strategies score 1.00 on
them and the metric is pinned at its ceiling, where it cannot detect a
regression. Only an identifier occurring in exactly one ticket separates BM25
from vector search, which is the difference hybrid retrieval exists to exploit.

The literal was written into the YAML by hand, against a pinned generator seed.
That held until a genuine bug fix elsewhere in the generator — a hard-deleted
customer whose signup date landed after their deletion — shifted the order
sequence by a few rows, and the anchor stopped occurring in any ticket at all.
The failure was loud, because `build_relevance` refuses to score a question
whose relevant set is empty, but the fix as written was "find another order id
and paste it in", which is the same coupling again with a fresh expiry date.

So the generator publishes the anchors instead. They go into
`seeds/manifest.json` alongside the row counts and the SCD2 demo constants, and
`meridian.rag.evaluate` resolves `content_from_anchor` against them. The
property the question needs — an identifier occurring in exactly one ticket —
is then established by looking at the corpus that was actually generated,
rather than asserted about one that was generated last year.
"""

from __future__ import annotations

import re
from collections import Counter

# Order references as they appear in ticket prose. The generator formats them
# as O + 7 digits; matching the shape rather than joining against the orders
# table keeps this honest about what a *reader* of the ticket would see, which
# is what a lexical retriever is scoring against.
ORDER_REF = re.compile(r"\bO\d{7}\b")


def _ticket_text(ticket: dict) -> str:
    return f"{ticket.get('subject', '')} {ticket.get('body', '')}"


def derive(tickets: list[dict]) -> dict:
    """Anchors for the golden question set.

    `singleton_order_id` is an order reference appearing in exactly one ticket.
    Chosen as the lexicographically smallest of the candidates rather than the
    first encountered: iteration order over the ticket list is stable for a
    pinned seed, but sorting makes the choice independent of it, so a change to
    how tickets are ordered does not silently move the anchor.

    Returns an empty mapping if no such reference exists — a corpus where every
    order is mentioned twice is a legitimate (if unlikely) generator outcome,
    and it is `build_relevance`'s job to complain about a question it cannot
    resolve, not this function's to invent an answer.
    """
    mentions: Counter[str] = Counter()
    owner: dict[str, str] = {}

    for ticket in tickets:
        for reference in set(ORDER_REF.findall(_ticket_text(ticket))):
            mentions[reference] += 1
            owner.setdefault(reference, ticket.get("ticket_id", ""))

    singletons = sorted(ref for ref, n in mentions.items() if n == 1)
    if not singletons:
        return {}

    chosen = singletons[0]
    return {
        "singleton_order_id": chosen,
        "singleton_order_ticket_id": owner[chosen],
        # Recorded so a reader can see how much slack there is. A corpus with
        # one singleton is one bug fix away from having none, and this number
        # says whether that is the situation.
        "singleton_order_candidates": len(singletons),
        # A spread of them, for the retrieval test that compares rankers.
        #
        # One anchor is an anecdote, and this project already learned that the
        # expensive way: the claim "vector search finds a bare rare identifier
        # at rank 1, and only loses it when the query dilutes it" was written
        # against a single hand-picked order and is false in general. Measured
        # over twelve, embeddings rank the right ticket first twice — order
        # references share a prefix and differ only in digits, so they all
        # embed to nearly the same point whether or not anything surrounds
        # them. Sampling across the corpus is what makes the test measure that
        # instead of re-discovering one lucky case.
        #
        # Evenly spaced rather than randomly drawn: no RNG to thread through,
        # no dependence on Python's hash seed, and the same twelve every time
        # for a given corpus.
        "singleton_order_samples": [
            {"order_id": ref, "ticket_id": owner[ref]}
            for ref in singletons[:: max(1, len(singletons) // 12)][:12]
        ],
    }
