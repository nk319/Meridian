"""Request and response shapes.

Pydantic does two things here that matter and one that does not. What matters:
it rejects a request body that violates a CONTRACTS.md §9 vocabulary before any
SQL is built, and it makes the OpenAPI document at `/docs` describe the actual
contract rather than a hand-written approximation. What does not matter is
serialisation speed, which is why nothing here is tuned for it.

The vocabularies are imported from `lake.silver_spec` rather than restated.
There are already two copies in the codebase — the generator's and the lake's —
and `tests/test_silver_spec.py` exists to keep those two honest. A third would
need a third test.
"""

from __future__ import annotations

import datetime as dt
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator

from ..lake.silver_spec import (
    SENTIMENT,
    TICKET_CHANNEL,
    TICKET_INTENT,
    TICKET_PRIORITY,
    TICKET_STATUS,
)


class TokenRequest(BaseModel):
    username: str
    password: str
    # Requested scopes, narrowed against what the user actually holds. A client
    # that only needs to read tickets should be able to ask for a token that
    # cannot spend model tokens, and downscoping is how that is expressed.
    scopes: list[str] | None = None


class TokenResponse(BaseModel):
    access_token: str
    token_type: Literal["bearer"] = "bearer"
    expires_in: int
    scopes: list[str]


class Ticket(BaseModel):
    ticket_id: str
    customer_id: str
    # Required, and both FKs are enforced by the database. Every ticket in this
    # source system is about an order — `db/init/05_oltp_ddl.sql` makes both
    # columns NOT NULL REFERENCES — so a nullable field here would model a
    # ticket the database cannot store.
    order_id: str
    created_ts: dt.datetime
    resolved_ts: dt.datetime | None = None
    status: str
    channel: str
    subject: str
    body: str
    intent: str
    priority: str
    sentiment: str


class TicketPage(BaseModel):
    """A cursor-paginated page.

    Keyset pagination on `(created_ts, ticket_id)`, not `OFFSET`. Offset
    pagination re-scans everything it skips, so page 500 costs 500 pages of
    work, and it silently repeats or drops rows when a row is inserted between
    two requests — which for a ticket feed being written to continuously is not
    a corner case, it is the normal state.

    The tie-break on `ticket_id` is not decoration: `created_ts` is not unique
    (the generator emits several tickets a minute), and a cursor on a
    non-unique column loses every row that shares the boundary value.
    """

    items: list[Ticket]
    next_cursor: str | None = None
    has_more: bool


class TicketCreate(BaseModel):
    customer_id: str
    order_id: str
    subject: Annotated[str, Field(min_length=1, max_length=200)]
    body: Annotated[str, Field(min_length=1, max_length=10000)]
    channel: str = "web_form"
    priority: str = "P3"
    # Absent on creation, and that is deliberate rather than an omission.
    # `intent` and `sentiment` are the source system's ground-truth labels, and
    # `mart_support_health` scores the model's predictions against them. Letting
    # a caller set them would let the thing being measured write its own answer
    # key.
    intent: str = "general_inquiry"

    @field_validator("channel")
    @classmethod
    def _channel(cls, value: str) -> str:
        if value not in TICKET_CHANNEL:
            raise ValueError(f"channel must be one of {list(TICKET_CHANNEL)}")
        return value

    @field_validator("priority")
    @classmethod
    def _priority(cls, value: str) -> str:
        if value not in TICKET_PRIORITY:
            raise ValueError(f"priority must be one of {list(TICKET_PRIORITY)}")
        return value

    @field_validator("intent")
    @classmethod
    def _intent(cls, value: str) -> str:
        if value not in TICKET_INTENT:
            raise ValueError(f"intent must be one of {list(TICKET_INTENT)}")
        return value


class TicketUpdate(BaseModel):
    status: str | None = None
    priority: str | None = None
    sentiment: str | None = None

    @field_validator("status")
    @classmethod
    def _status(cls, value: str | None) -> str | None:
        if value is not None and value not in TICKET_STATUS:
            raise ValueError(f"status must be one of {list(TICKET_STATUS)}")
        return value

    @field_validator("priority")
    @classmethod
    def _priority(cls, value: str | None) -> str | None:
        if value is not None and value not in TICKET_PRIORITY:
            raise ValueError(f"priority must be one of {list(TICKET_PRIORITY)}")
        return value

    @field_validator("sentiment")
    @classmethod
    def _sentiment(cls, value: str | None) -> str | None:
        if value is not None and value not in SENTIMENT:
            raise ValueError(f"sentiment must be one of {list(SENTIMENT)}")
        return value


# ---------------------------------------------------------------------------
# AI
# ---------------------------------------------------------------------------


class SearchHit(BaseModel):
    chunk_id: str
    ticket_id: str
    content: str
    similarity: float | None = None
    rrf_score: float | None = None
    lexical_rank: int | None = None
    vector_rank: int | None = None


class SearchResponse(BaseModel):
    query: str
    strategy: str
    hits: list[SearchHit]
    # Both ranks are returned per hit, not just the fused score. A hybrid
    # retriever that cannot say *which half* found a document is one you cannot
    # debug — and the per-ranker breakdown is what showed that fusion drops
    # some documents BM25 ranks first (docs/PROGRESS.md, Phase 4).
    took_ms: float


class AskRequest(BaseModel):
    question: Annotated[str, Field(min_length=3, max_length=1000)]
    top_k: Annotated[int, Field(ge=1, le=20)] = 5


class AskResponse(BaseModel):
    question: str
    answer: str
    abstained: bool
    # Which passages the answer was built from. Non-optional on purpose: an
    # answer without its sources is a claim, and the whole argument for
    # retrieval-augmented generation is that the claim can be checked.
    sources: list[SearchHit]
    model: str | None = None
    top_similarity: float | None = None
    took_ms: float


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded"]
    checks: dict[str, str]
