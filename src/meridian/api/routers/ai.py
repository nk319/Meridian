"""`/v1/ai/*` — search and question answering over the masked corpus.

Every handler here runs in a database session opened as `analytics_ro`, which
holds SELECT on `gold`, `rag` and `meta` and nothing at all on `secure`, `oltp`
or `silver`. CONTRACTS.md §1 specifies that, and the consequence is the point:
a request to `/v1/ai/ask` is *incapable* of reading a customer's email address.
Not "does not" — cannot. A prompt injection that talked the model into asking
for PII would get `InsufficientPrivilege` from Postgres.

Two further properties inherited from the Phase 1 layer, and worth restating
because an API is where people look for them:

**The corpus is masked before it is stored, not before it is returned.**
`rag.chunks` never held an unmasked identifier, so there is no filtering step
here that could be forgotten or bypassed. `tests/test_pii_manifest.py` is what
keeps that true.

**No API key is required.** Without `ANTHROPIC_API_KEY` the answer is built
extractively from the retrieved passages and `model` comes back null. Retrieval
is identical either way, and retrieval is the part being demonstrated.
"""

from __future__ import annotations

import time
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status

from ...rag.generate import answer_question
from ...rag.retrieve import STRATEGIES
from ..deps import get_retriever, require_ai
from ..models import AskRequest, AskResponse, SearchHit, SearchResponse

router = APIRouter(prefix="/v1/ai", tags=["ai"])


def _hit(chunk) -> SearchHit:
    return SearchHit(
        chunk_id=chunk.chunk_id,
        ticket_id=chunk.ticket_id,
        content=chunk.content,
        similarity=chunk.similarity,
        rrf_score=chunk.rrf_score,
        lexical_rank=chunk.lexical_rank,
        vector_rank=chunk.vector_rank,
    )


@router.get("/search", response_model=SearchResponse)
def search(
    _principal: Annotated[object, Depends(require_ai)],
    retriever: Annotated[object, Depends(get_retriever)],
    q: Annotated[str, Query(min_length=2, max_length=500)],
    strategy: str = "hybrid",
    top_k: Annotated[int, Query(ge=1, le=20)] = 5,
) -> SearchResponse:
    """Hybrid retrieval, with the per-ranker breakdown exposed.

    `lexical_rank` and `vector_rank` are returned alongside the fused score
    rather than hidden behind it. A hybrid retriever that cannot say which half
    found a document is one nobody can debug — and it is exactly that breakdown
    that showed fusion dropping documents BM25 ranked first (PROGRESS, Phase 4).
    """
    if strategy not in STRATEGIES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"strategy must be one of {sorted(STRATEGIES)}",
        )

    started = time.perf_counter()
    hits = retriever.search(q, strategy=strategy, top_k=top_k)
    return SearchResponse(
        query=q,
        strategy=strategy,
        hits=[_hit(h) for h in hits],
        took_ms=round((time.perf_counter() - started) * 1000, 1),
    )


@router.post("/ask", response_model=AskResponse)
def ask(
    payload: AskRequest,
    _principal: Annotated[object, Depends(require_ai)],
    retriever: Annotated[object, Depends(get_retriever)],
) -> AskResponse:
    """Answer from the corpus, or abstain.

    Abstention is a 200 with `abstained: true`, not a 404. The request
    succeeded and the honest answer is "the corpus does not cover this" — an
    error status would tell a client to retry something that will fail
    identically every time.
    """
    started = time.perf_counter()
    answer = answer_question(payload.question, retriever, top_k=payload.top_k)

    return AskResponse(
        question=answer.question,
        answer=answer.answer,
        abstained=answer.mode == "abstained",
        sources=[
            SearchHit(
                chunk_id=p["chunk_id"],
                ticket_id=p["ticket_id"],
                content=p["content"],
                similarity=p.get("similarity"),
                lexical_rank=p.get("lexical_rank"),
                vector_rank=p.get("vector_rank"),
            )
            for p in answer.passages
        ],
        model=answer.model,
        top_similarity=round(answer.top_similarity, 4),
        took_ms=round((time.perf_counter() - started) * 1000, 1),
    )
