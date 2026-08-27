"""Retrieval, abstention and the role boundary.

All database-backed. They skip cleanly without the `core` profile running, which
is what lets `pytest` be green in CI on a checkout with no Docker while still
being the thing that verifies the real system locally.
"""

from __future__ import annotations

import pytest

from meridian.rag.evaluate import (
    build_relevance,
    chance_recall,
    evaluate_strategy,
    load_golden,
)
from meridian.rag.generate import answer_question, choose_effort
from meridian.rag.masking import Masker
from meridian.rag.retrieve import STRATEGIES

pytestmark = pytest.mark.db

# The one identifier occurring in exactly one of the 1,311 tickets. See
# eval/golden_questions.yml: it is the case that separates the two rankers.
NEEDLE_ORDER = "O0000019"
NEEDLE_TICKET = "T000001"


def test_hybrid_search_returns_ranked_results(retriever, cfg):
    hits = retriever.search("order never arrived and tracking has not moved")
    assert len(hits) == cfg.top_k
    scores = [h.rrf_score for h in hits]
    assert scores == sorted(scores, reverse=True), "results are not ordered by RRF score"
    assert all(h.content for h in hits)


def test_every_strategy_returns_something(retriever):
    for strategy in STRATEGIES:
        hits = retriever.search("refund for a damaged item", strategy=strategy)
        assert hits, f"{strategy} returned nothing for an obviously answerable query"


def test_rejects_an_unknown_strategy(retriever):
    with pytest.raises(ValueError, match="strategy must be one of"):
        retriever.search("anything", strategy="magic")


def test_bm25_finds_a_diluted_identifier_that_vectors_miss(retriever):
    """The reason retrieval is hybrid rather than vector-only.

    The effect is narrower than "embeddings lose rare tokens", and the narrower
    version is the true one. Asked for the bare identifier, vector search finds
    it at rank 1 — a 384-dimensional vector of one rare token is a fine
    representation of that token. What it cannot survive is dilution: wrap the
    same identifier in ordinary words and the embedding is dominated by
    "problem", "reported" and "order", the rare token contributes almost
    nothing, and the top five come back full of other people's orders.

    BM25 is indifferent to the dilution. One token in one of 1,311 documents has
    a very high IDF whatever surrounds it in the query.

    Users ask questions, not tokens, so this is the realistic form.
    """
    question = f"What is the problem reported on order {NEEDLE_ORDER}?"

    lexical = [h.ticket_id for h in retriever.search(question, strategy="lexical")]
    assert lexical[0] == NEEDLE_TICKET, "BM25 failed on a diluted exact identifier"

    vector = [h.ticket_id for h in retriever.search(question, strategy="vector")]
    assert NEEDLE_TICKET not in vector, (
        "vector search now survives identifier dilution; if that is genuinely "
        "true the hybrid justification needs rewriting rather than this test "
        "relaxing"
    )

    hybrid = [h.ticket_id for h in retriever.search(question, strategy="hybrid")]
    assert NEEDLE_TICKET in hybrid, "fusion lost a document its lexical half ranked first"


def test_bare_identifier_is_found_by_every_strategy(retriever):
    """The control for the test above.

    Without it, that test's premise reads as "vectors cannot represent rare
    tokens", which is false and would send someone reworking the embedding model
    over a problem that does not exist.
    """
    for strategy in STRATEGIES:
        hits = [h.ticket_id for h in retriever.search(NEEDLE_ORDER, strategy=strategy)]
        assert hits[0] == NEEDLE_TICKET, f"{strategy} missed a bare identifier"


def test_rank_diagnostics_are_populated(retriever):
    """Each hit records which ranker found it and where. Without that, a hybrid
    result set is unattributable and a regression in one half is invisible."""
    hits = retriever.search("password reset email never arrives")
    assert any(h.vector_rank is not None for h in hits)
    assert any(h.lexical_rank is not None for h in hits)
    assert all(h.similarity is not None for h in hits if h.vector_rank is not None)


# ---------------------------------------------------------------------------
# abstention and generation
# ---------------------------------------------------------------------------


def test_abstains_on_an_off_domain_question(retriever):
    result = answer_question("What was the company share price last quarter?", retriever)
    assert result.mode == "abstained"
    assert "don't have" in result.answer


def test_answers_an_in_domain_question(retriever, cfg):
    result = answer_question("customers locked out of their accounts", retriever)
    assert result.mode in ("llm", "extractive")
    assert result.top_similarity >= cfg.abstain_similarity
    assert result.citations


def test_extractive_path_needs_no_api_key(retriever):
    """CONTRACTS.md requires the platform to run end to end without a key.

    Forced here regardless of the environment, so the path is exercised even on
    a machine that has one configured.
    """
    result = answer_question("why do customers ask for refunds?", retriever, use_llm=False)
    assert result.mode == "extractive"
    assert result.answer
    assert result.citations
    # The answer must be built from what was actually retrieved, not invented.
    retrieved = {p["ticket_id"] for p in result.passages}
    assert set(result.citations) <= retrieved


def test_answers_never_contain_pii(retriever, masker: Masker):
    """End to end: the text a caller receives carries no manifest term."""
    for question in (
        "customers locked out of their accounts",
        "orders that never arrived",
        "item arrived damaged",
    ):
        result = answer_question(question, retriever, use_llm=False)
        assert not masker.find_leaks(result.answer, limit=3)
        for passage in result.passages:
            assert not masker.find_leaks(passage["content"], limit=3)


def test_effort_scales_with_the_question():
    hits = []
    assert choose_effort("what is ticket T1 about", hits) in ("low", "medium")
    assert (
        choose_effort("compare refund and shipping complaints and say which is trending", hits)
        == "high"
    )


# ---------------------------------------------------------------------------
# the acceptance gate
# ---------------------------------------------------------------------------


def test_recall_at_5_meets_the_phase_1_bar(retriever, cfg, masker):
    """recall@5 >= 0.80 on the golden set, and meaningfully above chance."""
    golden = load_golden()
    corpus_path = cfg.seeds_dir / "rag" / "support_tickets.jsonl"
    relevance = build_relevance(golden, corpus_path, masker)
    with corpus_path.open(encoding="utf-8") as fh:
        corpus_size = sum(1 for line in fh if line.strip())

    report = evaluate_strategy(retriever, golden, relevance, "hybrid", 5, cfg.abstain_similarity)
    baseline = chance_recall(relevance, corpus_size, 5)

    assert report.recall_at_k >= 0.80, (
        f"recall@5 {report.recall_at_k:.2f} is below the Phase 1 acceptance bar"
    )
    assert report.recall_at_k > baseline * 2, (
        f"recall@5 {report.recall_at_k:.2f} is not meaningfully above the "
        f"chance baseline of {baseline:.2f}"
    )
    assert report.abstention_accuracy == 1.0, (
        f"abstention cases were answered: {[r.question_id for r in report.unanswerable if not r.abstained]}"
    )
    assert not report.false_abstentions, (
        f"answerable questions were refused: {report.false_abstentions}"
    )


def test_abstention_threshold_sits_inside_a_real_gap(retriever, cfg, masker):
    """A threshold outside the gap between the two sets is a number that happens
    to work, not a calibrated one."""
    golden = load_golden()
    corpus_path = cfg.seeds_dir / "rag" / "support_tickets.jsonl"
    relevance = build_relevance(golden, corpus_path, masker)
    report = evaluate_strategy(retriever, golden, relevance, "hybrid", 5, cfg.abstain_similarity)
    lowest_answerable, highest_unanswerable = report.separation
    assert highest_unanswerable < cfg.abstain_similarity < lowest_answerable, (
        f"threshold {cfg.abstain_similarity} is not between the unanswerable "
        f"ceiling ({highest_unanswerable:.3f}) and the answerable floor "
        f"({lowest_answerable:.3f})"
    )


# ---------------------------------------------------------------------------
# the role boundary
# ---------------------------------------------------------------------------


def test_retrieval_role_is_read_only(db, indexed):
    """Retrieval runs as analytics_ro. It must not be able to write the corpus
    it reads, or the AI layer's blast radius is the vector store."""
    import psycopg

    with pytest.raises(psycopg.errors.InsufficientPrivilege), db.cursor() as cur:
        cur.execute("DELETE FROM rag.chunks WHERE chunk_id = 'nope'")
    db.rollback()


def test_retrieval_role_cannot_reach_the_secure_schema(db):
    """The `secure` schema is where PII lives from Phase 2 onward.

    The full column-level proof (tests/test_pii_boundary.py, CONTRACTS.md §10)
    arrives with the table it selects from. This asserts the schema-level grant
    that makes it work, now, while the grants are being written.
    """
    with db.cursor() as cur:
        cur.execute("SELECT has_schema_privilege('secure', 'USAGE')")
        assert cur.fetchone()[0] is False, "analytics_ro can reach the PII schema"
        cur.execute("SELECT has_schema_privilege('rag', 'USAGE')")
        assert cur.fetchone()[0] is True, "analytics_ro cannot read the masked corpus"
