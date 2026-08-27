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
    resolve_anchors,
)
from meridian.rag.generate import answer_question, choose_effort
from meridian.rag.masking import Masker
from meridian.rag.retrieve import STRATEGIES

pytestmark = pytest.mark.db


# Order references occurring in exactly one ticket each, and the tickets they
# occur in. These are the cases that separate the two rankers — every other
# question on the golden set is a paraphrase of a template the intent grammar
# emits dozens of times, so all three strategies score 1.00 and the metric sits
# at its ceiling where it cannot detect anything.
#
# Read from the generator's manifest, and read as a *sample* rather than as one
# value. Both of those are repairs to real mistakes:
#
#   * They were literals. A bug fix in the generator — a hard-deleted customer
#     whose signup date landed after their deletion — shifted the order sequence
#     and left them naming a ticket that no longer existed, failing three tests
#     for a reason that had nothing to do with retrieval.
#
#   * It was one anchor, and one anchor is an anecdote. The claim it was chosen
#     to demonstrate ("vector search finds a bare rare identifier at rank 1 and
#     only loses it when the query dilutes it") turned out to be true of that
#     order and false in general — see the measurement in the test below.
@pytest.fixture(scope="session")
def anchors(cfg) -> dict:
    from meridian.rag.evaluate import load_anchors

    published = load_anchors(cfg.seeds_dir / "rag" / "support_tickets.jsonl")
    if not published.get("singleton_order_samples"):
        pytest.skip("seeds/manifest.json publishes no eval anchors — run `make seed`")
    return published


@pytest.fixture(scope="session")
def needle(anchors) -> tuple[str, str]:
    """The single anchor the golden question is scored on."""
    return anchors["singleton_order_id"], anchors["singleton_order_ticket_id"]


@pytest.fixture(scope="session")
def identifier_ranks(retriever, anchors) -> list[dict]:
    """Where each ranker puts the right ticket, for a spread of identifiers.

    Computed once and shared, because it is twelve orders times two phrasings
    times three strategies and each search embeds a query.
    """

    def rank(query: str, strategy: str, wanted: str) -> int | None:
        hits = [h.ticket_id for h in retriever.search(query, strategy=strategy)]
        return hits.index(wanted) + 1 if wanted in hits else None

    measured = []
    for sample in anchors["singleton_order_samples"]:
        order, ticket = sample["order_id"], sample["ticket_id"]
        diluted = f"What is the problem reported on order {order}?"
        measured.append(
            {
                "order_id": order,
                "ticket_id": ticket,
                "bare_vector": rank(order, "vector", ticket),
                "bare_lexical": rank(order, "lexical", ticket),
                "bare_hybrid": rank(order, "hybrid", ticket),
                "diluted_vector": rank(diluted, "vector", ticket),
                "diluted_lexical": rank(diluted, "lexical", ticket),
                "diluted_hybrid": rank(diluted, "hybrid", ticket),
            }
        )
    return measured


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


def test_lexical_ranks_every_rare_identifier_first(identifier_ranks):
    """BM25 finds a one-document token at rank 1, in every phrasing.

    This is the property that makes the lexical half worth its inverted index.
    A token appearing in one of 1,311 documents has an enormous IDF, and BM25
    is indifferent to what surrounds it in the query — so wrapping the
    identifier in ordinary words changes nothing.
    """
    misses = [r for r in identifier_ranks if r["bare_lexical"] != 1 or r["diluted_lexical"] != 1]
    assert not misses, f"BM25 failed to rank a unique identifier first: {misses}"


def test_vector_search_cannot_discriminate_between_identifiers(identifier_ranks):
    """The reason retrieval is hybrid rather than vector-only.

    The mechanism is narrower than "embeddings lose rare tokens", and narrower
    still than the version this test used to assert. Order references share a
    prefix and differ only in digits, so `O0000004` and `O0000034` embed to
    nearly the same point — a 384-dimensional sentence embedding has no way to
    represent "these two strings differ in one character, and that character is
    the whole query". Diluting the query with ordinary words makes it worse but
    is not the cause.

    Measured over twelve identifiers rather than argued from one: the earlier
    version of this test picked a single order, found that vector search ranked
    it first, and concluded embeddings handle bare identifiers fine. They do
    not — that order was one of the two in twelve where it happens to work.

    The bar is deliberately loose (fewer than half at rank 1). It is asserting
    that a real weakness exists, not pinning an exact score that a model
    upgrade would break.
    """
    total = len(identifier_ranks)
    bare_hits = sum(1 for r in identifier_ranks if r["bare_vector"] == 1)
    diluted_found = sum(1 for r in identifier_ranks if r["diluted_vector"] is not None)

    assert bare_hits * 2 < total, (
        f"vector search now ranks {bare_hits}/{total} bare identifiers first. If "
        f"that is genuinely true the hybrid justification needs rewriting rather "
        f"than this test relaxing."
    )
    assert diluted_found * 2 < total, (
        f"vector search retrieved {diluted_found}/{total} identifiers inside a "
        f"sentence — same conclusion as above."
    )


def test_fusion_mostly_recovers_what_the_lexical_half_found(identifier_ranks, cfg):
    """Fusion keeps most single-ranker wins, and measurably not all of them.

    The reason is RRF's agreement bias, and it is worth stating precisely
    because the arithmetic is counter-intuitive. A document one ranker puts
    first and the other misses entirely scores `1/(k+1)` = 1/61 = 0.0164. A
    document *both* rankers rank badly — 27th and 36th, say — scores
    `1/87 + 1/96` = 0.0219, and wins. Two mediocre agreements beat one perfect
    disagreement.

    That is inherent to RRF and is usually the behaviour you want: agreement
    between independent rankers is evidence. What sharpens it here is that
    `k` (60) is *larger than the candidate pool* (50). RRF's k was chosen in the
    original paper against TREC runs of a thousand documents, where it damps
    the top few ranks and leaves the rest of the curve to do the work. Against
    a pool of 50 it flattens the entire curve: the whole range from rank 1 to
    rank 50 spans 1/61 to 1/111, under a factor of two, so rank position
    carries much less signal than the mere fact of appearing on both lists.

    Not fixed here, deliberately. CONTRACTS.md §11 freezes k=60 and gives the
    reason — RRF's appeal is that it needs no per-corpus calibration, and a k
    tuned against this seed is exactly the calibration it exists to avoid. The
    honest response is to measure the cost and write it down, which is what this
    test is. `make rag-eval`'s per-strategy breakdown is where the same effect
    shows up as a number.

    The bar: fusion must keep at least three quarters of them. Below that, the
    lexical half is not contributing and the fusion is worth revisiting.
    """
    assert cfg.rrf_k > cfg.candidate_pool, (
        "the explanation above assumes k > pool; if that changed, re-measure "
        "rather than trusting this docstring"
    )

    total = len(identifier_ranks)
    kept = sum(
        1
        for r in identifier_ranks
        if r["bare_hybrid"] is not None and r["diluted_hybrid"] is not None
    )
    assert kept * 4 >= total * 3, (
        f"fusion kept only {kept}/{total} identifiers that BM25 ranked first — "
        f"below that the lexical half is being averaged away"
    )


def test_hybrid_is_never_worse_than_vector_alone_on_identifiers(identifier_ranks):
    """Whatever fusion costs against lexical, it must pay for itself elsewhere.

    This is the assertion that justifies hybrid over vector-only, which is the
    actual alternative — nobody was proposing a lexical-only retriever for a
    corpus of natural-language complaints. The comparison against BM25 above is
    a cost; this is the benefit.
    """
    vector_found = sum(1 for r in identifier_ranks if r["diluted_vector"] is not None)
    hybrid_found = sum(1 for r in identifier_ranks if r["diluted_hybrid"] is not None)
    assert hybrid_found > vector_found, (
        f"hybrid retrieved {hybrid_found} identifiers and vector alone "
        f"{vector_found} — fusion is adding nothing here"
    )


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
    corpus_path = cfg.seeds_dir / "rag" / "support_tickets.jsonl"
    # Anchors resolved before anything is asked: the question text and the
    # relevance spec both name `{singleton_order_id}`, and resolving only one
    # of them would ask about one order and score against another.
    golden = resolve_anchors(load_golden(), corpus_path)
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
    corpus_path = cfg.seeds_dir / "rag" / "support_tickets.jsonl"
    # Anchors resolved before anything is asked: the question text and the
    # relevance spec both name `{singleton_order_id}`, and resolving only one
    # of them would ask about one order and score against another.
    golden = resolve_anchors(load_golden(), corpus_path)
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
