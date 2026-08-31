"""The governance control: no manifest term ever reaches the vector store.

CONTRACTS.md §10 names this test as the proof of the PII claim, so what it does
and does not establish is worth being precise about.

The check is a case-folded substring scan, not the word-boundary match the
masker performs. That asymmetry is the point. A verifier built from the masker's
own regex can only show the masker is idempotent — it would agree with any bug
the masker has, including the \\b bug that silently broke phone-number matching
during development. Substring containment is strictly stronger than the
word-boundary match, so text passing this test holds no manifest term under any
tokenisation at all.

Two of these tests exist only to stop the others passing vacuously. "No PII was
found" is the expected result of a working masker and also the expected result of
an empty manifest, an empty corpus or an empty store, and those three outcomes
look identical in a test report.
"""

from __future__ import annotations

import pytest


def build_document(ticket: dict) -> str:
    """Exactly what the indexer embeds. Checking a different string would test a
    different system."""
    return f"Subject: {ticket['subject']}\n\n{ticket['body']}"


# ---------------------------------------------------------------------------
# guards against a vacuous pass
# ---------------------------------------------------------------------------


def test_manifest_is_populated(pii_manifest):
    counts = pii_manifest["counts"]
    assert counts["names"] > 100, "an empty manifest would make every leak test pass"
    assert counts["emails"] > 100
    assert counts["phones"] > 100


def test_corpus_really_contains_pii_before_masking(corpus, masker):
    """If the raw corpus held no PII, masking it would prove nothing."""
    leaky = sum(1 for t in corpus if masker.find_leaks(build_document(t), limit=1))
    assert leaky > 0, (
        "no ticket in the corpus contains a manifest term, so the masking tests "
        "below would pass against a masker that does nothing"
    )
    # The generator fills name/email/phone slots in roughly two thirds of its
    # templates; a sharp drop means the grammar changed and the corpus stopped
    # exercising masking.
    assert leaky > len(corpus) * 0.25, f"only {leaky}/{len(corpus)} tickets carry PII"


# ---------------------------------------------------------------------------
# the guarantee, without a database
# ---------------------------------------------------------------------------


def test_masking_removes_every_manifest_term_from_the_corpus(corpus, masker):
    """The strongest form of the check: the whole corpus, every term."""
    offenders = []
    for ticket in corpus:
        masked = masker.mask(build_document(ticket))
        leaks = masker.find_leaks(masked, limit=3)
        if leaks:
            offenders.append((ticket["ticket_id"], leaks))
    assert not offenders, f"{len(offenders)} tickets leaked PII, first: {offenders[:3]}"


def test_masking_is_deterministic(corpus, masker):
    """Masking twice must give the same bytes.

    The content hash is the indexer's skip key. If masking were
    order-dependent — say the dictionary alternation were built from an unsorted
    set — the hash would change on every run, nothing would ever be skipped, and
    the whole corpus would be re-embedded each time while looking correct.
    """
    for ticket in corpus[:200]:
        document = build_document(ticket)
        assert masker.mask(document) == masker.mask(document)


def test_masked_output_is_idempotent(corpus, masker):
    """Masking already-masked text changes nothing.

    Re-indexing masks text that may already carry placeholders. If a second pass
    rewrote them the content hash would differ from the first pass and the skip
    logic would thrash.
    """
    for ticket in corpus[:200]:
        once = masker.mask(build_document(ticket))
        assert masker.mask(once) == once


# ---------------------------------------------------------------------------
# the guarantee, against the real store
# ---------------------------------------------------------------------------


@pytest.mark.db
def test_vector_store_contains_no_manifest_terms(db, indexed, masker):
    """Read every chunk as analytics_ro and scan it for manifest terms."""
    with db.cursor() as cur:
        cur.execute("SELECT chunk_id, content FROM rag.chunks")
        rows = cur.fetchall()

    assert len(rows) == indexed
    offenders = []
    for chunk_id, content in rows:
        leaks = masker.find_leaks(content, limit=3)
        if leaks:
            offenders.append((chunk_id, leaks))
    assert not offenders, (
        f"{len(offenders)} chunks in rag.chunks contain manifest PII, first: {offenders[:3]}"
    )


@pytest.mark.db
def test_store_shows_masking_actually_ran(db, indexed):
    """Placeholders present in the store, in numbers matching the corpus.

    Without this, a store built from an empty-string masker — every chunk
    blank — would pass the leak test perfectly.
    """
    with db.cursor() as cur:
        cur.execute(
            "SELECT count(*) FILTER (WHERE content LIKE '%[CUSTOMER_NAME]%'), "
            "       count(*) FILTER (WHERE content LIKE '%[EMAIL]%'), "
            "       count(*) FILTER (WHERE content LIKE '%[PHONE]%') "
            "FROM rag.chunks"
        )
        names, emails, phones = cur.fetchone()
    assert names > 0 and emails > 0 and phones > 0, (
        f"redaction placeholders missing from the store "
        f"(names={names} emails={emails} phones={phones})"
    )


@pytest.mark.db
def test_redaction_placeholders_are_excluded_from_the_lexical_index(db, indexed):
    """Regression test.

    `[CUSTOMER_NAME]` analyses to the lexemes `custom` and `name`. Before the
    tsvector was built from redaction-stripped text they appeared in 394 of
    1,311 chunks — 30% of the corpus — and BM25 scored questions mentioning
    "customer" against redaction artefacts, matching exactly the documents whose
    text had been removed.
    """
    with db.cursor() as cur:
        cur.execute(
            "SELECT lexeme, count(*) FROM rag.chunk_terms "
            "WHERE lexeme IN ('custom','name','redact') GROUP BY lexeme"
        )
        found = dict(cur.fetchall())
    assert not found, f"redaction artefacts present in the BM25 index: {found}"
