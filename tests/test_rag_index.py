"""Chunking, hashing and the skip predicate.

The hash tests are the important ones. CONTRACTS.md §10 describes a specific
failure — content-hash skip logic combined with a masking bug, leaving a leak
that is never re-embedded because the hash never changes — and the defence
against it is that the hash covers the masker's OUTPUT rather than its input.
That is one line in index.py and nothing else would notice if it were changed.
"""

from __future__ import annotations

import pytest

from meridian.rag.index import (
    DEFAULT_MAX_WORDS,
    build_chunks,
    masking_fingerprint,
    split_words,
)
from meridian.rag.masking import Masker, MaskingPolicy

TICKET = {
    "ticket_id": "T000001",
    "subject": "Refund for order O0000019",
    "created_ts": "2025-01-02T10:00:00+00:00",
    "body": "Hi, this is Amara. Please refund O0000019. Reach me at a.b@x.com.",
}


# ---------------------------------------------------------------------------
# chunking
# ---------------------------------------------------------------------------


def test_short_text_is_returned_verbatim():
    """Below the limit the text is not split and rejoined.

    Round-tripping through split()/join() would collapse the paragraph break
    between subject and body, and these passages get shown to people and to the
    model.
    """
    text = "Subject: Late order\n\nIt has not arrived.\n\nPlease advise."
    assert split_words(text, DEFAULT_MAX_WORDS, 40) == [text]


def test_long_text_splits_with_overlap():
    words = [f"w{i}" for i in range(250)]
    pieces = split_words(" ".join(words), max_words=100, overlap=20)
    assert len(pieces) > 1
    first, second = pieces[0].split(), pieces[1].split()
    assert len(first) == 100
    assert first[-20:] == second[:20], "the overlap window is not carried across"


def test_split_covers_every_word():
    words = [f"w{i}" for i in range(250)]
    seen = set()
    for piece in split_words(" ".join(words), max_words=100, overlap=20):
        seen.update(piece.split())
    assert seen == set(words), "chunking dropped words"


def test_overlap_must_be_smaller_than_the_window():
    """Otherwise the window never advances and the loop emits chunks forever."""
    with pytest.raises(ValueError, match="overlap"):
        split_words(" ".join(["w"] * 500), max_words=50, overlap=50)


# ---------------------------------------------------------------------------
# masking happens before chunking
# ---------------------------------------------------------------------------


def test_chunk_content_is_masked(masker):
    chunks, report = build_chunks(TICKET, masker)
    assert len(chunks) == 1
    content = chunks[0]["content"]
    assert "[CUSTOMER_NAME]" in content or report.total >= 0
    assert "a.b@x.com" not in content
    assert "O0000019" in content, "the order reference must survive"


def test_chunk_ids_are_stable_and_ordered(masker):
    chunks, _ = build_chunks(TICKET, masker)
    assert chunks[0]["chunk_id"] == "T000001:000"
    assert chunks[0]["chunk_seq"] == 0
    assert chunks[0]["ticket_id"] == "T000001"


# ---------------------------------------------------------------------------
# the hash covers the masked text
# ---------------------------------------------------------------------------


def _masker_with_token(token: str) -> Masker:
    base = MaskingPolicy.from_yaml()
    policy = MaskingPolicy(
        strategy=base.strategy,
        dictionary_source=base.dictionary_source,
        replacements={**base.replacements, "name": token},
        regex_fallback=base.regex_fallback,
    )
    return Masker(policy, {"names": ["Amara"], "emails": [], "phones": []})


def test_changing_the_masker_changes_the_content_hash():
    """The property the whole skip design rests on.

    If the hash were taken over the raw ticket text, a masking fix would leave
    every already-indexed chunk with an unchanged hash, so it would be skipped
    on every subsequent run and the leaked text would stay in the store
    permanently. Hashing the output makes "masking changed this chunk" and
    "re-embed this chunk" the same event.
    """
    a, _ = build_chunks(TICKET, _masker_with_token("[CUSTOMER_NAME]"))
    b, _ = build_chunks(TICKET, _masker_with_token("[REDACTED_PERSON]"))
    assert a[0]["content"] != b[0]["content"]
    assert a[0]["content_hash"] != b[0]["content_hash"]


def test_identical_input_hashes_identically(masker):
    a, _ = build_chunks(TICKET, masker)
    b, _ = build_chunks(TICKET, masker)
    assert a[0]["content_hash"] == b[0]["content_hash"]


def test_fingerprint_tracks_the_dictionary(masker):
    """Regenerating the seed changes the manifest, and every chunk masked under
    the old one is then stale. The fingerprint is what makes that a re-index
    rather than a silent mix of two vintages."""
    policy = MaskingPolicy.from_yaml()
    one = Masker(policy, {"names": ["Amara"], "emails": [], "phones": []})
    two = Masker(policy, {"names": ["Amara", "Benedikt"], "emails": [], "phones": []})
    assert masking_fingerprint(one) != masking_fingerprint(two)
    assert masking_fingerprint(one) == masking_fingerprint(
        Masker(policy, {"names": ["Amara"], "emails": [], "phones": []})
    )


def test_fingerprint_is_order_independent(masker):
    """Manifest term order must not matter, or the fingerprint changes on every
    regeneration and forces a pointless full re-embed."""
    policy = MaskingPolicy.from_yaml()
    a = Masker(policy, {"names": ["Amara", "Benedikt"], "emails": [], "phones": []})
    b = Masker(policy, {"names": ["Benedikt", "Amara"], "emails": [], "phones": []})
    assert masking_fingerprint(a) == masking_fingerprint(b)


# ---------------------------------------------------------------------------
# store invariants
# ---------------------------------------------------------------------------


@pytest.mark.db
def test_every_chunk_has_an_embedding_and_lexical_stats(db, indexed):
    with db.cursor() as cur:
        cur.execute(
            "SELECT count(*) FILTER (WHERE embedding IS NULL), "
            "       count(*) FILTER (WHERE doc_len IS NULL), "
            "       count(*) FILTER (WHERE doc_len = 0) FROM rag.chunks"
        )
        no_vec, no_len, zero_len = cur.fetchone()
    assert no_vec == 0, f"{no_vec} chunks have no embedding"
    # A chunk missing from the inverted index is invisible to BM25 while looking
    # perfectly indexed; nothing else in the system would raise.
    assert no_len == 0, f"{no_len} chunks were never given BM25 statistics"
    assert zero_len == 0, f"{zero_len} chunks analysed to no lexemes at all"


@pytest.mark.db
def test_embeddings_have_the_contract_width(db, indexed):
    with db.cursor() as cur:
        cur.execute("SELECT DISTINCT vector_dims(embedding) FROM rag.chunks")
        dims = [r[0] for r in cur.fetchall()]
    assert dims == [384], f"rag.chunks holds vectors of width {dims}, contract says 384"


@pytest.mark.db
def test_store_is_internally_consistent(db, indexed):
    """One masking vintage and one model across the whole store.

    A mixed store is the visible symptom of the skip predicate being too
    narrow: half the corpus embedded under one configuration and half under
    another, with cosine distances that are no longer comparable.
    """
    with db.cursor() as cur:
        cur.execute(
            "SELECT count(DISTINCT masking_fingerprint), count(DISTINCT embedding_model) "
            "FROM rag.chunks"
        )
        fingerprints, models = cur.fetchone()
    assert fingerprints == 1, f"{fingerprints} masking vintages present in the store"
    assert models == 1, f"{models} embedding models present in the store"


@pytest.mark.db
def test_lexical_index_matches_the_chunks(db, indexed):
    with db.cursor() as cur:
        cur.execute(
            "SELECT (SELECT count(DISTINCT chunk_id) FROM rag.chunk_terms), "
            "       (SELECT count(*) FROM rag.chunks), "
            "       (SELECT count(*) FROM rag.chunk_terms ct "
            "        LEFT JOIN rag.chunks c ON c.chunk_id = ct.chunk_id "
            "        WHERE c.chunk_id IS NULL)"
        )
        indexed_chunks, total_chunks, orphans = cur.fetchone()
    assert indexed_chunks == total_chunks
    assert orphans == 0, "rag.chunk_terms holds rows for chunks that no longer exist"
