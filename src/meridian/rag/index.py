"""Chunk, mask, embed and upsert the support-ticket corpus into rag.chunks.

    python -m meridian.rag.index --since 2025-01-01T00:00:00+00:00

CONTRACTS.md §5 makes this a module entrypoint runnable without Airflow, with
JSON-line logs on stdout and the frozen exit codes. Airflow calls this same
command; it never imports anything in here.

Two sources, one pipeline. `--source silver` reads `silver.support_tickets`,
which is the real path now that Phase 2 has built it; `--source seeds` reads
the generated JSONL directly and needs no warehouse at all. Phase 1 shipped with
only the second, because the whole point of building the AI layer first was that
it could not wait for the lake — and keeping that path working means the RAG
layer stays independently runnable rather than becoming something you can only
demonstrate after a full pipeline run.

Everything after the read is identical. Masking, chunking, hashing and the
upsert never learn which source they were fed from.

Two orderings in here are load-bearing
--------------------------------------
Mask, then chunk. Chunking first would let a full name straddle a boundary, and
"Amara Abernathy" split across two chunks is no longer a dictionary match — each
half still is, in this corpus, but relying on that is relying on the manifest
happening to hold both parts of every name.

Hash the masked text, never the raw. The hash is a skip key, so hashing raw
input means a later masking fix leaves already-indexed chunks alone and the leak
is permanent (CONTRACTS.md §10). Hashing the output makes "masking changed this
chunk" and "re-embed this chunk" the same event.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sys
import time
import uuid
from pathlib import Path

from ..db import UpstreamUnavailable, connect
from ..runlog import EXIT_ERROR, EXIT_OK, EXIT_UPSTREAM_UNAVAILABLE, RunLogger
from ..settings import settings
from .embeddings import get_embedder
from .masking import Masker, MaskReport

# Long enough that every ticket in this corpus is a single chunk, so the common
# case keeps its paragraph breaks and reads like the original. The splitting
# path below is still real code on a real path — a corpus of knowledge-base
# articles would exercise it immediately — it just does not fire here.
DEFAULT_MAX_WORDS = 220
DEFAULT_OVERLAP_WORDS = 40

MASKED_FIELDS = ("subject", "body")


# ---------------------------------------------------------------------------
# chunking
# ---------------------------------------------------------------------------


def split_words(text: str, max_words: int, overlap: int) -> list[str]:
    """Sliding window over words, with overlap.

    Below the limit the text is returned untouched — not re-joined from a split
    — so newlines and spacing survive for the overwhelmingly common single-chunk
    case. Retrieved passages get shown to people and to a model; collapsing them
    to single-spaced word soup for no reason helps neither.
    """
    words = text.split()
    if len(words) <= max_words:
        return [text]
    if overlap >= max_words:
        raise ValueError(f"overlap {overlap} must be smaller than max_words {max_words}")

    step = max_words - overlap
    out: list[str] = []
    for start in range(0, len(words), step):
        window = words[start : start + max_words]
        if not window:
            break
        out.append(" ".join(window))
        if start + max_words >= len(words):
            break
    return out


def build_chunks(
    ticket: dict,
    masker: Masker,
    *,
    max_words: int = DEFAULT_MAX_WORDS,
    overlap: int = DEFAULT_OVERLAP_WORDS,
):
    """One ticket -> its masked chunks, plus what masking caught."""
    document = f"Subject: {ticket['subject']}\n\n{ticket['body']}"
    masked, report = masker.mask_with_report(document)

    chunks = []
    for seq, piece in enumerate(split_words(masked, max_words, overlap)):
        chunks.append(
            {
                "chunk_id": f"{ticket['ticket_id']}:{seq:03d}",
                "ticket_id": ticket["ticket_id"],
                "chunk_seq": seq,
                "content": piece,
                "content_hash": hashlib.sha256(piece.encode("utf-8")).hexdigest(),
                "word_count": len(piece.split()),
                "created_ts": ticket["created_ts"],
            }
        )
    return chunks, report


def masking_fingerprint(masker: Masker) -> str:
    """Identifies the masking configuration that produced a row.

    Covers the policy's replacement tokens and fallback patterns and every term
    in the dictionary. Part of the skip predicate, so regenerating the seed or
    editing pii_classification.yml re-embeds the corpus rather than leaving a
    mix of old and new rows behind. That costs a few seconds on 1,311 chunks and
    buys the guarantee that a masking fix always reaches the whole index — a
    trade worth making in exactly one direction.
    """
    h = hashlib.sha256()
    h.update(json.dumps(masker.policy.replacements, sort_keys=True).encode())
    h.update(json.dumps(masker.policy.regex_fallback, sort_keys=True).encode())
    for term in masker.manifest_terms():
        h.update(term.encode("utf-8"))
        h.update(b"\x00")
    return h.hexdigest()


# ---------------------------------------------------------------------------
# corpus
# ---------------------------------------------------------------------------


def load_corpus_from_seeds(path: Path, since: dt.datetime | None = None) -> list[dict]:
    if not path.is_file():
        raise FileNotFoundError(
            f"ticket corpus not found at {path}. Run `make seed` first, or pass "
            f"--source silver to read the warehouse instead."
        )
    rows = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            if since is not None and dt.datetime.fromisoformat(row["created_ts"]) < since:
                continue
            rows.append(row)
    return rows


def load_corpus_from_silver(since: dt.datetime | None = None) -> list[dict]:
    """Read the ticket corpus from the warehouse, as `rag_indexer`.

    That role holds SELECT on this one table and nothing else in `silver`
    (CONTRACTS.md §1), which is what makes "the AI layer cannot reach the rest of
    the warehouse" a property of the cluster rather than of this function.

    `created_ts` is normalised back to an ISO string so both sources hand the
    rest of the module identical rows. Otherwise every downstream step would
    have to know which source fed it, which is exactly the coupling that keeping
    two sources is meant to avoid.
    """
    sql = "SELECT ticket_id, subject, body, created_ts FROM silver.support_tickets"
    params: tuple = ()
    if since is not None:
        sql += " WHERE created_ts >= %s"
        params = (since,)

    with connect("rag_indexer", vectors=False) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        rows = [
            {
                "ticket_id": r[0],
                "subject": r[1],
                "body": r[2],
                "created_ts": r[3].isoformat(),
            }
            for r in cur.fetchall()
        ]

    if not rows and since is None:
        raise RuntimeError(
            "silver.support_tickets is empty. Run the batch pipeline first "
            "(`make ingest && make silver && make load-warehouse`), or pass "
            "--source seeds to index the generated corpus directly."
        )
    return rows


# ---------------------------------------------------------------------------
# store
# ---------------------------------------------------------------------------

UPSERT = """
INSERT INTO rag.chunks (
    chunk_id, ticket_id, chunk_seq, content, content_hash,
    masking_fingerprint, embedding_model, embedding, word_count,
    created_ts, source, indexed_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (chunk_id) DO UPDATE SET
    ticket_id           = EXCLUDED.ticket_id,
    chunk_seq           = EXCLUDED.chunk_seq,
    content             = EXCLUDED.content,
    content_hash        = EXCLUDED.content_hash,
    masking_fingerprint = EXCLUDED.masking_fingerprint,
    embedding_model     = EXCLUDED.embedding_model,
    embedding           = EXCLUDED.embedding,
    word_count          = EXCLUDED.word_count,
    created_ts          = EXCLUDED.created_ts,
    source              = EXCLUDED.source,
    indexed_at          = now()
"""


def apply_ddl(conn) -> None:
    ddl = (Path(__file__).parent / "ddl.sql").read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def existing_state(conn, ticket_ids: list[str]) -> dict[str, tuple[str, str, str]]:
    """chunk_id -> (content_hash, masking_fingerprint, embedding_model)."""
    if not ticket_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT chunk_id, content_hash, masking_fingerprint, embedding_model "
            "FROM rag.chunks WHERE ticket_id = ANY(%s)",
            (ticket_ids,),
        )
        return {r[0]: (r[1], r[2], r[3]) for r in cur.fetchall()}


REFRESH_TERMS = """
INSERT INTO rag.chunk_terms (chunk_id, lexeme, tf)
SELECT c.chunk_id, t.lexeme, coalesce(array_length(t.positions, 1), 1)
FROM rag.chunks c, unnest(c.content_tsv) AS t(lexeme, positions, weights)
WHERE c.chunk_id = ANY(%s)
ON CONFLICT (chunk_id, lexeme) DO UPDATE SET tf = EXCLUDED.tf
"""


def refresh_lexical_index(conn, written_ids: list[str]) -> int:
    """Rebuild the BM25 inverted index for the chunks that changed.

    Scoped to what actually changed, plus anything with a NULL doc_len. That
    second clause is not defensive padding: it is what makes the step
    self-healing, so an interrupted run — or a chunk written before this table
    existed — is repaired by the next run instead of scoring as if it had no
    text in it. A chunk missing from rag.chunk_terms is not an error anything
    would raise; it is a document BM25 silently cannot find.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT chunk_id FROM rag.chunks WHERE doc_len IS NULL OR chunk_id = ANY(%s)",
            (written_ids,),
        )
        stale = [r[0] for r in cur.fetchall()]
        if not stale:
            return 0

        # Delete first: a chunk whose text shrank keeps terms it no longer
        # contains, and those inflate its length normalisation forever.
        cur.execute("DELETE FROM rag.chunk_terms WHERE chunk_id = ANY(%s)", (stale,))
        cur.execute(REFRESH_TERMS, (stale,))
        cur.execute(
            "UPDATE rag.chunks c SET doc_len = coalesce(s.dl, 0) "
            "FROM (SELECT ch.chunk_id, sum(ct.tf) AS dl FROM rag.chunks ch "
            "      LEFT JOIN rag.chunk_terms ct ON ct.chunk_id = ch.chunk_id "
            "      WHERE ch.chunk_id = ANY(%s) GROUP BY ch.chunk_id) s "
            "WHERE c.chunk_id = s.chunk_id",
            (stale,),
        )
    conn.commit()
    return len(stale)


# ---------------------------------------------------------------------------
# entrypoint
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="meridian.rag.index",
        description="Chunk, mask, embed and upsert the ticket corpus into rag.chunks",
    )
    p.add_argument(
        "--since",
        type=dt.datetime.fromisoformat,
        default=None,
        help="only index tickets created at or after this ISO-8601 timestamp",
    )
    p.add_argument(
        "--source",
        choices=["silver", "seeds"],
        default="silver",
        help="where to read tickets from. `silver` is the pipeline path; `seeds` "
        "reads the generated corpus and needs no warehouse.",
    )
    p.add_argument("--limit", type=int, default=None, help="index at most N tickets")
    p.add_argument("--max-words", type=int, default=DEFAULT_MAX_WORDS)
    p.add_argument("--overlap", type=int, default=DEFAULT_OVERLAP_WORDS)
    p.add_argument("--batch-size", type=int, default=128)
    p.add_argument(
        "--rebuild",
        action="store_true",
        help="truncate rag.chunks first. The skip logic makes this unnecessary; "
        "it exists for when you want to prove that.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="chunk and mask but do not embed or write. Useful for checking what "
        "masking would do before spending the embedding pass.",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = settings()
    log = RunLogger("rag.index")
    started = dt.datetime.now(dt.UTC)
    t_start = time.perf_counter()

    log.emit(
        "start",
        source=args.source,
        since=args.since.isoformat() if args.since else None,
        embedding_model=cfg.embedding_model,
        rebuild=args.rebuild,
        dry_run=args.dry_run,
    )

    # --- mask + chunk -----------------------------------------------------
    masker = Masker.from_project()
    fingerprint = masking_fingerprint(masker)

    if args.source == "silver":
        tickets = load_corpus_from_silver(args.since)
    else:
        tickets = load_corpus_from_seeds(
            cfg.seeds_dir / "rag" / "support_tickets.jsonl", args.since
        )
    if args.limit is not None:
        tickets = tickets[: args.limit]

    all_chunks: list[dict] = []
    pii = MaskReport()
    with log.timed("chunk_and_mask", entity="support_tickets", rows_in=len(tickets)) as extra:
        for ticket in tickets:
            chunks, report = build_chunks(
                ticket, masker, max_words=args.max_words, overlap=args.overlap
            )
            all_chunks.extend(chunks)
            pii.merge(report)
        extra["rows_out"] = len(all_chunks)
        extra["pii_masked"] = pii.total
        extra["pii_by_dictionary"] = sum(pii.dictionary.values())
        extra["pii_by_regex"] = sum(pii.regex.values())

    # A chunk that still contains a manifest term must never reach the store.
    # Checking here, before the embedding pass, means the failure costs nothing
    # and names the ticket rather than surfacing later as a row in a table.
    for chunk in all_chunks:
        leaked = masker.find_leaks(chunk["content"], limit=3)
        if leaked:
            log.emit("pii_leak_detected", chunk_id=chunk["chunk_id"], terms=leaked)
            raise RuntimeError(
                f"masking left {leaked!r} in chunk {chunk['chunk_id']}. Refusing to "
                f"embed: an unmasked chunk that reaches the store is skipped on "
                f"every later run and never re-embedded (CONTRACTS.md §10)."
            )

    if args.dry_run:
        log.emit(
            "dry_run_complete",
            rows_in=len(tickets),
            rows_out=len(all_chunks),
            pii_masked=pii.total,
            duration_ms=round((time.perf_counter() - t_start) * 1000, 1),
        )
        return EXIT_OK

    # --- store ------------------------------------------------------------
    run_id = uuid.UUID(log.run_id)
    conn = connect("rag_indexer")
    try:
        apply_ddl(conn)

        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO rag.index_runs (run_id, started_at, status, source, "
                "since_ts, embedding_model) VALUES (%s, %s, 'RUNNING', %s, %s, %s)",
                (run_id, started, args.source, args.since, cfg.embedding_model),
            )
        conn.commit()

        if args.rebuild:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE rag.chunks CASCADE")
            conn.commit()
            log.emit("rebuild", detail="rag.chunks truncated")

        ticket_ids = [t["ticket_id"] for t in tickets]
        known = existing_state(conn, ticket_ids)

        to_embed = [
            c
            for c in all_chunks
            if known.get(c["chunk_id"]) != (c["content_hash"], fingerprint, cfg.embedding_model)
        ]
        skipped = len(all_chunks) - len(to_embed)
        log.emit(
            "skip_analysis",
            chunks_total=len(all_chunks),
            chunks_to_embed=len(to_embed),
            chunks_skipped=skipped,
        )

        embedded = 0
        if to_embed:
            embedder = get_embedder(cfg.embedding_model)
            with log.timed("embed", rows_in=len(to_embed)) as extra:
                for start in range(0, len(to_embed), args.batch_size):
                    batch = to_embed[start : start + args.batch_size]
                    vectors = embedder.embed_documents([c["content"] for c in batch])
                    with conn.cursor() as cur:
                        cur.executemany(
                            UPSERT,
                            [
                                (
                                    c["chunk_id"],
                                    c["ticket_id"],
                                    c["chunk_seq"],
                                    c["content"],
                                    c["content_hash"],
                                    fingerprint,
                                    cfg.embedding_model,
                                    v,
                                    c["word_count"],
                                    c["created_ts"],
                                    "support_tickets",
                                )
                                for c, v in zip(batch, vectors, strict=True)
                            ],
                        )
                    conn.commit()
                    embedded += len(batch)
                extra["rows_out"] = embedded

        # --- BM25 statistics ---------------------------------------------
        # After the vectors, before the counts: the lexical half of retrieval is
        # only as fresh as this table, and a chunk present in rag.chunks but
        # absent here is invisible to BM25 while looking perfectly indexed.
        with log.timed("refresh_lexical_index") as extra:
            extra["rows_out"] = refresh_lexical_index(conn, [c["chunk_id"] for c in to_embed])

        # --- reconcile deletions -----------------------------------------
        # Upserting alone leaves orphans behind: a ticket that lost text drops a
        # chunk, and a ticket removed from the corpus keeps all of them. Both
        # stay retrievable forever, which looks exactly like a retrieval bug.
        deleted = 0
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM rag.chunks WHERE ticket_id = ANY(%s) AND chunk_id <> ALL(%s)",
                (ticket_ids, [c["chunk_id"] for c in all_chunks]),
            )
            deleted += cur.rowcount
            # Only a full pass can tell an absent ticket from an out-of-window
            # one, so orphan removal is skipped when --since narrowed the input.
            if args.since is None and args.limit is None:
                cur.execute("DELETE FROM rag.chunks WHERE ticket_id <> ALL(%s)", (ticket_ids,))
                deleted += cur.rowcount
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                "SELECT (SELECT count(*) FROM rag.chunks), "
                "       (SELECT count(embedding) FROM rag.chunks), "
                "       (SELECT count(*) FROM rag.chunk_terms)"
            )
            total_rows, with_vectors, term_rows = cur.fetchone()

        duration_ms = round((time.perf_counter() - t_start) * 1000, 1)
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE rag.index_runs SET completed_at = now(), status = 'SUCCESS', "
                "documents_read = %s, chunks_total = %s, chunks_embedded = %s, "
                "chunks_skipped = %s, chunks_deleted = %s, pii_masked_total = %s, "
                "pii_by_dictionary = %s, pii_by_regex = %s, duration_ms = %s "
                "WHERE run_id = %s",
                (
                    len(tickets),
                    len(all_chunks),
                    embedded,
                    skipped,
                    deleted,
                    pii.total,
                    sum(pii.dictionary.values()),
                    sum(pii.regex.values()),
                    duration_ms,
                    run_id,
                ),
            )
        conn.commit()

        log.emit(
            "done",
            entity="support_tickets",
            rows_in=len(tickets),
            rows_out=embedded,
            chunks_skipped=skipped,
            chunks_deleted=deleted,
            store_rows=total_rows,
            store_rows_with_vectors=with_vectors,
            lexical_terms=term_rows,
            pii_masked=pii.total,
            duration_ms=duration_ms,
        )
    except Exception as exc:
        try:
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE rag.index_runs SET completed_at = now(), status = 'FAILED', "
                    "message = %s WHERE run_id = %s",
                    (f"{type(exc).__name__}: {exc}", run_id),
                )
            conn.commit()
        except Exception:  # noqa: BLE001 - the original failure is what matters
            pass
        raise
    finally:
        conn.close()

    return EXIT_OK


def cli() -> int:
    try:
        return main()
    except UpstreamUnavailable as exc:
        print(json.dumps({"event": "upstream_unavailable", "error": str(exc)}), flush=True)
        return EXIT_UPSTREAM_UNAVAILABLE
    except Exception as exc:  # noqa: BLE001 - top-level boundary, exit code is the contract
        print(
            json.dumps({"event": "error", "error": f"{type(exc).__name__}: {exc}"}),
            flush=True,
        )
        return EXIT_ERROR


if __name__ == "__main__":
    sys.exit(cli())
