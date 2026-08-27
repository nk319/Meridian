"""Hybrid retrieval: lexical and vector rankings fused with RRF, in one query.

    python -m meridian.rag.retrieve "customer charged twice for the same order"

Why hybrid
----------
The two rankers fail in opposite directions. Vector search finds "I was billed
for this order more than once" from a query about duplicate charges, which no
lexical ranker will match. What it loses is an identifier diluted by ordinary
words: asked for "O0000019" alone it returns the right ticket at rank 1, but
asked "What is the problem reported on order O0000019?" the embedding is
dominated by problem/reported/order and the rare token contributes almost
nothing — the right ticket falls out of the top five entirely. BM25 is
indifferent to that dilution, because one token in one of 1,311 documents has a
very high IDF however the query is phrased.

Users ask questions rather than tokens, so both failure modes are live at once,
and running a single ranker means accepting one of them permanently. Measured
rather than asserted: `make rag-eval` scores each half on its own, and vector-only
is the strategy that misses the identifier question.

Why RRF, and why it is one query
--------------------------------
Reciprocal Rank Fusion combines rankings by position, `1/(k + rank)`, never by
score. That matters because the two scores are not comparable and cannot be made
so: cosine similarity is bounded in [-1, 1] and Postgres's text rank is an
unbounded, corpus-dependent float. Any weighted sum of them needs a
normalisation constant that is really a hidden tuning parameter, and it silently
goes stale as the corpus grows. RRF has no such constant — k=60 is from the
original paper and is not corpus-tuned.

One SQL statement rather than two round trips and a merge in Python: the fusion
is a join, the database is better at joins, and doing it here means there is
exactly one implementation of the ranking rather than one per caller.

On BM25
-------
The lexical half is real BM25, scored in SQL from the rag.chunk_terms inverted
index the indexer maintains. Postgres's built-in `ts_rank_cd` was tried first
and is not a substitute: it weights term frequency and proximity but has no
inverse document frequency at all, so on a corpus where every ticket contains
the word "order" it cannot distinguish that from an order number appearing in
three documents. IDF is most of what makes BM25 work on this corpus.

The other thing built-in ranking got wrong is query semantics.
`websearch_to_tsquery` joins terms with AND, so a natural-language question of
eight words matched exactly zero of the 1,311 tickets — the lexical half
contributed nothing and "hybrid" was quietly vector-only. BM25 scores over the
union of query terms, weighting each by rarity, which is the behaviour a
question needs.

Connects as `analytics_ro`
--------------------------
The same read-only role the dashboard and every /v1/ai/* handler use, which
holds no grant on `secure`, `oltp` or `silver`. Retrieval running under the
least-privileged role available is what makes "the AI layer cannot reach PII" a
property of the cluster instead of a property of this file.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from dataclasses import asdict, dataclass

from ..db import UpstreamUnavailable, connect
from ..runlog import EXIT_ERROR, EXIT_OK, EXIT_UPSTREAM_UNAVAILABLE
from ..settings import settings
from .embeddings import BGE_QUERY_INSTRUCTION, get_embedder

STRATEGIES = ("hybrid", "vector", "lexical")

# The vector and lexical candidate pools are built independently, each ranked and
# truncated to :pool, then fused. Truncating before the join is what lets the
# HNSW and GIN indexes serve their halves: a row_number() computed over the whole
# table before a LIMIT would force a sequential scan and quietly turn an indexed
# lookup into a full scan that still returns correct answers.
# BM25 constants. The literature's defaults, and deliberately not exposed as
# settings: k1 and b are the kind of knob that gets tuned once against a handful
# of queries and then silently overfits the eval set it was tuned on.
BM25_K1 = 1.2
BM25_B = 0.75

# The vector and lexical candidate pools are built independently, each ranked and
# truncated to :pool, then fused. Truncating before the join is what lets the
# HNSW index serve its half: a row_number() computed over the whole table before
# a LIMIT would force a sequential scan and quietly turn an indexed lookup into a
# full scan that still returns correct answers.
SEARCH_SQL = """
WITH corpus AS (
    -- N and avgdl, computed rather than materialised. On 1,311 rows this is
    -- sub-millisecond and can never be stale; at a hundred times the size it
    -- becomes a maintained table, and that is the point at which staleness
    -- becomes a problem worth having.
    SELECT count(*)::float AS n_docs,
           coalesce(avg(doc_len)::float, 1.0) AS avg_len
    FROM rag.chunks
),
q_lex AS (
    -- The query through the same analyser as the corpus: same stemming, same
    -- stopword list. Analysing the two sides differently is the classic way to
    -- get a lexical ranker that silently matches nothing.
    SELECT DISTINCT lexeme
    FROM unnest(to_tsvector('english', %(qtext)s)) AS t(lexeme, positions, weights)
),
q_idf AS (
    -- Robertson-Sparck Jones IDF with the +1 smoothing, which keeps the score
    -- non-negative for a term appearing in more than half the corpus. Without
    -- it, "order" contributes a negative score here and actively pushes down
    -- documents that contain the user's own words.
    SELECT q.lexeme, ln(1.0 + (c.n_docs - d.df + 0.5) / (d.df + 0.5)) AS idf
    FROM q_lex q
    CROSS JOIN corpus c
    JOIN LATERAL (
        SELECT count(*)::float AS df
        FROM rag.chunk_terms ct
        WHERE ct.lexeme = q.lexeme
    ) d ON d.df > 0
),
lex_pool AS (
    SELECT ct.chunk_id,
           sum(
               q.idf * (ct.tf * (%(k1)s::float + 1.0))
               / (ct.tf + %(k1)s::float
                   * (1.0 - %(b)s::float
                      + %(b)s::float * coalesce(ch.doc_len, 0) / c.avg_len))
           ) AS score
    FROM rag.chunk_terms ct
    JOIN q_idf q        ON q.lexeme = ct.lexeme
    JOIN rag.chunks ch  ON ch.chunk_id = ct.chunk_id
    CROSS JOIN corpus c
    GROUP BY ct.chunk_id
    ORDER BY score DESC, ct.chunk_id
    LIMIT %(pool)s
),
lex AS (
    SELECT chunk_id,
           row_number() OVER (ORDER BY score DESC, chunk_id) AS rnk,
           score
    FROM lex_pool
),
vec_pool AS (
    SELECT chunk_id, embedding <=> %(qvec)s AS distance
    FROM rag.chunks
    WHERE embedding IS NOT NULL
    ORDER BY embedding <=> %(qvec)s
    LIMIT %(pool)s
),
vec AS (
    SELECT chunk_id,
           row_number() OVER (ORDER BY distance, chunk_id) AS rnk,
           1.0 - distance AS similarity
    FROM vec_pool
),
fused AS (
    SELECT
        coalesce(vec.chunk_id, lex.chunk_id) AS chunk_id,
        vec.rnk        AS vector_rank,
        lex.rnk        AS lexical_rank,
        vec.similarity AS similarity,
        lex.score      AS lexical_score,
        coalesce(%(w_vec)s::float / (%(k)s + vec.rnk), 0.0)
      + coalesce(%(w_lex)s::float / (%(k)s + lex.rnk), 0.0) AS rrf_score
    FROM vec FULL OUTER JOIN lex ON vec.chunk_id = lex.chunk_id
)
SELECT f.chunk_id, c.ticket_id, c.chunk_seq, c.content, c.created_ts,
       f.rrf_score, f.vector_rank, f.lexical_rank, f.similarity, f.lexical_score
FROM fused f
JOIN rag.chunks c ON c.chunk_id = f.chunk_id
WHERE (%(require_vector)s IS FALSE OR f.vector_rank IS NOT NULL)
  AND (%(require_lexical)s IS FALSE OR f.lexical_rank IS NOT NULL)
ORDER BY f.rrf_score DESC, f.chunk_id
LIMIT %(top_k)s
"""


@dataclass(frozen=True)
class RetrievedChunk:
    chunk_id: str
    ticket_id: str
    chunk_seq: int
    content: str
    created_ts: dt.datetime
    rrf_score: float
    vector_rank: int | None
    lexical_rank: int | None
    similarity: float | None
    lexical_score: float | None

    def as_dict(self) -> dict:
        d = asdict(self)
        d["created_ts"] = self.created_ts.isoformat()
        return d


class Retriever:
    """Reusable retriever. Holds a connection and an embedder, both expensive
    to build and both cheap to keep."""

    def __init__(self, conn, embedder=None, cfg=None) -> None:
        self.cfg = cfg or settings()
        self.conn = conn
        self.embedder = embedder or get_embedder(self.cfg.embedding_model)

    @classmethod
    def open(cls, *, query_instruction: str = BGE_QUERY_INSTRUCTION) -> Retriever:
        cfg = settings()
        return cls(
            connect("analytics_ro"),
            get_embedder(cfg.embedding_model, query_instruction),
            cfg,
        )

    def search(
        self,
        question: str,
        *,
        top_k: int | None = None,
        strategy: str = "hybrid",
        pool: int | None = None,
    ) -> list[RetrievedChunk]:
        if strategy not in STRATEGIES:
            raise ValueError(f"strategy must be one of {STRATEGIES}, got {strategy!r}")

        params = {
            "qvec": self.embedder.embed_query(question),
            "qtext": question,
            "pool": pool or self.cfg.candidate_pool,
            "k": self.cfg.rrf_k,
            "k1": BM25_K1,
            "b": BM25_B,
            "top_k": top_k or self.cfg.top_k,
            # Both pools are always built so the diagnostics (which ranker found
            # this, at what rank) are populated for every strategy. Weights and
            # the require_* filters are what actually select the strategy, so
            # the single-ranker baselines exercise the same SQL path the hybrid
            # does — a baseline measured through different code measures the
            # code as much as the strategy.
            "w_vec": 1.0 if strategy in ("hybrid", "vector") else 0.0,
            "w_lex": 1.0 if strategy in ("hybrid", "lexical") else 0.0,
            "require_vector": strategy == "vector",
            "require_lexical": strategy == "lexical",
        }
        with self.conn.cursor() as cur:
            cur.execute(SEARCH_SQL, params)
            rows = cur.fetchall()

        return [
            RetrievedChunk(
                chunk_id=r[0],
                ticket_id=r[1],
                chunk_seq=r[2],
                content=r[3],
                created_ts=r[4],
                rrf_score=float(r[5]),
                vector_rank=r[6],
                lexical_rank=r[7],
                similarity=float(r[8]) if r[8] is not None else None,
                lexical_score=float(r[9]) if r[9] is not None else None,
            )
            for r in rows
        ]

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> Retriever:
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def top_similarity(chunks: list[RetrievedChunk]) -> float:
    """Best cosine similarity among the results, or -1.0 if none carry one.

    This is the abstention signal. RRF scores cannot serve: `1/(k+1)` is the
    same number whether the top hit is a paraphrase or unrelated, because rank
    carries no notion of closeness. Similarity does.
    """
    sims = [c.similarity for c in chunks if c.similarity is not None]
    return max(sims) if sims else -1.0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="meridian.rag.retrieve", description="Hybrid search over the ticket corpus"
    )
    p.add_argument("question")
    p.add_argument("--top-k", type=int, default=None)
    p.add_argument("--strategy", choices=STRATEGIES, default="hybrid")
    p.add_argument("--json", action="store_true", help="emit JSON instead of a readable table")
    args = p.parse_args(argv)

    with Retriever.open() as r:
        hits = r.search(args.question, top_k=args.top_k, strategy=args.strategy)

    if args.json:
        print(json.dumps([h.as_dict() for h in hits], indent=2))
        return EXIT_OK

    print(f"query    : {args.question}")
    print(f"strategy : {args.strategy}   top similarity: {top_similarity(hits):.3f}")
    print()
    for i, h in enumerate(hits, 1):
        ranks = f"vec={h.vector_rank or '-':>3} lex={h.lexical_rank or '-':>3}"
        sim = f"{h.similarity:.3f}" if h.similarity is not None else "  -  "
        print(f"{i}. {h.ticket_id}  rrf={h.rrf_score:.5f}  sim={sim}  {ranks}")
        print(f"   {' '.join(h.content.split())[:150]}")
    return EXIT_OK


def cli() -> int:
    try:
        return main()
    except UpstreamUnavailable as exc:
        print(json.dumps({"event": "upstream_unavailable", "error": str(exc)}))
        return EXIT_UPSTREAM_UNAVAILABLE
    except Exception as exc:  # noqa: BLE001 - top-level boundary
        print(json.dumps({"event": "error", "error": f"{type(exc).__name__}: {exc}"}))
        return EXIT_ERROR


if __name__ == "__main__":
    sys.exit(cli())
