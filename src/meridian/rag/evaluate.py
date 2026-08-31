"""Measure retrieval quality against eval/golden_questions.yml.

    python -m meridian.rag.evaluate            # all three strategies
    python -m meridian.rag.evaluate --min-recall 0.80

Reports recall@5 for hybrid retrieval and for each half on its own, plus the
chance baseline. A single number with nothing to compare it against is not a
measurement: recall@5 of 0.80 sounds strong until you notice that guessing
scores 0.22 on this set and lexical-only scores 0.70, at which point the
interesting question is what the other half is buying.

`recall@k` here is the hit rate — the fraction of questions with at least one
relevant document in the top k. That is the usual meaning in RAG evaluation and
the one that matters for a generator that reads k passages, but it is not the
textbook definition of recall (relevant-retrieved over relevant-total), so it is
spelled out rather than left for the reader to assume. MRR@k is reported
alongside it because hit rate cannot tell rank 1 from rank 5, and a change that
moves every answer from first to fifth is invisible to it.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from ..db import UpstreamUnavailable, connect
from ..runlog import EXIT_DQ_BLOCK, EXIT_ERROR, EXIT_OK, EXIT_UPSTREAM_UNAVAILABLE
from ..settings import project_root, settings
from .embeddings import BGE_QUERY_INSTRUCTION
from .masking import Masker
from .retrieve import STRATEGIES, Retriever, top_similarity


def golden_path() -> Path:
    return project_root() / "eval" / "golden_questions.yml"


def load_golden(path: Path | None = None) -> dict:
    doc = yaml.safe_load((path or golden_path()).read_text(encoding="utf-8"))
    if not doc.get("questions"):
        raise ValueError(f"{path or golden_path()} contains no questions")
    return doc


def load_anchors(corpus_path: Path) -> dict:
    """Generator-published values the golden set is allowed to name.

    `seeds/manifest.json` sits two directories above the corpus. Read leniently:
    an absent or unreadable manifest is not an error here, it just means no
    question may use `content_from_anchor` — and the one that does will fail
    below with a message naming itself, which is more useful than a
    FileNotFoundError naming the manifest.
    """
    manifest = corpus_path.parent.parent / "manifest.json"
    if not manifest.is_file():
        return {}
    try:
        return json.loads(manifest.read_text(encoding="utf-8")).get("eval_anchors", {})
    except (json.JSONDecodeError, OSError):
        return {}


class _Anchors(dict):
    """A format mapping that leaves unknown placeholders alone.

    `str.format_map` raises on a missing key, which would turn any brace a
    question author writes for other reasons into a crash at eval time. Leaving
    it verbatim means an unresolved placeholder shows up in the reported
    question text, where a human reads it, rather than as a KeyError naming a
    word.
    """

    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def resolve_anchors(golden: dict, corpus_path: Path) -> dict:
    """Substitute generator-published anchors into the question text.

    The relevance spec and the question a retriever is actually handed have to
    name the same identifier, or the question asks about one order and is scored
    against another — which looks exactly like a retrieval failure. Both sides
    resolve from `seeds/manifest.json`, so they cannot drift apart.

    Returns a new document; the loaded YAML is not mutated, because
    `tests/test_rag_retrieval.py` loads it once per session and a mutation would
    make test order significant.
    """
    anchors = _Anchors(load_anchors(corpus_path))
    return {
        **golden,
        "questions": [
            {**q, "question": str(q["question"]).format_map(anchors)}
            for q in golden.get("questions", [])
        ],
    }


def build_relevance(golden: dict, corpus_path: Path, masker: Masker) -> dict[str, set[str]]:
    """Ticket IDs that count as relevant, per question.

    Derived from the generator's ground-truth labels and the masked corpus text,
    never from retrieval output. See the header of golden_questions.yml for why
    that distinction is the whole point of this function.
    """
    anchors = load_anchors(corpus_path)
    # Ground truth comes from the generated corpus, not from silver, and
    # deliberately so: `intent` is the label the generator assigned before any
    # of this existed. Reading it back out of the warehouse would work equally
    # well today and would quietly become circular the moment Phase 4's AI
    # enrichment starts writing a predicted intent into the same column.
    corpus = []
    with corpus_path.open(encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                corpus.append(json.loads(line))

    masked = {
        row["ticket_id"]: (
            row["intent"],
            masker.mask(f"Subject: {row['subject']}\n\n{row['body']}").lower(),
        )
        for row in corpus
    }

    relevance: dict[str, set[str]] = {}
    for q in golden["questions"]:
        if not q.get("expects_answer"):
            continue
        spec = q.get("relevant") or {}
        intent = spec.get("intent")
        needles = [c.lower() for c in spec.get("content_contains", [])]

        # Anchors, resolved from what the generator actually produced. A
        # literal identifier written into the YAML is correct until the day the
        # generator changes for an unrelated reason, and then it silently
        # matches nothing; naming the anchor instead moves the coupling to a
        # value that is recomputed with the corpus.
        for anchor in spec.get("content_from_anchor", []):
            value = anchors.get(anchor)
            if not value:
                raise ValueError(
                    f"golden question {q['id']!r} needs the {anchor!r} anchor, which "
                    f"seeds/manifest.json does not publish. Re-run `make seed`; if it "
                    f"is still missing, the generator could not find one and the "
                    f"question needs rewriting rather than repointing."
                )
            needles.append(str(value).lower())
        hits = {
            ticket_id
            for ticket_id, (row_intent, text) in masked.items()
            if (intent is None or row_intent == intent) and all(n in text for n in needles)
        }
        if not hits:
            raise ValueError(
                f"golden question {q['id']!r} matches no ticket in the corpus. Its "
                f"recall can only ever be 0, which reads as a retrieval failure "
                f"and is not one."
            )
        relevance[q["id"]] = hits
    return relevance


def chance_recall(relevance: dict[str, set[str]], corpus_size: int, k: int) -> float:
    """Expected recall@k from returning k documents at random.

    The number that makes the headline figure mean something. Computed
    analytically — 1 - (1 - p)^k per question, averaged — rather than sampled,
    so it does not add a random component to a deterministic report.
    """
    if not relevance:
        return 0.0
    total = 0.0
    for hits in relevance.values():
        p = len(hits) / corpus_size
        total += 1.0 - (1.0 - p) ** k
    return total / len(relevance)


@dataclass
class QuestionResult:
    question_id: str
    question: str
    expects_answer: bool
    top_similarity: float
    abstained: bool
    hit: bool | None = None
    reciprocal_rank: float = 0.0
    retrieved: list[dict] = field(default_factory=list)


@dataclass
class StrategyResult:
    strategy: str
    results: list[QuestionResult]
    k: int

    @property
    def answerable(self) -> list[QuestionResult]:
        return [r for r in self.results if r.expects_answer]

    @property
    def unanswerable(self) -> list[QuestionResult]:
        return [r for r in self.results if not r.expects_answer]

    @property
    def recall_at_k(self) -> float:
        rows = self.answerable
        return sum(1 for r in rows if r.hit) / len(rows) if rows else 0.0

    @property
    def precision_at_k(self) -> float:
        """Mean fraction of the k returned documents that are relevant.

        Recall@5 saturates at 1.00 on this corpus — every strategy finds
        something relevant for every question — and a metric pinned at its
        ceiling cannot detect a regression. Precision has headroom: it separates
        "found one good answer among five" from "found five", which is exactly
        the difference a generator reading all five experiences.
        """
        rows = self.answerable
        if not rows:
            return 0.0
        return sum(
            sum(1 for d in r.retrieved if d["relevant"]) / max(len(r.retrieved), 1) for r in rows
        ) / len(rows)

    @property
    def mrr_at_k(self) -> float:
        rows = self.answerable
        return sum(r.reciprocal_rank for r in rows) / len(rows) if rows else 0.0

    @property
    def abstention_measurable(self) -> bool:
        """Whether this strategy produces a similarity to threshold at all.

        Lexical-only returns documents that carry no cosine similarity unless
        they also surfaced in the vector pool, so every unanswerable question
        scores the -1.0 sentinel and "abstains" trivially. Reporting that as
        100% accuracy would credit BM25 with a judgement it never made.
        """
        return any(r.top_similarity > -1.0 for r in self.unanswerable)

    @property
    def abstention_accuracy(self) -> float | None:
        rows = self.unanswerable
        if not rows or not self.abstention_measurable:
            return None
        return sum(1 for r in rows if r.abstained) / len(rows)

    @property
    def false_abstentions(self) -> list[str]:
        return [r.question_id for r in self.answerable if r.abstained]

    @property
    def separation(self) -> tuple[float, float]:
        """(lowest answerable similarity, highest unanswerable similarity).

        A threshold is only meaningful inside this gap. If the two cross, no
        single cutoff separates the sets and the abstention score is an accident
        of where the constant happens to sit.
        """
        lo = min((r.top_similarity for r in self.answerable), default=0.0)
        hi = max((r.top_similarity for r in self.unanswerable), default=0.0)
        return lo, hi


def evaluate_strategy(
    retriever: Retriever,
    golden: dict,
    relevance: dict[str, set[str]],
    strategy: str,
    k: int,
    abstain_threshold: float,
) -> StrategyResult:
    results = []
    for q in golden["questions"]:
        chunks = retriever.search(q["question"], top_k=k, strategy=strategy)
        similarity = top_similarity(chunks)
        expects = bool(q.get("expects_answer"))

        hit: bool | None = None
        rr = 0.0
        relevant_ids = relevance.get(q["id"], set())
        if expects:
            hit = False
            for rank, chunk in enumerate(chunks, start=1):
                if chunk.ticket_id in relevant_ids:
                    hit = True
                    rr = 1.0 / rank
                    break

        results.append(
            QuestionResult(
                question_id=q["id"],
                question=q["question"],
                expects_answer=expects,
                top_similarity=similarity,
                # Measured with the same rule generate.py applies, so the number
                # describes the shipped behaviour rather than a parallel one.
                abstained=similarity < abstain_threshold,
                hit=hit,
                reciprocal_rank=rr,
                retrieved=[
                    {
                        "rank": i,
                        "ticket_id": c.ticket_id,
                        "relevant": c.ticket_id in relevant_ids,
                        "similarity": c.similarity,
                        "vector_rank": c.vector_rank,
                        "lexical_rank": c.lexical_rank,
                    }
                    for i, c in enumerate(chunks, start=1)
                ],
            )
        )
    return StrategyResult(strategy=strategy, results=results, k=k)


def persist(run_id: uuid.UUID, evaluated_at: dt.datetime, reports: list[StrategyResult]) -> None:
    """Write to rag.eval_results as rag_indexer.

    A second connection, because retrieval ran as analytics_ro and that role has
    SELECT and nothing else on `rag`. Reusing the read connection to write would
    have required widening the grant, which would have quietly removed the point
    of having two roles.
    """
    with connect("rag_indexer", vectors=False) as conn:
        with conn.cursor() as cur:
            for report in reports:
                for r in report.results:
                    cur.execute(
                        "INSERT INTO rag.eval_results (eval_run_id, evaluated_at, "
                        "strategy, question_id, question, expects_answer, hit, "
                        "reciprocal_rank, abstained, top_similarity, retrieved) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (
                            run_id,
                            evaluated_at,
                            report.strategy,
                            r.question_id,
                            r.question,
                            r.expects_answer,
                            r.hit,
                            r.reciprocal_rank,
                            r.abstained,
                            r.top_similarity,
                            json.dumps(r.retrieved),
                        ),
                    )
        conn.commit()


def render(reports: list[StrategyResult], baseline: float, threshold: float) -> str:
    k = reports[0].k
    out = [
        f"Retrieval quality — {len(reports[0].answerable)} answerable questions, "
        f"{len(reports[0].unanswerable)} abstention cases, k={k}",
        "",
        f"{'strategy':10} {f'recall@{k}':>10} {f'prec@{k}':>9} {f'MRR@{k}':>8} "
        f"{'abstain acc':>12} {'min ans sim':>12} {'max unans sim':>14}",
        "-" * 80,
    ]
    for r in sorted(reports, key=lambda x: -x.recall_at_k):
        lo, hi = r.separation
        acc = (
            f"{r.abstention_accuracy:12.2f}"
            if r.abstention_accuracy is not None
            else f"{'n/a':>12}"
        )
        hi_s = f"{hi:14.3f}" if r.abstention_measurable else f"{'n/a':>14}"
        out.append(
            f"{r.strategy:10} {r.recall_at_k:10.2f} {r.precision_at_k:9.2f} "
            f"{r.mrr_at_k:8.3f} {acc} {lo:12.3f} {hi_s}"
        )
    out += [
        "-" * 80,
        f"{'chance':10} {baseline:10.2f} {'-':>9} {'-':>8} {'-':>12} {'-':>12} {'-':>14}",
        "",
        f"abstention threshold: {threshold:.2f}",
    ]

    # Threshold commentary only makes sense for a strategy that has similarities.
    best = max(
        (r for r in reports if r.abstention_measurable),
        key=lambda r: r.recall_at_k,
        default=max(reports, key=lambda r: r.recall_at_k),
    )
    lo, hi = best.separation
    if not best.abstention_measurable:
        out.append("no strategy produced similarities; abstention not measurable")
    elif hi < lo:
        out.append(
            f"threshold has a usable margin: unanswerable top out at {hi:.3f}, "
            f"answerable bottom out at {lo:.3f} (gap {lo - hi:.3f})"
        )
    else:
        out.append(
            f"WARNING: the sets overlap ({best.strategy}): an unanswerable question "
            f"scored {hi:.3f} while an answerable one scored only {lo:.3f}. No single "
            f"threshold separates them."
        )
    if best.false_abstentions:
        out.append(f"WARNING: answerable questions wrongly abstained: {best.false_abstentions}")

    out.append("")
    out.append("per-question (best strategy: " + best.strategy + ")")
    for r in best.results:
        if r.expects_answer:
            mark = "hit " if r.hit else "MISS"
            out.append(
                f"  {mark} rr={r.reciprocal_rank:.2f} sim={r.top_similarity:.3f}  {r.question_id}"
            )
        else:
            mark = "abst" if r.abstained else "ANSW"
            out.append(f"  {mark}          sim={r.top_similarity:.3f}  {r.question_id}")
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="meridian.rag.evaluate",
        description="Measure retrieval quality against the golden question set",
    )
    p.add_argument("--strategy", action="append", choices=list(STRATEGIES), default=None)
    p.add_argument("--top-k", type=int, default=None)
    p.add_argument(
        "--min-recall",
        type=float,
        default=None,
        help="fail with exit code 2 if the best strategy scores below this",
    )
    p.add_argument(
        "--no-query-instruction",
        action="store_true",
        help="embed queries bare, without BGE's retrieval instruction prefix. "
        "The prefix is the model card's recommendation; this measures it.",
    )
    p.add_argument("--no-persist", action="store_true")
    p.add_argument("--json", action="store_true")
    args = p.parse_args(argv)

    cfg = settings()
    k = args.top_k or int(load_golden().get("recall_at_k", cfg.top_k))
    strategies = args.strategy or list(STRATEGIES)

    golden = load_golden()
    masker = Masker.from_project()
    corpus_path = cfg.seeds_dir / "rag" / "support_tickets.jsonl"
    golden = resolve_anchors(golden, corpus_path)
    relevance = build_relevance(golden, corpus_path, masker)

    with corpus_path.open(encoding="utf-8") as fh:
        corpus_size = sum(1 for line in fh if line.strip())
    baseline = chance_recall(relevance, corpus_size, k)

    instruction = "" if args.no_query_instruction else BGE_QUERY_INSTRUCTION
    run_id = uuid.uuid4()
    evaluated_at = dt.datetime.now(dt.UTC)

    with Retriever.open(query_instruction=instruction) as retriever:
        reports = [
            evaluate_strategy(retriever, golden, relevance, s, k, cfg.abstain_similarity)
            for s in strategies
        ]

    if not args.no_persist:
        persist(run_id, evaluated_at, reports)

    if args.json:
        print(
            json.dumps(
                {
                    "eval_run_id": str(run_id),
                    "k": k,
                    "chance_recall": round(baseline, 4),
                    "query_instruction": bool(instruction),
                    "abstain_threshold": cfg.abstain_similarity,
                    "strategies": {
                        r.strategy: {
                            "recall_at_k": round(r.recall_at_k, 4),
                            "precision_at_k": round(r.precision_at_k, 4),
                            "mrr_at_k": round(r.mrr_at_k, 4),
                            "abstention_accuracy": (
                                round(r.abstention_accuracy, 4)
                                if r.abstention_accuracy is not None
                                else None
                            ),
                            "false_abstentions": r.false_abstentions,
                            "min_answerable_similarity": round(r.separation[0], 4),
                            "max_unanswerable_similarity": round(r.separation[1], 4),
                        }
                        for r in reports
                    },
                },
                indent=2,
            )
        )
    else:
        if not instruction:
            print("(queries embedded WITHOUT the BGE retrieval instruction prefix)\n")
        print(render(reports, baseline, cfg.abstain_similarity))

    if args.min_recall is not None:
        best = max(r.recall_at_k for r in reports)
        if best < args.min_recall:
            print(
                f"\nFAIL: best recall@{k} {best:.2f} is below the required {args.min_recall:.2f}",
                file=sys.stderr,
            )
            return EXIT_DQ_BLOCK
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
