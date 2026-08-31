"""Answer a question from retrieved passages.

    python -m meridian.rag.generate "why do customers ask for refunds?"

Three outcomes, in priority order:

    abstained   nothing retrieved is close enough to the question
    llm         claude-opus-5 answered from the retrieved passages
    extractive  no API key, or the API failed — answer built from the passages

The extractive path is not a stub. CONTRACTS.md requires the platform to run end
to end with no ANTHROPIC_API_KEY set, and a demo that degrades to "AI
unavailable" has not demonstrated a retrieval system — it has demonstrated an
API key. Retrieval is the part being shown; the model writes the prose.

Abstention comes before both
----------------------------
A retrieval system that always answers is indistinguishable from one that
answers well, because the failure looks exactly like the success. So the
threshold is applied to cosine similarity, before any generation, and the
answer says so. Five of the fifteen questions in eval/golden_questions.yml exist
only to catch this regressing, and `make rag-eval` reports the margin between
the answerable and unanswerable sets rather than just a pass mark.

The threshold sits on similarity, not on the RRF score, because RRF scores carry
no notion of closeness: `1/(k+1)` is identical whether the top hit is a
paraphrase of the question or a random ticket.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field

from ..db import UpstreamUnavailable
from ..runlog import EXIT_ERROR, EXIT_OK, EXIT_UPSTREAM_UNAVAILABLE
from ..settings import settings
from .retrieve import RetrievedChunk, Retriever, top_similarity

SYSTEM_PROMPT = """\
You are a support-operations analyst for Meridian, an e-commerce company. You \
answer questions about customer support tickets using only the ticket excerpts \
provided to you.

Rules:
- Use only the supplied excerpts. Do not use outside knowledge about Meridian.
- Cite the ticket IDs that support each claim, like (T000123).
- If the excerpts do not answer the question, say so plainly. Do not speculate.
- Excerpts are redacted: [CUSTOMER_NAME], [EMAIL] and [PHONE] replace personal \
data. Never guess what was behind a redaction, and never ask for it.
- Be concise. Two or three sentences unless the question needs more.\
"""

# Words that carry no retrieval signal. Used only by the extractive path's
# sentence scoring — the BM25 half of retrieval uses Postgres's own stopword
# list, which is a different and better-maintained one.
_STOPWORDS = frozenset(
    "a an and are as at be been but by can did do does for from had has have how "  # noqa: SIM905
    "i if in is it its me my no not of on or our so than that the their them then "
    "there these they this to was we were what when where which who why will with "
    "you your".split()
)

_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+")

# Redaction placeholders, excluded from scoring for the same reason they are
# excluded from the tsvector: "[CUSTOMER_NAME]" contributes the words "customer"
# and "name", and a question about customers would then score every redacted
# sentence above every informative one.
_REDACTION = re.compile(r"\[[A-Z_]+\]")

# Crude suffix stripping, applied identically to the question and to the corpus.
# Not a real stemmer and not trying to be — but without it, "why do customers ask
# for refunds" shares no word at all with "I need a refund for order O0003687",
# and the extractive path falls through to quoting the first passage verbatim.
# The BM25 half of retrieval gets proper stemming from Postgres's english
# configuration; this exists only so the offline sentence scorer is not
# comparing surface forms.
_SUFFIXES = ("ingly", "edly", "ing", "ies", "ed", "es", "ly", "s")


def _stem(word: str) -> str:
    for suffix in _SUFFIXES:
        if word.endswith(suffix) and len(word) > len(suffix) + 2:
            word = word[: -len(suffix)] + ("y" if suffix == "ies" else "")
            break
    # "charge"/"charged" -> "charg" only if both sides lose the trailing e.
    if len(word) > 4 and word.endswith("e"):
        word = word[:-1]
    return word


@dataclass
class Answer:
    question: str
    answer: str
    mode: str  # 'llm' | 'extractive' | 'abstained'
    top_similarity: float
    citations: list[str] = field(default_factory=list)
    passages: list[dict] = field(default_factory=list)
    model: str | None = None
    thinking: str | None = None
    fallback_reason: str | None = None

    def as_dict(self) -> dict:
        return {
            "question": self.question,
            "answer": self.answer,
            "mode": self.mode,
            "top_similarity": round(self.top_similarity, 4),
            "citations": self.citations,
            "model": self.model,
            "fallback_reason": self.fallback_reason,
            "passages": self.passages,
        }


# ---------------------------------------------------------------------------
# prompt assembly
# ---------------------------------------------------------------------------


def format_passages(chunks: list[RetrievedChunk]) -> str:
    parts = []
    for c in chunks:
        parts.append(
            f'<ticket id="{c.ticket_id}" created="{c.created_ts.date().isoformat()}">\n'
            f"{c.content}\n</ticket>"
        )
    return "\n\n".join(parts)


def choose_effort(question: str, chunks: list[RetrievedChunk]) -> str:
    """Scale reasoning effort to what the question actually asks for.

    This is the adaptive half of adaptive thinking: `thinking: {type: adaptive}`
    lets the model decide *whether* to think, and effort sets how much budget it
    has to. "What is ticket T000123 about" and "compare refund and shipping
    complaints and say which is trending" are not the same problem, and paying
    the second one's price for the first is a cost with no return.

    Deliberately crude and readable. A learned router here would be a second
    model to evaluate and version, in service of a decision worth cents.
    """
    lowered = question.lower()
    analytical = (
        "compare",
        "trend",
        "why",
        "most common",
        "breakdown",
        "across",
        "pattern",
        "correlat",
        "versus",
        " vs ",
        "summar",
        "recommend",
    )
    if any(word in lowered for word in analytical) or len(question.split()) > 18:
        return "high"
    if len(chunks) <= 2 and len(question.split()) <= 8:
        return "low"
    return "medium"


# ---------------------------------------------------------------------------
# extractive fallback
# ---------------------------------------------------------------------------


def _content_words(text: str) -> set[str]:
    text = _REDACTION.sub(" ", text)
    return {_stem(w) for w in re.findall(r"[a-z0-9]+", text.lower()) if w not in _STOPWORDS}


def extractive_answer(question: str, chunks: list[RetrievedChunk], limit: int = 3) -> str:
    """Build an answer by selecting the sentences that best match the question.

    Not a summary and not pretending to be one: it quotes the corpus. Scoring is
    query-term overlap normalised by sentence length, so a long sentence does not
    win just by containing more words.
    """
    if not chunks:
        return "No matching support tickets were found."

    qwords = _content_words(question)
    scored: list[tuple[float, str, str]] = []
    for chunk in chunks:
        body = chunk.content.split("\n\n", 1)[-1]
        for sentence in _SENTENCE_SPLIT.split(body):
            sentence = " ".join(sentence.split())
            if len(sentence) < 25:
                continue
            words = _content_words(sentence)
            if not words:
                continue
            overlap = len(qwords & words)
            if overlap == 0:
                continue
            # The exponent is a mild length penalty, not a strong one. At 0.5 a
            # four-word fragment matching one query term ties a sixteen-word
            # sentence matching two, and the corpus is full of short template
            # fragments — "Please refund one of them." wins every time and says
            # nothing. At 0.25, absolute coverage of the question dominates.
            scored.append((overlap / (len(words) ** 0.25), chunk.ticket_id, sentence))

    scored.sort(key=lambda t: (-t[0], t[1], t[2]))

    seen_tickets: set[str] = set()
    seen_sentences: set[str] = set()
    lines: list[str] = []
    for _score, ticket_id, sentence in scored:
        # Two levels of deduplication, because the corpus is generated from an
        # intent grammar: the same sentence genuinely appears in dozens of
        # tickets, and three identical lines attributed to three ticket IDs
        # reads like three pieces of evidence when it is one.
        key = " ".join(sentence.lower().split())
        if ticket_id in seen_tickets or key in seen_sentences:
            continue
        seen_tickets.add(ticket_id)
        seen_sentences.add(key)
        lines.append(f"- ({ticket_id}) {sentence}")
        if len(lines) >= limit:
            break

    if not lines:
        # Retrieval cleared the abstention threshold but no single sentence
        # shares a content word with the question — normal for a paraphrase.
        # Quote the closest passage rather than claiming nothing was found.
        best = chunks[0]
        body = " ".join(best.content.split("\n\n", 1)[-1].split())
        return (
            f"The closest matching ticket is {best.ticket_id}:\n- {body[:400]}\n\n"
            f"(Extractive answer: no ANTHROPIC_API_KEY is set, so this quotes the "
            f"retrieved tickets rather than summarising them.)"
        )

    heading = f"{len(chunks)} support tickets match this question. The most relevant lines:"
    return (
        f"{heading}\n" + "\n".join(lines) + "\n\n"
        "(Extractive answer: no ANTHROPIC_API_KEY is set, so this quotes the "
        "retrieved tickets rather than summarising them.)"
    )


# ---------------------------------------------------------------------------
# LLM path
# ---------------------------------------------------------------------------


def llm_answer(question: str, chunks: list[RetrievedChunk]) -> tuple[str, str | None, str | None]:
    """Answer with claude-opus-5. Returns (answer, thinking, failure_reason).

    A failure reason rather than an exception: every API error here has the same
    correct response, which is to fall back to the extractive path and say why.
    Letting a rate limit take down `/v1/ai/ask` would mean the platform's
    availability is the vendor's availability, for a feature that has a working
    offline path sitting right next to it.
    """
    import anthropic

    cfg = settings()
    client = anthropic.Anthropic(api_key=cfg.anthropic_api_key)
    effort = choose_effort(question, chunks)

    try:
        response = client.messages.create(
            model=cfg.anthropic_model,
            max_tokens=16000,
            system=SYSTEM_PROMPT,
            # Adaptive thinking: the model decides whether a question needs
            # reasoning. `budget_tokens` is not merely deprecated on this model,
            # it is rejected with a 400 — effort is the control now.
            thinking={"type": "adaptive", "display": "summarized"},
            output_config={"effort": effort},
            messages=[
                {
                    "role": "user",
                    "content": (
                        f"Ticket excerpts:\n\n{format_passages(chunks)}\n\nQuestion: {question}"
                    ),
                }
            ],
        )
    except anthropic.APIStatusError as exc:
        return "", None, f"{type(exc).__name__}: HTTP {exc.status_code}"
    except anthropic.APIConnectionError as exc:
        return "", None, f"APIConnectionError: {exc}"

    # A refusal arrives as HTTP 200 with empty-ish content, so `stop_reason` has
    # to be checked before reading blocks or this returns a confident "".
    if response.stop_reason == "refusal":
        category = getattr(response.stop_details, "category", None)
        return "", None, f"refusal: {category}"

    text = "\n".join(b.text for b in response.content if b.type == "text").strip()
    thinking = "\n".join(
        b.thinking for b in response.content if b.type == "thinking" and b.thinking
    ).strip()
    if not text:
        return "", None, f"empty response (stop_reason={response.stop_reason})"
    return text, thinking or None, None


# ---------------------------------------------------------------------------
# orchestration
# ---------------------------------------------------------------------------

CITATION_RE = re.compile(r"\bT\d{6}\b")


def answer_question(
    question: str,
    retriever: Retriever,
    *,
    top_k: int | None = None,
    strategy: str = "hybrid",
    use_llm: bool = True,
) -> Answer:
    cfg = settings()
    chunks = retriever.search(question, top_k=top_k, strategy=strategy)
    similarity = top_similarity(chunks)
    passages = [
        {
            "ticket_id": c.ticket_id,
            "chunk_id": c.chunk_id,
            "similarity": c.similarity,
            "vector_rank": c.vector_rank,
            "lexical_rank": c.lexical_rank,
            "content": c.content,
        }
        for c in chunks
    ]

    if not chunks or similarity < cfg.abstain_similarity:
        return Answer(
            question=question,
            answer=(
                "I don't have support tickets that answer this. The closest match "
                f"scored {similarity:.2f} against a {cfg.abstain_similarity:.2f} "
                "threshold, which is not close enough to answer from."
            ),
            mode="abstained",
            top_similarity=similarity,
            passages=passages,
        )

    if use_llm and cfg.has_anthropic_key:
        text, thinking, reason = llm_answer(question, chunks)
        if text:
            return Answer(
                question=question,
                answer=text,
                mode="llm",
                top_similarity=similarity,
                citations=sorted(set(CITATION_RE.findall(text))),
                passages=passages,
                model=cfg.anthropic_model,
                thinking=thinking,
            )
        fallback_reason = reason
    else:
        fallback_reason = None if use_llm else "llm disabled by caller"
        if use_llm:
            fallback_reason = "no ANTHROPIC_API_KEY set"

    text = extractive_answer(question, chunks)
    return Answer(
        question=question,
        answer=text,
        mode="extractive",
        top_similarity=similarity,
        citations=sorted({c.ticket_id for c in chunks}),
        passages=passages,
        fallback_reason=fallback_reason,
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="meridian.rag.generate",
        description="Answer a question from the support-ticket corpus",
    )
    p.add_argument("question")
    p.add_argument("--top-k", type=int, default=None)
    p.add_argument("--strategy", default="hybrid", choices=["hybrid", "vector", "lexical"])
    p.add_argument(
        "--no-llm",
        action="store_true",
        help="force the extractive path even when a key is configured",
    )
    p.add_argument("--json", action="store_true")
    args = p.parse_args(argv)

    with Retriever.open() as r:
        result = answer_question(
            args.question,
            r,
            top_k=args.top_k,
            strategy=args.strategy,
            use_llm=not args.no_llm,
        )

    if args.json:
        print(json.dumps(result.as_dict(), indent=2))
        return EXIT_OK

    print(f"Q: {result.question}")
    print(f"   mode={result.mode}  top_similarity={result.top_similarity:.3f}", end="")
    print(f"  ({result.fallback_reason})" if result.fallback_reason else "")
    print()
    print(result.answer)
    if result.citations:
        print()
        print("cited:", ", ".join(result.citations))
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
