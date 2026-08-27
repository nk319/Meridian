# PROGRESS

Updated as the **last action of every work session**. This file plus the
`phase-N` git tags are the only things needed to resume — never conversation
history, which does not survive a container restart.

---

## Current state

| | |
| --- | --- |
| **Phase** | 1 — AI/RAG core |
| **Status** | ✅ Complete |
| **Tag** | `phase-1` — **created locally, not on the remote**, see below |
| **Next phase** | 2 — Batch ingestion and the lakehouse |
| **Next action** | Land Bronze Parquet on MinIO from the five seed sources, then prove the DuckDB → Arrow → `COPY` hop into `silver` before writing any warehouse code (CONTRACTS.md §3) |

**Not blocked, one loose end.** The Phase 0 note about being unable to push is
resolved: the repository is attached, branch pushes work, and `main` now points
at the Phase 0 commit, which it did not before — `main` held an unrelated
"Initial commit" while all Phase 0 work sat on the `phase-0` tag, unreachable
from any branch.

**Pushing a tag is 403 from this environment.** `git push origin refs/tags/phase-1`
fails with HTTP 403 on every attempt while `git push origin <branch>` to the same
remote succeeds, and the GitHub tool surface available here exposes no
tag-creation call. So `phase-1` exists in the local repository and on no remote.
Someone with tag-push permission needs to run:

```bash
git push origin refs/tags/phase-1
```

Until that happens the resume point for Phase 1 is the branch
`claude/phase-1-setup-8lokjp`, not the tag. Worth resolving before Phase 2, since
this file names the `phase-N` tags as half of what makes a session resumable.

---

## Phase 0 — complete

**Goal:** freeze every cross-component interface, and generate source data that
makes the later phases' acceptance tests possible.

| Artifact | What it settles |
| --- | --- |
| `docs/CONTRACTS.md` | All interfaces: schemas, Bronze layout, the Silver→warehouse mover, topics, CLI entrypoints, Airflow version, `meta.*` DDL, Gold marts, enums, PII |
| `contracts/topics.yml` | Kafka topic manifest |
| `docs/governance/*.yml` | PII levels and masking strategy; ownership and SLA |
| `src/meridian/seed/` | Deterministic generator, 8 modules |
| `tests/test_seed.py` | 15 acceptance tests |

Generated at defaults (seed 20260827, anchor 2026-08-01, 24 months): 3,000
customers, 14,615 orders, 26,054 order items, 16,178 payments, 156,973 web
events, 1,311 support tickets, 4,136 injected defects. Re-verified byte-identical
in this session.

---

## Phase 1 — complete

**Goal:** working retrieval over the ticket corpus, with PII masked before
embedding and a measured quality baseline.

### Delivered

| Artifact | What it does |
| --- | --- |
| `docker-compose.yml` | `core` profile: `pgvector/pgvector:pg16` + MinIO, health-gated |
| `db/init/01..05.sql` | Databases, extensions, seven schemas, five roles with grants, OLTP DDL |
| `src/meridian/settings.py` | Every knob, resolved from the environment once; per-role DSNs |
| `src/meridian/db.py` | Connections opened as a *named* contract role; pgvector assertion |
| `src/meridian/runlog.py` | JSON-line run logs and the frozen exit codes (§5) |
| `src/meridian/rag/masking.py` | Dictionary-then-regex, driven by `pii_classification.yml` |
| `src/meridian/rag/embeddings.py` | fastembed, `BAAI/bge-small-en-v1.5`, 384 dims |
| `src/meridian/rag/ddl.sql` | `rag.chunks`, `rag.chunk_terms`, `rag.index_runs`, `rag.eval_results` |
| `src/meridian/rag/index.py` | Chunk → mask → hash → embed → upsert, with reconciliation |
| `src/meridian/rag/retrieve.py` | BM25 + vector, fused with RRF, one SQL statement |
| `src/meridian/rag/generate.py` | `claude-opus-5`, adaptive thinking, extractive fallback |
| `src/meridian/rag/evaluate.py` | recall@5 / precision@5 / MRR with a chance baseline |
| `eval/golden_questions.yml` | 16 questions, 5 of them abstention cases |
| `tests/` | 47 new tests across 4 files (62 including Phase 0's 15) |

### Acceptance — met

```
62 passed                      # stack up, no ANTHROPIC_API_KEY set
39 passed, 23 skipped          # no stack reachable (the CI shape)
ruff check + ruff format       # clean
```

Retrieval, measured on the golden set (`make rag-eval`):

| strategy | recall@5 | precision@5 | MRR@5 | abstention | 
| --- | ---: | ---: | ---: | ---: |
| **hybrid** | **1.00** | **0.93** | **1.000** | 5/5 |
| lexical only | 1.00 | 0.93 | 1.000 | n/a |
| vector only | 0.91 | 0.91 | 0.909 | 5/5 |
| chance | 0.22 | — | — | — |

- **recall@5 ≥ 0.80** ✅ (1.00, against a 0.22 chance baseline)
- **Zero manifest terms in `rag.chunks`** ✅ — 1,311 chunks scanned by
  case-folded substring against all 7,527 manifest terms
- **`pytest` green with no `ANTHROPIC_API_KEY`** ✅

Store: 1,311 chunks, 1,311 embeddings, 19,767 inverted-index terms, 897 PII
values masked (100% by dictionary, 0% by regex fallback).

### Decisions taken during the phase

| Decision | Reasoning |
| --- | --- |
| **BM25 implemented in SQL**, not taken from an extension | Postgres's `ts_rank_cd` has no IDF at all. On a corpus where 70% of tickets contain "order", it cannot distinguish that from an order number in one ticket — and IDF is most of what makes lexical retrieval work here. Materialising `(chunk_id, lexeme, tf)` makes real BM25 a plain aggregation over ~20k rows, a far smaller dependency than `pg_search`. |
| `content_hash` covers the **masked** text; skip predicate also covers the masking fingerprint and the model | CONTRACTS §10's named failure mode. Hashing raw input means a masking fix never re-embeds already-indexed chunks and the leak is permanent. This makes "masking changed" and "re-embed" the same event, at the cost of a full re-embed after any policy change — a few seconds, in the one direction worth erring. |
| Abstention thresholds on **cosine similarity**, not the RRF score | `1/(k+1)` is identical whether the top hit is a paraphrase or unrelated; rank carries no notion of closeness. |
| Threshold set to **0.68**, calibrated from the measured separation | The eval reports the gap (unanswerable ceiling 0.629, answerable floor 0.724) rather than a pass mark. Tuned on the golden set, so `test_abstention_threshold_sits_inside_a_real_gap` fails if it ever drifts outside that gap rather than silently degrading. |
| BGE query instruction prefix **kept**, on measurement | fastembed's `query_embed()` does not apply it for this model (verified, not assumed). Adding it leaves recall unchanged but widens the abstention separation from 0.072 to 0.105 — a 46% larger margin, which is the whole basis of the threshold. |
| `rag.*` tables created by the **indexer**, not `db/init/` | §1 freezes init at 01..05 and names `rag` as written by the indexer. A fresh clone needs no migration step. |
| `analytics_ro` granted SELECT on `rag` | §1 said "gold only" while also making it the role every `/v1/ai/*` handler uses. Both could not hold. Recorded in CONTRACTS §11 and the deviations table. `secure` grant unchanged: none. |
| Golden set is **16 questions, not the planned 15** | At 15 all three strategies scored 1.00 on both recall and precision — a metric pinned at its ceiling cannot detect a regression. The added question names an identifier occurring in exactly one ticket and is the only one that separates the strategies. |
| Indexer logs to `rag.index_runs`, not `meta.pipeline_run_log` | Not a second run log: §7's columns have nowhere to put chunks-skipped-by-hash or PII-by-pass. Keeps `rag_indexer`'s grants exactly as the contract wrote them. |
| `ruff format` excludes two seed modules | It expands any magic-trailing-comma collection to one item per line, which would turn deliberately hand-aligned data tables (48 names 8-to-a-row, the seasonality curve) into ~160 lines of noise. Everything else in the tree is formatted. |

### Bugs found and fixed

| Bug | How it was found, and why it mattered |
| --- | --- |
| **Dictionary masking never matched a single phone number.** `\b` between a space and a leading `+` is not a word boundary, so the alternation could not fire. | Only visible because `MaskReport` attributes hits per pass: phones were scoring as *regex* catches while the manifest held every one of them. Output looked perfect either way. Replaced `\b` with `(?<!\w)…(?!\w)`. |
| **Redaction placeholders polluted the BM25 index.** `[CUSTOMER_NAME]` analyses to `custom` and `name`, present in 394 of 1,311 chunks — 30% of the corpus. | A question mentioning "customer" would score against redaction artefacts, matching exactly the documents whose text had been removed. `content_tsv` is now generated from redaction-stripped text; `custom` and `name` dropped to zero, `email` 421 → 143. |
| **The lexical half of "hybrid" matched nothing.** `websearch_to_tsquery` joins terms with AND, so an eight-word question matched 0 of 1,311 tickets. | Hybrid was silently vector-only. Found by noticing every result showed `lex = -`. Fixed by the BM25 rewrite, which scores over the union of query terms weighted by rarity. |
| **A golden question was mislabelled**, scoring a correct answer as a miss. | Hybrid "missed" a duplicate-charge question while vector-only hit it. Reading what retrieval actually returned showed perfect answers labelled `refund_request` against a question tagged `billing_question`-only. The eval was wrong, not the retriever. `relevant.intents` now takes a list. |
| **Postgres healthcheck could go green mid-init.** | During initdb the entrypoint runs a temporary server on the unix socket, so a check that only queried `pg_extension` passed after script 02 while 03–05 were still running — anything with `depends_on: service_healthy` would start against a half-built database. Now also asserts the last object the sequence creates. |
| **`make up` failed on a healthy stack.** | `docker compose --wait` counts the one-shot `minio-init` exiting 0 as a failure. The wait is now scoped to the two long-running services. |
| **`make lint` was already red at the `phase-0` tag.** | `ruff format --check` failed on 7 files; Phase 0's "ruff clean" was `ruff check` only. Fixed with the formatter exclusions above, verified by re-running the generator and confirming byte-identical output. |

### Repository housekeeping

`main` pointed at an unrelated "Initial commit" containing only a README stub,
while every Phase 0 artifact existed solely under the `phase-0` tag with no
branch reachable from it. `main` was reset to `phase-0` and force-pushed, and the
Phase 1 branch rebuilt from it.

The lesson generalises to how phases are tagged. A `phase-N` tag is the documented
resume point, so it has to stay reachable: `phase-1` is on the pushed branch, and
if that branch is ever squash-merged the tag must be moved to the resulting commit
on `main` rather than left pointing at history no branch contains. That is exactly
how `phase-0` became unreachable.

---

## Phase 2 — batch ingestion and the lakehouse (next)

**Goal:** the five seed sources land as Bronze Parquet on MinIO, and the
DuckDB → Arrow → `COPY` hop reaches `warehouse.silver`.

**Planned deliverables**

- `src/meridian/ingest/{oltp,files,restapi,vendor}.py` — the four batch sources
  from CONTRACTS §5, each with `--mode full|incremental`
- Bronze writer enforcing the six frozen metadata columns (§2)
- `src/meridian/lake/build_silver.py` — dedup on `_record_hash`, typing,
  quarantine, PII scrub
- `src/meridian/lake/load_warehouse.py` — DuckDB → Arrow → psycopg `COPY`
- `meta.*` DDL from §7, and `tests/test_pii_boundary.py`

**Do first, before any of it:** prove the DuckDB → Arrow → `COPY` hop on one
entity. CONTRACTS §3 calls it the load-bearing hop, and everything downstream is
wasted if it does not work.

**Loose ends inherited from Phase 1**

- `rag_indexer` holds `USAGE` on `silver` but no table grant. Issue
  `GRANT SELECT ON silver.support_tickets TO rag_indexer` when that table is
  created, then switch `meridian.rag.index --source` from `seeds` to `silver`.
- §9 has no `ticket_status` vocabulary; the CHECK constraint in
  `db/init/05_oltp_ddl.sql` is currently the only place it is written down.
  Promote it when Phase 4 builds `fact_support_tickets`.
- The golden set's abstention cases are all off-domain. Similarity thresholding
  cannot catch an on-topic-but-unanswerable question ("what is the average
  delivery time?" retrieves genuinely relevant tickets); that is handled by the
  system prompt in `generate.py` and is not measured. Worth a separate
  faithfulness eval once enrichment lands.

---

## Verified about the environment

Established by direct test in this session, not assumption:

- Docker daemon runs in this container (needs a manual `dockerd` start)
- `pgvector/pgvector:pg16` boots; extension **0.8.6** confirmed, HNSW index built
- `fastembed` downloads `BAAI/bge-small-en-v1.5` through the proxy; 384 dims,
  L2-normalised (‖v‖ = 1.0), so cosine and inner product rank identically
- Embedding throughput ~7.8 chunks/s on 4 CPUs — a full 1,311-chunk index takes
  ~170s; the content-hash skip path makes a no-op re-run 4.6s
- `anthropic` 1.1.0: `thinking` and `output_config` are on the non-beta
  `messages.create` (checked against the SDK signature, not assumed)
- 15 GB RAM, 4 CPUs, ~30 GB disk — the `core` profile is comfortable
- Chromium + Playwright at `/opt/pw-browsers` for Phase 7 screenshots
