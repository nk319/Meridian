# PROGRESS

Updated as the **last action of every work session**. This file plus the
`phase-N` git tags are the only things needed to resume — never conversation
history, which does not survive a container restart.

---

## Current state

| | |
| --- | --- |
| **Phase** | 0 — Contracts & seed data |
| **Status** | ✅ Complete |
| **Tag** | `phase-0` |
| **Next phase** | 1 — AI/RAG core |
| **Next action** | Stand up pgvector schema and the fastembed indexer against `seeds/rag/support_tickets.jsonl` |

**Blocked on:** pushing to GitHub. `nk319/Meridian` exists but is not attached to
the session — the `add_repo` tool call returns "requires approval" and the grant
is not registering, likely because the MCP server reconnected mid-session. All
work so far is committed to local git only. Nothing is lost, but nothing is
backed up either.

---

## Phase 0 — complete

**Goal:** freeze every cross-component interface, and generate source data that
makes the later phases' acceptance tests possible.

### Delivered

| Artifact | What it settles |
| --- | --- |
| `docs/CONTRACTS.md` | All ten interfaces: schemas, Bronze layout, the Silver→warehouse mover, topics, CLI entrypoints, Airflow version, `meta.*` DDL, Gold marts, enums, PII |
| `contracts/topics.yml` | Kafka topic manifest; Phase 5 generates broker init and client constants from it |
| `docs/governance/pii_classification.yml` | PII levels, masking strategy, and the three enforcement mechanisms |
| `docs/governance/owners.yml` | Ownership and SLA per Gold asset |
| `src/meridian/seed/` | Deterministic generator: 8 modules |
| `tests/test_seed.py` | 15 acceptance tests |

### Acceptance — passing

```
15 passed in 1.92s
```

Generated at defaults (seed 20260827, anchor 2026-08-01, 24 months):

| Entity | Rows | Source system |
| --- | ---: | --- |
| customers | 3,000 | oltp |
| orders | 14,615 | oltp |
| order_items | 26,054 | oltp |
| customer_change_log | 4 | oltp |
| customer_pii | 3,000 | secure |
| products | 221 | files (defects) |
| web_events | 156,973 | files (defects) |
| payments | 16,178 / 33 pages | vendor (defects) |
| support_tickets | 1,311 | restapi + rag |

History spans 730 days. 4,136 defects injected, all confined to third-party feeds.

### Decisions taken during the phase

| Decision | Reasoning |
| --- | --- |
| Package named `meridian`, not `ecom_platform` | Same non-shadowing property, matches repo name. Recorded in CONTRACTS.md. |
| `seeds/` is gitignored | 23 MB of regenerable output. The generator is deterministic; `make seed` reproduces it byte for byte. |
| Orders ingested from OLTP **only** | Four ingestion paths for one entity made the Bronze reconciliation check fail permanently. REST ingestion is repointed at support tickets and products. |
| PII split into `secure/customer_pii.csv` at generation time | Physical separation is the enforcement mechanism; doing it at generation means no later step can accidentally carry PII into `silver.customers`. |
| `ORDER_FREQ_SCALE` added | First run produced 2.2 orders/customer — too thin for cohort retention and RFM quintiles. Tuned to 4.9. |

### Bug found and fixed

`entities.make_products` used `hash(category)` for price banding. CPython
randomises string hashing per process unless `PYTHONHASHSEED` is pinned, so the
generator was **non-deterministic across runs** — every downstream row-count
assertion would have been intermittently flaky, in a way that looks like a
pipeline bug rather than a generator bug. Replaced with sha1. Caught by
`test_generator_is_deterministic`, which is why that test was written first.

---

## Phase 1 — AI/RAG core (next)

**Goal:** working retrieval over the ticket corpus, with PII masked before
embedding and a measured quality baseline.

**Planned deliverables**

- `docker-compose.yml` with the `core` profile: Postgres (`pgvector/pgvector:pg16`), MinIO
- `db/init/01..05.sql` — the strictly-ordered init scripts from CONTRACTS.md §1
- `src/meridian/rag/masking.py` — dictionary-then-regex, reading `known_pii_terms.json`
- `src/meridian/rag/embeddings.py` — fastembed, `BAAI/bge-small-en-v1.5`, 384 dims
- `src/meridian/rag/index.py` — chunk, mask, embed, upsert into `rag.chunks`
- `src/meridian/rag/retrieve.py` — hybrid BM25 + vector via RRF in one SQL CTE
- `src/meridian/rag/generate.py` — `claude-opus-5`, adaptive thinking, extractive fallback
- `eval/golden_questions.yml` — 15 questions, 5 of them abstention cases
- `tests/test_pii_manifest.py` — zero manifest terms in the vector store

**Acceptance:** `pytest` green with no `ANTHROPIC_API_KEY` set; recall@5 ≥ 0.80
on the golden set; zero manifest terms found in `rag.chunks`.

**Watch for:** pgvector must be verified present at startup, not assumed —
`postgres:16-alpine` silently lacks it and that failure surfaces as a confusing
error much later.

---

## Verified about the environment

Established by direct test, not assumption:

- Docker daemon runs in this container (needs manual `dockerd` start; not running by default)
- Registry pulls succeed through the proxy
- `pgvector/pgvector:pg16` boots; extension version 0.8.6 confirmed working with a real 384-dim query
- 15 GB RAM, 4 CPUs, ~30 GB disk — the `full` profile's ~6.5 GB peak fits
- Chromium + Playwright available at `/opt/pw-browsers` for Phase 7 screenshots
- **Cannot** create GitHub repos from here (403); repo creation is a manual step
