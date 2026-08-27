# Meridian

An end-to-end analytics platform for a mid-size online retailer — batch and
streaming ingestion, a Parquet lakehouse, a dimensional warehouse, orchestration,
data quality, governance, and a retrieval-augmented AI layer over customer
support history.

> **Build status: Phase 1 of 8 complete.** This README describes the system being
> built. Sections marked *planned* are not implemented yet. See
> [`docs/PROGRESS.md`](docs/PROGRESS.md) for exactly what exists today — it is
> updated at the end of every work session and is the resume point.

---

## The business problem

A retailer running on a transactional database can answer "what is this order's
status" instantly and "why did margin fall in the Northeast last quarter" not at
all. The operational system is built for row-at-a-time writes; the questions the
business actually asks are aggregations across millions of rows and several
sources that disagree with each other.

Meridian is the layer that closes that gap. It takes orders, payments, web
behaviour, product data and support conversations from five different source
systems, reconciles them, and serves them as a warehouse the business can query
without knowing where anything came from.

Concretely, it answers:

- **Revenue and trend** — what did we sell, through which channel, at what margin
- **Customer value** — who is valuable, who is at risk, how do cohorts retain
- **Payment health** — what share of authorisations fail, and why
- **Funnel** — where visitors drop out between view and purchase
- **Support load** — what customers complain about, at what volume and sentiment

The last one is where the AI layer earns its place. Support tickets are free
text; they are the richest signal in the business and the least queryable. The
platform embeds them, classifies intent and sentiment with an LLM, and writes
those classifications **back into the warehouse** as dimensions the dashboard
joins against — so "which product category generates the most angry tickets"
becomes a SQL question.

---

## Architecture

```
   ┌────────────┐  ┌───────────┐  ┌────────────┐  ┌───────────┐  ┌──────────┐
   │ OLTP       │  │ CSV/JSON  │  │ Own REST   │  │ Vendor    │  │ Redpanda │
   │ Postgres   │  │ file drop │  │ API        │  │ API       │  │ stream   │
   └─────┬──────┘  └─────┬─────┘  └─────┬──────┘  └─────┬─────┘  └────┬─────┘
         └───────────────┴──────────────┴───────────────┴─────────────┘
                                     │
                        full + incremental ingestion
                                     ▼
                    ┌────────────────────────────────┐
                    │ BRONZE — raw Parquet on MinIO  │
                    │ append-only, frozen metadata   │
                    └───────────────┬────────────────┘
                                    │  DuckDB over httpfs
                                    ▼
                    ┌────────────────────────────────┐
                    │ SILVER — typed, deduped, PII   │
                    │ scrubbed; bad rows quarantined │
                    └───────────────┬────────────────┘
                                    │  DuckDB → Arrow → COPY
                                    ▼
                    ┌────────────────────────────────┐
                    │ GOLD — dbt star schema         │
                    │ 3 dims · 5 facts · 7 marts     │
                    └───────────────┬────────────────┘
                          ┌─────────┴─────────┐
                          ▼                   ▼
                  ┌───────────────┐   ┌───────────────┐
                  │ Streamlit BI  │   │ RAG / Ask the │
                  │ dashboard     │   │ Data          │
                  └───────────────┘   └───────────────┘

        Airflow orchestrates every hop. Data quality gates each one.
```

The one architectural decision worth calling out: **DuckDB is the lake engine,
Postgres is the warehouse.** `dbt-postgres` cannot read Parquet from object
storage, so a design with "Parquet lake + dbt + Postgres" has a hole in the
middle where Bronze and Silver never reach dbt at all. DuckDB queries Parquet in
place over `httpfs`, and the Silver→warehouse hop streams through Arrow into
`COPY`. dbt never touches object storage; DuckDB never writes to Gold.

Full reasoning, and every other cross-component decision, is in
[`docs/CONTRACTS.md`](docs/CONTRACTS.md).

---

## Quickstart

```bash
cp .env.example .env      # then set the passwords; nothing has a default
make venv                 # .venv with the rag and dev extras
make seed                 # generate all source data (deterministic, ~15s)
make up                   # Postgres (pgvector) + MinIO, waits for health
make rag-index            # chunk, mask, embed, upsert  (~3 min first run)
make rag-eval             # measure recall@5 against the golden set
make test                 # the full suite
```

Then ask it something:

```bash
make search Q="tracking has not updated in over a week"
make ask    Q="why do customers ask for refunds?"
```

`make verify` runs the whole chain from a cold start, which is the honest way to
check any claim in this README. `make reset` destroys the volumes so
`db/init/*.sql` run again — Postgres only executes them on an empty data
directory, so `make down` alone will not re-run them.

**No `ANTHROPIC_API_KEY` is required.** Without one, retrieval is unchanged and
`make ask` answers extractively from the retrieved passages. With one, the same
passages go to `claude-opus-5`. A demo that degrades to "AI unavailable" has
demonstrated an API key, not a retrieval system.

`make seed` writes ~23 MB into `seeds/`, split by source system. It is gitignored
and fully reproducible: the same seed always produces byte-identical output.

---

## The AI layer

Built before the warehouse, deliberately. Support tickets are the one source
that needs no dimensional modelling to be useful, and building retrieval first
forces the PII question to be answered at the point where it is cheapest to get
right — before anything is embedded.

**Masking is dictionary-first.** The seed generator emits every name, email and
phone it invents into `seeds/known_pii_terms.json`, and masking reads that
manifest. Regex alone cannot recognise a name, and because the indexer skips
chunks whose content hash is unchanged, anything that leaks on the first pass is
never re-embedded — the leak becomes permanent and silent. The hash is therefore
taken over the *masked* text, so a masking fix and a re-embed are the same event.

**Retrieval is hybrid, and the fusion is one SQL statement.** BM25 over a
materialised inverted index, cosine similarity over an HNSW index, combined with
Reciprocal Rank Fusion. BM25 is implemented in SQL rather than borrowed from an
extension because Postgres's built-in `ts_rank_cd` has no inverse document
frequency at all — on a corpus where 70% of tickets contain the word "order" it
cannot tell that apart from an order number appearing in one.

Measured on `eval/golden_questions.yml` — 11 answerable questions, 5 that must
be refused:

| strategy | recall@5 | precision@5 | MRR@5 | abstention |
| --- | ---: | ---: | ---: | ---: |
| **hybrid** | **1.00** | **0.93** | **1.000** | **5/5** |
| lexical only | 1.00 | 0.93 | 1.000 | n/a |
| vector only | 0.91 | 0.91 | 0.909 | 5/5 |
| chance | 0.22 | — | — | — |

The chance row is why the others mean anything. The one question vector-only
misses names an order number that occurs in exactly one of the 1,311 tickets,
wrapped in ordinary words — asked for the bare identifier a vector finds it at
rank 1, but diluted by "what is the problem reported on order …" the embedding is
dominated by the common words and the right ticket falls out of the top five.
That single question is also what stops the eval saturating: without it all three
strategies score 1.00 on everything and the metric cannot detect a regression.

Abstention thresholds on cosine similarity rather than the fusion score, because
`1/(k+1)` is the same number whether the top hit is a paraphrase or unrelated.
`make rag-eval` prints the margin the threshold sits in (currently 0.629 to
0.724) rather than just a pass mark — a threshold outside that gap is a number
that happens to work, not a calibrated one.

---

## Repository layout

```
docker-compose.yml  `core` profile: Postgres (pgvector) + MinIO. `full` arrives later.
contracts/          topic manifest — broker init and client constants generate from it
db/init/            01..05 SQL, run once on an empty data directory, in this order
docs/
  CONTRACTS.md      the frozen interface between every component
  PROGRESS.md       current state and resume point
  governance/       PII classification, ownership and SLA
eval/
  golden_questions.yml   16 questions; how relevance is judged, and why it is not circular
src/meridian/
  settings.py       every knob, resolved from the environment once
  db.py             connections, opened as a named contract role
  runlog.py         JSON-line run logs and the frozen exit codes
  seed/             deterministic source-data generator
  rag/
    masking.py      dictionary-then-regex, driven by the governance YAML
    embeddings.py   fastembed, BAAI/bge-small-en-v1.5, 384 dims
    ddl.sql         the vector store; owned and applied by the indexer
    index.py        chunk → mask → hash → embed → upsert
    retrieve.py     BM25 + vector, fused with RRF in one statement
    generate.py     claude-opus-5, adaptive thinking, extractive fallback
    evaluate.py     recall@5 with a chance baseline and per-strategy breakdown
tests/              acceptance tests; DB-backed ones skip when the stack is down
```

---

## Concepts demonstrated

A map of each concept to the file that demonstrates it lives in
`docs/CONCEPTS.md` *(generated in Phase 8)*. Implemented so far:

| Concept | Where |
| --- | --- |
| Interface contracts | `docs/CONTRACTS.md` |
| Deterministic test data | `src/meridian/seed/` |
| Slowly Changing Dimension Type 2 | `seed/config.py` transitions → `dim_customer` (Phase 4) |
| PII classification and masking | `docs/governance/pii_classification.yml`, `seed/identity.py` |
| Data quality by design | `seed/defects.py` — known-bad rows for the DQ suite to catch |
| Kafka partitioning strategy | `contracts/topics.yml` |
| Least-privilege database roles | `db/init/04_roles.sql`, proven in `tests/test_rag_retrieval.py` |
| Hybrid retrieval (BM25 + vector, RRF) | `src/meridian/rag/retrieve.py` |
| BM25 implemented in SQL | `src/meridian/rag/ddl.sql`, `retrieve.py` |
| PII masking before embedding | `src/meridian/rag/masking.py`, `tests/test_pii_manifest.py` |
| Retrieval evaluation with a baseline | `eval/golden_questions.yml`, `rag/evaluate.py` |
| Graceful degradation without a vendor API | `src/meridian/rag/generate.py` |

---

## Notes on the data

It is synthetic, and generated to have the properties that make analytics
non-trivial:

- **24 months of history.** Ninety days makes a cohort retention heatmap a
  three-row triangle and collapses `NTILE(5)` RFM scoring into five identical
  buckets.
- **Lognormal basket values, Pareto order frequency.** A small share of customers
  place most orders, and order value has a long right tail — both true of real
  retail, and both things a normal distribution would hide.
- **Seasonality.** November and December carry a real peak.
- **A customer whose loyalty tier changes three times, and one hard delete.** An
  SCD2 snapshot over data that never changes produces one version per customer
  forever, and its tests pass trivially against an empty result set.
- **Defects, confined to third-party feeds.** Nulls, invalid enums, malformed
  dates, negative amounts and duplicate rows land in the file-drop and vendor
  sources only — never in OLTP, because orders are ingested from OLTP and
  corrupting that truth would make the reconciliation check fail permanently.

---

## Licence

Not yet chosen. Private repository.
