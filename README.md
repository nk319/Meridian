# Meridian

An end-to-end analytics platform for a mid-size online retailer — batch and
streaming ingestion, a Parquet lakehouse, a dimensional warehouse, orchestration,
data quality, governance, and a retrieval-augmented AI layer over customer
support history.

> **Build status: Phase 0 of 8 complete.** This README describes the system being
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

*Planned — `docker compose up` arrives in Phase 1.* Today:

```bash
make seed     # generate all source data (deterministic, ~15s)
make test     # 15 acceptance tests
make lint     # ruff
```

`make seed` writes ~23 MB into `seeds/`, split by source system. It is gitignored
and fully reproducible: the same seed always produces byte-identical output.

---

## Repository layout

```
contracts/          topic manifest — broker init and client constants generate from it
docs/
  CONTRACTS.md      the frozen interface between every component
  PROGRESS.md       current state and resume point
  governance/       PII classification, ownership and SLA
src/meridian/
  seed/             deterministic source-data generator
tests/              acceptance tests
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
