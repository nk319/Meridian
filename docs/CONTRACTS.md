# CONTRACTS

**Status: FROZEN as of Phase 0.**

This document is the single authority for every name that crosses a component
boundary. If code and this document disagree, the code is wrong.

Changing anything here after Phase 4 requires updating, in the same commit: the dbt
models, `dashboard/metrics.py`, the source-to-target mapping, and the data dictionary.
That is deliberate friction — it is cheaper than the alternative.

> **Why this exists.** Six independent component designs were produced for this
> platform. They specified five different warehouse schema layouts, four Kafka topic
> schemes, three data-quality results tables, and two incompatible `fact_orders`
> grains. The dashboard read `gold.mart_daily_sales`; dbt built
> `analytics_marts.agg_revenue_daily`. Not one dashboard query would have executed.
> The problem was not scope — it was that nobody owned the interface layer.

---

## 1. Databases, schemas, roles

**Postgres image: `pgvector/pgvector:pg16`.** Non-negotiable and used everywhere.
`postgres:16-alpine` has no pgvector and silently breaks the RAG layer. Startup
asserts the extension exists.

### Databases

| Database    | Purpose                                        |
| ----------- | ---------------------------------------------- |
| `oltp`      | Source system simulation. The API writes here. |
| `warehouse` | Lake landing, marts, ops metadata, vectors.    |
| `airflow`   | Airflow metadata only. Nothing else reads it.  |

### Schemas in `warehouse`

| Schema     | Contents                                            | Written by     |
| ---------- | --------------------------------------------------- | -------------- |
| `silver`   | Cleaned, typed, deduped. dbt's only source.         | loader         |
| `gold_stg` | dbt staging views                                    | dbt            |
| `gold_int` | dbt intermediate models                              | dbt            |
| `gold`     | Dims, facts, marts. The dashboard reads only this.  | dbt            |
| `meta`     | Ops, data quality, watermarks, lineage              | pipeline       |
| `rag`      | Vector store, chunks, eval results                  | rag indexer    |
| `secure`   | PII. Physically separated so grants can exclude it. | loader         |

Bronze is **not** a Postgres schema. Bronze lives only in object storage as Parquet
(§2). Anything that needs SQL over Bronze uses DuckDB against MinIO.

### Roles

| Role              | Grants                                                       |
| ----------------- | ------------------------------------------------------------ |
| `meridian_app`    | CRUD on `oltp.*`. No warehouse access.                       |
| `meridian_etl`    | Read `oltp.*`; write `silver.*`, `secure.*`, `meta.*`.       |
| `dbt_runner`      | Owns `gold_stg`, `gold_int`, `gold`. Reads `silver`.         |
| `analytics_ro`    | **SELECT on `gold` only.** No `secure`, no `oltp`, no `silver`. |
| `rag_indexer`     | Read `silver.support_*`; write `rag.*`. **No `secure` grant.** |

`analytics_ro` is what the Streamlit dashboard and every `/v1/ai/*` handler connect
as. `tests/test_pii_boundary.py` asserts `InsufficientPrivilege` when that role
selects a PII column. That test is the governance requirement's only actual proof.

`ALTER DEFAULT PRIVILEGES` is set so dbt-created tables are readable by
`analytics_ro` automatically, plus a belt-and-braces post-dbt `GRANT` task —
default privileges apply only to objects created after they are set.

### Init script ordering

Postgres aborts `docker-entrypoint-initdb.d` on any error, taking down every service
with `depends_on: service_healthy`. Scripts must create before they grant:

```
01_databases.sql    CREATE DATABASE oltp, warehouse, airflow
02_extensions.sql   CREATE EXTENSION vector, citext, pgcrypto  (unconditional)
03_schemas.sql      CREATE SCHEMA IF NOT EXISTS for all seven
04_roles.sql        CREATE ROLE, then GRANT  (schemas now exist)
05_oltp_ddl.sql     source-system tables
```

---

## 2. Bronze layout and metadata columns

```
s3://meridian-lake/bronze/{source}/{entity}/ingest_date=YYYY-MM-DD/part-{run_id}-{seq}.parquet
s3://meridian-lake/silver/{entity}/part-*.parquet
s3://meridian-lake/quarantine/{entity}/ingest_date=YYYY-MM-DD/part-*.parquet
```

`{source}` is one of `oltp`, `files`, `restapi`, `vendor`, `stream`.

Every Bronze file carries these columns in addition to source fields. The set is
frozen; adding one is a contract change.

| Column           | Type        | Meaning                                     |
| ---------------- | ----------- | ------------------------------------------- |
| `_ingested_at`   | timestamptz | Wall clock at write                         |
| `_ingest_run_id` | text        | UUID per pipeline run, joins to `meta`      |
| `_source_system` | text        | One of the five `{source}` values           |
| `_source_file`   | text        | Origin filename or endpoint path            |
| `_record_hash`   | text        | sha256 of business columns, for dedup       |
| `_batch_seq`     | bigint      | Monotonic within a run, for ordering        |

Bronze is append-only. Nothing rewrites it.

---

## 3. The Silver → warehouse mover

**This is the load-bearing hop.** The spec asked for Parquet in object storage, dbt
transformations, and a Postgres warehouse — but `dbt-postgres` cannot read Parquet
from MinIO, so as originally specified Bronze and Silver never reached dbt at all.

Resolution: **DuckDB is the lake engine, Postgres is the warehouse.**

```
Bronze Parquet (MinIO)
    ↓  DuckDB httpfs — SQL over Parquet, no load step
Silver Parquet (MinIO)          ← dedup, typing, PII scrub, quarantine
    ↓  DuckDB → Arrow → psycopg COPY
warehouse.silver.*  (Postgres)
    ↓  dbt-postgres
gold_stg → gold_int → gold
```

Arrow + `COPY` rather than DuckDB `ATTACH`: it streams in bounded memory, does not
require the Postgres extension, and fails loudly on type mismatch.

dbt never touches object storage. DuckDB never writes to `gold`. Prove this hop
works on day one, before any warehouse code.

---

## 4. Kafka topics

Broker is **Redpanda**, single node, Kafka wire protocol. Must run with
`--overprovisioned --smp 1 --memory 1G --reserve-memory 0M --check=false`; without
`--overprovisioned` Seastar busy-polls and pins a full core.

Topics are declared once in `contracts/topics.yml`. The broker init script and the
producer/consumer constants are **generated from it**, so drift is impossible.

| Topic                    | Partitions | Key           | Why that key                            |
| ------------------------ | ---------- | ------------- | --------------------------------------- |
| `ecom.web.events.v1`     | 3          | `session_id`  | All events in a session land in order   |
| `ecom.orders.placed.v1`  | 3          | `customer_id` | Per-customer ordering for SCD2 sanity   |
| `ecom.dlq.v1`            | 1          | `null`        | Dead letters; ordering irrelevant       |

Three partitions, not six: with three consumers you get one partition each, which
makes the rebalance demo legible. Six buys nothing here.

Consumer groups: `bronze-sink` (manual commit after persist) and `realtime-metrics`
(independent offsets). Two groups on the same topic is the only concrete proof that
group offsets are independent.

---

## 5. Python package and CLI entrypoints

Root package is **`meridian`**. (Never bare `platform` — it shadows a stdlib module.)

Every pipeline step is a module entrypoint runnable without Airflow. Airflow calls
these same commands; it never imports pipeline internals. This is what lets
`make demo` prove the platform works with the orchestrator switched off.

| Command                                            | Purpose                     |
| -------------------------------------------------- | --------------------------- |
| `python -m meridian.seed --out seeds/`             | Generate all source data    |
| `python -m meridian.ingest.oltp --mode full\|incremental`   | Postgres source     |
| `python -m meridian.ingest.files --mode full\|incremental`  | CSV/JSON drops      |
| `python -m meridian.ingest.restapi --mode incremental`      | Own REST API        |
| `python -m meridian.ingest.vendor --mode incremental`       | Simulated vendor    |
| `python -m meridian.lake.build_silver --entity <name>`      | Bronze → Silver     |
| `python -m meridian.lake.load_warehouse --entity <name>`    | Silver → Postgres   |
| `python -m meridian.dq.run --suite <name>`                  | Data quality        |
| `python -m meridian.rag.index --since <ts>`                 | Embed + upsert      |
| `python -m meridian.rag.enrich --limit <n>`                 | LLM enrichment      |

Exit codes: `0` success, `1` unexpected error, `2` data quality BLOCK, `3` upstream
unavailable. Logs are JSON lines on stdout with `run_id`, `step`, `entity`,
`rows_in`, `rows_out`, `duration_ms`.

---

## 6. Airflow

**Airflow 3.1.3**, pinned to the exact patch — the constraints branch is named per
patch, and `constraints-3.1.x` does not exist.

Executor: `LocalExecutor`. No Celery, no Redis, no workers.

dbt runs in an **isolated `/opt/dbt-venv`** invoked by absolute path, never
co-installed with Airflow. Airflow 3.1.3's constraints pin `protobuf==4.25.8`;
dbt-core requires `protobuf>=6.0`. Co-installing is an unresolvable resolver error —
the image provably cannot build. Backtracking dbt far enough lands on `pydantic<2`,
which breaks Airflow 3, FastAPI and pydantic-settings simultaneously.

---

## 7. `meta.*` tables

One DQ results table. One pipeline run log. Fed by Pandera, custom checks, and
parsed dbt `run_results.json` alike.

```sql
meta.dq_check_results (
  check_run_id    uuid,
  run_id          uuid,          -- joins meta.pipeline_run_log
  checked_at      timestamptz,
  source          text,          -- 'pandera' | 'dbt' | 'custom'
  suite           text,
  check_name      text,
  target_table    text,
  target_column   text,
  severity        text,          -- 'BLOCK' | 'QUARANTINE' | 'WARN'
  status          text,          -- 'PASS' | 'FAIL'
  rows_scanned    bigint,
  rows_failed     bigint,
  failure_pct     numeric,
  owner_team      text,
  repro_sql       text,          -- lets the dashboard link to a reproduction
  message         text
)

meta.pipeline_run_log (
  run_id        uuid primary key,
  dag_id        text,
  step          text,
  started_at    timestamptz,
  completed_at  timestamptz,     -- dashboard cache key reads THIS column
  status        text,            -- 'RUNNING' | 'SUCCESS' | 'FAILED'
  rows_in       bigint,
  rows_out      bigint
)

meta.ingest_watermarks (
  entity          text primary key,
  watermark_value timestamptz,
  updated_at      timestamptz
)

meta.kafka_consumer_offsets (
  consumer_group text,
  topic          text,
  partition      int,
  current_offset bigint,
  log_end_offset bigint,
  observed_at    timestamptz,
  primary key (consumer_group, topic, partition, observed_at)
)
```

The dashboard's cache key reads `meta.pipeline_run_log.completed_at`. When the
watermark is unavailable the dashboard shows a visible warning banner — never a
silent fallback constant, which hides a broken pipeline behind stale numbers.

---

## 8. Gold: the star schema and marts

### Grain — resolved

`fact_orders` is at **order-header grain**, and `fact_order_items` at **line grain**.

Two designers each specified one of these and declared it non-negotiable. Header-only
makes `dim_product` unjoinable and kills "top products"; line-only forces
`count(distinct order_id)` into every revenue and AOV query. The split costs one
small extra model and makes both correct.

**Expect this as the first schema question in an interview.** The answer is that
grain is a property of the business process, order headers and order lines are two
different processes, and a fact table serves exactly one grain.

### Surrogate keys

Suffix is `_sk`, everywhere, no exceptions. `dim_customer.customer_sk` is a surrogate
that changes across SCD2 versions; `customer_id` is the stable natural key.

### Tables

| Table                    | Grain                          |
| ------------------------ | ------------------------------ |
| `dim_date`               | one row per calendar day       |
| `dim_customer`           | one row per customer *version* (SCD2) |
| `dim_product`            | one row per product            |
| `fact_orders`            | one row per order              |
| `fact_order_items`       | one row per order line         |
| `fact_payments`          | one row per payment attempt    |
| `fact_web_events`        | one row per event              |
| `fact_support_tickets`   | one row per ticket, carries AI enrichment |

### SCD2 on `dim_customer`

Columns: `customer_sk`, `customer_id`, `loyalty_tier`, `segment`, `city`, `country`,
`valid_from`, `valid_to`, `is_current`, `version`.

Maintained by dbt snapshot, `check` strategy on `loyalty_tier` and `segment`, with
`hard_deletes='new_record'`.

**The generator must actually exercise this.** A snapshot over data that never
changes produces one version per customer forever, and its tests become tautologies
that pass on an empty result set. So the seed emits a designated customer with three
backdated `loyalty_tier` transitions and one customer who is hard-deleted. Three
singular tests assert: no overlapping validity windows per `customer_id`, exactly one
`is_current` per `customer_id`, and the demo customer has exactly three versions.

### Marts the dashboard reads

The dashboard reads **only** these, and only via `dashboard/metrics.py`.

| Mart                     | Feeds                                  |
| ------------------------ | -------------------------------------- |
| `mart_daily_sales`       | revenue, orders, AOV, trend            |
| `mart_customer_rfm`      | segmentation                           |
| `mart_cohort_retention`  | retention heatmap                      |
| `mart_product_performance` | top products, category drill          |
| `mart_payment_health`    | auth rate, failure reasons             |
| `mart_web_funnel`        | view → cart → checkout → purchase      |
| `mart_support_health`    | ticket volume, **AI sentiment/intent** |

`mart_support_health` depends structurally on the AI enrichment columns. That is
deliberate: it means the AI layer cannot quietly become a side attachment that
nothing consumes.

### Additivity rule

Non-additive measures carry an `nadd_` prefix (`nadd_aov`, `nadd_auth_rate`).
Anything without the prefix is safe to `SUM` across any dimension. A ratio summed
across a filter is the single most common silent dashboard bug.

---

## 9. Enum vocabularies

Frozen. The generator, the Pydantic models, the dbt tests, and the dashboard filters
all read these.

```yaml
order_status:     [pending, confirmed, shipped, delivered, cancelled, returned]
payment_status:   [authorized, captured, failed, refunded, chargeback]
payment_method:   [card, paypal, bank_transfer, gift_card]
loyalty_tier:     [bronze, silver, gold, platinum]
customer_segment: [new, active, at_risk, churned, vip]
channel:          [organic, paid_search, email, social, direct, affiliate]
device_type:      [desktop, mobile, tablet]
event_type:       [page_view, product_view, add_to_cart, begin_checkout, purchase, search]
ticket_intent:    [shipping_delay, refund_request, product_defect, billing_question,
                   account_access, return_process, general_inquiry]
ticket_priority:  [P1, P2, P3, P4]
sentiment:        [positive, neutral, negative]
dq_severity:      [BLOCK, QUARANTINE, WARN]
```

---

## 10. PII: classification and enforcement

PII is defined in `docs/governance/pii_classification.yml` and enforced three ways.

**Physical separation.** PII columns live in `secure.customer_pii`, never in
`silver.customers` and never in any `gold` model. The join key is `customer_id`.

**Grants.** `analytics_ro` and `rag_indexer` have no grant on `secure`. This is the
enforcement mechanism; the documentation merely describes it.

**A test.** `tests/test_pii_boundary.py` connects as `analytics_ro`, selects a PII
column, and asserts `InsufficientPrivilege`. A governance claim without this test is
a claim, not a control.

### PII in the RAG corpus — and why the sequencing matters

Support ticket text contains names, emails, and order references. It is masked
before embedding, never after.

Building the vector index before `dim_customer` exists would leave masking to regex
alone. Combined with content-hash skip logic — which exists so re-indexing is cheap —
unmasked chunks would then **never be re-embedded**. Customer names would sit in the
vector store permanently with nothing surfacing it.

So the seed generator emits `seeds/known_pii_terms.json`, a manifest of every
generated name, email, and phone number. Masking is dictionary-based from the very
first index and has no dependency on the warehouse existing.

`tests/test_pii_manifest.py` asserts the vector store contains zero terms from that
manifest. This is what makes AI-first sequencing safe rather than merely convenient.

---

## Deviations from the plan

| Plan said        | Built as   | Why                                                    |
| ---------------- | ---------- | ------------------------------------------------------ |
| `ecom_platform`  | `meridian` | Same non-shadowing property, matches the repo name.    |
