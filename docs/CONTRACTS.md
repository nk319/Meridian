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
| `meridian_etl`    | Read `oltp.*`; **owns** `silver`, `secure`, `meta`.          |
| `dbt_runner`      | Owns `gold_stg`, `gold_int`, `gold`. Reads `silver`.         |
| `analytics_ro`    | **SELECT on `gold`, `rag` and `meta`.** No `secure`, no `oltp`, no `silver`. |
| `rag_indexer`     | Read `silver.support_*`; write `rag.*`. **No `secure` grant.** |

`analytics_ro` is what the Streamlit dashboard and every `/v1/ai/*` handler connect
as. `tests/test_pii_boundary.py` asserts `InsufficientPrivilege` when that role
selects a PII column. That test is the governance requirement's only actual proof.

The `rag` and `meta` grants were added in Phases 1 and 2 and are corrections,
not widenings. This document already made `analytics_ro` the role the AI
handlers connect as, and those handlers read `rag.chunks`; §7 already said the
dashboard's cache key reads `meta.pipeline_run_log.completed_at`, and the
dashboard connects as the same role. "SELECT on `gold` only" could not coexist
with either. `rag` holds masked text by construction (§11) and `meta` holds ops
metadata; neither holds PII. The `secure` grant is unchanged — still none.

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

# Promoted in Phase 4, having been enforced as CHECK constraints since Phase 1.
# `ticket_status` is the ticket lifecycle, which this section originally had no
# vocabulary for at all.
ticket_status:    [open, pending, resolved]

# NOT `channel`. This is the contact channel a ticket arrived on; `channel`
# above is the marketing channel on orders and web events. Two real
# vocabularies that happen to share an English word, kept apart by name so the
# first person to union across them finds the distinction rather than the bug.
# Staged as `contact_channel` in gold, for the same reason.
ticket_channel:   [email, chat, phone, web_form]
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

## 11. The RAG store

Added in Phase 1. `rag.chunks` is a cross-component interface — the indexer
writes it, retrieval and every `/v1/ai/*` handler read it — so it belongs here
rather than in the module that happens to create it.

### Ownership

The `rag` schema is owned by `rag_indexer`, and its tables are created by
`python -m meridian.rag.index` from `src/meridian/rag/ddl.sql`, not by
`db/init/`. §1 freezes the init sequence at 01..05 and names `rag` as "written
by the rag indexer"; adding a `06_rag_ddl.sql` would contradict both. The
practical effect is that a fresh clone needs no migration step.

### The schema invariant

**`rag` holds masked text and nothing else.** That is a property of the schema,
not of particular tables, which is what makes it safe to grant `analytics_ro`
SELECT across it rather than on a hand-picked list that would drift.
`tests/test_pii_manifest.py` is what keeps the invariant true.

| Table | Grain | Notes |
| ----- | ----- | ----- |
| `rag.chunks` | one row per chunk of one ticket | masked `content`, `vector(384)` embedding, `content_hash`, `masking_fingerprint`, `embedding_model`, `doc_len` |
| `rag.chunk_terms` | one row per (chunk, lexeme) | the inverted index BM25 scores from; `tf` per term |
| `rag.index_runs` | one row per indexer run | chunks embedded/skipped/deleted, PII counts split by pass |
| `rag.eval_results` | one row per (eval run, strategy, question) | retrieval quality over time |

### Two properties that are load-bearing

**`content_hash` is taken over the masked text, never the raw.** The hash is the
indexer's skip key. Hashing raw input means a later masking fix leaves
already-indexed chunks with an unchanged hash, so they are skipped on every
subsequent run and the leaked text stays in the store permanently — the exact
failure §10 describes. Hashing the output makes "masking changed this chunk" and
"re-embed this chunk" the same event. The skip predicate is
`(content_hash, masking_fingerprint, embedding_model)`, so a change to the
policy, the manifest or the model re-embeds the corpus rather than leaving a mix
of vintages behind.

**`content_tsv` is generated from redaction-stripped text.** `[CUSTOMER_NAME]`
analyses to the lexemes `custom` and `name`, which would then appear in every
chunk that contained a person's name — 394 of 1,311 in the default seed, 30% of
the corpus. BM25 would treat them as ordinary terms with ordinary IDF, so a
question mentioning "customer" would score against redaction artefacts, matching
precisely the documents whose text had been removed. The placeholders stay in
`content`, where they inform a human reader and the model; they are not evidence
about topic.

### Retrieval

Hybrid: BM25 over `rag.chunk_terms` and cosine similarity over the HNSW index,
fused with Reciprocal Rank Fusion (`k=60`) in a single SQL statement.

BM25 is implemented in SQL rather than taken from an extension. Postgres's
built-in `ts_rank_cd` weights term frequency and proximity but has **no inverse
document frequency**, so on a corpus where 70% of tickets contain the word
"order" it cannot distinguish that from an order number appearing in one. IDF is
most of what makes lexical retrieval work here. Materialising `(chunk_id,
lexeme, tf)` makes real BM25 a plain aggregation — `df` is a COUNT, `dl` is a
SUM — at about 20,000 rows for this corpus, which is a smaller dependency than
pulling in `pg_search`.

Abstention thresholds on cosine similarity, not on the RRF score: `1/(k+1)` is
the same number whether the top hit is a paraphrase of the question or an
unrelated ticket, because rank carries no notion of closeness.

---

## 12. The batch pipeline

Added in Phase 2. §2 froze the Bronze layout and §3 the Silver→warehouse mover;
this is what the implementation of those settled that also crosses a boundary.

### One entity, one ingestion owner

| Entity | Source |
| --- | --- |
| `customers`, `orders`, `order_items`, `customer_change_log` | `oltp` |
| `products`, `web_events` | `files` |
| `support_tickets` | `restapi` |
| `payments` | `vendor` |

`support_tickets` lives in the `oltp` database and is still ingested from
`restapi`. That looks like an oversight and is not: an earlier design had one
entity arriving over four paths at once, which double-counted it in Bronze and
made the row-count reconciliation check fail permanently. The map above is
`meridian.ingest.base.OWNERSHIP`, and `check_ownership` refuses the mistake at
run time rather than leaving it to review.

### Bronze is raw; typing happens in Silver

The file, API and vendor feeds land as VARCHAR. They carry injected defects —
malformed dates, invalid enums, blanked required fields — and an ingestion layer
that typed on the way in would reject those rows at the door, leaving no
distinction between a row that was refused and a row that was never sent. OLTP
lands typed, because from a relational source a typed row *is* what was sent.

### Silver carries four of the six Bronze columns

`_source_system`, `_ingest_run_id`, `_ingested_at`, `_record_hash`.

`_source_file` and `_batch_seq` stop at Bronze. They describe a physical file,
and after dedup across runs a Silver row no longer corresponds to one — carrying
them would mean picking an arbitrary winner's file name and calling it
provenance.

### Silver has no cross-table foreign keys

A quarantined customer would make its orders unloadable, coupling every
entity's load to every other entity's quarantine decisions. Referential
integrity is asserted by dbt tests, where a violation is a reported failure
rather than a hard stop halfway through a load. The CHECK constraints on
vocabularies stay, because those are per-row and are what make Silver's
"bad rows were quarantined" guarantee enforced rather than claimed.

### Quarantine

```
s3://meridian-lake/quarantine/{entity}/ingest_date=YYYY-MM-DD/part-{run_id}.parquet
```

Rejected rows keep their **original** values plus one extra column,
`_quarantine_reason`, formatted `rule:column` — one of `missing`, `bad_type`,
`bad_enum`, `out_of_range`. Storing the cast values instead would fill the
quarantine with NULLs exactly where the bad data used to be.

Every rule writes a `meta.dq_check_results` row under suite `silver_build`, at
severity `QUARANTINE`. One additional check per entity, `quarantine_rate`, runs
at severity `BLOCK`: above 10% rejected the step exits 2 rather than publishing
a Silver table that is missing most of its rows, which downstream reads as a
business collapse rather than a pipeline failure.

### Full rebuild, and what that implies about naming

Silver is rebuilt from Bronze in full on every run, into a single
`part-000.parquet` per entity that is replaced in place. Naming it by run id
would leave every previous rebuild under the same glob, and the loader would
then read every generation at once — a duplicate explosion that grows by one
full copy per run and looks like a dedup bug rather than a naming one.

### PII never enters the lake

`secure.customer_pii` is loaded directly from the generated file by
`meridian.lake.load_warehouse`, bypassing Bronze and Silver entirely. That is
the enforcement mechanism from §10, not a shortcut: there is no stage that holds
restricted data alongside business data, so there is no step that could forget
to drop it. `build_silver.assert_no_restricted_columns` is the other half — it
reads the restricted list from `pii_classification.yml` and fails the build if
such a column ever appears in Bronze.

---

## 13. Data quality and orchestration

Added in Phase 3.

### Where a rule lives decides what it can catch

Three layers, and each one asserts something the others structurally cannot:

| Layer | Mechanism | Catches |
| --- | --- | --- |
| Write path | CHECK constraints (`db/init`, `warehouse/ddl.sql`) | a row that violates a frozen vocabulary, at insert |
| Load path | `lake/build_silver.py` | a row that fails to type or parse — quarantined with a reason |
| Post-load | `meridian.dq.run` | **relationships and distributions** |

The third is the one worth naming. A CHECK constraint is per-row: it cannot know
that an order references a customer who does not exist, that 72% of orders are
usually delivered, or that the catalogue runs at 43% margin. Every individual row
can be valid while the set as a whole is wrong, and that is precisely how an
upstream change arrives.

Suites: `referential`, `business`, `volume`, `freshness` (SQL, `source='custom'`)
and `schema` (Pandera, `source='pandera'`). `all` is their union.

### Severity decides consequence, not importance

`BLOCK` exits 2 and stops the pipeline. `WARN` is recorded and visible. Every
non-zero tolerance states its reason in `dq/suites.py` — a threshold picked by
guessing either never fires or always does.

The worked example is `fk_order_items_product_id`, which runs at WARN with a 3%
tolerance. Quarantine fans out: three rejected product rows (1.36% of the
catalogue) orphan 380 order lines (1.46% of the table). Those lines are valid and
will silently vanish from any join to `dim_product`. Blocking the pipeline would
be wrong; saying nothing would be worse.

### Freshness is about the pipeline, not the data

The generator writes to a fixed anchor date, so the newest order is always the
same age and a check on `max(order_ts)` would fail purely because time passed.
Freshness asserts that the last successful run of each step completed recently,
which is the thing that is actually true or false about a running platform.

### Orchestration

**Airflow 3.1.3**, pinned to the patch — the constraints branch is named per
patch and `constraints-3.1.x` does not exist. `LocalExecutor`.

The image carries **three Python environments**, and the separation is forced
rather than stylistic:

| Environment | Holds | protobuf |
| --- | --- | ---: |
| Airflow's own | the scheduler, api-server and dag-processor | 4.25.8 |
| `/opt/meridian-venv` | the pipeline and every dependency it has | — |
| `/opt/dbt-venv` | dbt | 6.33.6 |

§6 required the dbt split for exactly that pin. The same argument turned out to
apply to the RAG layer, which pulls onnxruntime through fastembed and has its own
protobuf floor — so the pipeline got the same treatment rather than a special
case. `airflow/Dockerfile` asserts all three still import at build time.

The consequence is the property §5 asked for: **every task is a subprocess, and
no DAG imports anything from `meridian`.** That is what lets `make pipeline`
prove the platform works with the orchestrator switched off, and it means a
pipeline dependency change cannot take the scheduler down.

### Data-aware scheduling

`meridian_batch`'s warehouse load declares `meridian://silver/support_tickets` as
an outlet; `meridian_rag` is scheduled **on that asset** rather than on a cron
expression. Indexing runs when the tickets it indexes have landed, instead of at
a time somebody guessed the batch would be done by. Re-indexing is cheap by
design — the content hash means an unchanged corpus re-embeds nothing — so a
spurious trigger costs seconds.

Airflow's metadata lives in the `airflow` database under its own `airflow` role,
which owns that database and holds nothing in `oltp` or `warehouse`. A
compromised orchestrator cannot read the warehouse; no pipeline role can perturb
Airflow's bookkeeping.

---

## Deviations from the plan

| Plan said        | Built as   | Why                                                    |
| ---------------- | ---------- | ------------------------------------------------------ |
| `ecom_platform`  | `meridian` | Same non-shadowing property, matches the repo name.    |
| §1: `analytics_ro` has "SELECT on `gold` only" | `gold` **and** `rag` | §1 also makes `analytics_ro` the role every `/v1/ai/*` handler connects as, and those handlers must read `rag.chunks`. Both could not hold. Safe because `rag` holds masked text by construction (§11) and `tests/test_pii_manifest.py` enforces it. The grant on `secure` is unchanged: still none. |
| §9 freezes one vocabulary named `channel` | two, sharing the name | `orders.channel` is the marketing channel (`organic`, `paid_search`, …); `support_tickets.channel` is the contact channel (`email`, `chat`, `phone`, `web_form`). Both are real and neither should borrow the other's name. Recorded so the first person to write a union across them finds this instead of the bug. |
| §9 has no `ticket_status` | `open`, `pending`, `resolved` | The source system has ticket lifecycle state and §9 never gave it a vocabulary. Enforced as a CHECK constraint in `db/init/05_oltp_ddl.sql` since Phase 1. **Promoted into §9 in Phase 4**, as this row said it would be. |
| §1: `rag_indexer` reads `silver.support_*` | schema USAGE only, so far | `silver.support_tickets` does not exist until Phase 2. Granting SELECT on all of `silver` now would be broader than the contract; the table-level grant is issued when the table is created. Phase 1 indexes from `seeds/` and needs no `silver` read at all. |
| §7: one pipeline run log | plus `rag.index_runs` | Not a second run log. `meta.pipeline_run_log` has nowhere to record chunks skipped by content hash or PII hits split by masking pass, which are the numbers that say whether the indexer is working. `rag_indexer` is still granted nothing on `meta`. |
| §1: `analytics_ro` has "SELECT on `gold` only" | `gold`, `rag` **and `meta`** | The second half of the same contradiction. §7 states the dashboard's cache key reads `meta.pipeline_run_log.completed_at`, and §1 makes `analytics_ro` the role the dashboard connects as. Read-only; `meta` holds no PII. The `secure` grant is still none. |
| §1: `meridian_etl` writes `silver`, `secure`, `meta` | and **owns** those schemas | Writing was not enough. `src/meridian/warehouse/ddl.sql` has to `GRANT SELECT ON silver.support_tickets TO rag_indexer` — the narrow grant §1 specifies, which could not be issued in `db/init/04` because the table did not exist yet — and only a schema's owner can grant on the tables in it. Mirrors `dbt_runner` owning `gold*` and `rag_indexer` owning `rag`. |
| §2: six Bronze metadata columns | four of them reach Silver | `_source_file` and `_batch_seq` describe a physical file. After dedup across runs a Silver row corresponds to no single file, so carrying them would mean presenting an arbitrary winner's filename as provenance. |
| `order_items` as the source has it | Bronze also carries `order_ts` | The source table has no timestamp of its own, so without the parent order's it could only ever be full-refreshed — a full reload of the largest child table on every run is what incremental ingestion exists to avoid. Dropped again in Silver; it is capture machinery, not a business column. |
| §9 has no `ticket_channel` | `email`, `chat`, `phone`, `web_form` | Same gap as `ticket_status`, and worse because the name collides: §9's frozen `channel` is the marketing channel on orders and web events. **Promoted into §9 in Phase 4** under its own name, and staged into gold as `contact_channel` so the collision cannot be reintroduced by a `select *`. |
| §6: dbt isolated from Airflow | **and the pipeline isolated too** | §6 named one irreconcilable pin. There are two: fastembed pulls onnxruntime, which has its own protobuf floor and would break Airflow exactly as dbt would. Three interpreters, not two. |
| §1: five roles | six — `airflow` added | §1 describes the `airflow` database as "metadata only" but named no role for it, so no role could connect to it at all. The new role owns that database and holds nothing anywhere else. |
| §1: `dbt_runner` "reads `silver`" | `silver` **and one table in `rag`** | §8 makes `gold.fact_support_tickets` carry the AI enrichment and says `mart_support_health` "depends structurally on the AI enrichment columns" — deliberately, so the AI layer cannot become a side attachment. That enrichment has to live somewhere dbt can see it. Resolved towards `rag` rather than `silver`: the alternative makes `rag_indexer` a writer of `silver` (which §1 gives to the loader) and puts a model's output in a layer defined as cleaned source data. `rag.ticket_enrichment` holds identifiers, §9 labels and provenance — no free text. |
| §1: `dbt_runner` owns three schemas | plus `TEMPORARY` on the database | `db/init` revokes ALL on `warehouse` from PUBLIC, which takes away the default TEMP grant. dbt snapshots stage incoming rows in a temporary table, so `dbt snapshot` fails with "permission denied to create temporary tables" for a role that can create every table in three schemas. Temp tables live in a per-session schema no other session can see. |
| §8: `dim_customer` "maintained by dbt snapshot" | plus a one-time history backfill | A snapshot records what it sees when it runs, and `silver.customers` holds current state — so a first run produces one version per customer, and §8's own three SCD2 tests all pass against a dimension with no history in it. `scripts/dbt_snapshot_backfill.sh` walks `silver.customer_change_log` and snapshots a point-in-time reconstruction at each transition, overriding `snapshot_get_time()` so `dbt_valid_from` carries the date the tier actually changed. Deliberately not an Airflow task: it is not idempotent. |
| §8: the earliest `dim_customer` version starts when the snapshot does | back-opened to `-infinity` | SCD2 history begins at the first change log entry (2025-02-07); orders begin 2024-08-20. A straight `order_ts BETWEEN valid_from AND valid_to` join would drop six months of revenue, silently, because an inner join to a dimension is exactly as quiet as a filter. The open version's `valid_to` is `infinity` for the symmetric reason: a null there costs a `coalesce` in every as-of join, which is one somebody forgets. |
| §11: RRF `k=60`, "deliberately not tuned per-corpus" | unchanged, and now measured | Kept — the reasoning is right and a k fitted to this seed is the calibration RRF exists to avoid. But `RAG_CANDIDATE_POOL` is 50, so k is *larger than the pool*: ranks 1 to 50 span 1/61 to 1/111, under a factor of two, and RRF's agreement bias dominates. Measured over twelve unique order identifiers, BM25 ranks the right ticket first 12/12 and fusion keeps 10 — a document both rankers rank 27th and 36th (0.0219) outscores one the lexical half ranks first (1/61 = 0.0164). `tests/test_rag_retrieval.py` asserts the cost rather than hiding it. |
| §4: topics declared once, "generated from it" | the init script is Python, not shell | A shell script running `rpk topic create` is a *second* place the partition counts live, which is the drift §4 introduced the manifest to prevent. `meridian.stream.admin --create` reads the same loader the producer and consumers do, and `tests/test_stream.py` greps the package for a hardcoded topic name. |
| §4 names three topics and their keys | plus Avro schemas under `contracts/schemas/` | The manifest already pointed at `schemas/*.avsc`; the files did not exist. Written as real schemas rather than decoration: enums for every §9 vocabulary, so an out-of-vocabulary value fails at the producer where the error names the field, and `decimal` rather than `double` for amounts. |
| §1: `silver` is written by the loader, from one owning source per entity | `web_events` also reads Bronze from `stream` | The one-entity-one-owner rule (§12) is what makes the four batch ingestors safe to run in parallel, and it is unchanged — `bronze-sink` writes the *same events* the `files` ingestor captures, under `source='stream'`, and Silver's dedup on the natural key collapses the two captures into one row. Measured: 156,877 + 414 Bronze rows in, still exactly 153,136 Silver rows out. Reconciling batch and stream here is the alternative to publishing two tables that disagree. |
| §7: one DQ table, one run log | plus `meta.stream_metrics` | Not a third results table. Windowed counts from the streaming path, which `meta.dq_check_results` (per-check outcomes) and `meta.pipeline_run_log` (per-step runs) have nowhere to put. Kept out of `gold` deliberately: these are what the stream believed at a point in time from an at-least-once feed, and the marts are what the batch path concluded after deduplication. They disagree, and that is the tradeoff rather than a defect. |
| §1 names no `users` table | API accounts come from the environment | The contract declares three databases and seven schemas, and none of them holds identities. Inventing one would be adding schema the contract does not have, for a data platform that needs *an* authentication story rather than an identity provider. `API_DEMO_PASSWORD` / `API_DEMO_USERS` seed them; with neither set `/v1/auth/token` returns 503, because there is deliberately no default credential. |
| §5's `.env` carries `API_INGEST_KEY` | plus three scopes on the JWT | A single "authenticated" flag cannot express "may read tickets but may not spend model tokens", and asking the model a question is the one operation here that costs money. Three scopes, not fifteen: a permission model nobody can hold in their head gets bypassed with a wildcard. The ingest key carries `tickets:write` only. |
| §5: the API is a Bronze source | it is now the *live* one, optionally | `meridian.ingest.restapi` reads the running service when `MERIDIAN_API_URL` is set and `seeds/` otherwise. Both paths end in the same `read_json`, so the transport is the only difference and nothing downstream can tell which ran — which is what Phase 2 fixed the columns, watermark and Bronze path for. |
| §8: the dashboard reads only `gold` | `gold` **and** `meta` | §7 already required it: the cache key is `meta.pipeline_run_log.completed_at`, and the DQ results the operations tab shows are in the same schema. `gold` is the analytical boundary and `meta` is the operational one; `tests/test_dashboard.py` asserts that `silver`, `secure`, `oltp`, `gold_stg` and `gold_int` appear nowhere in `metrics.py`. |
