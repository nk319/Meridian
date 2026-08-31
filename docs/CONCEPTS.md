# Concepts, and where each one lives

A map from a data-engineering idea to the file that demonstrates it, and — where
the demonstration cost something — to what it cost.

Two rules govern what is in this document.

**Every row points at code that runs.** Not at a comment claiming a technique is
used. If a concept is here, there is a file, and usually a test that fails when
the property stops holding.

**Where a decision had a losing alternative, the alternative is named.** "We use
a star schema" is not an insight; "we split the order fact into header and line
grains, because header-only makes `dim_product` unjoinable and line-only forces
`count(distinct order_id)` into every revenue query" is. The second column of
each table is where that lives.

Total: **274 tests**, **12,634 lines** of Python across `src/` and `dashboard/`,
plus 29 dbt models, 6 singular dbt tests and 38 data quality checks.

Each of those is a command, not a claim — §14's thirteenth row is what happens
when it is the other way round:

```bash
pytest --collect-only            # 274 tests collected
find src dashboard -name '*.py' -not -path '*/__pycache__/*' | xargs wc -l
cd dbt && dbt ls                 # Found 29 models, 1 snapshot, 87 data tests
                                 # `dbt run` reports PASS=28: one model is
                                 # ephemeral, so it is inlined, never built
ls dbt/tests/*.sql | wc -l       # 6 singular tests
python -c "from meridian.dq import suites; print(len(suites.build()['all']))"
                                 # 31 SQL checks, plus 7 Pandera frame specs = 38
```

---

## Contents

1. [Contracts and interfaces](#1-contracts-and-interfaces)
2. [Test data that is worth testing against](#2-test-data-that-is-worth-testing-against)
3. [The medallion architecture](#3-the-medallion-architecture)
4. [Ingestion](#4-ingestion)
5. [Dimensional modelling](#5-dimensional-modelling)
6. [Data quality](#6-data-quality)
7. [Orchestration](#7-orchestration)
8. [Streaming](#8-streaming)
9. [The AI layer](#9-the-ai-layer)
10. [Security and governance](#10-security-and-governance)
11. [The API](#11-the-api)
12. [The dashboard](#12-the-dashboard)
13. [Engineering practice](#13-engineering-practice)
14. [Mistakes this project actually made](#14-mistakes-this-project-actually-made)

---

## 1. Contracts and interfaces

| Concept | Where, and what it buys |
| --- | --- |
| **A frozen interface document** | [`docs/CONTRACTS.md`](CONTRACTS.md). Written before any code, and the reason the eight phases fit together: two components that agree about a table name in a design document and disagree in code produce a dashboard where not one query executes. The document's own header records that failure happening. |
| **Recording deviations rather than editing history** | [`docs/CONTRACTS.md`](CONTRACTS.md) — the deviations table, 21 rows. Every one is a case where the contract turned out to be wrong or under-specified. Silently editing the contract would have destroyed the evidence that the design was tested against reality. |
| **A single source of truth, enforced** | [`contracts/topics.yml`](../contracts/topics.yml) → [`stream/topics.py`](../src/meridian/stream/topics.py). Nothing else may name a topic; [`tests/test_stream.py`](../tests/test_stream.py) greps the package for a hardcoded one. A manifest that things bypass is documentation, not a source of truth. |
| **A resume point that survives a lost session** | [`docs/PROGRESS.md`](PROGRESS.md). Updated as the last action of every session. The alternative is conversation history, which does not survive a container restart. |

---

## 2. Test data that is worth testing against

| Concept | Where, and what it buys |
| --- | --- |
| **Deterministic generation** | [`seed/`](../src/meridian/seed/). One pinned seed, byte-identical output, 23 MB gitignored rather than committed. |
| **Distributions that make analytics non-trivial** | [`seed/config.py`](../src/meridian/seed/config.py). Lognormal basket values and Pareto order frequency, so RFM quintiles separate and a cohort heatmap has a shape. Uniform random data makes every analytical technique look like it works. |
| **24 months, not 90 days** | Ninety days makes a retention heatmap a three-row triangle and collapses `NTILE(5)` into five identical buckets. |
| **Deliberately injected defects** | [`seed/defects.py`](../src/meridian/seed/defects.py) — 4,133 of them, with a manifest. Unparseable timestamps, out-of-vocabulary enums, empty required fields. A quality suite that has never caught anything is untested. |
| **Data that exercises the feature it exists for** | [`seed/entities.py`](../src/meridian/seed/entities.py) emits `C000042` with three backdated tier transitions and hard-deletes `C000117`, so the SCD2 dimension and `hard_deletes='new_record'` have something to do. See §14 for what happened when that was subtly wrong. |
| **Publishing what the tests need to name** | [`seed/anchors.py`](../src/meridian/seed/anchors.py). The generator writes the values `eval/golden_questions.yml` and `tests/test_rag_retrieval.py` depend on, rather than those files hardcoding a value that a later generator change invalidates. |

---

## 3. The medallion architecture

| Concept | Where, and what it buys |
| --- | --- |
| **Bronze / Silver / Gold** | [`lake/`](../src/meridian/lake/). Bronze is append-only Parquet on object storage — deliberately **not** a Postgres schema. |
| **Raw capture vs. typed publication** | [`lake/bronze.py`](../src/meridian/lake/bronze.py) vs. [`lake/build_silver.py`](../src/meridian/lake/build_silver.py). Bronze stores what the source sent, defects included; Silver decides what it means. Typing at capture destroys the evidence of what arrived. |
| **A positional record hash** | [`lake/bronze.py`](../src/meridian/lake/bronze.py) — `record_hash_expr`. `concat_ws` drops NULLs, so `('a', NULL, 'b')` and `('a', 'b', NULL)` hash identically without a coalesced sentinel and two different records dedup into one. |
| **Quarantine with a machine-readable reason** | [`lake/build_silver.py`](../src/meridian/lake/build_silver.py). A rejected row keeps its original value and the reason it failed, and the counts land in `meta.dq_check_results`. |
| **DuckDB as the lake engine** | [`lake/duck.py`](../src/meridian/lake/duck.py). `dbt-postgres` cannot read Parquet from object storage, so "Parquet lake + dbt + Postgres" has a hole in the middle. DuckDB queries Parquet in place over `httpfs`. |
| **Streaming load in bounded memory** | [`lake/load_warehouse.py`](../src/meridian/lake/load_warehouse.py). DuckDB → Arrow → psycopg **binary** COPY. The load-bearing hop, and the one that makes the size of a table irrelevant to the memory of the loader. |

---

## 4. Ingestion

| Concept | Where, and what it buys |
| --- | --- |
| **Four source shapes, one framework** | [`ingest/`](../src/meridian/ingest/) — an OLTP database, a CSV drop, a REST API and a cursor-paginated vendor feed. |
| **One entity, one owner** | [`ingest/base.py`](../src/meridian/ingest/base.py) — the `OWNERSHIP` map, enforced by `check_ownership`. It is what makes the five ingest tasks safe to run in parallel; without it that parallelism is a race. |
| **Watermarked incremental capture** | [`ingest/base.py`](../src/meridian/ingest/base.py) — `incremental_where`, reading `meta.ingest_watermarks`. |
| **Anti-joining the watermark against Bronze** | Same file. A malformed row has no parseable timestamp, so a pure watermark re-captures it on every run forever. Anti-joining the record hash admits it exactly once. |
| **Cursor pagination, consumed** | [`ingest/vendor.py`](../src/meridian/ingest/vendor.py) follows `next_cursor` to exhaustion. |
| **The same contract over two transports** | [`ingest/restapi.py`](../src/meridian/ingest/restapi.py) reads a JSON document or the live API depending on one environment variable, and both end in the same `read_json`. The columns, watermark and Bronze path were fixed in Phase 2 precisely so Phase 6 could swap the transport and change nothing downstream. |

---

## 5. Dimensional modelling

| Concept | Where, and what it buys |
| --- | --- |
| **A star schema with a split fact grain** | [`dbt/models/marts/fact_orders.sql`](../dbt/models/marts/fact_orders.sql) (header) and [`fact_order_items.sql`](../dbt/models/marts/fact_order_items.sql) (line). Header-only makes `dim_product` unjoinable and kills "top products"; line-only forces `count(distinct order_id)` into every revenue and AOV query. The split costs one model and makes both correct. |
| **Proving the split did not lose money** | [`dbt/tests/revenue_reconciles_across_grains.sql`](../dbt/tests/revenue_reconciles_across_grains.sql). Two facts built from different sources and joined independently to a type-2 dimension; either can fan out or lose rows without the other noticing, and a fan-out on the line fact looks like a successful product launch. |
| **SCD2** | [`dbt/snapshots/customers_snapshot.sql`](../dbt/snapshots/customers_snapshot.sql) and [`dim_customer.sql`](../dbt/models/marts/dim_customer.sql). `check` strategy on `loyalty_tier` and `segment` — the `timestamp` strategy would open a new version when somebody corrected a city. |
| **Backfilling SCD2 history that predates the snapshot** | [`scripts/dbt_snapshot_backfill.sh`](../scripts/dbt_snapshot_backfill.sh) and [`macros/snapshot_get_time.sql`](../dbt/macros/snapshot_get_time.sql). A snapshot records what it sees when it runs; against current-state-only source data every SCD2 test passes on a dimension with no history in it. |
| **As-of joins** | Every fact — `order_ts >= valid_from AND order_ts < valid_to`. Joining on `is_current` instead is invisible: the numbers still add up, they are just answers to a different question. |
| **Closing validity windows at both ends** | `dim_customer` opens version 1 at `-infinity` and closes the current one at `infinity`. History starts at the first change log entry; orders start six months earlier, and an inner join to a dimension is exactly as quiet as a filter. |
| **Hard deletes as a dated event** | `hard_deletes='new_record'`. Under dbt's default the deleted customer's last version stays `is_current` forever and the dimension goes on asserting they are live — a failure with no error message attached to it. |
| **The additivity rule** | The `nadd_` prefix on every ratio, enforced by [`tests/test_gold.py`](../tests/test_gold.py) reading the information schema. A convention nothing checks is a comment. |
| **A gap-free date spine** | [`dim_date.sql`](../dbt/models/marts/dim_date.sql) and [`mart_daily_sales.sql`](../dbt/models/marts/mart_daily_sales.sql). A revenue chart built from `group by order_date` has no zeros, so a dead week renders as a straight line between the days either side of it. |
| **Session-grain funnels** | [`int_session_funnel.sql`](../dbt/models/intermediate/int_session_funnel.sql). An event-counted funnel can show more carts than views, because one session adds four items. |
| **First-order cohorts, not signup cohorts** | [`mart_cohort_retention.sql`](../dbt/models/marts/mart_cohort_retention.sql). Signup cohorts measure how good marketing was at collecting registrations. |
| **RFM as population quintiles** | [`mart_customer_rfm.sql`](../dbt/models/marts/mart_customer_rfm.sql) — `ntile(5)`. A hardcoded "spent over £500 = a 5" stops meaning anything the moment the business changes size. |

---

## 6. Data quality

| Concept | Where, and what it buys |
| --- | --- |
| **Three layers, by what each can catch** | CHECK constraints (write path) → [`build_silver.py`](../src/meridian/lake/build_silver.py) (load path) → [`dq/`](../src/meridian/dq/) (post-load). A CHECK constraint is per-row: it cannot know that 72% of orders are usually delivered. Every row can be valid while the set is wrong, and that is how an upstream change arrives. |
| **Declarative checks with a reproduction query** | [`dq/checks.py`](../src/meridian/dq/checks.py). A finding nobody can reproduce is a number nobody acts on. |
| **Distribution assertions** | [`dq/schemas.py`](../src/meridian/dq/schemas.py) — seven Pandera frames, every bound measured against the loaded warehouse and widened ~20%. |
| **Severity as consequence, not importance** | `BLOCK` exits 2 and stops the pipeline; `WARN` is recorded. `fk_order_items_product_id` runs at WARN with a 3% tolerance because 3 rejected products orphan 380 valid order lines — stopping would be wrong, silence would be worse. |
| **Freshness about the pipeline, not the data** | [`dq/suites.py`](../src/meridian/dq/suites.py). The generator writes to a fixed anchor, so a check on `max(order_ts)` fails purely because time passed, and a dashboard that is red by construction is one people stop reading. |
| **One results table across three tools** | `meta.dq_check_results`, fed by Pandera, custom SQL and [`dbt/results.py`](../src/meridian/dbt/results.py). The question at 3am is "what is failing", not "what is failing in each of two systems". |
| **`error` and `skipped` are failures** | [`dbt/results.py`](../src/meridian/dbt/results.py). A test that could not run has not passed, and recording it as PASS is how a test broken by a compilation error stays broken for a quarter. |

---

## 7. Orchestration

| Concept | Where, and what it buys |
| --- | --- |
| **Orchestration without import coupling** | [`airflow/dags/`](../airflow/dags/). No DAG imports anything from `meridian`; every task is a subprocess invoked by absolute path. That is what lets `make pipeline` prove the platform works with the orchestrator switched off. |
| **Dependency isolation by interpreter** | [`airflow/Dockerfile`](../airflow/Dockerfile) — three venvs in one image. Airflow 3.1.3 pins protobuf 4.25.8; dbt-core requires ≥6; fastembed pulls onnxruntime with its own floor. Not a preference — the image provably cannot build otherwise, and it asserts all three still import at build time. |
| **Data-aware (asset) scheduling** | [`meridian_rag.py`](../airflow/dags/meridian_rag.py) runs on `meridian://silver/support_tickets` rather than a cron guess that fires early on a slow day. |
| **Putting the asset on the task that proves the claim** | [`meridian_batch.py`](../airflow/dags/meridian_batch.py) — `GOLD_READY` hangs off `dbt_test`, not off the parse step that follows it on `all_done`. Otherwise a red suite wakes every consumer. |
| **A task that must run when the previous one failed** | Same file — `dbt_results` with `trigger_rule="all_done"`. Its whole job is recording what dbt found. |
| **Scheduling what a scheduler is good at** | [`meridian_stream_ops.py`](../airflow/dags/meridian_stream_ops.py) records consumer lag every 15 minutes and does not run the consumers. A consumer that finishes is a consumer that has stopped consuming. |

---

## 8. Streaming

| Concept | Where, and what it buys |
| --- | --- |
| **Partition keys as ordering guarantees** | [`contracts/topics.yml`](../contracts/topics.yml). `session_id` keeps a visitor's events in order; `customer_id` keeps two events for one customer from being processed out of order. [`topics.py`](../src/meridian/stream/topics.py) raises rather than round-robining a record with a null key. |
| **Avro without a Schema Registry** | [`contracts/schemas/`](../contracts/schemas/) and [`stream/client.py`](../src/meridian/stream/client.py). Single-object encoding — the `C3 01` marker plus a CRC-64-AVRO fingerprint — so a consumer can *detect* a schema it was not expecting. Avro is positional: decoding under the wrong schema does not error, it produces plausible nonsense. |
| **Enums in the schema, not strings** | The §9 vocabularies as Avro `enum`s, so `add_to_kart` fails at the producer where the error names the field, rather than days later as a funnel step that never fires. |
| **`decimal`, not `double`, for money** | [`contracts/schemas/order_placed.avsc`](../contracts/schemas/order_placed.avsc). A float amount is a rounding error waiting for a SUM. |
| **Manual offset commit, after the work** | [`stream/consume.py`](../src/meridian/stream/consume.py). `enable.auto.commit` defaults to *true*, and a consumer that auto-commits has acknowledged messages it may still drop. |
| **Committing every partition in a batch** | Same file. See §14 — this is the bug that made the rule concrete. |
| **A dead letter queue that unblocks a partition** | [`stream/consume.py`](../src/meridian/stream/consume.py) and [`stream/dlq.py`](../src/meridian/stream/dlq.py). The naive failure is a consumer that raises, restarts, re-reads the same message and raises again forever with lag climbing behind it. The envelope carries topic/partition/offset because a dead letter you cannot trace back is a log line. |
| **Independent consumer group offsets** | [`sink_bronze.py`](../src/meridian/stream/sink_bronze.py) and [`metrics.py`](../src/meridian/stream/metrics.py) on the same topic. Two groups is the only concrete proof that offsets are independent. |
| **Lag as a time series, not a gauge** | [`stream/lag.py`](../src/meridian/stream/lag.py). A single reading cannot distinguish a dead consumer from a slow one — one climbs without bound, the other plateaus. `observed_at` is in the primary key for that reason. |
| **Event-time windows, not arrival-time** | [`stream/metrics.py`](../src/meridian/stream/metrics.py). A replay of yesterday's messages would land every one in today's window, and the stream and batch views would disagree for a reason unrelated to the data. |
| **Batch and stream reconciled in one table** | [`lake/silver_spec.py`](../src/meridian/lake/silver_spec.py) — `also_from`. 156,877 file rows + 414 stream rows = 157,291 into Silver, and still exactly **153,136** rows out, because all 295 distinct streamed events were second captures and the natural-key dedup collapsed them. Publishing two tables and hoping nobody joins them is the alternative. |

---

## 9. The AI layer

| Concept | Where, and what it buys |
| --- | --- |
| **Hybrid retrieval, RRF-fused** | [`rag/retrieve.py`](../src/meridian/rag/retrieve.py). BM25 and cosine similarity in one SQL statement. |
| **BM25 implemented in SQL** | [`rag/ddl.sql`](../src/meridian/rag/ddl.sql) — a materialised `(chunk_id, lexeme, tf)` inverted index. Postgres's `ts_rank_cd` has **no inverse document frequency**, so on a corpus where 70% of tickets say "order" it cannot distinguish that from an order number appearing once. |
| **Retrieval evaluation with a chance baseline** | [`eval/golden_questions.yml`](../eval/golden_questions.yml), [`rag/evaluate.py`](../src/meridian/rag/evaluate.py). Relevance is defined from generator ground truth, never from retrieval output — the obvious construction makes recall 1.0 by definition. |
| **Abstention on similarity, not on rank** | `1/(k+1)` is the same number whether the top hit is a paraphrase of the question or an unrelated ticket, because rank carries no notion of closeness. |
| **Graceful degradation without a vendor** | [`rag/generate.py`](../src/meridian/rag/generate.py). No API key means an extractive answer from the retrieved passages, not "AI unavailable". |
| **Content hashing over the *masked* text** | [`rag/index.py`](../src/meridian/rag/index.py). Hashing raw input means a later masking fix leaves already-indexed chunks unchanged, so they are skipped forever and the leaked text stays permanently. |
| **Scoring the model against labels it never saw** | [`rag/enrich.py`](../src/meridian/rag/enrich.py) reads `rag.chunks`, which does not contain the ground-truth columns to leak. `mart_support_health` divides by *enriched* tickets, so no key means accuracy is null — "we did not ask" rather than "the model was wrong". |
| **A structural dependency on the AI layer** | `mart_support_health` reads the enrichment columns, so the AI layer cannot quietly become a side attachment nothing consumes. |

---

## 10. Security and governance

| Concept | Where, and what it buys |
| --- | --- |
| **Least-privilege roles** | [`db/init/04_roles.sql`](../db/init/04_roles.sql) — six, each doing one job. Proven, not asserted: [`tests/test_pii_boundary.py`](../tests/test_pii_boundary.py) expects `InsufficientPrivilege` when `analytics_ro` selects a PII column. |
| **PII physically separated** | The `secure` schema, populated by [`load_warehouse.load_customer_pii`](../src/meridian/lake/load_warehouse.py). Separate so grants can exclude it, rather than relying on a column list that drifts. |
| **PII never entering the lake** | Same file. `secure.customer_pii` is loaded straight from seeds to Postgres and never written to object storage. |
| **Masking before embedding, dictionary-first** | [`rag/masking.py`](../src/meridian/rag/masking.py) against [`seeds/known_pii_terms.json`](../src/meridian/seed/identity.py). Regex is the fallback, and the split is measured per pass so a rising regex share says the manifest is going stale. |
| **A schema-wide invariant instead of a table list** | `rag` holds masked text *by construction*, which is what makes granting `analytics_ro` SELECT across it safe. [`tests/test_pii_manifest.py`](../tests/test_pii_manifest.py) keeps it true. |
| **The security boundary chosen by the framework** | [`api/deps.py`](../src/meridian/api/deps.py). A handler cannot pick its own role. A `/v1/ai/ask` request runs in a session that is *incapable* of reading an email address — a prompt injection would get `InsufficientPrivilege` from Postgres. |
| **Ownership and SLAs as data** | [`docs/governance/owners.yml`](governance/owners.yml), [`pii_classification.yml`](governance/pii_classification.yml). |

---

## 11. The API

| Concept | Where, and what it buys |
| --- | --- |
| **JWT done correctly** | [`api/security.py`](../src/meridian/api/security.py). An explicit `algorithms` list (the `alg:none` forgery is a test), `aud` and `iss` checked so one leaked secret does not compromise every service sharing it, unknown scopes rejected, and a missing secret refused rather than generated per process. |
| **Two credentials for two kinds of caller** | Same file. A machine that must refresh a token every fifteen minutes will eventually fail to; the static ingest key carries `tickets:write` and nothing else. |
| **No default credential** | [`api/routers/auth.py`](../src/meridian/api/routers/auth.py). With nothing configured, `/v1/auth/token` returns 503. A hardcoded password in a repository is worse than no authentication, because it looks like authentication. |
| **Constant-time comparison** | `hmac.compare_digest`, and a password verified even for an unknown user so a missing account is not measurably faster. |
| **Keyset pagination** | [`api/routers/tickets.py`](../src/meridian/api/routers/tickets.py) — `(created_ts, ticket_id) > (?, ?)`. Offset re-scans what it skips and silently repeats or drops rows while the table is written to. The tie-break is load-bearing: `created_ts` is not unique. |
| **Liveness vs. readiness** | [`api/main.py`](../src/meridian/api/main.py). `/health` makes no database call — a restart does not fix a database outage, so a database check in liveness is a restart loop. |
| **Failing soft at startup** | Same file. A missing vector store leaves `/v1/ai/*` at 503 and the ticket endpoints working. |
| **One 500 shape, never the exception text** | Same file. A stack trace in a response body leaks table names and parameter values; it is logged in full and the caller gets a run id. |

---

## 12. The dashboard

| Concept | Where, and what it buys |
| --- | --- |
| **Exactly one read path** | [`dashboard/metrics.py`](../dashboard/metrics.py); `app.py` contains no SQL, enforced by [`tests/test_dashboard.py`](../tests/test_dashboard.py). A dashboard that reaches past the marts has become a second, untested transform layer free to disagree with dbt. |
| **Cache invalidation on a data watermark** | [`dashboard/app.py`](../dashboard/app.py). Keyed on `meta.pipeline_run_log.completed_at`, so a completed run invalidates every panel at once and a failed one invalidates none. |
| **A stale-data banner, never a fallback constant** | Same file — `freshness_banner`, with three distinct states. |
| **Non-additive measures recomputed** | `metrics.recompute_aov`. Summing `nadd_aov` across six channels gives **£659.73** where the correct figure is **£141.14**, and the wrong one draws as a perfectly plausible line. |
| **Rendering smoke-tested in a real browser** | [`dashboard/screenshots.py`](../dashboard/screenshots.py). Streamlit writes an uncaught exception into the page rather than failing the process, so a broken panel serves HTTP 200 all day. |

---

## 13. Engineering practice

| Concept | Where, and what it buys |
| --- | --- |
| **Frozen exit codes** | [`runlog.py`](../src/meridian/runlog.py) — 0/1/2/3. `2` is a data quality BLOCK and `3` is an unreachable dependency, which is what lets Airflow retry the second and page for the first. |
| **Structured logs as an interface** | Same file. JSON lines on stdout rather than through `logging`, because they are consumed and a handler configured elsewhere must not be able to reformat or swallow them. |
| **Every step runnable without the orchestrator** | Module entrypoints throughout, per CONTRACTS §5. |
| **Testing what survives installation** | [`tests/test_packaging.py`](../tests/test_packaging.py). A data file can be missing from a wheel while every test passes and every local run works — see §14. |
| **Tests that skip with a reason** | [`tests/conftest.py`](../tests/conftest.py). DB-backed tests skip cleanly without Docker, so the suite is green in CI and meaningful locally. An empty vector store is a skip, not a pass. |
| **Asserting the test is not vacuous** | `test_there_are_data_files_to_worry_about`, `_skip_without_gold`, the `indexed` fixture. A test that passes against an empty result set is worse than no test. |
| **Optional extras** | [`pyproject.toml`](../pyproject.toml) — `rag`, `stream`, `api`, `dashboard`, `dev`. `pip install -e .` does not drag onnxruntime in for somebody who only wants to generate seed data. |
| **CI that runs the real suite** | [`.github/workflows/ci.yml`](../.github/workflows/ci.yml). Two jobs: one with no Docker that relies on the tests skipping themselves rather than on a hand-maintained subset, and one that stands the whole platform up. Both found real bugs on their first runs — see §14. |
| **Deriving a check instead of listing it** | [`tests/test_packaging.py`](../tests/test_packaging.py) parses every import out of `src/` and resolves each to its distribution, rather than asserting against a list. The list version is what missed three dependencies. |

---

## 14. Mistakes this project actually made

The rest of this document describes what was built. This section is what it got
wrong first, because every row is a class of bug that is invisible until it is
not, and the fix is only interesting alongside the symptom.

| What happened | Why it was invisible |
| --- | --- |
| **`\b` never matched a phone number.** The masking regex used `\b` before a `+`, which is not a word boundary. | The dictionary pass caught the numbers anyway, so the corpus looked clean. Found by attributing masks per pass rather than counting them in total. |
| **Redaction tokens poisoned BM25.** `[CUSTOMER_NAME]` analyses to the lexemes `custom` and `name`, in 394 of 1,311 chunks. | BM25 gave them ordinary IDF, so a question mentioning "customer" scored against redaction artefacts — matching precisely the documents whose text had been removed. |
| **Hybrid retrieval was silently vector-only.** `websearch_to_tsquery` uses AND semantics and matched 0 of 1,311 chunks. | Results were still returned, and still plausible. Found by checking the lexical half in isolation, which is the only way. |
| **A `.sql` file was missing from the wheel.** package-data listed `meridian.rag` and `meridian.warehouse` was added later. | From a source checkout the file is simply on disk, so every test and every `make` target passed. It surfaced when an Airflow task ran from the installed copy. |
| **A hard-deleted customer was deleted four months before signing up.** The generator forced the SCD2 demo customer's signup early and left the deleted one's to random. | Every point-in-time reconstruction correctly excluded them at every date, so the row never entered the snapshot and `hard_deletes='new_record'` — the feature that customer exists for — was never exercised. |
| **A Kafka batch committed one partition.** `commit(message=batch[-1])` commits that message's partition; a poll loop is fed from all three. | No data lost — the rest are re-delivered — but the group never advanced and lag would climb without bound while the consumer reported success. Found because the lag table recorded partition 0 as "never committed" after a run that had plainly consumed it. |
| **A metrics upsert replaced instead of adding.** | Offsets guarantee a second run sees only what the first did not, so its counter starts at zero and overwriting discards everything already counted. Visible only as `events_by_type` summing to 2,892 against `events` at 2,884. |
| **Seven of eight dashboard tabs were broken with every test green.** psycopg maps `numeric` to `decimal.Decimal`, which lands in pandas as dtype `object`. | `.sum()` works on it, so the tests passed; `.nlargest()` raises and Plotly renders an empty axis. Streamlit writes the exception into the page rather than failing, so it also served HTTP 200. |
| **A load-bearing claim about the rankers was false.** "Vector search finds a bare rare identifier at rank 1 and only loses it when diluted" was written against one hand-picked order. | Measured over twelve: BM25 ranks the right ticket first **12/12**; vector manages **2/12** bare and **4/12** diluted. Order references share a prefix and differ only in digits, so they embed to nearly the same point. |
| **Three dependencies were declared nowhere.** `pandera` was hand-installed into a local venv and again into the Airflow image; `numpy` and `pydantic` arrived transitively through fastembed and fastapi. | Every machine that mattered had all three, so nothing anywhere said they were required. The packaging test that exists to catch this asserted against a hardcoded list of five packages — and a hand-written list only ever checks the dependencies somebody remembered, which are by definition not the ones that go missing. Found by CI on its first run, on a clean runner. |
| **Eight tests failed instead of skipping when nothing was configured.** Every skip guard was written for "the server is down". | "Not configured" is a different failure arriving by a different path: `settings.dsn()` raises before any connection is attempted, so `server_reachable()` never gets to report it. Invisible on any developer machine, because they all have a `.env`. Reproducing it needed the file *moved aside* — unsetting the variables was not enough, since `settings()` auto-loads it from disk. |
| **RRF's agreement bias costs real hits.** Fusion keeps 10 of those 12. | A document one ranker puts first scores `1/61` = 0.0164; one both rank 27th and 36th scores `1/87 + 1/96` = 0.0219 and wins. `k`=60 exceeds the 50-document candidate pool, so the whole rank curve spans under a factor of two. Not retuned — §11 freezes `k` and gives the reason — but measured and asserted rather than hidden. |
| **The API package never read `.env`.** `settings()` performs the `.env` load, and nothing under `meridian.api` calls `settings()` — it reads `API_JWT_SECRET` and friends from `os.environ` at the point of use, which is deliberate, but left it the one package with no config bootstrap at all. | Invisible anywhere the variables were already exported: CI writes them into `$GITHUB_ENV`, and any shell that has run `set -a; source .env` has them too. `/health` needs no token, so the server started and answered readiness checks. Only an authenticated call failed — meaning `make api-demo` died on step one from a fresh clone, while every other target in the Makefile worked. The README's "17/17 endpoint steps" had only ever been true on a machine that was already configured. |
| **This document's own headline count was wrong.** Every summary in the repository said "21 dbt models". dbt says `Found 29 models, 1 snapshot, 87 data tests`. | The number was hand-counted once and then copied into the README, this file and `PROGRESS.md`, where it agreed with itself in three places and was never re-derived from anything. The `87` beside it was correct, which is what made the pair look verified. Found only because somebody asked what the project contained and the answer was recomputed instead of quoted — the exact failure mode this section is about, in the section about it. |

Rows eleven and twelve arrived after the first ten, from CI's first two runs —
which is the argument for CI stated more sharply than any of the rest. Both had
been true for weeks; neither could be seen from a machine that had already been
set up correctly.

The last two arrived later still, both while demonstrating the finished project
rather than building it — which is its own lesson, since demonstrating it is the
first time anything ran the way a stranger would run it.

The thirteenth is the least comfortable of them: this document was itself the
thing asserting an unverified number. Nothing catches that, because a document
is not executable. The honest conclusion is that prose about a system is the one
artefact no test covers, and it decays exactly like code — which is why every
count above now names the command that produced it.

The fourteenth is the sharper one. Every environment that had ever run the API
already had its secrets exported, so the missing `.env` load could not be seen
from any of them. It took a clean shell to surface, the same way the CI findings
did — and it is the third time in this table that "works on a machine already
set up correctly" turned out to mean "does not work."

The pattern is the same in every row: **the failure had no symptom.** Nothing
errored, nothing went red, and in most cases the output looked entirely
reasonable. That is the argument for the contracts, the measured baselines, the
tests that assert they are not vacuous, and the reproduction query on every
quality finding — none of which are there to catch the bugs that announce
themselves.
