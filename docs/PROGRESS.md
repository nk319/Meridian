# PROGRESS

Updated as the **last action of every work session**. This file plus the
`phase-N` git tags are the only things needed to resume — never conversation
history, which does not survive a container restart.

---

## Current state

| | |
| --- | --- |
| **Phase** | 8 — `docs/CONCEPTS.md` and the final pass |
| **Status** | ✅ **Complete. All eight phases are done.** |
| **Tag** | `phase-3` … `phase-8` — all need a human to push them, see below |
| **Next phase** | None. The build is finished. |
| **Next action** | Nothing is outstanding in the code, and CI is green on both jobs. The two things a human can do that this session cannot: **push the tags** (below), and set `ANTHROPIC_API_KEY` + run `make rag-enrich` to populate `gold.mart_support_health`'s AI columns — the join, the agreement flags and the null-safe denominators are all exercised; only the model call is missing. |

### What this session can and cannot push

All git traffic leaves through a policy-enforcing egress proxy — `GITHUB_TOKEN`
is literally `proxy-injected`, so the session holds no credential of its own and
the proxy authenticates for it. Because the proxy re-terminates TLS it can read
the `POST /git-receive-pack` body, where the ref updates travel in plaintext, and
it refuses anything that is not an update to `refs/heads/*`:

| operation | result |
| --- | --- |
| read refs, clone, fetch | ✅ |
| create a branch | ✅ |
| update a branch, **including a force-push** | ✅ |
| **delete any ref** | ❌ 403 |
| **create or move a tag** | ❌ 403 |

The refusal is synthesised by the proxy, not GitHub: successful responses carry
an `X-Github-Request-Id` header and the 403s do not. So it is not a repository
permission, and retrying or routing around it is explicitly out of bounds
(`/root/.ccr/README.md`).

Practical consequences for a future session:

- **Never create a branch you intend to delete.** You will not be able to remove
  it, and there is no ref-deletion call in the GitHub tool surface either. This
  was learned by leaving a stray `tmp-proxy-probe` branch behind that a human
  had to clean up.
- **Tags must be pushed by a human**, from a clone whose git does not go through
  this proxy. `phase-0`, `phase-1` and `phase-2` are all on the remote and
  annotated; a future phase's tag will need the same treatment:

  ```bash
  git fetch origin
  git push origin --tags
  ```

  All the tags already exist locally in this session's clone but cannot leave it.
  To recreate them from scratch in your own clone:

  ```bash
  git tag -a phase-3 bed3a3f -m "Phase 3: data quality suites and orchestration"
  git tag -a phase-4 4d849e6 -m "Phase 4: dbt Gold star schema"
  git tag -a phase-5 1c0fce3 -m "Phase 5: Redpanda streaming"
  git tag -a phase-6 92c3429 -m "Phase 6: FastAPI service"
  git tag -a phase-7 7148694 -m "Phase 7: Streamlit dashboard"
  git tag -a phase-8 558c0d8 -m "Phase 8: docs and final verification"
  git push origin --tags
  ```

The branch `claude/phase-1-setup-8lokjp` carries every phase. (The name is Phase
1's; the session was told to develop there and not to push elsewhere without
permission.)

---

## Phase 0 — complete

Froze every cross-component interface in `docs/CONTRACTS.md` and built a
deterministic generator for five source systems: 3,000 customers, 14,615 orders,
26,054 order items, 16,178 payments, 156,037 web events, 1,311 support tickets,
4,136 injected defects confined to the third-party feeds. 15 acceptance tests.
Re-verified byte-identical in every session since.

## Phase 1 — complete

The AI/RAG core, built before the warehouse so the PII question had to be
answered before anything was embedded. Dictionary-first masking driven by the
generator's own manifest; hybrid retrieval with real BM25 scored in SQL, fused
with vector search by RRF in one statement; `claude-opus-5` with an extractive
fallback that needs no API key.

Measured on `eval/golden_questions.yml` (11 answerable, 5 that must be refused):
hybrid **recall@5 1.00 / precision@5 0.93 / MRR 1.000**, against a chance
baseline of 0.22 and vector-only at 0.91. Zero manifest terms in the vector
store.

---

## Phase 2 — complete

**Goal:** the five seed sources land as Bronze Parquet on MinIO, and the
DuckDB → Arrow → `COPY` hop reaches `warehouse.silver`.

### Done first, before anything else

CONTRACTS §3 calls the Silver→warehouse hop load-bearing, so it was spiked
before a line of pipeline code existed: Parquet written to MinIO, read back
through DuckDB's `httpfs`, streamed as Arrow into a binary `COPY`. 14,615 rows
in 0.1s, types preserved end to end. The spike also verified the claim §3 makes
about binary COPY failing loudly on a type mismatch — it does — rather than
building on top of an assumption.

### Delivered

| Artifact | What it does |
| --- | --- |
| `src/meridian/warehouse/ddl.sql` | `meta` (4 tables from §7), `silver` (8), `secure` (1) |
| `src/meridian/warehouse/bootstrap.py` | Applies it as `meridian_etl`, idempotently |
| `src/meridian/seed/load_oltp.py` | Populates the OLTP source database from `seeds/` |
| `src/meridian/lake/layout.py` | The frozen §2 paths and the six Bronze metadata columns |
| `src/meridian/lake/duck.py` | DuckDB wired to MinIO, timezone pinned, read-only OLTP attach |
| `src/meridian/lake/bronze.py` | Raw capture with lineage and a positional record hash |
| `src/meridian/ingest/base.py` | Watermarks, run log, and the one-entity-one-owner rule |
| `src/meridian/ingest/{oltp,files,restapi,vendor}.py` | The four batch sources, `--mode full\|incremental` |
| `src/meridian/lake/silver_spec.py` | What each Silver entity is allowed to contain |
| `src/meridian/lake/build_silver.py` | Dedup, type, validate, quarantine, DQ records |
| `src/meridian/lake/load_warehouse.py` | Silver → Postgres via Arrow + binary COPY |
| `tests/test_pii_boundary.py` | The governance control §10 names as the proof |
| `tests/test_lake_contracts.py`, `tests/test_lake_pipeline.py` | 88 further tests |

### Acceptance — met

All of it reproduced by `make verify` from destroyed volumes.

```
159 passed                     # stack up, no ANTHROPIC_API_KEY set
72 passed, 87 skipped          # no stack reachable (the CI shape)
ruff check + ruff format       # clean
```

The lake, after a full capture: **40 Bronze objects** (one per entity, plus the
vendor feed's 33 cursor-paginated pages), 8 Silver files, 7 quarantine files.

| Entity | Bronze | deduped | quarantined | Silver |
| --- | ---: | ---: | ---: | ---: |
| customers | 3,000 | 0 | 0 | 3,000 |
| orders | 14,615 | 0 | 0 | 14,615 |
| order_items | 26,054 | 0 | 0 | 26,054 |
| customer_change_log | 4 | 0 | 0 | 4 |
| products | 221 | 1 | 3 (1.36%) | 217 |
| web_events | 156,973 | 936 | 2,808 (1.80%) | 153,229 |
| support_tickets | 1,311 | 0 | 0 | 1,311 |
| payments | 16,275 | 86 | 291 (1.80%) | 15,887 |

`secure.customer_pii` holds 3,000 rows, loaded direct from the seed and never
through the lake. 217,317 rows reach the warehouse in total; the 153,229-row
web_events load streams in 4.2s.

Quarantine rates land at ~1.8% on exactly the three feeds the generator injects
defects into, and at zero on the four OLTP-sourced entities and the ticket feed —
which is the property Phase 0 built the generator to have, now measured rather
than assumed. The reasons map one-to-one onto the injected defect types:
`bad_type` ← malformed dates, `missing` ← blanked required fields, `bad_enum` ←
invalid enums, `out_of_range` ← negative amounts, and duplicate rows collapse on
`_record_hash`.

Retrieval is unchanged after switching the indexer to read `silver`: hybrid
recall@5 **1.00**, precision@5 0.93, MRR 1.000, abstention 5/5.

### Decisions taken during the phase

| Decision | Reasoning |
| --- | --- |
| **Bronze is raw**: text feeds land as VARCHAR, OLTP lands typed | The file, API and vendor feeds carry injected defects. Typing on the way in rejects those rows at the door, and a refused row is indistinguishable from one that was never sent. From a relational source, though, a typed row genuinely *is* what was sent. |
| Record hash coalesces NULLs to a sentinel before joining | `concat_ws` drops NULLs, so `('a', NULL, 'b')` and `('a', 'b', NULL)` would hash identically and two different records would dedup into one. |
| DuckDB session timezone pinned to UTC | The hash casts every column to VARCHAR, and a `timestamptz` renders in the session zone — the same row would hash differently on a machine set to Europe/Berlin. Same class of bug as the `hash()` randomisation Phase 0 found, and it fails the same way: silently, across environments. |
| Silver is a **full rebuild** into one `part-000.parquet` per entity | Bronze is the append-only record, so rebuilding is the definition of idempotent. Naming Silver files by run id would leave every previous rebuild under the same glob and the loader would read every generation at once. |
| Silver carries four of the six Bronze columns | `_source_file` and `_batch_seq` describe a physical file; after dedup a Silver row corresponds to none, so carrying them would present an arbitrary winner's filename as provenance. |
| No cross-table foreign keys in Silver | A quarantined customer would make its orders unloadable, coupling every entity's load to every other entity's quarantine decisions. dbt tests assert referential integrity, where a violation is reported rather than fatal. |
| Vocabulary CHECK constraints **kept** in Silver | Those are per-row, and they turn "bad rows were quarantined" from a claim about the quarantine code into something the database enforces. |
| `quarantine_rate` gate at severity BLOCK, 10% | A feed that is mostly rejected is not a quality finding, it is an outage. Exiting 2 beats publishing a table missing most of its rows, which downstream reads as a business collapse. |
| PII loaded **direct from the seed**, bypassing Bronze and Silver | §10's enforcement mechanism. No stage holds restricted data alongside business data, so no step can forget to drop it. `assert_no_restricted_columns` is the other half — it fails the build if such a column ever appears in Bronze. |
| `order_items` Bronze carries the parent's `order_ts` | The source table has no timestamp, so without it the largest child table could only ever be full-refreshed. Dropped again in Silver: capture machinery, not a business column. |
| `rag.index` now defaults to `--source silver` | That is the pipeline path now. `--source seeds` is kept and still tested, because Phase 1's promise was that the AI layer runs without the warehouse. |
| `load_oltp` uses DELETE, not TRUNCATE | Not an inefficiency to route around: §1 grants `meridian_app` CRUD, and TRUNCATE is a separate privilege. Widening the grant to make a fixture faster would hand the application role the ability to empty the source system — exactly the authority the narrow grant withholds. |

### Bugs found and fixed

| Bug | How it was found, and why it mattered |
| --- | --- |
| **Incremental re-captured every malformed row, on every run, forever.** | Found by re-running incremental after a full pass and seeing 905 web events and 74 payments where 0 were expected. `TRY_CAST` of a corrupt date is NULL, so `ts > watermark` silently drops those rows — but the obvious fix, `OR ts IS NULL`, makes them permanently "new" because they have no timestamp to age out. They are now admitted exactly once, by anti-joining the record hash against Bronze. Neither half of that is visible without checking the second run. |
| **`meridian_etl` could not GRANT on the tables it creates.** | `warehouse/ddl.sql` has to issue the narrow `silver.support_tickets` grant §1 specifies, and only a schema's owner can grant on its tables. `db/init/04` now gives it ownership of `silver`, `secure` and `meta`, mirroring `dbt_runner` owning `gold*`. |
| **DuckDB needs `pytz` to hand a `TIMESTAMPTZ` back to Python** — and does not require it at install time. | Surfaced as an ImportError from inside the query engine, several layers from anything mentioning time zones. The watermark is now fetched as text and parsed, which costs nothing and removes the dependency. |
| **`meridian_app` cannot TRUNCATE.** | The loader assumed CRUD included it. It does not — and the grant was right, so the loader changed rather than the grant. |
| **`\\connect` in a DDL file executed through psycopg.** | psql meta-commands are not SQL. Caught immediately; noted because the same file is read by both tools in other projects and the failure is not obvious. |

### Loose ends closed from Phase 1

- `rag_indexer` now holds `GRANT SELECT ON silver.support_tickets`, and
  `meridian.rag.index --source silver` reads it. Verified to produce a
  byte-identical index to the `seeds` path: same 1,311 chunks, same 897 PII
  values masked, same 19,767 lexical terms.
- `tests/test_pii_boundary.py` exists, which is what CONTRACTS §10 called the
  governance requirement's only actual proof.

---

## Phase 3 — complete

**Goal:** turn ad-hoc quarantine checks into a declared suite framework, and put
Airflow in front of the module entrypoints.

### Delivered

| Artifact | What it does |
| --- | --- |
| `src/meridian/dq/checks.py` | Declarative SQL assertions; every one carries a reproduction query |
| `src/meridian/dq/suites.py` | 31 checks: referential, business invariants, volume, freshness |
| `src/meridian/dq/schemas.py` | 7 Pandera frames asserting **distribution**, which per-row rules cannot see |
| `src/meridian/dq/run.py` | `python -m meridian.dq.run --suite <name>`, exit 2 on BLOCK |
| `airflow/Dockerfile` | Airflow 3.1.3 with three isolated interpreters |
| `airflow/dags/meridian_batch.py` | bootstrap → 5 parallel ingests → silver → load → DQ |
| `airflow/dags/meridian_rag.py` | asset-scheduled index + eval |
| `tests/test_dq.py`, `tests/test_packaging.py` | 27 further tests |

### Acceptance — met

```
186 passed                     # stack up, no ANTHROPIC_API_KEY
99 passed, 87 skipped          # no stack (the CI shape)
ruff check + ruff format       # clean
```

38 checks, 0 failing. Both DAGs run green in a real scheduler:

| DAG | tasks | outcome |
| --- | --- | --- |
| `meridian_batch` | 9 | all success, ~40s |
| `meridian_rag` | 2 | `asset_triggered` by the batch DAG's outlet; index 177s, eval 9s |

### Decisions taken

| Decision | Reasoning |
| --- | --- |
| Pandera asserts **distribution**, not schema | The database already enforces every vocabulary via CHECK constraints, and `build_silver` already types every row. Repeating either would be theatre. What neither can see is that 72% of orders are usually delivered or that margin runs at 43% — properties of the set, and exactly what breaks when an upstream system changes quietly. Every bound was measured against the loaded warehouse, then widened ~20%. |
| Freshness asserts the **pipeline**, not the data | The generator writes to a fixed anchor, so `max(order_ts)` ages every day and a check on it would go red for no reason. A dashboard that is red by construction is one people stop reading. |
| `fk_order_items_product_id` is WARN at 3%, not BLOCK | Quarantine fans out: 3 rejected products (1.36% of the catalogue) orphan 380 order lines (1.46%). The lines are valid and will vanish from any product join. Stopping the pipeline would be wrong; silence would be worse. |
| Three interpreters in the Airflow image | §6 named one irreconcilable pin (dbt's protobuf). There are two — fastembed pulls onnxruntime with its own floor. Verified in the built image: 4.25.8 in Airflow, 6.33.6 in dbt. |
| Every DAG task is a subprocess | No DAG imports `meridian`. That is what keeps `make pipeline` honest as a proof the platform runs without the orchestrator, and stops a pipeline dependency change taking down the scheduler. |
| `meridian_rag` scheduled on an asset | A cron guess either fires before the batch finishes or wastes an hour. Re-indexing is cheap by design, so a spurious trigger costs seconds. |
| A sixth role, `airflow` | §1 called the `airflow` database "metadata only" but named no role, so nothing could connect to it. It owns that database and holds nothing elsewhere. |

### Bugs found and fixed

| Bug | How it surfaced |
| --- | --- |
| **`meridian/warehouse/ddl.sql` was missing from the wheel.** Phase 2 declared package-data for `meridian.rag`, then added a second `.sql` under `meridian.warehouse` without declaring it. | Invisible everywhere — from a source checkout the file is simply on disk, so every test and every `make` target passed. It surfaced the first time a task ran from an *installed* copy inside the Airflow image: `FileNotFoundError`. Fixed with a wildcard, and `tests/test_packaging.py` now fails if the config is narrowed again. |
| **Airflow 3 tasks queued forever, then failed with `httpx.ConnectError`.** | Airflow 3 runs tasks through a Task Execution API instead of letting them touch the metadata database. The worker resolves it from `AIRFLOW__CORE__EXECUTION_API_SERVER_URL`, which defaults to `localhost:8080` — right for all-in-one, wrong for every split deployment. The symptom reads as a scheduler fault rather than a URL. |
| **`AIRFLOW_DB_PASSWORD` never reached Postgres.** | Added to `.env` but not to the postgres service's `environment:`. The `\getenv` guard in `04_roles.sql` caught it and aborted init rather than creating a blank-password role — the guard working exactly as designed. |
| **The image build could not reach apt, then could not verify TLS.** | Container builds do not go through the agent proxy. The apt layer turned out to be unnecessary (every dependency ships manylinux wheels) and was deleted; the proxy CA is staged into `airflow/certs/` by `make airflow-build` and skipped on networks that do not need it. |

---

## Phase 4 — complete

**Goal:** `gold_stg` → `gold_int` → `gold`, read by nothing but the dashboard.

### Delivered

| Artifact | What it does |
| --- | --- |
| `dbt/models/staging/` | 9 views, one per source relation. The only place that knows physical source names |
| `dbt/models/intermediate/` | 5 models: order/payment/line rollups, the session funnel, the point-in-time customer rebuild |
| `dbt/models/marts/` | 3 dims, 5 facts, 7 marts — the fifteen relations §8 names |
| `dbt/snapshots/customers_snapshot.sql` | SCD2, `check` on `loyalty_tier`+`segment`, `hard_deletes='new_record'` |
| `dbt/tests/` | 6 singular tests: four on SCD2, one reconciling the two order grains, one on fan-out |
| `dbt/macros/` | `generate_schema_name`, `surrogate_key`, `snapshot_get_time`, `unique_combination_of_columns` |
| `scripts/dbt_snapshot_backfill.sh` | One-time replay of the change log into the snapshot. Guarded, and NOT an Airflow task |
| `src/meridian/dbt/results.py` | `run_results.json` → `meta.dq_check_results` with `source='dbt'` |
| `src/meridian/rag/enrich.py` | The last §5 entrypoint. Classifies from masked text; no-ops without an API key |
| `src/meridian/seed/anchors.py` | Publishes the values the golden eval set needs to name |
| `tests/test_gold.py`, `tests/test_dbt_results.py` | 30 further tests |

### Acceptance — met

```
87 dbt tests                   # PASS=87 WARN=0 ERROR=0 SKIP=0
218 pytest                     # stack up, no ANTHROPIC_API_KEY
38 DQ checks, 0 failing
ruff check + ruff format       # clean
recall@5: hybrid 1.00 / lexical 1.00 / vector 0.91, abstention 1.00
```

`dim_customer` holds 3,003 rows for 3,000 customers: `C000042` has exactly three
versions with event-time validity windows, and `C000117` has a live version
closed at its deletion plus a tombstone. Both order grains reconcile to the
cent. No fact fans out.

### Decisions taken

| Decision | Reasoning |
| --- | --- |
| The snapshot is **backfilled from the change log**, once | A snapshot records what it sees when it runs, and `silver.customers` holds current state — so a first run gives one version per customer and §8's three SCD2 tests all pass against a dimension with no history. `snapshot_get_time()` is overridden so a replayed checkpoint stamps `dbt_valid_from` with the date the tier actually changed, not the date somebody ran the backfill. |
| The backfill is **guarded, not idempotent** | Replaying an old checkpoint after the snapshot reaches the present sees the customer's *old* tier, treats it as a fresh change, and appends a version recording time running backwards. A snapshot cannot see its own history, so the check cannot live inside it. |
| `dim_customer` version 1 opens at `-infinity` | SCD2 history starts at the first change log entry (2025-02-07); orders start 2024-08-20. A straight `BETWEEN` join would drop six months of revenue with no error — an inner join to a dimension is exactly as quiet as a filter. |
| `valid_to` is `infinity`, not null | A null is more honest and costs a `coalesce` in every as-of join, which is one somebody eventually forgets. |
| `date_sk` is `yyyymmdd`, the only non-hashed key | A date's natural key is already unique, immutable, dense and orderable. Hashing throws all four away to buy consistency with dimensions that have none of them. |
| The enrichment lands in `rag`, not `silver` | §8 makes the mart depend on it and §1 says dbt reads silver — both cannot hold. The alternative makes `rag_indexer` a writer of `silver` and puts a model's output in a layer defined as cleaned source data. |
| `dbt_results` runs on `all_done` | Its whole job is recording what dbt found, and a failing `dbt test` is exactly when that matters. The `GOLD_READY` asset hangs off `dbt_test` instead, so a red suite does not wake consumers. |
| `unique_combination_of_columns` written locally | dbt_utils has it, and taking the dependency means a network fetch before the project compiles. Fourteen lines keeps the whole project buildable offline. |
| `error` and `skipped` map to **FAIL** | A test that could not run has not passed. Recording it PASS is how a test broken by a compilation error stays broken for a quarter behind a green dashboard. |

### Bugs found and fixed

| Bug | How it surfaced |
| --- | --- |
| **The hard-deleted customer was deleted four months before they signed up.** `enrich_customers` forced the SCD2 demo customer's signup to precede its first transition but left the deleted customer's to the random skew. | Every point-in-time reconstruction correctly excluded `C000117` at every date, so the row never entered the snapshot and `hard_deletes='new_record'` — the whole reason the customer exists — was never exercised. The seed was wrong in a way that made the feature it demonstrates silently untested. |
| **`dbt snapshot` could not create temporary tables.** | `db/init` revokes ALL on the database from PUBLIC, which takes away the default TEMP grant. A role owning three schemas still cannot run a snapshot. Caught on the second backfill checkpoint, because the first run of a snapshot does not stage. |
| **Every singular dbt test was recorded under the name `meridian`.** | `unique_id` is `test.<project>.<name>.<hash>` for a generic test and `test.<project>.<name>` for a singular one. A fixed offset from the end gets one shape right; the six most interesting assertions in the project all shared one name in `meta.dq_check_results`. |
| **The golden eval set named an order reference that no longer existed.** | The seed fix above shifted the order sequence, and three retrieval tests failed for a reason unrelated to retrieval. `build_relevance` refused to score the question rather than silently returning 0 — the Phase 1 guard working — but the obvious repair was to paste in another literal and restart the same clock. The generator now publishes its anchors. |
| **A load-bearing claim about the rankers was false.** The test asserting "vector search finds a bare rare identifier at rank 1 and only loses it when the query dilutes it" was written against one hand-picked order. | Measured over twelve: BM25 ranks the right ticket first 12/12 in both phrasings; vector manages **2/12 bare** and **4/12 diluted**. Order references share a prefix and differ only in digits, so they embed to nearly the same point — dilution makes it worse but is not the mechanism. |

### Measured, and left alone

**Fusion keeps 10 of those 12, not all of them, and that is RRF working as
designed.** A document one ranker puts first and the other misses scores
`1/(k+1)` = 0.0164; a document *both* rank badly — 27th and 36th — scores
`1/87 + 1/96` = 0.0219 and wins. What sharpens it here is that `k` (60) is
**larger than `RAG_CANDIDATE_POOL` (50)**: the entire rank curve spans 1/61 to
1/111, under a factor of two, so appearing on both lists outweighs rank
position almost everywhere.

Not retuned. §11 freezes `k=60` and gives the reason — RRF's appeal is needing
no per-corpus calibration, and a `k` fitted to this seed is exactly that
calibration. `tests/test_rag_retrieval.py` asserts the cost (fusion must keep
≥ 3/4) and asserts the benefit (hybrid must beat vector alone) instead of
hiding either. **If Phase 7 or a later phase wants identifier lookup to be
exact, the fix is a direct `ticket_id`/`order_id` lookup path, not a tuned `k`.**

### Loose ends closed from earlier phases

- `ticket_status` and `ticket_channel` promoted into CONTRACTS §9, as the
  deviations table said Phase 4 would.
- `python -m meridian.rag.enrich` built — the last §5 command that did not exist.
- dbt telemetry disabled (`send_anonymous_usage_stats: false`), so the project
  compiles on a machine with no outbound network.

### Still open

- `mart_support_health` reports `nadd_ai_coverage_pct` of 0.00 because no
  `ANTHROPIC_API_KEY` is configured here. The join, the agreement flags and the
  null-safe accuracy denominators are all exercised; only the model call is not.
  Setting a key and running `make rag-enrich` populates it with no code change.

---

## Phase 5 — complete

**Goal:** Redpanda on the Kafka wire protocol, with the topics, producer, two
independent consumer groups and DLQ CONTRACTS.md §4 specifies.

### Delivered

| Artifact | What it does |
| --- | --- |
| `contracts/schemas/*.avsc` | Three Avro schemas the manifest already pointed at but which did not exist |
| `src/meridian/stream/topics.py` | The one manifest loader. Nothing else may name a topic |
| `src/meridian/stream/client.py` | Client config and Avro single-object framing, in one place |
| `src/meridian/stream/admin.py` | `--create` (the init script) and `--describe` (drift against the manifest) |
| `src/meridian/stream/produce.py` | Replays seed rows onto the topics; `--defects` feeds the DLQ |
| `src/meridian/stream/consume.py` | The shared loop: commit-after-work, per-partition commits, dead lettering |
| `src/meridian/stream/sink_bronze.py` | `bronze-sink` — Parquet into `bronze/stream/`, committing after the write |
| `src/meridian/stream/metrics.py` | `realtime-metrics` — event-time windows into `meta.stream_metrics` |
| `src/meridian/stream/lag.py` | Appends to `meta.kafka_consumer_offsets`; `--max-lag` exits 2 |
| `src/meridian/stream/dlq.py` | Reads the DLQ without joining a group |
| `airflow/dags/meridian_stream_ops.py` | Every 15 min: ensure topics, check drift, record lag |
| `tests/test_stream.py` | 16 tests; the broker-backed ones skip without Redpanda |

### Acceptance — met

```
226 pytest                     # stack up, broker up, no ANTHROPIC_API_KEY
ruff check + ruff format       # clean
lag drains to 0 on all 3 partitions, for both groups
```

`make stream-demo` produces 3,000 events with 2% corruption, runs both groups
to completion, and shows lag before and after alongside the dead letters. All
four failure kinds reach the DLQ: not-Avro, truncated, valid-under-another-
schema, and the seed's own injected defects (month 13, empty enums).

### The batch/stream reconciliation, measured

| | rows |
| --- | ---: |
| `bronze/files/web_events` | 156,877 |
| `bronze/stream/web_events` | 414 |
| Bronze read by `build_silver` | 157,291 |
| **`silver.web_events`** | **153,136** |

All 295 distinct streamed `event_id`s were second captures of events the file
drop already had, and the natural-key dedup collapsed them — the Silver row
count is unchanged by the streaming path existing. That is the Lambda shape
reconciled in one place, rather than two tables that disagree.

### Decisions taken

| Decision | Reasoning |
| --- | --- |
| The init script is Python, not shell | A shell script running `rpk topic create` is a second place the partition counts live, which is exactly what §4's manifest exists to prevent. A test greps the package for a hardcoded topic name. |
| Avro **single-object encoding**, no registry | There is no Schema Registry here, so the topic is the schema binding. The `C3 01` marker plus a CRC-64-AVRO fingerprint is what lets a consumer *detect* a schema it was not expecting — Avro is positional, so a wrong schema does not error, it produces plausible nonsense. |
| Enums in the schema, not strings | `add_to_kart` is accepted by a string field and surfaces days later as a funnel step that never fires. An enum rejects it at the producer, where the error names the field. |
| `decimal` for amounts, not `double` | A float amount is a rounding error waiting for a SUM. Same DECIMAL(12,2) the warehouse stores. |
| `enable.auto.commit = False`, in `client.py` | It defaults to *true*, and a consumer that auto-commits has acknowledged messages it may still drop. Set once in the shared config so the next consumer cannot omit it. |
| A poison message is dead-lettered, not raised | Raising restarts the consumer, which re-reads the same message from the uncommitted offset and raises again — forever, with the partition frozen. The DLQ envelope carries topic/partition/offset because a dead letter you cannot trace back is a log line. |
| The DLQ reader joins no group | Reading the queue must not consume it, and a diagnostic tool that joined a group would appear in the lag table it exists to help interpret. |
| Streaming is a Bronze **source**, not a parallel path | `layout.py` always listed `stream` among the five source systems. One Silver builder, one dedup, one DQ suite. |
| `meta.stream_metrics` is not in `gold` | These are what the stream believed from an at-least-once feed; the marts are what batch concluded after dedup. Keeping them apart is what lets a dashboard show the tradeoff instead of asserting there isn't one. |
| Lag is recorded on a **schedule** | A single reading cannot tell a dead consumer from a slow one — one climbs without bound, the other plateaus. Only a series distinguishes them, which is why `observed_at` is in the primary key. |

### Bugs found and fixed

| Bug | How it surfaced |
| --- | --- |
| **A batch committed only one partition.** `commit(message=batch[-1])` commits that message's partition; a poll loop is fed from all three. | No data was lost — the uncommitted messages are simply re-delivered — but the group never advanced on the other partitions, so lag would climb without bound while the consumer reported success. Found because `meta.kafka_consumer_offsets` recorded partition 0 as "never committed" after a run that had plainly consumed it. Now commits `max(offset) + 1` per partition in the batch. |
| **The metrics upsert replaced instead of adding.** | Offsets guarantee a second run sees only what the first did not, so its in-memory counter starts at zero — and `value = EXCLUDED.value` discards everything already counted. Visible as `events_by_type` summing to 2,892 against `events` at 2,884 over the same messages, the difference being (window, dimension) pairs the first run wrote and the second never touched. Now persists deltas against an additive upsert. |
| **The producer crashed on the seed's own defects.** | `seed/defects.py` injects unparseable timestamps and empty enums, and the reader converted as it read. Those rows are not a problem to route around — they are the most realistic messages this producer can send, so they now go on the wire as raw JSON and the consumers dead-letter them. |
| **`fastavro`'s logical types are asymmetric.** `timestamp-millis` accepts an int and returns a `datetime`. | The DLQ reader divided a `datetime` by 1000. Lossless and correct on both sides; the trap is assuming the type you wrote is the type you read. |

### Known and stated, not fixed

**At-least-once means `meta.stream_metrics` can double-count.** A crash between
writing a window's delta and committing the offset re-delivers those messages.
The Bronze sink has the identical exposure and Silver's record hash removes it
downstream; a counter has no such recourse. Exactly-once would need the offset
committed in the same Postgres transaction as the metric — a transactional
outbox keyed on the offset, which is a real design and a much larger one than a
demonstration of consumer-group semantics warrants.

---

## Phase 6 — complete

**Goal:** the platform's own REST API — the source system `meridian.ingest.restapi`
captures from, and `/v1/ai/*` over the Phase 1 RAG layer.

### Delivered

| Artifact | What it does |
| --- | --- |
| `src/meridian/api/security.py` | JWT issue/verify, scopes, scrypt passwords, constant-time key comparison |
| `src/meridian/api/deps.py` | The role a handler connects as, and who may call what |
| `src/meridian/api/models.py` | Request/response shapes; §9 vocabularies imported, not restated |
| `src/meridian/api/routers/auth.py` | `POST /v1/auth/token`, with scope narrowing |
| `src/meridian/api/routers/tickets.py` | List (keyset-paginated), get, create, patch |
| `src/meridian/api/routers/ai.py` | `GET /v1/ai/search`, `POST /v1/ai/ask` |
| `src/meridian/api/main.py` | The app, a soft-failing lifespan, `/health` and `/ready` |
| `src/meridian/api/token.py` | `eval $(make -s api-token)` |
| `src/meridian/api/demo.py` | Walks every endpoint over the network |
| `tests/test_api.py` | 24 tests; the auth half needs no database |

### Acceptance — met

```
250 pytest                     # stack up, broker up, no ANTHROPIC_API_KEY
ruff check + ruff format       # clean
make api-demo                  # 17 steps, 0 failures, over the network
```

End to end through the API: a full `restapi` capture read **1,313** tickets over
HTTP into Bronze, and an incremental one picked up exactly the **6** created
since the watermark.

### Decisions taken

| Decision | Reasoning |
| --- | --- |
| The connecting role is chosen by the **dependency**, not the handler | `meridian_app` for tickets, `analytics_ro` for `/v1/ai/*`. A handler that could pick its own would make §1 a convention rather than a property of the process. |
| Two credentials, not one | A machine that must refresh a token every fifteen minutes is a machine that will eventually fail to. The ingest key is static and carries `tickets:write` only — it is the credential most likely to end up in a config file. |
| Three scopes | Asking the model a question costs money and reading a ticket does not, and a single "authenticated" flag cannot express that. Three rather than fifteen, because a model nobody can hold in their head gets bypassed with a wildcard. |
| No default credential, anywhere | With no `API_DEMO_PASSWORD`, `/v1/auth/token` returns 503. A hardcoded password in a repository is worse than no authentication, because it looks like authentication. |
| Keyset pagination | Offset re-scans what it skips and silently repeats or drops rows while the table is being written to — the normal state for a ticket feed, not a corner case. The `ticket_id` tie-break is load-bearing: `created_ts` is not unique. |
| `since` uses `>=`, not `>` | A watermark equal to the maximum `created_ts` seen would, with `>`, skip every other ticket sharing that second. The duplicate `>=` admits is free — Bronze is append-only and Silver dedups on the record hash. |
| Callers cannot set `intent` or `sentiment` | They are the ground truth `mart_support_health` scores predictions against. A caller that could write them could write its own answer key. |
| `/health` and `/ready` are different endpoints | Liveness answers "should this be restarted". A restart does not index a corpus, so a vector-store check in liveness is a restart loop — the most common way a deployment cycles healthy pods. |
| Startup fails soft | A missing vector store leaves `/v1/ai/*` at 503 and the ticket endpoints working. Refusing to boot would let an unindexed corpus take down the source-system API, and those two have nothing to do with each other. |
| One 500 shape, never the exception text | A stack trace in a response body leaks table names, file paths and sometimes parameter values. It is logged in full and the caller gets a run id to quote. |
| The API reuses the Airflow image | That image already carries the pipeline with the `rag` and `stream` extras. A second 4 GB image differing only in its entrypoint would double the build for nothing — and they are genuinely the same codebase. |

### Bugs found and fixed

| Bug | How it surfaced |
| --- | --- |
| **The ticket handlers connected to the wrong database.** `connect()` defaults to `warehouse` and the first `oltp_connection` omitted the name. | Every ticket request returned `permission denied for database "warehouse"` — the grant boundary refusing correctly. Worth recording because the error names a database the endpoint has no business touching, which reads as a configuration problem rather than the correct refusal it is. |
| **`API_JWT_SECRET` was 24 bytes.** | PyJWT warns below 32 for HS256, and it is right: RFC 7518 §3.2 sets the minimum HMAC key length at the hash output size. Regenerated, and `.env.example` now says so. |
| **`order_id` was modelled as nullable.** | `db/init/05_oltp_ddl.sql` makes it `NOT NULL REFERENCES orders`, so the first create returned a `NotNullViolation` 500. A nullable field modelled a ticket the database cannot store. Now required, and a foreign-key violation is a 422 rather than an unhandled 500. |
| **The NDJSON temp file leaked.** `NamedTemporaryFile(delete=False)` with no cleanup. | One file per ingest run left in `/tmp` — on a scheduled capture, a slow disk-space leak nothing attributes to this module. Replaced with a `TemporaryDirectory` the generator holds open across its `yield`. |

### Loose ends closed from earlier phases

- `meridian.ingest.restapi` reads the live service, which Phase 2 deferred to here.
  Both paths end in the same `read_json`, so the transport is the only difference.
- Ruff now knows FastAPI's `Query`/`Depends`/`Header` are not mutable defaults
  (`extend-immutable-calls`), narrowly rather than by disabling B008.

---

## Phase 7 — complete

**Goal:** a dashboard that reads only `gold`, caches on the pipeline watermark,
and says so when the data is stale.

### Delivered

| Artifact | What it does |
| --- | --- |
| `dashboard/metrics.py` | Every query the dashboard makes. The only file with SQL in it |
| `dashboard/app.py` | Eight tabs; no SQL, enforced by test |
| `dashboard/screenshots.py` | Clicks every tab, fails on a rendered exception, writes `docs/images/` |
| `.streamlit/config.toml` | Local-development server settings, each with its reason |
| `tests/test_dashboard.py` | 13 tests: the read boundary, additivity, freshness |

### Acceptance — met

```
271 pytest                     # everything up, no ANTHROPIC_API_KEY
ruff check + ruff format       # clean, now including dashboard/
8/8 tabs render, 0 exceptions  # make dashboard-shots
```

Revenue £1,859,639 over 13,176 revenue-recognised orders, AOV £141.14,
margin £812,318 — and the funnel is monotonic at 49,420 → 34,800 → 24,239 →
17,685 → 14,362 sessions.

### Decisions taken

| Decision | Reasoning |
| --- | --- |
| One read path, enforced by a source test | A dashboard that reaches past the marts is a second transform layer — in Python, untested, free to disagree with dbt about what revenue means. When it does, nobody can tell which number is wrong. |
| The cache key is the watermark, not a TTL | A completed run invalidates all eight tabs at once; a failed one invalidates none, because `completed_at` stays null while a run is in flight. A TTL serves pre-load numbers for its duration and cannot say it is doing so. |
| Missing and stale are different banners | "The pipeline has never completed" and "the data is six hours old" need different responses. Collapsing them renders the first as the second. |
| `recompute_aov` / `recompute_rate` live in `metrics.py` | "Remember to divide rather than sum" is not a rule that survives the next panel. Recomputing once, centrally, is what makes the `nadd_` prefix mean something. |
| Date bounds come from the data | The generator writes to a fixed anchor, so a "last 30 days" default opens on an empty chart the moment the demo is a month old. |
| A fresh connection per query | Streamlit reruns the script on every interaction, on a thread that changes. A connection cached across reruns is a race that surfaces as `InFailedSqlTransaction` on an unrelated panel; the marts are pre-aggregated, so the connection is not the cost. |
| The screenshot run is a **test** | Streamlit renders an uncaught exception into the page rather than failing the process, so a broken panel serves HTTP 200 all day and no unit test can see it. Clicking every tab and failing on `stException` is the only automated way to find out. |
| The dashboard container gets `ANALYTICS_RO_PASSWORD` and no other | §1's boundary, enforced by omission as well as by grant: it cannot use a role whose password it does not have. |

### Bugs found and fixed

| Bug | How it surfaced |
| --- | --- |
| **Seven of eight tabs were broken and every test was green.** psycopg maps Postgres `numeric` to `decimal.Decimal`, which lands in pandas as dtype `object`. | `.sum()` works on it — so the unit tests passed — while `.nlargest()` raises `cannot use method 'nlargest' with this dtype` and Plotly renders an empty axis. Caught on the *first* run of `dashboard/screenshots.py`, which is precisely the case it was written for. Fixed once at the boundary in `_query` rather than with a cast in each caller. |
| **`astype(float)` on a pandas NA.** `replace(0, pd.NA)` before a division. | `TypeError: float() argument must be ... not 'NAType'`. Dividing by NaN yields NaN; dividing by pandas' NA yields an NAType that `astype` then refuses. `Series.where(x != 0)` is the version that works. |
| **Playwright could not find Chromium.** It resolves the browser by a build number baked into the Python package (1234); the image ships 1194. | The error says "run `playwright install`", which in this environment is both wrong and a large download. The path is now found by searching `PLAYWRIGHT_BROWSERS_PATH`. |

### The additivity rule, measured

| | value |
| --- | ---: |
| `sum(nadd_aov)` across the six channels of a day, averaged | **£659.73** |
| `sum(revenue) / sum(revenue_orders)`, averaged | **£141.14** |

A factor of 4.7, and the wrong one draws as a perfectly plausible chart line.
That is what the `nadd_` prefix is for, and `dashboard/metrics.py` is where
honouring it actually happens.

---

## Phase 8 — complete

**Goal:** map every concept to the code that demonstrates it, and verify the
whole platform from a destroyed database.

### Delivered

| Artifact | What it does |
| --- | --- |
| [`docs/CONCEPTS.md`](CONCEPTS.md) | 14 sections, ~100 concepts, each pointing at a file — and a final section on the ten bugs this project shipped and then found |
| `make verify-cold` | `make reset` first, so `db/init/*.sql` is actually exercised |
| README final pass | Verified numbers replacing every "planned" marker |

### Acceptance — met, from an empty database

`docker compose down -v` on every profile, then `make verify`, in **5m 02s**:

```
ruff check + ruff format       clean, 83 files
data quality                   38 checks, 0 failed, 0 blocking
dbt                            21 models, 87 tests, 0 errors
pytest                         271 passed
retrieval                      recall@5 1.00 hybrid / 1.00 lexical / 0.91 vector
                               abstention 1.00, threshold gap 0.626 → 0.730
```

Then the halves `make verify` does not start services for:

```
stream-demo    3,000 produced · 2,884 consumed per group · 116 dead-lettered
               lag drains to 0 on all 3 partitions, both groups
api-demo       17/17 endpoint steps, over the network
dashboard      8/8 tabs render, 0 exceptions
```

### Why `verify-cold` is a separate target

`make verify` runs against whatever database is there. That is the right default
— destroying somebody's volumes is not something a verification target should do
without being asked — but it means the one thing it never tests is `db/init/`,
which Postgres executes **only on an empty data directory**. A broken init
script survives every other target in the Makefile. `verify-cold` says what it
does in its name and is the run that would catch it.

### What CONCEPTS.md is for

Two rules govern it. Every row points at code that runs, not at a comment
claiming a technique is used. And where a decision had a losing alternative, the
alternative is named — "we use a star schema" is not an insight; "we split the
order fact into header and line grains because header-only makes `dim_product`
unjoinable and line-only forces `count(distinct order_id)` into every revenue
query" is.

Its last section is the one worth reading: **ten bugs this project shipped and
then found**, with why each was invisible. A `\b` that never matched a phone
number. Redaction tokens poisoning BM25 in 30% of the corpus. Hybrid retrieval
that was silently vector-only. A `.sql` file missing from the wheel. A customer
deleted four months before signing up. A Kafka batch committing one partition of
three. Seven of eight dashboard tabs broken with every test green.

The pattern is identical in every case: **the failure had no symptom.** Nothing
errored, nothing went red, and the output looked reasonable. That is the whole
argument for the contracts, the measured baselines, the tests that assert they
are not vacuous, and the reproduction query on every quality finding.

---

## The platform, finished

| Phase | What it added | Tag |
| --- | --- | --- |
| 0 | Contracts, seed generator, governance | `phase-0` |
| 1 | Masking, embeddings, hybrid retrieval, the golden eval | `phase-1` |
| 2 | Bronze/Silver, four ingestors, the warehouse loader | `phase-2` |
| 3 | Data quality suites, Airflow, three isolated interpreters | `phase-3` |
| 4 | The dbt star schema, SCD2 with real history, seven marts | `phase-4` |
| 5 | Redpanda, two consumer groups, a DLQ, lag as a time series | `phase-5` |
| 6 | The FastAPI service and the role boundary per handler | `phase-6` |
| 7 | The Streamlit dashboard, cached on the pipeline watermark | `phase-7` |
| 8 | `CONCEPTS.md` and cold verification | `phase-8` |

**12,589 lines** of Python across `src/` and `dashboard/`, 21 dbt models,
6 singular dbt tests, 38 data quality checks, 272 pytest tests, 3 Airflow DAGs,
5 source systems, 7 warehouse schemas and 6 database roles.

---

## After Phase 8 — CI, and the two bugs it found

Added because nothing re-ran the tests on push, which for a repository whose
stated selling point is "verified from a destroyed database" is the gap most
likely to make the README quietly false in six months.

| Job | What it does | Time |
| --- | --- | --- |
| `checks` | Lint plus the whole suite with no Docker. Relies on the tests skipping themselves rather than on a hand-maintained subset that drifts | ~1 min |
| `stack` | Postgres and MinIO up, then seed → pipeline → rag-index → gold → streaming → full suite → retrieval eval. The only job that exercises `db/init/*.sql` | ~6 min |

It paid for itself on the first two runs.

**Three dependencies were declared nowhere.** `pandera` (imported by
`dq/run.py` and `dq/schemas.py`) had been hand-installed into a local venv in
Phase 3 and hand-installed again into the Airflow image; `numpy` and `pydantic`
were arriving transitively through fastembed and fastapi. Every machine that
mattered had all three.

The real fix was the test, not the manifest.
`test_declared_dependencies_cover_what_the_pipeline_imports` asserted against a
hardcoded list of five packages — which is exactly why it missed these three. A
hand-written list only ever checks the dependencies somebody remembered, which
are by definition not the ones that go missing. It now parses every import out
of `src/` and resolves each back to its distribution via
`packages_distributions()`, because module and distribution names differ often
enough (`yaml`/PyYAML, `jwt`/PyJWT) that naive matching passes falsely.
Verified non-vacuous by deleting the `pandera` line and confirming it
reproduces the exact failure CI reported.

`pandera` and `pandas` went into **core** dependencies rather than a `dq`
extra. `make pipeline` ends in `meridian.dq.run --suite all`, and the `schema`
suite in that union is Pandera. The extra was considered and rejected: it would
mean `--suite all` quietly running a third fewer checks on a thin install,
which is precisely the silent failure this project exists to argue against.

**Eight tests failed instead of skipping when nothing was configured.** Every
skip guard in the suite was written for "the server is down". "Not configured"
is a different failure by a different path: `settings.dsn()` raises
`RuntimeError` before any connection is attempted, so `server_reachable()`
never gets to report it. `dashboard/metrics._query` let that through, and the
`duck` fixture had no guard at all — and fixture ordering meant `(duck, etl,
loaded)` built `duck` first, so it raised before `etl`'s skip could fire.

Invisible on any developer machine, because they all have a `.env`.
Reproducing it required moving the file **aside** — unsetting the variables was
not enough, since `settings()` auto-loads it from disk.

Both bugs had been true for weeks. Neither was visible from a machine that had
already been set up correctly, which is the argument for CI stated more sharply
than anything else in this document.

---

## Verified about the environment

Established by direct test, not assumption:

- Docker daemon runs in this container (needs a manual `dockerd` start)
- `pgvector/pgvector:pg16` boots; extension **0.8.6**, HNSW index built
- DuckDB **1.5.5** installs `httpfs` and `postgres` through the proxy; reads and
  writes Parquet on MinIO; attaches the OLTP database read-only
- Binary `COPY` **does** abort on a type mismatch — checked, because §3 claims it
- `fastembed` downloads `BAAI/bge-small-en-v1.5` through the proxy; 384 dims,
  L2-normalised
- Embedding throughput ~7.8 chunks/s on 4 CPUs — a full index takes ~170s; the
  content-hash skip path makes a no-op re-run ~5s
- `anthropic` 1.1.0: `thinking` and `output_config` are on the non-beta
  `messages.create` (checked against the SDK signature)
- 15 GB RAM, 4 CPUs, ~30 GB disk — the `core` profile is comfortable
- Chromium + Playwright at `/opt/pw-browsers` for Phase 7 screenshots
