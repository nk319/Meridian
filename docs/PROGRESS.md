# PROGRESS

Updated as the **last action of every work session**. This file plus the
`phase-N` git tags are the only things needed to resume — never conversation
history, which does not survive a container restart.

---

## Current state

| | |
| --- | --- |
| **Phase** | 3 — Data quality suites and orchestration |
| **Status** | ✅ Complete |
| **Tag** | `phase-3` — needs a human to push it, see below |
| **Next phase** | 4 — dbt Gold star schema |
| **Next action** | Build the dbt project: `gold_stg` → `gold_int` → `gold`, with `dim_customer` as an SCD2 snapshot and the split order-header / order-line fact grains (CONTRACTS.md §8). dbt is already installed at `/opt/dbt-venv` in the Airflow image; add `dbt_run` and `dbt_test` tasks to `meridian_batch` after `data_quality`. |

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
  git tag -a phase-N <sha> -m "Phase N: ..."
  git push origin refs/tags/phase-N
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

## Phase 4 — dbt Gold star schema (next)

**Goal:** `gold_stg` → `gold_int` → `gold`, read by nothing but the dashboard.

**Planned deliverables**

- dbt project run from `/opt/dbt-venv/bin/dbt` by absolute path (already in the image)
- `dim_date`, `dim_customer` (SCD2 snapshot, `check` strategy on `loyalty_tier`
  and `segment`, `hard_deletes='new_record'`), `dim_product`
- `fact_orders` at **order-header** grain and `fact_order_items` at **line**
  grain — §8 resolves that split and expects it as the first interview question
- `fact_payments`, `fact_web_events`, `fact_support_tickets`
- The seven marts from §8, with the `nadd_` prefix on every non-additive measure
- Three singular tests on the SCD2 dimension: no overlapping validity windows,
  exactly one `is_current` per customer, and demo customer `C000042` with
  exactly three versions
- `dbt run` / `dbt test` tasks appended to `meridian_batch`
- dbt `run_results.json` parsed into `meta.dq_check_results` with `source='dbt'`

**Watch for:** the generator emits `C000042` with three backdated tier
transitions and hard-deletes `C000117` precisely so the snapshot is exercised. A
snapshot over static data produces one version per customer and its tests pass
against an empty result set.

---

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
