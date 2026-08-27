# PROGRESS

Updated as the **last action of every work session**. This file plus the
`phase-N` git tags are the only things needed to resume — never conversation
history, which does not survive a container restart.

---

## Current state

| | |
| --- | --- |
| **Phase** | 2 — Batch ingestion and the lakehouse |
| **Status** | ✅ Complete |
| **Tag** | `phase-2` — **created locally, not on the remote**, see below |
| **Next phase** | 3 — Data quality suites and Airflow orchestration |
| **Next action** | Build `python -m meridian.dq.run --suite <name>` on top of the `meta.dq_check_results` table Phase 2 already writes to, then wrap the existing module entrypoints in Airflow 3.1.3 DAGs (CONTRACTS.md §6) |

**Pushing a tag is still 403 from this environment.** `git push origin
refs/tags/phase-N` fails with HTTP 403 on every attempt while
`git push origin <branch>` to the same remote succeeds, and the GitHub tool
surface available here exposes no tag-creation call. `phase-1` and `phase-2`
exist in the local repository and on no remote. Someone with tag-push permission
needs to run:

```bash
git push origin refs/tags/phase-1 refs/tags/phase-2
```

Until then the resume point is the branch `claude/phase-1-setup-8lokjp`, which
carries every phase. (The branch name is Phase 1's; the session was told to
develop there and not to push elsewhere without permission.)

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

## Phase 3 — data quality suites and orchestration (next)

**Goal:** turn the quarantine checks Phase 2 writes ad hoc into a declared suite
framework, and put Airflow in front of the module entrypoints.

**Planned deliverables**

- `python -m meridian.dq.run --suite <name>` (CONTRACTS §5), writing to the
  `meta.dq_check_results` table that already exists and is already populated
- Pandera schemas per entity, so the rules live beside the data contract rather
  than inside `build_silver`
- dbt `run_results.json` parsing into the same table (`source = 'dbt'`)
- Airflow **3.1.3**, pinned to the exact patch, `LocalExecutor`, with dbt in an
  isolated `/opt/dbt-venv` invoked by absolute path — CONTRACTS §6 explains why
  co-installing is an unresolvable resolver error, not a preference
- A Bronze→Silver→warehouse row-count reconciliation DAG task

**Watch for:** the `meta.dq_check_results` schema is already frozen and already
has rows in it. Phase 3 adds sources to it (`pandera`, `dbt`); it should not
reshape it.

**Loose ends inherited**

- §9 has no `ticket_status` or `ticket_channel` vocabulary. Both are enforced as
  CHECK constraints in `db/init/05_oltp_ddl.sql` and `lake/silver_spec.py`, and
  `ticket_channel` collides by name with §9's frozen marketing `channel`.
  Promote both when Phase 4 builds `fact_support_tickets`.
- `lake/silver_spec.py` duplicates §9's vocabularies so the lake does not import
  the stdlib-only seed package. `tests/test_lake_contracts.py` asserts the two
  agree; if a third copy appears, generate them instead.
- The golden set's abstention cases are all off-domain. Similarity thresholding
  cannot catch an on-topic-but-unanswerable question; that is handled by the
  system prompt in `generate.py` and is not measured. Worth a faithfulness eval
  once Phase 4's enrichment lands.
- Phase 0's `writers.py` says vendor pagination is "demonstrated in Phase 3".
  It was built in Phase 2; the comment predates the current numbering.

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
