-- Warehouse tables owned by the pipeline: meta, silver, secure.
--
-- Applied by `python -m meridian.warehouse.bootstrap` as `meridian_etl`, which
-- holds CREATE on exactly these three schemas. Not in db/init/: CONTRACTS.md §1
-- freezes that sequence at 01..05, and the same reasoning that put the RAG
-- store's DDL in the indexer (§11) applies here — the component that writes a
-- schema owns its shape, and a fresh clone needs no migration step.
--
-- Everything is idempotent. Re-running the bootstrap must never be destructive.
--
-- `gold*` is absent on purpose: those schemas are owned by dbt_runner and dbt
-- creates every object in them.
--
-- No psql meta-commands in here: bootstrap.py executes this file through
-- psycopg, which speaks SQL and not \connect.

-- ==========================================================================
-- meta — ops, data quality, watermarks, lineage. CONTRACTS.md §7.
-- ==========================================================================

-- One data-quality results table, fed by Pandera, custom checks and parsed dbt
-- run_results.json alike. One table rather than three is the point: the ops
-- dashboard has a single thing to read, and a check's severity is a column
-- rather than a choice of destination.
CREATE TABLE IF NOT EXISTS meta.dq_check_results (
    check_run_id  uuid        NOT NULL,
    run_id        uuid        NOT NULL,
    checked_at    timestamptz NOT NULL DEFAULT now(),
    source        text        NOT NULL CHECK (source IN ('pandera', 'dbt', 'custom')),
    suite         text        NOT NULL,
    check_name    text        NOT NULL,
    target_table  text        NOT NULL,
    target_column text,
    severity      text        NOT NULL CHECK (severity IN ('BLOCK', 'QUARANTINE', 'WARN')),
    status        text        NOT NULL CHECK (status IN ('PASS', 'FAIL')),
    rows_scanned  bigint,
    rows_failed   bigint,
    failure_pct   numeric,
    owner_team    text,
    -- Lets the dashboard link a failure to a reproduction rather than to a
    -- number. A DQ result nobody can reproduce is a number nobody acts on.
    repro_sql     text,
    message       text,
    PRIMARY KEY (check_run_id)
);

CREATE INDEX IF NOT EXISTS dq_check_results_run_id_idx  ON meta.dq_check_results (run_id);
CREATE INDEX IF NOT EXISTS dq_check_results_status_idx  ON meta.dq_check_results (status, checked_at DESC);
CREATE INDEX IF NOT EXISTS dq_check_results_target_idx  ON meta.dq_check_results (target_table);

CREATE TABLE IF NOT EXISTS meta.pipeline_run_log (
    run_id       uuid PRIMARY KEY,
    dag_id       text,
    step         text        NOT NULL,
    started_at   timestamptz NOT NULL,
    -- The dashboard's cache key reads THIS column (§7). It stays NULL while a
    -- run is in flight, which is what makes a half-finished run unable to
    -- present itself as fresh data.
    completed_at timestamptz,
    status       text        NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    rows_in      bigint,
    rows_out     bigint
);

CREATE INDEX IF NOT EXISTS pipeline_run_log_completed_idx ON meta.pipeline_run_log (completed_at DESC);
CREATE INDEX IF NOT EXISTS pipeline_run_log_step_idx      ON meta.pipeline_run_log (step, started_at DESC);

CREATE TABLE IF NOT EXISTS meta.ingest_watermarks (
    entity          text PRIMARY KEY,
    watermark_value timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS meta.kafka_consumer_offsets (
    consumer_group text        NOT NULL,
    topic          text        NOT NULL,
    partition      int         NOT NULL,
    current_offset bigint      NOT NULL,
    log_end_offset bigint      NOT NULL,
    observed_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, topic, partition, observed_at)
);


-- ==========================================================================
-- silver — cleaned, typed, deduped. dbt's only source.
-- ==========================================================================
--
-- Four lineage columns are carried down from Bronze: _source_system,
-- _ingest_run_id, _ingested_at and _record_hash. The other two Bronze columns
-- stop here. _source_file and _batch_seq describe a physical file, and after
-- dedup across runs a Silver row no longer corresponds to one — carrying them
-- would mean picking an arbitrary winner's file name and presenting it as
-- provenance.
--
-- CHECK constraints repeat the frozen vocabularies from §9. Silver's guarantee
-- is that bad rows were quarantined; a constraint is what turns that from a
-- claim about the quarantine code into something the database enforces. If a
-- quarantine rule is ever wrong, the load fails here instead of poisoning gold.
--
-- No cross-table foreign keys. A quarantined customer would make its orders
-- unloadable, coupling every entity's load to every other entity's quarantine
-- decisions. Referential integrity is asserted by dbt tests, where a violation
-- is a reported failure rather than a hard stop halfway through a load.

CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id     text PRIMARY KEY,
    city            text NOT NULL,
    country         text NOT NULL,
    signup_date     date NOT NULL,
    loyalty_tier    text NOT NULL CHECK (loyalty_tier IN ('bronze','silver','gold','platinum')),
    segment         text NOT NULL CHECK (segment IN ('new','active','at_risk','churned','vip')),
    is_deleted      boolean     NOT NULL,
    updated_at      timestamptz NOT NULL,
    _source_system  text        NOT NULL,
    _ingest_run_id  uuid        NOT NULL,
    _ingested_at    timestamptz NOT NULL,
    _record_hash    text        NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.orders (
    order_id        text PRIMARY KEY,
    customer_id     text NOT NULL,
    order_ts        timestamptz NOT NULL,
    order_date      date        NOT NULL,
    status          text NOT NULL CHECK (status IN ('pending','confirmed','shipped','delivered','cancelled','returned')),
    channel         text NOT NULL CHECK (channel IN ('organic','paid_search','email','social','direct','affiliate')),
    device_type     text NOT NULL CHECK (device_type IN ('desktop','mobile','tablet')),
    gross_amount    numeric(12,2) NOT NULL CHECK (gross_amount    >= 0),
    discount_amount numeric(12,2) NOT NULL CHECK (discount_amount >= 0),
    shipping_amount numeric(12,2) NOT NULL CHECK (shipping_amount >= 0),
    tax_amount      numeric(12,2) NOT NULL CHECK (tax_amount      >= 0),
    total_amount    numeric(12,2) NOT NULL CHECK (total_amount    >= 0),
    _source_system  text        NOT NULL,
    _ingest_run_id  uuid        NOT NULL,
    _ingested_at    timestamptz NOT NULL,
    _record_hash    text        NOT NULL
);
CREATE INDEX IF NOT EXISTS silver_orders_customer_idx ON silver.orders (customer_id);
CREATE INDEX IF NOT EXISTS silver_orders_date_idx     ON silver.orders (order_date);

CREATE TABLE IF NOT EXISTS silver.order_items (
    order_item_id   text PRIMARY KEY,
    order_id        text NOT NULL,
    line_number     int  NOT NULL CHECK (line_number > 0),
    product_id      text NOT NULL,
    quantity        int  NOT NULL CHECK (quantity > 0),
    unit_price      numeric(12,2) NOT NULL CHECK (unit_price  >= 0),
    line_amount     numeric(12,2) NOT NULL CHECK (line_amount >= 0),
    _source_system  text        NOT NULL,
    _ingest_run_id  uuid        NOT NULL,
    _ingested_at    timestamptz NOT NULL,
    _record_hash    text        NOT NULL,
    UNIQUE (order_id, line_number)
);
CREATE INDEX IF NOT EXISTS silver_order_items_order_idx   ON silver.order_items (order_id);
CREATE INDEX IF NOT EXISTS silver_order_items_product_idx ON silver.order_items (product_id);

CREATE TABLE IF NOT EXISTS silver.customer_change_log (
    customer_id     text        NOT NULL,
    changed_at      timestamptz NOT NULL,
    field           text        NOT NULL,
    old_value       text,
    new_value       text,
    _source_system  text        NOT NULL,
    _ingest_run_id  uuid        NOT NULL,
    _ingested_at    timestamptz NOT NULL,
    _record_hash    text        NOT NULL,
    PRIMARY KEY (customer_id, changed_at, field)
);

CREATE TABLE IF NOT EXISTS silver.products (
    product_id      text PRIMARY KEY,
    sku             text NOT NULL,
    product_name    text NOT NULL,
    category        text NOT NULL,
    subcategory     text NOT NULL,
    unit_price      numeric(12,2) NOT NULL CHECK (unit_price >= 0),
    unit_cost       numeric(12,2) NOT NULL CHECK (unit_cost  >= 0),
    is_active       boolean       NOT NULL,
    _source_system  text        NOT NULL,
    _ingest_run_id  uuid        NOT NULL,
    _ingested_at    timestamptz NOT NULL,
    _record_hash    text        NOT NULL
);
CREATE INDEX IF NOT EXISTS silver_products_category_idx ON silver.products (category);

CREATE TABLE IF NOT EXISTS silver.web_events (
    event_id        text PRIMARY KEY,
    session_id      text NOT NULL,
    customer_id     text,
    event_ts        timestamptz NOT NULL,
    event_type      text NOT NULL CHECK (event_type IN ('page_view','product_view','add_to_cart','begin_checkout','purchase','search')),
    product_id      text,
    order_id        text,
    channel         text NOT NULL CHECK (channel IN ('organic','paid_search','email','social','direct','affiliate')),
    device_type     text NOT NULL CHECK (device_type IN ('desktop','mobile','tablet')),
    _source_system  text        NOT NULL,
    _ingest_run_id  uuid        NOT NULL,
    _ingested_at    timestamptz NOT NULL,
    _record_hash    text        NOT NULL
);
CREATE INDEX IF NOT EXISTS silver_web_events_session_idx ON silver.web_events (session_id, event_ts);
CREATE INDEX IF NOT EXISTS silver_web_events_ts_idx      ON silver.web_events (event_ts);

-- subject and body are classified `sensitive` (§10) and stay unmasked here:
-- this is the warehouse's copy of record. Masking happens at the RAG boundary,
-- where `rag_indexer` reads this table and writes only masked text into `rag`.
-- That is why rag_indexer is granted SELECT on this one table and nothing else
-- in silver.
CREATE TABLE IF NOT EXISTS silver.support_tickets (
    ticket_id       text PRIMARY KEY,
    customer_id     text NOT NULL,
    order_id        text NOT NULL,
    created_ts      timestamptz NOT NULL,
    resolved_ts     timestamptz,
    status          text NOT NULL CHECK (status IN ('open','pending','resolved')),
    channel         text NOT NULL CHECK (channel IN ('email','chat','phone','web_form')),
    subject         text NOT NULL,
    body            text NOT NULL,
    intent          text NOT NULL CHECK (intent IN ('shipping_delay','refund_request','product_defect','billing_question','account_access','return_process','general_inquiry')),
    priority        text NOT NULL CHECK (priority  IN ('P1','P2','P3','P4')),
    sentiment       text NOT NULL CHECK (sentiment IN ('positive','neutral','negative')),
    _source_system  text        NOT NULL,
    _ingest_run_id  uuid        NOT NULL,
    _ingested_at    timestamptz NOT NULL,
    _record_hash    text        NOT NULL,
    CONSTRAINT silver_tickets_resolved_after_created
        CHECK (resolved_ts IS NULL OR resolved_ts >= created_ts)
);
CREATE INDEX IF NOT EXISTS silver_tickets_created_idx ON silver.support_tickets (created_ts);

CREATE TABLE IF NOT EXISTS silver.payments (
    payment_id      text PRIMARY KEY,
    order_id        text NOT NULL,
    attempt_number  int  NOT NULL CHECK (attempt_number > 0),
    payment_method  text NOT NULL CHECK (payment_method IN ('card','paypal','bank_transfer','gift_card')),
    status          text NOT NULL CHECK (status IN ('authorized','captured','failed','refunded','chargeback')),
    amount          numeric(12,2) NOT NULL CHECK (amount >= 0),
    processed_ts    timestamptz NOT NULL,
    failure_reason  text,
    _source_system  text        NOT NULL,
    _ingest_run_id  uuid        NOT NULL,
    _ingested_at    timestamptz NOT NULL,
    _record_hash    text        NOT NULL
);
CREATE INDEX IF NOT EXISTS silver_payments_order_idx ON silver.payments (order_id);


-- ==========================================================================
-- secure — PII, physically separated so grants can exclude it. §10.
-- ==========================================================================
--
-- This table is loaded DIRECTLY from seeds/secure/customer_pii.csv and never
-- passes through Bronze or Silver. That is the enforcement mechanism, not a
-- convenience: if PII travelled the lake, every later step would have to
-- remember to drop it, and one of them eventually would not. There is no code
-- path that can carry a name into `silver` because there is no `silver` stage
-- that ever holds one.
CREATE TABLE IF NOT EXISTS secure.customer_pii (
    customer_id  text PRIMARY KEY,
    first_name   text   NOT NULL,
    last_name    text   NOT NULL,
    email        citext NOT NULL UNIQUE,
    phone        text   NOT NULL,
    loaded_at    timestamptz NOT NULL DEFAULT now()
);


-- ==========================================================================
-- Grants that default privileges cannot express.
-- ==========================================================================

-- CONTRACTS §1 grants rag_indexer "silver.support_*" and nothing more. Default
-- privileges are per-schema, so the narrow grant has to be explicit, and it
-- could not be issued in db/init/04 because the table did not exist yet.
-- This closes the loose end Phase 1 recorded.
GRANT SELECT ON silver.support_tickets TO rag_indexer;

-- The ops dashboard reads meta (§7). The schema-level USAGE and the default
-- privilege live in db/init/04_roles.sql with every other schema-level grant;
-- this is the belt-and-braces half for tables created before those defaults
-- applied.
GRANT SELECT ON ALL TABLES IN SCHEMA meta TO analytics_ro;

-- Belt and braces for dbt, matching the pattern §1 asks for on the gold side:
-- default privileges only apply to objects created after they were set, so a
-- table added by hand later would otherwise be invisible to dbt.
GRANT SELECT ON ALL TABLES IN SCHEMA silver TO dbt_runner;
