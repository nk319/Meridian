-- 05 — the source system's tables.
--
-- These mirror seeds/oltp/*.csv column for column. That is the point: the
-- generator, this DDL and the ingestion layer are three places the same shape
-- has to be written down, and Phase 0 froze it so they cannot drift.
--
-- Note what is NOT here: no PII columns on `customers`. The generator splits
-- names, emails and phone numbers into seeds/secure/customer_pii.csv at
-- generation time, and they load straight into warehouse.secure.customer_pii.
-- Physical separation is the enforcement mechanism (CONTRACTS.md §10); if the
-- source table carried PII, every downstream step would have to remember to
-- drop it, and one of them eventually would not.
--
-- CHECK constraints spell out the frozen vocabularies from CONTRACTS.md §9. An
-- enum written only in a YAML file is a comment; a CHECK constraint is a
-- control, and it fails on the row that broke it rather than three models later.

\connect oltp

-- --------------------------------------------------------------------------
-- customers
-- --------------------------------------------------------------------------
CREATE TABLE customers (
    customer_id   text        PRIMARY KEY,
    city          text        NOT NULL,
    country       text        NOT NULL,
    signup_date   date        NOT NULL,
    loyalty_tier  text        NOT NULL
                  CHECK (loyalty_tier IN ('bronze', 'silver', 'gold', 'platinum')),
    segment       text        NOT NULL
                  CHECK (segment IN ('new', 'active', 'at_risk', 'churned', 'vip')),
    is_deleted    boolean     NOT NULL DEFAULT false,
    updated_at    timestamptz NOT NULL
);

COMMENT ON TABLE customers IS
    'Source-system customers. Carries no PII by construction — see secure.customer_pii.';

CREATE INDEX customers_updated_at_idx ON customers (updated_at);

-- --------------------------------------------------------------------------
-- orders
-- --------------------------------------------------------------------------
CREATE TABLE orders (
    order_id         text        PRIMARY KEY,
    customer_id      text        NOT NULL REFERENCES customers (customer_id),
    order_ts         timestamptz NOT NULL,
    order_date       date        NOT NULL,
    status           text        NOT NULL
                     CHECK (status IN ('pending', 'confirmed', 'shipped',
                                       'delivered', 'cancelled', 'returned')),
    channel          text        NOT NULL
                     CHECK (channel IN ('organic', 'paid_search', 'email',
                                        'social', 'direct', 'affiliate')),
    device_type      text        NOT NULL
                     CHECK (device_type IN ('desktop', 'mobile', 'tablet')),
    gross_amount     numeric(12, 2) NOT NULL CHECK (gross_amount    >= 0),
    discount_amount  numeric(12, 2) NOT NULL CHECK (discount_amount >= 0),
    shipping_amount  numeric(12, 2) NOT NULL CHECK (shipping_amount >= 0),
    tax_amount       numeric(12, 2) NOT NULL CHECK (tax_amount      >= 0),
    total_amount     numeric(12, 2) NOT NULL CHECK (total_amount    >= 0)
);

CREATE INDEX orders_customer_id_idx ON orders (customer_id);
CREATE INDEX orders_order_date_idx  ON orders (order_date);
CREATE INDEX orders_order_ts_idx    ON orders (order_ts);

-- --------------------------------------------------------------------------
-- order_items — line grain, one row per line. CONTRACTS.md §8 keeps this
-- separate from order-header grain all the way through to the fact tables.
-- --------------------------------------------------------------------------
CREATE TABLE order_items (
    order_item_id  text           PRIMARY KEY,
    order_id       text           NOT NULL REFERENCES orders (order_id),
    line_number    int            NOT NULL CHECK (line_number > 0),
    product_id     text           NOT NULL,
    quantity       int            NOT NULL CHECK (quantity > 0),
    unit_price     numeric(12, 2) NOT NULL CHECK (unit_price >= 0),
    line_amount    numeric(12, 2) NOT NULL CHECK (line_amount >= 0),
    UNIQUE (order_id, line_number)
);

CREATE INDEX order_items_order_id_idx   ON order_items (order_id);
CREATE INDEX order_items_product_id_idx ON order_items (product_id);

-- No FK to a products table: products arrive on the `files` feed, not from the
-- OLTP database (CONTRACTS.md §5), and that feed carries injected defects.
-- A foreign key here would reject dirty rows at load time and rob the Phase 3
-- quarantine path of the exact case it exists to handle.

-- --------------------------------------------------------------------------
-- customer_change_log — what makes the SCD2 dimension real rather than
-- tautological. CONTRACTS.md §8.
-- --------------------------------------------------------------------------
CREATE TABLE customer_change_log (
    change_id    bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id  text        NOT NULL REFERENCES customers (customer_id),
    changed_at   timestamptz NOT NULL,
    field        text        NOT NULL,
    old_value    text,
    new_value    text
);

CREATE INDEX customer_change_log_customer_id_idx ON customer_change_log (customer_id, changed_at);

-- --------------------------------------------------------------------------
-- support_tickets — the RAG corpus at source. Served by the REST API in
-- Phase 6 and ingested over it; Phase 1 indexes the same rows straight from
-- seeds/rag/support_tickets.jsonl so the AI layer does not wait on ingestion.
--
-- `subject` and `body` are classified sensitive (free text that demonstrably
-- contains identifiers) and are masked before they ever reach `rag`. They are
-- unmasked here because this is the source system, which is the one place the
-- real text legitimately lives.
-- --------------------------------------------------------------------------
CREATE TABLE support_tickets (
    ticket_id    text        PRIMARY KEY,
    customer_id  text        NOT NULL REFERENCES customers (customer_id),
    order_id     text        NOT NULL REFERENCES orders (order_id),
    created_ts   timestamptz NOT NULL,
    resolved_ts  timestamptz,
    status       text        NOT NULL
                 CHECK (status IN ('open', 'pending', 'resolved')),
    -- Contact channel, NOT the marketing channel of the same name on `orders`.
    -- CONTRACTS.md §9 freezes one vocabulary called `channel` and it is the
    -- marketing one; this column is a different vocabulary that happens to
    -- share a name. Recorded in CONTRACTS.md so the collision is deliberate
    -- rather than discovered by whoever writes the first union.
    channel      text        NOT NULL
                 CHECK (channel IN ('email', 'chat', 'phone', 'web_form')),
    subject      text        NOT NULL,
    body         text        NOT NULL,
    -- Ground truth. The AI enrichment predicts these independently and is
    -- scored against them; they are never fed to the model.
    intent       text        NOT NULL
                 CHECK (intent IN ('shipping_delay', 'refund_request',
                                   'product_defect', 'billing_question',
                                   'account_access', 'return_process',
                                   'general_inquiry')),
    priority     text        NOT NULL CHECK (priority  IN ('P1', 'P2', 'P3', 'P4')),
    sentiment    text        NOT NULL CHECK (sentiment IN ('positive', 'neutral', 'negative')),
    CONSTRAINT support_tickets_resolved_after_created
        CHECK (resolved_ts IS NULL OR resolved_ts >= created_ts)
);

CREATE INDEX support_tickets_customer_id_idx ON support_tickets (customer_id);
CREATE INDEX support_tickets_created_ts_idx  ON support_tickets (created_ts);
CREATE INDEX support_tickets_intent_idx      ON support_tickets (intent);

-- --------------------------------------------------------------------------
-- Belt-and-braces grants.
--
-- 04 set ALTER DEFAULT PRIVILEGES, which covers every table above because they
-- are created afterwards by the same role. These explicit grants exist for the
-- table someone adds by hand later, outside the init sequence, when the default
-- privileges no longer apply. CONTRACTS.md §1 asks for exactly this pattern on
-- the dbt side and the reasoning is identical here.
-- --------------------------------------------------------------------------
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES    IN SCHEMA public TO meridian_app;
GRANT USAGE, SELECT                  ON ALL SEQUENCES IN SCHEMA public TO meridian_app;
GRANT SELECT                         ON ALL TABLES    IN SCHEMA public TO meridian_etl;
