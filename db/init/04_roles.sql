-- 04 — roles and grants.
--
-- Runs after 03, so every schema this file grants on already exists. That
-- ordering is the whole reason these scripts are numbered: Postgres aborts
-- docker-entrypoint-initdb.d on the first error and brings down every service
-- waiting on `service_healthy`, so a GRANT against a missing schema does not
-- fail one statement — it fails the entire stack.
--
-- Passwords come from the environment through psql's \getenv, never from a file
-- in this repo. docker-compose.yml passes them to the postgres container.

\set app_pw ''
\set etl_pw ''
\set dbt_pw ''
\set ro_pw  ''
\set rag_pw ''
\set airflow_pw ''

\getenv app_pw MERIDIAN_APP_PASSWORD
\getenv etl_pw MERIDIAN_ETL_PASSWORD
\getenv dbt_pw DBT_RUNNER_PASSWORD
\getenv ro_pw  ANALYTICS_RO_PASSWORD
\getenv rag_pw RAG_INDEXER_PASSWORD
\getenv airflow_pw AIRFLOW_DB_PASSWORD

-- An unset variable and an empty one are the same mistake with different
-- symptoms; both produce a login role with a blank password, which is worse
-- than a failed build. Catch them together, before CREATE ROLE.
SELECT (:'app_pw' = '' OR :'etl_pw' = '' OR :'dbt_pw' = ''
        OR :'ro_pw' = '' OR :'rag_pw' = '' OR :'airflow_pw' = '') AS missing_pw \gset

\if :missing_pw
\echo '>>> FATAL: one or more Meridian role passwords are unset or empty.'
DO LANGUAGE plpgsql $fatal$
BEGIN
    RAISE EXCEPTION
        'Role passwords are unset or empty. Set MERIDIAN_APP_PASSWORD, '
        'MERIDIAN_ETL_PASSWORD, DBT_RUNNER_PASSWORD, ANALYTICS_RO_PASSWORD, '
        'RAG_INDEXER_PASSWORD and AIRFLOW_DB_PASSWORD in .env — see '
        '.env.example. Refusing to create login roles with a blank password.';
END
$fatal$;
\endif

-- --------------------------------------------------------------------------
-- Roles. CONTRACTS.md §1.
-- --------------------------------------------------------------------------

CREATE ROLE meridian_app  LOGIN PASSWORD :'app_pw';
CREATE ROLE meridian_etl  LOGIN PASSWORD :'etl_pw';
CREATE ROLE dbt_runner    LOGIN PASSWORD :'dbt_pw';
CREATE ROLE analytics_ro  LOGIN PASSWORD :'ro_pw';
CREATE ROLE rag_indexer   LOGIN PASSWORD :'rag_pw';

-- Airflow's own metadata role. §1 describes the `airflow` database as "metadata
-- only, nothing else reads it", and this is what makes that true in both
-- directions: the scheduler owns that database outright and holds nothing
-- anywhere else, so a compromised orchestrator cannot read the warehouse, and no
-- pipeline role can perturb Airflow's own bookkeeping.
CREATE ROLE airflow       LOGIN PASSWORD :'airflow_pw';

COMMENT ON ROLE meridian_app IS 'CRUD on oltp.*. No warehouse access.';
COMMENT ON ROLE meridian_etl IS 'Read oltp.*; write silver.*, secure.*, meta.*.';
COMMENT ON ROLE dbt_runner   IS 'Owns gold_stg, gold_int, gold. Reads silver.';
COMMENT ON ROLE analytics_ro IS 'SELECT on gold and on the masked rag corpus. Nothing else.';
COMMENT ON ROLE rag_indexer  IS 'Read silver.support_*; write rag.*. No secure grant.';
COMMENT ON ROLE airflow      IS 'Owns the airflow metadata database. No access to oltp or warehouse.';

-- --------------------------------------------------------------------------
-- Database-level CONNECT.
--
-- PUBLIC holds CONNECT on every new database by default, which would let
-- analytics_ro open a session against `oltp` and read whatever PUBLIC can see
-- there. Revoke first, then grant deliberately. This is what makes "no oltp
-- access" a fact about the cluster rather than a sentence in a document.
-- --------------------------------------------------------------------------

REVOKE ALL ON DATABASE oltp      FROM PUBLIC;
REVOKE ALL ON DATABASE warehouse FROM PUBLIC;
REVOKE ALL ON DATABASE airflow   FROM PUBLIC;

GRANT CONNECT ON DATABASE oltp      TO meridian_app, meridian_etl;
GRANT CONNECT ON DATABASE warehouse TO meridian_etl, dbt_runner, analytics_ro, rag_indexer;
GRANT CONNECT ON DATABASE airflow   TO airflow;

-- dbt snapshots stage the incoming rows in a temporary table before merging
-- them, so a role that can create every table in three schemas still cannot run
-- `dbt snapshot` without this. TEMP is granted to PUBLIC by default and the
-- REVOKE above takes it away — correctly, since PUBLIC includes every role in
-- the cluster; this hands it back to the one role that needs it. Temporary
-- tables live in a per-session schema that no other session can see, so this
-- widens nothing that `secure` cares about.
GRANT TEMPORARY ON DATABASE warehouse TO dbt_runner;

-- Airflow runs its own schema migrations, so it needs to own the database it
-- migrates rather than merely write to it.
ALTER DATABASE airflow OWNER TO airflow;

-- --------------------------------------------------------------------------
-- oltp: the source system.
--
-- No tables exist yet — 05 creates them. ALTER DEFAULT PRIVILEGES is therefore
-- the mechanism that matters here; 05 re-grants explicitly at the end because
-- default privileges apply only to objects created after they are set, and a
-- future hand-run DDL is exactly the case that silently misses out.
-- --------------------------------------------------------------------------

\connect oltp

GRANT USAGE ON SCHEMA public TO meridian_app, meridian_etl;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO meridian_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO meridian_app;

-- The ETL reads the source system. It never writes to it.
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO meridian_etl;

-- --------------------------------------------------------------------------
-- warehouse.
-- --------------------------------------------------------------------------

\connect warehouse

-- meridian_etl owns the three schemas it writes, mirroring dbt_runner owning
-- gold* and rag_indexer owning rag. Ownership rather than USAGE+CREATE because
-- the component that writes a schema also has to be able to GRANT on the tables
-- it creates there — src/meridian/warehouse/ddl.sql issues exactly two such
-- grants, and a non-owner cannot.
ALTER SCHEMA silver OWNER TO meridian_etl;
ALTER SCHEMA secure OWNER TO meridian_etl;
ALTER SCHEMA meta   OWNER TO meridian_etl;

-- dbt owns its three schemas outright, so `dbt run` can create, drop and
-- replace models without a superuser in the loop.
ALTER SCHEMA gold_stg OWNER TO dbt_runner;
ALTER SCHEMA gold_int OWNER TO dbt_runner;
ALTER SCHEMA gold     OWNER TO dbt_runner;
GRANT USAGE ON SCHEMA silver TO dbt_runner;

-- analytics_ro: SELECT on gold, and nothing else in the warehouse except the
-- masked RAG corpus and the ops metadata below. No secure, no oltp, no silver.
GRANT USAGE ON SCHEMA gold TO analytics_ro;
ALTER DEFAULT PRIVILEGES FOR ROLE dbt_runner IN SCHEMA gold
    GRANT SELECT ON TABLES TO analytics_ro;

-- rag_indexer owns `rag` and creates its own tables there (see
-- src/meridian/rag/ddl.sql), so the indexer needs no superuser and no
-- out-of-band migration step.
ALTER SCHEMA rag OWNER TO rag_indexer;
GRANT USAGE ON SCHEMA silver TO rag_indexer;
-- Deliberately NOT `GRANT SELECT ON ALL TABLES IN SCHEMA silver`. The contract
-- grants this role `silver.support_*` and nothing more; the table-level grant
-- is issued in Phase 2, when silver.support_tickets is first created. Phase 1
-- indexes from seeds/rag/support_tickets.jsonl and needs no silver read at all.

-- analytics_ro reads the RAG corpus: this is what /v1/ai/* and the dashboard
-- query at request time. Safe because `rag` holds masked text by construction
-- — that is the schema's invariant, not a property of particular tables — and
-- tests/test_pii_manifest.py is what keeps the invariant true.
-- Recorded as a deviation in CONTRACTS.md: §1 said "SELECT on gold only" while
-- also making analytics_ro the role every AI handler connects as. Both could
-- not hold.
GRANT USAGE ON SCHEMA rag TO analytics_ro;
ALTER DEFAULT PRIVILEGES FOR ROLE rag_indexer IN SCHEMA rag
    GRANT SELECT ON TABLES TO analytics_ro;

-- The ops dashboard reads meta. §7 states its cache key reads
-- meta.pipeline_run_log.completed_at, and §1 makes analytics_ro the role the
-- dashboard connects as — so "SELECT on gold only" and "the dashboard reads
-- pipeline_run_log" could not both be true. The same class of contradiction as
-- the `rag` one above, resolved the same way: read-only, and meta holds no PII.
GRANT USAGE ON SCHEMA meta TO analytics_ro;
ALTER DEFAULT PRIVILEGES FOR ROLE meridian_etl IN SCHEMA meta
    GRANT SELECT ON TABLES TO analytics_ro;

-- dbt reads silver; the tables are created later by meridian_etl, so this is
-- the default-privilege half and warehouse/ddl.sql carries the explicit half.
ALTER DEFAULT PRIVILEGES FOR ROLE meridian_etl IN SCHEMA silver
    GRANT SELECT ON TABLES TO dbt_runner;

-- dbt also reads `rag`, for one table: rag.ticket_enrichment, the LLM's
-- predicted intent and sentiment. §8 makes gold.fact_support_tickets carry the
-- AI enrichment and says mart_support_health "depends structurally on the AI
-- enrichment columns" — deliberately, so the AI layer cannot become a side
-- attachment nothing consumes. §1 says dbt reads silver. Both cannot hold
-- unless the enrichment lands somewhere dbt can see it.
--
-- Resolved towards `rag` rather than `silver`, because the alternative is
-- worse in two ways at once: it would make rag_indexer a writer of `silver`
-- (§1 says the loader writes silver, and gives rag_indexer read on
-- silver.support_* and write on rag.* only), and it would put a model's output
-- in a layer defined as cleaned source data. The read is safe for the same
-- reason analytics_ro's is: `rag` holds masked text by construction, and the
-- enrichment table holds labels drawn from a frozen vocabulary — no free text
-- at all. Recorded as a deviation in CONTRACTS.md.
GRANT USAGE ON SCHEMA rag TO dbt_runner;
ALTER DEFAULT PRIVILEGES FOR ROLE rag_indexer IN SCHEMA rag
    GRANT SELECT ON TABLES TO dbt_runner;

-- `secure` is the PII schema. Nobody but the ETL touches it. PUBLIC never had
-- USAGE on a newly created schema, but stating the revoke makes the intent
-- reviewable instead of relying on a default the reader has to know.
REVOKE ALL ON SCHEMA secure FROM PUBLIC;
