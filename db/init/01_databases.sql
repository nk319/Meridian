-- 01 — databases.
--
-- Runs against the POSTGRES_DB database as the superuser, before anything else.
-- CREATE DATABASE cannot run inside a transaction block; psql executes each
-- statement in this file with its own implicit commit, so this is fine as-is
-- but must never be wrapped in BEGIN/COMMIT.
--
-- CONTRACTS.md §1. Three databases, no more: `airflow` is metadata only and
-- nothing else ever reads it.

CREATE DATABASE oltp;
COMMENT ON DATABASE oltp IS 'Source system simulation. The API writes here.';

CREATE DATABASE warehouse;
COMMENT ON DATABASE warehouse IS 'Lake landing, marts, ops metadata, vectors.';

CREATE DATABASE airflow;
COMMENT ON DATABASE airflow IS 'Airflow metadata only. Nothing else reads it.';
