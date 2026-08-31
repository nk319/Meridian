-- 02 — extensions.
--
-- Unconditional CREATE EXTENSION, per CONTRACTS.md §1, and that is deliberate:
-- `CREATE EXTENSION IF NOT EXISTS vector` on an image without pgvector still
-- errors, but an image swap is exactly the mistake this file exists to catch,
-- so the statement that catches it should be the plainest possible one.
--
-- Postgres aborts docker-entrypoint-initdb.d on any error and takes the whole
-- container down with it, so a missing extension fails here — loudly, at
-- startup — instead of surfacing as "type vector does not exist" from the
-- indexer several phases later.

\connect warehouse

CREATE EXTENSION vector;
CREATE EXTENSION citext;
CREATE EXTENSION pgcrypto;

-- Belt and braces. If someone later softens the statements above to
-- IF NOT EXISTS against an image that lacks pgvector, this still stops the
-- build with a message that names the actual cause.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector') THEN
        RAISE EXCEPTION
            'pgvector is not installed in `warehouse`. The image must be '
            'pgvector/pgvector:pg16 — postgres:16-alpine does not carry it '
            '(CONTRACTS.md §1).';
    END IF;
END
$$;

\connect oltp

-- The OLTP source system needs case-insensitive email and hashing, but has no
-- use for vectors: nothing embeds from the source database.
CREATE EXTENSION citext;
CREATE EXTENSION pgcrypto;
