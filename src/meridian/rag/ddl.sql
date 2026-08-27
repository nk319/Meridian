-- The RAG vector store.
--
-- Applied by `python -m meridian.rag.index` as the `rag_indexer` role, which
-- owns the `rag` schema (db/init/04_roles.sql). Deliberately not in
-- db/init/: CONTRACTS.md §1 freezes that sequence at 01..05 and names the rag
-- schema as "written by the rag indexer". The indexer owning its own tables
-- also means a fresh clone needs no migration step the README has to remember
-- to mention.
--
-- Everything here is idempotent. Re-running the indexer must never be a
-- destructive act.

CREATE TABLE IF NOT EXISTS rag.chunks (
    chunk_id             text PRIMARY KEY,
    ticket_id            text        NOT NULL,
    chunk_seq            int         NOT NULL,

    -- Masked text, and only masked text. This is the invariant the whole schema
    -- rests on: `rag` never holds an unmasked identifier, which is why
    -- analytics_ro can be granted SELECT on the schema as a whole rather than
    -- on a hand-picked list of tables that would drift.
    content              text        NOT NULL,

    -- sha256 of the MASKED content, not the raw. Hashing the raw text is the
    -- specific mistake CONTRACTS.md §10 warns about: the hash is a skip key, so
    -- hashing pre-masking input means a later masking fix leaves already-indexed
    -- chunks untouched and the leak becomes permanent. Hashing the output makes
    -- "the masker changed this chunk" and "re-embed this chunk" the same event.
    content_hash         text        NOT NULL,

    -- Provenance for the two things that decide what a row means. Both are part
    -- of the skip predicate, so changing either re-embeds the corpus.
    masking_fingerprint  text        NOT NULL,
    embedding_model      text        NOT NULL,

    embedding            vector(384),
    word_count           int         NOT NULL,

    -- Document length in lexemes, the `dl` term in BM25's length
    -- normalisation. Populated by the indexer's lexical refresh alongside
    -- rag.chunk_terms, not by a generated column: deriving it needs unnest(),
    -- a set-returning function, which GENERATED ALWAYS AS does not allow.
    doc_len              int         CHECK (doc_len IS NULL OR doc_len >= 0),

    -- Named word_count, not token_count: it is a whitespace split, and calling
    -- it tokens would imply agreement with the model's tokeniser that it does
    -- not have.

    created_ts           timestamptz NOT NULL,
    source               text        NOT NULL DEFAULT 'support_tickets',
    indexed_at           timestamptz NOT NULL DEFAULT now(),

    -- The analysed form of `content`, maintained by Postgres so it can never
    -- disagree with the text it came from. A trigger-maintained column can; a
    -- generated one cannot. rag.chunk_terms is derived from this, which is why
    -- the BM25 statistics and the stored text cannot drift apart either.
    --
    -- The redaction placeholders are stripped before analysis, and that is not
    -- cosmetic. `[CUSTOMER_NAME]` analyses to the lexemes `custom` and `name`,
    -- which then appear in every chunk that happened to contain a person's
    -- name — 394 of 1,311 here, 30% of the corpus. BM25 would treat `custom` as
    -- an ordinary corpus term with an ordinary IDF, so a question mentioning
    -- "customer" would score against redaction artefacts rather than content,
    -- and the documents it matched would be precisely the ones whose text had
    -- been removed. The placeholders stay in `content`, where they inform a
    -- human reader and the model; they are just not evidence about topic.
    content_tsv tsvector GENERATED ALWAYS AS (
        to_tsvector('english', regexp_replace(content, '\[[A-Z_]+\]', ' ', 'g'))
    ) STORED,

    CONSTRAINT chunks_ticket_seq_uniq UNIQUE (ticket_id, chunk_seq),
    CONSTRAINT chunks_word_count_positive CHECK (word_count > 0)
);

-- Migrate an index built before redaction stripping. A generated column's
-- expression cannot be altered in place, so the column is replaced and every
-- chunk marked for a lexical rebuild. Embeddings are untouched: the masked text
-- did not change, only what the lexical side is allowed to see, so this costs a
-- second rather than a re-embedding pass.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_attrdef d
        JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
        WHERE d.adrelid = 'rag.chunks'::regclass
          AND a.attname = 'content_tsv'
          AND pg_get_expr(d.adbin, d.adrelid) NOT LIKE '%regexp_replace%'
    ) THEN
        ALTER TABLE rag.chunks DROP COLUMN content_tsv;
        ALTER TABLE rag.chunks ADD COLUMN content_tsv tsvector GENERATED ALWAYS AS (
            to_tsvector('english', regexp_replace(content, '\[[A-Z_]+\]', ' ', 'g'))
        ) STORED;
        UPDATE rag.chunks SET doc_len = NULL;
        RAISE NOTICE 'rag.chunks.content_tsv rebuilt without redaction placeholders';
    END IF;
END
$$;


COMMENT ON TABLE rag.chunks IS
    'Masked support-ticket chunks with embeddings. Holds no unmasked identifier '
    'by construction; tests/test_pii_manifest.py is what keeps that true.';

-- HNSW rather than IVFFlat: IVFFlat needs a populated table before its lists can
-- be trained sensibly, so building it on an empty schema gives a degenerate
-- index that silently stays bad until someone reindexes. HNSW has no such
-- build-order trap.
--
-- Cosine, matching the query operator in retrieve.py. bge vectors are L2
-- normalised, which makes cosine and inner product rank identically — but the
-- operator class and the query operator must agree or the index is skipped and
-- the scan goes sequential, quietly.
CREATE INDEX IF NOT EXISTS chunks_embedding_hnsw_idx
    ON rag.chunks USING hnsw (embedding vector_cosine_ops);

-- The BM25 path in retrieve.py does not use this index — it scores from
-- rag.chunk_terms. It is kept because `content_tsv @@ to_tsquery(...)` remains
-- the natural way to spot-check the corpus from psql, and on 1,311 rows it
-- costs a couple of hundred kilobytes.
CREATE INDEX IF NOT EXISTS chunks_content_tsv_idx
    ON rag.chunks USING gin (content_tsv);


-- --------------------------------------------------------------------------
-- The inverted index BM25 scores from.
--
-- CONTRACTS.md calls for BM25, and stock Postgres has no BM25: `ts_rank_cd`
-- weights by term frequency and proximity but has no inverse document
-- frequency at all, so it cannot tell a rare order number from the word
-- "order". On a support corpus where every ticket says "order", that is the
-- difference between a lexical ranker that works and one that returns the
-- corpus in arbitrary order.
--
-- Materialising (chunk_id, lexeme, tf) makes real BM25 a plain SQL aggregation:
-- df is a COUNT over this table, dl is the sum of tf, and both come from the
-- same tsvector the text column generates. About 33,000 rows for this corpus.
-- The alternative — a third-party extension such as pg_search — is a large
-- dependency to carry for a corpus this size.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rag.chunk_terms (
    chunk_id text NOT NULL REFERENCES rag.chunks (chunk_id) ON DELETE CASCADE,
    lexeme   text NOT NULL,
    tf       int  NOT NULL CHECK (tf > 0),
    PRIMARY KEY (chunk_id, lexeme)
);

-- Document frequency is a COUNT over one lexeme, so this index is the one that
-- decides whether scoring is a lookup or a scan.
CREATE INDEX IF NOT EXISTS chunk_terms_lexeme_idx ON rag.chunk_terms (lexeme);

CREATE INDEX IF NOT EXISTS chunks_ticket_id_idx  ON rag.chunks (ticket_id);
CREATE INDEX IF NOT EXISTS chunks_created_ts_idx ON rag.chunks (created_ts);


-- Per-run detail for the indexer. Complements meta.pipeline_run_log rather than
-- duplicating it: that table's columns (dag_id, step, rows_in, rows_out) have
-- nowhere to put "how many chunks were skipped because their hash matched",
-- which is the number that tells you whether the skip logic is working.
CREATE TABLE IF NOT EXISTS rag.index_runs (
    run_id            uuid        PRIMARY KEY,
    started_at        timestamptz NOT NULL,
    completed_at      timestamptz,
    status            text        NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    source            text        NOT NULL,
    since_ts          timestamptz,
    embedding_model   text        NOT NULL,
    documents_read    int         NOT NULL DEFAULT 0,
    chunks_total      int         NOT NULL DEFAULT 0,
    chunks_embedded   int         NOT NULL DEFAULT 0,
    chunks_skipped    int         NOT NULL DEFAULT 0,
    chunks_deleted    int         NOT NULL DEFAULT 0,
    pii_masked_total  int         NOT NULL DEFAULT 0,
    -- Split out because the ratio is diagnostic: regex hits rising means the
    -- manifest is going stale, and the next unusual identifier is the one that
    -- gets through.
    pii_by_dictionary int         NOT NULL DEFAULT 0,
    pii_by_regex      int         NOT NULL DEFAULT 0,
    duration_ms       numeric,
    message           text
);


-- Retrieval quality over time. Written by `python -m meridian.rag.evaluate`.
-- Kept in the database rather than a JSON file so the ops dashboard can plot
-- the trend without a second storage story.
CREATE TABLE IF NOT EXISTS rag.eval_results (
    eval_run_id      uuid        NOT NULL,
    evaluated_at     timestamptz NOT NULL,
    strategy         text        NOT NULL,   -- 'hybrid' | 'vector' | 'lexical'
    question_id      text        NOT NULL,
    question         text        NOT NULL,
    expects_answer   boolean     NOT NULL,
    hit              boolean,                -- NULL for abstention questions
    reciprocal_rank  numeric,
    abstained        boolean,
    top_similarity   numeric,
    retrieved        jsonb       NOT NULL,
    PRIMARY KEY (eval_run_id, strategy, question_id)
);


-- --------------------------------------------------------------------------
-- The LLM's read on each ticket.
--
-- Lives in `rag` rather than `silver` because the vocabulary that produced it
-- is a model, not a source system: silver is cleaned source data, and a
-- prediction is neither. gold.fact_support_tickets joins it alongside the
-- source system's own `intent` and `sentiment`, which db/init/05_oltp_ddl.sql
-- marks as ground truth and never feeds to the model — so the fact table can
-- carry both and the mart can report how often they agree.
--
-- The schema invariant holds: every column here is either an identifier
-- already in `rag.chunks`, a label from a frozen CONTRACTS.md §9 vocabulary,
-- or provenance. No free text, so nothing to mask.
--
-- Empty is a valid state. Without an ANTHROPIC_API_KEY there is no model to
-- ask, `meridian.rag.enrich` writes nothing, and every ai_* column downstream
-- is NULL — which docs/governance/owners.yml states as the contract for this
-- table's consumers rather than something they discover.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rag.ticket_enrichment (
    ticket_id       text PRIMARY KEY,

    predicted_intent text CHECK (predicted_intent IN (
        'shipping_delay', 'refund_request', 'product_defect',
        'billing_question', 'account_access', 'return_process',
        'general_inquiry')),
    predicted_sentiment text CHECK (predicted_sentiment IN (
        'positive', 'neutral', 'negative')),

    -- The model's own stated confidence, not a calibrated probability. Kept
    -- because a low-confidence disagreement and a high-confidence one are
    -- different events, and named so nobody mistakes it for the latter.
    confidence      numeric CHECK (confidence IS NULL
                                   OR (confidence >= 0 AND confidence <= 1)),

    -- What the model was shown. The masked chunk text, hashed the same way
    -- rag.chunks hashes it, so re-running after a masking change re-enriches
    -- exactly the tickets whose input actually changed — the same skip-key
    -- reasoning as the indexer, for the same reason.
    content_hash    text        NOT NULL,
    model           text        NOT NULL,
    enriched_at     timestamptz NOT NULL DEFAULT now(),

    -- Set when the model returned something outside the vocabulary or nothing
    -- parseable. The row still exists, with null labels: "we asked and got
    -- nothing usable" and "we never asked" are different states, and a missing
    -- row cannot express the first.
    error           text
);

COMMENT ON TABLE rag.ticket_enrichment IS
    'LLM-predicted intent and sentiment per ticket. Scored against the source '
    'system''s own labels in gold.mart_support_health. Empty without an API key.';

CREATE INDEX IF NOT EXISTS ticket_enrichment_model_idx
    ON rag.ticket_enrichment (model);


-- Explicit grants for the tables above. ALTER DEFAULT PRIVILEGES in
-- db/init/04_roles.sql covers tables created after it runs — which is all of
-- these — but only for roles that existed when it ran. Stating them here means
-- a database initialised before the dbt_runner grant was added still ends up
-- correct on the next indexer run, rather than needing a manual repair.
GRANT SELECT ON ALL TABLES IN SCHEMA rag TO analytics_ro;
GRANT SELECT ON rag.ticket_enrichment TO dbt_runner;
