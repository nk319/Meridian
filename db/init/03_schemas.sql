-- 03 — schemas.
--
-- All seven warehouse schemas from CONTRACTS.md §1, created before 04 grants on
-- them. Bronze is deliberately absent: it lives only in object storage as
-- Parquet (§2), and anything needing SQL over it uses DuckDB against MinIO.

\connect warehouse

CREATE SCHEMA silver;
COMMENT ON SCHEMA silver IS 'Cleaned, typed, deduped. dbt''s only source. Written by the loader.';

CREATE SCHEMA gold_stg;
COMMENT ON SCHEMA gold_stg IS 'dbt staging views.';

CREATE SCHEMA gold_int;
COMMENT ON SCHEMA gold_int IS 'dbt intermediate models.';

CREATE SCHEMA gold;
COMMENT ON SCHEMA gold IS 'Dims, facts, marts. The dashboard reads only this.';

CREATE SCHEMA meta;
COMMENT ON SCHEMA meta IS 'Ops, data quality, watermarks, lineage.';

CREATE SCHEMA rag;
COMMENT ON SCHEMA rag IS 'Vector store, chunks, eval results. Written by the rag indexer.';

CREATE SCHEMA secure;
COMMENT ON SCHEMA secure IS 'PII. Physically separated so grants can exclude it.';
