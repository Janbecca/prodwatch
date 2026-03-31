-- Purpose: Trace report generation provenance (provider/model/prompt_version).
-- Adds optional columns to report table.
--
-- NOTE: SQLite ALTER TABLE ADD COLUMN is not idempotent; if a column already exists,
-- skip that statement manually.

ALTER TABLE report ADD COLUMN provider_name TEXT;
ALTER TABLE report ADD COLUMN model_name TEXT;
ALTER TABLE report ADD COLUMN prompt_version TEXT;
ALTER TABLE report ADD COLUMN generated_at DATETIME;
ALTER TABLE report ADD COLUMN raw_response TEXT;
-- error_message may already exist; if not, add it.
ALTER TABLE report ADD COLUMN error_message TEXT;

