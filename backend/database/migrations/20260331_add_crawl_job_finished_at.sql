-- Purpose: Align crawl_job schema with the "refresh main loop" lifecycle fields.
-- Adds `finished_at` (alias-like) to complement existing `ended_at`.
-- Safe to run multiple times only if your migration runner guards; SQLite itself
-- does not support IF NOT EXISTS for ADD COLUMN in older versions.

ALTER TABLE crawl_job ADD COLUMN finished_at DATETIME;

-- Optional backfill: keep finished_at consistent with ended_at for existing rows.
UPDATE crawl_job
SET finished_at = ended_at
WHERE finished_at IS NULL AND ended_at IS NOT NULL;

