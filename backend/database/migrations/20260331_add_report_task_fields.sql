-- Purpose: Make report generation queryable as a "task".
-- Adds task-like fields that can be used by future async generation.

ALTER TABLE report ADD COLUMN trigger_type TEXT;
ALTER TABLE report ADD COLUMN started_at DATETIME;
ALTER TABLE report ADD COLUMN finished_at DATETIME;

-- Backfill for existing rows.
UPDATE report
SET trigger_type = COALESCE(trigger_type, 'manual')
WHERE trigger_type IS NULL;

UPDATE report
SET started_at = COALESCE(started_at, created_at)
WHERE started_at IS NULL AND created_at IS NOT NULL;

UPDATE report
SET finished_at = COALESCE(finished_at, updated_at)
WHERE finished_at IS NULL AND updated_at IS NOT NULL AND status IN ('success','failed','done','error');

