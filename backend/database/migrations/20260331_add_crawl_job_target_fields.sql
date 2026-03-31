-- Purpose: Make crawl_job_target traceable to project config and observable.
-- Adds:
-- - project_id: which project this target belongs to (denormalized from crawl_job)
-- - status: pending/running/success/failed
-- - created_at: when the target row was created
--
-- Note: SQLite cannot add FK constraints via ALTER TABLE easily; keep it simple.

ALTER TABLE crawl_job_target ADD COLUMN project_id INTEGER;
ALTER TABLE crawl_job_target ADD COLUMN status TEXT;
ALTER TABLE crawl_job_target ADD COLUMN created_at DATETIME;

-- Backfill project_id from crawl_job.project_id
UPDATE crawl_job_target
SET project_id = (
  SELECT project_id FROM crawl_job cj WHERE cj.id = crawl_job_target.crawl_job_id
)
WHERE project_id IS NULL;

-- Backfill created_at from crawl_job.started_at when available.
UPDATE crawl_job_target
SET created_at = (
  SELECT started_at FROM crawl_job cj WHERE cj.id = crawl_job_target.crawl_job_id
)
WHERE created_at IS NULL;

-- Default status for existing rows.
UPDATE crawl_job_target
SET status = 'success'
WHERE status IS NULL;

-- Helpful indexes for querying/debugging.
CREATE INDEX IF NOT EXISTS idx_crawl_job_target_crawl_job_id ON crawl_job_target(crawl_job_id);
CREATE INDEX IF NOT EXISTS idx_crawl_job_target_project_id ON crawl_job_target(project_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_crawl_job_target_combo ON crawl_job_target(crawl_job_id, platform_id, brand_id, keyword);

