-- Purpose: Per-task model/provider config.
-- Allows selecting provider/model per task_type, with fallback settings.

CREATE TABLE IF NOT EXISTS llm_task_config (
  task_type TEXT PRIMARY KEY,
  provider TEXT NOT NULL,
  model TEXT,
  fallback_provider TEXT NOT NULL DEFAULT 'mock',
  fallback_model TEXT,
  updated_at DATETIME
);

