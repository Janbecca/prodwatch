-- Purpose: Persist LLM call metadata and raw input/output for debugging.
-- Must not break business flow if missing; router logs only when this table exists.

CREATE TABLE IF NOT EXISTS llm_call_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_type TEXT NOT NULL,
  provider TEXT NOT NULL,
  model TEXT,
  prompt_version TEXT,
  ok INTEGER NOT NULL,
  error_message TEXT,
  request_json TEXT,
  response_json TEXT,
  created_at DATETIME
);

CREATE INDEX IF NOT EXISTS idx_llm_call_log_task_time ON llm_call_log(task_type, created_at);

