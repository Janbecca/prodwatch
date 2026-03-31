-- Purpose: Record report generation failures for UI/debugging.
-- Adds `error_message` to `report` so /api/reports/create can mark failed with details.

ALTER TABLE report ADD COLUMN error_message TEXT;

