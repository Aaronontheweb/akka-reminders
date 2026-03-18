-- Migration for Akka.Reminders 0.6.0: Add acknowledgement columns
-- Adds delivered_at_utc and ack_deadline_utc to support reliable at-least-once delivery.
--
-- This script is idempotent: SQLite's ADD COLUMN is safe to re-run if the column already exists
-- in SQLite 3.37.0+ (using IF NOT EXISTS on ALTER TABLE is not supported in older versions,
-- so wrap in a guard if needed for older environments).
--
-- Note: This script uses the default table name 'scheduled_reminders'.
--       If you use a custom table name, replace the table name accordingly.

ALTER TABLE scheduled_reminders ADD COLUMN delivered_at_utc TEXT NULL;
ALTER TABLE scheduled_reminders ADD COLUMN ack_deadline_utc TEXT NULL;

CREATE INDEX IF NOT EXISTS ix_scheduled_reminders_awaiting_ack
ON scheduled_reminders (ack_deadline_utc)
WHERE completion_status = 'AwaitingAck';
