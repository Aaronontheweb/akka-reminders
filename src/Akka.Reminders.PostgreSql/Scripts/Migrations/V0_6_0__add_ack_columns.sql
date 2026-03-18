-- Migration for Akka.Reminders 0.6.0: Add acknowledgement columns
-- Adds delivered_at_utc and ack_deadline_utc to support reliable at-least-once delivery.
--
-- This script is idempotent: IF NOT EXISTS guards prevent errors on repeated runs.
--
-- Note: This script uses the default schema 'reminders' and table 'scheduled_reminders'.
--       If you use a custom schema or table name, replace them accordingly.

ALTER TABLE reminders.scheduled_reminders ADD COLUMN IF NOT EXISTS delivered_at_utc TIMESTAMP WITH TIME ZONE NULL;
ALTER TABLE reminders.scheduled_reminders ADD COLUMN IF NOT EXISTS ack_deadline_utc TIMESTAMP WITH TIME ZONE NULL;

CREATE INDEX IF NOT EXISTS ix_scheduled_reminders_awaiting_ack
ON reminders.scheduled_reminders (ack_deadline_utc)
WHERE completion_status = 'AwaitingAck';
