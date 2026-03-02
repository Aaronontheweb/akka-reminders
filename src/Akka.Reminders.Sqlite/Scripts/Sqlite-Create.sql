-- Akka.Reminders SQLite Schema Creation Script
-- This script creates the table for storing reminders in SQLite.
--
-- Usage:
--   1. Review and modify the table name if needed (default: 'scheduled_reminders')
--   2. Execute this script against your SQLite database
--
-- Note: This script is idempotent - it will only create objects if they don't already exist.

CREATE TABLE IF NOT EXISTS scheduled_reminders (
    shard_region_name TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    reminder_key TEXT NOT NULL,
    when_utc TEXT NOT NULL,
    repeat_interval_ticks INTEGER NULL,
    serializer_id INTEGER NOT NULL,
    manifest TEXT NULL,
    payload BLOB NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_failure_reason TEXT NULL,
    is_completed INTEGER NOT NULL DEFAULT 0,
    completed_at_utc TEXT NULL,
    completion_status TEXT NOT NULL DEFAULT 'Pending',

    PRIMARY KEY (shard_region_name, entity_id, reminder_key)
);

CREATE INDEX IF NOT EXISTS ix_scheduled_reminders_due_reminders
ON scheduled_reminders (when_utc, shard_region_name, entity_id)
WHERE is_completed = 0;

CREATE INDEX IF NOT EXISTS ix_scheduled_reminders_cleanup
ON scheduled_reminders (completed_at_utc)
WHERE is_completed = 1;
