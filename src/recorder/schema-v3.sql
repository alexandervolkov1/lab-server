-- Additive v2 -> v3 migration; historical sessions have audit disabled.
ALTER TABLE sessions ADD COLUMN audit_enabled INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sessions ADD COLUMN measurements_enabled INTEGER NOT NULL DEFAULT 1;
ALTER TABLE sessions ADD COLUMN audit_incomplete INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sessions ADD COLUMN audit_dropped_events TEXT NOT NULL DEFAULT '0';
CREATE TABLE audit_events (
 id INTEGER PRIMARY KEY,
 session_id INTEGER NOT NULL REFERENCES sessions(id),
 operation_id TEXT NOT NULL,
 sequence INTEGER NOT NULL,
 registered_at_ns INTEGER NOT NULL,
 kind TEXT NOT NULL,
 context_json TEXT NOT NULL,
 details_json TEXT NOT NULL,
 UNIQUE(session_id, operation_id, sequence)
) STRICT;
