-- Temporary resource transport, independent of saved posts; paired endpoint is shared.
CREATE TABLE IF NOT EXISTS inbox_resources (
  id TEXT PRIMARY KEY,
  library_id TEXT NOT NULL REFERENCES inbox_endpoints(library_id),
  fingerprint TEXT NOT NULL,
  channel_id TEXT NOT NULL,
  message_id TEXT NOT NULL,
  url TEXT NOT NULL,
  name TEXT NOT NULL,
  size INTEGER NOT NULL,
  state TEXT NOT NULL DEFAULT 'queued'
    CHECK(state IN ('queued','downloading','importing','waiting_version','imported','failed','cancelled')),
  error TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  UNIQUE(library_id, fingerprint)
);
CREATE INDEX IF NOT EXISTS idx_inbox_resource_queue
  ON inbox_resources(library_id, state, created_at);
CREATE INDEX IF NOT EXISTS idx_inbox_resource_expiry ON inbox_resources(expires_at);
