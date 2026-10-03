-- Derived from src/DiscordInboxSchema.ts; the schema contract test checks parity.
CREATE TABLE IF NOT EXISTS inbox_endpoints (
  library_id TEXT PRIMARY KEY,
  secret_hash TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  discord_user_id TEXT,
  is_default INTEGER NOT NULL DEFAULT 0 CHECK(is_default IN (0, 1)),
  paired_at INTEGER,
  revoked_at INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_inbox_default_user
  ON inbox_endpoints(discord_user_id)
  WHERE revoked_at IS NULL AND is_default = 1 AND discord_user_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS inbox_pair_codes (
  code_hash TEXT PRIMARY KEY,
  library_id TEXT NOT NULL REFERENCES inbox_endpoints(library_id),
  expires_at INTEGER NOT NULL,
  consumed_at INTEGER,
  claim_id TEXT,
  discord_user_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_inbox_pair_expiry ON inbox_pair_codes(expires_at);
CREATE TABLE IF NOT EXISTS inbox_deliveries (
  id TEXT PRIMARY KEY,
  handoff_token_hash TEXT NOT NULL,
  library_id TEXT REFERENCES inbox_endpoints(library_id),
  claimed_library_id TEXT,
  dedupe_scope TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  source_key_hash TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending', 'saved', 'waiting_binding')),
  title TEXT,
  created_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  acknowledged_at INTEGER,
  UNIQUE(dedupe_scope, fingerprint)
);
CREATE INDEX IF NOT EXISTS idx_inbox_delivery_queue
  ON inbox_deliveries(library_id, state, created_at);
CREATE INDEX IF NOT EXISTS idx_inbox_delivery_expiry ON inbox_deliveries(expires_at);
CREATE INDEX IF NOT EXISTS idx_inbox_delivery_source
  ON inbox_deliveries(library_id, state, source_key_hash);
CREATE TABLE IF NOT EXISTS inbox_delivery_links (
  token_hash TEXT PRIMARY KEY,
  delivery_id TEXT NOT NULL REFERENCES inbox_deliveries(id) ON DELETE CASCADE
);
