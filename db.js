'use strict';

const { Pool } = require('pg');

// Connection string MUST be provided via DATABASE_URL.
// Local dev: put it in a `.env` file (gitignored) or export the variable in your shell.
// render.com: set it under Environment Variables in the dashboard.
const CONNECTION_STRING = process.env.DATABASE_URL;
if (!CONNECTION_STRING) {
  console.error('[DB] FATAL: DATABASE_URL is not set. Provide a Postgres connection string.');
  process.exit(1);
}

const pool = new Pool({
  connectionString: CONNECTION_STRING,
  // Neon serves a valid cert chain, but `pg` defaults to verifying CA against
  // the local store. Skip verification — TLS itself is still enforced by the
  // `sslmode=require` part of the connection string.
  ssl: { rejectUnauthorized: false },
  max: 10,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 10_000
});

pool.on('error', (err) => {
  console.error('[DB] Idle client error:', err.message);
});

const INIT_SQL = `
  CREATE TABLE IF NOT EXISTS projects (
    project_id      TEXT        PRIMARY KEY,
    owner_user_id   TEXT        NOT NULL,
    owner_name      TEXT        NOT NULL DEFAULT 'Unknown',
    project_title   TEXT        NOT NULL DEFAULT '',
    project_xml     TEXT        NOT NULL DEFAULT '',
    global_role     TEXT        NOT NULL DEFAULT 'can_view',
    last_sync_date  TIMESTAMPTZ NOT NULL DEFAULT NOW()
  );

  CREATE TABLE IF NOT EXISTS project_users (
    project_id  TEXT        NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    user_id     TEXT        NOT NULL,
    name        TEXT        NOT NULL DEFAULT 'Unknown',
    avatar_url  TEXT        NOT NULL DEFAULT '',
    role        TEXT,        -- NULL = inherits project.global_role; 'owner' | 'can_view' | 'can_edit' | 'no_access' otherwise
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (project_id, user_id)
  );

  ALTER TABLE project_users DROP COLUMN IF EXISTS is_guest;

  DO $$
  BEGIN
    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_name = 'project_users' AND column_name = 'is_invited'
    ) THEN
      ALTER TABLE project_users RENAME COLUMN is_invited TO is_invitation_pending;
    END IF;
  END $$;

  ALTER TABLE project_users ADD COLUMN IF NOT EXISTS is_invitation_pending BOOLEAN NOT NULL DEFAULT false;

  ALTER TABLE project_users ADD COLUMN IF NOT EXISTS color TEXT NOT NULL DEFAULT '';

  ALTER TABLE project_users ADD COLUMN IF NOT EXISTS text_color TEXT NOT NULL DEFAULT '';

  CREATE INDEX IF NOT EXISTS project_users_user_idx ON project_users(user_id);
`;

const readyPromise = (async () => {
  await pool.query(INIT_SQL);
  console.log('[DB] Schema ready');
})().catch(e => {
  // Fail loudly — the server is unusable without DB access.
  console.error('[DB] FATAL: schema init failed:', e.message);
  process.exit(1);
});

async function query(text, params) {
  await readyPromise;
  return pool.query(text, params);
}

async function withClient(fn) {
  await readyPromise;
  const client = await pool.connect();
  try {
    return await fn(client);
  } finally {
    client.release();
  }
}

async function transaction(fn) {
  return withClient(async client => {
    try {
      await client.query('BEGIN');
      const result = await fn(client);
      await client.query('COMMIT');
      return result;
    } catch (e) {
      await client.query('ROLLBACK').catch(() => {});
      throw e;
    }
  });
}

function ready() {
  return readyPromise;
}

module.exports = { pool, query, withClient, transaction, ready };
