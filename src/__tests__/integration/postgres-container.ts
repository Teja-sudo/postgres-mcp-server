/**
 * Postgres test harness — adapter for two backends:
 *
 *   1. AUDIT cluster (preferred when AUDIT_PG_URL is set):
 *      Connects directly to a pre-existing PG instance. Used for the
 *      audit loop against a real, persistent cluster.
 *
 *   2. testcontainers (fallback when AUDIT_PG_URL is not set but
 *      POSTGRES_INTEGRATION_TESTS=1): spins up postgres:16-alpine.
 *      Used in CI on ubuntu-latest.
 *
 *   3. Skipped (default): no real PG → describe.skip.
 *
 * Tests call startPostgres() and stopPostgres(); both backends expose
 * the same surface (host/port/user/password/database getters) so the
 * test files don't need to know which backend they're on.
 */

import { Pool } from 'pg';
import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql';

/** Adapter interface — both testcontainers and audit cluster expose
 *  this minimal surface so the test code is backend-agnostic. */
export interface PgHandle {
  getHost(): string;
  getPort(): number;
  getUsername(): string;
  getPassword(): string;
  getDatabase(): string;
  getConnectionUri(): string;
  /** Stop and clean up. For audit cluster this is a no-op (the cluster
   *  is shared across tests). For testcontainers it stops the container. */
  stop(): Promise<void>;
}

const AUDIT_URL = process.env.AUDIT_PG_URL;

/** True when integration tests are enabled (audit cluster OR testcontainers). */
export const INTEGRATION_ENABLED =
  !!AUDIT_URL || process.env.POSTGRES_INTEGRATION_TESTS === '1';

export const describeIntegration = INTEGRATION_ENABLED
  ? describe
  : describe.skip;

/**
 * Start (or attach to) a Postgres backend and return a handle + pool +
 * connection string. When AUDIT_PG_URL is set, attaches to that
 * cluster (no container spin-up). Otherwise falls back to testcontainers.
 *
 * @param dbOverride - optional database name to use for this handle.
 *   When omitted, uses the database from AUDIT_PG_URL (audit) or the
 *   default container database.
 */
export async function startPostgres(
  dbOverride?: string
): Promise<{ container: PgHandle; pool: Pool; connectionString: string }> {
  if (AUDIT_URL) {
    return startAuditHandle(AUDIT_URL, dbOverride);
  }
  return startContainerHandle(dbOverride);
}

/** Stop helper — backend-agnostic. */
export async function stopPostgres(handle: PgHandle, pool: Pool): Promise<void> {
  try { await pool.end(); } catch { /* ignored */ }
  try { await handle.stop(); } catch { /* ignored */ }
}

/**
 * Reset the database to a known clean state. Drops the public schema
 * and recreates it. Beforehand, terminate any other backends connected
 * to this database (relevant only on the audit cluster where multiple
 * test runs share the same DB).
 */
export async function resetDatabase(pool: Pool): Promise<void> {
  await pool.query(`
    DROP SCHEMA IF EXISTS public CASCADE;
    CREATE SCHEMA public;
    GRANT ALL ON SCHEMA public TO public;
  `);
}

// =============================================================
// Audit-cluster backend
// =============================================================

interface ParsedAuditUrl {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
}

function parseAuditUrl(url: string): ParsedAuditUrl {
  // Accept both URL form and key=value form
  if (url.startsWith('postgres://') || url.startsWith('postgresql://')) {
    const u = new URL(url);
    return {
      host: u.hostname,
      port: Number(u.port || 5432),
      user: decodeURIComponent(u.username),
      password: decodeURIComponent(u.password),
      database: u.pathname.replace(/^\//, '') || 'postgres',
    };
  }
  // key=value parser
  const out: Record<string, string> = {};
  for (const part of url.split(/\s+/)) {
    const idx = part.indexOf('=');
    if (idx === -1) continue;
    out[part.slice(0, idx)] = part.slice(idx + 1);
  }
  return {
    host: out.host || '127.0.0.1',
    port: Number(out.port || 5432),
    user: out.user || 'postgres',
    password: out.password || '',
    database: out.dbname || out.database || 'postgres',
  };
}

class AuditHandle implements PgHandle {
  constructor(private readonly parsed: ParsedAuditUrl) {}
  getHost(): string { return this.parsed.host; }
  getPort(): number { return this.parsed.port; }
  getUsername(): string { return this.parsed.user; }
  getPassword(): string { return this.parsed.password; }
  getDatabase(): string { return this.parsed.database; }
  getConnectionUri(): string {
    const u = encodeURIComponent(this.parsed.user);
    const p = encodeURIComponent(this.parsed.password);
    return `postgres://${u}:${p}@${this.parsed.host}:${this.parsed.port}/${this.parsed.database}`;
  }
  async stop(): Promise<void> {
    // No-op for audit cluster - it's shared across tests and torn down
    // externally (audit-cluster.sh stop or pg_ctl stop).
  }
}

async function startAuditHandle(
  url: string,
  dbOverride?: string
): Promise<{ container: PgHandle; pool: Pool; connectionString: string }> {
  const parsed = parseAuditUrl(url);
  const targetDb = dbOverride ?? parsed.database;

  // Ensure the target database exists on the audit cluster. We connect
  // to the maintenance DB first, CREATE DATABASE if needed, then connect
  // to the target. This makes each test file independent — name your DB
  // and the harness will provision it.
  if (dbOverride && dbOverride !== parsed.database) {
    const adminPool = new Pool({
      host: parsed.host,
      port: parsed.port,
      user: parsed.user,
      password: parsed.password,
      database: parsed.database, // 'audit_db' or whatever the URL points to
      max: 1,
      connectionTimeoutMillis: 10000,
    });
    try {
      const exists = await adminPool.query(
        'SELECT 1 FROM pg_database WHERE datname = $1',
        [dbOverride]
      );
      if (exists.rowCount === 0) {
        // CREATE DATABASE doesn't accept parameters; identifier must be
        // safe — restrict to [a-zA-Z0-9_] for safety.
        if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(dbOverride)) {
          throw new Error(`Unsafe database name: ${dbOverride}`);
        }
        await adminPool.query(`CREATE DATABASE "${dbOverride}"`);
      }
    } finally {
      await adminPool.end();
    }
  }

  parsed.database = targetDb;
  const handle = new AuditHandle(parsed);
  const connectionString = handle.getConnectionUri();
  const pool = new Pool({
    host: parsed.host,
    port: parsed.port,
    user: parsed.user,
    password: parsed.password,
    database: parsed.database,
    max: 5,
    idleTimeoutMillis: 5000,
    connectionTimeoutMillis: 10000,
  });
  return { container: handle, pool, connectionString };
}

// =============================================================
// testcontainers backend
// =============================================================

class ContainerHandle implements PgHandle {
  constructor(private readonly c: StartedPostgreSqlContainer) {}
  getHost(): string { return this.c.getHost(); }
  getPort(): number { return this.c.getPort(); }
  getUsername(): string { return this.c.getUsername(); }
  getPassword(): string { return this.c.getPassword(); }
  getDatabase(): string { return this.c.getDatabase(); }
  getConnectionUri(): string { return this.c.getConnectionUri(); }
  async stop(): Promise<void> { await this.c.stop(); }
}

async function startContainerHandle(
  dbOverride?: string
): Promise<{ container: PgHandle; pool: Pool; connectionString: string }> {
  let builder = new PostgreSqlContainer('postgres:16-alpine')
    .withUsername('test_user')
    .withPassword('test_password');
  if (dbOverride) builder = builder.withDatabase(dbOverride);
  else builder = builder.withDatabase('postgres_mcp_test');
  const c = await builder.start();
  const handle = new ContainerHandle(c);
  const pool = new Pool({
    connectionString: handle.getConnectionUri(),
    max: 5,
    idleTimeoutMillis: 5000,
  });
  return { container: handle, pool, connectionString: handle.getConnectionUri() };
}
