/**
 * Postgres testcontainer harness
 *
 * Spins up a real PG instance per integration test suite. Opt-in: set
 * POSTGRES_INTEGRATION_TESTS=1 in the environment to run these tests
 * locally. CI sets it always. Without the env var, integration suites
 * use describe.skip and never touch Docker.
 */

import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { Pool } from 'pg';

/** True when integration tests are enabled (env var set). */
export const INTEGRATION_ENABLED = process.env.POSTGRES_INTEGRATION_TESTS === '1';

/**
 * Convenience wrapper: returns describe (run) when integration is on,
 * describe.skip (skip silently) otherwise.
 *
 * Usage:
 *   import { describeIntegration } from './postgres-container.js';
 *   describeIntegration('SP-1 dry-run trust', () => { ... });
 */
export const describeIntegration = INTEGRATION_ENABLED
  ? describe
  : describe.skip;

/**
 * Start a Postgres 16 container and connect a pg.Pool to it.
 * Returns the started container (for cleanup) and the pool.
 *
 * Caller is responsible for calling stopPostgres() in afterAll.
 */
export async function startPostgres(): Promise<{
  container: StartedPostgreSqlContainer;
  pool: Pool;
  connectionString: string;
}> {
  const container = await new PostgreSqlContainer('postgres:16-alpine')
    .withDatabase('postgres_mcp_test')
    .withUsername('test_user')
    .withPassword('test_password')
    .start();

  const connectionString = container.getConnectionUri();
  const pool = new Pool({
    connectionString,
    max: 5,
    idleTimeoutMillis: 5000,
  });

  return { container, pool, connectionString };
}

/** Tear down the container and pool. */
export async function stopPostgres(
  container: StartedPostgreSqlContainer,
  pool: Pool
): Promise<void> {
  try { await pool.end(); } catch { /* ignored */ }
  try { await container.stop(); } catch { /* ignored */ }
}

/**
 * Reset the database to a known clean state. Drops all tables in the
 * current `public` schema and re-creates the schema. Useful in
 * beforeEach to give each test a fresh slate without restarting the
 * container (which is slow).
 */
export async function resetDatabase(pool: Pool): Promise<void> {
  await pool.query(`
    DROP SCHEMA IF EXISTS public CASCADE;
    CREATE SCHEMA public;
    GRANT ALL ON SCHEMA public TO test_user;
    GRANT ALL ON SCHEMA public TO public;
  `);
}
