/**
 * Hotfix 3.0.3 regression tests.
 *
 * Two real-world bugs surfaced by a user against a multi-tenant
 * staging cluster:
 *
 *  1. list_databases aborted with "permission denied for database X"
 *     when ANY one database on the server lacked CONNECT for the
 *     listing role. The filter arg was also applied JS-side, after
 *     the per-row pg_database_size() call, so the filter couldn't
 *     spare the listing.
 *
 *  2. switch_server_db rejected digit-leading database names with
 *     "Invalid database name." even though PG accepts them.
 *
 * Skipped silently when AUDIT_PG_URL isn't set.
 */

import { afterAll, beforeAll, expect, it } from '@jest/globals';
import { Pool } from 'pg';
import {
  describeIntegration,
  PgHandle,
  startPostgres,
  stopPostgres,
} from '../integration/postgres-container.js';
import { resetDbManager, getDbManager } from '../../db-manager.js';
import {
  listDatabases,
  switchServerDb,
} from '../../tools/index.js';
import {
  validateDatabaseName,
  validateSchemaName,
  isValidDatabaseName,
  isValidSchemaName,
} from '../../db-manager/validation.js';

describeIntegration('hotfix 3.0.3 — staging multi-tenant regression', () => {
  let handle: PgHandle, pool: Pool;
  /** Non-superuser role used by the MCP. Lacks CONNECT on SECURED_DB. */
  const LIMITED_ROLE = 'hotfix303_limited';
  const LIMITED_PASSWORD = 'hotfix303_limited_pw';
  /** Database the LIMITED_ROLE can connect to (the "main" DB). */
  const MAIN_DB = 'hotfix303_main_db';
  /** Secondary database LIMITED_ROLE has NO CONNECT on (bug 1 trigger). */
  const SECURED_DB = 'secured_no_connect_db';
  /** Digit-leading database name (bug 2 trigger). */
  const DIGIT_LEADING_DB = '1188_tenant_db';

  beforeAll(async () => {
    // 1. Spin up the cluster handle. We use it only as an admin
    //    channel for provisioning — the MCP itself connects as the
    //    LIMITED_ROLE so privilege checks are real.
    const started = await startPostgres('hotfix303_seed');
    handle = started.container;
    pool = started.pool;

    const adminPool = new Pool({
      host: handle.getHost(),
      port: handle.getPort(),
      user: handle.getUsername(),
      password: handle.getPassword(),
      database: handle.getDatabase(),
      max: 1,
    });
    try {
      // Best-effort cleanup of residue from a prior failed run.
      await adminPool.query(`DROP DATABASE IF EXISTS "${SECURED_DB}"`).catch(() => {});
      await adminPool.query(`DROP DATABASE IF EXISTS "${DIGIT_LEADING_DB}"`).catch(() => {});
      await adminPool.query(`DROP DATABASE IF EXISTS "${MAIN_DB}"`).catch(() => {});
      await adminPool.query(`DROP ROLE IF EXISTS ${LIMITED_ROLE}`).catch(() => {});

      // 2. Non-superuser role for the MCP connection. Superusers
      //    bypass CONNECT checks, so the test would falsely pass
      //    with the audit_owner role.
      await adminPool.query(
        `CREATE ROLE ${LIMITED_ROLE} LOGIN PASSWORD '${LIMITED_PASSWORD}' NOSUPERUSER NOCREATEDB`
      );

      // 3. Three databases:
      //    - MAIN_DB:         owned by LIMITED_ROLE       → CONNECT OK
      //    - DIGIT_LEADING_DB: owned by LIMITED_ROLE      → CONNECT OK
      //    - SECURED_DB:      owned by audit_owner; PUBLIC + LIMITED_ROLE
      //                       have CONNECT revoked       → CONNECT DENIED
      await adminPool.query(`CREATE DATABASE "${MAIN_DB}" OWNER ${LIMITED_ROLE}`);
      await adminPool.query(`CREATE DATABASE "${DIGIT_LEADING_DB}" OWNER ${LIMITED_ROLE}`);
      await adminPool.query(`CREATE DATABASE "${SECURED_DB}"`);
      await adminPool.query(`REVOKE CONNECT ON DATABASE "${SECURED_DB}" FROM PUBLIC`);
      await adminPool.query(`REVOKE CONNECT ON DATABASE "${SECURED_DB}" FROM ${LIMITED_ROLE}`);
    } finally {
      await adminPool.end();
    }

    // 4. Configure the MCP to use LIMITED_ROLE @ MAIN_DB. listDatabases
    //    will then run as that non-superuser role and hit the exact
    //    permission_denied case the user reported.
    process.env.PG_NAME_H303 = 'hotfix303';
    process.env.PG_HOST_H303 = handle.getHost();
    process.env.PG_PORT_H303 = String(handle.getPort());
    process.env.PG_USERNAME_H303 = LIMITED_ROLE;
    process.env.PG_PASSWORD_H303 = LIMITED_PASSWORD;
    process.env.PG_DATABASE_H303 = MAIN_DB;
    process.env.PG_DEFAULT_H303 = 'true';
    process.env.PG_SSL_H303 = 'false';

    resetDbManager();
    await switchServerDb({ server: 'hotfix303' });
  }, 120_000);

  afterAll(async () => {
    resetDbManager();
    // Best-effort cleanup of the auxiliary databases + role.
    const adminPool = new Pool({
      host: handle.getHost(),
      port: handle.getPort(),
      user: handle.getUsername(),
      password: handle.getPassword(),
      database: handle.getDatabase(),
      max: 1,
    });
    try {
      await adminPool.query(`DROP DATABASE IF EXISTS "${SECURED_DB}"`).catch(() => {});
      await adminPool.query(`DROP DATABASE IF EXISTS "${DIGIT_LEADING_DB}"`).catch(() => {});
      await adminPool.query(`DROP DATABASE IF EXISTS "${MAIN_DB}"`).catch(() => {});
      await adminPool.query(`DROP ROLE IF EXISTS ${LIMITED_ROLE}`).catch(() => {});
    } finally {
      await adminPool.end();
    }
    await stopPostgres(handle, pool);
    for (const k of [
      'PG_NAME_H303','PG_HOST_H303','PG_PORT_H303','PG_USERNAME_H303',
      'PG_PASSWORD_H303','PG_DATABASE_H303','PG_DEFAULT_H303','PG_SSL_H303',
    ]) delete process.env[k];
  }, 60_000);

  // ============================================================
  // Bug 1: list_databases must NOT abort on permission_denied
  // ============================================================
  it('list_databases completes when a database denies CONNECT', async () => {
    // Before the fix, this call threw "permission denied for database X"
    // because pg_database_size() was called for every row in pg_database.
    const r = await listDatabases({ serverName: 'hotfix303' });

    // The listing should return *some* databases, INCLUDING the secured
    // one. The secured DB should have canConnect:false and size:null;
    // accessible DBs should have canConnect:true and a real size.
    expect(r.databases.length).toBeGreaterThan(0);

    const secured = r.databases.find((db) => db.name === SECURED_DB);
    expect(secured).toBeDefined();
    expect(secured!.canConnect).toBe(false);
    expect(secured!.size).toBeNull();

    const accessible = r.databases.find((db) => db.name === DIGIT_LEADING_DB);
    expect(accessible).toBeDefined();
    expect(accessible!.canConnect).toBe(true);
    expect(typeof accessible!.size).toBe('string');
  }, 60_000);

  it('list_databases filter is applied IN SQL (not after pg_database_size)', async () => {
    // Filter on a name that does NOT include the secured DB. Even if
    // the role lacks CONNECT elsewhere, the SQL-side ILIKE only invokes
    // pg_database_size on the matching row, and the listing completes
    // cleanly.
    const r = await listDatabases({
      serverName: 'hotfix303',
      filter: '1188',
    });
    expect(r.databases.map((d) => d.name)).toContain(DIGIT_LEADING_DB);
    // Secured DB must NOT match the filter, so it's not even listed.
    expect(r.databases.find((d) => d.name === SECURED_DB)).toBeUndefined();
  }, 60_000);

  // ============================================================
  // Bug 2: validators accept digit-leading PG identifiers
  // ============================================================
  it('validateDatabaseName accepts digit-leading names', () => {
    expect(() => validateDatabaseName('1188')).not.toThrow();
    expect(() => validateDatabaseName('42_tenant_db')).not.toThrow();
    expect(() => validateDatabaseName('2024q1_archive')).not.toThrow();
    expect(isValidDatabaseName('1188')).toBe(true);
    // Still rejects real injection patterns.
    expect(() => validateDatabaseName("db'name")).toThrow();
    expect(() => validateDatabaseName('db;DROP')).toThrow();
    expect(() => validateDatabaseName('db--comment')).toThrow();
  });

  it('validateSchemaName accepts digit-leading names', () => {
    expect(() => validateSchemaName('1schema')).not.toThrow();
    expect(() => validateSchemaName('2024q1')).not.toThrow();
    expect(isValidSchemaName('1schema')).toBe(true);
    // Still rejects real injection patterns / illegal chars.
    expect(() => validateSchemaName('schema;DROP')).toThrow();
    expect(() => validateSchemaName('my-schema')).toThrow();
  });

  it('switch_server_db succeeds with a digit-leading database name', async () => {
    // The user reported switching to the literal numeric name failed.
    // Real DB names also commonly include a digit-leading suffix; the
    // tool should be able to handle both shapes.
    const r = await switchServerDb({
      server: 'hotfix303',
      database: DIGIT_LEADING_DB,
    });
    expect(r.success).toBe(true);
    expect(r.currentDatabase).toBe(DIGIT_LEADING_DB);

    // Reset to the main DB for any later tests in the file.
    resetDbManager();
    await switchServerDb({ server: 'hotfix303' });
  }, 60_000);

  it('switch_server_db with purely numeric name passes validation', async () => {
    // Validation-only check — there's no DB literally named '1188' on
    // the cluster, so the actual switch fails at PG connect time. The
    // important thing is the error is a PG connect error, NOT
    // "Invalid database name."
    await expect(
      switchServerDb({ server: 'hotfix303', database: '1188' })
    ).rejects.toThrow(/Failed to switch/);
    // Verify the error wraps a PG-side failure, not a client-side
    // validation rejection.
    try {
      await switchServerDb({ server: 'hotfix303', database: '1188' });
    } catch (err) {
      const msg = String((err as Error).message);
      expect(msg).not.toMatch(/Invalid database name/i);
    }
    // Restore connection.
    resetDbManager();
    await switchServerDb({ server: 'hotfix303' });
  }, 60_000);
});
