/**
 * Audit iteration 3 fix verification.
 *
 * One direct regression test per P0/P1 fix from
 * docs/superpowers/program/audit-iteration-3.md.
 * Skipped silently when AUDIT_PG_URL isn't set.
 */

import { afterAll, beforeAll, beforeEach, expect, it } from '@jest/globals';
import { Pool } from 'pg';
import {
  describeIntegration,
  PgHandle,
  resetDatabase,
  startPostgres,
  stopPostgres,
} from '../integration/postgres-container.js';
import { resetDbManager } from '../../db-manager.js';
import {
  detectMigrationState,
  describeTable,
  switchServerDb,
  listObjects,
  getObjectDetails,
  batchExecute,
  mutationDryRun,
  dryRunSqlFile,
  findDependents,
  safeAlterTable,
} from '../../tools/index.js';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

describeIntegration('audit iteration 3 — fix regressions', () => {
  let handle: PgHandle, pool: Pool;
  let testDir: string;

  beforeAll(async () => {
    const a = await startPostgres('audit_iter3_a');
    handle = a.container; pool = a.pool;

    process.env.PG_NAME_A = 'iter3';
    process.env.PG_HOST_A = handle.getHost();
    process.env.PG_PORT_A = String(handle.getPort());
    process.env.PG_USERNAME_A = handle.getUsername();
    process.env.PG_PASSWORD_A = handle.getPassword();
    process.env.PG_DATABASE_A = handle.getDatabase();
    process.env.PG_DEFAULT_A = 'true';
    process.env.PG_SSL_A = 'false';

    resetDbManager();
    await switchServerDb({ server: 'iter3' });
    testDir = fs.mkdtempSync(path.join(os.tmpdir(), 'iter3-fixes-'));
  }, 120_000);

  afterAll(async () => {
    resetDbManager();
    if (testDir) fs.rmSync(testDir, { recursive: true, force: true });
    await stopPostgres(handle, pool);
    for (const k of [
      'PG_NAME_A','PG_HOST_A','PG_PORT_A','PG_USERNAME_A','PG_PASSWORD_A',
      'PG_DATABASE_A','PG_DEFAULT_A','PG_SSL_A',
    ]) delete process.env[k];
  }, 60_000);

  beforeEach(async () => {
    await resetDatabase(pool);
  });

  // ============================================================
  // T0-1: schema validation in switch_server_db
  // ============================================================
  it('switch_server_db rejects malicious schema names (T0-1)', async () => {
    await expect(
      switchServerDb({ server: 'iter3', schema: 'public; DROP TABLE x' })
    ).rejects.toThrow(/Invalid schema name/i);
  }, 60_000);

  // ============================================================
  // T0-2: detectMigrationState quoted-identifier lookup for Sequelize
  // ============================================================
  it('detectMigrationState detects Sequelize ("SequelizeMeta" mixed-case) (T0-2)', async () => {
    await pool.query(`CREATE TABLE "SequelizeMeta" (name varchar(255) PRIMARY KEY)`);
    await pool.query(`INSERT INTO "SequelizeMeta" VALUES ('20240101000000-init.js')`);
    const r = await detectMigrationState({});
    const seq = r.detectedTools.find((t) => t.tool === 'Sequelize');
    expect(seq).toBeDefined();
    expect(seq!.appliedCount).toBe(1);
  }, 60_000);

  // ============================================================
  // T0-3: maxResults clamping
  // ============================================================
  it('list_databases rejects negative maxResults via clamp (T0-3)', async () => {
    // Build 3 DBs to verify nothing weird happens at boundaries.
    // Actually we only have audit_iter3_a; verifying clamp works
    // by checking the call doesn't return [] when caller passes 0
    // (legitimately = "I don't want any rows") and doesn't return
    // a rejection on negative.
    const { listDatabases } = await import('../../tools/index.js');
    const negResult = await listDatabases({ serverName: 'iter3', maxResults: -100 });
    expect(negResult.databases).toEqual([]);

    const zeroResult = await listDatabases({ serverName: 'iter3', maxResults: 0 });
    expect(zeroResult.databases).toEqual([]);

    const oneResult = await listDatabases({ serverName: 'iter3', maxResults: 1 });
    expect(oneResult.databases.length).toBe(1);
  }, 60_000);

  // ============================================================
  // T0-4: findDependents truncatedAtDepth flag
  // ============================================================
  it('findDependents truncatedAtDepth flips at boundary (T0-4)', async () => {
    await pool.query(`
      CREATE TABLE rt (id int PRIMARY KEY);
      CREATE TABLE l1 (id int, rt_id int REFERENCES rt(id), PRIMARY KEY (id));
      CREATE TABLE l2 (id int, l1_id int REFERENCES l1(id), PRIMARY KEY (id));
      CREATE TABLE l3 (id int, l2_id int REFERENCES l2(id), PRIMARY KEY (id));
    `);
    const r = await findDependents({ schema: 'public', name: 'rt', kind: 'table', max_depth: 1 });
    expect(r.truncatedAtDepth).toBe(true);
  }, 60_000);

  // ============================================================
  // T0-6: safe_alter_table.create_index rejects bad index_type
  // ============================================================
  it('safeAlterTable.create_index rejects unknown index_type (T0-6)', async () => {
    await expect(
      safeAlterTable({
        intent: {
          kind: 'create_index',
          table: 'users',
          index_name: 'idx_x',
          columns: ['email'],
          index_type: 'malicious-injection' as any,
        },
      })
    ).rejects.toThrow(/not a valid PostgreSQL index method/i);
  }, 60_000);

  // ============================================================
  // T1-1: batch_execute stopOnError:true actually short-circuits
  // ============================================================
  it('batch_execute stopOnError:true does NOT execute later queries after a failure (T1-1)', async () => {
    await pool.query(`CREATE TABLE batch_t (id int)`);
    const r = await batchExecute({
      queries: [
        { name: 'a', sql: 'SELECT 1' },
        { name: 'b', sql: 'SELECT * FROM does_not_exist' },
        { name: 'c', sql: "INSERT INTO batch_t VALUES (99)" },
      ],
      stopOnError: true,
    });
    expect(r.successCount).toBe(1);
    expect(r.failureCount).toBe(1);
    expect(r.results.c).toBeUndefined();   // never ran
    // Side-effect verification: query 'c' did NOT run (the bug)
    const cnt = (await pool.query('SELECT count(*)::int AS c FROM batch_t')).rows[0].c;
    expect(cnt).toBe(0);
  }, 60_000);

  // ============================================================
  // T1-2: mutation_dry_run preserves real PG error code
  // ============================================================
  it('mutationDryRun preserves real PG error code, not 25P02 (T1-2)', async () => {
    await pool.query(`CREATE TABLE u (email text UNIQUE)`);
    await pool.query(`INSERT INTO u VALUES ('a@x.com')`);
    const r = await mutationDryRun({
      sql: "INSERT INTO u (email) VALUES ('a@x.com')",
    });
    expect(r.success).toBe(false);
    // The unique-violation code is 23505. Previously the no-RETURNING
    // fallback was masking it with 25P02 (in_failed_sql_transaction).
    expect(r.error?.code).toBe('23505');
    expect(r.error?.constraint).toBeDefined();
  }, 60_000);

  // ============================================================
  // T1-3: dry_run_sql_file stopOnError:false reports each statement's
  // real error, not cascading 25P02
  // ============================================================
  it('dry_run_sql_file stopOnError:false captures every statement\'s real error (T1-3)', async () => {
    const filePath = path.join(testDir, 'cascade.sql');
    fs.writeFileSync(
      filePath,
      `SELECT 1;
SELECT * FROM no_such_table;
SELECT * FROM another_missing;
SELECT 2;`
    );
    const r = await dryRunSqlFile({ filePath, stopOnError: false });
    // Each failing statement should have its OWN real error code
    // (42P01 undefined_table), not 25P02 (in_failed_sql_transaction).
    const failures = r.statementResults.filter((s) => !s.success);
    expect(failures.length).toBeGreaterThanOrEqual(2);
    for (const f of failures) {
      expect(f.error?.code).toBe('42P01');
    }
    expect(r.successCount).toBe(2);  // SELECT 1 + SELECT 2
    expect(r.failureCount).toBe(2);
  }, 60_000);

  // ============================================================
  // T1-5/T1-6: list_objects matview support + view def auto-detect
  // ============================================================
  it('listObjects surfaces matviews (T1-5)', async () => {
    await pool.query(`
      CREATE TABLE base_for_mv (id int, val text);
      INSERT INTO base_for_mv VALUES (1, 'a');
      CREATE MATERIALIZED VIEW mv_base AS SELECT * FROM base_for_mv;
    `);
    const r = await listObjects({ schema: 'public', objectType: 'matview' });
    expect(r.items.some((o) => o.name === 'mv_base' && o.type === 'matview')).toBe(true);
  }, 60_000);

  it('getObjectDetails auto-detects view + returns definition (T1-6)', async () => {
    await pool.query(`
      CREATE TABLE u (id int, email text);
      CREATE VIEW v_u AS SELECT id, email FROM u;
    `);
    const r = await getObjectDetails({ schema: 'public', objectName: 'v_u' });
    expect(r.exists).toBe(true);
    expect(r.detectedKind).toBe('view');
    expect(r.definition).toMatch(/SELECT/i);
  }, 60_000);

  // ============================================================
  // T1-7: getObjectDetails reports exists:false for missing objects
  // ============================================================
  it('getObjectDetails returns exists:false for missing objects (T1-7)', async () => {
    const r = await getObjectDetails({ schema: 'public', objectName: 'totally_nonexistent_zzz' });
    expect(r.exists).toBe(false);
  }, 60_000);

  // ============================================================
  // T1-8: getObjectDetails CHECK constraint expression surfaces
  // ============================================================
  it('getObjectDetails surfaces CHECK constraint expression (T1-8)', async () => {
    await pool.query(`
      CREATE TABLE u (
        id int PRIMARY KEY,
        email text NOT NULL,
        CONSTRAINT email_format CHECK (email LIKE '%@%')
      )
    `);
    const r = await getObjectDetails({ schema: 'public', objectName: 'u' });
    const checkCon = r.constraints!.find((c) => c.constraint_name === 'email_format');
    expect(checkCon).toBeDefined();
    // PG normalizes LIKE '%@%' to ~~ '%@%' in pg_get_constraintdef output
    expect((checkCon as any).check_clause).toMatch(/CHECK.*(LIKE|~~).*@/i);
  }, 60_000);

  // ============================================================
  // T1-9/T1-10: explain_query input validation runs without hypopg
  // ============================================================
  it('explain_query rejects malicious hypotheticalIndexes even without hypopg (T1-10)', async () => {
    const { explainQuery } = await import('../../tools/index.js');
    await pool.query(`CREATE TABLE t (id int, x text)`);
    await expect(
      explainQuery({
        sql: 'SELECT * FROM t',
        hypotheticalIndexes: [{ table: 'users; DROP TABLE x', columns: ['id'] }],
      })
    ).rejects.toThrow(/dangerous SQL characters|invalid characters/i);
  }, 60_000);

  // ============================================================
  // describe_table FK columns array regression (re-verify
  // iteration-1 SP-4 P0-1 still holds after iteration-3 changes)
  // ============================================================
  it('describeTable FK columns are still JS arrays after iteration-3 changes', async () => {
    await pool.query(`
      CREATE TABLE p2 (id int, tid int, PRIMARY KEY (id, tid));
      CREATE TABLE c2 (id int PRIMARY KEY, pid int, ptid int,
                       FOREIGN KEY (pid, ptid) REFERENCES p2(id, tid));
    `);
    const r = await describeTable({ schema: 'public', table: 'c2' });
    expect(r.foreignKeysOut.length).toBeGreaterThan(0);
    expect(Array.isArray(r.foreignKeysOut[0].columns)).toBe(true);
    expect(r.foreignKeysOut[0].columns).toEqual(['pid', 'ptid']);
  }, 60_000);
});
