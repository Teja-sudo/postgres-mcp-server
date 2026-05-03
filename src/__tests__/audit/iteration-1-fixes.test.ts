/**
 * Audit iteration 1 fix verification.
 *
 * One direct regression test per fix from
 * docs/superpowers/program/audit-iteration-1.md.
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
  describeTable,
  exportToSqlFile,
  generateSeedData,
  detectMigrationState,
  lockCheck,
  schemaDiff,
  switchServerDb,
  transferObjects,
  findDependents,
} from '../../tools/index.js';
import { buildComplexSchema } from './complex-schema.js';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

describeIntegration('audit iteration 1 — fix regressions', () => {
  let handleA: PgHandle, poolA: Pool;
  let handleB: PgHandle, poolB: Pool;
  let testDir: string;

  beforeAll(async () => {
    const a = await startPostgres('audit_iter1_a');
    const b = await startPostgres('audit_iter1_b');
    handleA = a.container; poolA = a.pool;
    handleB = b.container; poolB = b.pool;

    process.env.PG_NAME_A = 'iterA';
    process.env.PG_HOST_A = handleA.getHost();
    process.env.PG_PORT_A = String(handleA.getPort());
    process.env.PG_USERNAME_A = handleA.getUsername();
    process.env.PG_PASSWORD_A = handleA.getPassword();
    process.env.PG_DATABASE_A = handleA.getDatabase();
    process.env.PG_DEFAULT_A = 'true';
    process.env.PG_SSL_A = 'false';

    process.env.PG_NAME_B = 'iterB';
    process.env.PG_HOST_B = handleB.getHost();
    process.env.PG_PORT_B = String(handleB.getPort());
    process.env.PG_USERNAME_B = handleB.getUsername();
    process.env.PG_PASSWORD_B = handleB.getPassword();
    process.env.PG_DATABASE_B = handleB.getDatabase();
    process.env.PG_SSL_B = 'false';

    resetDbManager();
    await switchServerDb({ server: 'iterA' });

    testDir = fs.mkdtempSync(path.join(os.tmpdir(), 'iter1-fixes-'));
  }, 180_000);

  afterAll(async () => {
    resetDbManager();
    if (testDir) fs.rmSync(testDir, { recursive: true, force: true });
    await stopPostgres(handleA, poolA);
    await stopPostgres(handleB, poolB);
    for (const k of [
      'PG_NAME_A','PG_HOST_A','PG_PORT_A','PG_USERNAME_A','PG_PASSWORD_A','PG_DATABASE_A','PG_DEFAULT_A','PG_SSL_A',
      'PG_NAME_B','PG_HOST_B','PG_PORT_B','PG_USERNAME_B','PG_PASSWORD_B','PG_DATABASE_B','PG_SSL_B',
    ]) delete process.env[k];
  }, 60_000);

  // ============================================================
  // SP-6 P0 fix: generateSeedData precedence bug
  // ============================================================
  it('SP-6: generateSeedData no longer crashes with cast type bool to oid[]', async () => {
    await resetDatabase(poolA);
    await poolA.query(`CREATE TABLE seed_t (id serial PRIMARY KEY, name text NOT NULL UNIQUE)`);
    const r = await generateSeedData({
      table: 'seed_t', count: 50, apply: true,
    });
    expect(r.rowsApplied).toBe(50);
    const c = await poolA.query('SELECT count(*)::int AS c FROM seed_t');
    expect(c.rows[0].c).toBe(50);
  }, 60_000);

  // ============================================================
  // SP-4 P0 fix: array_agg name[] type → cast to ::text
  // ============================================================
  it('SP-4: describeTable returns FK columns as JS arrays, not literal strings', async () => {
    await resetDatabase(poolA);
    await poolA.query(`
      CREATE TABLE parent (id int, tenant_id int, PRIMARY KEY (id, tenant_id));
      CREATE TABLE child (id int PRIMARY KEY, parent_id int, parent_tenant_id int,
                          FOREIGN KEY (parent_id, parent_tenant_id) REFERENCES parent(id, tenant_id));
    `);
    const result = await describeTable({ schema: 'public', table: 'child' });
    expect(result.foreignKeysOut.length).toBeGreaterThanOrEqual(1);
    const fk = result.foreignKeysOut.find((f) => f.referencedTable.includes('parent'))!;
    expect(Array.isArray(fk.columns)).toBe(true);
    expect(fk.columns).toEqual(['parent_id', 'parent_tenant_id']);
    expect(Array.isArray(fk.referencedColumns)).toBe(true);
  }, 60_000);

  // ============================================================
  // SP-2/SP-3 P0 fix: extension-owned objects filtered
  // ============================================================
  it('SP-2/3: schema_dump does not re-emit extension-owned functions', async () => {
    await resetDatabase(poolA);
    await poolA.query(`CREATE EXTENSION IF NOT EXISTS pgcrypto`);
    await poolA.query(`CREATE TABLE t1 (id uuid DEFAULT gen_random_uuid())`);
    const filePath = path.join(testDir, 'ext-filter.sql');
    await exportToSqlFile({
      filePath, mode: 'overwrite', what: { kind: 'schema_dump', schema: 'public' },
    });
    const sql = fs.readFileSync(filePath, 'utf-8');
    // Should mention CREATE EXTENSION but NOT redefine pgcrypto's functions
    expect(sql).toContain('CREATE EXTENSION IF NOT EXISTS "pgcrypto"');
    expect(sql).not.toMatch(/CREATE OR REPLACE FUNCTION (?:"public"\.)?"?armor"?/i);
    expect(sql).not.toMatch(/CREATE OR REPLACE FUNCTION (?:"public"\.)?"?gen_random_uuid"?/i);
  }, 60_000);

  // ============================================================
  // SP-2/SP-3 P0 fix: serial-owned sequences filtered
  // ============================================================
  it('SP-2: schema_dump does not double-emit serial-backed sequences', async () => {
    await resetDatabase(poolA);
    await poolA.query(`CREATE TABLE counter (id serial PRIMARY KEY, name text)`);
    const filePath = path.join(testDir, 'serial-filter.sql');
    await exportToSqlFile({
      filePath, mode: 'overwrite', what: { kind: 'schema_dump', schema: 'public' },
    });
    const sql = fs.readFileSync(filePath, 'utf-8');
    // The table CREATE should use `serial`, NOT a separate CREATE SEQUENCE
    expect(sql).toContain('serial');
    expect(sql).not.toMatch(/CREATE SEQUENCE IF NOT EXISTS "public"\."counter_id_seq"/);
  }, 60_000);

  // ============================================================
  // SP-2/SP-3 P0 fix: IDENTITY/GENERATED columns excluded from INSERT
  // ============================================================
  it('SP-2: data export skips IDENTITY and GENERATED columns', async () => {
    await resetDatabase(poolA);
    await poolA.query(`
      CREATE TABLE gid (
        id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        a int NOT NULL,
        b int NOT NULL,
        sum int GENERATED ALWAYS AS (a + b) STORED
      );
      INSERT INTO gid (a, b) VALUES (1, 2), (3, 4);
    `);
    const filePath = path.join(testDir, 'gen-skip.sql');
    await exportToSqlFile({
      filePath, mode: 'overwrite', what: { kind: 'data', tables: ['gid'] },
    });
    const sql = fs.readFileSync(filePath, 'utf-8');
    expect(sql).toContain('INSERT INTO');
    // Column list should not contain "id" or "sum"
    const insertCols = /INSERT INTO[^(]+\(([^)]+)\)/.exec(sql)![1];
    expect(insertCols).not.toMatch(/\bid\b/);
    expect(insertCols).not.toMatch(/\bsum\b/);
    expect(insertCols).toMatch(/\ba\b/);
    expect(insertCols).toMatch(/\bb\b/);

    // Replay it on a fresh schema and confirm rows land cleanly
    await poolB.query(`DROP TABLE IF EXISTS gid`);
    await poolB.query(`
      CREATE TABLE gid (
        id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        a int NOT NULL,
        b int NOT NULL,
        sum int GENERATED ALWAYS AS (a + b) STORED
      );
    `);
    await poolB.query(sql);
    const r = await poolB.query('SELECT a, b, sum FROM gid ORDER BY id');
    expect(r.rows).toEqual([
      { a: 1, b: 2, sum: 3 },
      { a: 3, b: 4, sum: 7 },
    ]);
  }, 60_000);

  // ============================================================
  // SP-2/SP-3 P0 fix: sequence setval after data load
  // ============================================================
  it('SP-3: transfer with data syncs sequence state on target', async () => {
    await resetDatabase(poolA);
    await resetDatabase(poolB);
    await poolA.query(`CREATE TABLE seq_t (id serial PRIMARY KEY, name text)`);
    await poolA.query(`INSERT INTO seq_t (name) VALUES ('a'), ('b'), ('c'), ('d'), ('e')`);

    const r = await transferObjects({
      from: { server: 'iterA' },
      to: { server: 'iterB' },
      objects: [{ kind: 'table', name: 'seq_t' }],
      include: 'both',
      if_exists: 'error',
    });
    expect(r.applied).toBe(true);
    expect(r.rowsTransferred).toBe(5);

    // Insert a new row on the target — its id must be > 5 (no collision)
    const ins = await poolB.query(
      `INSERT INTO seq_t (name) VALUES ('f') RETURNING id`
    );
    expect(Number(ins.rows[0].id)).toBeGreaterThan(5);
  }, 90_000);

  // ============================================================
  // SP-3 P0 fix: if_exists: 'skip' actually skips data
  // ============================================================
  it('SP-3: if_exists=skip skips data when target table has rows', async () => {
    await resetDatabase(poolA);
    await resetDatabase(poolB);
    await poolA.query(`CREATE TABLE skip_t (id int PRIMARY KEY, n text)`);
    await poolA.query(`INSERT INTO skip_t VALUES (1, 'a'), (2, 'b')`);
    await poolB.query(`CREATE TABLE skip_t (id int PRIMARY KEY, n text)`);
    await poolB.query(`INSERT INTO skip_t VALUES (99, 'pre-existing')`);

    const r = await transferObjects({
      from: { server: 'iterA' },
      to: { server: 'iterB' },
      objects: [{ kind: 'table', name: 'skip_t' }],
      include: 'both',
      if_exists: 'skip',
    });

    // Target row count should still be 1 (the pre-existing row)
    const c = await poolB.query('SELECT count(*)::int AS c, MIN(id)::int AS m FROM skip_t');
    expect(c.rows[0].c).toBe(1);
    expect(c.rows[0].m).toBe(99);
    expect(r.warnings.some((w) => /Skipped data for/i.test(w))).toBe(true);
  }, 90_000);

  // ============================================================
  // SP-5 P0 fix: lockCheck handles BEGIN; <DDL>; COMMIT;
  // ============================================================
  it('SP-5: lockCheck recognizes DDL inside a BEGIN/COMMIT wrapper', async () => {
    const r = await lockCheck({
      sql: 'BEGIN; DROP TABLE users; COMMIT;',
      estimate_duration: false,
    });
    expect(r.detectedLockLevel).toBe('AccessExclusiveLock');
    expect(r.notes).toMatch(/DROP TABLE/i);
  }, 30_000);

  // ============================================================
  // SP-5 P0 fix: detectMigrationState column-shape verification
  // ============================================================
  it('SP-5: detectMigrationState does NOT false-positive on user table named `migrations`', async () => {
    await resetDatabase(poolA);
    // Create a user table named `migrations` with TOTALLY different columns
    await poolA.query(`CREATE TABLE migrations (status text, recipe_id int, applied_at timestamptz)`);
    const r = await detectMigrationState({});
    expect(r.detectedTools.find((t) => t.tool === 'TypeORM')).toBeUndefined();
    expect(r.notDetected).toContain('TypeORM');
  }, 60_000);

  it('SP-5: detectMigrationState detects Flyway when shape matches', async () => {
    await resetDatabase(poolA);
    await poolA.query(`
      CREATE TABLE flyway_schema_history (
        installed_rank int PRIMARY KEY,
        version varchar(50),
        description text NOT NULL,
        type varchar(20) NOT NULL,
        script varchar(1000) NOT NULL,
        checksum int,
        installed_by varchar(100),
        installed_on timestamp DEFAULT now(),
        execution_time int NOT NULL,
        success boolean NOT NULL
      );
      INSERT INTO flyway_schema_history (installed_rank, version, description, type, script, checksum, installed_by, execution_time, success)
      VALUES (1, '1.0', 'init', 'SQL', 'V1__init.sql', 0, 'me', 100, true);
    `);
    const r = await detectMigrationState({});
    const flyway = r.detectedTools.find((t) => t.tool === 'Flyway');
    expect(flyway).toBeDefined();
    expect(flyway!.appliedCount).toBe(1);
  }, 60_000);

  // ============================================================
  // SP-5 P0 fix: ADD COLUMN NOT NULL warning
  // ============================================================
  it('SP-5: lockCheck warns about ADD COLUMN NOT NULL with no default', async () => {
    const r = await lockCheck({
      sql: 'ALTER TABLE users ADD COLUMN status text NOT NULL',
      estimate_duration: false,
    });
    expect(r.warnings.some((w) => /fails immediately if the table has any rows/i.test(w))).toBe(true);
  }, 30_000);

  // ============================================================
  // SP-4 P0 fix: schema_diff column-type drift uses ALTER, not DROP+CREATE
  // ============================================================
  it('SP-4: schema_diff for column-type-only drift emits ALTER, not DROP TABLE', async () => {
    await resetDatabase(poolA);
    await resetDatabase(poolB);
    await poolA.query(`CREATE TABLE diff_t (id int, n varchar(255))`);
    await poolB.query(`CREATE TABLE diff_t (id int, n text)`);
    const r = await schemaDiff({
      source: { server: 'iterA' },
      target: { server: 'iterB' },
    });
    const mod = r.toModify.find((m) => m.name === 'diff_t');
    expect(mod).toBeDefined();
    expect(mod!.suggestedSql).toMatch(/ALTER TABLE/);
    expect(mod!.suggestedSql).toMatch(/ALTER COLUMN .* TYPE/);
    expect(mod!.suggestedSql).not.toMatch(/DROP TABLE/);
  }, 60_000);

  // ============================================================
  // SP-4 P0 fix: findDependents recurses through FK to dependent tables
  // ============================================================
  it('SP-4: findDependents reaches transitively dependent tables via FK', async () => {
    await resetDatabase(poolA);
    await poolA.query(`
      CREATE TABLE parent_p (id int PRIMARY KEY);
      CREATE TABLE child_c (id int PRIMARY KEY, parent_id int REFERENCES parent_p(id));
    `);
    const r = await findDependents({ schema: 'public', name: 'parent_p', kind: 'table' });
    expect(r.dependents.some((d) => d.name === 'child_c')).toBe(true);
  }, 60_000);

  // ============================================================
  // SP-4 P0 fix: findDependents filters TOAST and self-array-types
  // ============================================================
  it('SP-4: findDependents does not report TOAST or self-array-types', async () => {
    await resetDatabase(poolA);
    await poolA.query(`CREATE TABLE filter_t (id int PRIMARY KEY, big text)`);
    const r = await findDependents({ schema: 'public', name: 'filter_t', kind: 'table' });
    expect(r.dependents.every((d) => !d.name.startsWith('pg_toast_'))).toBe(true);
    expect(r.dependents.every((d) => !(d.kind === 'type' && d.name === 'filter_t'))).toBe(true);
  }, 60_000);

  // ============================================================
  // SP-4 P1 fix: describeTable surfaces GENERATED + IDENTITY +
  // column comments
  // ============================================================
  it('SP-4: describeTable flags GENERATED/IDENTITY columns and surfaces column comments', async () => {
    await resetDatabase(poolA);
    await poolA.query(`
      CREATE TABLE flagged_t (
        id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        a int NOT NULL,
        b int NOT NULL,
        sum int GENERATED ALWAYS AS (a + b) STORED
      );
      COMMENT ON COLUMN flagged_t.a IS 'first operand';
    `);
    const r = await describeTable({ schema: 'public', table: 'flagged_t' });
    const id = r.columns.find((c) => c.name === 'id')!;
    const sum = r.columns.find((c) => c.name === 'sum')!;
    const a = r.columns.find((c) => c.name === 'a')!;
    expect(id.generated).toBe('identity_always');
    expect(sum.generated).toBe('stored');
    expect(a.comment).toBe('first operand');
  }, 60_000);

  // ============================================================
  // SP-2 P0 fix: trigger disable around data section
  // ============================================================
  it('SP-2: schema_dump+include_data disables triggers around data load', async () => {
    await resetDatabase(poolA);
    await poolA.query(`
      CREATE TABLE src_t (id int PRIMARY KEY, n text);
      CREATE TABLE log_t (msg text);
      CREATE FUNCTION log_insert() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN INSERT INTO log_t VALUES ('inserted ' || NEW.id::text); RETURN NEW; END $$;
      CREATE TRIGGER trg AFTER INSERT ON src_t FOR EACH ROW EXECUTE FUNCTION log_insert();
      INSERT INTO src_t VALUES (1, 'a'), (2, 'b'), (3, 'c');
    `);
    const filePath = path.join(testDir, 'trig.sql');
    await exportToSqlFile({
      filePath, mode: 'overwrite',
      what: { kind: 'schema_dump', schema: 'public', include_data: true },
    });
    const sql = fs.readFileSync(filePath, 'utf-8');
    expect(sql).toContain("session_replication_role = 'replica'");
    expect(sql).toContain("session_replication_role = 'origin'");
    // Replay on a fresh DB
    await resetDatabase(poolB);
    await poolB.query(sql);
    const srcCount = (await poolB.query('SELECT count(*)::int AS c FROM src_t')).rows[0].c;
    const logCount = (await poolB.query('SELECT count(*)::int AS c FROM log_t')).rows[0].c;
    // Source had 3 rows + 3 log entries (from trigger). After replay,
    // src_t has 3 rows; log_t should have 3 too (the originals,
    // not 6 from a re-fired trigger).
    expect(srcCount).toBe(3);
    expect(logCount).toBe(3);
  }, 90_000);

  // ============================================================
  // SP-2 P0 fix: empty arrays render as '{}'::text[] (PG can cast)
  // ============================================================
  it('SP-2: empty array column round-trips through formatSqlLiteral', async () => {
    await resetDatabase(poolA);
    await poolA.query(`
      CREATE TABLE arr_t (id int PRIMARY KEY, tags text[] NOT NULL DEFAULT '{}');
      INSERT INTO arr_t VALUES (1, '{}'), (2, '{a,b}');
    `);
    const filePath = path.join(testDir, 'arr.sql');
    await exportToSqlFile({
      filePath, mode: 'overwrite', what: { kind: 'data', tables: ['arr_t'] },
    });
    const sql = fs.readFileSync(filePath, 'utf-8');
    expect(sql).toMatch(/'\{\}'::text\[\]/);
    // Replay
    await resetDatabase(poolB);
    await poolB.query(`CREATE TABLE arr_t (id int PRIMARY KEY, tags text[] NOT NULL DEFAULT '{}')`);
    await poolB.query(sql);
    const c = (await poolB.query('SELECT count(*)::int AS c FROM arr_t')).rows[0].c;
    expect(c).toBe(2);
  }, 60_000);
});
