/**
 * Audit iteration 2 — cross-pack end-to-end pipeline.
 *
 * Builds a complex schema on src, runs every pack's tools against
 * it, transfers to dst, validates round-trip, exercises follow-on
 * tools on dst. The point is to confirm the packs compose: each
 * tool's output is a valid input to the next.
 *
 * Also serves as iteration 2's re-audit — if any iteration 1 fix
 * regressed something, the round-trip step will fail.
 */

import { afterAll, beforeAll, expect, it } from '@jest/globals';
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
  detectMigrationState,
  exportToSqlFile,
  findDependents,
  lockCheck,
  schemaDiff,
  switchServerDb,
  transferObjects,
  columnProfile,
  generateSeedData,
  safeAlterTable,
} from '../../tools/index.js';
import { buildComplexSchema } from './complex-schema.js';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

describeIntegration('audit iteration 2 — cross-pack E2E', () => {
  let srcHandle: PgHandle, srcPool: Pool;
  let dstHandle: PgHandle, dstPool: Pool;
  let testDir: string;

  beforeAll(async () => {
    const src = await startPostgres('audit_iter2_src');
    const dst = await startPostgres('audit_iter2_dst');
    srcHandle = src.container; srcPool = src.pool;
    dstHandle = dst.container; dstPool = dst.pool;

    process.env.PG_NAME_SRC = 'iterSrc';
    process.env.PG_HOST_SRC = srcHandle.getHost();
    process.env.PG_PORT_SRC = String(srcHandle.getPort());
    process.env.PG_USERNAME_SRC = srcHandle.getUsername();
    process.env.PG_PASSWORD_SRC = srcHandle.getPassword();
    process.env.PG_DATABASE_SRC = srcHandle.getDatabase();
    process.env.PG_DEFAULT_SRC = 'true';
    process.env.PG_SSL_SRC = 'false';

    process.env.PG_NAME_DST = 'iterDst';
    process.env.PG_HOST_DST = dstHandle.getHost();
    process.env.PG_PORT_DST = String(dstHandle.getPort());
    process.env.PG_USERNAME_DST = dstHandle.getUsername();
    process.env.PG_PASSWORD_DST = dstHandle.getPassword();
    process.env.PG_DATABASE_DST = dstHandle.getDatabase();
    process.env.PG_SSL_DST = 'false';

    resetDbManager();
    await switchServerDb({ server: 'iterSrc' });

    testDir = fs.mkdtempSync(path.join(os.tmpdir(), 'iter2-e2e-'));
  }, 240_000);

  afterAll(async () => {
    resetDbManager();
    if (testDir) fs.rmSync(testDir, { recursive: true, force: true });
    await stopPostgres(srcHandle, srcPool);
    await stopPostgres(dstHandle, dstPool);
    for (const k of [
      'PG_NAME_SRC','PG_HOST_SRC','PG_PORT_SRC','PG_USERNAME_SRC','PG_PASSWORD_SRC','PG_DATABASE_SRC','PG_DEFAULT_SRC','PG_SSL_SRC',
      'PG_NAME_DST','PG_HOST_DST','PG_PORT_DST','PG_USERNAME_DST','PG_PASSWORD_DST','PG_DATABASE_DST','PG_SSL_DST',
    ]) delete process.env[k];
  }, 60_000);

  it('full pipeline: build → describe → diff → transfer → re-diff (empty) → insert → profile', async () => {
    // Build complex schema on src + a small empty target
    await buildComplexSchema(srcPool, { totalRows: 3000 });
    await resetDatabase(dstPool);

    // === Pack SP-4 awareness: describeTable confirms no regressions
    const usersDesc = await describeTable({ schema: 'public', table: 'users' });
    expect(usersDesc.exists).toBe(true);
    expect(usersDesc.columns.length).toBeGreaterThanOrEqual(8);
    // FK columns are JS arrays (not literal strings) - SP-4 P0 fix
    if (usersDesc.foreignKeysIn.length > 0) {
      const inFk = usersDesc.foreignKeysIn[0];
      expect(Array.isArray(inFk.sourceColumns)).toBe(true);
      expect(Array.isArray(inFk.referencedColumns)).toBe(true);
    }
    // GENERATED + IDENTITY columns flagged (SP-4 P1 fix)
    const productsDesc = await describeTable({ schema: 'public', table: 'products' });
    const idCol = productsDesc.columns.find((c) => c.name === 'id')!;
    const ptw = productsDesc.columns.find((c) => c.name === 'price_with_tax')!;
    expect(idCol.generated).toBe('identity_always');
    expect(ptw.generated).toBe('stored');

    // === SP-4 findDependents: should reach across FK chain
    const tenantsDeps = await findDependents({ schema: 'public', name: 'tenants', kind: 'table' });
    expect(tenantsDeps.dependents.some((d) => d.name === 'users')).toBe(true);
    expect(tenantsDeps.dependents.some((d) => d.name === 'orders')).toBe(true);
    // No TOAST or self-array spam
    expect(tenantsDeps.dependents.every((d) => !d.name.startsWith('pg_toast_'))).toBe(true);
    expect(tenantsDeps.dependents.every((d) => !(d.kind === 'type' && d.name === 'tenants'))).toBe(true);

    // === SP-4 schemaDiff: src vs empty dst → toCreate populated
    const diffBefore = await schemaDiff({
      source: { server: 'iterSrc' },
      target: { server: 'iterDst' },
    });
    expect(diffBefore.toCreate.length).toBeGreaterThan(5);
    expect(diffBefore.toDrop.length).toBe(0);
    expect(diffBefore.migrationSql).toContain('CREATE TABLE');

    // === SP-3 transfer: full schema + data, src → dst
    const transferResult = await transferObjects({
      from: { server: 'iterSrc' },
      to: { server: 'iterDst' },
      objects: '*',
      include: 'both',
      if_exists: 'error',
    });
    expect(transferResult.applied).toBe(true);
    expect(transferResult.rowsTransferred).toBeGreaterThan(100);

    // === Re-diff: should be empty (or at most matview-state drift)
    const diffAfter = await schemaDiff({
      source: { server: 'iterSrc' },
      target: { server: 'iterDst' },
    });
    // Tables, views, matviews, sequences, types, functions: structural diff
    // should be empty after transfer.
    const tableCreates = diffAfter.toCreate.filter((o) => o.kind === 'table');
    const tableDrops = diffAfter.toDrop.filter((o) => o.kind === 'table');
    expect(tableCreates).toEqual([]);
    expect(tableDrops).toEqual([]);

    // === SP-3 fix verification: insert new row uses next sequence value
    // (no PK collision because setval was emitted post-data-load)
    const newTenant = await dstPool.query(
      `INSERT INTO tenants (slug, name) VALUES ('post-transfer', 'Post Transfer') RETURNING id`
    );
    const newId = Number(newTenant.rows[0].id);
    const maxIdR = await dstPool.query(`SELECT MAX(id)::int AS m FROM tenants`);
    expect(newId).toBe(Number(maxIdR.rows[0].m));

    // === SP-6 columnProfile on dst (proves data is real and types parsed correctly)
    const profile = await columnProfile({
      schema: 'public', table: 'users',
      columns: ['country', 'is_admin', 'email'],
      sample_threshold: 100_000_000,  // never sample for this test
      override_schema: 'public',
      server: 'iterDst',
    });
    const country = profile.profiles.find((p) => p.column === 'country')!;
    expect(country.distinctCount).toBeGreaterThan(0);
    expect(country.distinctCount).toBeLessThanOrEqual(6);
    expect(country.topValues!.length).toBeGreaterThan(0);

    // === SP-5 lockCheck on a hypothetical migration
    const lockResult = await lockCheck({
      sql: 'CREATE INDEX CONCURRENTLY idx_users_country ON users (country)',
      estimate_duration: false,
    });
    expect(lockResult.detectedLockLevel).toBe('ShareUpdateExclusiveLock');

    // === SP-5 lockCheck inside BEGIN/COMMIT (regression check)
    const wrapped = await lockCheck({
      sql: 'BEGIN; ALTER TABLE users ADD COLUMN nickname text; COMMIT;',
      estimate_duration: false,
    });
    expect(wrapped.detectedLockLevel).toBe('AccessExclusiveLock');

    // === SP-5 detectMigrationState (no migration tool here)
    const ms = await detectMigrationState({});
    expect(ms.detectedTools).toEqual([]);
    expect(ms.notDetected).toContain('Flyway');

    // === SP-5 safe_alter_table recipe is well-formed
    const recipe = await safeAlterTable({
      intent: {
        kind: 'create_index', table: 'users', index_name: 'idx_iter2_email',
        columns: ['email'], unique: false,
      },
    });
    expect(recipe.recipe.length).toBeGreaterThan(0);
    expect(recipe.scriptSql).toContain('CONCURRENTLY');
  }, 240_000);

  it('export → drop → replay round-trip preserves user data and matview content', async () => {
    // src already has the complex schema from the prior test
    const exportPath = path.join(testDir, 'roundtrip.sql');
    await switchServerDb({ server: 'iterSrc' });

    const ex = await exportToSqlFile({
      filePath: exportPath,
      mode: 'overwrite',
      what: { kind: 'schema_dump', schema: 'public', include_data: true },
    });
    expect(ex.objectsExported).toBeGreaterThan(5);
    expect(ex.rowsExported).toBeGreaterThan(100);

    // Wipe dst entirely and replay the file
    await resetDatabase(dstPool);
    const sql = fs.readFileSync(exportPath, 'utf-8');
    await dstPool.query(sql);

    // schema_diff between src and replayed dst should be empty
    const diff = await schemaDiff({
      source: { server: 'iterSrc' },
      target: { server: 'iterDst' },
    });
    expect(diff.toCreate.filter((o) => o.kind === 'table')).toEqual([]);
    expect(diff.toDrop.filter((o) => o.kind === 'table')).toEqual([]);

    // Verify trigger-fired audit_log has the same count on dst as src
    // (NOT 2× — that would mean triggers re-fired during replay)
    const srcCnt = (await srcPool.query('SELECT count(*)::int AS c FROM audit_log')).rows[0].c;
    const dstCnt = (await dstPool.query('SELECT count(*)::int AS c FROM audit_log')).rows[0].c;
    expect(dstCnt).toBe(srcCnt);

    // Sequence state is past max(id) so a fresh insert succeeds
    await dstPool.query(`INSERT INTO tenants (slug, name) VALUES ('round-trip-fresh', 'fresh')`);

    // Matview was refreshed
    const mvR = await dstPool.query('SELECT count(*)::int AS c FROM tenant_revenue');
    expect(Number(mvR.rows[0].c)).toBeGreaterThan(0);
  }, 300_000);

  it('generate_seed_data populates a fresh table on dst', async () => {
    await dstPool.query(`DROP TABLE IF EXISTS seed_e2e CASCADE`);
    await dstPool.query(`CREATE TABLE seed_e2e (id serial PRIMARY KEY, name text NOT NULL UNIQUE, n int NOT NULL)`);

    await switchServerDb({ server: 'iterDst' });
    const r = await generateSeedData({
      table: 'seed_e2e', count: 200, apply: true,
    });
    expect(r.rowsApplied).toBe(200);
    const c = (await dstPool.query('SELECT count(*)::int AS c FROM seed_e2e')).rows[0].c;
    expect(c).toBe(200);

    // Restore src as default for any subsequent tests
    await switchServerDb({ server: 'iterSrc' });
  }, 60_000);
});
