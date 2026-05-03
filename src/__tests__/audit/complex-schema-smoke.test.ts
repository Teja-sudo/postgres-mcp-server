/**
 * Smoke test that the complex-schema fixture builds cleanly against
 * a real PG instance. Skipped when the audit cluster isn't configured.
 */

import { afterAll, beforeAll, expect, it } from '@jest/globals';
import { Pool } from 'pg';
import { describeIntegration, startPostgres, stopPostgres } from '../integration/postgres-container.js';
import { buildComplexSchema } from './complex-schema.js';

describeIntegration('audit fixture: complex-schema builds cleanly', () => {
  let pool: Pool;
  let handle: { stop: () => Promise<void> };

  beforeAll(async () => {
    const started = await startPostgres('audit_complex_smoke');
    pool = started.pool;
    handle = started.container;
  }, 120_000);

  afterAll(async () => {
    if (handle && pool) {
      await stopPostgres(handle as any, pool);
    }
  }, 60_000);

  it('builds with seed and counts ≥ 1000 rows', async () => {
    const result = await buildComplexSchema(pool, { totalRows: 2000 });
    expect(result.tableCount).toBeGreaterThanOrEqual(6);
    expect(result.rowCount).toBeGreaterThan(1000);
    // Verify an indexed query returns sensibly
    const r = await pool.query(`SELECT count(*)::int AS c FROM users WHERE country = 'US'`);
    expect(r.rows[0].c).toBeGreaterThan(0);
  }, 120_000);

  it('fixture creates expected object kinds', async () => {
    // After previous test, schema should still exist
    const counts = await pool.query(`
      SELECT
        (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind='r') AS tables,
        (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind='v') AS views,
        (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind='m') AS matviews,
        (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.prokind='f') AS funcs,
        (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND p.prokind='p') AS procs,
        (SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND NOT t.tgisinternal) AS triggers,
        (SELECT count(*) FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname='public' AND t.typtype IN ('e','c') AND NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.reltype = t.oid AND c.relkind <> 'c')) AS types
    `);
    const c = counts.rows[0];
    expect(Number(c.tables)).toBeGreaterThanOrEqual(6);
    expect(Number(c.views)).toBeGreaterThanOrEqual(2);
    expect(Number(c.matviews)).toBeGreaterThanOrEqual(1);
    expect(Number(c.funcs)).toBeGreaterThanOrEqual(2);
    expect(Number(c.procs)).toBeGreaterThanOrEqual(1);
    expect(Number(c.triggers)).toBeGreaterThanOrEqual(2);
    expect(Number(c.types)).toBeGreaterThanOrEqual(1);
  }, 60_000);
});
