/**
 * SP-3 integration tests — transfer_objects.
 *
 * Spins up TWO PG containers and validates schema/data transfer
 * between them. Skipped silently unless POSTGRES_INTEGRATION_TESTS=1.
 */

import { afterAll, beforeAll, beforeEach, expect, it } from '@jest/globals';
import { Pool } from 'pg';
import { StartedPostgreSqlContainer, PostgreSqlContainer } from '@testcontainers/postgresql';
import {
  describeIntegration,
  resetDatabase,
} from './postgres-container.js';

import { resetDbManager } from '../../db-manager.js';
import { transferObjects, switchServerDb } from '../../tools/index.js';

describeIntegration('SP-3 transfer_objects', () => {
  let srcContainer: StartedPostgreSqlContainer;
  let dstContainer: StartedPostgreSqlContainer;
  let srcPool: Pool;
  let dstPool: Pool;

  beforeAll(async () => {
    [srcContainer, dstContainer] = await Promise.all([
      new PostgreSqlContainer('postgres:16-alpine')
        .withDatabase('source_db').withUsername('su').withPassword('sp').start(),
      new PostgreSqlContainer('postgres:16-alpine')
        .withDatabase('target_db').withUsername('tu').withPassword('tp').start(),
    ]);

    srcPool = new Pool({ connectionString: srcContainer.getConnectionUri(), max: 5 });
    dstPool = new Pool({ connectionString: dstContainer.getConnectionUri(), max: 5 });

    process.env.PG_NAME_SRC = 'src';
    process.env.PG_HOST_SRC = srcContainer.getHost();
    process.env.PG_PORT_SRC = String(srcContainer.getPort());
    process.env.PG_USERNAME_SRC = srcContainer.getUsername();
    process.env.PG_PASSWORD_SRC = srcContainer.getPassword();
    process.env.PG_DATABASE_SRC = srcContainer.getDatabase();
    process.env.PG_DEFAULT_SRC = 'true';
    process.env.PG_SSL_SRC = 'false';

    process.env.PG_NAME_DST = 'dst';
    process.env.PG_HOST_DST = dstContainer.getHost();
    process.env.PG_PORT_DST = String(dstContainer.getPort());
    process.env.PG_USERNAME_DST = dstContainer.getUsername();
    process.env.PG_PASSWORD_DST = dstContainer.getPassword();
    process.env.PG_DATABASE_DST = dstContainer.getDatabase();
    process.env.PG_SSL_DST = 'false';

    resetDbManager();
    await switchServerDb({ server: 'src' });
  }, 180_000);

  afterAll(async () => {
    resetDbManager();
    await Promise.all([
      srcPool.end().catch(() => {}),
      dstPool.end().catch(() => {}),
    ]);
    await Promise.all([
      srcContainer.stop().catch(() => {}),
      dstContainer.stop().catch(() => {}),
    ]);
    for (const k of [
      'PG_NAME_SRC', 'PG_HOST_SRC', 'PG_PORT_SRC', 'PG_USERNAME_SRC',
      'PG_PASSWORD_SRC', 'PG_DATABASE_SRC', 'PG_DEFAULT_SRC', 'PG_SSL_SRC',
      'PG_NAME_DST', 'PG_HOST_DST', 'PG_PORT_DST', 'PG_USERNAME_DST',
      'PG_PASSWORD_DST', 'PG_DATABASE_DST', 'PG_SSL_DST',
    ]) delete process.env[k];
  }, 60_000);

  beforeEach(async () => {
    await Promise.all([resetDatabase(srcPool), resetDatabase(dstPool)]);
  });

  it('transfers a single table DDL between servers', async () => {
    await srcPool.query(`
      CREATE TABLE widgets (
        id serial PRIMARY KEY,
        name text NOT NULL UNIQUE,
        created_at timestamptz DEFAULT now()
      );
    `);

    const result = await transferObjects({
      from: { server: 'src' },
      to: { server: 'dst' },
      objects: [{ kind: 'table', name: 'widgets' }],
      include: 'ddl',
    });

    expect(result.applied).toBe(true);
    expect(result.objectsTransferred).toBeGreaterThanOrEqual(1);

    const r = await dstPool.query(`SELECT to_regclass('public.widgets') AS reg`);
    expect(r.rows[0].reg).not.toBeNull();
  }, 90_000);

  it('transfers DDL + data', async () => {
    await srcPool.query(`
      CREATE TABLE items (id int PRIMARY KEY, label text);
      INSERT INTO items VALUES (1, 'a'), (2, 'b'), (3, 'c');
    `);

    const result = await transferObjects({
      from: { server: 'src' },
      to: { server: 'dst' },
      objects: [{ kind: 'table', name: 'items' }],
      include: 'both',
    });

    expect(result.applied).toBe(true);
    expect(result.rowsTransferred).toBe(3);

    const r = await dstPool.query(`SELECT id, label FROM items ORDER BY id`);
    expect(r.rows).toEqual([
      { id: 1, label: 'a' },
      { id: 2, label: 'b' },
      { id: 3, label: 'c' },
    ]);
  }, 90_000);

  it('respects if_exists=error when target object already exists', async () => {
    await srcPool.query(`CREATE TABLE conflict_t (id int)`);
    await dstPool.query(`CREATE TABLE conflict_t (id int)`);

    await expect(
      transferObjects({
        from: { server: 'src' },
        to: { server: 'dst' },
        objects: [{ kind: 'table', name: 'conflict_t' }],
        include: 'ddl',
        if_exists: 'error',
      })
    ).rejects.toThrow(/already exists/i);
  }, 90_000);

  it('skips existing target object when if_exists=skip', async () => {
    await srcPool.query(`CREATE TABLE skip_t (id int, val text)`);
    await dstPool.query(`CREATE TABLE skip_t (id int, val text DEFAULT 'preset')`);

    const result = await transferObjects({
      from: { server: 'src' },
      to: { server: 'dst' },
      objects: [{ kind: 'table', name: 'skip_t' }],
      include: 'ddl',
      if_exists: 'skip',
    });

    expect(result.warnings.some((w) => /Skipped existing/i.test(w))).toBe(true);

    // Target still has the original DEFAULT 'preset'
    const r = await dstPool.query(`
      SELECT pg_get_expr(adbin, adrelid) AS def
      FROM pg_attrdef ad
      JOIN pg_class c ON c.oid = ad.adrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'public' AND c.relname = 'skip_t'
    `);
    expect(r.rows[0]?.def).toContain('preset');
  }, 90_000);

  it('replaces target object when if_exists=replace', async () => {
    await srcPool.query(`CREATE TABLE replace_t (id int, name text)`);
    await dstPool.query(`CREATE TABLE replace_t (id int)`); // missing column

    const result = await transferObjects({
      from: { server: 'src' },
      to: { server: 'dst' },
      objects: [{ kind: 'table', name: 'replace_t' }],
      include: 'ddl',
      if_exists: 'replace',
    });

    expect(result.applied).toBe(true);
    const r = await dstPool.query(`
      SELECT column_name FROM information_schema.columns
      WHERE table_schema='public' AND table_name='replace_t'
      ORDER BY ordinal_position`);
    expect(r.rows.map((x) => x.column_name)).toEqual(['id', 'name']);
  }, 90_000);

  it('dry_run emits SQL to output_file without touching target', async () => {
    await srcPool.query(`CREATE TABLE dryrun_t (id int)`);
    const tmpFile = require('path').join(require('os').tmpdir(), `sp3-dr-${Date.now()}.sql`);

    const result = await transferObjects({
      from: { server: 'src' },
      to: { server: 'dst' },
      objects: [{ kind: 'table', name: 'dryrun_t' }],
      include: 'ddl',
      dry_run: true,
      output_file: tmpFile,
    });

    expect(result.applied).toBe(false);
    expect(result.dryRun).toBe(true);

    // Target untouched
    const r = await dstPool.query(`SELECT to_regclass('public.dryrun_t') AS reg`);
    expect(r.rows[0].reg).toBeNull();

    // File was written
    const fs = require('fs');
    expect(fs.existsSync(tmpFile)).toBe(true);
    const content = fs.readFileSync(tmpFile, 'utf-8');
    expect(content).toContain('CREATE TABLE');
    expect(content).toContain('dryrun_t');
    fs.unlinkSync(tmpFile);
  }, 90_000);

  it('transfers multiple tables with FKs in correct order', async () => {
    await srcPool.query(`
      CREATE TABLE parents (id serial PRIMARY KEY, name text);
      CREATE TABLE children (
        id serial PRIMARY KEY,
        parent_id int NOT NULL REFERENCES parents(id),
        name text
      );
      INSERT INTO parents (name) VALUES ('p1');
      INSERT INTO children (parent_id, name) VALUES (1, 'c1');
    `);

    const result = await transferObjects({
      from: { server: 'src' },
      to: { server: 'dst' },
      objects: '*',
      include: 'both',
    });

    expect(result.applied).toBe(true);
    expect(result.rowsTransferred).toBe(2);

    const c = await dstPool.query('SELECT name, parent_id FROM children');
    expect(c.rows[0].name).toBe('c1');
  }, 90_000);
});
