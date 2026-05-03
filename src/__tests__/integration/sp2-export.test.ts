/**
 * SP-2 integration tests — introspection + export_to_sql_file.
 *
 * Real Postgres via testcontainers. Validates DDL extraction for each
 * supported object kind, dependency ordering, and round-trip
 * (extract → write → execute on fresh DB → verify schema matches).
 */

import { afterAll, beforeAll, beforeEach, expect, it } from '@jest/globals';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { Pool } from 'pg';
import { StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import {
  describeIntegration,
  startPostgres,
  stopPostgres,
  resetDatabase,
} from './postgres-container.js';

import { resetDbManager } from '../../db-manager.js';
import {
  exportToSqlFile,
  switchServerDb,
} from '../../tools/index.js';

describeIntegration('SP-2 export_to_sql_file', () => {
  let container: StartedPostgreSqlContainer;
  let pool: Pool;
  let testDir: string;

  beforeAll(async () => {
    const started = await startPostgres();
    container = started.container;
    pool = started.pool;

    process.env.PG_NAME_TEST = 'sp2test';
    process.env.PG_HOST_TEST = container.getHost();
    process.env.PG_PORT_TEST = String(container.getPort());
    process.env.PG_USERNAME_TEST = container.getUsername();
    process.env.PG_PASSWORD_TEST = container.getPassword();
    process.env.PG_DATABASE_TEST = container.getDatabase();
    process.env.PG_DEFAULT_TEST = 'true';
    process.env.PG_SSL_TEST = 'false';

    resetDbManager();
    await switchServerDb({ server: 'sp2test' });

    testDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sp2-export-'));
  }, 120_000);

  afterAll(async () => {
    resetDbManager();
    if (testDir) fs.rmSync(testDir, { recursive: true, force: true });
    await stopPostgres(container, pool);
    delete process.env.PG_NAME_TEST;
    delete process.env.PG_HOST_TEST;
    delete process.env.PG_PORT_TEST;
    delete process.env.PG_USERNAME_TEST;
    delete process.env.PG_PASSWORD_TEST;
    delete process.env.PG_DATABASE_TEST;
    delete process.env.PG_DEFAULT_TEST;
    delete process.env.PG_SSL_TEST;
  }, 60_000);

  beforeEach(async () => {
    await resetDatabase(pool);
  });

  it('exports a single table DDL with constraints', async () => {
    await pool.query(`
      CREATE TABLE export_users (
        id serial PRIMARY KEY,
        email text NOT NULL UNIQUE,
        created_at timestamptz DEFAULT now()
      );
    `);

    const filePath = path.join(testDir, 'users.sql');
    const result = await exportToSqlFile({
      filePath,
      mode: 'overwrite',
      what: { kind: 'objects', objects: [{ kind: 'table', name: 'export_users' }] },
    });

    expect(result.objectsExported).toBe(1);
    const content = fs.readFileSync(filePath, 'utf-8');
    expect(content).toContain('CREATE TABLE');
    expect(content).toContain('export_users');
    expect(content).toContain('PRIMARY KEY');
    expect(content).toContain('UNIQUE');
    expect(content).toMatch(/-- postgres-mcp-server export/);
  }, 60_000);

  it('exports schema_dump with multiple tables and FKs', async () => {
    await pool.query(`
      CREATE TABLE authors (
        id serial PRIMARY KEY,
        name text NOT NULL
      );
      CREATE TABLE books (
        id serial PRIMARY KEY,
        author_id int NOT NULL REFERENCES authors(id),
        title text NOT NULL
      );
    `);

    const filePath = path.join(testDir, 'schema.sql');
    const result = await exportToSqlFile({
      filePath,
      mode: 'overwrite',
      what: { kind: 'schema_dump', schema: 'public' },
    });

    expect(result.objectsExported).toBeGreaterThanOrEqual(2);
    const content = fs.readFileSync(filePath, 'utf-8');

    // Both tables present
    expect(content).toContain('authors');
    expect(content).toContain('books');
    // FK emitted as ALTER TABLE (so cycles between tables can break safely)
    expect(content).toMatch(/ALTER TABLE.*ADD CONSTRAINT.*FOREIGN KEY/i);
    // authors should appear before books (alphabetical) - or before the FK ALTER at least
    const authorsIdx = content.indexOf('CREATE TABLE IF NOT EXISTS "public"."authors"');
    const fkAlterIdx = content.indexOf('FOREIGN KEY');
    expect(authorsIdx).toBeGreaterThan(0);
    expect(authorsIdx).toBeLessThan(fkAlterIdx);
  }, 60_000);

  it('exports data as INSERT statements', async () => {
    await pool.query(`
      CREATE TABLE simple (id int PRIMARY KEY, val text);
      INSERT INTO simple VALUES (1, 'one'), (2, 'two'), (3, 'three');
    `);

    const filePath = path.join(testDir, 'data.sql');
    const result = await exportToSqlFile({
      filePath,
      mode: 'overwrite',
      what: { kind: 'data', tables: ['simple'] },
    });

    expect(result.rowsExported).toBe(3);
    const content = fs.readFileSync(filePath, 'utf-8');
    expect(content).toContain('INSERT INTO');
    expect(content).toContain("'one'");
    expect(content).toContain("'two'");
    expect(content).toContain("'three'");
  }, 60_000);

  it('exports data with WHERE filter', async () => {
    await pool.query(`
      CREATE TABLE filtered (id int, status text);
      INSERT INTO filtered VALUES (1, 'active'), (2, 'inactive'), (3, 'active');
    `);

    const filePath = path.join(testDir, 'filtered.sql');
    const result = await exportToSqlFile({
      filePath,
      mode: 'overwrite',
      what: { kind: 'data', tables: ['filtered'], where: "status = 'active'" },
    });

    expect(result.rowsExported).toBe(2);
  }, 60_000);

  it('schema_dump with include_data round-trips cleanly', async () => {
    await pool.query(`
      CREATE TABLE rt_users (id serial PRIMARY KEY, name text);
      INSERT INTO rt_users (name) VALUES ('Alice'), ('Bob');
    `);

    const filePath = path.join(testDir, 'rt.sql');
    await exportToSqlFile({
      filePath,
      mode: 'overwrite',
      what: { kind: 'schema_dump', schema: 'public', include_data: true },
    });

    // Drop everything and replay the file
    await pool.query('DROP TABLE IF EXISTS rt_users CASCADE');
    const sql = fs.readFileSync(filePath, 'utf-8');
    await pool.query(sql);

    const r = await pool.query('SELECT name FROM rt_users ORDER BY id');
    expect(r.rows.map((x) => x.name)).toEqual(['Alice', 'Bob']);
  }, 60_000);

  it('append mode adds separator banner without removing prior content', async () => {
    const filePath = path.join(testDir, 'append.sql');
    fs.writeFileSync(filePath, '-- prior content\n');

    await pool.query('CREATE TABLE a1 (id int)');

    await exportToSqlFile({
      filePath,
      mode: 'append',
      what: { kind: 'objects', objects: [{ kind: 'table', name: 'a1' }] },
    });

    const content = fs.readFileSync(filePath, 'utf-8');
    expect(content).toContain('-- prior content');
    expect(content).toContain('-- postgres-mcp-server export');
    expect(content).toContain('CREATE TABLE');
  }, 60_000);

  it('refuses to write to .env or node_modules paths', async () => {
    await expect(
      exportToSqlFile({
        filePath: path.join(testDir, '.env.sql'),
        mode: 'overwrite',
        what: { kind: 'schema_dump' },
      })
    ).rejects.toThrow(/sensitive|.env|.sql/i);
  }, 60_000);

  it('source banner shows server alias, never host or port', async () => {
    const filePath = path.join(testDir, 'banner.sql');
    await exportToSqlFile({
      filePath,
      mode: 'overwrite',
      what: { kind: 'schema_dump' },
    });

    const content = fs.readFileSync(filePath, 'utf-8');
    expect(content).toMatch(/server="sp2test"/);
    // Host/port must NOT leak into the banner or anywhere in the file
    const host = container.getHost();
    expect(content).not.toContain(host);
    expect(content).not.toContain(String(container.getPort()));
  }, 60_000);

  it('exports query_result as INSERTs into target table', async () => {
    await pool.query(`
      CREATE TABLE source_t (id int, val text);
      INSERT INTO source_t VALUES (1, 'a'), (2, 'b');
    `);

    const filePath = path.join(testDir, 'qr.sql');
    const result = await exportToSqlFile({
      filePath,
      mode: 'overwrite',
      what: {
        kind: 'query_result',
        sql: 'SELECT * FROM source_t WHERE id = 1',
        target_table: 'target_t',
      },
    });

    expect(result.rowsExported).toBe(1);
    const content = fs.readFileSync(filePath, 'utf-8');
    expect(content).toContain('INSERT INTO');
    expect(content).toContain('"public"."target_t"');
  }, 60_000);

  it('exports view DDL with dependency on underlying table', async () => {
    await pool.query(`
      CREATE TABLE base_t (id int, name text);
      CREATE VIEW v_base AS SELECT id, name FROM base_t;
    `);

    const filePath = path.join(testDir, 'view.sql');
    const result = await exportToSqlFile({
      filePath,
      mode: 'overwrite',
      what: { kind: 'schema_dump', schema: 'public' },
    });

    expect(result.objectsExported).toBeGreaterThanOrEqual(2);
    const content = fs.readFileSync(filePath, 'utf-8');
    // View should be defined in the file
    expect(content).toContain('CREATE OR REPLACE VIEW');
    expect(content).toContain('v_base');
  }, 60_000);
});
