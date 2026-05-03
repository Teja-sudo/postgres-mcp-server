/**
 * SP-4 integration tests — describe_table, find_dependents, schema_diff.
 */

import { afterAll, beforeAll, beforeEach, expect, it } from '@jest/globals';
import { Pool } from 'pg';
import { describeIntegration, resetDatabase, startPostgres, PgHandle } from './postgres-container.js';
import { resetDbManager } from '../../db-manager.js';
import {
  describeTable,
  findDependents,
  schemaDiff,
  switchServerDb,
} from '../../tools/index.js';

describeIntegration('SP-4 schema awareness pack', () => {
  let containerA: PgHandle;
  let containerB: PgHandle;
  let poolA: Pool;
  let poolB: Pool;

  beforeAll(async () => {
    const [a, b] = await Promise.all([
      startPostgres('audit_sp4_a'),
      startPostgres('audit_sp4_b'),
    ]);
    containerA = a.container;
    containerB = b.container;
    poolA = a.pool;
    poolB = b.pool;

    process.env.PG_NAME_A = 'envA';
    process.env.PG_HOST_A = containerA.getHost();
    process.env.PG_PORT_A = String(containerA.getPort());
    process.env.PG_USERNAME_A = containerA.getUsername();
    process.env.PG_PASSWORD_A = containerA.getPassword();
    process.env.PG_DATABASE_A = containerA.getDatabase();
    process.env.PG_DEFAULT_A = 'true';
    process.env.PG_SSL_A = 'false';

    process.env.PG_NAME_B = 'envB';
    process.env.PG_HOST_B = containerB.getHost();
    process.env.PG_PORT_B = String(containerB.getPort());
    process.env.PG_USERNAME_B = containerB.getUsername();
    process.env.PG_PASSWORD_B = containerB.getPassword();
    process.env.PG_DATABASE_B = containerB.getDatabase();
    process.env.PG_SSL_B = 'false';

    resetDbManager();
    await switchServerDb({ server: 'envA' });
  }, 180_000);

  afterAll(async () => {
    resetDbManager();
    await Promise.all([poolA.end().catch(() => {}), poolB.end().catch(() => {})]);
    await Promise.all([containerA.stop().catch(() => {}), containerB.stop().catch(() => {})]);
    for (const k of [
      'PG_NAME_A', 'PG_HOST_A', 'PG_PORT_A', 'PG_USERNAME_A',
      'PG_PASSWORD_A', 'PG_DATABASE_A', 'PG_DEFAULT_A', 'PG_SSL_A',
      'PG_NAME_B', 'PG_HOST_B', 'PG_PORT_B', 'PG_USERNAME_B',
      'PG_PASSWORD_B', 'PG_DATABASE_B', 'PG_SSL_B',
    ]) delete process.env[k];
  }, 60_000);

  beforeEach(async () => {
    await Promise.all([resetDatabase(poolA), resetDatabase(poolB)]);
  });

  it('describe_table returns rich blob in single call', async () => {
    await poolA.query(`
      CREATE TABLE customers (
        id serial PRIMARY KEY,
        email text NOT NULL UNIQUE,
        country text
      );
      CREATE TABLE orders (
        id serial PRIMARY KEY,
        customer_id int NOT NULL REFERENCES customers(id),
        amount numeric
      );
      INSERT INTO customers (email, country) VALUES
        ('a@x.com', 'US'), ('b@x.com', 'UK');
      INSERT INTO orders (customer_id, amount) VALUES (1, 10), (2, 20);
      ANALYZE;
    `);

    const result = await describeTable({ table: 'customers' });
    expect(result.exists).toBe(true);
    expect(result.columns.map((c) => c.name)).toEqual(
      expect.arrayContaining(['id', 'email', 'country'])
    );
    expect(result.primaryKey).toEqual(['id']);
    expect(result.foreignKeysIn.length).toBe(1);
    expect(result.foreignKeysIn[0].sourceTable).toContain('orders');
    expect(result.foreignKeysOut.length).toBe(0);
    expect(result.indexes.length).toBeGreaterThanOrEqual(2); // PK + UNIQUE
    expect(result.sampleRows.length).toBeGreaterThan(0);
    expect(result.size).toBeDefined();
  }, 60_000);

  it('find_dependents finds views that depend on a table', async () => {
    await poolA.query(`
      CREATE TABLE base (id int PRIMARY KEY, val text);
      CREATE VIEW v1 AS SELECT id FROM base;
      CREATE VIEW v2 AS SELECT id FROM v1;
    `);

    const result = await findDependents({ name: 'base', kind: 'table' });
    expect(result.totalDependents).toBeGreaterThanOrEqual(1);
    const names = result.dependents.map((d) => d.name);
    // v1 should be a direct dependent
    expect(names).toContain('v1');
  }, 60_000);

  it('schema_diff reports CREATE for tables in source not in target', async () => {
    await poolA.query(`CREATE TABLE only_in_a (id int)`);

    const result = await schemaDiff({
      source: { server: 'envA' },
      target: { server: 'envB' },
    });

    expect(result.toCreate.some((o) => o.kind === 'table' && o.name === 'only_in_a')).toBe(true);
    expect(result.migrationSql).toContain('CREATE TABLE');
  }, 60_000);

  it('schema_diff reports DROP for tables in target not in source', async () => {
    await poolB.query(`CREATE TABLE only_in_b (id int)`);

    const result = await schemaDiff({
      source: { server: 'envA' },
      target: { server: 'envB' },
    });

    expect(result.toDrop.some((o) => o.kind === 'table' && o.name === 'only_in_b')).toBe(true);
    expect(result.migrationSql).toContain('DROP TABLE');
  }, 60_000);

  it('schema_diff with identical schemas returns empty diff', async () => {
    await poolA.query(`CREATE TABLE same_t (id int, val text)`);
    await poolB.query(`CREATE TABLE same_t (id int, val text)`);

    const result = await schemaDiff({
      source: { server: 'envA' },
      target: { server: 'envB' },
    });

    expect(result.toCreate.filter((o) => o.kind === 'table').length).toBe(0);
    expect(result.toDrop.filter((o) => o.kind === 'table').length).toBe(0);
    expect(result.toModify.filter((o) => o.kind === 'table').length).toBe(0);
  }, 60_000);
});
