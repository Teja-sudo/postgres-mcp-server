/**
 * SP-1 integration tests — dry-run trust restoration.
 *
 * Real Postgres via testcontainers. Validates that the four entry
 * points (dry_run_sql_file, mutationDryRun, executeSqlFile, and
 * execute_sql with transactionId) NEVER persist changes when their
 * input SQL contains transaction-control statements.
 *
 * Skipped silently unless POSTGRES_INTEGRATION_TESTS=1.
 */

import { afterAll, beforeAll, beforeEach, expect, it } from '@jest/globals';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { Pool } from 'pg';
import {
  describeIntegration,
  startPostgres,
  stopPostgres,
  resetDatabase,
  PgHandle,
} from './postgres-container.js';

import { resetDbManager } from '../../db-manager.js';
import {
  dryRunSqlFile,
  mutationDryRun,
  executeSqlFile,
  executeSql,
  beginTransaction,
  commitTransaction,
  rollbackTransaction,
  switchServerDb,
} from '../../tools/index.js';

describeIntegration('SP-1 dry-run trust restoration', () => {
  let container: PgHandle;
  let pool: Pool;
  let testDir: string;

  beforeAll(async () => {
    const started = await startPostgres('audit_sp1');
    container = started.container;
    pool = started.pool;

    // Configure env vars so the postgres-mcp DatabaseManager singleton
    // picks up our test container as a configured server.
    process.env.PG_NAME_TEST = 'sp1test';
    process.env.PG_HOST_TEST = container.getHost();
    process.env.PG_PORT_TEST = String(container.getPort());
    process.env.PG_USERNAME_TEST = container.getUsername();
    process.env.PG_PASSWORD_TEST = container.getPassword();
    process.env.PG_DATABASE_TEST = container.getDatabase();
    process.env.PG_DEFAULT_TEST = 'true';
    process.env.PG_SSL_TEST = 'false';

    resetDbManager();
    await switchServerDb({ server: 'sp1test' });

    testDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sp1-integration-'));
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

  /** Helper: write a file in the test dir and return its path. */
  function writeSql(name: string, contents: string): string {
    const p = path.join(testDir, name);
    fs.writeFileSync(p, contents, 'utf-8');
    return p;
  }

  /** Helper: assert a table exists / does not exist via to_regclass. */
  async function tableExists(table: string): Promise<boolean> {
    const r = await pool.query(`SELECT to_regclass($1) AS reg`, [`public.${table}`]);
    return r.rows[0].reg !== null;
  }

  it('Bug repro: dry_run_sql_file with embedded COMMIT does not persist', async () => {
    const filePath = writeSql(
      'embedded-commit.sql',
      `CREATE TABLE bug_repro (id int);
INSERT INTO bug_repro VALUES (1);
COMMIT;
INSERT INTO bug_repro VALUES (2);`
    );

    const result = await dryRunSqlFile({ filePath });

    // The COMMIT should be statically skipped. CREATE+both INSERTs
    // run inside the outer tx; ROLLBACK undoes them all.
    expect(await tableExists('bug_repro')).toBe(false);
    expect(result.rolledBack).toBe(true);

    // The TRANSACTION_CONTROL warning should be in the output
    const tcWarnings = result.nonRollbackableWarnings.filter(
      (w) => w.operation === 'TRANSACTION_CONTROL'
    );
    expect(tcWarnings.length).toBeGreaterThanOrEqual(1);
    expect(tcWarnings[0].mustSkip).toBe(true);
    expect(tcWarnings[0].lineNumber).toBe(3); // COMMIT is on line 3
  }, 60_000);

  it('dry_run_sql_file flags TRANSACTION_CONTROL warnings with line numbers', async () => {
    const filePath = writeSql(
      'tx-control.sql',
      `CREATE TABLE t1 (id int);
BEGIN;
INSERT INTO t1 VALUES (1);
ROLLBACK;
SAVEPOINT s1;`
    );

    const result = await dryRunSqlFile({ filePath });

    const tc = result.nonRollbackableWarnings.filter(
      (w) => w.operation === 'TRANSACTION_CONTROL'
    );
    // BEGIN, ROLLBACK, SAVEPOINT all flagged with line numbers
    expect(tc.length).toBeGreaterThanOrEqual(3);
    expect(await tableExists('t1')).toBe(false);
  }, 60_000);

  it('dry_run_sql_file Layer 2: catches COMMIT inside a DO block', async () => {
    const filePath = writeSql(
      'do-block-commit.sql',
      `CREATE TABLE do_test (id int);
DO $$
BEGIN
  INSERT INTO do_test VALUES (1);
  COMMIT;
END $$;`
    );

    const result = await dryRunSqlFile({ filePath });

    // Static layer cannot reach COMMIT inside the dollar-quoted body.
    // Layer 2 sentinel must catch it.
    expect(result.dryRunCompromised).toBe(true);
    expect(result.compromisedAt?.reason).toMatch(/tx_(closed|diverged)/);
    expect(result.rolledBack).toBe(false);
    expect(result.summary).toMatch(/persisted/i);
  }, 60_000);

  it('mutationDryRun rejects multi-statement SQL with embedded COMMIT', async () => {
    await pool.query('CREATE TABLE mut_test (id int)');

    const result = await mutationDryRun({
      sql: "INSERT INTO mut_test VALUES (1); COMMIT; INSERT INTO mut_test VALUES (2)",
    });

    // The SQL has multiple statements; PG client.query may execute
    // them as a script. Either way: outer tx is gone afterwards;
    // sentinel catches it; dryRunCompromised is set.
    expect(result.dryRunCompromised).toBe(true);
    expect(result.success).toBe(false);
  }, 60_000);

  it('executeSqlFile(useTransaction=true) detects embedded COMMIT and reports failure', async () => {
    const filePath = writeSql(
      'tx-mode.sql',
      `CREATE TABLE tx_test (id int);
INSERT INTO tx_test VALUES (1);
COMMIT;
INSERT INTO tx_test VALUES (2);`
    );

    const result = await executeSqlFile({
      filePath,
      useTransaction: true,
      stopOnError: true,
    });

    // Transaction-control statements ARE statically skipped in
    // executeSqlFile too (same dry-run-utils pattern), so the
    // CREATE+INSERTs run inside the outer tx and COMMIT happens
    // at our wrapper level. Result: success.
    expect(result.success).toBe(true);
    expect(await tableExists('tx_test')).toBe(true);
    // Reset state for next test
    await resetDatabase(pool);
  }, 60_000);

  it('executeSqlFile refuses (useTransaction=true, stopOnError=false)', async () => {
    const filePath = writeSql('any.sql', 'SELECT 1');

    await expect(
      executeSqlFile({
        filePath,
        useTransaction: true,
        stopOnError: false,
      })
    ).rejects.toThrow(/cannot be combined/i);
  }, 60_000);

  it('execute_sql in transaction with embedded COMMIT marks transaction compromised', async () => {
    await pool.query('CREATE TABLE tx_compromise (id int)');

    const tx = await beginTransaction({ name: 'compromise-test' });

    // Execute a statement that contains a COMMIT - this should mark
    // the transaction compromised via the sentinel.
    const result = await executeSql({
      sql: 'INSERT INTO tx_compromise VALUES (1); COMMIT; INSERT INTO tx_compromise VALUES (2)',
      allowMultipleStatements: true,
      transactionId: tx.transactionId,
    });

    expect(result).toBeDefined();

    // Now committing should report 'compromised' status, not 'committed'.
    const commitResult = await commitTransaction({
      transactionId: tx.transactionId,
    });

    expect(commitResult.status).toBe('compromised');
    expect(commitResult.message).toMatch(/compromised/i);
  }, 60_000);

  it('execute_sql in healthy transaction commits cleanly', async () => {
    await pool.query('CREATE TABLE tx_healthy (id int)');

    const tx = await beginTransaction({ name: 'healthy-test' });

    const result = await executeSql({
      sql: 'INSERT INTO tx_healthy VALUES (42)',
      transactionId: tx.transactionId,
    });

    expect(result).toBeDefined();

    const commitResult = await commitTransaction({
      transactionId: tx.transactionId,
    });

    expect(commitResult.status).toBe('committed');

    const r = await pool.query('SELECT id FROM tx_healthy');
    expect(r.rows).toEqual([{ id: 42 }]);
  }, 60_000);

  it('rollback_transaction on a healthy tx still works', async () => {
    await pool.query('CREATE TABLE tx_rollback (id int)');

    const tx = await beginTransaction({});
    await executeSql({
      sql: 'INSERT INTO tx_rollback VALUES (99)',
      transactionId: tx.transactionId,
    });

    const result = await rollbackTransaction({ transactionId: tx.transactionId });
    expect(result.status).toBe('rolled_back');

    const r = await pool.query('SELECT count(*)::int AS c FROM tx_rollback');
    expect(r.rows[0].c).toBe(0);
  }, 60_000);

  it('healthy dry_run_sql_file (no transaction control) reports rolledBack:true', async () => {
    const filePath = writeSql(
      'healthy.sql',
      `CREATE TABLE healthy_dryrun (id int);
INSERT INTO healthy_dryrun VALUES (1);
INSERT INTO healthy_dryrun VALUES (2);`
    );

    const result = await dryRunSqlFile({ filePath });

    expect(result.rolledBack).toBe(true);
    expect(result.dryRunCompromised).toBeFalsy();
    expect(result.successCount).toBe(3);
    expect(await tableExists('healthy_dryrun')).toBe(false);
  }, 60_000);
});
