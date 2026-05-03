/**
 * SP-5 unit tests for lock_check and safe_alter_table.
 * (detect_migration_state needs real PG → integration suite.)
 */

import { describe, it, expect, jest } from '@jest/globals';

// Mock db-manager so lock_check can run without a real connection.
jest.unstable_mockModule('../db-manager.js', () => ({
  getDbManager: jest.fn(() => ({
    getClient: jest.fn(async () => ({
      query: jest.fn(async () => ({ rows: [] })),
      release: jest.fn(),
    })),
    getClientWithOverride: jest.fn(async () => ({
      client: { query: jest.fn(async () => ({ rows: [] })) },
      release: jest.fn(),
      server: 's',
      database: 'd',
      schema: 'public',
    })),
  })),
  resetDbManager: jest.fn(),
  OverrideClientResult: {},
}));

const { lockCheck, safeAlterTable } = await import('../tools/safety-tools.js');

describe('lock_check', () => {
  it('detects ACCESS EXCLUSIVE for DROP TABLE', async () => {
    const r = await lockCheck({ sql: 'DROP TABLE users', estimate_duration: false });
    expect(r.detectedLockLevel).toBe('AccessExclusiveLock');
    expect(r.warnings.some((w) => /ACCESS EXCLUSIVE/i.test(w))).toBe(true);
  });

  it('detects SHARE UPDATE EXCLUSIVE for CREATE INDEX CONCURRENTLY', async () => {
    const r = await lockCheck({
      sql: 'CREATE INDEX CONCURRENTLY idx_email ON users (email)',
      estimate_duration: false,
    });
    expect(r.detectedLockLevel).toBe('ShareUpdateExclusiveLock');
  });

  it('detects SHARE for plain CREATE INDEX and recommends CONCURRENTLY', async () => {
    const r = await lockCheck({
      sql: 'CREATE INDEX idx_email ON users (email)',
      estimate_duration: false,
    });
    expect(r.detectedLockLevel).toBe('ShareLock');
    expect(r.recommendations.some((rec) => /CONCURRENTLY/.test(rec))).toBe(true);
  });

  it('detects rewrite for VACUUM FULL', async () => {
    const r = await lockCheck({ sql: 'VACUUM FULL users', estimate_duration: false });
    expect(r.detectedLockLevel).toBe('AccessExclusiveLock');
    expect(r.forcesTableRewrite).toBe(true);
  });

  it('detects ALTER COLUMN TYPE as rewrite', async () => {
    const r = await lockCheck({
      sql: 'ALTER TABLE users ALTER COLUMN id TYPE bigint',
      estimate_duration: false,
    });
    expect(r.forcesTableRewrite).toBe(true);
  });

  it('detects ADD COLUMN with volatile DEFAULT as rewrite', async () => {
    const r = await lockCheck({
      sql: "ALTER TABLE users ADD COLUMN created_at timestamptz DEFAULT now()",
      estimate_duration: false,
    });
    expect(r.forcesTableRewrite).toBe(true);
  });

  it('does not force rewrite for ADD COLUMN with constant default', async () => {
    const r = await lockCheck({
      sql: "ALTER TABLE users ADD COLUMN status text DEFAULT 'active'",
      estimate_duration: false,
    });
    expect(r.forcesTableRewrite).toBe(false);
  });

  it('recommends NOT VALID + VALIDATE for adding FK', async () => {
    const r = await lockCheck({
      sql: 'ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id)',
      estimate_duration: false,
    });
    expect(r.recommendations.some((rec) => /NOT VALID/.test(rec))).toBe(true);
  });

  it('strips comments before matching', async () => {
    const r = await lockCheck({
      sql: '-- a comment\nDROP TABLE users',
      estimate_duration: false,
    });
    expect(r.detectedLockLevel).toBe('AccessExclusiveLock');
  });
});

describe('safe_alter_table', () => {
  it('produces 4-step recipe for add_not_null_column_with_default', async () => {
    const r = await safeAlterTable({
      intent: {
        kind: 'add_not_null_column_with_default',
        table: 'users',
        column: 'status',
        type: 'text',
        default_expr: "'active'",
      },
    });
    expect(r.recipe.length).toBe(4);
    expect(r.recipe[0].sql).toContain('ADD COLUMN');
    expect(r.recipe[0].sql).not.toContain('NOT NULL');
    expect(r.recipe[1].sql).toContain('UPDATE');
    expect(r.recipe[2].sql).toContain('SET DEFAULT');
    expect(r.recipe[3].sql).toContain('NOT NULL');
    expect(r.scriptSql).toContain('Step 1');
    expect(r.scriptSql).toContain('Step 4');
  });

  it('produces NOT VALID + VALIDATE recipe for add_not_null', async () => {
    const r = await safeAlterTable({
      intent: { kind: 'add_not_null', table: 'users', column: 'email' },
    });
    expect(r.recipe.length).toBe(4);
    expect(r.recipe[0].sql).toContain('NOT VALID');
    expect(r.recipe[1].sql).toContain('VALIDATE CONSTRAINT');
  });

  it('produces NOT VALID FK recipe for add_foreign_key', async () => {
    const r = await safeAlterTable({
      intent: {
        kind: 'add_foreign_key',
        table: 'orders',
        constraint_name: 'fk_user',
        columns: ['user_id'],
        references_table: 'users',
        references_columns: ['id'],
      },
    });
    expect(r.recipe.length).toBe(2);
    expect(r.recipe[0].sql).toContain('FOREIGN KEY');
    expect(r.recipe[0].sql).toContain('NOT VALID');
    expect(r.recipe[1].sql).toContain('VALIDATE CONSTRAINT');
  });

  it('produces CONCURRENTLY recipe for create_index', async () => {
    const r = await safeAlterTable({
      intent: {
        kind: 'create_index',
        table: 'users',
        index_name: 'idx_email',
        columns: ['email'],
      },
    });
    expect(r.recipe.length).toBe(1);
    expect(r.recipe[0].sql).toContain('CONCURRENTLY');
    expect(r.recipe[0].sql).toContain('btree');
  });

  it('produces unique CONCURRENTLY index when requested', async () => {
    const r = await safeAlterTable({
      intent: {
        kind: 'create_index',
        table: 'users',
        index_name: 'uniq_email',
        columns: ['email'],
        unique: true,
      },
    });
    expect(r.recipe[0].sql).toContain('UNIQUE INDEX CONCURRENTLY');
  });

  it('produces CONCURRENTLY drop for drop_index', async () => {
    const r = await safeAlterTable({
      intent: { kind: 'drop_index', index_name: 'idx_old' },
    });
    expect(r.recipe[0].sql).toContain('DROP INDEX CONCURRENTLY');
  });
});
