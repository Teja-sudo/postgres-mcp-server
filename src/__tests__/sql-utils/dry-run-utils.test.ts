/**
 * Dry-Run Utilities Tests
 */

import {
  extractDryRunError,
  detectNonRollbackableOperations,
  hasMustSkipWarning,
  getSkipReason,
} from '../../tools/sql/utils/dry-run-utils.js';

describe('dry-run-utils', () => {
  describe('extractDryRunError', () => {
    it('should extract message from Error object', () => {
      const error = new Error('Test error message');
      const result = extractDryRunError(error);
      expect(result.message).toBe('Test error message');
    });

    it('should convert non-Error to string', () => {
      const result = extractDryRunError('string error');
      expect(result.message).toBe('string error');
    });

    it('should extract PostgreSQL error fields', () => {
      const pgError = {
        message: 'duplicate key error',
        code: '23505',
        severity: 'ERROR',
        detail: 'Key (id)=(1) already exists.',
        hint: 'Use ON CONFLICT to handle duplicates.',
        schema: 'public',
        table: 'users',
        column: 'id',
        constraint: 'users_pkey',
      };
      const result = extractDryRunError(pgError);

      expect(result.message).toBe('duplicate key error');
      expect(result.code).toBe('23505');
      expect(result.severity).toBe('ERROR');
      expect(result.detail).toBe('Key (id)=(1) already exists.');
      expect(result.hint).toBe('Use ON CONFLICT to handle duplicates.');
      expect(result.schema).toBe('public');
      expect(result.table).toBe('users');
      expect(result.column).toBe('id');
      expect(result.constraint).toBe('users_pkey');
    });

    it('should extract position fields as numbers', () => {
      const pgError = {
        message: 'syntax error',
        position: 42,
        internalPosition: 10,
      };
      const result = extractDryRunError(pgError);

      expect(result.position).toBe(42);
      expect(result.internalPosition).toBe(10);
    });
  });

  describe('detectNonRollbackableOperations', () => {
    describe('must-skip operations', () => {
      it('should detect VACUUM', () => {
        const warnings = detectNonRollbackableOperations('VACUUM ANALYZE users');
        expect(warnings).toHaveLength(1);
        expect(warnings[0].operation).toBe('VACUUM');
        expect(warnings[0].mustSkip).toBe(true);
      });

      it('should detect CREATE INDEX CONCURRENTLY', () => {
        const warnings = detectNonRollbackableOperations(
          'CREATE INDEX CONCURRENTLY idx_name ON users(name)'
        );
        expect(warnings).toHaveLength(1);
        expect(warnings[0].operation).toBe('CREATE_INDEX_CONCURRENTLY');
        expect(warnings[0].mustSkip).toBe(true);
      });

      it('should detect REINDEX CONCURRENTLY', () => {
        const warnings = detectNonRollbackableOperations('REINDEX CONCURRENTLY INDEX idx_name');
        expect(warnings).toHaveLength(1);
        expect(warnings[0].operation).toBe('REINDEX_CONCURRENTLY');
        expect(warnings[0].mustSkip).toBe(true);
      });

      it('should detect CREATE DATABASE', () => {
        const warnings = detectNonRollbackableOperations('CREATE DATABASE mydb');
        expect(warnings).toHaveLength(1);
        expect(warnings[0].operation).toBe('CREATE_DATABASE');
        expect(warnings[0].mustSkip).toBe(true);
      });

      it('should detect DROP DATABASE', () => {
        const warnings = detectNonRollbackableOperations('DROP DATABASE mydb');
        expect(warnings).toHaveLength(1);
        expect(warnings[0].operation).toBe('DROP_DATABASE');
        expect(warnings[0].mustSkip).toBe(true);
      });

      it('should detect NEXTVAL', () => {
        const warnings = detectNonRollbackableOperations("SELECT NEXTVAL('seq_name')");
        expect(warnings).toHaveLength(1);
        expect(warnings[0].operation).toBe('SEQUENCE');
        expect(warnings[0].mustSkip).toBe(true);
      });

      it('should detect SETVAL', () => {
        const warnings = detectNonRollbackableOperations("SELECT SETVAL('seq_name', 100)");
        expect(warnings).toHaveLength(1);
        expect(warnings[0].operation).toBe('SEQUENCE');
        expect(warnings[0].mustSkip).toBe(true);
      });

      it('should detect CLUSTER', () => {
        const warnings = detectNonRollbackableOperations('CLUSTER users USING idx_name');
        expect(warnings).toHaveLength(1);
        expect(warnings[0].operation).toBe('CLUSTER');
        expect(warnings[0].mustSkip).toBe(true);
      });
    });

    describe('warning-only operations', () => {
      it('should detect INSERT INTO with mustSkip=false', () => {
        const warnings = detectNonRollbackableOperations('INSERT INTO users (name) VALUES ($1)');
        expect(warnings).toHaveLength(1);
        expect(warnings[0].operation).toBe('SEQUENCE');
        expect(warnings[0].mustSkip).toBe(false);
      });

      it('should detect NOTIFY with mustSkip=false', () => {
        const warnings = detectNonRollbackableOperations("NOTIFY channel, 'payload'");
        expect(warnings).toHaveLength(1);
        expect(warnings[0].operation).toBe('NOTIFY');
        expect(warnings[0].mustSkip).toBe(false);
      });
    });

    describe('safe operations', () => {
      it('should return empty array for SELECT', () => {
        const warnings = detectNonRollbackableOperations('SELECT * FROM users');
        expect(warnings).toHaveLength(0);
      });

      it('should return empty array for UPDATE', () => {
        const warnings = detectNonRollbackableOperations('UPDATE users SET name = $1');
        expect(warnings).toHaveLength(0);
      });

      it('should return empty array for DELETE', () => {
        const warnings = detectNonRollbackableOperations('DELETE FROM users WHERE id = $1');
        expect(warnings).toHaveLength(0);
      });

      it('should return empty array for CREATE TABLE', () => {
        const warnings = detectNonRollbackableOperations('CREATE TABLE test (id INT)');
        expect(warnings).toHaveLength(0);
      });

      it('should return empty array for regular CREATE INDEX', () => {
        const warnings = detectNonRollbackableOperations('CREATE INDEX idx_name ON users(name)');
        expect(warnings).toHaveLength(0);
      });
    });

    describe('line number tracking', () => {
      it('should include statement index and line number', () => {
        const warnings = detectNonRollbackableOperations('VACUUM users', 2, 10);
        expect(warnings[0].statementIndex).toBe(2);
        expect(warnings[0].lineNumber).toBe(10);
      });
    });

    describe('TRANSACTION_CONTROL operations (SP-1)', () => {
      it.each([
        ['BEGIN;', 'BEGIN'],
        ['BEGIN', 'BEGIN'],
        ['START TRANSACTION;', 'START TRANSACTION'],
        ['COMMIT;', 'COMMIT'],
        ['COMMIT', 'COMMIT'],
        ['ROLLBACK;', 'ROLLBACK'],
        ['ABORT;', 'ABORT'],
        ['SAVEPOINT s1;', 'SAVEPOINT'],
        ['RELEASE SAVEPOINT s1;', 'RELEASE SAVEPOINT'],
        ['ROLLBACK TO SAVEPOINT s1;', 'ROLLBACK TO'],
      ])('flags TRANSACTION_CONTROL with mustSkip:true for: %s', (sql) => {
        const warnings = detectNonRollbackableOperations(sql);
        const tc = warnings.filter((w) => w.operation === 'TRANSACTION_CONTROL');
        expect(tc.length).toBeGreaterThanOrEqual(1);
        expect(tc[0].mustSkip).toBe(true);
      });

      it('strips leading line comments before matching', () => {
        // The COMMIT keyword preceded by a comment should still be caught
        const warnings = detectNonRollbackableOperations('-- migration step\nCOMMIT;');
        const tc = warnings.filter((w) => w.operation === 'TRANSACTION_CONTROL');
        expect(tc.length).toBeGreaterThanOrEqual(1);
      });

      it('strips leading block comments before matching', () => {
        const warnings = detectNonRollbackableOperations('/* note */ COMMIT;');
        const tc = warnings.filter((w) => w.operation === 'TRANSACTION_CONTROL');
        expect(tc.length).toBeGreaterThanOrEqual(1);
      });

      it('does NOT flag END (avoids false positives on CASE/plpgsql blocks)', () => {
        // 'END' on its own would be a transaction-control synonym for COMMIT,
        // but the false-positive risk against CASE...END / END IF / plpgsql
        // block terminators is too high. Layer 2 catches it at runtime.
        const warnings = detectNonRollbackableOperations('END;');
        const tc = warnings.filter((w) => w.operation === 'TRANSACTION_CONTROL');
        expect(tc).toHaveLength(0);
      });

      it('does NOT flag CREATE TABLE that just happens to mention BEGIN as a string', () => {
        // The static layer is loose with strings (existing limitation), but
        // the keyword must appear at the START of a statement to be flagged.
        // CREATE TABLE doesn't start with BEGIN.
        const warnings = detectNonRollbackableOperations(
          "CREATE TABLE log (msg text); -- BEGIN tx tracker"
        );
        const tc = warnings.filter((w) => w.operation === 'TRANSACTION_CONTROL');
        expect(tc).toHaveLength(0);
      });

      it('flags ROLLBACK without TO via the specific pattern, not ROLLBACK TO', () => {
        const rollbackOnly = detectNonRollbackableOperations('ROLLBACK;');
        const rollbackTo = detectNonRollbackableOperations('ROLLBACK TO sp1;');
        // Both should be flagged, with different messages
        const r1 = rollbackOnly.filter((w) => w.operation === 'TRANSACTION_CONTROL');
        const r2 = rollbackTo.filter((w) => w.operation === 'TRANSACTION_CONTROL');
        expect(r1.length).toBeGreaterThanOrEqual(1);
        expect(r2.length).toBeGreaterThanOrEqual(1);
      });
    });
  });

  describe('hasMustSkipWarning', () => {
    it('should return true when mustSkip warning exists', () => {
      const warnings = detectNonRollbackableOperations('VACUUM users');
      expect(hasMustSkipWarning(warnings)).toBe(true);
    });

    it('should return false when only warning-only operations', () => {
      const warnings = detectNonRollbackableOperations('INSERT INTO users (name) VALUES ($1)');
      expect(hasMustSkipWarning(warnings)).toBe(false);
    });

    it('should return false for empty array', () => {
      expect(hasMustSkipWarning([])).toBe(false);
    });
  });

  describe('getSkipReason', () => {
    it('should return skip reason for must-skip warnings', () => {
      const warnings = detectNonRollbackableOperations('VACUUM users');
      const reason = getSkipReason(warnings);
      expect(reason).toContain('VACUUM');
      expect(reason).toContain('cannot run inside a transaction');
    });

    it('should return empty string for warning-only operations', () => {
      const warnings = detectNonRollbackableOperations('INSERT INTO users (name) VALUES ($1)');
      const reason = getSkipReason(warnings);
      expect(reason).toBe('');
    });

    it('should combine multiple skip reasons', () => {
      // Simulate multiple must-skip warnings
      const warnings = [
        ...detectNonRollbackableOperations('VACUUM users'),
        ...detectNonRollbackableOperations("SELECT NEXTVAL('seq')"),
      ];
      const reason = getSkipReason(warnings);
      expect(reason).toContain('VACUUM');
      expect(reason).toContain('NEXTVAL');
    });
  });
});
