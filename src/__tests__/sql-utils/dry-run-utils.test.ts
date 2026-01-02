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
