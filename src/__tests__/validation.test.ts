import { describe, it, expect } from '@jest/globals';
import {
  validateIdentifier,
  escapeIdentifier,
  isReadOnlySql,
  validatePositiveInteger,
  validateIndexType,
} from '../utils/validation.js';

describe('Validation Utilities', () => {
  describe('validateIdentifier', () => {
    it('should accept valid identifiers', () => {
      expect(validateIdentifier('users', 'table')).toBe('users');
      expect(validateIdentifier('user_accounts', 'table')).toBe('user_accounts');
      expect(validateIdentifier('_private', 'table')).toBe('_private');
      expect(validateIdentifier('Table123', 'table')).toBe('Table123');
      expect(validateIdentifier('column$1', 'column')).toBe('column$1');
    });

    it('should reject empty or null identifiers', () => {
      expect(() => validateIdentifier('', 'table')).toThrow('table is required');
      expect(() => validateIdentifier(null as any, 'table')).toThrow('table is required');
      expect(() => validateIdentifier(undefined as any, 'table')).toThrow('table is required');
    });

    it('should reject identifiers starting with numbers', () => {
      expect(() => validateIdentifier('123table', 'table')).toThrow('invalid characters');
    });

    it('should reject identifiers with special characters', () => {
      expect(() => validateIdentifier('table;drop', 'table')).toThrow('invalid characters');
      expect(() => validateIdentifier("table'", 'table')).toThrow('invalid characters');
      expect(() => validateIdentifier('table"', 'table')).toThrow('invalid characters');
      expect(() => validateIdentifier('table--', 'table')).toThrow('invalid characters');
      expect(() => validateIdentifier('table/*', 'table')).toThrow('invalid characters');
    });

    it('should reject identifiers that are too long', () => {
      const longName = 'a'.repeat(64);
      expect(() => validateIdentifier(longName, 'table')).toThrow('63 characters or less');
    });

    it('should reject SQL injection attempts', () => {
      expect(() => validateIdentifier('users; DROP TABLE users;--', 'table')).toThrow('invalid characters');
      expect(() => validateIdentifier("users' OR '1'='1", 'table')).toThrow('invalid characters');
      expect(() => validateIdentifier('users UNION SELECT', 'table')).toThrow('invalid characters');
    });
  });

  describe('escapeIdentifier', () => {
    it('should wrap identifiers in double quotes', () => {
      expect(escapeIdentifier('users')).toBe('"users"');
      expect(escapeIdentifier('public')).toBe('"public"');
    });

    it('should double internal double quotes', () => {
      expect(escapeIdentifier('table"name')).toBe('"table""name"');
      expect(escapeIdentifier('a"b"c')).toBe('"a""b""c"');
    });

    it('should reject empty identifiers', () => {
      expect(() => escapeIdentifier('')).toThrow('Identifier is required');
      expect(() => escapeIdentifier(null as any)).toThrow('Identifier is required');
    });
  });

  describe('isReadOnlySql', () => {
    describe('should allow read-only queries', () => {
      it('SELECT statements', () => {
        expect(isReadOnlySql('SELECT * FROM users').isReadOnly).toBe(true);
        expect(isReadOnlySql('SELECT id, name FROM users WHERE id = 1').isReadOnly).toBe(true);
        expect(isReadOnlySql('  SELECT * FROM users  ').isReadOnly).toBe(true);
      });

      it('EXPLAIN statements', () => {
        expect(isReadOnlySql('EXPLAIN SELECT * FROM users').isReadOnly).toBe(true);
        expect(isReadOnlySql('EXPLAIN ANALYZE SELECT * FROM users').isReadOnly).toBe(true);
      });

      it('WITH (CTE) SELECT statements', () => {
        expect(isReadOnlySql('WITH cte AS (SELECT * FROM users) SELECT * FROM cte').isReadOnly).toBe(true);
      });

      it('SHOW statements', () => {
        expect(isReadOnlySql('SHOW search_path').isReadOnly).toBe(true);
      });
    });

    describe('should block write operations', () => {
      it('INSERT statements', () => {
        const result = isReadOnlySql('INSERT INTO users (name) VALUES (\'test\')');
        expect(result.isReadOnly).toBe(false);
        expect(result.reason).toContain('INSERT');
      });

      it('UPDATE statements', () => {
        const result = isReadOnlySql('UPDATE users SET name = \'test\'');
        expect(result.isReadOnly).toBe(false);
        expect(result.reason).toContain('UPDATE');
      });

      it('DELETE statements', () => {
        const result = isReadOnlySql('DELETE FROM users WHERE id = 1');
        expect(result.isReadOnly).toBe(false);
        expect(result.reason).toContain('DELETE');
      });

      it('DROP statements', () => {
        const result = isReadOnlySql('DROP TABLE users');
        expect(result.isReadOnly).toBe(false);
        expect(result.reason).toContain('DROP');
      });

      it('CREATE statements', () => {
        const result = isReadOnlySql('CREATE TABLE test (id INT)');
        expect(result.isReadOnly).toBe(false);
        expect(result.reason).toContain('CREATE');
      });

      it('ALTER statements', () => {
        const result = isReadOnlySql('ALTER TABLE users ADD COLUMN email TEXT');
        expect(result.isReadOnly).toBe(false);
        expect(result.reason).toContain('ALTER');
      });

      it('TRUNCATE statements', () => {
        const result = isReadOnlySql('TRUNCATE TABLE users');
        expect(result.isReadOnly).toBe(false);
        expect(result.reason).toContain('TRUNCATE');
      });

      it('GRANT/REVOKE statements', () => {
        expect(isReadOnlySql('GRANT SELECT ON users TO public').isReadOnly).toBe(false);
        expect(isReadOnlySql('REVOKE SELECT ON users FROM public').isReadOnly).toBe(false);
      });
    });

    describe('should detect SQL injection via CTEs', () => {
      it('CTE with INSERT', () => {
        const result = isReadOnlySql('WITH x AS (INSERT INTO users VALUES (1) RETURNING *) SELECT * FROM x');
        expect(result.isReadOnly).toBe(false);
      });

      it('CTE with DELETE', () => {
        const result = isReadOnlySql('WITH deleted AS (DELETE FROM users RETURNING *) SELECT * FROM deleted');
        expect(result.isReadOnly).toBe(false);
      });

      it('CTE with UPDATE', () => {
        const result = isReadOnlySql('WITH updated AS (UPDATE users SET x=1 RETURNING *) SELECT * FROM updated');
        expect(result.isReadOnly).toBe(false);
      });
    });

    describe('should detect dangerous functions', () => {
      it('file read functions', () => {
        expect(isReadOnlySql('SELECT pg_read_file(\'/etc/passwd\')').isReadOnly).toBe(false);
        expect(isReadOnlySql('SELECT pg_read_binary_file(\'/etc/passwd\')').isReadOnly).toBe(false);
      });

      it('large object functions', () => {
        expect(isReadOnlySql('SELECT lo_import(\'/etc/passwd\')').isReadOnly).toBe(false);
        expect(isReadOnlySql('SELECT lo_export(12345, \'/tmp/file\')').isReadOnly).toBe(false);
      });

      it('dblink functions', () => {
        expect(isReadOnlySql('SELECT dblink_exec(\'host=evil\', \'DROP TABLE users\')').isReadOnly).toBe(false);
      });
    });

    describe('should handle comments and whitespace', () => {
      it('single-line comments', () => {
        const result = isReadOnlySql('-- comment\nDELETE FROM users');
        expect(result.isReadOnly).toBe(false);
      });

      it('multi-line comments', () => {
        const result = isReadOnlySql('/* comment */ DELETE FROM users');
        expect(result.isReadOnly).toBe(false);
      });

      it('comments hiding write operations', () => {
        const result = isReadOnlySql('SELECT * FROM users; -- \nDELETE FROM users');
        expect(result.isReadOnly).toBe(false);
      });
    });

    describe('should handle edge cases', () => {
      it('empty or null SQL', () => {
        expect(isReadOnlySql('').isReadOnly).toBe(false);
        expect(isReadOnlySql(null as any).isReadOnly).toBe(false);
      });

      it('case insensitivity', () => {
        expect(isReadOnlySql('delete FROM users').isReadOnly).toBe(false);
        expect(isReadOnlySql('DELETE from users').isReadOnly).toBe(false);
        expect(isReadOnlySql('DeLeTe FrOm users').isReadOnly).toBe(false);
      });
    });
  });

  describe('validatePositiveInteger', () => {
    it('should return value for valid integers', () => {
      expect(validatePositiveInteger(5, 'limit')).toBe(5);
      expect(validatePositiveInteger(100, 'limit', 1, 1000)).toBe(100);
      expect(validatePositiveInteger('10', 'limit')).toBe(10);
    });

    it('should return min for undefined/null', () => {
      expect(validatePositiveInteger(undefined, 'limit', 1, 100)).toBe(1);
      expect(validatePositiveInteger(null, 'limit', 5, 100)).toBe(5);
    });

    it('should throw for out of range values', () => {
      expect(() => validatePositiveInteger(0, 'limit', 1, 100)).toThrow('between 1 and 100');
      expect(() => validatePositiveInteger(101, 'limit', 1, 100)).toThrow('between 1 and 100');
      expect(() => validatePositiveInteger(-5, 'limit', 1, 100)).toThrow('between 1 and 100');
    });

    it('should throw for non-numeric values', () => {
      expect(() => validatePositiveInteger('abc', 'limit')).toThrow('must be an integer');
      expect(() => validatePositiveInteger(NaN, 'limit')).toThrow('must be an integer');
    });
  });

  describe('validateIndexType', () => {
    it('should accept valid index types', () => {
      expect(validateIndexType('btree')).toBe('btree');
      expect(validateIndexType('BTREE')).toBe('btree');
      expect(validateIndexType('hash')).toBe('hash');
      expect(validateIndexType('gist')).toBe('gist');
      expect(validateIndexType('gin')).toBe('gin');
      expect(validateIndexType('brin')).toBe('brin');
      expect(validateIndexType('spgist')).toBe('spgist');
    });

    it('should default to btree for empty/undefined', () => {
      expect(validateIndexType('')).toBe('btree');
      expect(validateIndexType(undefined as any)).toBe('btree');
    });

    it('should throw for invalid index types', () => {
      expect(() => validateIndexType('invalid')).toThrow('Invalid index type');
      expect(() => validateIndexType('index')).toThrow('Invalid index type');
    });
  });
});
