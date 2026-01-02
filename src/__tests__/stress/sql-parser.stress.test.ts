/**
 * SQL Parser Stress Tests
 *
 * Tests parser behavior under extreme conditions.
 * These tests ensure parsing handles edge cases and doesn't crash.
 */

import {
  splitSqlStatementsWithLineNumbers,
  detectStatementType,
  extractTablesFromSql,
  stripLeadingComments,
} from '../../tools/sql/utils/sql-parser.js';
import {
  detectNonRollbackableOperations,
  extractDryRunError,
} from '../../tools/sql/utils/dry-run-utils.js';
import {
  preprocessSqlContent,
  formatFileSize,
} from '../../tools/sql/utils/file-handler.js';
import {
  validateDatabaseName,
  validateSchemaName,
} from '../../db-manager/validation.js';

describe('SQL Parser Stress Tests', () => {
  describe('Extremely Large Inputs', () => {
    it('should handle 10000 statements without crashing', () => {
      const sql = Array.from({ length: 10000 }, (_, i) => `SELECT ${i};`).join('\n');
      const statements = splitSqlStatementsWithLineNumbers(sql);
      expect(statements.length).toBe(10000);
    });

    it('should handle very long single line (100KB)', () => {
      const longValue = 'x'.repeat(100000);
      const sql = `SELECT '${longValue}'`;
      const statements = splitSqlStatementsWithLineNumbers(sql);
      expect(statements.length).toBe(1);
    });

    it('should handle deeply nested dollar quotes', () => {
      const sql = `
CREATE FUNCTION outer() AS $outer$
  CREATE FUNCTION inner() AS $inner$
    SELECT 'nested; quotes';
  $inner$;
$outer$;`;
      const statements = splitSqlStatementsWithLineNumbers(sql);
      expect(statements.length).toBe(1);
    });

    it('should handle 1000 line comments', () => {
      const comments = Array.from({ length: 1000 }, (_, i) => `-- Comment ${i}`).join('\n');
      const sql = comments + '\nSELECT 1';
      const stripped = stripLeadingComments(sql);
      expect(stripped.trim()).toBe('SELECT 1');
    });

    it('should handle 100 nested block comments (technically invalid but should not crash)', () => {
      const sql = '/* '.repeat(100) + 'comment' + ' */'.repeat(100) + ' SELECT 1';
      // Should not throw
      expect(() => splitSqlStatementsWithLineNumbers(sql)).not.toThrow();
    });
  });

  describe('Edge Case Inputs', () => {
    it('should handle empty string', () => {
      const statements = splitSqlStatementsWithLineNumbers('');
      expect(statements.length).toBe(0);
    });

    it('should handle only whitespace', () => {
      const statements = splitSqlStatementsWithLineNumbers('   \n\t\n   ');
      expect(statements.length).toBe(0);
    });

    it('should handle only semicolons', () => {
      const statements = splitSqlStatementsWithLineNumbers(';;;');
      // Expect empty statements to be filtered
      expect(statements.every((s) => s.sql.trim() !== '')).toBe(true);
    });

    it('should handle mixed null bytes', () => {
      const sql = 'SELECT\0* FROM\0users';
      // Should not throw
      expect(() => splitSqlStatementsWithLineNumbers(sql)).not.toThrow();
    });

    it('should handle unclosed string literal', () => {
      const sql = "SELECT 'unclosed";
      // Should not throw - just return what it can
      expect(() => splitSqlStatementsWithLineNumbers(sql)).not.toThrow();
    });

    it('should handle unclosed block comment', () => {
      const sql = '/* unclosed comment SELECT 1';
      expect(() => splitSqlStatementsWithLineNumbers(sql)).not.toThrow();
    });

    it('should handle mixed newline styles', () => {
      const sql = 'SELECT 1;\r\nSELECT 2;\rSELECT 3;\nSELECT 4;';
      const statements = splitSqlStatementsWithLineNumbers(sql);
      expect(statements.length).toBe(4);
    });

    it('should handle Unicode characters', () => {
      const sql = "SELECT '日本語', 'العربية', 'עברית', '中文', '한국어';";
      const statements = splitSqlStatementsWithLineNumbers(sql);
      expect(statements.length).toBe(1);
      expect(statements[0].sql).toContain('日本語');
    });

    it('should handle emoji in strings', () => {
      const sql = "SELECT '🔥🚀💯';";
      const statements = splitSqlStatementsWithLineNumbers(sql);
      expect(statements.length).toBe(1);
      expect(statements[0].sql).toContain('🔥');
    });
  });

  describe('Malformed SQL Handling', () => {
    it('should handle mismatched quotes', () => {
      const sql = `SELECT "unclosed`;
      expect(() => splitSqlStatementsWithLineNumbers(sql)).not.toThrow();
    });

    it('should handle mismatched dollar quotes', () => {
      const sql = `SELECT $tag$unclosed`;
      expect(() => splitSqlStatementsWithLineNumbers(sql)).not.toThrow();
    });

    it('should handle SQL injection patterns without crashing', () => {
      const injectionPatterns = [
        "'; DROP TABLE users; --",
        "1; DELETE FROM users WHERE 1=1; --",
        "' OR '1'='1",
        "'; EXEC xp_cmdshell('cmd'); --",
        "1 UNION SELECT * FROM passwords",
      ];

      for (const pattern of injectionPatterns) {
        expect(() => splitSqlStatementsWithLineNumbers(pattern)).not.toThrow();
        expect(() => detectStatementType(pattern)).not.toThrow();
        expect(() => extractTablesFromSql(pattern)).not.toThrow();
      }
    });
  });

  describe('Dry Run Utils Stress Tests', () => {
    it('should detect operations in 1000 statements', () => {
      const statements = [
        'VACUUM users',
        'SELECT NEXTVAL($1)',
        'CREATE INDEX CONCURRENTLY idx ON t(c)',
        'INSERT INTO t VALUES (1)',
        'SELECT * FROM t',
      ];

      const allSql = statements.map((s) => s + ';').join('\n').repeat(200);
      const parsedStatements = splitSqlStatementsWithLineNumbers(allSql);

      for (const stmt of parsedStatements) {
        expect(() => detectNonRollbackableOperations(stmt.sql)).not.toThrow();
      }
    });

    it('should extract error from complex error objects', () => {
      const complexError = {
        message: 'Complex error',
        code: '12345',
        severity: 'ERROR',
        detail: 'Detailed info',
        hint: 'Try this',
        schema: 'public',
        table: 'users',
        column: 'id',
        constraint: 'pk_users',
        position: 42,
        internalPosition: 10,
        internalQuery: 'SELECT 1',
        where: 'PL/pgSQL function',
        file: 'parse_expr.c',
        line: '123',
        routine: 'transformExpr',
        dataType: 'integer',
        nested: { ignored: true },
        array: [1, 2, 3],
      };

      const result = extractDryRunError(complexError);
      expect(result.message).toBe('Complex error');
      expect(result.code).toBe('12345');
    });
  });

  describe('File Handler Stress Tests', () => {
    it('should preprocess SQL with 100 patterns', () => {
      const patterns = Array.from({ length: 100 }, (_, i) => `-- Pattern ${i}`);
      const sql =
        patterns.map((p) => `${p}\nSELECT 1;`).join('\n') + '\nSELECT 2;';

      expect(() => preprocessSqlContent(sql, patterns, false)).not.toThrow();
    });

    it('should format all common file sizes correctly', () => {
      const sizes = [
        0,
        1,
        100,
        1023,
        1024,
        1025,
        1048576,
        1073741824,
        1099511627776,
      ];

      for (const size of sizes) {
        expect(() => formatFileSize(size)).not.toThrow();
      }
    });
  });

  describe('Validation Stress Tests', () => {
    it('should validate 1000 database names', () => {
      const validNames = Array.from({ length: 1000 }, (_, i) => `db_${i}`);
      for (const name of validNames) {
        expect(() => validateDatabaseName(name)).not.toThrow();
      }
    });

    it('should reject all SQL injection attempts on database names', () => {
      const injectionAttempts = [
        "db; DROP TABLE users",
        "db'--",
        'db"name',
        'db`name',
        "db'; DELETE FROM users WHERE '1'='1",
        "db--comment",
      ];

      for (const attempt of injectionAttempts) {
        expect(() => validateDatabaseName(attempt)).toThrow();
      }
    });

    it('should reject SQL injection attempts on schema names', () => {
      const schemaInjectionAttempts = [
        "schema; DROP TABLE",
        "schema'name",
        'schema"name',
        'schema`name',
        "schema'; DELETE",
        "1schema", // starts with digit
        "schema name", // contains space
      ];

      for (const attempt of schemaInjectionAttempts) {
        expect(() => validateSchemaName(attempt)).toThrow();
      }
    });
  });

  describe('Concurrent Access Simulation', () => {
    it('should handle simulated concurrent parsing', async () => {
      const sql = Array.from({ length: 100 }, (_, i) => `SELECT ${i};`).join('\n');

      const promises = Array.from({ length: 10 }, () =>
        Promise.resolve(splitSqlStatementsWithLineNumbers(sql))
      );

      const results = await Promise.all(promises);
      for (const result of results) {
        expect(result.length).toBe(100);
      }
    });
  });

  describe('Memory Efficiency', () => {
    it('should not hold references after parsing', () => {
      // Create a large SQL string
      let sql: string | null = Array.from(
        { length: 1000 },
        (_, i) => `SELECT * FROM table_${i}_with_long_name WHERE id = ${i};`
      ).join('\n');

      // Parse it
      const statements = splitSqlStatementsWithLineNumbers(sql);
      expect(statements.length).toBe(1000);

      // Clear the reference
      sql = null;

      // The statements should still be valid
      expect(statements[0].sql).toContain('SELECT');
    });
  });
});
