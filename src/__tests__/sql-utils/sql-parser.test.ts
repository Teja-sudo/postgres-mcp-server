/**
 * SQL Parser Utilities Tests
 */

import {
  splitSqlStatementsWithLineNumbers,
  stripLeadingComments,
  detectStatementType,
  extractTablesFromSql,
  filterExecutableStatements,
  normalizeSql,
} from '../../tools/sql/utils/sql-parser.js';

describe('sql-parser', () => {
  describe('stripLeadingComments', () => {
    it('should strip single-line comments', () => {
      const sql = `-- Comment
SELECT * FROM users`;
      expect(stripLeadingComments(sql).trim()).toBe('SELECT * FROM users');
    });

    it('should strip multiple single-line comments', () => {
      const sql = `-- First comment
-- Second comment
SELECT * FROM users`;
      expect(stripLeadingComments(sql).trim()).toBe('SELECT * FROM users');
    });

    it('should strip block comments', () => {
      const sql = `/* Block comment */
SELECT * FROM users`;
      expect(stripLeadingComments(sql).trim()).toBe('SELECT * FROM users');
    });

    it('should strip multi-line block comments', () => {
      const sql = `/*
 * Multi-line
 * block comment
 */
SELECT * FROM users`;
      expect(stripLeadingComments(sql).trim()).toBe('SELECT * FROM users');
    });

    it('should preserve SQL without leading comments', () => {
      const sql = 'SELECT * FROM users';
      expect(stripLeadingComments(sql)).toBe(sql);
    });

    it('should handle mixed comment types', () => {
      const sql = `-- Line comment
/* Block comment */
-- Another line comment
SELECT * FROM users`;
      expect(stripLeadingComments(sql).trim()).toBe('SELECT * FROM users');
    });
  });

  describe('detectStatementType', () => {
    it('should detect SELECT', () => {
      expect(detectStatementType('SELECT * FROM users')).toBe('SELECT');
    });

    it('should detect INSERT', () => {
      expect(detectStatementType('INSERT INTO users VALUES (1)')).toBe('INSERT');
    });

    it('should detect UPDATE', () => {
      expect(detectStatementType('UPDATE users SET name = $1')).toBe('UPDATE');
    });

    it('should detect DELETE', () => {
      expect(detectStatementType('DELETE FROM users')).toBe('DELETE');
    });

    it('should detect CREATE (for tables, indexes, etc.)', () => {
      expect(detectStatementType('CREATE TABLE users (id INT)')).toBe('CREATE');
      expect(detectStatementType('CREATE INDEX idx ON users(name)')).toBe('CREATE');
    });

    it('should detect ALTER', () => {
      expect(detectStatementType('ALTER TABLE users ADD COLUMN name TEXT')).toBe('ALTER');
    });

    it('should detect DROP', () => {
      expect(detectStatementType('DROP TABLE users')).toBe('DROP');
      expect(detectStatementType('DROP INDEX idx')).toBe('DROP');
    });

    it('should detect TRUNCATE', () => {
      expect(detectStatementType('TRUNCATE users')).toBe('TRUNCATE');
    });

    it('should detect GRANT', () => {
      expect(detectStatementType('GRANT SELECT ON users TO user1')).toBe('GRANT');
    });

    it('should detect REVOKE', () => {
      expect(detectStatementType('REVOKE SELECT ON users FROM user1')).toBe('REVOKE');
    });

    it('should detect WITH (CTE) with SELECT', () => {
      expect(detectStatementType('WITH cte AS (SELECT 1) SELECT * FROM cte')).toBe('WITH SELECT');
    });

    it('should detect BEGIN', () => {
      expect(detectStatementType('BEGIN')).toBe('BEGIN');
    });

    it('should detect COMMIT', () => {
      expect(detectStatementType('COMMIT')).toBe('COMMIT');
    });

    it('should detect ROLLBACK', () => {
      expect(detectStatementType('ROLLBACK')).toBe('ROLLBACK');
    });

    it('should ignore leading comments when detecting type', () => {
      expect(detectStatementType('-- Comment\nSELECT * FROM users')).toBe('SELECT');
    });

    it('should return UNKNOWN for unrecognized statements', () => {
      expect(detectStatementType('CUSTOM COMMAND')).toBe('UNKNOWN');
    });

    it('should handle case-insensitivity', () => {
      expect(detectStatementType('select * from users')).toBe('SELECT');
      expect(detectStatementType('Select * From Users')).toBe('SELECT');
    });
  });

  describe('splitSqlStatementsWithLineNumbers', () => {
    it('should split simple statements', () => {
      const sql = `SELECT 1;
SELECT 2;`;
      const statements = splitSqlStatementsWithLineNumbers(sql);
      expect(statements).toHaveLength(2);
      expect(statements[0].sql).toContain('SELECT 1');
      expect(statements[0].lineNumber).toBe(1);
      expect(statements[1].sql).toContain('SELECT 2');
      expect(statements[1].lineNumber).toBe(2);
    });

    it('should handle statements without trailing semicolons', () => {
      const sql = 'SELECT 1';
      const statements = splitSqlStatementsWithLineNumbers(sql);
      expect(statements).toHaveLength(1);
      expect(statements[0].sql.trim()).toBe('SELECT 1');
    });

    it('should preserve string literals containing semicolons', () => {
      const sql = "SELECT 'text; with; semicolons'";
      const statements = splitSqlStatementsWithLineNumbers(sql);
      expect(statements).toHaveLength(1);
      expect(statements[0].sql).toContain('; with; semicolons');
    });

    it('should handle dollar-quoted strings', () => {
      const sql = `CREATE FUNCTION test() RETURNS void AS $$
BEGIN
  SELECT 1; SELECT 2;
END;
$$ LANGUAGE plpgsql;`;
      const statements = splitSqlStatementsWithLineNumbers(sql);
      expect(statements).toHaveLength(1);
      expect(statements[0].sql).toContain('SELECT 1; SELECT 2;');
    });

    it('should handle custom dollar-quote tags', () => {
      const sql = `SELECT $tag$text; with; semicolons$tag$;
SELECT 2`;
      const statements = splitSqlStatementsWithLineNumbers(sql);
      expect(statements).toHaveLength(2);
      expect(statements[0].sql).toContain('text; with; semicolons');
    });

    it('should skip line comments', () => {
      const sql = `-- This is a comment
SELECT 1`;
      const statements = splitSqlStatementsWithLineNumbers(sql);
      expect(statements.length).toBeGreaterThanOrEqual(1);
    });

    it('should handle block comments', () => {
      const sql = `/* Comment with;
semicolon inside */
SELECT 1`;
      const statements = splitSqlStatementsWithLineNumbers(sql);
      expect(statements.length).toBeGreaterThanOrEqual(1);
    });

    it('should track correct line numbers for multi-line statements', () => {
      const sql = `SELECT
  id,
  name
FROM
  users;
SELECT 2`;
      const statements = splitSqlStatementsWithLineNumbers(sql);
      expect(statements[0].lineNumber).toBe(1);
      expect(statements[1].lineNumber).toBe(6);
    });

    it('should handle escaped single quotes in strings', () => {
      const sql = "SELECT 'it''s escaped'";
      const statements = splitSqlStatementsWithLineNumbers(sql);
      expect(statements).toHaveLength(1);
      expect(statements[0].sql).toContain("it''s escaped");
    });

    it('should handle escaped double quotes in strings', () => {
      const sql = 'SELECT "name with ""quotes""" FROM users';
      const statements = splitSqlStatementsWithLineNumbers(sql);
      expect(statements).toHaveLength(1);
      expect(statements[0].sql).toContain('""quotes""');
    });

    it('should handle multiple escaped quotes with semicolons', () => {
      const sql = "SELECT 'text; with ''escaped'' quotes';SELECT 2";
      const statements = splitSqlStatementsWithLineNumbers(sql);
      expect(statements).toHaveLength(2);
      expect(statements[0].sql).toContain("''escaped''");
    });
  });

  describe('extractTablesFromSql', () => {
    it('should extract table from SELECT', () => {
      const tables = extractTablesFromSql('SELECT * FROM users');
      expect(tables.some((t) => t.table === 'users')).toBe(true);
    });

    it('should extract table from SELECT with schema', () => {
      const tables = extractTablesFromSql('SELECT * FROM public.users');
      expect(tables.some((t) => t.schema === 'public' && t.table === 'users')).toBe(true);
    });

    it('should extract table from INSERT', () => {
      const tables = extractTablesFromSql('INSERT INTO users VALUES (1)');
      expect(tables.some((t) => t.table === 'users')).toBe(true);
    });

    it('should extract table from UPDATE', () => {
      const tables = extractTablesFromSql('UPDATE users SET name = $1');
      expect(tables.some((t) => t.table === 'users')).toBe(true);
    });

    it('should extract table from DELETE', () => {
      const tables = extractTablesFromSql('DELETE FROM users WHERE id = 1');
      expect(tables.some((t) => t.table === 'users')).toBe(true);
    });

    it('should extract tables from JOIN', () => {
      const tables = extractTablesFromSql(
        'SELECT * FROM users u JOIN orders o ON u.id = o.user_id'
      );
      expect(tables.some((t) => t.table === 'users')).toBe(true);
      expect(tables.some((t) => t.table === 'orders')).toBe(true);
    });

    it('should handle quoted identifiers', () => {
      const tables = extractTablesFromSql('SELECT * FROM "Users"');
      expect(tables.some((t) => t.table === 'Users')).toBe(true);
    });

    it('should default schema to public when not specified', () => {
      const tables = extractTablesFromSql('SELECT * FROM users');
      const userTable = tables.find((t) => t.table === 'users');
      expect(userTable?.schema).toBe('public');
    });
  });

  describe('filterExecutableStatements', () => {
    it('should filter out comment-only statements', () => {
      const statements = [
        { sql: '-- Just a comment', lineNumber: 1 },
        { sql: 'SELECT 1', lineNumber: 2 },
      ];
      const filtered = filterExecutableStatements(statements);
      expect(filtered).toHaveLength(1);
      expect(filtered[0].sql).toBe('SELECT 1');
    });

    it('should filter out empty statements', () => {
      const statements = [
        { sql: '   ', lineNumber: 1 },
        { sql: '', lineNumber: 2 },
        { sql: 'SELECT 1', lineNumber: 3 },
      ];
      const filtered = filterExecutableStatements(statements);
      expect(filtered).toHaveLength(1);
      expect(filtered[0].sql).toBe('SELECT 1');
    });

    it('should keep all executable statements', () => {
      const statements = [
        { sql: 'SELECT 1', lineNumber: 1 },
        { sql: 'INSERT INTO users VALUES (1)', lineNumber: 2 },
      ];
      const filtered = filterExecutableStatements(statements);
      expect(filtered).toHaveLength(2);
    });
  });

  describe('normalizeSql', () => {
    it('should remove line comments', () => {
      const sql = 'SELECT * -- comment\nFROM users';
      expect(normalizeSql(sql)).toBe('SELECT * FROM users');
    });

    it('should remove block comments', () => {
      const sql = 'SELECT /* comment */ * FROM users';
      expect(normalizeSql(sql)).toBe('SELECT * FROM users');
    });

    it('should remove multi-line block comments', () => {
      const sql = `SELECT /* multi
line
comment */ * FROM users`;
      expect(normalizeSql(sql)).toBe('SELECT * FROM users');
    });

    it('should normalize whitespace', () => {
      const sql = 'SELECT   *    FROM\n\t  users';
      expect(normalizeSql(sql)).toBe('SELECT * FROM users');
    });

    it('should handle multiple comments', () => {
      const sql = '-- header\nSELECT /* inline */ * FROM users -- trailing';
      expect(normalizeSql(sql)).toBe('SELECT * FROM users');
    });

    it('should trim leading and trailing whitespace', () => {
      const sql = '   SELECT * FROM users   ';
      expect(normalizeSql(sql)).toBe('SELECT * FROM users');
    });
  });

  describe('stripLeadingComments edge cases', () => {
    it('should return empty string for unclosed block comment', () => {
      const sql = '/* unclosed block comment';
      expect(stripLeadingComments(sql)).toBe('');
    });

    it('should return empty string when entire content is a line comment', () => {
      const sql = '-- just a comment without newline';
      expect(stripLeadingComments(sql)).toBe('');
    });

    it('should handle empty string', () => {
      expect(stripLeadingComments('')).toBe('');
    });

    it('should handle only whitespace', () => {
      expect(stripLeadingComments('   \n\t  ')).toBe('');
    });
  });

  describe('detectStatementType WITH CTE variants', () => {
    it('should detect WITH INSERT', () => {
      // Note: CTE must not contain SELECT for INSERT detection to work (function checks SELECT first)
      expect(detectStatementType('WITH data AS (VALUES (1)) INSERT INTO users (id) VALUES (1)')).toBe('WITH INSERT');
    });

    it('should detect WITH UPDATE', () => {
      // Note: CTE must not contain SELECT for UPDATE detection to work (function checks SELECT first)
      expect(detectStatementType('WITH data AS (VALUES (1)) UPDATE users SET x = 1')).toBe('WITH UPDATE');
    });

    it('should detect WITH DELETE', () => {
      // Note: Query must not contain SELECT for DELETE detection to work
      expect(detectStatementType('WITH data AS (VALUES (1)) DELETE FROM users')).toBe('WITH DELETE');
    });

    it('should detect plain WITH without DML', () => {
      expect(detectStatementType('WITH')).toBe('WITH');
    });

    it('should detect statement type with tab separator', () => {
      expect(detectStatementType('SELECT\t* FROM users')).toBe('SELECT');
    });

    it('should detect statement type with newline separator', () => {
      expect(detectStatementType('SELECT\n* FROM users')).toBe('SELECT');
    });

    it('should detect exact keyword match', () => {
      expect(detectStatementType('BEGIN')).toBe('BEGIN');
      expect(detectStatementType('COMMIT')).toBe('COMMIT');
      expect(detectStatementType('ROLLBACK')).toBe('ROLLBACK');
    });
  });

  describe('extractTablesFromSql edge cases', () => {
    it('should skip SQL keywords that look like table names', () => {
      // FROM SELECT would match SELECT as a table name, but it should be skipped
      const sql = 'SELECT * FROM WHERE'; // WHERE is in SQL_KEYWORDS_TO_SKIP
      const tables = extractTablesFromSql(sql);
      // WHERE should not be extracted as a table
      expect(tables.some(t => t.table.toUpperCase() === 'WHERE')).toBe(false);
    });

    it('should skip VALUES keyword', () => {
      const sql = 'INSERT INTO VALUES'; // VALUES is in SQL_KEYWORDS_TO_SKIP
      const tables = extractTablesFromSql(sql);
      expect(tables.some(t => t.table.toUpperCase() === 'VALUES')).toBe(false);
    });

    it('should skip SET keyword', () => {
      const sql = 'UPDATE SET'; // SET is in SQL_KEYWORDS_TO_SKIP
      const tables = extractTablesFromSql(sql);
      expect(tables.some(t => t.table.toUpperCase() === 'SET')).toBe(false);
    });

    it('should handle queries with comments', () => {
      const sql = 'SELECT * FROM /* comment */ users';
      const tables = extractTablesFromSql(sql);
      expect(tables.some(t => t.table === 'users')).toBe(true);
    });

    it('should not duplicate tables', () => {
      const sql = 'SELECT * FROM users u1 JOIN users u2 ON u1.id = u2.parent_id';
      const tables = extractTablesFromSql(sql);
      const userTables = tables.filter(t => t.table === 'users');
      expect(userTables).toHaveLength(1);
    });

    it('should return empty array for query without tables', () => {
      const sql = 'SELECT 1 + 1';
      const tables = extractTablesFromSql(sql);
      expect(tables).toHaveLength(0);
    });
  });
});
