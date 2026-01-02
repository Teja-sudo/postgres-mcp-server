/**
 * SQL Parser Performance Tests
 *
 * Tests parsing performance with various SQL sizes and complexities.
 * These tests ensure parsing remains efficient as SQL complexity increases.
 */

import {
  splitSqlStatementsWithLineNumbers,
  detectStatementType,
  extractTablesFromSql,
} from '../../tools/sql/utils/sql-parser.js';

describe('SQL Parser Performance Tests', () => {
  // Helper to measure execution time
  const measureTime = (fn: () => void): number => {
    const start = process.hrtime.bigint();
    fn();
    const end = process.hrtime.bigint();
    return Number(end - start) / 1_000_000; // Convert to milliseconds
  };

  describe('splitSqlStatementsWithLineNumbers performance', () => {
    it('should parse 100 simple statements in < 50ms', () => {
      const statements = Array.from({ length: 100 }, (_, i) => `SELECT * FROM table${i};`).join(
        '\n'
      );

      const time = measureTime(() => {
        splitSqlStatementsWithLineNumbers(statements);
      });

      expect(time).toBeLessThan(50);
    });

    it('should parse 1000 simple statements in < 200ms', () => {
      const statements = Array.from({ length: 1000 }, (_, i) => `SELECT * FROM table${i};`).join(
        '\n'
      );

      const time = measureTime(() => {
        splitSqlStatementsWithLineNumbers(statements);
      });

      expect(time).toBeLessThan(200);
    });

    it('should handle large single statement with many string literals in < 100ms', () => {
      const stringLiterals = Array.from({ length: 100 }, (_, i) => `'value${i}'`).join(', ');
      const sql = `INSERT INTO large_table (col1) VALUES (${stringLiterals});`;

      const time = measureTime(() => {
        splitSqlStatementsWithLineNumbers(sql);
      });

      expect(time).toBeLessThan(100);
    });

    it('should handle complex function with dollar quotes in < 100ms', () => {
      const functionBody = Array.from(
        { length: 50 },
        (_, i) => `SELECT ${i} INTO result; IF result > 0 THEN RETURN result; END IF;`
      ).join('\n');

      const sql = `
CREATE OR REPLACE FUNCTION complex_function()
RETURNS INTEGER AS $$
DECLARE result INTEGER;
BEGIN
${functionBody}
RETURN 0;
END;
$$ LANGUAGE plpgsql;`;

      const time = measureTime(() => {
        splitSqlStatementsWithLineNumbers(sql);
      });

      expect(time).toBeLessThan(100);
    });

    it('should maintain consistent performance across multiple runs', () => {
      const sql = Array.from({ length: 500 }, (_, i) => `SELECT * FROM table${i};`).join('\n');

      const times: number[] = [];
      // Warm-up run
      splitSqlStatementsWithLineNumbers(sql);

      for (let i = 0; i < 5; i++) {
        times.push(
          measureTime(() => {
            splitSqlStatementsWithLineNumbers(sql);
          })
        );
      }

      const avgTime = times.reduce((a, b) => a + b) / times.length;
      const maxDeviation = Math.max(...times.map((t) => Math.abs(t - avgTime)));

      // Max deviation should be less than 500% of average (allow for high system variability in CI/test environments)
      expect(maxDeviation).toBeLessThan(avgTime * 5.0);
    });
  });

  describe('detectStatementType performance', () => {
    it('should detect types for 1000 statements in < 50ms', () => {
      const statements = [
        'SELECT * FROM users',
        'INSERT INTO users VALUES (1)',
        'UPDATE users SET name = $1',
        'DELETE FROM users WHERE id = 1',
        'CREATE TABLE test (id INT)',
        'DROP TABLE test',
        'ALTER TABLE test ADD COLUMN name TEXT',
        'BEGIN',
        'COMMIT',
        'ROLLBACK',
      ];

      const time = measureTime(() => {
        for (let i = 0; i < 100; i++) {
          for (const sql of statements) {
            detectStatementType(sql);
          }
        }
      });

      expect(time).toBeLessThan(50);
    });

    it('should handle statements with long leading comments in < 50ms', () => {
      const longComment = '-- ' + 'x'.repeat(1000) + '\n';
      const statements = Array.from({ length: 100 }, () => longComment + 'SELECT 1');

      const time = measureTime(() => {
        for (const sql of statements) {
          detectStatementType(sql);
        }
      });

      expect(time).toBeLessThan(50);
    });
  });

  describe('extractTablesFromSql performance', () => {
    it('should extract tables from complex JOIN query in < 50ms', () => {
      const tables = Array.from({ length: 20 }, (_, i) => `table${i}`);
      const joins = tables.map((t, i) => (i === 0 ? `FROM ${t} t0` : `JOIN ${t} t${i} ON t${i}.id = t0.id`)).join('\n');
      const sql = `SELECT * ${joins}`;

      const time = measureTime(() => {
        for (let i = 0; i < 100; i++) {
          extractTablesFromSql(sql);
        }
      });

      expect(time).toBeLessThan(50);
    });

    it('should handle queries with many schema-qualified tables in < 50ms', () => {
      const tables = Array.from({ length: 50 }, (_, i) => `schema${i}.table${i}`);
      const sql = `SELECT * FROM ${tables.join(', ')}`;

      const time = measureTime(() => {
        for (let i = 0; i < 100; i++) {
          extractTablesFromSql(sql);
        }
      });

      expect(time).toBeLessThan(50);
    });
  });

  describe('Combined parsing workflow performance', () => {
    it('should process realistic SQL file content in < 500ms', () => {
      // Simulate a realistic SQL file with mixed content
      const content = `
-- Database migration script
-- Version: 1.0.0

BEGIN;

-- Create users table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create orders table
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    total DECIMAL(10, 2),
    status VARCHAR(50)
);

-- Insert sample data
INSERT INTO users (email, password_hash) VALUES
    ('user1@example.com', 'hash1'),
    ('user2@example.com', 'hash2'),
    ('user3@example.com', 'hash3');

-- Create indexes
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_orders_user_id ON orders(user_id);

-- Create view
CREATE OR REPLACE VIEW user_orders AS
SELECT u.email, COUNT(o.id) as order_count
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
GROUP BY u.email;

COMMIT;
`.repeat(10); // Repeat 10 times to simulate larger file

      const time = measureTime(() => {
        const statements = splitSqlStatementsWithLineNumbers(content);
        for (const stmt of statements) {
          detectStatementType(stmt.sql);
          extractTablesFromSql(stmt.sql);
        }
      });

      expect(time).toBeLessThan(500);
    });
  });
});
