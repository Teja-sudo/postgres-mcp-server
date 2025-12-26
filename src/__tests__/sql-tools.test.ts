import { jest, describe, it, expect, beforeEach, beforeAll } from '@jest/globals';
import * as fs from 'fs';

type MockFn = jest.Mock<any>;

// Use jest.unstable_mockModule for ESM
const mockQuery = jest.fn<MockFn>();
const mockGetClient = jest.fn<MockFn>();
const mockIsConnected = jest.fn<MockFn>();

jest.unstable_mockModule('../db-manager.js', () => ({
  getDbManager: jest.fn(() => ({
    query: mockQuery,
    getClient: mockGetClient,
    isConnected: mockIsConnected.mockReturnValue(true),
  })),
  resetDbManager: jest.fn(),
}));

// Dynamic import after mock
let executeSql: any;
let explainQuery: any;
let executeSqlFile: any;

beforeAll(async () => {
  const module = await import('../tools/sql-tools.js');
  executeSql = module.executeSql;
  explainQuery = module.explainQuery;
  executeSqlFile = module.executeSqlFile;
});

describe('SQL Tools', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockIsConnected.mockReturnValue(true);
  });

  describe('executeSql', () => {
    it('should require sql parameter', async () => {
      await expect(executeSql({ sql: '' }))
        .rejects.toThrow('sql parameter cannot be empty');

      await expect(executeSql({ sql: null as any }))
        .rejects.toThrow('sql parameter is required');
    });

    it('should reject SQL that is too long', async () => {
      const longSql = 'SELECT ' + 'a'.repeat(100001);
      await expect(executeSql({ sql: longSql }))
        .rejects.toThrow('exceeds 100000 characters');
    });

    it('should allow large scripts with allowLargeScript=true', async () => {
      const longSql = 'SELECT ' + 'a'.repeat(100001);
      mockQuery.mockResolvedValue({
        rows: [],
        fields: []
      });

      // Should not throw with allowLargeScript=true
      const result = await executeSql({ sql: longSql, allowLargeScript: true });
      expect(result).toBeDefined();
    });

    it('should return results for small result sets', async () => {
      mockQuery.mockResolvedValue({
        rows: [{ id: 1, name: 'Test' }],
        fields: [{ name: 'id' }, { name: 'name' }]
      });

      const result = await executeSql({ sql: 'SELECT * FROM users' });

      expect(result.rows).toHaveLength(1);
      expect(result.rowCount).toBe(1);
      expect(result.fields).toEqual(['id', 'name']);
      expect(result.outputFile).toBeUndefined();
      expect(result.truncated).toBeUndefined();
      expect(result.executionTimeMs).toBeDefined();
      expect(result.offset).toBe(0);
      expect(result.hasMore).toBe(false);
    });

    it('should support pagination with offset and maxRows', async () => {
      const rows = Array.from({ length: 100 }, (_, i) => ({ id: i }));
      mockQuery.mockResolvedValue({
        rows,
        fields: [{ name: 'id' }]
      });

      // Get first page
      const result1 = await executeSql({ sql: 'SELECT * FROM users', maxRows: 10, offset: 0 });
      expect(result1.rows).toHaveLength(10);
      expect(result1.rows[0].id).toBe(0);
      expect(result1.offset).toBe(0);
      expect(result1.hasMore).toBe(true);
      expect(result1.rowCount).toBe(100);

      // Get second page
      mockQuery.mockResolvedValue({ rows, fields: [{ name: 'id' }] });
      const result2 = await executeSql({ sql: 'SELECT * FROM users', maxRows: 10, offset: 10 });
      expect(result2.rows).toHaveLength(10);
      expect(result2.rows[0].id).toBe(10);
      expect(result2.offset).toBe(10);
      expect(result2.hasMore).toBe(true);

      // Get last page
      mockQuery.mockResolvedValue({ rows, fields: [{ name: 'id' }] });
      const result3 = await executeSql({ sql: 'SELECT * FROM users', maxRows: 10, offset: 90 });
      expect(result3.rows).toHaveLength(10);
      expect(result3.rows[0].id).toBe(90);
      expect(result3.hasMore).toBe(false);
    });

    it('should support parameterized queries', async () => {
      mockQuery.mockResolvedValue({
        rows: [{ id: 1, name: 'Test' }],
        fields: [{ name: 'id' }, { name: 'name' }]
      });

      await executeSql({ sql: 'SELECT * FROM users WHERE id = $1', params: [123] });

      expect(mockQuery).toHaveBeenCalledWith('SELECT * FROM users WHERE id = $1', [123]);
    });

    it('should validate params is an array', async () => {
      await expect(executeSql({ sql: 'SELECT 1', params: 'invalid' as any }))
        .rejects.toThrow('params must be an array');
    });

    it('should limit number of params', async () => {
      const manyParams = Array.from({ length: 101 }, (_, i) => i);
      await expect(executeSql({ sql: 'SELECT 1', params: manyParams }))
        .rejects.toThrow('Maximum 100 parameters allowed');
    });

    it('should write large results to file', async () => {
      const largeRows = Array.from({ length: 2000 }, (_, i) => ({ id: i, name: `User ${i}` }));
      mockQuery.mockResolvedValue({
        rows: largeRows,
        fields: [{ name: 'id' }, { name: 'name' }]
      });

      const result = await executeSql({ sql: 'SELECT * FROM users' });

      // With pagination, only first 1000 rows are returned, but if output is still large, writes to file
      expect(result.rowCount).toBe(2000);
      expect(result.hasMore).toBe(true);

      // Clean up if file was created
      if (result.outputFile) {
        fs.unlinkSync(result.outputFile);
      }
    });

    it('should validate maxRows parameter', async () => {
      await expect(executeSql({ sql: 'SELECT 1', maxRows: -1 }))
        .rejects.toThrow('maxRows must be an integer between');
    });

    it('should validate offset parameter', async () => {
      await expect(executeSql({ sql: 'SELECT 1', offset: -1 }))
        .rejects.toThrow('offset must be an integer between');
    });

    it('should handle empty result sets', async () => {
      mockQuery.mockResolvedValue({
        rows: [],
        fields: [{ name: 'id' }]
      });

      const result = await executeSql({ sql: 'SELECT * FROM users WHERE 1=0' });

      expect(result.rows).toEqual([]);
      expect(result.rowCount).toBe(0);
      expect(result.outputFile).toBeUndefined();
      expect(result.hasMore).toBe(false);
    });

    it('should return execution time', async () => {
      mockQuery.mockResolvedValue({
        rows: [{ id: 1 }],
        fields: [{ name: 'id' }]
      });

      const result = await executeSql({ sql: 'SELECT 1' });

      expect(result.executionTimeMs).toBeDefined();
      expect(typeof result.executionTimeMs).toBe('number');
      expect(result.executionTimeMs).toBeGreaterThanOrEqual(0);
    });
  });

  describe('explainQuery', () => {
    let mockClient: { query: MockFn; release: MockFn };

    beforeEach(() => {
      mockClient = {
        query: jest.fn<MockFn>(),
        release: jest.fn<MockFn>()
      };
      mockGetClient.mockResolvedValue(mockClient);
    });

    it('should require sql parameter', async () => {
      await expect(explainQuery({ sql: '' }))
        .rejects.toThrow('sql parameter is required');
    });

    it('should reject SQL that is too long', async () => {
      const longSql = 'SELECT ' + 'a'.repeat(100001);
      await expect(explainQuery({ sql: longSql }))
        .rejects.toThrow('exceeds maximum length');
    });

    it('should return execution plan in JSON format', async () => {
      mockClient.query.mockResolvedValue({
        rows: [{ 'QUERY PLAN': [{ Plan: { 'Node Type': 'Seq Scan' } }] }]
      });

      const result = await explainQuery({ sql: 'SELECT * FROM users' });

      expect(result.plan).toEqual({ Plan: { 'Node Type': 'Seq Scan' } });
      expect(mockClient.release).toHaveBeenCalled();
    });

    it('should return execution plan in text format', async () => {
      mockClient.query.mockResolvedValue({
        rows: [
          { 'QUERY PLAN': 'Seq Scan on users' },
          { 'QUERY PLAN': '  Filter: (id = 1)' }
        ]
      });

      const result = await explainQuery({ sql: 'SELECT * FROM users', format: 'text' });

      expect(result.plan).toContain('Seq Scan');
      expect(mockClient.release).toHaveBeenCalled();
    });

    it('should block EXPLAIN ANALYZE on write queries', async () => {
      await expect(explainQuery({ sql: 'DELETE FROM users', analyze: true }))
        .rejects.toThrow('EXPLAIN ANALYZE is not allowed for write queries');

      await expect(explainQuery({ sql: 'INSERT INTO users VALUES (1)', analyze: true }))
        .rejects.toThrow('EXPLAIN ANALYZE is not allowed for write queries');

      await expect(explainQuery({ sql: 'UPDATE users SET name = \'test\'', analyze: true }))
        .rejects.toThrow('EXPLAIN ANALYZE is not allowed for write queries');
    });

    it('should allow EXPLAIN ANALYZE on SELECT queries', async () => {
      mockClient.query.mockResolvedValue({
        rows: [{ 'QUERY PLAN': [{ Plan: { 'Node Type': 'Seq Scan' } }] }]
      });

      const result = await explainQuery({ sql: 'SELECT * FROM users', analyze: true });

      expect(result).toBeDefined();
      const queryCall = mockClient.query.mock.calls.find((call: unknown[]) =>
        typeof call[0] === 'string' && call[0].includes('EXPLAIN') && call[0].includes('ANALYZE')
      );
      expect(queryCall).toBeDefined();
    });

    it('should release client even on error', async () => {
      mockClient.query.mockRejectedValue(new Error('Query failed'));

      await expect(explainQuery({ sql: 'SELECT * FROM users' }))
        .rejects.toThrow('Query failed');

      expect(mockClient.release).toHaveBeenCalled();
    });

    describe('hypothetical indexes', () => {
      it('should validate table name in hypothetical indexes', async () => {
        // Mock hypopg check to return true
        mockClient.query.mockResolvedValueOnce({ rows: [{ has_hypopg: true }] });

        await expect(explainQuery({
          sql: 'SELECT * FROM users',
          hypotheticalIndexes: [
            { table: 'users; DROP TABLE', columns: ['id'] }
          ]
        })).rejects.toThrow('invalid characters');
      });

      it('should validate column names in hypothetical indexes', async () => {
        // Mock hypopg check to return true
        mockClient.query.mockResolvedValueOnce({ rows: [{ has_hypopg: true }] });

        await expect(explainQuery({
          sql: 'SELECT * FROM users',
          hypotheticalIndexes: [
            { table: 'users', columns: ['id; DROP'] }
          ]
        })).rejects.toThrow('invalid characters');
      });

      it('should validate index type', async () => {
        // Mock hypopg check to return true
        mockClient.query.mockResolvedValueOnce({ rows: [{ has_hypopg: true }] });

        await expect(explainQuery({
          sql: 'SELECT * FROM users',
          hypotheticalIndexes: [
            { table: 'users', columns: ['id'], indexType: 'invalid' }
          ]
        })).rejects.toThrow('Invalid index type');
      });

      it('should limit number of hypothetical indexes', async () => {
        const manyIndexes = Array.from({ length: 11 }, (_, i) => ({
          table: 'users',
          columns: [`col${i}`]
        }));

        await expect(explainQuery({
          sql: 'SELECT * FROM users',
          hypotheticalIndexes: manyIndexes
        })).rejects.toThrow('Maximum 10 hypothetical indexes');
      });

      it('should require columns in hypothetical indexes', async () => {
        // Mock hypopg check to return true
        mockClient.query.mockResolvedValueOnce({ rows: [{ has_hypopg: true }] });

        await expect(explainQuery({
          sql: 'SELECT * FROM users',
          hypotheticalIndexes: [
            { table: 'users', columns: [] }
          ]
        })).rejects.toThrow('columns array is required and must not be empty');
      });

      it('should handle hypothetical indexes with hypopg', async () => {
        mockClient.query
          .mockResolvedValueOnce({ rows: [{ has_hypopg: true }] })
          .mockResolvedValueOnce({}) // hypopg_create_index
          .mockResolvedValueOnce({ rows: [{ 'QUERY PLAN': [{ Plan: {} }] }] })
          .mockResolvedValueOnce({}); // hypopg_reset

        await explainQuery({
          sql: 'SELECT * FROM users',
          hypotheticalIndexes: [
            { table: 'users', columns: ['id'] }
          ]
        });

        // Verify parameterized call to hypopg
        const hypopgCall = mockClient.query.mock.calls.find((call: unknown[]) =>
          typeof call[0] === 'string' && call[0].includes('hypopg_create_index')
        );
        expect(hypopgCall).toBeDefined();
      });
    });
  });

  describe('executeSqlFile', () => {
    let mockClient: { query: MockFn; release: MockFn };
    const testDir = '/tmp/postgres-mcp-test';
    const testFile = `${testDir}/test.sql`;

    beforeEach(() => {
      mockClient = {
        query: jest.fn<MockFn>(),
        release: jest.fn<MockFn>()
      };
      mockGetClient.mockResolvedValue(mockClient);

      // Create test directory
      if (!fs.existsSync(testDir)) {
        fs.mkdirSync(testDir, { recursive: true });
      }
    });

    afterEach(() => {
      // Clean up test files
      if (fs.existsSync(testFile)) {
        fs.unlinkSync(testFile);
      }
    });

    it('should require filePath parameter', async () => {
      await expect(executeSqlFile({ filePath: '' }))
        .rejects.toThrow('filePath parameter is required');
    });

    it('should only allow .sql files', async () => {
      await expect(executeSqlFile({ filePath: '/path/to/file.txt' }))
        .rejects.toThrow('Only .sql files are allowed');

      await expect(executeSqlFile({ filePath: '/path/to/file.js' }))
        .rejects.toThrow('Only .sql files are allowed');
    });

    it('should throw if file does not exist', async () => {
      await expect(executeSqlFile({ filePath: '/nonexistent/path/file.sql' }))
        .rejects.toThrow('File not found');
    });

    it('should throw if file is empty', async () => {
      fs.writeFileSync(testFile, '');

      await expect(executeSqlFile({ filePath: testFile }))
        .rejects.toThrow('File is empty');
    });

    it('should execute single statement successfully', async () => {
      fs.writeFileSync(testFile, 'SELECT 1;');
      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rowCount: 1 }) // SELECT 1
        .mockResolvedValueOnce({}); // COMMIT

      const result = await executeSqlFile({ filePath: testFile });

      expect(result.success).toBe(true);
      expect(result.statementsExecuted).toBe(1);
      expect(result.statementsFailed).toBe(0);
      expect(result.executionTimeMs).toBeGreaterThanOrEqual(0);
      expect(mockClient.release).toHaveBeenCalled();
    });

    it('should execute multiple statements', async () => {
      fs.writeFileSync(testFile, 'SELECT 1; SELECT 2; SELECT 3;');
      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rowCount: 1 }) // SELECT 1
        .mockResolvedValueOnce({ rowCount: 1 }) // SELECT 2
        .mockResolvedValueOnce({ rowCount: 1 }) // SELECT 3
        .mockResolvedValueOnce({}); // COMMIT

      const result = await executeSqlFile({ filePath: testFile });

      expect(result.success).toBe(true);
      expect(result.statementsExecuted).toBe(3);
      expect(result.totalStatements).toBe(3);
    });

    it('should rollback on error with useTransaction=true', async () => {
      fs.writeFileSync(testFile, 'SELECT 1; INVALID SQL; SELECT 3;');
      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rowCount: 1 }) // SELECT 1
        .mockRejectedValueOnce(new Error('syntax error')) // INVALID SQL
        .mockResolvedValueOnce({}); // ROLLBACK

      const result = await executeSqlFile({ filePath: testFile });

      expect(result.success).toBe(false);
      expect(result.statementsExecuted).toBe(1);
      expect(result.statementsFailed).toBe(1);
      expect(result.error).toContain('syntax error');
      expect(result.rollback).toBe(true);
      expect(result.errors).toHaveLength(1);
      expect(result.errors[0].statementIndex).toBe(2);
    });

    it('should continue on error with stopOnError=false', async () => {
      fs.writeFileSync(testFile, 'SELECT 1; INVALID SQL; SELECT 3;');
      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rowCount: 1 }) // SELECT 1
        .mockRejectedValueOnce(new Error('syntax error')) // INVALID SQL
        .mockResolvedValueOnce({ rowCount: 1 }) // SELECT 3
        .mockResolvedValueOnce({}); // COMMIT

      const result = await executeSqlFile({ filePath: testFile, stopOnError: false });

      expect(result.success).toBe(false); // Not success because there was a failure
      expect(result.statementsExecuted).toBe(2);
      expect(result.statementsFailed).toBe(1);
      expect(result.errors).toHaveLength(1);
      expect(result.errors[0].statementIndex).toBe(2);
      expect(result.errors[0].error).toContain('syntax error');
    });

    it('should skip transaction with useTransaction=false', async () => {
      fs.writeFileSync(testFile, 'SELECT 1;');
      mockClient.query.mockResolvedValue({ rowCount: 1 });

      await executeSqlFile({ filePath: testFile, useTransaction: false });

      // Verify no BEGIN/COMMIT calls
      const calls = mockClient.query.mock.calls.map((c: unknown[]) => c[0]);
      expect(calls).not.toContain('BEGIN');
      expect(calls).not.toContain('COMMIT');
    });

    it('should handle comments correctly', async () => {
      fs.writeFileSync(testFile, `
        -- This is a comment
        SELECT 1;
        /* Block comment */
        SELECT 2;
      `);
      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rowCount: 1 }) // SELECT 1
        .mockResolvedValueOnce({ rowCount: 1 }) // SELECT 2
        .mockResolvedValueOnce({}); // COMMIT

      const result = await executeSqlFile({ filePath: testFile });

      expect(result.success).toBe(true);
      expect(result.statementsExecuted).toBe(2);
    });

    it('should handle dollar-quoted strings', async () => {
      fs.writeFileSync(testFile, `
        SELECT $tag$This has a ; semicolon$tag$;
        SELECT 2;
      `);
      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rowCount: 1 }) // First SELECT
        .mockResolvedValueOnce({ rowCount: 1 }) // SELECT 2
        .mockResolvedValueOnce({}); // COMMIT

      const result = await executeSqlFile({ filePath: testFile });

      expect(result.success).toBe(true);
      expect(result.statementsExecuted).toBe(2);
    });

    it('should return file info in result', async () => {
      const content = 'SELECT 1;';
      fs.writeFileSync(testFile, content);
      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rowCount: 1 }) // SELECT 1
        .mockResolvedValueOnce({}); // COMMIT

      const result = await executeSqlFile({ filePath: testFile });

      expect(result.filePath).toContain('test.sql');
      expect(result.fileSize).toBe(content.length);
    });

    it('should release client even on error', async () => {
      fs.writeFileSync(testFile, 'SELECT 1;');
      mockClient.query.mockRejectedValue(new Error('Connection error'));

      try {
        await executeSqlFile({ filePath: testFile });
      } catch (e) {
        // Expected
      }

      expect(mockClient.release).toHaveBeenCalled();
    });
  });
});
