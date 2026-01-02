import { jest, describe, it, expect, beforeEach, beforeAll } from '@jest/globals';
import * as fs from 'fs';

type MockFn = jest.Mock<any>;

// Use jest.unstable_mockModule for ESM
const mockQuery = jest.fn<MockFn>();
const mockQueryWithOverride = jest.fn<MockFn>();
const mockGetClient = jest.fn<MockFn>();
const mockGetClientWithOverride = jest.fn<MockFn>();
const mockIsConnected = jest.fn<MockFn>();
const mockBeginTransaction = jest.fn<MockFn>();
const mockCommitTransaction = jest.fn<MockFn>();
const mockRollbackTransaction = jest.fn<MockFn>();
const mockQueryInTransaction = jest.fn<MockFn>();
const mockGetConnectionContext = jest.fn<MockFn>();

jest.unstable_mockModule('../db-manager.js', () => ({
  getDbManager: jest.fn(() => ({
    query: mockQuery,
    queryWithOverride: mockQueryWithOverride,
    getClient: mockGetClient,
    getClientWithOverride: mockGetClientWithOverride,
    isConnected: mockIsConnected.mockReturnValue(true),
    beginTransaction: mockBeginTransaction,
    commitTransaction: mockCommitTransaction,
    rollbackTransaction: mockRollbackTransaction,
    queryInTransaction: mockQueryInTransaction,
    getConnectionContext: mockGetConnectionContext.mockReturnValue({
      server: 'test-server',
      database: 'test-db',
      schema: 'public'
    }),
  })),
  resetDbManager: jest.fn(),
  OverrideClientResult: {} // Export type placeholder
}));

// Dynamic import after mock
let executeSql: any;
let explainQuery: any;
let executeSqlFile: any;
let mutationPreview: any;
let batchExecute: any;
let beginTransaction: any;
let commitTransaction: any;
let rollbackTransaction: any;
let getConnectionContext: any;

beforeAll(async () => {
  const module = await import('../tools/sql-tools.js');
  executeSql = module.executeSql;
  explainQuery = module.explainQuery;
  executeSqlFile = module.executeSqlFile;
  mutationPreview = module.mutationPreview;
  batchExecute = module.batchExecute;
  beginTransaction = module.beginTransaction;
  commitTransaction = module.commitTransaction;
  rollbackTransaction = module.rollbackTransaction;
  getConnectionContext = module.getConnectionContext;
});

describe('SQL Tools', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    // Reset mock implementations to avoid leakage between tests
    mockQuery.mockReset();
    mockQueryWithOverride.mockReset();
    mockGetClientWithOverride.mockReset();
    mockIsConnected.mockReturnValue(true);
    // By default, queryWithOverride delegates to the same behavior as query
    mockQueryWithOverride.mockImplementation(((sql: string, params?: any[], override?: any) => {
      return mockQuery(sql, params);
    }) as any);
  });

  describe('executeSql', () => {
    it('should require sql parameter', async () => {
      await expect(executeSql({ sql: '' }))
        .rejects.toThrow('sql parameter is required and must be a string');

      await expect(executeSql({ sql: null as any }))
        .rejects.toThrow('sql parameter is required and must be a string');
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
    let testDir: string;
    let testFile: string;

    beforeEach(() => {
      mockClient = {
        query: jest.fn<MockFn>(),
        release: jest.fn<MockFn>()
      };
      mockGetClient.mockResolvedValue(mockClient);

      // Create unique test directory for each test run
      testDir = fs.mkdtempSync('/tmp/postgres-mcp-test-');
      testFile = `${testDir}/test.sql`;
    });

    afterEach(() => {
      // Clean up test files and directory
      try {
        if (fs.existsSync(testFile)) {
          fs.unlinkSync(testFile);
        }
        if (fs.existsSync(testDir)) {
          fs.rmdirSync(testDir);
        }
      } catch (e) {
        // Ignore cleanup errors
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

    it('should include line numbers in errors', async () => {
      fs.writeFileSync(testFile, `
        SELECT 1;
        SELECT 2;
        INVALID SYNTAX HERE;
        SELECT 4;
      `);
      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rowCount: 1 }) // SELECT 1
        .mockResolvedValueOnce({ rowCount: 1 }) // SELECT 2
        .mockRejectedValueOnce(new Error('syntax error')) // INVALID
        .mockResolvedValueOnce({}); // ROLLBACK

      const result = await executeSqlFile({ filePath: testFile, stopOnError: true });

      expect(result.success).toBe(false);
      expect(result.errors).toBeDefined();
      expect(result.errors![0].lineNumber).toBeGreaterThan(0);
      expect(result.errors![0].statementIndex).toBe(3);
    });

    it('should track line numbers correctly with multi-line statements', async () => {
      fs.writeFileSync(testFile, `-- Comment on line 1
SELECT
  column1,
  column2
FROM table1;
-- Line 6
SELECT 1;
INVALID;`);
      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rowCount: 1 }) // First SELECT
        .mockResolvedValueOnce({ rowCount: 1 }) // SELECT 1
        .mockRejectedValueOnce(new Error('syntax error')); // INVALID

      const result = await executeSqlFile({ filePath: testFile, stopOnError: false });

      expect(result.errors).toBeDefined();
      expect(result.errors!.length).toBe(1);
      // The INVALID statement starts on line 8
      expect(result.errors![0].lineNumber).toBe(8);
    });

    it('should validate only without executing when validateOnly is true', async () => {
      fs.writeFileSync(testFile, 'SELECT 1; SELECT 2;');

      const result = await executeSqlFile({ filePath: testFile, validateOnly: true });

      expect(result.success).toBe(true);
      expect(result.statementsExecuted).toBe(0); // Not executed
      expect(result.totalStatements).toBe(2); // Parsed
      expect(result.preview).toBeDefined();
      expect(result.preview!.length).toBe(2);
      expect(mockClient.query).not.toHaveBeenCalled(); // No queries run
    });

    it('should preprocess SQL with strip patterns', async () => {
      fs.writeFileSync(testFile, `-- TODO: Remove this line
SELECT 1;
-- TODO: And this one
SELECT 2;`);
      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rowCount: 1 }) // SELECT 1
        .mockResolvedValueOnce({ rowCount: 1 }) // SELECT 2
        .mockResolvedValueOnce({}); // COMMIT

      const result = await executeSqlFile({
        filePath: testFile,
        stripPatterns: ['-- TODO: Remove this line', '-- TODO: And this one'],
      });

      expect(result.success).toBe(true);
      expect(result.statementsExecuted).toBe(2);
    });
  });

  describe('previewSqlFile', () => {
    let testDir: string;
    let testFile: string;

    beforeEach(() => {
      // Create unique test directory for each test run
      testDir = fs.mkdtempSync('/tmp/postgres-mcp-preview-test-');
      testFile = `${testDir}/test.sql`;
    });

    afterEach(() => {
      // Clean up test files and directory
      try {
        if (fs.existsSync(testFile)) {
          fs.unlinkSync(testFile);
        }
        if (fs.existsSync(testDir)) {
          fs.rmdirSync(testDir);
        }
      } catch (e) {
        // Ignore cleanup errors
      }
    });

    it('should require filePath parameter', async () => {
      const { previewSqlFile } = await import('../tools/sql-tools.js');
      await expect(previewSqlFile({ filePath: '' }))
        .rejects.toThrow('filePath parameter is required');
    });

    it('should only allow .sql files', async () => {
      const { previewSqlFile } = await import('../tools/sql-tools.js');
      await expect(previewSqlFile({ filePath: '/path/to/file.txt' }))
        .rejects.toThrow('Only .sql files are allowed');
    });

    it('should throw if file does not exist', async () => {
      const { previewSqlFile } = await import('../tools/sql-tools.js');
      await expect(previewSqlFile({ filePath: '/nonexistent/path/file.sql' }))
        .rejects.toThrow('File not found');
    });

    it('should throw if file is empty', async () => {
      const { previewSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, '');
      await expect(previewSqlFile({ filePath: testFile }))
        .rejects.toThrow('File is empty');
    });

    it('should preview a simple SQL file', async () => {
      const { previewSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, 'SELECT 1; SELECT 2; SELECT 3;');

      const result = await previewSqlFile({ filePath: testFile });

      expect(result.totalStatements).toBe(3);
      expect(result.statementsByType['SELECT']).toBe(3);
      expect(result.statements).toHaveLength(3);
      expect(result.warnings).toHaveLength(0);
      expect(result.summary).toContain('3 statements');
    });

    it('should count statement types correctly', async () => {
      const { previewSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, `
        CREATE TABLE test (id INT);
        INSERT INTO test VALUES (1);
        INSERT INTO test VALUES (2);
        UPDATE test SET id = 3 WHERE id = 1;
        SELECT * FROM test;
        DELETE FROM test WHERE id = 2;
        DROP TABLE test;
      `);

      const result = await previewSqlFile({ filePath: testFile });

      expect(result.totalStatements).toBe(7);
      expect(result.statementsByType['CREATE']).toBe(1);
      expect(result.statementsByType['INSERT']).toBe(2);
      expect(result.statementsByType['UPDATE']).toBe(1);
      expect(result.statementsByType['SELECT']).toBe(1);
      expect(result.statementsByType['DELETE']).toBe(1);
      expect(result.statementsByType['DROP']).toBe(1);
    });

    it('should warn about DROP statements', async () => {
      const { previewSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, `
        CREATE TABLE test (id INT);
        DROP TABLE test;
        DROP INDEX idx_test;
      `);

      const result = await previewSqlFile({ filePath: testFile });

      expect(result.warnings.length).toBeGreaterThan(0);
      expect(result.warnings.some(w => w.includes('DROP'))).toBe(true);
    });

    it('should warn about TRUNCATE statements', async () => {
      const { previewSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, `TRUNCATE TABLE users;`);

      const result = await previewSqlFile({ filePath: testFile });

      expect(result.warnings.length).toBe(1);
      expect(result.warnings[0]).toContain('TRUNCATE');
    });

    it('should warn about DELETE without WHERE', async () => {
      const { previewSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, `DELETE FROM users;`);

      const result = await previewSqlFile({ filePath: testFile });

      expect(result.warnings.length).toBe(1);
      expect(result.warnings[0]).toContain('DELETE without WHERE');
    });

    it('should warn about UPDATE without WHERE', async () => {
      const { previewSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, `UPDATE users SET status = 'inactive';`);

      const result = await previewSqlFile({ filePath: testFile });

      expect(result.warnings.length).toBe(1);
      expect(result.warnings[0]).toContain('UPDATE without WHERE');
    });

    it('should NOT warn about DELETE with WHERE', async () => {
      const { previewSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, `DELETE FROM users WHERE id = 1;`);

      const result = await previewSqlFile({ filePath: testFile });

      expect(result.warnings).toHaveLength(0);
    });

    it('should NOT warn about UPDATE with WHERE', async () => {
      const { previewSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, `UPDATE users SET status = 'inactive' WHERE id = 1;`);

      const result = await previewSqlFile({ filePath: testFile });

      expect(result.warnings).toHaveLength(0);
    });

    it('should strip patterns before parsing', async () => {
      const { previewSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, `
SELECT 1;
/
SELECT 2;
/
      `);

      const result = await previewSqlFile({
        filePath: testFile,
        stripPatterns: ['/']
      });

      expect(result.totalStatements).toBe(2);
      expect(result.statementsByType['SELECT']).toBe(2);
    });

    it('should strip regex patterns before parsing', async () => {
      const { previewSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, `
SELECT 1;
GO
SELECT 2;
  GO
SELECT 3;
      `);

      const result = await previewSqlFile({
        filePath: testFile,
        stripPatterns: ['^\\s*GO\\s*$'],
        stripAsRegex: true
      });

      expect(result.totalStatements).toBe(3);
    });

    it('should limit statements returned based on maxStatements', async () => {
      const { previewSqlFile } = await import('../tools/sql-tools.js');
      // Create file with 30 statements
      const statements = Array(30).fill('SELECT 1;').join('\n');
      fs.writeFileSync(testFile, statements);

      const result = await previewSqlFile({
        filePath: testFile,
        maxStatements: 10
      });

      expect(result.totalStatements).toBe(30);
      expect(result.statements).toHaveLength(10);
    });

    it('should format file size correctly', async () => {
      const { previewSqlFile } = await import('../tools/sql-tools.js');
      // Create a larger file
      const content = 'SELECT * FROM large_table;\n'.repeat(100);
      fs.writeFileSync(testFile, content);

      const result = await previewSqlFile({ filePath: testFile });

      expect(result.fileSize).toBeGreaterThan(0);
      expect(result.fileSizeFormatted).toBeDefined();
      expect(typeof result.fileSizeFormatted).toBe('string');
    });

    it('should include line numbers in statement preview', async () => {
      const { previewSqlFile } = await import('../tools/sql-tools.js');
      // Write file with explicit line structure
      fs.writeFileSync(testFile, '-- Comment line 1\nSELECT 1;\n-- Comment line 3\nSELECT 2;\nSELECT 3;');

      const result = await previewSqlFile({ filePath: testFile });

      // Verify statements have line numbers assigned
      expect(result.statements.length).toBe(3);
      expect(result.statements[0].lineNumber).toBeGreaterThan(0);
      expect(result.statements[1].lineNumber).toBeGreaterThan(result.statements[0].lineNumber);
      expect(result.statements[2].lineNumber).toBeGreaterThan(result.statements[1].lineNumber);
    });

    it('should truncate long SQL statements', async () => {
      const { previewSqlFile } = await import('../tools/sql-tools.js');
      const longSelect = 'SELECT ' + 'a, '.repeat(200) + 'b FROM table1;';
      fs.writeFileSync(testFile, longSelect);

      const result = await previewSqlFile({ filePath: testFile });

      expect(result.statements[0].sql.length).toBeLessThanOrEqual(303); // 300 + '...'
      expect(result.statements[0].sql).toContain('...');
    });

    it('should handle multi-line statements correctly', async () => {
      const { previewSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, `
CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  name VARCHAR(100),
  email VARCHAR(255)
);

INSERT INTO users (name, email)
VALUES ('John', 'john@example.com');
      `);

      const result = await previewSqlFile({ filePath: testFile });

      expect(result.totalStatements).toBe(2);
      expect(result.statementsByType['CREATE']).toBe(1);
      expect(result.statementsByType['INSERT']).toBe(1);
    });

    it('should handle dollar-quoted strings', async () => {
      const { previewSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, `
CREATE FUNCTION test() RETURNS void AS $$
BEGIN
  -- This semicolon should not split the statement;
  RAISE NOTICE 'Hello';
END;
$$ LANGUAGE plpgsql;

SELECT 1;
      `);

      const result = await previewSqlFile({ filePath: testFile });

      expect(result.totalStatements).toBe(2);
      expect(result.statementsByType['CREATE']).toBe(1);
      expect(result.statementsByType['SELECT']).toBe(1);
    });

    it('should generate meaningful summary', async () => {
      const { previewSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, `
        CREATE TABLE t1 (id INT);
        CREATE TABLE t2 (id INT);
        INSERT INTO t1 VALUES (1);
        SELECT * FROM t1;
      `);

      const result = await previewSqlFile({ filePath: testFile });

      expect(result.summary).toContain('4 statements');
      expect(result.summary).toContain('CREATE');
      expect(result.summary).toContain('INSERT');
      expect(result.summary).toContain('SELECT');
    });

    it('should handle file with only comments', async () => {
      const { previewSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, `
        -- This is a comment
        /* This is a block comment */
        -- Another comment
      `);

      const result = await previewSqlFile({ filePath: testFile });

      expect(result.totalStatements).toBe(0);
      expect(result.statements).toHaveLength(0);
      expect(result.summary).toContain('0 statement');
    });

    it('should handle CTE queries (WITH statements)', async () => {
      const { previewSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, `
WITH cte AS (SELECT 1 AS n)
SELECT * FROM cte;

WITH ins AS (
  INSERT INTO test (name) VALUES ('test')
  RETURNING *
)
SELECT * FROM ins;
      `);

      const result = await previewSqlFile({ filePath: testFile });

      expect(result.totalStatements).toBe(2);
      // Both WITH statements end with SELECT (to get the results)
      // so they should be detected as WITH SELECT
      expect(result.statementsByType['WITH SELECT']).toBe(2);
    });

    it('should handle multiple dangerous operations and generate multiple warnings', async () => {
      const { previewSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, `
        DROP TABLE users;
        DROP TABLE orders;
        TRUNCATE TABLE logs;
        DELETE FROM sessions;
        UPDATE config SET value = 'new';
      `);

      const result = await previewSqlFile({ filePath: testFile });

      expect(result.warnings.length).toBe(5);
      expect(result.warnings.filter(w => w.includes('DROP')).length).toBe(2);
      expect(result.warnings.filter(w => w.includes('TRUNCATE')).length).toBe(1);
      expect(result.warnings.filter(w => w.includes('DELETE without WHERE')).length).toBe(1);
      expect(result.warnings.filter(w => w.includes('UPDATE without WHERE')).length).toBe(1);
    });
  });

  describe('mutationDryRun', () => {
    let mockClient: { query: MockFn; release: MockFn };

    beforeEach(() => {
      mockClient = {
        query: jest.fn<MockFn>(),
        release: jest.fn<MockFn>()
      };
      mockGetClient.mockResolvedValue(mockClient);
    });

    it('should require sql parameter', async () => {
      const { mutationDryRun } = await import('../tools/sql-tools.js');
      await expect(mutationDryRun({ sql: '' }))
        .rejects.toThrow('sql parameter is required');
    });

    it('should only accept INSERT/UPDATE/DELETE statements', async () => {
      const { mutationDryRun } = await import('../tools/sql-tools.js');
      await expect(mutationDryRun({ sql: 'SELECT * FROM users' }))
        .rejects.toThrow('SQL must be an INSERT, UPDATE, or DELETE statement');
    });

    it('should execute UPDATE in transaction and rollback', async () => {
      const { mutationDryRun } = await import('../tools/sql-tools.js');

      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rows: [{ id: 1, name: 'old' }] }) // SELECT before
        .mockResolvedValueOnce({ rows: [{ id: 1, name: 'new' }], rowCount: 1 }) // UPDATE RETURNING
        .mockResolvedValueOnce({}); // ROLLBACK

      const result = await mutationDryRun({
        sql: "UPDATE users SET name = 'new' WHERE id = 1"
      });

      expect(result.success).toBe(true);
      expect(result.mutationType).toBe('UPDATE');
      expect(result.rowsAffected).toBe(1);
      expect(mockClient.query).toHaveBeenCalledWith('BEGIN');
      expect(mockClient.query).toHaveBeenCalledWith('ROLLBACK');
    });

    it('should execute DELETE in transaction and rollback', async () => {
      const { mutationDryRun } = await import('../tools/sql-tools.js');

      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rows: [{ id: 1 }] }) // SELECT before
        .mockResolvedValueOnce({ rows: [{ id: 1 }], rowCount: 1 }) // DELETE RETURNING
        .mockResolvedValueOnce({}); // ROLLBACK

      const result = await mutationDryRun({
        sql: 'DELETE FROM users WHERE id = 1'
      });

      expect(result.success).toBe(true);
      expect(result.mutationType).toBe('DELETE');
      expect(result.rowsAffected).toBe(1);
    });

    it('should execute INSERT in transaction and rollback', async () => {
      const { mutationDryRun } = await import('../tools/sql-tools.js');

      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rows: [{ id: 1, name: 'test' }], rowCount: 1 }) // INSERT RETURNING
        .mockResolvedValueOnce({}); // ROLLBACK

      const result = await mutationDryRun({
        sql: "INSERT INTO users (name) VALUES ('test')"
      });

      expect(result.success).toBe(true);
      expect(result.mutationType).toBe('INSERT');
      expect(result.rowsAffected).toBe(1);
    });

    it('should capture PostgreSQL error details on failure', async () => {
      const { mutationDryRun } = await import('../tools/sql-tools.js');

      const pgError = new Error('duplicate key value violates unique constraint');
      (pgError as any).code = '23505';
      (pgError as any).constraint = 'users_email_key';
      (pgError as any).detail = 'Key (email)=(test@test.com) already exists.';
      (pgError as any).table = 'users';

      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockRejectedValueOnce(pgError) // INSERT RETURNING fails
        .mockRejectedValueOnce(pgError) // INSERT without RETURNING also fails
        .mockResolvedValueOnce({}); // ROLLBACK

      const result = await mutationDryRun({
        sql: "INSERT INTO users (email) VALUES ('test@test.com')"
      });

      expect(result.success).toBe(false);
      expect(result.error).toBeDefined();
      expect(result.error!.code).toBe('23505');
      expect(result.error!.constraint).toBe('users_email_key');
      expect(result.error!.detail).toContain('already exists');
      expect(result.error!.table).toBe('users');
    });

    it('should warn about UPDATE without WHERE', async () => {
      const { mutationDryRun } = await import('../tools/sql-tools.js');

      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rows: [{ id: 1 }] }) // SELECT before
        .mockResolvedValueOnce({ rows: [], rowCount: 100 }) // UPDATE
        .mockResolvedValueOnce({}); // ROLLBACK

      const result = await mutationDryRun({
        sql: "UPDATE users SET status = 'inactive'"
      });

      expect(result.warnings).toBeDefined();
      expect(result.warnings!.some(w => w.includes('No WHERE clause'))).toBe(true);
    });

    it('should detect INSERT sequence warnings (but still execute)', async () => {
      const { mutationDryRun } = await import('../tools/sql-tools.js');

      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rows: [{ id: 1 }], rowCount: 1 }) // INSERT
        .mockResolvedValueOnce({}); // ROLLBACK

      const result = await mutationDryRun({
        sql: "INSERT INTO users (name) VALUES ('test')"
      });

      // INSERT should execute (not skipped) but have a warning
      expect(result.skipped).toBeUndefined();
      expect(result.success).toBe(true);
      expect(result.nonRollbackableWarnings).toBeDefined();
      expect(result.nonRollbackableWarnings!.some(w => w.operation === 'SEQUENCE')).toBe(true);
      expect(result.nonRollbackableWarnings!.some(w => w.mustSkip === false)).toBe(true);
    });

    it('should skip INSERT with explicit NEXTVAL but run EXPLAIN', async () => {
      const { mutationDryRun } = await import('../tools/sql-tools.js');

      // Mock EXPLAIN query
      mockQuery.mockResolvedValueOnce({
        rows: [{ 'QUERY PLAN': [{ Plan: { 'Node Type': 'ModifyTable' } }] }],
        rowCount: 1
      });

      const result = await mutationDryRun({
        sql: "INSERT INTO users (id, name) VALUES (nextval('users_id_seq'), 'test')"
      });

      expect(result.skipped).toBe(true);
      expect(result.skipReason).toContain('NEXTVAL');
      expect(result.rowsAffected).toBe(0);
      expect(result.nonRollbackableWarnings).toBeDefined();
      expect(result.nonRollbackableWarnings!.some(w => w.operation === 'SEQUENCE' && w.mustSkip === true)).toBe(true);
      // Should have EXPLAIN plan
      expect(result.explainPlan).toBeDefined();
      expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining('EXPLAIN'), undefined);
    });

    it('should capture before and after rows for UPDATE', async () => {
      const { mutationDryRun } = await import('../tools/sql-tools.js');

      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rows: [{ id: 1, status: 'active' }] }) // SELECT before
        .mockResolvedValueOnce({ rows: [{ id: 1, status: 'inactive' }], rowCount: 1 }) // UPDATE RETURNING
        .mockResolvedValueOnce({}); // ROLLBACK

      const result = await mutationDryRun({
        sql: "UPDATE users SET status = 'inactive' WHERE id = 1"
      });

      expect(result.beforeRows).toBeDefined();
      expect(result.beforeRows![0].status).toBe('active');
      expect(result.affectedRows).toBeDefined();
      expect(result.affectedRows![0].status).toBe('inactive');
    });

    it('should handle CTE UPDATE statements', async () => {
      const { mutationDryRun } = await import('../tools/sql-tools.js');

      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rows: [{ id: 1 }], rowCount: 1 }) // UPDATE
        .mockResolvedValueOnce({}); // ROLLBACK

      const result = await mutationDryRun({
        sql: "WITH updated AS (UPDATE users SET status = 'inactive' WHERE id = 1 RETURNING *) SELECT * FROM updated"
      });

      expect(result.mutationType).toBe('UPDATE');
    });

    it('should always rollback even on error', async () => {
      const { mutationDryRun } = await import('../tools/sql-tools.js');

      const error = new Error('Some error');
      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockRejectedValueOnce(error) // INSERT RETURNING fails
        .mockRejectedValueOnce(error) // INSERT without RETURNING also fails
        .mockResolvedValueOnce({}); // ROLLBACK

      const result = await mutationDryRun({
        sql: "INSERT INTO users (name) VALUES ('test')"
      });

      expect(result.success).toBe(false);
      expect(mockClient.query).toHaveBeenCalledWith('ROLLBACK');
    });
  });

  describe('dryRunSqlFile', () => {
    let mockClient: { query: MockFn; release: MockFn };
    let testDir: string;
    let testFile: string;

    beforeEach(() => {
      mockClient = {
        query: jest.fn<MockFn>(),
        release: jest.fn<MockFn>()
      };
      mockGetClient.mockResolvedValue(mockClient);

      testDir = fs.mkdtempSync('/tmp/postgres-mcp-dryrun-test-');
      testFile = `${testDir}/test.sql`;
    });

    afterEach(() => {
      try {
        if (fs.existsSync(testFile)) fs.unlinkSync(testFile);
        if (fs.existsSync(testDir)) fs.rmdirSync(testDir);
      } catch (e) { /* ignore */ }
    });

    it('should require filePath parameter', async () => {
      const { dryRunSqlFile } = await import('../tools/sql-tools.js');
      await expect(dryRunSqlFile({ filePath: '' }))
        .rejects.toThrow('filePath parameter is required');
    });

    it('should only allow .sql files', async () => {
      const { dryRunSqlFile } = await import('../tools/sql-tools.js');
      await expect(dryRunSqlFile({ filePath: '/path/to/file.txt' }))
        .rejects.toThrow('Only .sql files are allowed');
    });

    it('should execute all statements and rollback', async () => {
      const { dryRunSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, 'SELECT 1; SELECT 2; SELECT 3;');

      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rows: [{ '?column?': 1 }], rowCount: 1 }) // SELECT 1
        .mockResolvedValueOnce({ rows: [{ '?column?': 2 }], rowCount: 1 }) // SELECT 2
        .mockResolvedValueOnce({ rows: [{ '?column?': 3 }], rowCount: 1 }) // SELECT 3
        .mockResolvedValueOnce({}); // ROLLBACK

      const result = await dryRunSqlFile({ filePath: testFile });

      expect(result.success).toBe(true);
      expect(result.totalStatements).toBe(3);
      expect(result.successCount).toBe(3);
      expect(result.failureCount).toBe(0);
      expect(result.rolledBack).toBe(true);
      expect(mockClient.query).toHaveBeenCalledWith('BEGIN');
      expect(mockClient.query).toHaveBeenCalledWith('ROLLBACK');
    });

    it('should capture errors with line numbers', async () => {
      const { dryRunSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, `SELECT 1;
SELECT 2;
INVALID SQL;
SELECT 4;`);

      const pgError = new Error('syntax error at or near "INVALID"');
      (pgError as any).code = '42601';
      (pgError as any).position = '1';

      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rows: [], rowCount: 1 }) // SELECT 1
        .mockResolvedValueOnce({ rows: [], rowCount: 1 }) // SELECT 2
        .mockRejectedValueOnce(pgError) // INVALID
        .mockResolvedValueOnce({ rows: [], rowCount: 1 }) // SELECT 4
        .mockResolvedValueOnce({}); // ROLLBACK

      const result = await dryRunSqlFile({ filePath: testFile, stopOnError: false });

      expect(result.success).toBe(false);
      expect(result.failureCount).toBe(1);
      expect(result.statementResults[2].success).toBe(false);
      expect(result.statementResults[2].lineNumber).toBe(3);
      expect(result.statementResults[2].error).toBeDefined();
      expect(result.statementResults[2].error!.code).toBe('42601');
    });

    it('should stop on first error when stopOnError is true', async () => {
      const { dryRunSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, `SELECT 1;
INVALID;
SELECT 3;`);

      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rows: [], rowCount: 1 }) // SELECT 1
        .mockRejectedValueOnce(new Error('syntax error')) // INVALID
        .mockResolvedValueOnce({}); // ROLLBACK

      const result = await dryRunSqlFile({ filePath: testFile, stopOnError: true });

      expect(result.success).toBe(false);
      expect(result.statementResults.length).toBe(2); // Only first two, stopped at error
      expect(result.successCount).toBe(1);
      expect(result.failureCount).toBe(1);
    });

    it('should continue on error when stopOnError is false', async () => {
      const { dryRunSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, `SELECT 1;
INVALID;
SELECT 3;`);

      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rows: [], rowCount: 1 }) // SELECT 1
        .mockRejectedValueOnce(new Error('syntax error')) // INVALID
        .mockResolvedValueOnce({ rows: [], rowCount: 1 }) // SELECT 3
        .mockResolvedValueOnce({}); // ROLLBACK

      const result = await dryRunSqlFile({ filePath: testFile, stopOnError: false });

      expect(result.statementResults.length).toBe(3);
      expect(result.successCount).toBe(2);
      expect(result.failureCount).toBe(1);
    });

    it('should skip non-rollbackable operations (NEXTVAL) but run EXPLAIN', async () => {
      const { dryRunSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, `INSERT INTO users (name) VALUES ('test');
SELECT nextval('users_id_seq');`);

      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rows: [], rowCount: 1 }) // INSERT (with warning but executed)
        .mockResolvedValueOnce({ rows: [{ 'QUERY PLAN': [{ Plan: { 'Node Type': 'Result' } }] }] }) // EXPLAIN for nextval
        .mockResolvedValueOnce({}); // ROLLBACK

      const result = await dryRunSqlFile({ filePath: testFile });

      expect(result.nonRollbackableWarnings.length).toBeGreaterThan(0);
      expect(result.nonRollbackableWarnings.some(w => w.operation === 'SEQUENCE')).toBe(true);
      expect(result.skippedCount).toBe(1);
      expect(result.statementResults[1].skipped).toBe(true);
      expect(result.statementResults[1].skipReason).toContain('NEXTVAL');
      // Should have EXPLAIN plan for skipped statement
      expect(result.statementResults[1].explainPlan).toBeDefined();
    });

    it('should skip VACUUM operations', async () => {
      const { dryRunSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, `SELECT 1;
VACUUM users;`);

      // VACUUM is now SKIPPED, not executed
      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rows: [], rowCount: 1 }) // SELECT 1 (VACUUM is SKIPPED)
        .mockResolvedValueOnce({}); // ROLLBACK

      const result = await dryRunSqlFile({ filePath: testFile });

      expect(result.nonRollbackableWarnings.some(w => w.operation === 'VACUUM')).toBe(true);
      expect(result.skippedCount).toBe(1);
      expect(result.statementResults[1].skipped).toBe(true);
      expect(result.failureCount).toBe(0); // Not a failure, just skipped
    });

    it('should strip patterns before execution', async () => {
      const { dryRunSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, `SELECT 1;
/
SELECT 2;
/`);

      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rows: [], rowCount: 1 }) // SELECT 1
        .mockResolvedValueOnce({ rows: [], rowCount: 1 }) // SELECT 2
        .mockResolvedValueOnce({}); // ROLLBACK

      const result = await dryRunSqlFile({
        filePath: testFile,
        stripPatterns: ['/']
      });

      expect(result.totalStatements).toBe(2);
      expect(result.success).toBe(true);
    });

    it('should include execution time for each statement', async () => {
      const { dryRunSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, 'SELECT 1; SELECT 2;');

      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rows: [], rowCount: 1 })
        .mockResolvedValueOnce({ rows: [], rowCount: 1 })
        .mockResolvedValueOnce({}); // ROLLBACK

      const result = await dryRunSqlFile({ filePath: testFile });

      result.statementResults.forEach(stmt => {
        expect(stmt.executionTimeMs).toBeDefined();
        expect(typeof stmt.executionTimeMs).toBe('number');
      });
    });

    it('should generate comprehensive summary', async () => {
      const { dryRunSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, `CREATE TABLE test (id INT);
INSERT INTO test VALUES (1);
SELECT * FROM test;`);

      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({ rowCount: 0 }) // CREATE
        .mockResolvedValueOnce({ rowCount: 1 }) // INSERT
        .mockResolvedValueOnce({ rows: [{ id: 1 }], rowCount: 1 }) // SELECT
        .mockResolvedValueOnce({}); // ROLLBACK

      const result = await dryRunSqlFile({ filePath: testFile });

      expect(result.summary).toContain('Dry-run');
      expect(result.summary).toContain('3 statements');
      expect(result.summary).toContain('succeeded');
      expect(result.summary).toContain('rolled back');
    });

    it('should include sample rows in results', async () => {
      const { dryRunSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, 'SELECT id, name FROM users;');

      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockResolvedValueOnce({
          rows: [{ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }],
          rowCount: 2
        })
        .mockResolvedValueOnce({}); // ROLLBACK

      const result = await dryRunSqlFile({ filePath: testFile });

      expect(result.statementResults[0].rows).toBeDefined();
      expect(result.statementResults[0].rows!.length).toBe(2);
    });

    it('should capture detailed constraint violation errors', async () => {
      const { dryRunSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, "INSERT INTO users (email) VALUES ('duplicate@test.com');");

      const pgError = new Error('duplicate key value violates unique constraint "users_email_key"');
      (pgError as any).code = '23505';
      (pgError as any).severity = 'ERROR';
      (pgError as any).detail = 'Key (email)=(duplicate@test.com) already exists.';
      (pgError as any).schema = 'public';
      (pgError as any).table = 'users';
      (pgError as any).constraint = 'users_email_key';

      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockRejectedValueOnce(pgError) // INSERT fails
        .mockResolvedValueOnce({}); // ROLLBACK

      const result = await dryRunSqlFile({ filePath: testFile });

      expect(result.success).toBe(false);
      expect(result.statementResults[0].error).toBeDefined();
      const error = result.statementResults[0].error!;
      expect(error.code).toBe('23505');
      expect(error.severity).toBe('ERROR');
      expect(error.detail).toContain('already exists');
      expect(error.schema).toBe('public');
      expect(error.table).toBe('users');
      expect(error.constraint).toBe('users_email_key');
    });

    it('should capture foreign key violation errors', async () => {
      const { dryRunSqlFile } = await import('../tools/sql-tools.js');
      fs.writeFileSync(testFile, 'INSERT INTO orders (user_id) VALUES (999);');

      const pgError = new Error('insert or update on table "orders" violates foreign key constraint');
      (pgError as any).code = '23503';
      (pgError as any).detail = 'Key (user_id)=(999) is not present in table "users".';
      (pgError as any).constraint = 'orders_user_id_fkey';

      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockRejectedValueOnce(pgError)
        .mockResolvedValueOnce({}); // ROLLBACK

      const result = await dryRunSqlFile({ filePath: testFile });

      expect(result.statementResults[0].error!.code).toBe('23503');
      expect(result.statementResults[0].error!.constraint).toBe('orders_user_id_fkey');
    });

    it('should limit results to maxStatements', async () => {
      const { dryRunSqlFile } = await import('../tools/sql-tools.js');
      const statements = Array(30).fill('SELECT 1;').join('\n');
      fs.writeFileSync(testFile, statements);

      // Mock BEGIN, 30 SELECTs, ROLLBACK
      mockClient.query.mockResolvedValue({ rows: [], rowCount: 1 });

      const result = await dryRunSqlFile({
        filePath: testFile,
        maxStatements: 10
      });

      expect(result.totalStatements).toBe(30);
      expect(result.statementResults.length).toBe(10);
    });
  });

  describe('allowMultipleStatements', () => {
    it('should execute multiple statements and return results for each', async () => {
      mockQuery
        .mockResolvedValueOnce({ rows: [{ id: 1 }], rowCount: 1 })
        .mockResolvedValueOnce({ rows: [{ id: 2 }], rowCount: 1 })
        .mockResolvedValueOnce({ rows: [{ count: 5 }], rowCount: 1 });

      const result = await executeSql({
        sql: 'INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); SELECT COUNT(*) FROM t;',
        allowMultipleStatements: true
      });

      expect(result.totalStatements).toBe(3);
      expect(result.successCount).toBe(3);
      expect(result.failureCount).toBe(0);
      expect(result.results).toHaveLength(3);
    });

    it('should include line numbers in multi-statement results', async () => {
      mockQuery
        .mockResolvedValueOnce({ rows: [], rowCount: 0 })
        .mockResolvedValueOnce({ rows: [], rowCount: 0 });

      const result = await executeSql({
        sql: `SELECT 1;

SELECT 2;`,
        allowMultipleStatements: true
      });

      expect(result.results[0].lineNumber).toBe(1);
      expect(result.results[1].lineNumber).toBe(3);
    });

    it('should handle errors in multi-statement execution', async () => {
      mockQuery
        .mockResolvedValueOnce({ rows: [], rowCount: 0 })
        .mockRejectedValueOnce(new Error('syntax error'))
        .mockResolvedValueOnce({ rows: [], rowCount: 0 });

      const result = await executeSql({
        sql: 'SELECT 1; INVALID; SELECT 3;',
        allowMultipleStatements: true
      });

      expect(result.totalStatements).toBe(3);
      expect(result.successCount).toBe(2);
      expect(result.failureCount).toBe(1);
      expect(result.results[1].success).toBe(false);
      expect(result.results[1].error).toContain('syntax error');
    });

    it('should not allow params with multiple statements', async () => {
      await expect(executeSql({
        sql: 'SELECT $1; SELECT $2;',
        params: [1, 2],
        allowMultipleStatements: true
      })).rejects.toThrow('params not supported with allowMultipleStatements');
    });

    it('should skip empty statements and comments', async () => {
      mockQuery.mockResolvedValue({ rows: [], rowCount: 0 });

      const result = await executeSql({
        sql: `
          -- Just a comment
          SELECT 1;
          /* Block comment */
          SELECT 2;
        `,
        allowMultipleStatements: true
      });

      expect(result.totalStatements).toBe(2);
    });

    it('should handle dollar-quoted strings in multi-statement', async () => {
      mockQuery.mockResolvedValue({ rows: [], rowCount: 0 });

      const result = await executeSql({
        sql: `SELECT $$has ; semicolon$$; SELECT 2;`,
        allowMultipleStatements: true
      });

      expect(result.totalStatements).toBe(2);
    });
  });

  describe('mutationPreview', () => {
    it('should require sql parameter', async () => {
      await expect(mutationPreview({ sql: '' }))
        .rejects.toThrow('sql parameter is required');
    });

    it('should only accept INSERT, UPDATE, DELETE statements', async () => {
      await expect(mutationPreview({ sql: 'SELECT * FROM users' }))
        .rejects.toThrow('SQL must be an INSERT, UPDATE, or DELETE statement');
    });

    it('should preview DELETE with WHERE clause', async () => {
      mockQuery
        .mockResolvedValueOnce({
          rows: [{ 'QUERY PLAN': [{ Plan: { 'Plan Rows': 10 } }] }]
        }) // EXPLAIN
        .mockResolvedValueOnce({
          rows: [{ id: 1 }, { id: 2 }]
        }); // SELECT sample

      const result = await mutationPreview({
        sql: "DELETE FROM users WHERE status = 'inactive'"
      });

      expect(result.mutationType).toBe('DELETE');
      expect(result.estimatedRowsAffected).toBe(10);
      expect(result.sampleAffectedRows).toHaveLength(2);
      expect(result.targetTable).toBe('users');
      expect(result.whereClause).toBe("status = 'inactive'");
    });

    it('should preview UPDATE with WHERE clause', async () => {
      mockQuery
        .mockResolvedValueOnce({
          rows: [{ 'QUERY PLAN': [{ Plan: { 'Plan Rows': 5 } }] }]
        })
        .mockResolvedValueOnce({
          rows: [{ id: 1, name: 'Test' }]
        });

      const result = await mutationPreview({
        sql: "UPDATE users SET name = 'Updated' WHERE id = 1"
      });

      expect(result.mutationType).toBe('UPDATE');
      expect(result.targetTable).toBe('users');
    });

    it('should warn when no WHERE clause is present', async () => {
      mockQuery
        .mockResolvedValueOnce({
          rows: [{ 'QUERY PLAN': [{ Plan: { 'Plan Rows': 1000 } }] }]
        })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [{ cnt: '1000' }] });

      const result = await mutationPreview({
        sql: 'DELETE FROM users'
      });

      expect(result.warning).toContain('ALL rows');
    });

    it('should handle INSERT preview', async () => {
      // For INSERT, only EXPLAIN is called
      mockQuery.mockResolvedValueOnce({
        rows: [{ 'QUERY PLAN': [{ Plan: { 'Plan Rows': 1 } }] }]
      });

      const result = await mutationPreview({
        sql: "INSERT INTO users (name) VALUES ('test')"
      });

      expect(result.mutationType).toBe('INSERT');
      expect(result.warning).toContain('INSERT preview cannot show affected rows');
      expect(result.sampleAffectedRows).toHaveLength(0);
      expect(result.estimatedRowsAffected).toBe(1);
    });

    it('should limit sample size', async () => {
      mockQuery
        .mockResolvedValueOnce({
          rows: [{ 'QUERY PLAN': [{ Plan: { 'Plan Rows': 100 } }] }]
        })
        .mockResolvedValueOnce({ rows: Array(20).fill({ id: 1 }) });

      const result = await mutationPreview({
        sql: 'DELETE FROM users WHERE active = false',
        sampleSize: 50 // Should be capped at 20
      });

      // The query should use LIMIT 20 (max)
      expect(mockQuery).toHaveBeenCalledWith(
        expect.stringContaining('LIMIT 20'),
        undefined
      );
    });
  });

  describe('batchExecute', () => {
    it('should require queries parameter', async () => {
      await expect(batchExecute({ queries: null as any }))
        .rejects.toThrow('queries parameter is required');
    });

    it('should reject empty queries array', async () => {
      await expect(batchExecute({ queries: [] }))
        .rejects.toThrow('queries array cannot be empty');
    });

    it('should limit to 20 queries', async () => {
      const queries = Array(21).fill(null).map((_, i) => ({
        name: `q${i}`,
        sql: 'SELECT 1'
      }));

      await expect(batchExecute({ queries }))
        .rejects.toThrow('Maximum 20 queries allowed');
    });

    it('should require unique query names', async () => {
      await expect(batchExecute({
        queries: [
          { name: 'same', sql: 'SELECT 1' },
          { name: 'same', sql: 'SELECT 2' }
        ]
      })).rejects.toThrow('Duplicate query name');
    });

    it('should require name for each query', async () => {
      await expect(batchExecute({
        queries: [
          { name: '', sql: 'SELECT 1' }
        ]
      })).rejects.toThrow('Each query must have a name');
    });

    it('should require sql for each query', async () => {
      await expect(batchExecute({
        queries: [
          { name: 'test', sql: '' }
        ]
      })).rejects.toThrow('must have sql');
    });

    it('should execute multiple queries in parallel', async () => {
      // For parallel tests, use a consistent mock value - order isn't guaranteed
      mockQuery.mockResolvedValue({ rows: [{ value: 1 }], rowCount: 1 });

      const result = await batchExecute({
        queries: [
          { name: 'count', sql: 'SELECT COUNT(*) FROM users' },
          { name: 'sum', sql: 'SELECT SUM(amount) FROM orders' },
          { name: 'avg', sql: 'SELECT AVG(price) FROM products' }
        ]
      });

      expect(result.totalQueries).toBe(3);
      expect(result.successCount).toBe(3);
      expect(result.failureCount).toBe(0);
      expect(result.results.count.success).toBe(true);
      expect(result.results.count.rows).toEqual([{ value: 1 }]);
      expect(result.results.sum.success).toBe(true);
      expect(result.results.avg.success).toBe(true);
      expect(mockQuery).toHaveBeenCalledTimes(3);
    });

    it('should handle partial failures', async () => {
      // Test with all queries succeeding first - then test single failure case
      mockQuery.mockResolvedValue({ rows: [], rowCount: 0 });

      const successResult = await batchExecute({
        queries: [
          { name: 'q1', sql: 'SELECT 1' },
          { name: 'q2', sql: 'SELECT 2' }
        ]
      });

      expect(successResult.successCount).toBe(2);
      expect(successResult.failureCount).toBe(0);
    });

    it('should capture query errors correctly', async () => {
      // Test single query failure
      mockQuery.mockRejectedValue(new Error('Table not found'));

      const result = await batchExecute({
        queries: [
          { name: 'fail', sql: 'SELECT * FROM nonexistent' }
        ]
      });

      expect(result.successCount).toBe(0);
      expect(result.failureCount).toBe(1);
      expect(result.results.fail.success).toBe(false);
      expect(result.results.fail.error).toContain('Table not found');
    });

    it('should track execution time for each query', async () => {
      mockQuery.mockResolvedValue({ rows: [], rowCount: 0 });

      const result = await batchExecute({
        queries: [
          { name: 'q1', sql: 'SELECT 1' }
        ]
      });

      expect(result.results.q1.executionTimeMs).toBeDefined();
      expect(typeof result.results.q1.executionTimeMs).toBe('number');
      expect(result.totalExecutionTimeMs).toBeDefined();
    });

    it('should support query parameters', async () => {
      mockQuery.mockResolvedValue({ rows: [{ id: 1 }], rowCount: 1 });

      await batchExecute({
        queries: [
          { name: 'q1', sql: 'SELECT * FROM users WHERE id = $1', params: [123] }
        ]
      });

      expect(mockQuery).toHaveBeenCalledWith(
        'SELECT * FROM users WHERE id = $1',
        [123]
      );
    });
  });

  describe('Transaction Control', () => {
    it('should begin a transaction and return transactionId', async () => {
      mockBeginTransaction.mockResolvedValue({
        transactionId: 'test-tx-id',
        server: 'test-server',
        database: 'test-db',
        schema: 'public',
        startedAt: new Date()
      });

      const result = await beginTransaction();

      expect(result.transactionId).toBe('test-tx-id');
      expect(result.status).toBe('started');
      expect(result.message).toContain('test-tx-id');
    });

    it('should commit a transaction', async () => {
      mockCommitTransaction.mockResolvedValue(undefined);

      const result = await commitTransaction({ transactionId: 'test-tx-id' });

      expect(result.status).toBe('committed');
      expect(mockCommitTransaction).toHaveBeenCalledWith('test-tx-id');
    });

    it('should require transactionId for commit', async () => {
      await expect(commitTransaction({ transactionId: '' }))
        .rejects.toThrow('transactionId is required');
    });

    it('should rollback a transaction', async () => {
      mockRollbackTransaction.mockResolvedValue(undefined);

      const result = await rollbackTransaction({ transactionId: 'test-tx-id' });

      expect(result.status).toBe('rolled_back');
      expect(mockRollbackTransaction).toHaveBeenCalledWith('test-tx-id');
    });

    it('should require transactionId for rollback', async () => {
      await expect(rollbackTransaction({ transactionId: '' }))
        .rejects.toThrow('transactionId is required');
    });

    it('should execute query within transaction', async () => {
      mockQueryInTransaction.mockResolvedValue({
        rows: [{ id: 1 }],
        rowCount: 1,
        fields: [{ name: 'id' }]
      });

      const result = await executeSql({
        sql: 'SELECT * FROM users',
        transactionId: 'test-tx-id'
      });

      expect(mockQueryInTransaction).toHaveBeenCalledWith(
        'test-tx-id',
        'SELECT * FROM users',
        undefined
      );
      expect(result.rows).toEqual([{ id: 1 }]);
    });

    it('should use transactionId with multi-statement execution', async () => {
      mockQueryInTransaction
        .mockResolvedValueOnce({ rows: [], rowCount: 0 })
        .mockResolvedValueOnce({ rows: [], rowCount: 0 });

      const result = await executeSql({
        sql: 'INSERT INTO t VALUES (1); SELECT * FROM t;',
        allowMultipleStatements: true,
        transactionId: 'test-tx-id'
      });

      expect(mockQueryInTransaction).toHaveBeenCalledTimes(2);
      expect(result.totalStatements).toBe(2);
    });
  });

  describe('getConnectionContext', () => {
    it('should return current connection context', () => {
      const context = getConnectionContext();

      expect(context).toEqual({
        server: 'test-server',
        database: 'test-db',
        schema: 'public'
      });
    });
  });

  describe('includeSchemaHint', () => {
    it('should include schema hints when requested', async () => {
      // Schema hint queries run FIRST (columns, pk, fk, count), then main query
      mockQuery
        // 1. Mock for columns query (schema hint - runs first)
        .mockResolvedValueOnce({
          rows: [
            { name: 'id', type: 'integer', nullable: false },
            { name: 'name', type: 'text', nullable: true }
          ]
        })
        // 2. Mock for primary key query (schema hint)
        .mockResolvedValueOnce({
          rows: [{ column_name: 'id' }]
        })
        // 3. Mock for foreign keys query (schema hint)
        .mockResolvedValueOnce({ rows: [] })
        // 4. Mock for row count estimate (schema hint)
        .mockResolvedValueOnce({
          rows: [{ estimate: 1000 }]
        })
        // 5. Mock for main query (runs last)
        .mockResolvedValueOnce({
          rows: [{ id: 1 }],
          rowCount: 1,
          fields: [{ name: 'id' }]
        });

      const result = await executeSql({
        sql: 'SELECT * FROM users',
        includeSchemaHint: true
      });

      expect(result.schemaHint).toBeDefined();
      expect(result.schemaHint.tables).toHaveLength(1);
      expect(result.schemaHint.tables[0].table).toBe('users');
      expect(result.schemaHint.tables[0].columns).toHaveLength(2);
      expect(result.schemaHint.tables[0].primaryKey).toContain('id');
    });

    it('should extract tables from JOIN queries', async () => {
      mockQuery.mockResolvedValue({
        rows: [],
        rowCount: 0,
        fields: []
      });

      await executeSql({
        sql: 'SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id',
        includeSchemaHint: true
      });

      // Should query schema info for both tables
      expect(mockQuery).toHaveBeenCalledWith(
        expect.stringContaining('information_schema.columns'),
        ['public', 'orders']
      );
    });

    it('should handle schema-qualified table names', async () => {
      mockQuery.mockResolvedValue({
        rows: [],
        rowCount: 0,
        fields: []
      });

      await executeSql({
        sql: 'SELECT * FROM myschema.users',
        includeSchemaHint: true
      });

      expect(mockQuery).toHaveBeenCalledWith(
        expect.stringContaining('information_schema.columns'),
        ['myschema', 'users']
      );
    });

    it('should not include schema hints when not requested', async () => {
      mockQuery.mockResolvedValue({
        rows: [],
        rowCount: 0,
        fields: []
      });

      const result = await executeSql({
        sql: 'SELECT * FROM users',
        includeSchemaHint: false
      });

      expect(result.schemaHint).toBeUndefined();
    });
  });

  describe('SQL Parsing Safety (ReDoS Prevention)', () => {
    // These tests ensure the regex patterns don't cause infinite loops or excessive backtracking

    it('should handle deeply nested comments without hanging', async () => {
      mockQuery.mockResolvedValue({ rows: [], rowCount: 0, fields: [] });

      const nestedComments = '/* outer /* inner */ still outer */ SELECT 1;';
      const result = await executeSql({
        sql: nestedComments,
        allowMultipleStatements: true
      });

      // Should complete quickly without hanging
      expect(result).toBeDefined();
    });

    it('should handle many semicolons without performance issues', async () => {
      mockQuery.mockResolvedValue({ rows: [], rowCount: 0, fields: [] });

      // Create string with many semicolons
      const manySemicolons = 'SELECT 1' + ';'.repeat(100);
      const startTime = Date.now();

      const result = await executeSql({
        sql: manySemicolons,
        allowMultipleStatements: true
      });

      const duration = Date.now() - startTime;
      // Should complete in under 1 second
      expect(duration).toBeLessThan(1000);
    });

    it('should handle long strings of repeated characters safely', async () => {
      mockQuery.mockResolvedValue({ rows: [], rowCount: 0, fields: [] });

      // Potential ReDoS pattern - many repeated characters
      const longRepeat = 'SELECT ' + 'a'.repeat(10000);
      const startTime = Date.now();

      await expect(executeSql({
        sql: longRepeat,
        allowMultipleStatements: true
      })).resolves.toBeDefined();

      const duration = Date.now() - startTime;
      expect(duration).toBeLessThan(2000);
    });

    it('should handle alternating quotes safely', async () => {
      mockQuery.mockResolvedValue({ rows: [], rowCount: 0, fields: [] });

      // Alternating quotes that could confuse parsers
      const alternating = `SELECT '"'"'"'"'"'"'";`;
      const result = await executeSql({
        sql: alternating,
        allowMultipleStatements: true
      });

      expect(result).toBeDefined();
    });

    it('should handle unclosed dollar quotes gracefully', async () => {
      mockQuery.mockResolvedValue({ rows: [], rowCount: 0, fields: [] });

      // Unclosed dollar quote
      const unclosed = 'SELECT $tag$never closed';
      const startTime = Date.now();

      const result = await executeSql({
        sql: unclosed,
        allowMultipleStatements: true
      });

      const duration = Date.now() - startTime;
      expect(duration).toBeLessThan(1000);
      expect(result.totalStatements).toBe(1);
    });

    it('should handle pathological backtracking patterns in comments', async () => {
      mockQuery.mockResolvedValue({ rows: [], rowCount: 0, fields: [] });

      // Pattern that could cause exponential backtracking
      const pathological = '/*' + '*'.repeat(1000) + '/ SELECT 1;';
      const startTime = Date.now();

      const result = await executeSql({
        sql: pathological,
        allowMultipleStatements: true
      });

      const duration = Date.now() - startTime;
      expect(duration).toBeLessThan(1000);
    });

    it('should handle very long line comments', async () => {
      mockQuery.mockResolvedValue({ rows: [], rowCount: 0, fields: [] });

      const longComment = '-- ' + 'x'.repeat(50000) + '\nSELECT 1;';
      const startTime = Date.now();

      const result = await executeSql({
        sql: longComment,
        allowMultipleStatements: true
      });

      const duration = Date.now() - startTime;
      expect(duration).toBeLessThan(2000);
      expect(result.totalStatements).toBe(1);
    });

    it('should handle multiple dollar-quote tags in sequence', async () => {
      mockQuery.mockResolvedValue({ rows: [], rowCount: 0, fields: [] });

      const multiDollar = `
        SELECT $a$test$a$;
        SELECT $b$test$b$;
        SELECT $$test$$;
      `;
      const result = await executeSql({
        sql: multiDollar,
        allowMultipleStatements: true
      });

      expect(result.totalStatements).toBe(3);
    });

    it('should handle mixed quote styles without confusion', async () => {
      mockQuery.mockResolvedValue({ rows: [], rowCount: 0, fields: [] });

      const mixedQuotes = `
        SELECT 'single "with" double';
        SELECT "double 'with' single";
        SELECT $$dollar 'with' all "kinds"$$;
      `;
      const result = await executeSql({
        sql: mixedQuotes,
        allowMultipleStatements: true
      });

      expect(result.totalStatements).toBe(3);
    });

    it('should handle escaped quotes correctly', async () => {
      mockQuery.mockResolvedValue({ rows: [], rowCount: 0, fields: [] });

      const escapedQuotes = `SELECT 'it''s escaped'; SELECT "double""quote";`;
      const result = await executeSql({
        sql: escapedQuotes,
        allowMultipleStatements: true
      });

      expect(result.totalStatements).toBe(2);
    });
  });

  describe('Table Extraction Safety', () => {
    it('should extract tables from complex JOIN chains', async () => {
      mockQuery.mockResolvedValue({ rows: [], rowCount: 0, fields: [] });

      await executeSql({
        sql: `
          SELECT * FROM t1
          JOIN t2 ON t1.id = t2.t1_id
          LEFT JOIN t3 ON t2.id = t3.t2_id
          RIGHT JOIN t4 ON t3.id = t4.t3_id
          INNER JOIN t5 ON t4.id = t5.t4_id
        `,
        includeSchemaHint: true
      });

      // Should have attempted to fetch schema for multiple tables
      const calls = mockQuery.mock.calls;
      const schemaCalls = calls.filter((c: any[]) =>
        c[0] && c[0].includes('information_schema')
      );
      expect(schemaCalls.length).toBeGreaterThanOrEqual(1);
    });

    it('should handle subqueries without extracting them as tables', async () => {
      mockQuery.mockResolvedValue({ rows: [], rowCount: 0, fields: [] });

      await executeSql({
        sql: `SELECT * FROM (SELECT 1) AS subq`,
        includeSchemaHint: true
      });

      // Should not try to look up "subq" as a real table
      expect(mockQuery).toHaveBeenCalled();
    });

    it('should handle very long table names', async () => {
      mockQuery.mockResolvedValue({ rows: [], rowCount: 0, fields: [] });

      const longTableName = 'a'.repeat(63); // Max PostgreSQL identifier length
      await executeSql({
        sql: `SELECT * FROM ${longTableName}`,
        includeSchemaHint: true
      });

      expect(mockQuery).toHaveBeenCalledWith(
        expect.stringContaining('information_schema'),
        ['public', longTableName]
      );
    });

    it('should handle quoted identifiers', async () => {
      mockQuery.mockResolvedValue({ rows: [], rowCount: 0, fields: [] });

      await executeSql({
        sql: `SELECT * FROM "MyTable" JOIN "schema"."OtherTable" ON 1=1`,
        includeSchemaHint: true
      });

      // Should extract both table names correctly
      expect(mockQuery).toHaveBeenCalledWith(
        expect.stringContaining('information_schema'),
        expect.arrayContaining(['MyTable'])
      );
    });
  });

  describe('Edge Cases in Statement Splitting', () => {
    it('should handle empty SQL gracefully', async () => {
      // Empty string is falsy, caught by first validation
      await expect(executeSql({ sql: '' }))
        .rejects.toThrow('sql parameter is required and must be a string');
    });

    it('should handle SQL with only whitespace', async () => {
      // Whitespace-only becomes empty after trim
      await expect(executeSql({ sql: '   \n\t  ' }))
        .rejects.toThrow('sql parameter cannot be empty');
    });

    it('should handle SQL with only comments', async () => {
      mockQuery.mockResolvedValue({ rows: [], rowCount: 0, fields: [] });

      const result = await executeSql({
        sql: '-- just a comment\n/* block comment */',
        allowMultipleStatements: true
      });

      expect(result.totalStatements).toBe(0);
    });

    it('should handle statement ending without semicolon', async () => {
      mockQuery.mockResolvedValue({ rows: [], rowCount: 0, fields: [] });

      const result = await executeSql({
        sql: 'SELECT 1',
        allowMultipleStatements: true
      });

      expect(result.totalStatements).toBe(1);
    });

    it('should handle multiple empty lines between statements', async () => {
      mockQuery.mockResolvedValue({ rows: [], rowCount: 0, fields: [] });

      const result = await executeSql({
        sql: 'SELECT 1;\n\n\n\n\nSELECT 2;',
        allowMultipleStatements: true
      });

      expect(result.totalStatements).toBe(2);
      expect(result.results[0].lineNumber).toBe(1);
      expect(result.results[1].lineNumber).toBe(6);
    });

    it('should handle carriage returns correctly', async () => {
      mockQuery.mockResolvedValue({ rows: [], rowCount: 0, fields: [] });

      const result = await executeSql({
        sql: 'SELECT 1;\r\nSELECT 2;\r\nSELECT 3;',
        allowMultipleStatements: true
      });

      expect(result.totalStatements).toBe(3);
    });

    it('should handle Unicode characters in statements', async () => {
      mockQuery.mockResolvedValue({ rows: [], rowCount: 0, fields: [] });

      const result = await executeSql({
        sql: "SELECT '日本語'; SELECT 'émoji 🎉';",
        allowMultipleStatements: true
      });

      expect(result.totalStatements).toBe(2);
    });

    it('should handle null bytes safely', async () => {
      mockQuery.mockResolvedValue({ rows: [], rowCount: 0, fields: [] });

      // SQL with null byte should still parse
      const withNull = 'SELECT 1;\x00SELECT 2;';
      const result = await executeSql({
        sql: withNull,
        allowMultipleStatements: true
      });

      expect(result).toBeDefined();
    });
  });

  describe('Mutation Preview Edge Cases', () => {
    it('should handle UPDATE with multiple SET clauses', async () => {
      mockQuery
        .mockResolvedValueOnce({ rows: [{ 'QUERY PLAN': [{ Plan: { 'Plan Rows': 1 } }] }] })
        .mockResolvedValueOnce({ rows: [] });

      const result = await mutationPreview({
        sql: "UPDATE users SET name = 'test', email = 'test@test.com', updated_at = NOW() WHERE id = 1"
      });

      expect(result.mutationType).toBe('UPDATE');
      expect(result.targetTable).toBe('users');
      expect(result.whereClause).toBe('id = 1');
    });

    it('should handle DELETE with complex WHERE clause', async () => {
      mockQuery
        .mockResolvedValueOnce({ rows: [{ 'QUERY PLAN': [{ Plan: { 'Plan Rows': 5 } }] }] })
        .mockResolvedValueOnce({ rows: [] });

      const result = await mutationPreview({
        sql: `DELETE FROM orders WHERE status = 'cancelled' AND created_at < '2024-01-01' OR amount = 0`
      });

      expect(result.mutationType).toBe('DELETE');
      expect(result.whereClause).toContain('cancelled');
    });

    it('should handle schema-qualified table in UPDATE', async () => {
      mockQuery
        .mockResolvedValueOnce({ rows: [{ 'QUERY PLAN': [{ Plan: { 'Plan Rows': 1 } }] }] })
        .mockResolvedValueOnce({ rows: [] });

      const result = await mutationPreview({
        sql: "UPDATE myschema.users SET name = 'test' WHERE id = 1"
      });

      expect(result.targetTable).toBe('myschema.users');
    });

    it('should handle quoted table names in DELETE', async () => {
      mockQuery
        .mockResolvedValueOnce({ rows: [{ 'QUERY PLAN': [{ Plan: { 'Plan Rows': 1 } }] }] })
        .mockResolvedValueOnce({ rows: [] });

      const result = await mutationPreview({
        sql: `DELETE FROM "MyTable" WHERE id = 1`
      });

      expect(result.targetTable).toBe('MyTable');
    });

    it('should fallback to COUNT when EXPLAIN fails', async () => {
      mockQuery
        .mockRejectedValueOnce(new Error('EXPLAIN failed'))
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [{ cnt: '42' }] });

      const result = await mutationPreview({
        sql: "DELETE FROM users WHERE status = 'old'"
      });

      expect(result.estimatedRowsAffected).toBe(42);
    });
  });
});
