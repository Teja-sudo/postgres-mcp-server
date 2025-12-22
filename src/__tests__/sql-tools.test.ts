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

beforeAll(async () => {
  const module = await import('../tools/sql-tools.js');
  executeSql = module.executeSql;
  explainQuery = module.explainQuery;
});

describe('SQL Tools', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockIsConnected.mockReturnValue(true);
  });

  describe('executeSql', () => {
    it('should require sql parameter', async () => {
      await expect(executeSql({ sql: '' }))
        .rejects.toThrow('sql parameter is required');

      await expect(executeSql({ sql: null as any }))
        .rejects.toThrow('sql parameter is required');
    });

    it('should reject SQL that is too long', async () => {
      const longSql = 'SELECT ' + 'a'.repeat(100001);
      await expect(executeSql({ sql: longSql }))
        .rejects.toThrow('exceeds maximum length');
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
    });

    it('should write large results to file', async () => {
      const largeRows = Array.from({ length: 2000 }, (_, i) => ({ id: i, name: `User ${i}` }));
      mockQuery.mockResolvedValue({
        rows: largeRows,
        fields: [{ name: 'id' }, { name: 'name' }]
      });

      const result = await executeSql({ sql: 'SELECT * FROM users' });

      expect(result.rows).toEqual([]);
      expect(result.rowCount).toBe(2000);
      expect(result.outputFile).toBeDefined();
      expect(result.truncated).toBe(true);

      // Verify file was created with correct permissions
      if (result.outputFile) {
        expect(fs.existsSync(result.outputFile)).toBe(true);

        // Read and verify content
        const content = JSON.parse(fs.readFileSync(result.outputFile, 'utf-8'));
        expect(content.totalRows).toBe(2000);
        expect(content.rows).toHaveLength(2000);

        // Clean up
        fs.unlinkSync(result.outputFile);
      }
    });

    it('should respect maxRows parameter', async () => {
      const rows = Array.from({ length: 50 }, (_, i) => ({ id: i }));
      mockQuery.mockResolvedValue({
        rows,
        fields: [{ name: 'id' }]
      });

      const result = await executeSql({ sql: 'SELECT * FROM users', maxRows: 10 });

      // Should write to file because rows > maxRows
      expect(result.truncated).toBe(true);
      expect(result.outputFile).toBeDefined();

      // Clean up
      if (result.outputFile) {
        fs.unlinkSync(result.outputFile);
      }
    });

    it('should validate maxRows parameter', async () => {
      // Invalid maxRows should throw
      await expect(executeSql({ sql: 'SELECT 1', maxRows: -1 }))
        .rejects.toThrow('maxRows must be an integer between');
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
});
