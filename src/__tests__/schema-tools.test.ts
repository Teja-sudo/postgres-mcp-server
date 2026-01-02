import { jest, describe, it, expect, beforeEach, beforeAll } from '@jest/globals';

type MockFn = jest.Mock<any>;

// Use jest.unstable_mockModule for ESM
const mockQuery = jest.fn<MockFn>();
const mockQueryWithOverride = jest.fn<MockFn>();
const mockIsConnected = jest.fn<MockFn>();

jest.unstable_mockModule('../db-manager.js', () => ({
  getDbManager: jest.fn(() => ({
    query: mockQuery,
    queryWithOverride: mockQueryWithOverride,
    isConnected: mockIsConnected.mockReturnValue(true),
  })),
  resetDbManager: jest.fn(),
}));

// Dynamic import after mock
let listSchemas: any;
let listObjects: any;
let getObjectDetails: any;

beforeAll(async () => {
  const module = await import('../tools/schema-tools.js');
  listSchemas = module.listSchemas;
  listObjects = module.listObjects;
  getObjectDetails = module.getObjectDetails;
});

describe('Schema Tools', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockIsConnected.mockReturnValue(true);
    // By default, queryWithOverride delegates to the same behavior as query
    mockQueryWithOverride.mockImplementation(((sql: string, params?: any[], override?: any) => {
      return mockQuery(sql, params);
    }) as any);
  });

  describe('listSchemas', () => {
    it('should list schemas without system schemas by default', async () => {
      mockQuery.mockResolvedValue({
        rows: [
          { schema_name: 'public', owner: 'postgres' },
          { schema_name: 'app', owner: 'app_user' }
        ]
      });

      const result = await listSchemas({});

      expect(result).toHaveLength(2);
      expect(result[0].schema_name).toBe('public');

      // Verify the query excludes system schemas
      const queryCall = mockQuery.mock.calls[0][0] as string;
      expect(queryCall).toContain('NOT IN');
      expect(queryCall).toContain('pg_catalog');
    });

    it('should include system schemas when requested', async () => {
      mockQuery.mockResolvedValue({
        rows: [
          { schema_name: 'public', owner: 'postgres' },
          { schema_name: 'pg_catalog', owner: 'postgres' },
          { schema_name: 'information_schema', owner: 'postgres' }
        ]
      });

      const result = await listSchemas({ includeSystemSchemas: true });

      expect(result).toHaveLength(3);

      // Verify the query doesn't have the exclusion
      const queryCall = mockQuery.mock.calls[0][0] as string;
      expect(queryCall).not.toContain('NOT IN');
    });
  });

  describe('listObjects', () => {
    it('should require schema parameter', async () => {
      await expect(listObjects({ schema: '' }))
        .rejects.toThrow('schema parameter is required');

      await expect(listObjects({ schema: undefined as any }))
        .rejects.toThrow('schema parameter is required');
    });

    it('should validate schema name', async () => {
      // Schema with SQL injection patterns throws specific error
      await expect(listObjects({ schema: 'public; DROP TABLE users;--' }))
        .rejects.toThrow('potentially dangerous SQL characters');

      // Schema with invalid characters throws pattern validation error
      await expect(listObjects({ schema: "schema'" }))
        .rejects.toThrow('invalid characters');
    });

    it('should list all object types by default', async () => {
      // New implementation uses UNION query with pagination
      mockQueryWithOverride
        .mockResolvedValueOnce({ rows: [{ total: '3' }] }) // count query
        .mockResolvedValueOnce({
          rows: [
            { name: 'users', type: 'table', owner: 'app', schema: 'public' },
            { name: 'active_users', type: 'view', owner: '', schema: 'public' },
            { name: 'users_id_seq', type: 'sequence', owner: '', schema: 'public' }
          ]
        });

      const result = await listObjects({ schema: 'public' });

      expect(result.items).toHaveLength(3);
      expect(result.totalCount).toBe(3);
      expect(mockQueryWithOverride).toHaveBeenCalledTimes(2); // count + paginated query
    });

    it('should filter by object type', async () => {
      mockQueryWithOverride
        .mockResolvedValueOnce({ rows: [{ total: '1' }] }) // count query
        .mockResolvedValueOnce({
          rows: [{ name: 'users', type: 'table', owner: 'app', schema: 'public' }]
        });

      const result = await listObjects({ schema: 'public', objectType: 'table' });

      expect(result.items).toHaveLength(1);
      expect(mockQueryWithOverride).toHaveBeenCalledTimes(2); // count + paginated query
    });

    it('should validate filter parameter', async () => {
      await expect(listObjects({ schema: 'public', filter: 'test; DROP TABLE' }))
        .rejects.toThrow('filter contains invalid characters');
    });

    it('should reject filter that is too long', async () => {
      const longFilter = 'a'.repeat(129);
      await expect(listObjects({ schema: 'public', filter: longFilter }))
        .rejects.toThrow('filter must be 128 characters or less');
    });

    it('should use parameterized queries for filter', async () => {
      mockQuery.mockResolvedValue({ rows: [] });

      await listObjects({ schema: 'public', objectType: 'table', filter: 'user' });

      expect(mockQuery).toHaveBeenCalledWith(
        expect.stringContaining('ILIKE'),
        ['public', 'user']
      );
    });

    describe('pagination', () => {
      it('should return paginated result with metadata', async () => {
        // First call: count query
        mockQueryWithOverride
          .mockResolvedValueOnce({ rows: [{ total: '25' }] })
          // Second call: paginated data
          .mockResolvedValueOnce({
            rows: [
              { name: 'users', type: 'table', owner: 'app', schema: 'public' },
              { name: 'orders', type: 'table', owner: 'app', schema: 'public' },
            ]
          });

        const result = await listObjects({ schema: 'public', objectType: 'table', limit: 2, offset: 0 });

        expect(result.items).toHaveLength(2);
        expect(result.totalCount).toBe(25);
        expect(result.limit).toBe(2);
        expect(result.offset).toBe(0);
        expect(result.hasMore).toBe(true);
      });

      it('should set hasMore to false when on last page', async () => {
        mockQueryWithOverride
          .mockResolvedValueOnce({ rows: [{ total: '5' }] })
          .mockResolvedValueOnce({
            rows: [
              { name: 'users', type: 'table', owner: 'app', schema: 'public' },
            ]
          });

        const result = await listObjects({ schema: 'public', objectType: 'table', limit: 10, offset: 4 });

        expect(result.hasMore).toBe(false);
      });

      it('should handle empty results', async () => {
        mockQueryWithOverride
          .mockResolvedValueOnce({ rows: [{ total: '0' }] })
          .mockResolvedValueOnce({ rows: [] });

        const result = await listObjects({ schema: 'empty_schema', objectType: 'table' });

        expect(result.items).toHaveLength(0);
        expect(result.totalCount).toBe(0);
        expect(result.hasMore).toBe(false);
      });

      it('should use default limit when not specified', async () => {
        mockQueryWithOverride
          .mockResolvedValueOnce({ rows: [{ total: '5' }] })
          .mockResolvedValueOnce({ rows: [] });

        const result = await listObjects({ schema: 'public' });

        expect(result.limit).toBe(100); // DEFAULT_LIST_LIMIT
      });

      it('should reject limit exceeding maximum', async () => {
        await expect(listObjects({ schema: 'public', limit: 1001 }))
          .rejects.toThrow('must be an integer between');
      });

      it('should reject negative offset', async () => {
        await expect(listObjects({ schema: 'public', offset: -1 }))
          .rejects.toThrow('must be an integer between');
      });
    });
  });

  describe('getObjectDetails', () => {
    it('should require schema parameter', async () => {
      await expect(getObjectDetails({ schema: '', objectName: 'users' }))
        .rejects.toThrow('schema parameter is required');
    });

    it('should require objectName parameter', async () => {
      await expect(getObjectDetails({ schema: 'public', objectName: '' }))
        .rejects.toThrow('objectName parameter is required');
    });

    it('should validate schema name', async () => {
      // Schema with SQL injection patterns throws specific error
      await expect(getObjectDetails({ schema: 'public; DROP', objectName: 'users' }))
        .rejects.toThrow('potentially dangerous SQL characters');
    });

    it('should validate object name', async () => {
      // Object name with SQL injection patterns throws specific error
      await expect(getObjectDetails({ schema: 'public', objectName: 'users; DROP' }))
        .rejects.toThrow('potentially dangerous SQL characters');
    });

    it('should return columns, constraints, and indexes', async () => {
      mockQuery
        .mockResolvedValueOnce({
          rows: [
            { column_name: 'id', data_type: 'integer', is_nullable: 'NO', column_default: null, character_maximum_length: null },
            { column_name: 'name', data_type: 'text', is_nullable: 'YES', column_default: null, character_maximum_length: null }
          ]
        })
        .mockResolvedValueOnce({
          rows: [
            { constraint_name: 'users_pkey', constraint_type: 'PRIMARY KEY', table_name: 'users', column_name: 'id', foreign_table_name: null, foreign_column_name: null }
          ]
        })
        .mockResolvedValueOnce({
          rows: [
            { index_name: 'users_pkey', index_definition: 'CREATE UNIQUE INDEX...', is_unique: true, is_primary: true }
          ]
        })
        .mockResolvedValueOnce({
          rows: [{ size: '16 kB', row_count: 100 }]
        });

      const result = await getObjectDetails({ schema: 'public', objectName: 'users' });

      expect(result.columns).toHaveLength(2);
      expect(result.constraints).toHaveLength(1);
      expect(result.indexes).toHaveLength(1);
      expect(result.size).toBe('16 kB');
      expect(result.rowCount).toBe(100);
    });

    it('should use parameterized queries', async () => {
      mockQuery.mockResolvedValue({ rows: [] });

      await getObjectDetails({ schema: 'public', objectName: 'users' });

      // All queries should use parameters
      mockQuery.mock.calls.forEach((call: unknown[]) => {
        if (call[1]) {
          expect(call[1]).toEqual(['public', 'users']);
        }
      });
    });

    it('should get view definition when objectType is view', async () => {
      mockQuery
        .mockResolvedValueOnce({ rows: [] }) // columns
        .mockResolvedValueOnce({ rows: [] }) // constraints
        .mockResolvedValueOnce({ rows: [] }) // indexes
        .mockResolvedValueOnce({ rows: [] }) // size
        .mockResolvedValueOnce({ rows: [{ definition: 'SELECT * FROM users' }] }); // view def

      const result = await getObjectDetails({ schema: 'public', objectName: 'active_users', objectType: 'view' });

      expect(result.definition).toBe('SELECT * FROM users');
    });

    it('should handle size query failure gracefully', async () => {
      mockQuery
        .mockResolvedValueOnce({ rows: [] }) // columns
        .mockResolvedValueOnce({ rows: [] }) // constraints
        .mockResolvedValueOnce({ rows: [] }) // indexes
        .mockRejectedValueOnce(new Error('Size query failed')); // size query fails

      const result = await getObjectDetails({ schema: 'public', objectName: 'special_view' });

      // Should not throw, just skip size info
      expect(result.size).toBeUndefined();
      expect(result.rowCount).toBeUndefined();
    });

    it('should handle view definition query failure gracefully', async () => {
      mockQuery
        .mockResolvedValueOnce({ rows: [] }) // columns
        .mockResolvedValueOnce({ rows: [] }) // constraints
        .mockResolvedValueOnce({ rows: [] }) // indexes
        .mockResolvedValueOnce({ rows: [] }) // size
        .mockRejectedValueOnce(new Error('View definition failed')); // view def query fails

      const result = await getObjectDetails({ schema: 'public', objectName: 'broken_view', objectType: 'view' });

      // Should not throw, just skip definition
      expect(result.definition).toBeUndefined();
    });
  });
});
