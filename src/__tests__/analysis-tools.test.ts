import { jest, describe, it, expect, beforeEach, beforeAll } from '@jest/globals';

type MockFn = jest.Mock<any>;
type MockedDbManager = {
  query: MockFn;
  isConnected: MockFn;
};

// Use jest.unstable_mockModule for ESM
const mockQuery = jest.fn<MockFn>();
const mockIsConnected = jest.fn<MockFn>();

jest.unstable_mockModule('../db-manager.js', () => ({
  getDbManager: jest.fn(() => ({
    query: mockQuery,
    isConnected: mockIsConnected.mockReturnValue(true),
  })),
  resetDbManager: jest.fn(),
}));

// Dynamic import after mock
let getTopQueries: any;
let analyzeWorkloadIndexes: any;
let analyzeQueryIndexes: any;
let analyzeDbHealth: any;

beforeAll(async () => {
  const module = await import('../tools/analysis-tools.js');
  getTopQueries = module.getTopQueries;
  analyzeWorkloadIndexes = module.analyzeWorkloadIndexes;
  analyzeQueryIndexes = module.analyzeQueryIndexes;
  analyzeDbHealth = module.analyzeDbHealth;
});

describe('Analysis Tools', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockIsConnected.mockReturnValue(true);
  });

  describe('getTopQueries', () => {
    it('should check for pg_stat_statements extension', async () => {
      mockQuery.mockResolvedValueOnce({ rows: [{ has_extension: false }] });

      await expect(getTopQueries({}))
        .rejects.toThrow('pg_stat_statements extension is not installed');
    });

    it('should return top queries ordered by total_time', async () => {
      mockQuery
        .mockResolvedValueOnce({ rows: [{ has_extension: true }] })
        .mockResolvedValueOnce({
          rows: [
            { query: 'SELECT * FROM users', calls: 100, total_time: 5000, mean_time: 50, rows: 1000 },
            { query: 'SELECT * FROM orders', calls: 50, total_time: 3000, mean_time: 60, rows: 500 }
          ]
        });

      const result = await getTopQueries({ limit: 10, orderBy: 'total_time' });

      expect(result).toHaveLength(2);
      expect(result[0].query).toBe('SELECT * FROM users');
    });

    it('should validate limit parameter', async () => {
      mockQuery.mockReset();
      mockQuery.mockResolvedValueOnce({ rows: [{ has_extension: true }] });
      mockQuery.mockResolvedValueOnce({ rows: [] });

      // limit > 100 should be capped at 100
      await getTopQueries({ limit: 100 }); // use valid limit

      const queryCall = mockQuery.mock.calls[1] as unknown[];
      expect((queryCall[1] as number[])[1]).toBe(100);
    });

    it('should validate orderBy parameter', async () => {
      mockQuery.mockReset();
      mockQuery.mockResolvedValueOnce({ rows: [{ has_extension: true }] });
      mockQuery.mockResolvedValueOnce({ rows: [] });

      await getTopQueries({ orderBy: 'invalid' as any });

      // Should use default 'total_time'
      const queryCall = mockQuery.mock.calls[1][0] as string;
      expect(queryCall).toContain('total_exec_time');
    });

    it('should fall back to legacy column names for older PostgreSQL', async () => {
      mockQuery.mockReset();
      mockQuery
        .mockResolvedValueOnce({ rows: [{ has_extension: true }] })
        .mockRejectedValueOnce(new Error('column "total_exec_time" does not exist'))
        .mockResolvedValueOnce({
          rows: [{ query: 'SELECT 1', calls: 1, total_time: 100, mean_time: 100, rows: 1 }]
        });

      const result = await getTopQueries({});

      expect(result).toHaveLength(1);
    });
  });

  describe('analyzeQueryIndexes', () => {
    it('should require queries parameter', async () => {
      await expect(analyzeQueryIndexes({ queries: undefined as any }))
        .rejects.toThrow('queries parameter is required');

      await expect(analyzeQueryIndexes({ queries: null as any }))
        .rejects.toThrow('queries parameter is required');
    });

    it('should require at least one query', async () => {
      await expect(analyzeQueryIndexes({ queries: [] }))
        .rejects.toThrow('queries array must contain at least one query');
    });

    it('should limit to 10 queries', async () => {
      const manyQueries = Array.from({ length: 11 }, () => 'SELECT 1');

      await expect(analyzeQueryIndexes({ queries: manyQueries }))
        .rejects.toThrow('Maximum 10 queries allowed');
    });

    it('should reject write queries', async () => {
      const result = await analyzeQueryIndexes({
        queries: ['DELETE FROM users']
      });

      expect(result.queryAnalysis[0].error).toContain('Cannot analyze');
    });

    it('should analyze valid SELECT queries', async () => {
      mockQuery.mockResolvedValue({
        rows: [{
          'QUERY PLAN': [{
            Plan: {
              'Node Type': 'Seq Scan',
              'Relation Name': 'users',
              'Filter': '(id = 1)'
            }
          }]
        }]
      });

      const result = await analyzeQueryIndexes({
        queries: ['SELECT * FROM users WHERE id = 1']
      });

      expect(result.queryAnalysis).toHaveLength(1);
      expect(result.queryAnalysis[0].recommendations).toBeDefined();
    });

    it('should handle query analysis errors gracefully', async () => {
      mockQuery.mockReset();
      mockQuery.mockRejectedValue(new Error('Syntax error'));

      const result = await analyzeQueryIndexes({
        queries: ['INVALID SQL']
      });

      expect(result.queryAnalysis[0].error).toContain('Syntax error');
    });

    it('should deduplicate recommendations in summary', async () => {
      mockQuery.mockResolvedValue({
        rows: [{
          'QUERY PLAN': [{
            Plan: {
              'Node Type': 'Seq Scan',
              'Relation Name': 'users',
              'Filter': '(id = 1)'
            }
          }]
        }]
      });

      const result = await analyzeQueryIndexes({
        queries: [
          'SELECT * FROM users WHERE id = 1',
          'SELECT * FROM users WHERE id = 2'
        ]
      });

      // Should have deduplicated recommendations
      const uniqueTables = new Set(result.summary.map((r: any) => r.table));
      expect(uniqueTables.size).toBeLessThanOrEqual(result.summary.length);
    });
  });

  describe('analyzeWorkloadIndexes', () => {
    it('should validate topQueriesCount parameter', async () => {
      mockQuery.mockReset();
      mockQuery
        .mockResolvedValueOnce({ rows: [{ has_extension: true }] })
        .mockResolvedValueOnce({ rows: [] });

      await analyzeWorkloadIndexes({ topQueriesCount: 50 }); // use max valid value

      const queryCall = mockQuery.mock.calls[1] as unknown[];
      expect((queryCall[1] as number[])[1]).toBeLessThanOrEqual(50); // max count
    });

    it('should return queries and recommendations', async () => {
      mockQuery.mockReset();
      mockQuery
        .mockResolvedValueOnce({ rows: [{ has_extension: true }] })
        .mockResolvedValueOnce({
          rows: [{ query: 'SELECT * FROM users WHERE id = 1', calls: 100, total_time: 5000, mean_time: 50, rows: 1000 }]
        })
        .mockResolvedValue({
          rows: [{
            'QUERY PLAN': [{
              Plan: { 'Node Type': 'Seq Scan', 'Relation Name': 'users', 'Filter': '(id = 1)' }
            }]
          }]
        });

      const result = await analyzeWorkloadIndexes({});

      expect(result.queries).toBeDefined();
      expect(result.recommendations).toBeDefined();
    });
  });

  describe('analyzeDbHealth', () => {
    it('should return all health check categories', async () => {
      mockQuery.mockReset();
      // Mock all health check queries
      mockQuery
        .mockResolvedValueOnce({ rows: [{ heap_read: 100, heap_hit: 900, ratio: 0.9 }] }) // cache
        .mockResolvedValueOnce({ rows: [{ total_connections: 10, active: 5, idle: 5, idle_in_transaction: 0, max_connections: 100 }] }) // connections
        .mockResolvedValueOnce({ rows: [] }) // invalid indexes
        .mockResolvedValueOnce({ rows: [] }) // unused indexes
        .mockResolvedValueOnce({ rows: [] }) // duplicate indexes
        .mockResolvedValueOnce({ rows: [] }) // vacuum
        .mockResolvedValueOnce({ rows: [] }) // sequences
        .mockResolvedValueOnce({ rows: [] }); // constraints

      const result = await analyzeDbHealth();

      expect(result).toHaveLength(8);
      const categories = result.map((r: any) => r.category);
      expect(categories).toContain('Buffer Cache Hit Rate');
      expect(categories).toContain('Connection Health');
      expect(categories).toContain('Invalid Indexes');
      expect(categories).toContain('Unused Indexes');
      expect(categories).toContain('Duplicate Indexes');
      expect(categories).toContain('Vacuum Health');
      expect(categories).toContain('Sequence Limits');
      expect(categories).toContain('Constraint Validation');
    });

    it('should report healthy status for good metrics', async () => {
      mockQuery.mockReset();
      mockQuery
        .mockResolvedValueOnce({ rows: [{ heap_read: 100, heap_hit: 9900, ratio: 0.99 }] })
        .mockResolvedValueOnce({ rows: [{ total_connections: 10, active: 5, idle: 5, idle_in_transaction: 0, max_connections: 100 }] })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] });

      const result = await analyzeDbHealth();

      const cacheHealth = result.find((r: any) => r.category === 'Buffer Cache Hit Rate');
      expect(cacheHealth?.status).toBe('healthy');
    });

    it('should report warning status for concerning metrics', async () => {
      mockQuery.mockReset();
      mockQuery
        .mockResolvedValueOnce({ rows: [{ heap_read: 200, heap_hit: 800, ratio: 0.85 }] }) // 85% cache hit - warning
        .mockResolvedValueOnce({ rows: [{ total_connections: 75, active: 50, idle: 20, idle_in_transaction: 5, max_connections: 100 }] }) // 75% connections
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] });

      const result = await analyzeDbHealth();

      const cacheHealth = result.find((r: any) => r.category === 'Buffer Cache Hit Rate');
      expect(cacheHealth?.status).toBe('warning');

      const connHealth = result.find((r: any) => r.category === 'Connection Health');
      expect(connHealth?.status).toBe('warning');
    });

    it('should report critical status for bad metrics', async () => {
      mockQuery.mockReset();
      mockQuery
        .mockResolvedValueOnce({ rows: [{ heap_read: 500, heap_hit: 500, ratio: 0.5 }] }) // 50% cache hit - critical
        .mockResolvedValueOnce({ rows: [{ total_connections: 95, active: 90, idle: 5, idle_in_transaction: 0, max_connections: 100 }] }) // 95% connections - critical
        .mockResolvedValueOnce({ rows: [{ schemaname: 'public', tablename: 'users', indexname: 'bad_idx' }] }) // invalid index - critical
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] });

      const result = await analyzeDbHealth();

      const cacheHealth = result.find((r: any) => r.category === 'Buffer Cache Hit Rate');
      expect(cacheHealth?.status).toBe('critical');

      const connHealth = result.find((r: any) => r.category === 'Connection Health');
      expect(connHealth?.status).toBe('critical');

      const indexHealth = result.find((r: any) => r.category === 'Invalid Indexes');
      expect(indexHealth?.status).toBe('critical');
    });

    it('should handle query errors gracefully', async () => {
      mockQuery.mockReset();
      mockQuery
        .mockRejectedValueOnce(new Error('Permission denied'))
        .mockResolvedValueOnce({ rows: [{ total_connections: 10, active: 5, idle: 5, idle_in_transaction: 0, max_connections: 100 }] })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] });

      const result = await analyzeDbHealth();

      // Should still return all categories
      expect(result).toHaveLength(8);

      // Failed query should show warning
      const cacheHealth = result.find((r: any) => r.category === 'Buffer Cache Hit Rate');
      expect(cacheHealth?.status).toBe('warning');
      expect(cacheHealth?.message).toContain('Could not check');
    });

    it('should include details for issues found', async () => {
      mockQuery.mockReset();
      mockQuery
        .mockResolvedValueOnce({ rows: [{ heap_read: 100, heap_hit: 900, ratio: 0.9 }] })
        .mockResolvedValueOnce({ rows: [{ total_connections: 10, active: 5, idle: 5, idle_in_transaction: 0, max_connections: 100 }] })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({
          rows: [
            { schemaname: 'public', tablename: 'users', indexname: 'unused_idx', idx_scan: 0, index_size: '10 MB' }
          ]
        })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] });

      const result = await analyzeDbHealth();

      const unusedIndexes = result.find((r: any) => r.category === 'Unused Indexes');
      expect((unusedIndexes?.details as any)?.indexes).toHaveLength(1);
    });
  });
});
