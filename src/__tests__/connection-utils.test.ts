/**
 * Tests for connection-utils.ts - Pure function tests and async operations
 */

import { jest, describe, it, expect, beforeEach, beforeAll } from '@jest/globals';

type MockFn = jest.Mock<any>;

// Mock client
const mockClient = {
  query: jest.fn<MockFn>(),
  release: jest.fn<MockFn>(),
};

// Mock database manager
const mockGetClient = jest.fn<MockFn>();
const mockGetClientWithOverride = jest.fn<MockFn>();

jest.unstable_mockModule('../db-manager.js', () => ({
  getDbManager: jest.fn(() => ({
    getClient: mockGetClient,
    getClientWithOverride: mockGetClientWithOverride,
  })),
  resetDbManager: jest.fn(),
}));

// Dynamic import after mock
let buildConnectionOverride: any;
let validateOverrideWithTransaction: any;
let acquireClient: any;
let withClient: any;
let withTransaction: any;
let withDryRunTransaction: any;

beforeAll(async () => {
  const module = await import('../tools/sql/utils/connection-utils.js');
  buildConnectionOverride = module.buildConnectionOverride;
  validateOverrideWithTransaction = module.validateOverrideWithTransaction;
  acquireClient = module.acquireClient;
  withClient = module.withClient;
  withTransaction = module.withTransaction;
  withDryRunTransaction = module.withDryRunTransaction;
});

describe('Connection Utils', () => {
  describe('buildConnectionOverride', () => {
    it('should return undefined when no parameters provided', () => {
      const result = buildConnectionOverride({});
      expect(result).toBeUndefined();
    });

    it('should return override when server is provided', () => {
      const result = buildConnectionOverride({ server: 'my-server' });
      expect(result).toEqual({
        server: 'my-server',
        database: undefined,
        schema: undefined,
      });
    });

    it('should return override when database is provided', () => {
      const result = buildConnectionOverride({ database: 'my-db' });
      expect(result).toEqual({
        server: undefined,
        database: 'my-db',
        schema: undefined,
      });
    });

    it('should return override when schema is provided', () => {
      const result = buildConnectionOverride({ schema: 'my-schema' });
      expect(result).toEqual({
        server: undefined,
        database: undefined,
        schema: 'my-schema',
      });
    });

    it('should return override with all parameters', () => {
      const result = buildConnectionOverride({
        server: 'my-server',
        database: 'my-db',
        schema: 'my-schema',
      });
      expect(result).toEqual({
        server: 'my-server',
        database: 'my-db',
        schema: 'my-schema',
      });
    });

    it('should return override with server and database only', () => {
      const result = buildConnectionOverride({
        server: 'my-server',
        database: 'my-db',
      });
      expect(result).toEqual({
        server: 'my-server',
        database: 'my-db',
        schema: undefined,
      });
    });

    it('should return override with database and schema only', () => {
      const result = buildConnectionOverride({
        database: 'my-db',
        schema: 'my-schema',
      });
      expect(result).toEqual({
        server: undefined,
        database: 'my-db',
        schema: 'my-schema',
      });
    });
  });

  describe('validateOverrideWithTransaction', () => {
    it('should not throw when no override and no transaction', () => {
      expect(() => validateOverrideWithTransaction({})).not.toThrow();
    });

    it('should not throw when override but no transaction', () => {
      expect(() =>
        validateOverrideWithTransaction({ server: 'my-server' })
      ).not.toThrow();
    });

    it('should not throw when no override but has transaction', () => {
      expect(() =>
        validateOverrideWithTransaction({}, 'txn-123')
      ).not.toThrow();
    });

    it('should throw when server override is used with transaction', () => {
      expect(() =>
        validateOverrideWithTransaction({ server: 'my-server' }, 'txn-123')
      ).toThrow('Connection override (server/database/schema) cannot be used with transactions');
    });

    it('should throw when database override is used with transaction', () => {
      expect(() =>
        validateOverrideWithTransaction({ database: 'my-db' }, 'txn-123')
      ).toThrow('Connection override (server/database/schema) cannot be used with transactions');
    });

    it('should throw when schema override is used with transaction', () => {
      expect(() =>
        validateOverrideWithTransaction({ schema: 'my-schema' }, 'txn-123')
      ).toThrow('Connection override (server/database/schema) cannot be used with transactions');
    });

    it('should throw when all overrides are used with transaction', () => {
      expect(() =>
        validateOverrideWithTransaction(
          { server: 'srv', database: 'db', schema: 'schema' },
          'txn-456'
        )
      ).toThrow('Connection override (server/database/schema) cannot be used with transactions');
    });

    it('should not throw with empty transaction ID', () => {
      expect(() =>
        validateOverrideWithTransaction({ server: 'my-server' }, '')
      ).not.toThrow();
    });
  });

  describe('acquireClient', () => {
    beforeEach(() => {
      jest.clearAllMocks();
      mockClient.query.mockReset();
      mockClient.release.mockReset();
      mockGetClient.mockResolvedValue(mockClient);
      mockGetClientWithOverride.mockResolvedValue({
        client: mockClient,
        release: jest.fn(),
        server: 'test-server',
        database: 'test-db',
        schema: 'test-schema',
      });
    });

    it('should acquire regular client when no override', async () => {
      const result = await acquireClient();

      expect(result.isOverride).toBe(false);
      expect(result.client).toBe(mockClient);
      expect(mockGetClient).toHaveBeenCalled();
      expect(mockGetClientWithOverride).not.toHaveBeenCalled();
    });

    it('should acquire override client when override provided', async () => {
      const override = { server: 'my-server', database: 'my-db' };
      const result = await acquireClient(override);

      expect(result.isOverride).toBe(true);
      expect(result.connectionInfo).toBeDefined();
      expect(mockGetClientWithOverride).toHaveBeenCalledWith(override);
      expect(mockGetClient).not.toHaveBeenCalled();
    });

    it('should provide release function for regular client', async () => {
      const result = await acquireClient();

      result.release();
      expect(mockClient.release).toHaveBeenCalled();
    });
  });

  describe('withClient', () => {
    beforeEach(() => {
      jest.clearAllMocks();
      mockClient.query.mockReset();
      mockClient.release.mockReset();
      mockGetClient.mockResolvedValue(mockClient);
    });

    it('should execute function and release client on success', async () => {
      const fn = jest.fn<MockFn>().mockResolvedValue('result');

      const result = await withClient(undefined, fn);

      expect(result).toBe('result');
      expect(fn).toHaveBeenCalledWith(mockClient);
      expect(mockClient.release).toHaveBeenCalled();
    });

    it('should release client on error', async () => {
      const fn = jest.fn<MockFn>().mockRejectedValue(new Error('Test error'));

      await expect(withClient(undefined, fn)).rejects.toThrow('Test error');
      expect(mockClient.release).toHaveBeenCalled();
    });
  });

  describe('withTransaction', () => {
    beforeEach(() => {
      jest.clearAllMocks();
      mockClient.query.mockResolvedValue({});
      mockClient.release.mockReset();
      mockGetClient.mockResolvedValue(mockClient);
    });

    it('should execute function in transaction and commit on success', async () => {
      const fn = jest.fn<MockFn>().mockResolvedValue('result');

      const result = await withTransaction(undefined, fn);

      expect(result).toBe('result');
      expect(mockClient.query).toHaveBeenCalledWith('BEGIN');
      expect(mockClient.query).toHaveBeenCalledWith('COMMIT');
      expect(mockClient.release).toHaveBeenCalled();
    });

    it('should rollback on error', async () => {
      const fn = jest.fn<MockFn>().mockRejectedValue(new Error('Test error'));

      await expect(withTransaction(undefined, fn)).rejects.toThrow('Test error');
      expect(mockClient.query).toHaveBeenCalledWith('BEGIN');
      expect(mockClient.query).toHaveBeenCalledWith('ROLLBACK');
      expect(mockClient.release).toHaveBeenCalled();
    });

    it('should rollback on success when rollbackOnComplete is true', async () => {
      const fn = jest.fn<MockFn>().mockResolvedValue('result');

      const result = await withTransaction(undefined, fn, true);

      expect(result).toBe('result');
      expect(mockClient.query).toHaveBeenCalledWith('BEGIN');
      expect(mockClient.query).toHaveBeenCalledWith('ROLLBACK');
      expect(mockClient.query).not.toHaveBeenCalledWith('COMMIT');
    });

    it('should handle rollback error gracefully', async () => {
      const fn = jest.fn<MockFn>().mockRejectedValue(new Error('Test error'));
      mockClient.query
        .mockResolvedValueOnce({}) // BEGIN
        .mockRejectedValueOnce(new Error('Rollback failed')); // ROLLBACK fails

      await expect(withTransaction(undefined, fn)).rejects.toThrow('Test error');
      expect(mockClient.release).toHaveBeenCalled();
    });
  });

  describe('withDryRunTransaction', () => {
    beforeEach(() => {
      jest.clearAllMocks();
      mockClient.query.mockResolvedValue({});
      mockClient.release.mockReset();
      mockGetClient.mockResolvedValue(mockClient);
    });

    it('should always rollback (dry-run mode)', async () => {
      const fn = jest.fn<MockFn>().mockResolvedValue('result');

      const result = await withDryRunTransaction(undefined, fn);

      expect(result).toBe('result');
      expect(mockClient.query).toHaveBeenCalledWith('BEGIN');
      expect(mockClient.query).toHaveBeenCalledWith('ROLLBACK');
      expect(mockClient.query).not.toHaveBeenCalledWith('COMMIT');
    });
  });
});
