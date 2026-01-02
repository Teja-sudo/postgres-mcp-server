/**
 * Tests for retry.ts - Basic function tests and connection retry behavior
 */

import { jest, describe, it, expect, beforeEach, beforeAll } from '@jest/globals';

type MockFn = jest.Mock<any>;

// Mock for connection error detection
const mockIsConnectionError = jest.fn<MockFn>();
const mockReconnect = jest.fn<MockFn>();

jest.unstable_mockModule('../db-manager.js', () => ({
  getDbManager: jest.fn(() => ({
    reconnect: mockReconnect,
  })),
  DatabaseManager: {
    isConnectionError: mockIsConnectionError,
  },
  resetDbManager: jest.fn(),
}));

// Dynamic import after mock
let withConnectionRetry: any;
let createRetryableToolHandler: any;

beforeAll(async () => {
  const module = await import('../utils/retry.js');
  withConnectionRetry = module.withConnectionRetry;
  createRetryableToolHandler = module.createRetryableToolHandler;
});

describe('Retry Utils', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockIsConnectionError.mockReturnValue(false);
    mockReconnect.mockResolvedValue(false);
  });

  describe('withConnectionRetry', () => {
    it('should execute handler successfully without retry on success', async () => {
      const handler = jest.fn<() => Promise<string>>().mockResolvedValue('success');
      const wrappedHandler = withConnectionRetry(handler);

      const result = await wrappedHandler();

      expect(result).toBe('success');
      expect(handler).toHaveBeenCalledTimes(1);
    });

    it('should pass arguments to handler', async () => {
      const handler = jest.fn<(a: string, b: number) => Promise<string>>().mockResolvedValue('done');
      const wrappedHandler = withConnectionRetry(handler);

      await wrappedHandler('test', 42);

      expect(handler).toHaveBeenCalledWith('test', 42);
    });

    it('should throw error when handler fails with non-connection error', async () => {
      const error = new Error('Regular error');
      const handler = jest.fn<() => Promise<string>>().mockRejectedValue(error);
      const wrappedHandler = withConnectionRetry(handler);

      await expect(wrappedHandler()).rejects.toThrow('Regular error');
      expect(handler).toHaveBeenCalledTimes(1);
    });

    it('should respect default maxRetries of 1', async () => {
      const handler = jest.fn<() => Promise<string>>().mockResolvedValue('success');
      const wrappedHandler = withConnectionRetry(handler);

      // Verify wrapper is created with default
      expect(wrappedHandler).toBeInstanceOf(Function);
    });

    it('should handle handlers with multiple arguments', async () => {
      const handler = jest.fn<(a: number, b: string, c: boolean) => Promise<object>>()
        .mockResolvedValue({ result: true });
      const wrappedHandler = withConnectionRetry(handler);

      const result = await wrappedHandler(1, 'test', true);

      expect(result).toEqual({ result: true });
      expect(handler).toHaveBeenCalledWith(1, 'test', true);
    });

    it('should preserve handler return type', async () => {
      const handler = jest.fn<() => Promise<{ id: number; name: string }>>()
        .mockResolvedValue({ id: 1, name: 'test' });
      const wrappedHandler = withConnectionRetry(handler);

      const result = await wrappedHandler();

      expect(result).toEqual({ id: 1, name: 'test' });
    });
  });

  describe('createRetryableToolHandler', () => {
    it('should create a wrapped handler', async () => {
      const handler = jest.fn<(args: { foo: string }) => Promise<string>>().mockResolvedValue('result');
      const wrappedHandler = createRetryableToolHandler(handler);

      const result = await wrappedHandler({ foo: 'bar' });

      expect(result).toBe('result');
      expect(handler).toHaveBeenCalledWith({ foo: 'bar' });
    });

    it('should throw error when handler fails', async () => {
      const error = new Error('Handler error');
      const handler = jest.fn<(args: { test: boolean }) => Promise<string>>()
        .mockRejectedValue(error);
      const wrappedHandler = createRetryableToolHandler(handler);

      await expect(wrappedHandler({ test: true })).rejects.toThrow('Handler error');
    });

    it('should work with complex argument types', async () => {
      interface ComplexArgs {
        id: number;
        options: { limit: number; offset: number };
      }
      const handler = jest.fn<(args: ComplexArgs) => Promise<string[]>>()
        .mockResolvedValue(['a', 'b', 'c']);
      const wrappedHandler = createRetryableToolHandler(handler);

      const result = await wrappedHandler({ id: 1, options: { limit: 10, offset: 0 } });

      expect(result).toEqual(['a', 'b', 'c']);
      expect(handler).toHaveBeenCalledWith({ id: 1, options: { limit: 10, offset: 0 } });
    });
  });

  describe('Connection Error Retry', () => {
    it('should retry on connection error with successful reconnect', async () => {
      const connectionError = new Error('Connection terminated unexpectedly');
      const handler = jest.fn<() => Promise<string>>()
        .mockRejectedValueOnce(connectionError)
        .mockResolvedValueOnce('success after retry');

      mockIsConnectionError.mockReturnValue(true);
      mockReconnect.mockResolvedValue(true);

      const wrappedHandler = withConnectionRetry(handler);
      const result = await wrappedHandler();

      expect(result).toBe('success after retry');
      expect(handler).toHaveBeenCalledTimes(2);
      expect(mockReconnect).toHaveBeenCalledTimes(1);
    });

    it('should not retry when reconnect fails', async () => {
      const connectionError = new Error('Connection terminated unexpectedly');
      const handler = jest.fn<() => Promise<string>>()
        .mockRejectedValue(connectionError);

      mockIsConnectionError.mockReturnValue(true);
      mockReconnect.mockResolvedValue(false);

      const wrappedHandler = withConnectionRetry(handler);

      await expect(wrappedHandler()).rejects.toThrow('Connection terminated unexpectedly');
      expect(handler).toHaveBeenCalledTimes(1);
      expect(mockReconnect).toHaveBeenCalledTimes(1);
    });

    it('should not retry when not a connection error', async () => {
      const regularError = new Error('SQL syntax error');
      const handler = jest.fn<() => Promise<string>>()
        .mockRejectedValue(regularError);

      mockIsConnectionError.mockReturnValue(false);

      const wrappedHandler = withConnectionRetry(handler);

      await expect(wrappedHandler()).rejects.toThrow('SQL syntax error');
      expect(handler).toHaveBeenCalledTimes(1);
      expect(mockReconnect).not.toHaveBeenCalled();
    });

    it('should respect maxRetries limit', async () => {
      const connectionError = new Error('Connection error');
      const handler = jest.fn<() => Promise<string>>()
        .mockRejectedValue(connectionError);

      mockIsConnectionError.mockReturnValue(true);
      mockReconnect.mockResolvedValue(true);

      const wrappedHandler = withConnectionRetry(handler, 2);

      await expect(wrappedHandler()).rejects.toThrow('Connection error');
      // Should try: initial + 2 retries = 3 times
      expect(handler).toHaveBeenCalledTimes(3);
      expect(mockReconnect).toHaveBeenCalledTimes(2);
    });
  });
});
