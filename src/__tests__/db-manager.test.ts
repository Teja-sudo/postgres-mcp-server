import { jest, describe, it, expect, beforeEach, afterEach } from '@jest/globals';
import { DatabaseManager, getDbManager, resetDbManager } from '../db-manager.js';

describe('DatabaseManager', () => {
  beforeEach(() => {
    // Reset the singleton and environment
    resetDbManager();
    delete process.env.POSTGRES_SERVERS;
    delete process.env.POSTGRES_ACCESS_MODE;
  });

  afterEach(() => {
    resetDbManager();
  });

  describe('constructor and configuration', () => {
    it('should handle missing POSTGRES_SERVERS env var', () => {
      const manager = new DatabaseManager();
      expect(manager.getServerNames()).toEqual([]);
      expect(manager.getServersConfig()).toEqual({});
    });

    it('should parse valid POSTGRES_SERVERS config', () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        dev: { host: 'localhost', port: '5432', username: 'user', password: 'pass' },
        prod: { host: 'prod.example.com', port: '5433', username: 'admin', password: 'secret' }
      });

      const manager = new DatabaseManager();
      expect(manager.getServerNames()).toEqual(['dev', 'prod']);

      const devConfig = manager.getServerConfig('dev');
      expect(devConfig).toEqual({ host: 'localhost', port: '5432', username: 'user', password: 'pass' });
    });

    it('should handle invalid JSON in POSTGRES_SERVERS', () => {
      process.env.POSTGRES_SERVERS = 'invalid json';
      const manager = new DatabaseManager();
      expect(manager.getServerNames()).toEqual([]);
    });

    it('should validate server config structure', () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        invalid: { port: '5432' } // missing host
      });
      const manager = new DatabaseManager();
      expect(manager.getServerNames()).toEqual([]);
    });

    it('should return null for non-existent server', () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        dev: { host: 'localhost', port: '5432', username: 'user', password: 'pass' }
      });
      const manager = new DatabaseManager();
      expect(manager.getServerConfig('nonexistent')).toBeNull();
    });
  });

  describe('access mode from environment', () => {
    it('should default to full access mode', () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        dev: { host: 'localhost', port: '5432', username: 'user', password: 'pass' }
      });

      const manager = getDbManager();
      expect(manager.isReadOnly()).toBe(false);
    });

    it('should enable read-only mode with POSTGRES_ACCESS_MODE=readonly', () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        dev: { host: 'localhost', port: '5432', username: 'user', password: 'pass' }
      });
      process.env.POSTGRES_ACCESS_MODE = 'readonly';

      const manager = getDbManager();
      expect(manager.isReadOnly()).toBe(true);
    });

    it('should enable read-only mode with POSTGRES_ACCESS_MODE=read-only', () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        dev: { host: 'localhost', port: '5432', username: 'user', password: 'pass' }
      });
      process.env.POSTGRES_ACCESS_MODE = 'read-only';

      const manager = getDbManager();
      expect(manager.isReadOnly()).toBe(true);
    });

    it('should enable read-only mode with POSTGRES_ACCESS_MODE=ro', () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        dev: { host: 'localhost', port: '5432', username: 'user', password: 'pass' }
      });
      process.env.POSTGRES_ACCESS_MODE = 'ro';

      const manager = getDbManager();
      expect(manager.isReadOnly()).toBe(true);
    });

    it('should handle case-insensitive access mode', () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        dev: { host: 'localhost', port: '5432', username: 'user', password: 'pass' }
      });
      process.env.POSTGRES_ACCESS_MODE = 'READONLY';

      const manager = getDbManager();
      expect(manager.isReadOnly()).toBe(true);
    });
  });

  describe('connection state', () => {
    it('should start with no connection', () => {
      const manager = new DatabaseManager();
      expect(manager.isConnected()).toBe(false);
      expect(manager.getCurrentState()).toEqual({
        currentServer: null,
        currentDatabase: null,
        currentSchema: null
      });
    });

    it('should throw when server not found', async () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        dev: { host: 'localhost', port: '5432', username: 'user', password: 'pass' }
      });
      const manager = new DatabaseManager();

      await expect(manager.switchServer('nonexistent')).rejects.toThrow("Server 'nonexistent' not found");
    });

    it('should validate database name', async () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        dev: { host: 'localhost', port: '5432', username: 'user', password: 'pass' }
      });
      const manager = new DatabaseManager();

      await expect(manager.switchServer('dev', 'invalid;db')).rejects.toThrow('Invalid database name');
      await expect(manager.switchServer('dev', 'db--name')).rejects.toThrow('Invalid database name');
      await expect(manager.switchServer('dev', '123db')).rejects.toThrow('Invalid database name');
    });

    it('should throw when querying without connection', async () => {
      const manager = new DatabaseManager();
      await expect(manager.query('SELECT 1')).rejects.toThrow('No database connection');
    });

    it('should throw when getting client without connection', async () => {
      const manager = new DatabaseManager();
      await expect(manager.getClient()).rejects.toThrow('No database connection');
    });

    it('should throw when switching database without server', async () => {
      const manager = new DatabaseManager();
      await expect(manager.switchDatabase('mydb')).rejects.toThrow('No server selected');
    });
  });

  describe('read-only mode enforcement', () => {
    let manager: DatabaseManager;

    beforeEach(() => {
      manager = new DatabaseManager(true); // read-only mode
    });

    it('should block INSERT queries in read-only mode', async () => {
      // Mock the pool to test the validation
      (manager as any).currentPool = { query: jest.fn() };

      await expect(manager.query("INSERT INTO users VALUES (1)")).rejects.toThrow('Read-only mode violation');
    });

    it('should block UPDATE queries in read-only mode', async () => {
      (manager as any).currentPool = { query: jest.fn() };

      await expect(manager.query("UPDATE users SET name = 'test'")).rejects.toThrow('Read-only mode violation');
    });

    it('should block DELETE queries in read-only mode', async () => {
      (manager as any).currentPool = { query: jest.fn() };

      await expect(manager.query("DELETE FROM users")).rejects.toThrow('Read-only mode violation');
    });

    it('should block DROP queries in read-only mode', async () => {
      (manager as any).currentPool = { query: jest.fn() };

      await expect(manager.query("DROP TABLE users")).rejects.toThrow('Read-only mode violation');
    });

    it('should block CREATE queries in read-only mode', async () => {
      (manager as any).currentPool = { query: jest.fn() };

      await expect(manager.query("CREATE TABLE test (id INT)")).rejects.toThrow('Read-only mode violation');
    });

    it('should block CTE with write operations', async () => {
      (manager as any).currentPool = { query: jest.fn() };

      await expect(manager.query("WITH x AS (DELETE FROM users RETURNING *) SELECT * FROM x"))
        .rejects.toThrow('Read-only mode violation');
    });

    it('should allow SELECT queries in read-only mode', async () => {
      const mockQuery = jest.fn<() => Promise<{ rows: unknown[]; fields: unknown[] }>>()
        .mockResolvedValue({ rows: [], fields: [] });
      (manager as any).currentPool = { query: mockQuery };

      await manager.query("SELECT * FROM users");
      expect(mockQuery).toHaveBeenCalled();
    });
  });

  describe('full access mode', () => {
    let manager: DatabaseManager;

    beforeEach(() => {
      manager = new DatabaseManager(false); // full access mode
    });

    it('should allow INSERT queries in full access mode', async () => {
      const mockQuery = jest.fn<() => Promise<{ rows: unknown[]; fields: unknown[] }>>()
        .mockResolvedValue({ rows: [], fields: [] });
      (manager as any).currentPool = { query: mockQuery };

      await manager.query("INSERT INTO users VALUES (1)");
      expect(mockQuery).toHaveBeenCalled();
    });

    it('should allow UPDATE queries in full access mode', async () => {
      const mockQuery = jest.fn<() => Promise<{ rows: unknown[]; fields: unknown[] }>>()
        .mockResolvedValue({ rows: [], fields: [] });
      (manager as any).currentPool = { query: mockQuery };

      await manager.query("UPDATE users SET name = 'test'");
      expect(mockQuery).toHaveBeenCalled();
    });
  });

  describe('singleton behavior', () => {
    it('should return same instance on multiple calls', () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        dev: { host: 'localhost', port: '5432', username: 'user', password: 'pass' }
      });

      const manager1 = getDbManager();
      const manager2 = getDbManager();
      expect(manager1).toBe(manager2);
    });

    it('should create new instance after reset', () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        dev: { host: 'localhost', port: '5432', username: 'user', password: 'pass' }
      });

      const manager1 = getDbManager();
      resetDbManager();
      const manager2 = getDbManager();
      expect(manager1).not.toBe(manager2);
    });
  });

  describe('query validation', () => {
    it('should reject empty SQL', async () => {
      const manager = new DatabaseManager(false);
      (manager as any).currentPool = { query: jest.fn() };

      await expect(manager.query('')).rejects.toThrow('SQL query is required');
      await expect(manager.query(null as any)).rejects.toThrow('SQL query is required');
    });
  });

  describe('setReadOnlyMode', () => {
    it('should allow changing read-only mode', () => {
      const manager = new DatabaseManager(true);
      expect(manager.isReadOnly()).toBe(true);

      manager.setReadOnlyMode(false);
      expect(manager.isReadOnly()).toBe(false);

      manager.setReadOnlyMode(true);
      expect(manager.isReadOnly()).toBe(true);
    });
  });

  describe('setQueryTimeout', () => {
    it('should set query timeout within limits', () => {
      const manager = new DatabaseManager();

      manager.setQueryTimeout(60000);
      expect((manager as any).queryTimeoutMs).toBe(60000);
    });

    it('should enforce minimum timeout', () => {
      const manager = new DatabaseManager();

      manager.setQueryTimeout(100);
      expect((manager as any).queryTimeoutMs).toBe(1000);
    });

    it('should enforce maximum timeout', () => {
      const manager = new DatabaseManager();

      manager.setQueryTimeout(1000000);
      expect((manager as any).queryTimeoutMs).toBe(300000);
    });
  });
});
