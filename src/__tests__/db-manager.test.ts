import { jest, describe, it, expect, beforeEach, afterEach } from '@jest/globals';
import { DatabaseManager, getDbManager, resetDbManager } from '../db-manager.js';

// Helper to clear all PG_* environment variables
function clearPgEnvVars() {
  for (const key of Object.keys(process.env)) {
    if (key.startsWith('PG_')) {
      delete process.env[key];
    }
  }
}

describe('DatabaseManager', () => {
  beforeEach(() => {
    // Reset the singleton and environment
    resetDbManager();
    delete process.env.POSTGRES_SERVERS;
    delete process.env.POSTGRES_ACCESS_MODE;
    clearPgEnvVars();
  });

  afterEach(() => {
    resetDbManager();
    clearPgEnvVars();
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

  describe('PG_* environment variable configuration', () => {
    it('should parse servers from PG_* env vars', () => {
      process.env.PG_NAME_1 = 'dev';
      process.env.PG_HOST_1 = 'localhost';
      process.env.PG_PORT_1 = '5432';
      process.env.PG_USERNAME_1 = 'user';
      process.env.PG_PASSWORD_1 = 'pass';
      process.env.PG_DATABASE_1 = 'mydb';
      process.env.PG_SCHEMA_1 = 'public';
      process.env.PG_SSL_1 = 'true';
      process.env.PG_DEFAULT_1 = 'true';

      const manager = new DatabaseManager();
      expect(manager.getServerNames()).toContain('dev');

      const config = manager.getServerConfig('dev');
      expect(config).toEqual({
        host: 'localhost',
        port: '5432',
        username: 'user',
        password: 'pass',
        defaultDatabase: 'mydb',
        defaultSchema: 'public',
        ssl: true,
        isDefault: true
      });
    });

    it('should parse multiple servers from PG_* env vars', () => {
      process.env.PG_NAME_1 = 'dev';
      process.env.PG_HOST_1 = 'localhost';
      process.env.PG_USERNAME_1 = 'dev_user';
      process.env.PG_PASSWORD_1 = 'dev_pass';

      process.env.PG_NAME_2 = 'prod';
      process.env.PG_HOST_2 = 'prod.example.com';
      process.env.PG_USERNAME_2 = 'prod_user';
      process.env.PG_PASSWORD_2 = 'prod_pass';
      process.env.PG_SSL_2 = 'require';

      const manager = new DatabaseManager();
      expect(manager.getServerNames()).toContain('dev');
      expect(manager.getServerNames()).toContain('prod');

      expect(manager.getServerConfig('dev')?.host).toBe('localhost');
      expect(manager.getServerConfig('prod')?.host).toBe('prod.example.com');
      expect(manager.getServerConfig('prod')?.ssl).toBe('require');
    });

    it('should use default port when not specified', () => {
      process.env.PG_NAME_1 = 'dev';
      process.env.PG_HOST_1 = 'localhost';
      process.env.PG_USERNAME_1 = 'user';

      const manager = new DatabaseManager();
      expect(manager.getServerConfig('dev')?.port).toBe('5432');
    });

    it('should skip servers missing required fields', () => {
      // Missing PG_HOST_1
      process.env.PG_NAME_1 = 'incomplete';
      process.env.PG_USERNAME_1 = 'user';

      const manager = new DatabaseManager();
      expect(manager.getServerNames()).not.toContain('incomplete');
    });

    it('should skip servers missing username', () => {
      process.env.PG_NAME_1 = 'nouser';
      process.env.PG_HOST_1 = 'localhost';
      // Missing PG_USERNAME_1

      const manager = new DatabaseManager();
      expect(manager.getServerNames()).not.toContain('nouser');
    });

    it('should parse SSL options correctly', () => {
      process.env.PG_NAME_1 = 'ssl_true';
      process.env.PG_HOST_1 = 'host1';
      process.env.PG_USERNAME_1 = 'user';
      process.env.PG_SSL_1 = 'true';

      process.env.PG_NAME_2 = 'ssl_false';
      process.env.PG_HOST_2 = 'host2';
      process.env.PG_USERNAME_2 = 'user';
      process.env.PG_SSL_2 = 'false';

      process.env.PG_NAME_3 = 'ssl_require';
      process.env.PG_HOST_3 = 'host3';
      process.env.PG_USERNAME_3 = 'user';
      process.env.PG_SSL_3 = 'require';

      const manager = new DatabaseManager();
      expect(manager.getServerConfig('ssl_true')?.ssl).toBe(true);
      expect(manager.getServerConfig('ssl_false')?.ssl).toBe(false);
      expect(manager.getServerConfig('ssl_require')?.ssl).toBe('require');
    });

    it('should allow named suffixes like _DEV, _PROD', () => {
      process.env.PG_NAME_DEV = 'development';
      process.env.PG_HOST_DEV = 'dev.example.com';
      process.env.PG_USERNAME_DEV = 'dev_user';

      process.env.PG_NAME_PROD = 'production';
      process.env.PG_HOST_PROD = 'prod.example.com';
      process.env.PG_USERNAME_PROD = 'prod_user';

      const manager = new DatabaseManager();
      expect(manager.getServerNames()).toContain('development');
      expect(manager.getServerNames()).toContain('production');
    });

    it('should merge PG_* vars with POSTGRES_SERVERS (PG_* takes precedence)', () => {
      // JSON config
      process.env.POSTGRES_SERVERS = JSON.stringify({
        dev: { host: 'json-host', port: '5432', username: 'json-user', password: 'json-pass' },
        staging: { host: 'staging-host', port: '5432', username: 'staging-user', password: 'staging-pass' }
      });

      // PG_* config (should override 'dev')
      process.env.PG_NAME_1 = 'dev';
      process.env.PG_HOST_1 = 'env-host';
      process.env.PG_USERNAME_1 = 'env-user';
      process.env.PG_PASSWORD_1 = 'env-pass';

      const manager = new DatabaseManager();

      // dev should come from PG_* (takes precedence)
      expect(manager.getServerConfig('dev')?.host).toBe('env-host');
      expect(manager.getServerConfig('dev')?.username).toBe('env-user');

      // staging should still come from JSON
      expect(manager.getServerConfig('staging')?.host).toBe('staging-host');
    });

    it('should parse context from PG_CONTEXT_* env vars', () => {
      process.env.PG_NAME_1 = 'dev';
      process.env.PG_HOST_1 = 'localhost';
      process.env.PG_USERNAME_1 = 'user';
      process.env.PG_CONTEXT_1 = 'Development server. Safe for any queries.';

      process.env.PG_NAME_PROD = 'production';
      process.env.PG_HOST_PROD = 'prod.example.com';
      process.env.PG_USERNAME_PROD = 'prod_user';
      process.env.PG_CONTEXT_PROD = 'PRODUCTION - Read-only queries only. Always use LIMIT.';

      const manager = new DatabaseManager();

      expect(manager.getServerConfig('dev')?.context).toBe('Development server. Safe for any queries.');
      expect(manager.getServerConfig('production')?.context).toBe('PRODUCTION - Read-only queries only. Always use LIMIT.');
    });

    it('should parse context from POSTGRES_SERVERS JSON', () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        dev: {
          host: 'localhost',
          port: '5432',
          username: 'user',
          password: 'pass',
          context: 'Test environment with sample data.'
        },
        prod: {
          host: 'prod.example.com',
          port: '5432',
          username: 'prod_user',
          password: 'prod_pass',
          context: 'Production - be careful!'
        }
      });

      const manager = new DatabaseManager();

      expect(manager.getServerConfig('dev')?.context).toBe('Test environment with sample data.');
      expect(manager.getServerConfig('prod')?.context).toBe('Production - be careful!');
    });

    it('should include context in getConnectionInfo', () => {
      process.env.PG_NAME_1 = 'dev';
      process.env.PG_HOST_1 = 'localhost';
      process.env.PG_USERNAME_1 = 'user';
      process.env.PG_CONTEXT_1 = 'Development context here';

      const manager = new DatabaseManager();
      // Note: Not connected yet, so context should be undefined
      const infoBeforeConnect = manager.getConnectionInfo();
      expect(infoBeforeConnect.context).toBeUndefined();
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

  describe('server-level access mode', () => {
    it('should use server-level access mode from PG_ACCESS_MODE_*', () => {
      process.env.PG_NAME_1 = 'prod';
      process.env.PG_HOST_1 = 'localhost';
      process.env.PG_USERNAME_1 = 'user';
      process.env.PG_PASSWORD_1 = 'pass';
      process.env.PG_ACCESS_MODE_1 = 'readonly';

      const manager = new DatabaseManager();
      expect(manager.getEffectiveAccessMode('prod', null)).toBe('readonly');
    });

    it('should support full access mode at server level', () => {
      process.env.PG_NAME_1 = 'dev';
      process.env.PG_HOST_1 = 'localhost';
      process.env.PG_USERNAME_1 = 'user';
      process.env.PG_PASSWORD_1 = 'pass';
      process.env.PG_ACCESS_MODE_1 = 'full';

      const manager = new DatabaseManager();
      expect(manager.getEffectiveAccessMode('dev', null)).toBe('full');
    });

    it('should support short access mode values (ro, rw)', () => {
      process.env.PG_NAME_1 = 'prod';
      process.env.PG_HOST_1 = 'localhost';
      process.env.PG_USERNAME_1 = 'user';
      process.env.PG_PASSWORD_1 = 'pass';
      process.env.PG_ACCESS_MODE_1 = 'ro';

      process.env.PG_NAME_2 = 'dev';
      process.env.PG_HOST_2 = 'localhost';
      process.env.PG_USERNAME_2 = 'user';
      process.env.PG_PASSWORD_2 = 'pass';
      process.env.PG_ACCESS_MODE_2 = 'rw';

      const manager = new DatabaseManager();
      expect(manager.getEffectiveAccessMode('prod', null)).toBe('readonly');
      expect(manager.getEffectiveAccessMode('dev', null)).toBe('full');
    });

    it('should override global access mode with server-level', () => {
      process.env.POSTGRES_ACCESS_MODE = 'readonly';
      process.env.PG_NAME_1 = 'dev';
      process.env.PG_HOST_1 = 'localhost';
      process.env.PG_USERNAME_1 = 'user';
      process.env.PG_PASSWORD_1 = 'pass';
      process.env.PG_ACCESS_MODE_1 = 'full';

      const manager = new DatabaseManager(true); // global readonly
      expect(manager.getEffectiveAccessMode('dev', null)).toBe('full'); // server overrides
      expect(manager.isReadOnly()).toBe(true); // global still readonly
    });

    it('should fallback to global when server has no access mode', () => {
      process.env.POSTGRES_ACCESS_MODE = 'readonly';
      process.env.PG_NAME_1 = 'prod';
      process.env.PG_HOST_1 = 'localhost';
      process.env.PG_USERNAME_1 = 'user';
      process.env.PG_PASSWORD_1 = 'pass';
      // No PG_ACCESS_MODE_1

      const manager = new DatabaseManager(true); // global readonly
      expect(manager.getEffectiveAccessMode('prod', null)).toBe('readonly');
    });
  });

  describe('database-level access mode', () => {
    it('should parse database access modes from PG_DB_ACCESS_MODES_*', () => {
      process.env.PG_NAME_1 = 'prod';
      process.env.PG_HOST_1 = 'localhost';
      process.env.PG_USERNAME_1 = 'user';
      process.env.PG_PASSWORD_1 = 'pass';
      process.env.PG_DB_ACCESS_MODES_1 = 'analytics:full,users:readonly';

      const manager = new DatabaseManager();
      expect(manager.getEffectiveAccessMode('prod', 'analytics')).toBe('full');
      expect(manager.getEffectiveAccessMode('prod', 'users')).toBe('readonly');
    });

    it('should support short mode values in database access modes', () => {
      process.env.PG_NAME_1 = 'prod';
      process.env.PG_HOST_1 = 'localhost';
      process.env.PG_USERNAME_1 = 'user';
      process.env.PG_PASSWORD_1 = 'pass';
      process.env.PG_DB_ACCESS_MODES_1 = 'dev_db:rw,prod_db:ro';

      const manager = new DatabaseManager();
      expect(manager.getEffectiveAccessMode('prod', 'dev_db')).toBe('full');
      expect(manager.getEffectiveAccessMode('prod', 'prod_db')).toBe('readonly');
    });

    it('should override server-level with database-level', () => {
      process.env.PG_NAME_1 = 'prod';
      process.env.PG_HOST_1 = 'localhost';
      process.env.PG_USERNAME_1 = 'user';
      process.env.PG_PASSWORD_1 = 'pass';
      process.env.PG_ACCESS_MODE_1 = 'readonly'; // server is readonly
      process.env.PG_DB_ACCESS_MODES_1 = 'analytics:full'; // but analytics is full

      const manager = new DatabaseManager();
      expect(manager.getEffectiveAccessMode('prod', 'analytics')).toBe('full');
      expect(manager.getEffectiveAccessMode('prod', 'users')).toBe('readonly'); // fallback to server
    });

    it('should handle spaces in database access modes', () => {
      process.env.PG_NAME_1 = 'prod';
      process.env.PG_HOST_1 = 'localhost';
      process.env.PG_USERNAME_1 = 'user';
      process.env.PG_PASSWORD_1 = 'pass';
      process.env.PG_DB_ACCESS_MODES_1 = 'db1:full , db2:readonly , db3:ro';

      const manager = new DatabaseManager();
      expect(manager.getEffectiveAccessMode('prod', 'db1')).toBe('full');
      expect(manager.getEffectiveAccessMode('prod', 'db2')).toBe('readonly');
      expect(manager.getEffectiveAccessMode('prod', 'db3')).toBe('readonly');
    });

    it('should handle invalid database access mode entries gracefully', () => {
      process.env.PG_NAME_1 = 'prod';
      process.env.PG_HOST_1 = 'localhost';
      process.env.PG_USERNAME_1 = 'user';
      process.env.PG_PASSWORD_1 = 'pass';
      process.env.PG_DB_ACCESS_MODES_1 = 'valid:full,invalid,alsoInvalid:badmode';

      const manager = new DatabaseManager(false); // global full access
      expect(manager.getEffectiveAccessMode('prod', 'valid')).toBe('full');
      // Invalid entries should be ignored, fallback to global default
      expect(manager.getEffectiveAccessMode('prod', 'invalid')).toBe('full');
    });
  });

  describe('access mode priority', () => {
    it('should follow priority: database > server > global', () => {
      process.env.POSTGRES_ACCESS_MODE = 'readonly'; // global
      process.env.PG_NAME_1 = 'myserver';
      process.env.PG_HOST_1 = 'localhost';
      process.env.PG_USERNAME_1 = 'user';
      process.env.PG_PASSWORD_1 = 'pass';
      process.env.PG_ACCESS_MODE_1 = 'full'; // server overrides global
      process.env.PG_DB_ACCESS_MODES_1 = 'protected:readonly'; // db overrides server

      const manager = new DatabaseManager(true); // global readonly

      // database-level takes highest priority
      expect(manager.getEffectiveAccessMode('myserver', 'protected')).toBe('readonly');
      // server-level for unspecified databases
      expect(manager.getEffectiveAccessMode('myserver', 'other_db')).toBe('full');
      // unknown server falls back to global
      expect(manager.getEffectiveAccessMode('unknown', 'any_db')).toBe('readonly');
    });

    it('should use isReadOnlyFor for context-aware checks', () => {
      process.env.PG_NAME_1 = 'prod';
      process.env.PG_HOST_1 = 'localhost';
      process.env.PG_USERNAME_1 = 'user';
      process.env.PG_PASSWORD_1 = 'pass';
      process.env.PG_ACCESS_MODE_1 = 'readonly';
      process.env.PG_DB_ACCESS_MODES_1 = 'analytics:full';

      const manager = new DatabaseManager();
      expect(manager.isReadOnlyFor('prod', 'users')).toBe(true);
      expect(manager.isReadOnlyFor('prod', 'analytics')).toBe(false);
    });
  });

  describe('JSON config access mode', () => {
    it('should support accessMode in JSON config', () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        prod: {
          host: 'localhost',
          username: 'user',
          password: 'pass',
          accessMode: 'readonly'
        }
      });

      const manager = new DatabaseManager();
      expect(manager.getEffectiveAccessMode('prod', null)).toBe('readonly');
    });

    it('should support databaseAccessModes object in JSON config', () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        prod: {
          host: 'localhost',
          username: 'user',
          password: 'pass',
          accessMode: 'readonly',
          databaseAccessModes: {
            analytics: 'full',
            users: 'readonly'
          }
        }
      });

      const manager = new DatabaseManager();
      expect(manager.getEffectiveAccessMode('prod', 'analytics')).toBe('full');
      expect(manager.getEffectiveAccessMode('prod', 'users')).toBe('readonly');
      expect(manager.getEffectiveAccessMode('prod', 'other')).toBe('readonly'); // server fallback
    });

    it('should support databaseAccessModes string format in JSON config', () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        prod: {
          host: 'localhost',
          username: 'user',
          password: 'pass',
          databaseAccessModes: 'analytics:full,users:ro'
        }
      });

      const manager = new DatabaseManager();
      expect(manager.getEffectiveAccessMode('prod', 'analytics')).toBe('full');
      expect(manager.getEffectiveAccessMode('prod', 'users')).toBe('readonly');
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
      await expect(manager.switchServer('dev', 'db--name')).rejects.toThrow('Invalid database name'); // SQL comment
      await expect(manager.switchServer('dev', "db'name")).rejects.toThrow('Invalid database name'); // quote
      // Hotfix-3.0.3: digit-leading names are NOW accepted (the value
      // passes through escapeIdentifier which quotes it).  '123db' /
      // '1188' would only fail at the actual PG connect step if the
      // server lacks the database — that's not a validation error.
      // Compound-hyphen names are still allowed.
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
