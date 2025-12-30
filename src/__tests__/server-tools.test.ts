import { describe, it, expect, beforeEach, afterEach } from '@jest/globals';
import { listServers, listDatabases, switchServerDb } from '../tools/server-tools.js';
import { resetDbManager } from '../db-manager.js';

interface ServerInfo {
  name: string;
  isConnected: boolean;
  isDefault: boolean;
  defaultDatabase?: string;
  defaultSchema?: string;
  context?: string;
}

describe('Server Tools', () => {
  beforeEach(() => {
    resetDbManager();
    delete process.env.POSTGRES_SERVERS;
    delete process.env.POSTGRES_ACCESS_MODE;
  });

  afterEach(() => {
    resetDbManager();
  });

  describe('listServers', () => {
    it('should return empty list when no servers configured', async () => {
      const result = await listServers({});

      expect(result.servers).toEqual([]);
      expect(result.currentServer).toBeNull();
      expect(result.currentDatabase).toBeNull();
    });

    it('should list all configured servers', async () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        dev: { host: 'dev.example.com', port: '5432', username: 'user', password: 'pass' },
        staging: { host: 'staging.example.com', port: '5432', username: 'user', password: 'pass' },
        prod: { host: 'prod.example.com', port: '5432', username: 'user', password: 'pass' }
      });

      const result = await listServers({});

      expect(result.servers).toHaveLength(3);
      expect(result.servers.map((s: ServerInfo) => s.name)).toContain('dev');
      expect(result.servers.map((s: ServerInfo) => s.name)).toContain('staging');
      expect(result.servers.map((s: ServerInfo) => s.name)).toContain('prod');
    });

    it('should filter servers by name', async () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        dev: { host: 'dev.example.com', port: '5432', username: 'user', password: 'pass' },
        dev_backup: { host: 'dev-backup.example.com', port: '5432', username: 'user', password: 'pass' },
        prod: { host: 'prod.example.com', port: '5432', username: 'user', password: 'pass' }
      });

      const result = await listServers({ filter: 'dev' });

      expect(result.servers).toHaveLength(2);
      expect(result.servers.map((s: ServerInfo) => s.name)).toContain('dev');
      expect(result.servers.map((s: ServerInfo) => s.name)).toContain('dev_backup');
      expect(result.servers.map((s: ServerInfo) => s.name)).not.toContain('prod');
    });

    it('should show server connection status', async () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        dev: { host: 'localhost', port: '5432', username: 'user', password: 'pass' }
      });

      const result = await listServers({});

      expect(result.servers[0].isConnected).toBe(false);
    });

    it('should return context for servers with AI context configured', async () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        dev: {
          host: 'localhost',
          port: '5432',
          username: 'user',
          password: 'pass',
          context: 'Development server - safe for any queries'
        },
        prod: {
          host: 'prod.example.com',
          port: '5432',
          username: 'user',
          password: 'pass',
          context: 'PRODUCTION - Read-only queries only. Always use LIMIT.'
        }
      });

      const result = await listServers({});

      expect(result.servers).toHaveLength(2);
      const devServer = result.servers.find((s: ServerInfo) => s.name === 'dev');
      const prodServer = result.servers.find((s: ServerInfo) => s.name === 'prod');

      expect(devServer?.context).toBe('Development server - safe for any queries');
      expect(prodServer?.context).toBe('PRODUCTION - Read-only queries only. Always use LIMIT.');
    });

    it('should not expose host or port in response', async () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        dev: { host: 'secret.example.com', port: '5432', username: 'user', password: 'pass' }
      });

      const result = await listServers({});

      // Verify host and port are not exposed
      expect(result.servers[0]).not.toHaveProperty('host');
      expect(result.servers[0]).not.toHaveProperty('port');
      expect(result.servers[0]).toHaveProperty('name');
      expect(result.servers[0]).toHaveProperty('isConnected');
    });
  });

  describe('listDatabases', () => {
    it('should require serverName parameter', async () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        dev: { host: 'localhost', port: '5432', username: 'user', password: 'pass' }
      });

      await expect(listDatabases({ serverName: '' }))
        .rejects.toThrow('serverName is required');
    });

    it('should throw when server not found', async () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        dev: { host: 'localhost', port: '5432', username: 'user', password: 'pass' }
      });

      await expect(listDatabases({ serverName: 'nonexistent' }))
        .rejects.toThrow("Server 'nonexistent' not found");
    });

    it('should include available servers in error message', async () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        dev: { host: 'localhost', port: '5432', username: 'user', password: 'pass' },
        prod: { host: 'prod.example.com', port: '5432', username: 'user', password: 'pass' }
      });

      await expect(listDatabases({ serverName: 'nonexistent' }))
        .rejects.toThrow('Available servers: dev, prod');
    });
  });

  describe('switchServerDb', () => {
    it('should throw when server not found', async () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        dev: { host: 'localhost', port: '5432', username: 'user', password: 'pass' }
      });

      await expect(switchServerDb({ server: 'nonexistent' }))
        .rejects.toThrow("Server 'nonexistent' not found");
    });

    it('should validate server parameter is required', async () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        dev: { host: 'localhost', port: '5432', username: 'user', password: 'pass' }
      });

      await expect(switchServerDb({ server: '' }))
        .rejects.toThrow();
    });

    it('should validate database name format', async () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        dev: { host: 'localhost', port: '5432', username: 'user', password: 'pass' }
      });

      await expect(switchServerDb({ server: 'dev', database: 'invalid;db' }))
        .rejects.toThrow('Invalid database name');
    });

    // Note: Actual connection tests would require a real database
    // These tests focus on input validation
  });
});
