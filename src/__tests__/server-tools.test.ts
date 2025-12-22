import { describe, it, expect, beforeEach, afterEach } from '@jest/globals';
import { listServersAndDbs, switchServerDb } from '../tools/server-tools.js';
import { resetDbManager } from '../db-manager.js';

describe('Server Tools', () => {
  beforeEach(() => {
    resetDbManager();
    delete process.env.POSTGRES_SERVERS;
    delete process.env.POSTGRES_ACCESS_MODE;
  });

  afterEach(() => {
    resetDbManager();
  });

  describe('listServersAndDbs', () => {
    it('should return empty list when no servers configured', async () => {
      const result = await listServersAndDbs({});

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

      const result = await listServersAndDbs({});

      expect(result.servers).toHaveLength(3);
      expect(result.servers.map(s => s.name)).toContain('dev');
      expect(result.servers.map(s => s.name)).toContain('staging');
      expect(result.servers.map(s => s.name)).toContain('prod');
    });

    it('should filter servers by name', async () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        dev: { host: 'dev.example.com', port: '5432', username: 'user', password: 'pass' },
        dev_backup: { host: 'dev-backup.example.com', port: '5432', username: 'user', password: 'pass' },
        prod: { host: 'prod.example.com', port: '5432', username: 'user', password: 'pass' }
      });

      const result = await listServersAndDbs({ filter: 'dev' });

      expect(result.servers).toHaveLength(2);
      expect(result.servers.map(s => s.name)).toContain('dev');
      expect(result.servers.map(s => s.name)).toContain('dev_backup');
      expect(result.servers.map(s => s.name)).not.toContain('prod');
    });

    it('should filter servers by host', async () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        server1: { host: 'us-east.example.com', port: '5432', username: 'user', password: 'pass' },
        server2: { host: 'us-west.example.com', port: '5432', username: 'user', password: 'pass' },
        server3: { host: 'eu.example.com', port: '5432', username: 'user', password: 'pass' }
      });

      const result = await listServersAndDbs({ filter: 'us-' });

      expect(result.servers).toHaveLength(2);
    });

    it('should show server connection status', async () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        dev: { host: 'localhost', port: '5432', username: 'user', password: 'pass' }
      });

      const result = await listServersAndDbs({});

      expect(result.servers[0].isConnected).toBe(false);
    });

    it('should use default port when not specified', async () => {
      process.env.POSTGRES_SERVERS = JSON.stringify({
        dev: { host: 'localhost', username: 'user', password: 'pass' }
      });

      const result = await listServersAndDbs({});

      expect(result.servers[0].port).toBe('5432');
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
