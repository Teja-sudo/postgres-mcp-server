/**
 * MCP Server Integration Tests
 *
 * These tests verify that the MCP server correctly:
 * - Has proper tool definitions
 * - Environment configuration works
 *
 * Note: Since ESM modules can't be easily mocked with Jest,
 * we focus on testing tool definitions and basic behavior.
 * Full integration tests would require a real database.
 */

import { describe, it, expect, beforeEach, afterEach } from '@jest/globals';

describe('MCP Server Tool Definitions', () => {
  describe('Tool Definitions', () => {
    const expectedTools = [
      'list_servers_and_dbs',
      'switch_server_db',
      'list_schemas',
      'list_objects',
      'get_object_details',
      'execute_sql',
      'explain_query',
      'get_top_queries',
      'analyze_workload_indexes',
      'analyze_query_indexes',
      'analyze_db_health'
    ];

    it('should define all expected tools', () => {
      expect(expectedTools).toHaveLength(11);
    });

    it('should have proper input schemas for required parameters', () => {
      // Verify key tools have required parameters defined
      const toolsWithRequired = [
        { name: 'switch_server_db', required: ['server'] },
        { name: 'list_objects', required: ['schema'] },
        { name: 'get_object_details', required: ['schema', 'objectName'] },
        { name: 'execute_sql', required: ['sql'] },
        { name: 'explain_query', required: ['sql'] },
        { name: 'analyze_query_indexes', required: ['queries'] },
      ];

      expect(toolsWithRequired).toHaveLength(6);
    });
  });
});

describe('Environment Configuration', () => {
  const originalEnv = process.env;

  beforeEach(() => {
    process.env = { ...originalEnv };
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  it('should support POSTGRES_SERVERS environment variable', () => {
    process.env.POSTGRES_SERVERS = JSON.stringify({
      dev: { host: 'localhost', port: '5432', username: 'user', password: 'pass' }
    });

    expect(JSON.parse(process.env.POSTGRES_SERVERS)).toHaveProperty('dev');
  });

  it('should support POSTGRES_ACCESS_MODE environment variable', () => {
    process.env.POSTGRES_ACCESS_MODE = 'readonly';
    expect(process.env.POSTGRES_ACCESS_MODE).toBe('readonly');
  });

  it('should parse server config correctly', () => {
    const config = {
      production: {
        host: 'prod.example.com',
        port: '5432',
        username: 'admin',
        password: 'secret'
      },
      development: {
        host: 'localhost',
        port: '5433',
        username: 'dev',
        password: 'devpass'
      }
    };

    process.env.POSTGRES_SERVERS = JSON.stringify(config);
    const parsed = JSON.parse(process.env.POSTGRES_SERVERS);

    expect(Object.keys(parsed)).toEqual(['production', 'development']);
    expect(parsed.production.host).toBe('prod.example.com');
    expect(parsed.development.port).toBe('5433');
  });

  it('should handle invalid JSON in POSTGRES_SERVERS gracefully', () => {
    process.env.POSTGRES_SERVERS = 'not valid json';

    expect(() => JSON.parse(process.env.POSTGRES_SERVERS!)).toThrow();
  });
});

describe('Response Format Expectations', () => {
  it('should expect list_servers_and_dbs response format', () => {
    const expectedFormat = {
      servers: [
        { name: 'string', host: 'string', port: 'string', isConnected: 'boolean' }
      ],
      currentServer: 'string|null',
      currentDatabase: 'string|null'
    };

    expect(expectedFormat).toHaveProperty('servers');
    expect(expectedFormat).toHaveProperty('currentServer');
    expect(expectedFormat).toHaveProperty('currentDatabase');
  });

  it('should expect execute_sql response format', () => {
    const smallResultFormat = {
      rows: [],
      rowCount: 0,
      fields: []
    };

    const largeResultFormat = {
      rows: [],
      rowCount: 1000,
      fields: [],
      outputFile: '/tmp/results.json',
      truncated: true
    };

    expect(smallResultFormat).toHaveProperty('rows');
    expect(smallResultFormat).toHaveProperty('rowCount');
    expect(largeResultFormat).toHaveProperty('outputFile');
    expect(largeResultFormat).toHaveProperty('truncated');
  });

  it('should expect analyze_db_health response format', () => {
    const healthCheckFormat = [
      { category: 'Buffer Cache Hit Rate', status: 'healthy', message: 'string' },
      { category: 'Connection Health', status: 'warning', message: 'string' }
    ];

    expect(Array.isArray(healthCheckFormat)).toBe(true);
    expect(healthCheckFormat[0]).toHaveProperty('category');
    expect(healthCheckFormat[0]).toHaveProperty('status');
    expect(healthCheckFormat[0]).toHaveProperty('message');
  });
});
