import { Pool, PoolClient, QueryResult, QueryResultRow } from 'pg';
import {
  ServerConfig,
  ServersConfig,
  ConnectionState,
  DatabaseInfo
} from './types.js';
import { isReadOnlySql } from './utils/validation.js';

const DEFAULT_PORT = '5432';
const DEFAULT_DATABASE = 'postgres';
const DEFAULT_QUERY_TIMEOUT_MS = 30000; // 30 seconds
const MAX_QUERY_TIMEOUT_MS = 300000; // 5 minutes

/**
 * Determines the access mode from environment variable.
 * POSTGRES_ACCESS_MODE can be 'readonly' or 'full' (default).
 */
function getAccessModeFromEnv(): boolean {
  const mode = process.env.POSTGRES_ACCESS_MODE?.toLowerCase().trim();
  if (mode === 'readonly' || mode === 'read-only' || mode === 'ro') {
    return true; // read-only mode
  }
  // Default is 'full' access (read-only = false)
  return false;
}

export class DatabaseManager {
  private serversConfig: ServersConfig;
  private connectionState: ConnectionState;
  private currentPool: Pool | null = null;
  private readOnlyMode: boolean;
  private queryTimeoutMs: number;

  constructor(readOnlyMode: boolean = true, queryTimeoutMs: number = DEFAULT_QUERY_TIMEOUT_MS) {
    this.serversConfig = this.loadServersConfig();
    this.connectionState = {
      currentServer: null,
      currentDatabase: null
    };
    this.readOnlyMode = readOnlyMode;
    this.queryTimeoutMs = Math.min(queryTimeoutMs, MAX_QUERY_TIMEOUT_MS);
  }

  private loadServersConfig(): ServersConfig {
    const configEnv = process.env.POSTGRES_SERVERS;
    if (!configEnv) {
      console.error('Warning: POSTGRES_SERVERS environment variable not set. Using empty config.');
      return {};
    }

    try {
      const parsed = JSON.parse(configEnv);

      // Validate the structure
      if (typeof parsed !== 'object' || parsed === null) {
        throw new Error('POSTGRES_SERVERS must be a JSON object');
      }

      for (const [name, config] of Object.entries(parsed)) {
        if (!config || typeof config !== 'object') {
          throw new Error(`Server '${name}' configuration is invalid`);
        }
        const serverConfig = config as any;
        if (!serverConfig.host || typeof serverConfig.host !== 'string') {
          throw new Error(`Server '${name}' must have a valid 'host' string`);
        }
      }

      return parsed as ServersConfig;
    } catch (error) {
      console.error('Error parsing POSTGRES_SERVERS:', error);
      return {};
    }
  }

  public getServersConfig(): ServersConfig {
    // Return a copy to prevent mutation
    return JSON.parse(JSON.stringify(this.serversConfig));
  }

  public getServerNames(): string[] {
    return Object.keys(this.serversConfig);
  }

  public getServerConfig(serverName: string): ServerConfig | null {
    const config = this.serversConfig[serverName];
    if (!config) return null;
    // Return a copy to prevent mutation
    return { ...config };
  }

  public getCurrentState(): ConnectionState {
    return { ...this.connectionState };
  }

  public isConnected(): boolean {
    return this.currentPool !== null;
  }

  public async switchServer(serverName: string, database?: string): Promise<void> {
    const serverConfig = this.getServerConfig(serverName);
    if (!serverConfig) {
      throw new Error(`Server '${serverName}' not found in configuration`);
    }

    // Close existing pool if any
    if (this.currentPool) {
      await this.currentPool.end();
      this.currentPool = null;
    }

    const dbName = database || DEFAULT_DATABASE;

    // Validate database name
    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(dbName)) {
      throw new Error('Invalid database name. Only alphanumeric characters and underscores are allowed.');
    }

    this.currentPool = new Pool({
      host: serverConfig.host,
      port: parseInt(serverConfig.port || DEFAULT_PORT, 10),
      user: serverConfig.username,
      password: serverConfig.password,
      database: dbName,
      max: 10,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 10000,
      statement_timeout: this.queryTimeoutMs
    });

    // Handle pool errors
    this.currentPool.on('error', (err) => {
      console.error('Unexpected pool error:', err);
    });

    // Test connection
    try {
      const client = await this.currentPool.connect();
      client.release();
      this.connectionState.currentServer = serverName;
      this.connectionState.currentDatabase = dbName;
    } catch (error) {
      await this.currentPool.end();
      this.currentPool = null;
      throw new Error(`Failed to connect to server '${serverName}': ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  public async switchDatabase(database: string): Promise<void> {
    if (!this.connectionState.currentServer) {
      throw new Error('No server selected. Please switch to a server first.');
    }

    await this.switchServer(this.connectionState.currentServer, database);
  }

  public async listDatabases(): Promise<DatabaseInfo[]> {
    const result = await this.query<DatabaseInfo>(`
      SELECT
        datname as name,
        pg_catalog.pg_get_userbyid(datdba) as owner,
        pg_catalog.pg_encoding_to_char(encoding) as encoding,
        pg_catalog.pg_size_pretty(pg_catalog.pg_database_size(datname)) as size
      FROM pg_catalog.pg_database
      WHERE datistemplate = false
      ORDER BY datname
    `);
    return result.rows;
  }

  public async query<T extends QueryResultRow = any>(sql: string, params?: any[]): Promise<QueryResult<T>> {
    if (!this.currentPool) {
      throw new Error('No database connection. Please switch to a server and database first.');
    }

    if (!sql || typeof sql !== 'string') {
      throw new Error('SQL query is required and must be a string');
    }

    // Check for read-only mode violations using improved validation
    if (this.readOnlyMode) {
      const { isReadOnly, reason } = isReadOnlySql(sql);
      if (!isReadOnly) {
        throw new Error(`Read-only mode violation: ${reason}`);
      }
    }

    try {
      return await this.currentPool.query<T>(sql, params);
    } catch (error) {
      // Provide more context in error messages
      if (error instanceof Error) {
        throw new Error(`Query failed: ${error.message}`);
      }
      throw error;
    }
  }

  public async getClient(): Promise<PoolClient> {
    if (!this.currentPool) {
      throw new Error('No database connection. Please switch to a server and database first.');
    }
    return this.currentPool.connect();
  }

  public async close(): Promise<void> {
    if (this.currentPool) {
      await this.currentPool.end();
      this.currentPool = null;
      this.connectionState.currentServer = null;
      this.connectionState.currentDatabase = null;
    }
  }

  public isReadOnly(): boolean {
    return this.readOnlyMode;
  }

  public setReadOnlyMode(readOnly: boolean): void {
    this.readOnlyMode = readOnly;
  }

  public setQueryTimeout(timeoutMs: number): void {
    this.queryTimeoutMs = Math.min(Math.max(1000, timeoutMs), MAX_QUERY_TIMEOUT_MS);
  }
}

// Singleton instance
let dbManager: DatabaseManager | null = null;

/**
 * Gets the singleton DatabaseManager instance.
 * The access mode is determined by the POSTGRES_ACCESS_MODE environment variable:
 * - 'readonly', 'read-only', 'ro': Read-only mode (prevents write operations)
 * - 'full' (default): Full access mode (allows all operations)
 */
export function getDbManager(): DatabaseManager {
  if (!dbManager) {
    const readOnlyMode = getAccessModeFromEnv();
    dbManager = new DatabaseManager(readOnlyMode);
    console.error(`PostgreSQL MCP: Access mode = ${readOnlyMode ? 'readonly' : 'full'}`);
  }
  return dbManager;
}

export function resetDbManager(): void {
  if (dbManager) {
    dbManager.close().catch(console.error);
    dbManager = null;
  }
}
