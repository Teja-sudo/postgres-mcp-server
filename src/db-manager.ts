import { Pool, PoolClient, QueryResult, QueryResultRow } from 'pg';
import {
  ServerConfig,
  ServersConfig,
  ConnectionState,
  DatabaseInfo
} from './types.js';

const DEFAULT_PORT = '5432';
const DEFAULT_DATABASE = 'postgres';

export class DatabaseManager {
  private serversConfig: ServersConfig;
  private connectionState: ConnectionState;
  private currentPool: Pool | null = null;
  private readOnlyMode: boolean;

  constructor(readOnlyMode: boolean = true) {
    this.serversConfig = this.loadServersConfig();
    this.connectionState = {
      currentServer: null,
      currentDatabase: null
    };
    this.readOnlyMode = readOnlyMode;
  }

  private loadServersConfig(): ServersConfig {
    const configEnv = process.env.POSTGRES_SERVERS;
    if (!configEnv) {
      console.error('Warning: POSTGRES_SERVERS environment variable not set. Using empty config.');
      return {};
    }

    try {
      return JSON.parse(configEnv) as ServersConfig;
    } catch (error) {
      console.error('Error parsing POSTGRES_SERVERS:', error);
      return {};
    }
  }

  public getServersConfig(): ServersConfig {
    return this.serversConfig;
  }

  public getServerNames(): string[] {
    return Object.keys(this.serversConfig);
  }

  public getServerConfig(serverName: string): ServerConfig | null {
    return this.serversConfig[serverName] || null;
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
    }

    const dbName = database || DEFAULT_DATABASE;

    this.currentPool = new Pool({
      host: serverConfig.host,
      port: parseInt(serverConfig.port || DEFAULT_PORT, 10),
      user: serverConfig.username,
      password: serverConfig.password,
      database: dbName,
      max: 10,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 10000
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
      throw new Error(`Failed to connect to server '${serverName}': ${error}`);
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

    // Check for read-only mode violations
    if (this.readOnlyMode) {
      const upperSql = sql.trim().toUpperCase();
      const writeOperations = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 'TRUNCATE', 'GRANT', 'REVOKE'];

      for (const op of writeOperations) {
        if (upperSql.startsWith(op)) {
          throw new Error(`Write operation '${op}' is not allowed in read-only mode`);
        }
      }
    }

    return this.currentPool.query<T>(sql, params);
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
}

// Singleton instance
let dbManager: DatabaseManager | null = null;

export function getDbManager(readOnlyMode: boolean = true): DatabaseManager {
  if (!dbManager) {
    dbManager = new DatabaseManager(readOnlyMode);
  }
  return dbManager;
}
