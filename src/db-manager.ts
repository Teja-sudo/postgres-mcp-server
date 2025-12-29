import { Pool, PoolClient, QueryResult, QueryResultRow } from "pg";
import { v4 as uuidv4 } from "uuid";
import {
  ServerConfig,
  ServersConfig,
  ConnectionState,
  ConnectionInfo,
  DatabaseInfo,
  ConnectionContext,
  TransactionInfo,
} from "./types.js";
import { isReadOnlySql } from "./utils/validation.js";

const DEFAULT_PORT = "5432";
const DEFAULT_DATABASE = "postgres";
const DEFAULT_SCHEMA = "public";
const DEFAULT_QUERY_TIMEOUT_MS = 30000; // 30 seconds
const MAX_QUERY_TIMEOUT_MS = 300000; // 5 minutes

/**
 * Converts the ServerConfig ssl option to pg Pool ssl config format.
 */
function getSslConfig(ssl: ServerConfig["ssl"]): boolean | object | undefined {
  if (ssl === undefined || ssl === false || ssl === "disable") {
    return undefined;
  }

  if (ssl === true || ssl === "require") {
    // Most cloud providers need rejectUnauthorized: false for self-signed certs
    return { rejectUnauthorized: false };
  }

  if (ssl === "prefer" || ssl === "allow") {
    return { rejectUnauthorized: false };
  }

  if (typeof ssl === "object") {
    return ssl;
  }

  return undefined;
}

/**
 * Determines the access mode from environment variable.
 * POSTGRES_ACCESS_MODE can be 'readonly' or 'full' (default).
 */
function getAccessModeFromEnv(): boolean {
  const mode = process.env.POSTGRES_ACCESS_MODE?.toLowerCase().trim();
  if (mode === "readonly" || mode === "read-only" || mode === "ro") {
    return true; // read-only mode
  }
  // Default is 'full' access (read-only = false)
  return false;
}

/**
 * Parses SSL configuration from environment variable string.
 * Accepts: "true", "false", "require", "prefer", "allow", "disable", or JSON object
 */
function parseSslFromEnv(sslValue: string | undefined): ServerConfig["ssl"] {
  if (!sslValue) return undefined;

  const lower = sslValue.toLowerCase().trim();
  if (lower === "true" || lower === "1") return true;
  if (lower === "false" || lower === "0") return false;
  if (
    lower === "require" ||
    lower === "prefer" ||
    lower === "allow" ||
    lower === "disable"
  ) {
    return lower as "require" | "prefer" | "allow" | "disable";
  }

  // Try parsing as JSON object
  try {
    const parsed = JSON.parse(sslValue);
    if (typeof parsed === "object") return parsed;
  } catch {
    // Not valid JSON, ignore
  }

  return undefined;
}

/**
 * Loads server configurations from individual PG_* environment variables.
 * Pattern: PG_NAME_1, PG_HOST_1, PG_PORT_1, PG_USERNAME_1, PG_PASSWORD_1,
 *          PG_DATABASE_1, PG_SCHEMA_1, PG_SSL_1, PG_DEFAULT_1, PG_CONTEXT_1
 */
function loadServersFromEnvVars(): ServersConfig {
  const servers: ServersConfig = {};
  const suffixes = new Set<string>();

  // Find all unique suffixes (e.g., _1, _2, _DEV, _PROD)
  for (const key of Object.keys(process.env)) {
    if (key.startsWith("PG_NAME_")) {
      const suffix = key.substring("PG_NAME".length); // includes the underscore
      suffixes.add(suffix);
    }
  }

  // Build server configs from each suffix
  for (const suffix of suffixes) {
    const name = process.env[`PG_NAME${suffix}`];
    const host = process.env[`PG_HOST${suffix}`];
    const username = process.env[`PG_USERNAME${suffix}`];
    const password = process.env[`PG_PASSWORD${suffix}`];

    // Name and host are required
    if (!name || !host) {
      console.error(
        `Warning: PG_NAME${suffix} or PG_HOST${suffix} missing, skipping server config`
      );
      continue;
    }

    // Username and password are required
    if (!username) {
      console.error(
        `Warning: PG_USERNAME${suffix} missing for server '${name}', skipping`
      );
      continue;
    }

    const config: ServerConfig = {
      host,
      port: process.env[`PG_PORT${suffix}`] || DEFAULT_PORT,
      username,
      password: password || "",
      defaultDatabase: process.env[`PG_DATABASE${suffix}`],
      defaultSchema: process.env[`PG_SCHEMA${suffix}`],
      isDefault: process.env[`PG_DEFAULT${suffix}`]?.toLowerCase() === "true",
      ssl: parseSslFromEnv(process.env[`PG_SSL${suffix}`]),
      context: process.env[`PG_CONTEXT${suffix}`],
    };

    servers[name] = config;
  }

  return servers;
}

/**
 * Loads server configurations from POSTGRES_SERVERS JSON environment variable.
 * (Legacy format for backward compatibility)
 */
function loadServersFromJson(): ServersConfig {
  const configEnv = process.env.POSTGRES_SERVERS;
  if (!configEnv) {
    return {};
  }

  try {
    const parsed = JSON.parse(configEnv);

    // Validate the structure
    if (typeof parsed !== "object" || parsed === null) {
      throw new Error("POSTGRES_SERVERS must be a JSON object");
    }

    for (const [name, config] of Object.entries(parsed)) {
      if (!config || typeof config !== "object") {
        throw new Error(`Server '${name}' configuration is invalid`);
      }
      const serverConfig = config as any;
      if (!serverConfig.host || typeof serverConfig.host !== "string") {
        throw new Error(`Server '${name}' must have a valid 'host' string`);
      }
    }

    return parsed as ServersConfig;
  } catch (error) {
    console.error("Error parsing POSTGRES_SERVERS:", error);
    return {};
  }
}

export class DatabaseManager {
  private serversConfig: ServersConfig;
  private connectionState: ConnectionState;
  private currentPool: Pool | null = null;
  private readOnlyMode: boolean;
  private queryTimeoutMs: number;
  private activeTransactions: Map<string, { client: PoolClient; info: TransactionInfo }> = new Map();

  constructor(
    readOnlyMode: boolean = true,
    queryTimeoutMs: number = DEFAULT_QUERY_TIMEOUT_MS
  ) {
    this.serversConfig = this.loadServersConfig();
    this.connectionState = {
      currentServer: null,
      currentDatabase: null,
      currentSchema: null,
    };
    this.readOnlyMode = readOnlyMode;
    this.queryTimeoutMs = Math.min(queryTimeoutMs, MAX_QUERY_TIMEOUT_MS);
  }

  private loadServersConfig(): ServersConfig {
    // Load from both sources - PG_* env vars take precedence over POSTGRES_SERVERS JSON
    const jsonServers = loadServersFromJson();
    const envServers = loadServersFromEnvVars();

    // Merge: env vars override JSON config
    const merged = { ...jsonServers, ...envServers };

    if (Object.keys(merged).length === 0) {
      console.error(
        "Warning: No server configuration found. Set PG_* environment variables or POSTGRES_SERVERS."
      );
    }

    return merged;
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

  public getDefaultServerName(): string | null {
    for (const [name, config] of Object.entries(this.serversConfig)) {
      if (config.isDefault) {
        return name;
      }
    }
    // If no default is set, return the first server
    const names = this.getServerNames();
    return names.length > 0 ? names[0] : null;
  }

  public async switchServer(
    serverName: string,
    database?: string,
    schema?: string
  ): Promise<void> {
    const serverConfig = this.getServerConfig(serverName);
    if (!serverConfig) {
      throw new Error(`Server '${serverName}' not found in configuration`);
    }

    // Close existing pool if any
    if (this.currentPool) {
      await this.currentPool.end();
      this.currentPool = null;
    }

    // Use provided database, server's default, or system default
    const dbName = database || serverConfig.defaultDatabase || DEFAULT_DATABASE;

    // Validate database name - allow alphanumeric, underscores, hyphens, but block SQL injection
    // PostgreSQL allows hyphens in database names when quoted (pg library handles this)
    if (
      !/^[a-zA-Z_][a-zA-Z0-9_-]*$/.test(dbName) ||
      /--|;|'|"|`/.test(dbName)
    ) {
      throw new Error(
        "Invalid database name. Allowed: letters, digits, underscores, hyphens. Cannot contain SQL characters (;, --, quotes)."
      );
    }

    const sslConfig = getSslConfig(serverConfig.ssl);

    this.currentPool = new Pool({
      host: serverConfig.host,
      port: parseInt(serverConfig.port || DEFAULT_PORT, 10),
      user: serverConfig.username,
      password: serverConfig.password,
      database: dbName,
      max: 10,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 10000,
      statement_timeout: this.queryTimeoutMs,
      ...(sslConfig && { ssl: sslConfig }),
    });

    // Handle pool errors
    this.currentPool.on("error", (err) => {
      console.error("Unexpected pool error:", err);
    });

    // Test connection
    try {
      const client = await this.currentPool.connect();
      client.release();
      this.connectionState.currentServer = serverName;
      this.connectionState.currentDatabase = dbName;
      // Use provided schema, server's default, or system default
      this.connectionState.currentSchema =
        schema || serverConfig.defaultSchema || DEFAULT_SCHEMA;
    } catch (error) {
      await this.currentPool.end();
      this.currentPool = null;
      throw new Error(
        `Failed to connect to server '${serverName}': ${
          error instanceof Error ? error.message : String(error)
        }`
      );
    }
  }

  public setCurrentSchema(schema: string): void {
    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(schema)) {
      throw new Error(
        "Invalid schema name. Only alphanumeric characters and underscores are allowed."
      );
    }
    this.connectionState.currentSchema = schema;
  }

  public getConnectionInfo(): ConnectionInfo {
    const currentServer = this.connectionState.currentServer;
    const serverConfig = currentServer ? this.serversConfig[currentServer] : null;

    return {
      isConnected: this.isConnected(),
      server: currentServer,
      database: this.connectionState.currentDatabase,
      schema: this.connectionState.currentSchema,
      accessMode: this.readOnlyMode ? "readonly" : "full",
      context: serverConfig?.context,
      user: serverConfig?.username,
    };
  }

  public async connectToDefault(): Promise<boolean> {
    const defaultServer = this.getDefaultServerName();
    if (!defaultServer) {
      return false;
    }

    try {
      await this.switchServer(defaultServer);
      return true;
    } catch (error) {
      console.error(`Failed to connect to default server: ${error}`);
      return false;
    }
  }

  public async switchDatabase(database: string): Promise<void> {
    if (!this.connectionState.currentServer) {
      throw new Error("No server selected. Please switch to a server first.");
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

  public async query<T extends QueryResultRow = any>(
    sql: string,
    params?: any[]
  ): Promise<QueryResult<T>> {
    if (!this.currentPool) {
      throw new Error(
        "No database connection. Please switch to a server and database first."
      );
    }

    if (!sql || typeof sql !== "string") {
      throw new Error("SQL query is required and must be a string");
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
      throw new Error(
        "No database connection. Please switch to a server and database first."
      );
    }
    return this.currentPool.connect();
  }

  public async close(): Promise<void> {
    if (this.currentPool) {
      await this.currentPool.end();
      this.currentPool = null;
      this.connectionState.currentServer = null;
      this.connectionState.currentDatabase = null;
      this.connectionState.currentSchema = null;
    }
  }

  /**
   * Invalidates the current connection without clearing the connection state.
   * This allows for automatic reconnection using the stored server/database/schema.
   */
  public async invalidateConnection(): Promise<void> {
    if (this.currentPool) {
      try {
        await this.currentPool.end();
      } catch (error) {
        console.error("Error closing pool during invalidation:", error);
      }
      this.currentPool = null;
    }
  }

  /**
   * Reconnects to the current server/database/schema.
   * Uses the stored connection state to re-establish the connection.
   * @returns true if reconnection was successful, false otherwise
   */
  public async reconnect(): Promise<boolean> {
    const { currentServer, currentDatabase, currentSchema } =
      this.connectionState;

    if (!currentServer) {
      console.error("Cannot reconnect: no server was previously connected");
      return false;
    }

    try {
      // Invalidate first to ensure clean state
      await this.invalidateConnection();

      // Reconnect with stored state
      await this.switchServer(
        currentServer,
        currentDatabase || undefined,
        currentSchema || undefined
      );
      console.error(
        `Reconnected to server '${currentServer}', database '${currentDatabase}'`
      );
      return true;
    } catch (error) {
      console.error(
        `Failed to reconnect: ${
          error instanceof Error ? error.message : String(error)
        }`
      );
      return false;
    }
  }

  /**
   * Checks if an error indicates a stale/broken connection that should trigger reconnection.
   */
  public static isConnectionError(error: any): boolean {
    if (!error) return false;

    const errorMessage = error.message || String(error);
    const errorCode = error.code || "";

    // Common connection error patterns
    const connectionErrorPatterns = [
      "Connection terminated",
      "connection terminated",
      "ECONNRESET",
      "ECONNREFUSED",
      "ETIMEDOUT",
      "EPIPE",
      "read ECONNRESET",
      "write ECONNRESET",
      "Client has encountered a connection error",
      "Connection lost",
      "Connection refused",
      "server closed the connection unexpectedly",
      "terminating connection due to administrator command",
      "SSL connection has been closed unexpectedly",
      "could not connect to server",
      "the database system is starting up",
      "the database system is shutting down",
      "no connection to the server",
      "server conn crashed",
      "database removed",
    ];

    // Check error codes
    const connectionErrorCodes = [
      "57P01", // admin_shutdown
      "57P02", // crash_shutdown
      "57P03", // cannot_connect_now
      "08000", // connection_exception
      "08003", // connection_does_not_exist
      "08006", // connection_failure
      "08001", // sqlclient_unable_to_establish_sqlconnection
      "08004", // sqlserver_rejected_establishment_of_sqlconnection
    ];

    // Check if error message matches any pattern
    for (const pattern of connectionErrorPatterns) {
      if (errorMessage.toLowerCase().includes(pattern.toLowerCase())) {
        return true;
      }
    }

    // Check if error code matches
    if (connectionErrorCodes.includes(errorCode)) {
      return true;
    }

    return false;
  }

  public isReadOnly(): boolean {
    return this.readOnlyMode;
  }

  public setReadOnlyMode(readOnly: boolean): void {
    this.readOnlyMode = readOnly;
  }

  public setQueryTimeout(timeoutMs: number): void {
    this.queryTimeoutMs = Math.min(
      Math.max(1000, timeoutMs),
      MAX_QUERY_TIMEOUT_MS
    );
  }

  /**
   * Returns the current connection context for including in tool responses
   */
  public getConnectionContext(): ConnectionContext {
    return {
      server: this.connectionState.currentServer,
      database: this.connectionState.currentDatabase,
      schema: this.connectionState.currentSchema,
    };
  }

  /**
   * Begins a new transaction and returns a transaction ID
   * @param name Optional human-readable name for the transaction
   */
  public async beginTransaction(name?: string): Promise<TransactionInfo> {
    if (!this.currentPool) {
      throw new Error(
        "No database connection. Please switch to a server and database first."
      );
    }

    const transactionId = uuidv4();
    const client = await this.currentPool.connect();

    try {
      await client.query("BEGIN");

      const info: TransactionInfo = {
        transactionId,
        name,
        server: this.connectionState.currentServer || "",
        database: this.connectionState.currentDatabase || "",
        schema: this.connectionState.currentSchema || "",
        startedAt: new Date(),
      };

      this.activeTransactions.set(transactionId, { client, info });
      return info;
    } catch (error) {
      client.release();
      throw error;
    }
  }

  /**
   * Commits an active transaction
   */
  public async commitTransaction(transactionId: string): Promise<void> {
    const transaction = this.activeTransactions.get(transactionId);
    if (!transaction) {
      throw new Error(`Transaction not found: ${transactionId}`);
    }

    try {
      await transaction.client.query("COMMIT");
    } finally {
      transaction.client.release();
      this.activeTransactions.delete(transactionId);
    }
  }

  /**
   * Rolls back an active transaction
   */
  public async rollbackTransaction(transactionId: string): Promise<void> {
    const transaction = this.activeTransactions.get(transactionId);
    if (!transaction) {
      throw new Error(`Transaction not found: ${transactionId}`);
    }

    try {
      await transaction.client.query("ROLLBACK");
    } finally {
      transaction.client.release();
      this.activeTransactions.delete(transactionId);
    }
  }

  /**
   * Executes a query within a transaction
   */
  public async queryInTransaction<T extends QueryResultRow = any>(
    transactionId: string,
    sql: string,
    params?: any[]
  ): Promise<QueryResult<T>> {
    const transaction = this.activeTransactions.get(transactionId);
    if (!transaction) {
      throw new Error(`Transaction not found: ${transactionId}`);
    }

    // Check for read-only mode violations
    if (this.readOnlyMode) {
      const { isReadOnly, reason } = isReadOnlySql(sql);
      if (!isReadOnly) {
        throw new Error(`Read-only mode violation: ${reason}`);
      }
    }

    return transaction.client.query<T>(sql, params);
  }

  /**
   * Gets information about an active transaction
   */
  public getTransactionInfo(transactionId: string): TransactionInfo | null {
    const transaction = this.activeTransactions.get(transactionId);
    return transaction ? { ...transaction.info } : null;
  }

  /**
   * Lists all active transactions
   */
  public listActiveTransactions(): TransactionInfo[] {
    return Array.from(this.activeTransactions.values()).map((t) => ({
      ...t.info,
    }));
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
    console.error(
      `PostgreSQL MCP: Access mode = ${readOnlyMode ? "readonly" : "full"}`
    );
  }
  return dbManager;
}

export function resetDbManager(): void {
  if (dbManager) {
    dbManager.close().catch(console.error);
    dbManager = null;
  }
}
