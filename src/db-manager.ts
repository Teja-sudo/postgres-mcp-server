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
  ConnectionOverride,
} from "./types.js";
import { isReadOnlySql } from "./utils/validation.js";
import { validateDatabaseName, validateSchemaName } from "./db-manager/validation.js";

const DEFAULT_PORT = "5432";
const DEFAULT_DATABASE = "postgres";
const DEFAULT_SCHEMA = "public";
const DEFAULT_QUERY_TIMEOUT_MS = 30000; // 30 seconds
const MAX_QUERY_TIMEOUT_MS = 300000; // 5 minutes

/** SSL configuration object for pg Pool */
interface SslConfigObject {
  rejectUnauthorized?: boolean;
  ca?: string;
  [key: string]: unknown;
}

// Pool cache configuration
const MAX_CACHED_POOLS = 10; // Maximum number of cached pools for override connections
const POOL_IDLE_TIMEOUT_MS = 300000; // 5 minutes - idle pools are closed after this time
const POOL_CLEANUP_INTERVAL_MS = 60000; // 1 minute - interval for checking and cleaning up idle pools
const MAX_POOL_SIZE_MAIN = 10; // Max connections for main pool
const MAX_POOL_SIZE_CACHED = 5; // Max connections per cached pool
const MAX_TOTAL_CONNECTIONS = 50; // Global limit across all pools

/**
 * Represents a cached connection pool with metadata for LRU eviction and idle cleanup
 */
interface CachedPool {
  pool: Pool;
  serverName: string;
  database: string;
  lastUsed: number;
  createdAt: number;
  activeConnections: number; // Track active connections for this pool
}

/**
 * Result from getClientWithOverride containing the client, release function, and resolved schema
 */
export interface OverrideClientResult {
  client: PoolClient;
  release: () => void;
  server: string;
  database: string;
  schema: string;
  context?: string;
  isOverride: boolean;
}

/**
 * Converts the ServerConfig ssl option to pg Pool ssl config format.
 * Returns undefined for disabled SSL, or an SSL config object for enabled SSL.
 */
function getSslConfig(ssl: ServerConfig["ssl"]): SslConfigObject | undefined {
  if (ssl === undefined || ssl === false || ssl === "disable") {
    return undefined;
  }

  if (ssl === true || ssl === "require" || ssl === "prefer" || ssl === "allow") {
    // Most cloud providers need rejectUnauthorized: false for self-signed certs
    return { rejectUnauthorized: false };
  }

  if (typeof ssl === "object") {
    return ssl as SslConfigObject;
  }

  return undefined;
}

/**
 * Determines the access mode from environment variable.
 * POSTGRES_ACCESS_MODE can be 'readonly' or 'full' (default).
 */
function getAccessModeFromEnv(): boolean {
  const mode = process.env.POSTGRES_ACCESS_MODE?.toLowerCase().trim();
  return mode === "readonly" || mode === "read-only" || mode === "ro";
}

/**
 * Parses SSL configuration from environment variable string.
 * Accepts: "true", "false", "require", "prefer", "allow", "disable", or JSON object
 * Note: Returns union type intentionally - this is a parser function
 */
// eslint-disable-next-line sonarjs/function-return-type
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
  private activeTransactions: Map<
    string,
    { client: PoolClient; info: TransactionInfo }
  > = new Map();

  // Pool cache for connection overrides (keyed by "serverName:database")
  private poolCache: Map<string, CachedPool> = new Map();
  private poolCleanupInterval: ReturnType<typeof setInterval> | null = null;

  // Track pool creation promises to prevent race conditions
  // When multiple concurrent requests need the same pool, they all wait for the same Promise
  private poolCreationPromises: Map<string, Promise<CachedPool>> = new Map();

  // Track total active connections across all pools for global limit enforcement
  private totalActiveConnections: number = 0;
  private mainPoolActiveConnections: number = 0;

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

    // Start pool cleanup interval
    this.startPoolCleanup();
  }

  /**
   * Starts the periodic cleanup of idle cached pools
   */
  private startPoolCleanup(): void {
    if (this.poolCleanupInterval) {
      return;
    }

    this.poolCleanupInterval = setInterval(() => {
      this.cleanupIdlePools();
    }, POOL_CLEANUP_INTERVAL_MS);

    // Allow the process to exit even if the interval is running
    if (this.poolCleanupInterval.unref) {
      this.poolCleanupInterval.unref();
    }
  }

  /**
   * Cleans up pools that have been idle for too long
   */
  private cleanupIdlePools(): void {
    const now = Date.now();
    const poolsToRemove: string[] = [];

    for (const [key, cached] of this.poolCache.entries()) {
      if (now - cached.lastUsed > POOL_IDLE_TIMEOUT_MS) {
        poolsToRemove.push(key);
      }
    }

    for (const key of poolsToRemove) {
      const cached = this.poolCache.get(key);
      if (cached) {
        cached.pool.end().catch((err) => {
          console.error(`Error closing idle pool ${key}:`, err);
        });
        this.poolCache.delete(key);
      }
    }
  }

  /**
   * Generates a cache key for a server/database combination
   */
  private getPoolCacheKey(serverName: string, database: string): string {
    return `${serverName}:${database}`;
  }

  /**
   * Evicts the least recently used pool when cache is full
   */
  private evictLruPool(): void {
    if (this.poolCache.size < MAX_CACHED_POOLS) {
      return;
    }

    let oldestKey: string | null = null;
    let oldestTime = Infinity;

    for (const [key, cached] of this.poolCache.entries()) {
      if (cached.lastUsed < oldestTime) {
        oldestTime = cached.lastUsed;
        oldestKey = key;
      }
    }

    if (oldestKey) {
      const cached = this.poolCache.get(oldestKey);
      if (cached) {
        cached.pool.end().catch((err) => {
          console.error(`Error closing evicted pool ${oldestKey}:`, err);
        });
        this.poolCache.delete(oldestKey);
      }
    }
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

    // Validate database name for SQL injection prevention
    validateDatabaseName(dbName);

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
    validateSchemaName(schema);
    this.connectionState.currentSchema = schema;
  }

  public getConnectionInfo(): ConnectionInfo {
    const currentServer = this.connectionState.currentServer;
    const serverConfig = currentServer
      ? this.serversConfig[currentServer]
      : null;

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

  /**
   * Checks if we can acquire a new connection without exceeding global limits
   */
  private canAcquireConnection(): boolean {
    return this.totalActiveConnections < MAX_TOTAL_CONNECTIONS;
  }

  /**
   * Acquires a client from the main pool with proper tracking
   */
  private async acquireMainPoolClient(): Promise<{ client: PoolClient; release: () => void }> {
    if (!this.currentPool) {
      throw new Error(
        "No database connection. Please switch to a server and database first."
      );
    }

    if (!this.canAcquireConnection()) {
      throw new Error(
        `Connection limit reached (${MAX_TOTAL_CONNECTIONS} max). Please wait for existing operations to complete.`
      );
    }

    const client = await this.currentPool.connect();
    this.totalActiveConnections++;
    this.mainPoolActiveConnections++;

    const release = () => {
      this.totalActiveConnections = Math.max(0, this.totalActiveConnections - 1);
      this.mainPoolActiveConnections = Math.max(0, this.mainPoolActiveConnections - 1);
      client.release();
    };

    return { client, release };
  }

  /**
   * Acquires a client from a cached pool with proper tracking
   */
  private async acquireCachedPoolClient(
    cached: CachedPool
  ): Promise<{ client: PoolClient; release: () => void }> {
    if (!this.canAcquireConnection()) {
      throw new Error(
        `Connection limit reached (${MAX_TOTAL_CONNECTIONS} max). Please wait for existing operations to complete.`
      );
    }

    const client = await cached.pool.connect();
    this.totalActiveConnections++;
    cached.activeConnections++;
    cached.lastUsed = Date.now();

    const release = () => {
      this.totalActiveConnections = Math.max(0, this.totalActiveConnections - 1);
      cached.activeConnections = Math.max(0, cached.activeConnections - 1);
      client.release();
    };

    return { client, release };
  }

  /**
   * Creates a new cached pool. This method handles concurrent creation requests
   * by storing the creation promise so multiple callers wait for the same pool.
   */
  private async getOrCreateCachedPool(
    serverName: string,
    database: string,
    serverConfig: ServerConfig
  ): Promise<CachedPool> {
    const cacheKey = this.getPoolCacheKey(serverName, database);

    // Check if pool already exists
    const existing = this.poolCache.get(cacheKey);
    if (existing) {
      return existing;
    }

    // Check if pool creation is already in progress (handles concurrent requests)
    const inProgress = this.poolCreationPromises.get(cacheKey);
    if (inProgress) {
      return inProgress;
    }

    // Create pool - wrap in promise and store it so concurrent requests wait
    const creationPromise = (async (): Promise<CachedPool> => {
      try {
        // Double-check after acquiring "lock" (in case another request completed)
        const existingAfterLock = this.poolCache.get(cacheKey);
        if (existingAfterLock) {
          return existingAfterLock;
        }

        // Evict LRU pool if cache is full
        this.evictLruPool();

        // Create new pool with smaller size for cached connections
        const sslConfig = getSslConfig(serverConfig.ssl);
        const pool = new Pool({
          host: serverConfig.host,
          port: parseInt(serverConfig.port || DEFAULT_PORT, 10),
          user: serverConfig.username,
          password: serverConfig.password,
          database,
          max: MAX_POOL_SIZE_CACHED,
          idleTimeoutMillis: 30000,
          connectionTimeoutMillis: 10000,
          statement_timeout: this.queryTimeoutMs,
          ...(sslConfig && { ssl: sslConfig }),
        });

        // Handle pool errors - remove from cache on critical errors
        pool.on("error", (err) => {
          console.error(`Pool error for ${cacheKey}:`, err);
          this.poolCache.delete(cacheKey);
        });

        // Test connection before caching
        const testClient = await pool.connect();
        testClient.release();

        const cached: CachedPool = {
          pool,
          serverName,
          database,
          lastUsed: Date.now(),
          createdAt: Date.now(),
          activeConnections: 0,
        };

        this.poolCache.set(cacheKey, cached);
        return cached;
      } finally {
        // Always remove the creation promise when done (success or failure)
        this.poolCreationPromises.delete(cacheKey);
      }
    })();

    // Store the promise so concurrent requests can wait for it
    this.poolCreationPromises.set(cacheKey, creationPromise);

    return creationPromise;
  }

  /**
   * Gets a client with optional connection override for one-time execution.
   * Handles concurrent calls efficiently by:
   * - Reusing existing pools for the same server/database
   * - Preventing duplicate pool creation through promise caching
   * - Tracking active connections for resource management
   * - Enforcing global connection limits
   *
   * @param override - Optional connection override parameters
   * @returns Object containing the client, release function, and resolved connection info
   * @throws Error if no connection and no override, or if override server not found
   */
  public async getClientWithOverride(
    override?: ConnectionOverride
  ): Promise<OverrideClientResult> {
    // If no override, use current connection
    if (!override || (!override.server && !override.database && !override.schema)) {
      if (!this.currentPool) {
        throw new Error(
          "No database connection. Please switch to a server and database first, or provide server/database/schema override parameters."
        );
      }

      const { client, release } = await this.acquireMainPoolClient();
      const serverConfig = this.connectionState.currentServer
        ? this.serversConfig[this.connectionState.currentServer]
        : null;

      // Set search_path to current schema
      const schema = this.connectionState.currentSchema || DEFAULT_SCHEMA;
      try {
        await client.query(`SET search_path TO ${this.escapeIdentifier(schema)}`);
      } catch (error) {
        release();
        throw new Error(
          `Failed to set schema '${schema}': ${error instanceof Error ? error.message : String(error)}`
        );
      }

      return {
        client,
        release,
        server: this.connectionState.currentServer || "",
        database: this.connectionState.currentDatabase || "",
        schema,
        context: serverConfig?.context,
        isOverride: false,
      };
    }

    // Resolve server - use override or current
    const serverName = override.server || this.connectionState.currentServer;
    if (!serverName) {
      throw new Error(
        "No server specified and no current connection. Provide 'server' parameter or connect first."
      );
    }

    const serverConfig = this.getServerConfig(serverName);
    if (!serverConfig) {
      const availableServers = this.getServerNames();
      throw new Error(
        `Server '${serverName}' not found. Available servers: ${availableServers.join(", ") || "none configured"}`
      );
    }

    // Resolve database - use override, or if same server use current, or use server default
    let database: string;
    if (override.database) {
      database = override.database;
    } else if (serverName === this.connectionState.currentServer && this.connectionState.currentDatabase) {
      database = this.connectionState.currentDatabase;
    } else {
      database = serverConfig.defaultDatabase || DEFAULT_DATABASE;
    }

    // Validate database name for SQL injection prevention
    validateDatabaseName(database);

    // Resolve schema
    let schema: string;
    if (override.schema) {
      schema = override.schema;
    } else if (serverName === this.connectionState.currentServer && this.connectionState.currentSchema) {
      schema = this.connectionState.currentSchema;
    } else {
      schema = serverConfig.defaultSchema || DEFAULT_SCHEMA;
    }

    // Validate schema name
    validateSchemaName(schema);

    // Check if this is the same as current connection (can use main pool)
    const isSameAsMain =
      serverName === this.connectionState.currentServer &&
      database === this.connectionState.currentDatabase;

    if (isSameAsMain && this.currentPool) {
      // Use main pool but with potentially different schema
      const { client, release } = await this.acquireMainPoolClient();

      try {
        await client.query(`SET search_path TO ${this.escapeIdentifier(schema)}`);
      } catch (error) {
        release();
        throw new Error(
          `Failed to set schema '${schema}': ${error instanceof Error ? error.message : String(error)}`
        );
      }

      return {
        client,
        release,
        server: serverName,
        database,
        schema,
        context: serverConfig.context,
        isOverride: override.schema !== undefined && override.schema !== this.connectionState.currentSchema,
      };
    }

    // Get or create cached pool for the override connection
    // This handles concurrent requests efficiently
    const cached = await this.getOrCreateCachedPool(serverName, database, serverConfig);
    const { client, release } = await this.acquireCachedPoolClient(cached);

    try {
      await client.query(`SET search_path TO ${this.escapeIdentifier(schema)}`);
    } catch (error) {
      release();
      throw new Error(
        `Failed to set schema '${schema}': ${error instanceof Error ? error.message : String(error)}`
      );
    }

    return {
      client,
      release,
      server: serverName,
      database,
      schema,
      context: serverConfig.context,
      isOverride: true,
    };
  }

  /**
   * Executes a query with optional connection override.
   * Convenience method that handles client lifecycle automatically.
   *
   * @param sql - SQL query to execute
   * @param params - Query parameters
   * @param override - Optional connection override parameters
   * @returns Query result with connection info
   */
  public async queryWithOverride<T extends QueryResultRow = any>(
    sql: string,
    params?: any[],
    override?: ConnectionOverride
  ): Promise<QueryResult<T> & { connectionInfo: { server: string; database: string; schema: string } }> {
    if (!sql || typeof sql !== "string") {
      throw new Error("SQL query is required and must be a string");
    }

    // Check for read-only mode violations
    if (this.readOnlyMode) {
      const { isReadOnly, reason } = isReadOnlySql(sql);
      if (!isReadOnly) {
        throw new Error(`Read-only mode violation: ${reason}`);
      }
    }

    const { client, release, server, database, schema } =
      await this.getClientWithOverride(override);

    try {
      const result = await client.query<T>(sql, params);
      return {
        ...result,
        connectionInfo: { server, database, schema },
      };
    } finally {
      release();
    }
  }

  /**
   * Escapes a PostgreSQL identifier to prevent SQL injection
   */
  private escapeIdentifier(identifier: string): string {
    // Double any double quotes and wrap in double quotes
    return `"${identifier.replace(/"/g, '""')}"`;
  }

  /**
   * Closes all cached pools and cleans up resources
   */
  public async closeAllCachedPools(): Promise<void> {
    const closePromises: Promise<void>[] = [];

    for (const [key, cached] of this.poolCache.entries()) {
      closePromises.push(
        cached.pool.end().catch((err) => {
          console.error(`Error closing pool ${key}:`, err);
        })
      );
    }

    await Promise.all(closePromises);
    this.poolCache.clear();
  }

  /**
   * Gets comprehensive statistics about all connection pools (for monitoring/debugging)
   */
  public getConnectionStats(): {
    totalActiveConnections: number;
    maxTotalConnections: number;
    mainPool: {
      activeConnections: number;
      maxSize: number;
      isConnected: boolean;
    };
    cachedPools: {
      count: number;
      maxCount: number;
      totalActiveConnections: number;
      pools: Array<{
        key: string;
        serverName: string;
        database: string;
        activeConnections: number;
        maxSize: number;
        lastUsed: Date;
        createdAt: Date;
        idleTimeRemaining: number;
      }>;
    };
    pendingPoolCreations: number;
  } {
    const now = Date.now();
    const pools = Array.from(this.poolCache.entries()).map(([key, cached]) => ({
      key,
      serverName: cached.serverName,
      database: cached.database,
      activeConnections: cached.activeConnections,
      maxSize: MAX_POOL_SIZE_CACHED,
      lastUsed: new Date(cached.lastUsed),
      createdAt: new Date(cached.createdAt),
      idleTimeRemaining: Math.max(0, POOL_IDLE_TIMEOUT_MS - (now - cached.lastUsed)),
    }));

    const cachedPoolsTotalActive = Array.from(this.poolCache.values())
      .reduce((sum, cached) => sum + cached.activeConnections, 0);

    return {
      totalActiveConnections: this.totalActiveConnections,
      maxTotalConnections: MAX_TOTAL_CONNECTIONS,
      mainPool: {
        activeConnections: this.mainPoolActiveConnections,
        maxSize: MAX_POOL_SIZE_MAIN,
        isConnected: this.currentPool !== null,
      },
      cachedPools: {
        count: this.poolCache.size,
        maxCount: MAX_CACHED_POOLS,
        totalActiveConnections: cachedPoolsTotalActive,
        pools,
      },
      pendingPoolCreations: this.poolCreationPromises.size,
    };
  }

  /**
   * @deprecated Use getConnectionStats() instead
   * Gets statistics about cached pools (for monitoring/debugging)
   */
  public getCachedPoolStats(): {
    count: number;
    maxSize: number;
    pools: Array<{ key: string; serverName: string; database: string; lastUsed: Date; createdAt: Date }>;
  } {
    const pools = Array.from(this.poolCache.entries()).map(([key, cached]) => ({
      key,
      serverName: cached.serverName,
      database: cached.database,
      lastUsed: new Date(cached.lastUsed),
      createdAt: new Date(cached.createdAt),
    }));

    return {
      count: this.poolCache.size,
      maxSize: MAX_CACHED_POOLS,
      pools,
    };
  }

  public async close(): Promise<void> {
    // Stop cleanup interval
    if (this.poolCleanupInterval) {
      clearInterval(this.poolCleanupInterval);
      this.poolCleanupInterval = null;
    }

    // Close all cached pools
    await this.closeAllCachedPools();

    // Close main pool
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
      "timed out",
      "idle",
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
