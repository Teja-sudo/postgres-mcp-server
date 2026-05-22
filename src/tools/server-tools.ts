import { getDbManager } from '../db-manager.js';
import { DatabaseInfo, ConnectionInfo } from '../types.js';

// Simple server info without databases (host/port hidden for security)
interface ServerInfo {
  name: string;
  isConnected: boolean;
  isDefault: boolean;
  defaultDatabase?: string;
  defaultSchema?: string;
  /** AI context/guidance for this server */
  context?: string;
}

interface ListServersResult {
  servers: ServerInfo[];
  currentServer: string | null;
  currentDatabase: string | null;
  currentSchema: string | null;
}

// Database listing result
interface ListDatabasesResult {
  serverName: string;
  databases: DatabaseInfo[];
  currentDatabase: string | null;
}

/**
 * SQL query used by listDatabases in both the in-pool (connected server)
 * and temp-pool (different server) paths. Kept in one place so the
 * permission-denied / filter behavior stays consistent.
 *
 * - `$1::boolean` — when true, include templates (template0/template1)
 * - `$2::text`    — optional ILIKE filter, NULL means no filter
 *
 * Guards `pg_database_size()` behind `has_database_privilege(..., 'CONNECT')`
 * so listing never aborts on a database the role can't access. `size` is
 * NULL for inaccessible databases; `canConnect` indicates which.
 */
const LIST_DATABASES_SQL = `
  SELECT
    datname AS name,
    pg_catalog.pg_get_userbyid(datdba) AS owner,
    pg_catalog.pg_encoding_to_char(encoding) AS encoding,
    CASE WHEN pg_catalog.has_database_privilege(datname, 'CONNECT')
         THEN pg_catalog.pg_size_pretty(pg_catalog.pg_database_size(datname))
         ELSE NULL
    END AS size,
    pg_catalog.has_database_privilege(datname, 'CONNECT') AS "canConnect"
  FROM pg_catalog.pg_database
  WHERE ($1::boolean OR datistemplate = false)
    AND ($2::text IS NULL OR datname ILIKE '%' || $2 || '%')
  ORDER BY datname
`;

/**
 * Creates a temporary database connection for listing databases.
 */
async function createTempConnection(serverName: string, config: any): Promise<any> {
  const { Pool } = await import('pg');

  const sslConfig = getSslConfigForTemp(config.ssl);

  const pool = new Pool({
    host: config.host,
    port: parseInt(config.port || '5432', 10),
    user: config.username,
    password: config.password,
    database: config.defaultDatabase || 'postgres',
    max: 1,
    idleTimeoutMillis: 5000,
    connectionTimeoutMillis: 10000,
    ...(sslConfig && { ssl: sslConfig })
  });

  // Test connection
  try {
    const client = await pool.connect();
    client.release();
  } catch (error) {
    await pool.end();
    throw error;
  }

  return {
    async listDatabases(opts: { filter?: string; includeTemplates?: boolean }): Promise<DatabaseInfo[]> {
      const result = await pool.query(LIST_DATABASES_SQL, [
        opts.includeTemplates ?? false,
        opts.filter ?? null,
      ]);
      return result.rows;
    },
    async close(): Promise<void> {
      await pool.end();
    }
  };
}

/** SSL configuration object for pg Pool */
interface SslConfigObject {
  rejectUnauthorized?: boolean;
  ca?: string;
  [key: string]: unknown;
}

/**
 * Helper to get SSL config for temporary connections.
 * Returns undefined for disabled SSL, or an SSL config object for enabled SSL.
 */
function getSslConfigForTemp(ssl: unknown): SslConfigObject | undefined {
  if (ssl === undefined || ssl === false || ssl === 'disable') {
    return undefined;
  }
  if (ssl === true || ssl === 'require' || ssl === 'prefer' || ssl === 'allow') {
    return { rejectUnauthorized: false };
  }
  if (typeof ssl === 'object' && ssl !== null) {
    return ssl as SslConfigObject;
  }
  return undefined;
}

/**
 * Lists all configured PostgreSQL servers (without database details).
 * This is a lightweight operation that doesn't require database connections.
 */
export async function listServers(args: {
  filter?: string;
}): Promise<ListServersResult> {
  const dbManager = getDbManager();
  const serversConfig = dbManager.getServersConfig();
  const currentState = dbManager.getCurrentState();

  let serverNames = Object.keys(serversConfig);

  // Apply filter if provided (filter by server name only)
  if (args.filter) {
    const filterLower = args.filter.toLowerCase();
    serverNames = serverNames.filter(name =>
      name.toLowerCase().includes(filterLower)
    );
  }

  const servers: ServerInfo[] = [];
  const defaultServerName = dbManager.getDefaultServerName();

  for (const name of serverNames) {
    const config = serversConfig[name];
    const isConnected = currentState.currentServer === name;

    servers.push({
      name,
      isConnected,
      isDefault: config.isDefault === true || name === defaultServerName,
      defaultDatabase: config.defaultDatabase,
      defaultSchema: config.defaultSchema,
      context: config.context
    });
  }

  return {
    servers,
    currentServer: currentState.currentServer,
    currentDatabase: currentState.currentDatabase,
    currentSchema: currentState.currentSchema
  };
}

/**
 * Lists databases in a specific server.
 * If not connected to the specified server, creates a temporary connection.
 *
 * @param serverName - Required. The server name to list databases from.
 * @param filter - Optional. Filter databases by name.
 * @param includeSystemDbs - Optional. Include template0 and template1.
 * @param maxResults - Optional. Limit number of results (default 50, max 200).
 */
export async function listDatabases(args: {
  serverName: string;
  filter?: string;
  includeSystemDbs?: boolean;
  maxResults?: number;
}): Promise<ListDatabasesResult> {
  if (!args.serverName || typeof args.serverName !== 'string') {
    throw new Error('serverName is required. Use list_servers to see available servers.');
  }

  const dbManager = getDbManager();
  const serversConfig = dbManager.getServersConfig();
  const currentState = dbManager.getCurrentState();

  // Validate server exists
  if (!serversConfig[args.serverName]) {
    const availableServers = Object.keys(serversConfig).join(', ');
    throw new Error(`Server '${args.serverName}' not found. Available servers: ${availableServers}`);
  }

  // Audit-iteration-3 fix (group 1 P1-3, P1-4): clamp maxResults
  // to a sane positive range. Previously `|| 50` swallowed an
  // explicit `0` and negative values caused `slice(0, -100)` which
  // silently dropped trailing rows. We now use `??` so 0 is honored
  // (= "return zero rows") and Math.max enforces a non-negative
  // floor before the upper bound.
  const requested = args.maxResults ?? 50;
  const maxResults = Math.min(Math.max(requested, 0), 200);

  // Hotfix-3.0.3 (list_databases aborted on permission_denied): push
  // the ILIKE filter into the SQL so the per-row pg_database_size()
  // call only fires on the candidate set the caller asked for, and
  // guard the size lookup behind has_database_privilege so a single
  // inaccessible database doesn't abort the entire listing.
  // `includeTemplates` is forwarded into SQL too — the JS-side
  // filter that used to drop templates is no longer needed.
  const listOpts = {
    filter: args.filter,
    includeTemplates: args.includeSystemDbs === true,
  };

  let databases: DatabaseInfo[];

  // If connected to this server, use existing connection
  if (currentState.currentServer === args.serverName) {
    databases = await dbManager.listDatabases(listOpts);
  } else {
    // Create temporary connection to the server
    const config = serversConfig[args.serverName];
    const tempDbManager = await createTempConnection(args.serverName, config);

    try {
      databases = await tempDbManager.listDatabases(listOpts);
    } finally {
      await tempDbManager.close();
    }
  }

  // Limit results
  databases = databases.slice(0, maxResults);

  return {
    serverName: args.serverName,
    databases,
    currentDatabase: currentState.currentServer === args.serverName ? currentState.currentDatabase : null
  };
}

export async function switchServerDb(args: {
  server: string;
  database?: string;
  schema?: string;
}): Promise<{ success: boolean; message: string; currentServer: string; currentDatabase: string; currentSchema: string; context?: string }> {
  const dbManager = getDbManager();

  try {
    await dbManager.switchServer(args.server, args.database, args.schema);
    const connectionInfo = dbManager.getConnectionInfo();

    const dbPart = args.database ? `, database '${args.database}'` : '';
    const schemaPart = args.schema ? `, schema '${args.schema}'` : '';

    return {
      success: true,
      message: `Successfully connected to server '${args.server}'${dbPart}${schemaPart}`,
      currentServer: connectionInfo.server!,
      currentDatabase: connectionInfo.database!,
      currentSchema: connectionInfo.schema!,
      context: connectionInfo.context
    };
  } catch (error) {
    throw new Error(`Failed to switch: ${error}`, { cause: error });
  }
}

/**
 * Gets the current connection details including server, database, schema, and access mode.
 */
export async function getCurrentConnection(): Promise<ConnectionInfo> {
  const dbManager = getDbManager();
  return dbManager.getConnectionInfo();
}
