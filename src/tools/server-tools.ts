import { getDbManager } from '../db-manager.js';
import { DatabaseInfo, ConnectionInfo } from '../types.js';

interface ListServersResult {
  servers: {
    name: string;
    host: string;
    port: string;
    isConnected: boolean;
    isDefault: boolean;
    defaultDatabase?: string;
    defaultSchema?: string;
    databases?: DatabaseInfo[];
    databaseError?: string;
  }[];
  currentServer: string | null;
  currentDatabase: string | null;
  currentSchema: string | null;
}

export async function listServersAndDbs(args: {
  serverFilter?: string;
  databaseFilter?: string;
  includeSystemDbs?: boolean;
  fetchDatabases?: boolean;
  searchAllServers?: boolean;
}): Promise<ListServersResult> {
  const dbManager = getDbManager();
  const serversConfig = dbManager.getServersConfig();
  const currentState = dbManager.getCurrentState();

  let serverNames = Object.keys(serversConfig);

  // Apply server filter if provided
  if (args.serverFilter) {
    const filterLower = args.serverFilter.toLowerCase();
    serverNames = serverNames.filter(name =>
      name.toLowerCase().includes(filterLower) ||
      serversConfig[name].host.toLowerCase().includes(filterLower)
    );
  }

  const servers: ListServersResult['servers'] = [];
  const defaultServerName = dbManager.getDefaultServerName();
  const systemDbs = ['template0', 'template1'];

  for (const name of serverNames) {
    const config = serversConfig[name];
    const isConnected = currentState.currentServer === name;

    const serverInfo: ListServersResult['servers'][0] = {
      name,
      host: config.host,
      port: config.port || '5432',
      isConnected,
      isDefault: config.isDefault === true || name === defaultServerName,
      defaultDatabase: config.defaultDatabase,
      defaultSchema: config.defaultSchema
    };

    // Fetch databases if requested
    if (args.fetchDatabases) {
      // Fetch from current connection
      if (isConnected) {
        try {
          let databases = await dbManager.listDatabases();

          // Filter system databases unless explicitly included
          if (!args.includeSystemDbs) {
            databases = databases.filter(db => !systemDbs.includes(db.name));
          }

          // Apply database filter
          if (args.databaseFilter) {
            const filterLower = args.databaseFilter.toLowerCase();
            databases = databases.filter(db =>
              db.name.toLowerCase().includes(filterLower)
            );
          }

          serverInfo.databases = databases;
        } catch (error) {
          serverInfo.databaseError = error instanceof Error ? error.message : 'Failed to fetch databases';
        }
      }
      // Fetch from non-connected servers if searchAllServers is true
      else if (args.searchAllServers) {
        try {
          // Temporarily connect to this server to list databases
          const tempDbManager = await createTempConnection(name, config);
          if (tempDbManager) {
            try {
              let databases = await tempDbManager.listDatabases();

              // Filter system databases unless explicitly included
              if (!args.includeSystemDbs) {
                databases = databases.filter((db: DatabaseInfo) => !systemDbs.includes(db.name));
              }

              // Apply database filter
              if (args.databaseFilter) {
                const filterLower = args.databaseFilter.toLowerCase();
                databases = databases.filter((db: DatabaseInfo) =>
                  db.name.toLowerCase().includes(filterLower)
                );
              }

              serverInfo.databases = databases;
            } finally {
              await tempDbManager.close();
            }
          }
        } catch (error) {
          serverInfo.databaseError = error instanceof Error ? error.message : 'Failed to connect';
        }
      }
    }

    servers.push(serverInfo);
  }

  return {
    servers,
    currentServer: currentState.currentServer,
    currentDatabase: currentState.currentDatabase,
    currentSchema: currentState.currentSchema
  };
}

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
    async listDatabases(): Promise<DatabaseInfo[]> {
      const result = await pool.query(`
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
    },
    async close(): Promise<void> {
      await pool.end();
    }
  };
}

/**
 * Helper to get SSL config for temporary connections.
 */
function getSslConfigForTemp(ssl: any): boolean | object | undefined {
  if (ssl === undefined || ssl === false || ssl === 'disable') {
    return undefined;
  }
  if (ssl === true || ssl === 'require' || ssl === 'prefer' || ssl === 'allow') {
    return { rejectUnauthorized: false };
  }
  if (typeof ssl === 'object') {
    return ssl;
  }
  return undefined;
}

export async function switchServerDb(args: {
  server: string;
  database?: string;
  schema?: string;
}): Promise<{ success: boolean; message: string; currentServer: string; currentDatabase: string; currentSchema: string }> {
  const dbManager = getDbManager();

  try {
    await dbManager.switchServer(args.server, args.database, args.schema);
    const state = dbManager.getCurrentState();

    return {
      success: true,
      message: `Successfully connected to server '${args.server}'${args.database ? `, database '${args.database}'` : ''}${args.schema ? `, schema '${args.schema}'` : ''}`,
      currentServer: state.currentServer!,
      currentDatabase: state.currentDatabase!,
      currentSchema: state.currentSchema!
    };
  } catch (error) {
    throw new Error(`Failed to switch: ${error}`);
  }
}

/**
 * Gets the current connection details including server, database, schema, and access mode.
 */
export async function getCurrentConnection(): Promise<ConnectionInfo> {
  const dbManager = getDbManager();
  return dbManager.getConnectionInfo();
}
