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
  }[];
  currentServer: string | null;
  currentDatabase: string | null;
  currentSchema: string | null;
}

export async function listServersAndDbs(args: {
  filter?: string;
  includeSystemDbs?: boolean;
  fetchDatabases?: boolean;
}): Promise<ListServersResult> {
  const dbManager = getDbManager();
  const serversConfig = dbManager.getServersConfig();
  const currentState = dbManager.getCurrentState();

  let serverNames = Object.keys(serversConfig);

  // Apply filter if provided
  if (args.filter) {
    const filterLower = args.filter.toLowerCase();
    serverNames = serverNames.filter(name =>
      name.toLowerCase().includes(filterLower) ||
      serversConfig[name].host.toLowerCase().includes(filterLower)
    );
  }

  const servers: ListServersResult['servers'] = [];

  const defaultServerName = dbManager.getDefaultServerName();

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

    // Fetch databases only if requested and connected to this server
    if (args.fetchDatabases && isConnected) {
      try {
        let databases = await dbManager.listDatabases();

        // Filter system databases unless explicitly included
        if (!args.includeSystemDbs) {
          const systemDbs = ['template0', 'template1'];
          databases = databases.filter(db => !systemDbs.includes(db.name));
        }

        // Apply database filter
        if (args.filter) {
          const filterLower = args.filter.toLowerCase();
          databases = databases.filter(db =>
            db.name.toLowerCase().includes(filterLower)
          );
        }

        serverInfo.databases = databases;
      } catch (error) {
        // If we can't fetch databases, just skip
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
