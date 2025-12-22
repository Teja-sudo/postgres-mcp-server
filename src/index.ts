#!/usr/bin/env node

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
  Tool,
} from '@modelcontextprotocol/sdk/types.js';

import { getDbManager, resetDbManager } from './db-manager.js';
import {
  listServersAndDbs,
  switchServerDb,
  listSchemas,
  listObjects,
  getObjectDetails,
  executeSql,
  explainQuery,
  getTopQueries,
  analyzeWorkloadIndexes,
  analyzeQueryIndexes,
  analyzeDbHealth
} from './tools/index.js';

// Tool definitions
const tools: Tool[] = [
  {
    name: 'list_servers_and_dbs',
    description: 'Lists all configured PostgreSQL servers and their databases. Can filter servers and databases by name. Use fetchDatabases=true to list databases (requires connecting to the server first).',
    inputSchema: {
      type: 'object',
      properties: {
        filter: {
          type: 'string',
          description: 'Filter servers and databases by name (case-insensitive partial match)'
        },
        includeSystemDbs: {
          type: 'boolean',
          description: 'Include system databases (template0, template1)',
          default: false
        },
        fetchDatabases: {
          type: 'boolean',
          description: 'Fetch list of databases from connected server',
          default: false
        }
      }
    }
  },
  {
    name: 'switch_server_db',
    description: 'Switch to a different PostgreSQL server and optionally a specific database. Must be called before using other database tools.',
    inputSchema: {
      type: 'object',
      properties: {
        server: {
          type: 'string',
          description: 'Name of the server to connect to (from POSTGRES_SERVERS config)'
        },
        database: {
          type: 'string',
          description: 'Name of the database to connect to (defaults to "postgres")'
        }
      },
      required: ['server']
    }
  },
  {
    name: 'list_schemas',
    description: 'Lists all database schemas available in the current PostgreSQL database.',
    inputSchema: {
      type: 'object',
      properties: {
        includeSystemSchemas: {
          type: 'boolean',
          description: 'Include system schemas (pg_catalog, information_schema, etc.)',
          default: false
        }
      }
    }
  },
  {
    name: 'list_objects',
    description: 'Lists database objects (tables, views, sequences, extensions) within a specified schema.',
    inputSchema: {
      type: 'object',
      properties: {
        schema: {
          type: 'string',
          description: 'Schema name to list objects from'
        },
        objectType: {
          type: 'string',
          enum: ['table', 'view', 'sequence', 'extension', 'all'],
          description: 'Type of objects to list',
          default: 'all'
        },
        filter: {
          type: 'string',
          description: 'Filter objects by name (case-insensitive partial match)'
        }
      },
      required: ['schema']
    }
  },
  {
    name: 'get_object_details',
    description: 'Provides detailed information about a specific database object including columns, constraints, indexes, size, and row count.',
    inputSchema: {
      type: 'object',
      properties: {
        schema: {
          type: 'string',
          description: 'Schema name containing the object'
        },
        objectName: {
          type: 'string',
          description: 'Name of the object to get details for'
        },
        objectType: {
          type: 'string',
          enum: ['table', 'view', 'sequence'],
          description: 'Type of the object'
        }
      },
      required: ['schema', 'objectName']
    }
  },
  {
    name: 'execute_sql',
    description: 'Executes SQL statements on the database. Read-only mode prevents write operations (INSERT, UPDATE, DELETE, etc.). Large outputs are written to a temp file and the file path is returned.',
    inputSchema: {
      type: 'object',
      properties: {
        sql: {
          type: 'string',
          description: 'SQL statement to execute'
        },
        maxRows: {
          type: 'number',
          description: 'Maximum rows to return directly (default: 1000). Larger results are written to file.',
          default: 1000
        }
      },
      required: ['sql']
    }
  },
  {
    name: 'explain_query',
    description: 'Gets the execution plan for a SQL query, showing how PostgreSQL will process it. Can simulate hypothetical indexes if hypopg extension is installed.',
    inputSchema: {
      type: 'object',
      properties: {
        sql: {
          type: 'string',
          description: 'SQL query to explain'
        },
        analyze: {
          type: 'boolean',
          description: 'Actually execute the query to get real timing (default: false). Only allowed for SELECT queries.',
          default: false
        },
        buffers: {
          type: 'boolean',
          description: 'Include buffer usage statistics',
          default: false
        },
        format: {
          type: 'string',
          enum: ['text', 'json', 'yaml', 'xml'],
          description: 'Output format for the plan',
          default: 'json'
        },
        hypotheticalIndexes: {
          type: 'array',
          description: 'Hypothetical indexes to simulate (requires hypopg extension)',
          items: {
            type: 'object',
            properties: {
              table: {
                type: 'string',
                description: 'Table name for the hypothetical index'
              },
              columns: {
                type: 'array',
                items: { type: 'string' },
                description: 'Columns to include in the index'
              },
              indexType: {
                type: 'string',
                description: 'Index type (btree, hash, gist, etc.)',
                default: 'btree'
              }
            },
            required: ['table', 'columns']
          }
        }
      },
      required: ['sql']
    }
  },
  {
    name: 'get_top_queries',
    description: 'Reports the slowest SQL queries based on total execution time using pg_stat_statements data. Requires pg_stat_statements extension.',
    inputSchema: {
      type: 'object',
      properties: {
        limit: {
          type: 'number',
          description: 'Number of queries to return (1-100)',
          default: 10
        },
        orderBy: {
          type: 'string',
          enum: ['total_time', 'mean_time', 'calls'],
          description: 'How to order the results',
          default: 'total_time'
        },
        minCalls: {
          type: 'number',
          description: 'Minimum number of calls to include a query',
          default: 1
        }
      }
    }
  },
  {
    name: 'analyze_workload_indexes',
    description: 'Analyzes the database workload (using pg_stat_statements) to identify resource-intensive queries and recommends optimal indexes for them.',
    inputSchema: {
      type: 'object',
      properties: {
        topQueriesCount: {
          type: 'number',
          description: 'Number of top queries to analyze (1-50)',
          default: 20
        },
        includeHypothetical: {
          type: 'boolean',
          description: 'Include hypothetical index analysis (requires hypopg)',
          default: false
        }
      }
    }
  },
  {
    name: 'analyze_query_indexes',
    description: 'Analyzes specific SQL queries (up to 10) and recommends optimal indexes for them.',
    inputSchema: {
      type: 'object',
      properties: {
        queries: {
          type: 'array',
          items: { type: 'string' },
          description: 'List of SQL queries to analyze (max 10)',
          maxItems: 10
        }
      },
      required: ['queries']
    }
  },
  {
    name: 'analyze_db_health',
    description: 'Performs comprehensive database health checks including: buffer cache hit rates, connection health, constraint validation, index health (duplicate/unused/invalid), sequence limits, and vacuum health.',
    inputSchema: {
      type: 'object',
      properties: {}
    }
  }
];

// Create MCP server
const server = new Server(
  {
    name: 'postgres-mcp-server',
    version: '1.0.0',
  },
  {
    capabilities: {
      tools: {},
    },
  }
);

// Handle list_tools request
server.setRequestHandler(ListToolsRequestSchema, async () => {
  return { tools };
});

// Handle tool calls
server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;

  try {
    let result: any;

    switch (name) {
      case 'list_servers_and_dbs':
        result = await listServersAndDbs(args as any);
        break;

      case 'switch_server_db':
        result = await switchServerDb(args as any);
        break;

      case 'list_schemas':
        result = await listSchemas(args as any);
        break;

      case 'list_objects':
        result = await listObjects(args as any);
        break;

      case 'get_object_details':
        result = await getObjectDetails(args as any);
        break;

      case 'execute_sql':
        result = await executeSql(args as any);
        // Special handling for large output
        if (result.outputFile) {
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify({
                  message: `Output too large (${result.rowCount} rows). Results written to file.`,
                  outputFile: result.outputFile,
                  rowCount: result.rowCount,
                  fields: result.fields,
                  hint: 'Read the file optimally using offset/limit or run the query with filters to reduce output.'
                }, null, 2)
              }
            ]
          };
        }
        break;

      case 'explain_query':
        result = await explainQuery(args as any);
        break;

      case 'get_top_queries':
        result = await getTopQueries(args as any);
        break;

      case 'analyze_workload_indexes':
        result = await analyzeWorkloadIndexes(args as any);
        break;

      case 'analyze_query_indexes':
        result = await analyzeQueryIndexes(args as any);
        break;

      case 'analyze_db_health':
        result = await analyzeDbHealth();
        break;

      default:
        throw new Error(`Unknown tool: ${name}`);
    }

    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify(result, null, 2)
        }
      ]
    };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify({ error: errorMessage }, null, 2)
        }
      ],
      isError: true
    };
  }
});

// Graceful shutdown handling
async function shutdown(): Promise<void> {
  console.error('Shutting down PostgreSQL MCP Server...');
  try {
    resetDbManager();
  } catch (error) {
    console.error('Error during shutdown:', error);
  }
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
process.on('SIGHUP', shutdown);

// Handle uncaught errors
process.on('uncaughtException', (error) => {
  console.error('Uncaught exception:', error);
  shutdown();
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled rejection at:', promise, 'reason:', reason);
});

// Start server
async function main(): Promise<void> {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error('PostgreSQL MCP Server started');
}

main().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});
