#!/usr/bin/env node

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";

import { resetDbManager } from "./db-manager.js";
import {
  listServers,
  listDatabases,
  switchServerDb,
  getCurrentConnection,
  listSchemas,
  listObjects,
  getObjectDetails,
  executeSql,
  executeSqlFile,
  explainQuery,
  getTopQueries,
  analyzeWorkloadIndexes,
  analyzeQueryIndexes,
  analyzeDbHealth,
  mutationPreview,
  batchExecute,
} from "./tools/index.js";
import { withConnectionRetry } from "./utils/index.js";

// Wrap tools that require active database connection with auto-retry logic
const listSchemasWithRetry = withConnectionRetry(listSchemas);
const listObjectsWithRetry = withConnectionRetry(listObjects);
const getObjectDetailsWithRetry = withConnectionRetry(getObjectDetails);
const executeSqlWithRetry = withConnectionRetry(executeSql);
const executeSqlFileWithRetry = withConnectionRetry(executeSqlFile);
const explainQueryWithRetry = withConnectionRetry(explainQuery);
const getTopQueriesWithRetry = withConnectionRetry(getTopQueries);
const analyzeWorkloadIndexesWithRetry = withConnectionRetry(analyzeWorkloadIndexes);
const analyzeQueryIndexesWithRetry = withConnectionRetry(analyzeQueryIndexes);
const analyzeDbHealthWithRetry = withConnectionRetry(async () => analyzeDbHealth());
const mutationPreviewWithRetry = withConnectionRetry(mutationPreview);
const batchExecuteWithRetry = withConnectionRetry(batchExecute);

// Create MCP server using the new high-level API
const server = new McpServer(
  {
    name: "postgres-mcp-server",
    version: "1.8.0",
  },
  {
    capabilities: {
      tools: {},
    },
  }
);

// Register tools with improved descriptions

server.registerTool(
  "list_servers",
  {
    description:
      "List all configured PostgreSQL servers. Call this FIRST to discover available server names before using list_databases or switch_server_db. Returns server names and connection status.",
    inputSchema: z.object({
      filter: z
        .string()
        .optional()
        .describe("Filter servers by name (case-insensitive partial match)"),
    }),
  },
  async (args) => {
    const result = await listServers(args);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.registerTool(
  "list_databases",
  {
    description:
      "List databases in a specific PostgreSQL server. REQUIRES serverName parameter - use list_servers first to get valid server names. Do NOT guess server names.",
    inputSchema: z.object({
      serverName: z
        .string()
        .describe("REQUIRED: Server name from list_servers. Do NOT use database names here."),
      filter: z
        .string()
        .optional()
        .describe("Filter databases by name (case-insensitive partial match)"),
      includeSystemDbs: z
        .boolean()
        .optional()
        .default(false)
        .describe("Include system databases (template0, template1)"),
      maxResults: z
        .number()
        .optional()
        .default(50)
        .describe("Maximum databases to return (default: 50, max: 200)"),
    }),
  },
  async (args) => {
    const result = await listDatabases(args);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.registerTool(
  "switch_server_db",
  {
    description:
      "Connect to a PostgreSQL server and database. MUST be called before executing queries. Use list_servers to find server names, list_databases to find database names.",
    inputSchema: z.object({
      server: z.string().describe("Server name from list_servers (NOT the host)"),
      database: z
        .string()
        .optional()
        .describe("Database name from list_databases (defaults to server's default or 'postgres')"),
      schema: z
        .string()
        .optional()
        .describe("Schema name (defaults to 'public')"),
    }),
  },
  async (args) => {
    const result = await switchServerDb(args);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.registerTool(
  "get_current_connection",
  {
    description:
      "Get current connection status. Returns server name, database, schema, and access mode (readonly/full). Call this to verify your connection before running queries.",
    inputSchema: z.object({}),
  },
  async () => {
    const result = await getCurrentConnection();
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.registerTool(
  "list_schemas",
  {
    description:
      "List all schemas in the current database. Requires active connection (use switch_server_db first).",
    inputSchema: z.object({
      includeSystemSchemas: z
        .boolean()
        .optional()
        .default(false)
        .describe("Include system schemas (pg_catalog, information_schema, etc.)"),
    }),
  },
  async (args) => {
    const result = await listSchemasWithRetry(args);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.registerTool(
  "list_objects",
  {
    description:
      "List tables, views, sequences, or extensions in a schema. Requires active connection.",
    inputSchema: z.object({
      schema: z.string().describe("Schema name (e.g., 'public')"),
      objectType: z
        .enum(["table", "view", "sequence", "extension", "all"])
        .optional()
        .default("all")
        .describe("Type of objects to list"),
      filter: z
        .string()
        .optional()
        .describe("Filter objects by name (case-insensitive partial match)"),
    }),
  },
  async (args) => {
    const result = await listObjectsWithRetry(args);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.registerTool(
  "get_object_details",
  {
    description:
      "Get detailed info about a table/view/sequence: columns, data types, constraints, indexes, size, row count. Use this to understand table structure before writing queries.",
    inputSchema: z.object({
      schema: z.string().describe("Schema name containing the object"),
      objectName: z.string().describe("Name of the table, view, or sequence"),
      objectType: z
        .enum(["table", "view", "sequence"])
        .optional()
        .describe("Object type (auto-detected if not specified)"),
    }),
  },
  async (args) => {
    const result = await getObjectDetailsWithRetry(args);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.registerTool(
  "execute_sql",
  {
    description:
      "Execute SQL queries. Supports SELECT, INSERT, UPDATE, DELETE (if not in readonly mode). Use $1, $2 placeholders with params array to prevent SQL injection. Returns rows, execution time, and pagination info. Use includeSchemaHint for table context.",
    inputSchema: z.object({
      sql: z
        .string()
        .describe("SQL statement. Use $1, $2, etc. for parameterized queries."),
      params: z
        .array(z.any())
        .optional()
        .describe("Parameters for $1, $2, etc. placeholders (e.g., [123, 'value'])"),
      maxRows: z
        .number()
        .optional()
        .default(1000)
        .describe("Max rows to return (default: 1000, max: 100000)"),
      offset: z
        .number()
        .optional()
        .default(0)
        .describe("Skip rows for pagination"),
      allowLargeScript: z
        .boolean()
        .optional()
        .default(false)
        .describe("Bypass 100KB SQL limit for deployment scripts"),
      includeSchemaHint: z
        .boolean()
        .optional()
        .default(false)
        .describe("Include schema info (columns, PKs, FKs) for tables in the query. Helps understand table structure."),
    }),
  },
  async (args) => {
    const result = await executeSqlWithRetry(args);
    // Special handling for large output
    if (result.outputFile) {
      return {
        content: [
          {
            type: "text",
            text: JSON.stringify(
              {
                message: `Output too large (${result.rowCount} rows). Results written to file.`,
                outputFile: result.outputFile,
                rowCount: result.rowCount,
                fields: result.fields,
                hint: "Use offset/maxRows to paginate, or add WHERE clauses to reduce results.",
              },
              null,
              2
            ),
          },
        ],
      };
    }
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.registerTool(
  "execute_sql_file",
  {
    description:
      "Execute a .sql file from the filesystem. Useful for running migration scripts, schema changes, or data imports. Supports transaction mode for atomic execution. Max file size: 50MB. Returns detailed error info when stopOnError=false.",
    inputSchema: z.object({
      filePath: z
        .string()
        .describe("Absolute or relative path to the .sql file to execute"),
      useTransaction: z
        .boolean()
        .optional()
        .default(true)
        .describe("Wrap execution in a transaction (default: true). If any statement fails, all changes are rolled back."),
      stopOnError: z
        .boolean()
        .optional()
        .default(true)
        .describe("Stop execution on first error (default: true). If false, continues with remaining statements."),
    }),
  },
  async (args) => {
    const result = await executeSqlFileWithRetry(args);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.registerTool(
  "mutation_preview",
  {
    description:
      "Preview the effect of INSERT/UPDATE/DELETE without executing. Shows estimated rows affected and sample of rows that would be modified. Use this before running destructive queries to verify the impact.",
    inputSchema: z.object({
      sql: z
        .string()
        .describe("The INSERT, UPDATE, or DELETE statement to preview"),
      sampleSize: z
        .number()
        .optional()
        .default(5)
        .describe("Number of sample rows to show (default: 5, max: 20)"),
    }),
  },
  async (args) => {
    const result = await mutationPreviewWithRetry(args);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.registerTool(
  "batch_execute",
  {
    description:
      "Execute multiple SQL queries in parallel. Returns all results keyed by query name. Efficient for fetching multiple independent pieces of data in one call.",
    inputSchema: z.object({
      queries: z
        .array(
          z.object({
            name: z.string().describe("Unique name for this query (used as key in results)"),
            sql: z.string().describe("SQL query to execute"),
            params: z.array(z.any()).optional().describe("Query parameters"),
          })
        )
        .describe("Array of queries to execute (max 20)"),
      stopOnError: z
        .boolean()
        .optional()
        .default(false)
        .describe("Stop on first error (default: false, continues with all queries)"),
    }),
  },
  async (args) => {
    const result = await batchExecuteWithRetry(args);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.registerTool(
  "explain_query",
  {
    description:
      "Show PostgreSQL's execution plan for a query. Use this to understand query performance and identify missing indexes. analyze=true runs the query to get actual timings (SELECT only).",
    inputSchema: z.object({
      sql: z.string().describe("SQL query to explain"),
      analyze: z
        .boolean()
        .optional()
        .default(false)
        .describe("Execute query for real timing (SELECT only, blocked for writes)"),
      buffers: z
        .boolean()
        .optional()
        .default(false)
        .describe("Include buffer/cache statistics"),
      format: z
        .enum(["text", "json", "yaml", "xml"])
        .optional()
        .default("json")
        .describe("Output format"),
      hypotheticalIndexes: z
        .array(
          z.object({
            table: z.string().describe("Table name"),
            columns: z.array(z.string()).describe("Columns for the index"),
            indexType: z.string().optional().default("btree").describe("Index type"),
          })
        )
        .optional()
        .describe("Test hypothetical indexes (requires hypopg extension)"),
    }),
  },
  async (args) => {
    const result = await explainQueryWithRetry(args);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.registerTool(
  "get_top_queries",
  {
    description:
      "Find slowest queries from pg_stat_statements. Requires pg_stat_statements extension enabled. Use this to identify performance bottlenecks.",
    inputSchema: z.object({
      limit: z
        .number()
        .optional()
        .default(10)
        .describe("Number of queries to return (1-100)"),
      orderBy: z
        .enum(["total_time", "mean_time", "calls"])
        .optional()
        .default("total_time")
        .describe("Sort by total time, average time, or call count"),
      minCalls: z
        .number()
        .optional()
        .default(1)
        .describe("Minimum call count to include"),
    }),
  },
  async (args) => {
    const result = await getTopQueriesWithRetry(args);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.registerTool(
  "analyze_workload_indexes",
  {
    description:
      "Analyze database workload and recommend indexes. Uses pg_stat_statements to find slow queries and suggests indexes to improve them.",
    inputSchema: z.object({
      topQueriesCount: z
        .number()
        .optional()
        .default(20)
        .describe("Number of top queries to analyze (1-50)"),
      includeHypothetical: z
        .boolean()
        .optional()
        .default(false)
        .describe("Test recommendations with hypothetical indexes (requires hypopg)"),
    }),
  },
  async (args) => {
    const result = await analyzeWorkloadIndexesWithRetry(args);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.registerTool(
  "analyze_query_indexes",
  {
    description:
      "Recommend indexes for specific SQL queries. Provide up to 10 SELECT queries and get index recommendations.",
    inputSchema: z.object({
      queries: z
        .array(z.string())
        .max(10)
        .describe("SQL SELECT queries to analyze (max 10)"),
    }),
  },
  async (args) => {
    const result = await analyzeQueryIndexesWithRetry(args);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.registerTool(
  "analyze_db_health",
  {
    description:
      "Run comprehensive database health checks: cache hit rates, connection usage, index health (invalid/unused/duplicate), vacuum status, sequence limits, unvalidated constraints. Returns issues with severity levels.",
    inputSchema: z.object({}),
  },
  async () => {
    const result = await analyzeDbHealthWithRetry();
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

// Graceful shutdown handling
async function shutdown(): Promise<void> {
  console.error("Shutting down PostgreSQL MCP Server...");
  try {
    resetDbManager();
    await server.close();
  } catch (error) {
    console.error("Error during shutdown:", error);
  }
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
process.on("SIGHUP", shutdown);

// Handle uncaught errors
process.on("uncaughtException", (error) => {
  console.error("Uncaught exception:", error);
  shutdown();
});

process.on("unhandledRejection", (reason, promise) => {
  console.error("Unhandled rejection at:", promise, "reason:", reason);
});

// Start server
async function main(): Promise<void> {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("PostgreSQL MCP Server started");
}

main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});
