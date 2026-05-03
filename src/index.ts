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
  previewSqlFile,
  dryRunSqlFile,
  explainQuery,
  getTopQueries,
  analyzeWorkloadIndexes,
  analyzeQueryIndexes,
  analyzeDbHealth,
  mutationPreview,
  mutationDryRun,
  batchExecute,
  beginTransaction,
  commitTransaction,
  rollbackTransaction,
  getConnectionContext,
  getTransactionInfo,
  listActiveTransactions,
  exportToSqlFile,
  transferObjects,
  describeTable,
  findDependents,
  schemaDiff,
  lockCheck,
  detectMigrationState,
  safeAlterTable,
  columnProfile,
  generateSeedData,
  findBlockingQueries,
  killQuery,
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
const mutationDryRunWithRetry = withConnectionRetry(mutationDryRun);
const dryRunSqlFileWithRetry = withConnectionRetry(dryRunSqlFile);
const batchExecuteWithRetry = withConnectionRetry(batchExecute);
const beginTransactionWithRetry = withConnectionRetry(beginTransaction);
const commitTransactionWithRetry = withConnectionRetry(commitTransaction);
const rollbackTransactionWithRetry = withConnectionRetry(rollbackTransaction);
const getTransactionInfoWithRetry = withConnectionRetry(getTransactionInfo);
const listActiveTransactionsWithRetry = withConnectionRetry(listActiveTransactions);
const exportToSqlFileWithRetry = withConnectionRetry(exportToSqlFile);
const transferObjectsWithRetry = withConnectionRetry(transferObjects);
const describeTableWithRetry = withConnectionRetry(describeTable);
const findDependentsWithRetry = withConnectionRetry(findDependents);
const schemaDiffWithRetry = withConnectionRetry(schemaDiff);
const lockCheckWithRetry = withConnectionRetry(lockCheck);
const detectMigrationStateWithRetry = withConnectionRetry(detectMigrationState);
// safe_alter_table doesn't need a connection (pure logic)
const columnProfileWithRetry = withConnectionRetry(columnProfile);
const generateSeedDataWithRetry = withConnectionRetry(generateSeedData);
const findBlockingQueriesWithRetry = withConnectionRetry(findBlockingQueries);
const killQueryWithRetry = withConnectionRetry(killQuery);

/**
 * Helper to add connection context to any result
 */
function withContext<T>(result: T): T & { connection: ReturnType<typeof getConnectionContext> } {
  return {
    ...result,
    connection: getConnectionContext()
  };
}

// Create MCP server using the new high-level API
const server = new McpServer(
  {
    name: "postgres-mcp-server",
    version: "2.0.0",
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
      "List all schemas in the current database. Requires active connection (use switch_server_db first). Optionally use server/database/schema params for one-time execution on a different server without changing the main connection.",
    inputSchema: z.object({
      includeSystemSchemas: z
        .boolean()
        .optional()
        .default(false)
        .describe("Include system schemas (pg_catalog, information_schema, etc.)"),
      // Connection override parameters
      server: z
        .string()
        .optional()
        .describe("One-time server override. Execute on this server without changing main connection."),
      database: z
        .string()
        .optional()
        .describe("One-time database override. Uses this database for execution without changing main connection."),
      schema: z
        .string()
        .optional()
        .describe("One-time schema override. Sets search_path for this execution only."),
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
      "List tables, views, sequences, or extensions in a schema. Requires active connection. Optionally use server/database/targetSchema params for one-time execution on a different server.",
    inputSchema: z.object({
      schema: z.string().describe("Schema name to list objects from (e.g., 'public')"),
      objectType: z
        .enum(["table", "view", "sequence", "extension", "all"])
        .optional()
        .default("all")
        .describe("Type of objects to list"),
      filter: z
        .string()
        .optional()
        .describe("Filter objects by name (case-insensitive partial match)"),
      // Connection override parameters
      server: z
        .string()
        .optional()
        .describe("One-time server override. Execute on this server without changing main connection."),
      database: z
        .string()
        .optional()
        .describe("One-time database override. Uses this database for execution."),
      targetSchema: z
        .string()
        .optional()
        .describe("One-time schema override. Sets search_path for this execution only."),
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
      "Get detailed info about a table/view/sequence: columns, data types, constraints, indexes, size, row count. Use this to understand table structure before writing queries. Optionally use server/database/targetSchema params for one-time execution on a different server.",
    inputSchema: z.object({
      schema: z.string().describe("Schema name containing the object"),
      objectName: z.string().describe("Name of the table, view, or sequence"),
      objectType: z
        .enum(["table", "view", "sequence"])
        .optional()
        .describe("Object type (auto-detected if not specified)"),
      // Connection override parameters
      server: z
        .string()
        .optional()
        .describe("One-time server override. Execute on this server without changing main connection."),
      database: z
        .string()
        .optional()
        .describe("One-time database override. Uses this database for execution."),
      targetSchema: z
        .string()
        .optional()
        .describe("One-time schema override. Sets search_path for this execution only."),
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
      "Execute SQL queries. Supports SELECT, INSERT, UPDATE, DELETE (if not in readonly mode). Use $1, $2 placeholders with params array to prevent SQL injection. Use allowMultipleStatements to run multiple statements separated by semicolons. Use transactionId to run within a transaction. Optionally use server/database/schema params for one-time execution on a different server without changing the main connection.",
    inputSchema: z.object({
      sql: z
        .string()
        .describe("SQL statement(s). Use $1, $2, etc. for parameterized queries."),
      params: z
        .array(z.any())
        .optional()
        .describe("Parameters for $1, $2, etc. placeholders (e.g., [123, 'value']). Not supported with allowMultipleStatements."),
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
        .describe("Include schema info (columns, PKs, FKs) for tables in the query."),
      allowMultipleStatements: z
        .boolean()
        .optional()
        .default(false)
        .describe("Allow multiple SQL statements separated by semicolons. Returns results for each statement."),
      transactionId: z
        .string()
        .optional()
        .describe("Execute within an active transaction. Get this from begin_transaction."),
      maxEstimatedRows: z
        .number()
        .optional()
        .describe(
          "SP-7 query budget: refuse to run if the planner estimates more than this many rows. " +
          "Pre-EXPLAIN check on read-only queries only. Useful as a backstop for AI-generated queries."
        ),
      maxEstimatedCost: z
        .number()
        .optional()
        .describe(
          "SP-7 query budget: refuse to run if the planner estimates total cost above this. " +
          "Read-only queries only."
        ),
      // Connection override parameters for one-time execution
      server: z
        .string()
        .optional()
        .describe("One-time server override. Execute on this server without changing main connection. Cannot be used with transactionId."),
      database: z
        .string()
        .optional()
        .describe("One-time database override. Uses this database for execution without changing main connection."),
      schema: z
        .string()
        .optional()
        .describe("One-time schema override. Sets search_path for this execution only."),
    }),
  },
  async (args) => {
    const result = await executeSqlWithRetry(args);
    // Special handling for large output (single statement mode)
    if ('outputFile' in result && result.outputFile) {
      return {
        content: [
          {
            type: "text",
            text: JSON.stringify(
              withContext({
                message: `Output too large (${result.rowCount} rows). Results written to file.`,
                outputFile: result.outputFile,
                rowCount: result.rowCount,
                fields: result.fields,
                hint: "Use offset/maxRows to paginate, or add WHERE clauses to reduce results.",
              }),
              null,
              2
            ),
          },
        ],
      };
    }
    return { content: [{ type: "text", text: JSON.stringify(withContext(result), null, 2) }] };
  }
);

server.registerTool(
  "execute_sql_file",
  {
    description:
      "Execute a .sql file from the filesystem. Useful for running migration scripts, schema changes, or data imports. Supports transaction mode for atomic execution. Max file size: 50MB. Use validateOnly=true to preview without executing. Use stripPatterns to remove delimiters like '/' (Liquibase) or 'GO' (SQL Server). Optionally use server/database/schema params for one-time execution on a different server.",
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
      stripPatterns: z
        .array(z.string())
        .optional()
        .describe("Patterns to strip from SQL before execution. E.g., ['/'] for Liquibase, ['GO'] for SQL Server. By default, patterns are matched as literal strings on their own line."),
      stripAsRegex: z
        .boolean()
        .optional()
        .default(false)
        .describe("If true, stripPatterns are treated as regex patterns (default: false). Use for complex patterns like '^\\\\s*/\\\\s*$'."),
      validateOnly: z
        .boolean()
        .optional()
        .default(false)
        .describe("If true, parse and preview the file without executing (default: false). Returns statement count and types."),
      // Connection override parameters
      server: z
        .string()
        .optional()
        .describe("One-time server override. Execute on this server without changing main connection."),
      database: z
        .string()
        .optional()
        .describe("One-time database override. Uses this database for execution."),
      schema: z
        .string()
        .optional()
        .describe("One-time schema override. Sets search_path for this execution only."),
    }),
  },
  async (args) => {
    const result = await executeSqlFileWithRetry(args);
    return { content: [{ type: "text", text: JSON.stringify(withContext(result), null, 2) }] };
  }
);

server.registerTool(
  "preview_sql_file",
  {
    description:
      "Preview a SQL file without executing it. Shows statement count, types breakdown, and warnings for potentially dangerous operations (DROP, TRUNCATE, DELETE/UPDATE without WHERE). Similar to mutation_preview but for SQL files. Use this before execute_sql_file to understand what a migration will do.",
    inputSchema: z.object({
      filePath: z
        .string()
        .describe("Absolute or relative path to the .sql file to preview"),
      stripPatterns: z
        .array(z.string())
        .optional()
        .describe("Patterns to strip from SQL before parsing. E.g., ['/'] for Liquibase, ['GO'] for SQL Server."),
      stripAsRegex: z
        .boolean()
        .optional()
        .default(false)
        .describe("If true, stripPatterns are treated as regex patterns (default: false)."),
      maxStatements: z
        .number()
        .optional()
        .default(20)
        .describe("Maximum number of statements to show in preview (default: 20, max: 100)."),
    }),
  },
  async (args) => {
    const result = await previewSqlFile(args);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.registerTool(
  "mutation_preview",
  {
    description:
      "Preview the effect of INSERT/UPDATE/DELETE without executing. Shows estimated rows affected and sample of rows that would be modified. Use this before running destructive queries to verify the impact. Optionally use server/database/schema params for one-time execution on a different server.",
    inputSchema: z.object({
      sql: z
        .string()
        .describe("The INSERT, UPDATE, or DELETE statement to preview"),
      sampleSize: z
        .number()
        .optional()
        .default(5)
        .describe("Number of sample rows to show (default: 5, max: 20)"),
      // Connection override parameters
      server: z
        .string()
        .optional()
        .describe("One-time server override. Execute on this server without changing main connection."),
      database: z
        .string()
        .optional()
        .describe("One-time database override. Uses this database for execution."),
      schema: z
        .string()
        .optional()
        .describe("One-time schema override. Sets search_path for this execution only."),
    }),
  },
  async (args) => {
    const result = await mutationPreviewWithRetry(args);
    return { content: [{ type: "text", text: JSON.stringify(withContext(result), null, 2) }] };
  }
);

server.registerTool(
  "mutation_dry_run",
  {
    description:
      "Execute INSERT/UPDATE/DELETE in dry-run mode - actually runs the SQL within a transaction, captures REAL results (exact row counts, actual errors, before/after data), then ROLLBACK so nothing persists. More accurate than mutation_preview. Use this to verify mutations will work correctly before committing. Returns detailed PostgreSQL error info (code, constraint, hint) on failure. Optionally use server/database/schema params for one-time execution on a different server.",
    inputSchema: z.object({
      sql: z
        .string()
        .describe("The INSERT, UPDATE, or DELETE statement to dry-run"),
      sampleSize: z
        .number()
        .optional()
        .default(10)
        .describe("Number of sample rows to return (default: 10, max: 20)"),
      // Connection override parameters
      server: z
        .string()
        .optional()
        .describe("One-time server override. Execute on this server without changing main connection."),
      database: z
        .string()
        .optional()
        .describe("One-time database override. Uses this database for execution."),
      schema: z
        .string()
        .optional()
        .describe("One-time schema override. Sets search_path for this execution only."),
    }),
  },
  async (args) => {
    const result = await mutationDryRunWithRetry(args);
    return { content: [{ type: "text", text: JSON.stringify(withContext(result), null, 2) }] };
  }
);

server.registerTool(
  "dry_run_sql_file",
  {
    description:
      "Execute a SQL file in dry-run mode - actually runs ALL statements within a transaction, captures REAL results for each (row counts, errors with line numbers, constraint violations), then ROLLBACK so nothing persists. Perfect for testing migrations before deploying. Returns detailed error info including PostgreSQL error codes, constraint names, and hints to help quickly fix issues. Warns about non-rollbackable operations (sequences, VACUUM, etc.). Optionally use server/database/schema params for one-time execution on a different server.",
    inputSchema: z.object({
      filePath: z
        .string()
        .describe("Absolute or relative path to the .sql file to dry-run"),
      stripPatterns: z
        .array(z.string())
        .optional()
        .describe("Patterns to strip from SQL before execution (e.g., ['/'] for Liquibase)"),
      stripAsRegex: z
        .boolean()
        .optional()
        .default(false)
        .describe("If true, stripPatterns are treated as regex patterns"),
      maxStatements: z
        .number()
        .optional()
        .default(50)
        .describe("Maximum statements to include in results (default: 50, max: 200)"),
      stopOnError: z
        .boolean()
        .optional()
        .default(false)
        .describe("Stop on first error (default: false - continues to show ALL errors)"),
      // Connection override parameters
      server: z
        .string()
        .optional()
        .describe("One-time server override. Execute on this server without changing main connection."),
      database: z
        .string()
        .optional()
        .describe("One-time database override. Uses this database for execution."),
      schema: z
        .string()
        .optional()
        .describe("One-time schema override. Sets search_path for this execution only."),
    }),
  },
  async (args) => {
    const result = await dryRunSqlFileWithRetry(args);
    return { content: [{ type: "text", text: JSON.stringify(withContext(result), null, 2) }] };
  }
);

server.registerTool(
  "batch_execute",
  {
    description:
      "Execute multiple SQL queries in parallel. Returns all results keyed by query name. Efficient for fetching multiple independent pieces of data in one call. Optionally use server/database/schema params for one-time execution on a different server.",
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
      // Connection override parameters
      server: z
        .string()
        .optional()
        .describe("One-time server override. Execute on this server without changing main connection."),
      database: z
        .string()
        .optional()
        .describe("One-time database override. Uses this database for execution."),
      schema: z
        .string()
        .optional()
        .describe("One-time schema override. Sets search_path for this execution only."),
    }),
  },
  async (args) => {
    const result = await batchExecuteWithRetry(args);
    return { content: [{ type: "text", text: JSON.stringify(withContext(result), null, 2) }] };
  }
);

server.registerTool(
  "begin_transaction",
  {
    description:
      "Start a new database transaction. Returns a transactionId to use with execute_sql, commit_transaction, or rollback_transaction. Transactions allow atomic execution of multiple statements.",
    inputSchema: z.object({
      name: z
        .string()
        .optional()
        .describe("Optional human-readable name for the transaction to help identify it later"),
    }),
  },
  async (args) => {
    const result = await beginTransactionWithRetry(args);
    return { content: [{ type: "text", text: JSON.stringify(withContext(result), null, 2) }] };
  }
);

server.registerTool(
  "commit_transaction",
  {
    description:
      "Commit an active transaction, making all changes permanent.",
    inputSchema: z.object({
      transactionId: z
        .string()
        .describe("The transaction ID returned by begin_transaction"),
    }),
  },
  async (args) => {
    const result = await commitTransactionWithRetry(args);
    return { content: [{ type: "text", text: JSON.stringify(withContext(result), null, 2) }] };
  }
);

server.registerTool(
  "rollback_transaction",
  {
    description:
      "Rollback an active transaction, undoing all changes made within it.",
    inputSchema: z.object({
      transactionId: z
        .string()
        .describe("The transaction ID returned by begin_transaction"),
    }),
  },
  async (args) => {
    const result = await rollbackTransactionWithRetry(args);
    return { content: [{ type: "text", text: JSON.stringify(withContext(result), null, 2) }] };
  }
);

server.registerTool(
  "get_transaction_info",
  {
    description:
      "Get information about an active transaction, including its name, server, database, and when it started.",
    inputSchema: z.object({
      transactionId: z
        .string()
        .describe("The transaction ID returned by begin_transaction"),
    }),
  },
  async (args) => {
    const result = await getTransactionInfoWithRetry(args);
    return { content: [{ type: "text", text: JSON.stringify(withContext(result), null, 2) }] };
  }
);

server.registerTool(
  "list_transactions",
  {
    description:
      "List all active transactions. Returns transaction details including name, server, database, and start time.",
    inputSchema: z.object({}),
  },
  async () => {
    const result = await listActiveTransactionsWithRetry();
    return { content: [{ type: "text", text: JSON.stringify(withContext(result), null, 2) }] };
  }
);

server.registerTool(
  "explain_query",
  {
    description:
      "Show PostgreSQL's execution plan for a query. Use this to understand query performance and identify missing indexes. analyze=true runs the query to get actual timings (SELECT only). Optionally use server/database/schema params for one-time execution on a different server.",
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
      // Connection override parameters
      server: z
        .string()
        .optional()
        .describe("One-time server override. Execute on this server without changing main connection."),
      database: z
        .string()
        .optional()
        .describe("One-time database override. Uses this database for execution."),
      schema: z
        .string()
        .optional()
        .describe("One-time schema override. Sets search_path for this execution only."),
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
  "export_to_sql_file",
  {
    description:
      "Export schema (DDL) and/or data from the connected database to a .sql file. Supports four content kinds via the 'what' parameter: 'objects' (DDL of a list of objects), 'data' (INSERT statements for tables), 'schema_dump' (full schema, optionally with data), 'query_result' (SELECT result emitted as INSERTs into a target table). Mode is 'append' (default, appends to existing file with separator banner) or 'overwrite'. The header banner records timestamp and source server alias (host/port hidden). Use this before transfer_objects or for migration script generation.",
    inputSchema: z.object({
      filePath: z.string().describe("Absolute or relative path to the .sql file. Must end with .sql."),
      mode: z
        .enum(["append", "overwrite"])
        .optional()
        .default("append")
        .describe("File write mode. Append (default) preserves existing content with a separator banner; overwrite replaces the file."),
      what: z
        .union([
          z.object({
            kind: z.literal("objects"),
            objects: z
              .array(
                z.object({
                  kind: z.enum([
                    "extension", "schema", "sequence", "type",
                    "table", "index", "view", "matview",
                    "function", "procedure", "trigger",
                  ]),
                  name: z.string(),
                  schema: z.string().optional(),
                })
              )
              .describe("List of objects to export DDL for. Topologically ordered by dependency."),
          }),
          z.object({
            kind: z.literal("data"),
            tables: z.array(z.string()).describe("Schema-qualified or scope-relative table names."),
            format: z.literal("insert").optional().default("insert"),
            where: z.string().optional().describe("WHERE clause (without the keyword)"),
            orderBy: z.string().optional(),
            limit: z.number().optional(),
          }),
          z.object({
            kind: z.literal("schema_dump"),
            schema: z.string().optional(),
            include_data: z.boolean().optional().default(false),
          }),
          z.object({
            kind: z.literal("query_result"),
            sql: z.string(),
            target_table: z.string().describe("Schema-qualified target table for the emitted INSERT statements."),
          }),
        ]),
      include_create_if_not_exists: z.boolean().optional().default(true),
      confirm_overwrite: z.boolean().optional().describe(
        "When mode='overwrite' and file was modified <60s ago, set true to confirm. Foot-gun guard."
      ),
      server: z.string().optional().describe("One-time server override."),
      database: z.string().optional().describe("One-time database override."),
      schema: z.string().optional().describe("One-time schema override (sets default schema for refs)."),
    }),
  },
  async (args) => {
    const result = await exportToSqlFileWithRetry(args as any);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.registerTool(
  "transfer_objects",
  {
    description:
      "Transfer schema (DDL) and/or data from one configured server/database to another (same server, different DB, or fully remote). Builds on the introspection module for DDL extraction with topological ordering. Modes: include='ddl'|'data'|'both'. Behavior on existing target objects: if_exists='skip'|'replace'|'error'. dry_run=true emits the would-be SQL to output_file or returns inline (no target writes). Both endpoints must be configured servers (PG_NAME_*); ad-hoc connection strings are not accepted (security). Refuses if target's effective access mode is readonly. FK constraints between tables are emitted as ALTER TABLE statements appended after tables to handle inter-table dependency cycles.",
    inputSchema: z.object({
      from: z.object({
        server: z.string(),
        database: z.string().optional(),
        schema: z.string().optional(),
      }).describe("Source endpoint."),
      to: z.object({
        server: z.string(),
        database: z.string().optional(),
        schema: z.string().optional(),
      }).describe("Target endpoint."),
      objects: z
        .union([
          z.literal("*"),
          z.array(
            z.object({
              kind: z.enum([
                "extension", "schema", "sequence", "type",
                "table", "index", "view", "matview",
                "function", "procedure", "trigger",
              ]),
              name: z.string(),
              schema: z.string().optional(),
            })
          ),
        ])
        .describe("List of objects, or '*' for all objects in source schema."),
      include: z
        .enum(["ddl", "data", "both"])
        .optional()
        .default("both"),
      if_exists: z
        .enum(["skip", "replace", "error"])
        .optional()
        .default("error")
        .describe("Behavior when a target object already exists."),
      data_strategy: z
        .literal("insert_batches")
        .optional()
        .default("insert_batches"),
      dry_run: z
        .boolean()
        .optional()
        .default(false)
        .describe("Generate SQL without applying. Use with output_file."),
      output_file: z
        .string()
        .optional()
        .describe("When dry_run is true, write generated SQL to this .sql path."),
    }),
  },
  async (args) => {
    const result = await transferObjectsWithRetry(args as any);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.registerTool(
  "describe_table",
  {
    description:
      "Single rich call describing a table: columns (type/nullable/default + null %/distinct ratio from pg_stats), primary key, foreign keys going OUT (this table → others) AND coming IN (others → this table), all indexes (with definitions), table size, row-count estimate, and sample rows. Replaces ~5 separate calls (get_object_details + LIMIT 5 + COUNT(*) + pg_stats).",
    inputSchema: z.object({
      table: z.string().describe("Table name (unqualified — use schema for the schema)."),
      schema: z.string().optional().default("public"),
      sample_size: z.number().optional().default(5).describe("Number of sample rows to fetch (0 to skip)."),
      profile_columns: z.array(z.string()).optional().describe("Columns to profile (default: all up to 20)."),
      server: z.string().optional(),
      database: z.string().optional(),
      override_schema: z.string().optional().describe("One-time schema override for the connection (separate from `schema` which is the table's schema)."),
    }),
  },
  async (args) => {
    const result = await describeTableWithRetry(args as any);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.registerTool(
  "find_dependents",
  {
    description:
      "Find what depends on a database object before dropping it. Recursively walks pg_depend, classifies dependents (views, foreign keys, functions, materialized views, indexes, rules) and reports each with its depth from the target. Use this BEFORE running DROP CASCADE to understand the blast radius. Returns the dependent objects flattened with `depth` (1 = directly depends, 2 = depends on a depth-1 dependent, etc).",
    inputSchema: z.object({
      name: z.string().describe("Object name."),
      kind: z.enum([
        "table", "view", "matview", "sequence", "index",
        "function", "procedure", "type", "extension", "schema",
      ]).optional().default("table"),
      schema: z.string().optional().default("public"),
      max_depth: z.number().optional().default(5).describe("Recursion limit (1-10)."),
      server: z.string().optional(),
      database: z.string().optional(),
      override_schema: z.string().optional(),
    }),
  },
  async (args) => {
    const result = await findDependentsWithRetry(args as any);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.registerTool(
  "schema_diff",
  {
    description:
      "Compute the DDL delta between two { server, database, schema } endpoints. Returns objects to CREATE (in source but not target), DROP (in target but not source), and MODIFY (in both, but DDL differs), plus a single `migrationSql` script that, when applied to the TARGET, converges its schema with the SOURCE. CREATE OR REPLACE is used for views/functions/procedures; DROP+CREATE for everything else. Source is the source of truth.",
    inputSchema: z.object({
      source: z.object({
        server: z.string(),
        database: z.string().optional(),
        schema: z.string().optional(),
      }),
      target: z.object({
        server: z.string(),
        database: z.string().optional(),
        schema: z.string().optional(),
      }),
    }),
  },
  async (args) => {
    const result = await schemaDiffWithRetry(args as any);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.registerTool(
  "lock_check",
  {
    description:
      "Static analysis of a SQL statement to determine the PostgreSQL lock level it will require, whether it forces a full table rewrite, and an estimated duration based on target table size. Returns warnings for ACCESS EXCLUSIVE locks on busy production tables and concrete recommendations (e.g., use CREATE INDEX CONCURRENTLY, NOT VALID + VALIDATE CONSTRAINT, etc). Use BEFORE running DDL on production. Knows lock semantics for ALTER TABLE variants, CREATE/DROP INDEX (concurrent vs not), VACUUM, CLUSTER, REFRESH MATERIALIZED VIEW, and more.",
    inputSchema: z.object({
      sql: z.string().describe("SQL DDL statement to analyze."),
      estimate_duration: z.boolean().optional().default(true).describe("Look up target table size to estimate duration."),
      server: z.string().optional(),
      database: z.string().optional(),
      schema: z.string().optional(),
    }),
  },
  async (args) => {
    const result = await lockCheckWithRetry(args as any);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.registerTool(
  "detect_migration_state",
  {
    description:
      "Probe the database for migration tool tracker tables (Liquibase, Flyway, Alembic, Prisma, Knex, Sequelize, Django, Rails, Goose, TypeORM). Returns which tools are detected, the schema and table holding their state, the count of applied migrations, and the latest version. AI agents use this to immediately understand whether the DB is managed by a migration tool before suggesting changes.",
    inputSchema: z.object({
      schemas: z.array(z.string()).optional().describe("Schemas to probe. Default: all non-system schemas."),
      server: z.string().optional(),
      database: z.string().optional(),
      schema: z.string().optional(),
    }),
  },
  async (args) => {
    const result = await detectMigrationStateWithRetry(args as any);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.registerTool(
  "safe_alter_table",
  {
    description:
      "Convert a high-level intent ('add NOT NULL column with default', 'add NOT NULL', 'add foreign key', 'add CHECK', 'create index', 'drop index') into a multi-step zero-downtime DDL recipe. Each step has its own SQL, expected lock level, and notes. Pipe the resulting `scriptSql` through `dry_run_sql_file` for verification, then through `executeSqlFile(useTransaction=false)` for the production rollout (CONCURRENTLY operations cannot run inside a transaction).",
    inputSchema: z.object({
      intent: z
        .union([
          z.object({
            kind: z.literal("add_not_null_column_with_default"),
            table: z.string(),
            column: z.string(),
            type: z.string(),
            default_expr: z.string(),
          }),
          z.object({
            kind: z.literal("add_not_null"),
            table: z.string(),
            column: z.string(),
          }),
          z.object({
            kind: z.literal("add_foreign_key"),
            table: z.string(),
            constraint_name: z.string(),
            columns: z.array(z.string()),
            references_table: z.string(),
            references_columns: z.array(z.string()),
          }),
          z.object({
            kind: z.literal("add_check"),
            table: z.string(),
            constraint_name: z.string(),
            check_expr: z.string(),
          }),
          z.object({
            kind: z.literal("create_index"),
            table: z.string(),
            index_name: z.string(),
            columns: z.array(z.string()),
            index_type: z.string().optional().default("btree"),
            unique: z.boolean().optional().default(false),
          }),
          z.object({
            kind: z.literal("drop_index"),
            index_name: z.string(),
            schema: z.string().optional(),
          }),
        ]),
    }),
  },
  async (args) => {
    const result = await safeAlterTable(args as any);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.registerTool(
  "column_profile",
  {
    description:
      "Single-pass profile per column: null %, distinct count, top-K values with frequencies, min/max, and type-aware stats (avg/stddev for numeric, length distribution for text, range for temporal). Uses TABLESAMPLE BERNOULLI for tables larger than `sample_threshold` (default 1M rows) to keep latency bounded. Replaces a dozen separate exploratory queries an AI agent would otherwise run to understand a column's shape.",
    inputSchema: z.object({
      table: z.string(),
      schema: z.string().optional().default("public"),
      columns: z.array(z.string()).optional().describe("Specific columns to profile (default: all up to 30)."),
      sample_percent: z.number().optional().default(10),
      sample_threshold: z.number().optional().default(1_000_000),
      top_k: z.number().optional().default(10).describe("Top-K values per column (max 25)."),
      server: z.string().optional(),
      database: z.string().optional(),
      override_schema: z.string().optional(),
    }),
  },
  async (args) => {
    const result = await columnProfileWithRetry(args as any);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.registerTool(
  "generate_seed_data",
  {
    description:
      "Generate schema-aware fake seed data for a table. Respects NOT NULL, UNIQUE/PK (with retry-with-collision-suffix), enum types (cycles through labels), defaults (uses DEFAULT for unknown types), text length limits, and FK columns (skipped or filled — caller's choice). Generates type-appropriate values for numeric, text, boolean, uuid, date/timestamp, bytea, JSON, inet, cidr, macaddr. Per-column overrides via `column_values`. Apply directly (default) or return SQL only via `apply: false`.",
    inputSchema: z.object({
      table: z.string(),
      schema: z.string().optional().default("public"),
      count: z.number().min(1).max(100_000),
      column_values: z.record(z.string(), z.string()).optional()
        .describe("Per-column SQL value override (e.g. { country: \"'US'\", priority: '1' }). Quoted as PG literals."),
      skip_fks: z.boolean().optional().default(false),
      apply: z.boolean().optional().default(true).describe("Apply to DB (default true) or return SQL only (false)."),
      server: z.string().optional(),
      database: z.string().optional(),
      override_schema: z.string().optional(),
    }),
  },
  async (args) => {
    const result = await generateSeedDataWithRetry(args as any);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.registerTool(
  "find_blocking_queries",
  {
    description:
      "Show currently-blocking sessions in a friendly tree (blocker → blocked) using pg_stat_activity ⨝ pg_blocking_pids(). Replaces the gnarly join an AI agent struggles to write. Returns each session's pid, user, database, application name, state, current query, time in state, and wait_event. Use to diagnose slowdowns and pick a candidate for kill_query.",
    inputSchema: z.object({
      include_idle: z.boolean().optional().default(true),
      limit: z.number().optional().default(50),
      server: z.string().optional(),
      database: z.string().optional(),
      schema: z.string().optional(),
    }),
  },
  async (args) => {
    const result = await findBlockingQueriesWithRetry(args as any);
    return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
  }
);

server.registerTool(
  "kill_query",
  {
    description:
      "Cancel or terminate a backend session by PID. mode='cancel' (soft, pg_cancel_backend) interrupts the current statement; mode='terminate' (hard, pg_terminate_backend) kills the entire backend. Both require confirm:true. Refused if the target server's effective access mode is readonly. Returns a snapshot of the target session before signaling.",
    inputSchema: z.object({
      pid: z.number().describe("Backend PID to signal."),
      mode: z.enum(["cancel", "terminate"]).describe("Soft cancel (statement only) or hard terminate (backend)."),
      confirm: z.boolean().describe("Required confirmation. Foot-gun guard."),
      server: z.string().optional(),
      database: z.string().optional(),
      schema: z.string().optional(),
    }),
  },
  async (args) => {
    const result = await killQueryWithRetry(args as any);
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
