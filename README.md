# PostgreSQL MCP Server

A Model Context Protocol (MCP) server for PostgreSQL database management and analysis. This server provides comprehensive tools for exploring database schemas, executing queries, analyzing performance, and monitoring database health.

## Installation

```bash
npm install -g postgres-mcp-server
```

Or run directly with npx:

```bash
npx postgres-mcp-server
```

## Configuration

### Environment Variables

You can configure servers using either **individual environment variables** (recommended) or a **JSON string** (legacy).

#### Option 1: Individual Environment Variables (Recommended)

Configure each server using `PG_*` prefixed variables with a numeric or named suffix:

```bash
# Server 1 - Development
export PG_NAME_1="dev"
export PG_HOST_1="dev.example.com"
export PG_PORT_1="5432"
export PG_USERNAME_1="dev_user"
export PG_PASSWORD_1="dev_password"
export PG_DATABASE_1="myapp_dev"
export PG_SCHEMA_1="public"
export PG_SSL_1="true"
export PG_DEFAULT_1="true"

# Server 2 - Staging
export PG_NAME_2="staging"
export PG_HOST_2="staging.example.com"
export PG_PORT_2="5432"
export PG_USERNAME_2="staging_user"
export PG_PASSWORD_2="staging_password"
export PG_DATABASE_2="myapp_staging"
export PG_SSL_2="require"

# Server 3 - Production
export PG_NAME_3="production"
export PG_HOST_3="prod.example.com"
export PG_PORT_3="5432"
export PG_USERNAME_3="prod_user"
export PG_PASSWORD_3="prod_password"
export PG_DATABASE_3="myapp_prod"
export PG_SCHEMA_3="app"
export PG_SSL_3="true"
```

**Environment Variable Reference:**

| Variable          | Required | Description                                                        |
| ----------------- | -------- | ------------------------------------------------------------------ |
| `PG_NAME_{n}`     | Yes      | Server name (used to identify the server)                          |
| `PG_HOST_{n}`     | Yes      | PostgreSQL server hostname                                         |
| `PG_PORT_{n}`     | No       | Port number (default: "5432")                                      |
| `PG_USERNAME_{n}` | Yes      | Database username                                                  |
| `PG_PASSWORD_{n}` | No       | Database password                                                  |
| `PG_DATABASE_{n}` | No       | Default database (default: "postgres")                             |
| `PG_SCHEMA_{n}`   | No       | Default schema (default: "public")                                 |
| `PG_SSL_{n}`      | No       | SSL mode: `true`, `false`, `require`, `prefer`, `allow`, `disable` |
| `PG_DEFAULT_{n}`  | No       | Set to `true` to make this the default server                      |

**Note:** The suffix `{n}` can be any string (e.g., `_1`, `_2`, `_DEV`, `_PROD`). The system detects servers by looking for `PG_NAME_*` variables.

#### Option 2: JSON Configuration (Legacy)

Set the `POSTGRES_SERVERS` environment variable with a JSON object:

```bash
export POSTGRES_SERVERS='{
  "dev": {
    "host": "dev.example.com",
    "port": "5432",
    "username": "your_username",
    "password": "your_password",
    "defaultDatabase": "myapp_dev",
    "defaultSchema": "public",
    "isDefault": true,
    "ssl": true
  },
  "staging": {
    "host": "staging.example.com",
    "port": "5432",
    "username": "your_username",
    "password": "your_password",
    "defaultDatabase": "myapp_staging",
    "ssl": "require"
  }
}'
```

**JSON Configuration Options:**

- `host` (required): PostgreSQL server hostname
- `port` (optional): Port number (default: "5432")
- `username` (required): Database username
- `password` (required): Database password
- `defaultDatabase` (optional): Default database to connect to (default: "postgres")
- `defaultSchema` (optional): Default schema to use (default: "public")
- `isDefault` (optional): Mark this server as the default server to connect to
- `ssl` (optional): SSL/TLS connection configuration:
  - `true` or `"require"`: Enable SSL (recommended for cloud databases)
  - `"prefer"`: Use SSL if available
  - `"allow"`: Try non-SSL first, then SSL
  - `false` or `"disable"`: Disable SSL
  - Object: `{ "rejectUnauthorized": false, "ca": "...", "cert": "...", "key": "..." }`

**Note:** If both formats are used, individual `PG_*` variables take precedence over `POSTGRES_SERVERS`.

#### POSTGRES_ACCESS_MODE (optional)

Controls whether write operations are allowed:

- `full` (default): Full access - allows all SQL operations including INSERT, UPDATE, DELETE, CREATE, DROP, etc.
- `readonly` / `read-only` / `ro`: Read-only mode - only SELECT and other read operations are allowed

```bash
# For read-only access (recommended for production)
export POSTGRES_ACCESS_MODE="readonly"

# For full access (use with caution)
export POSTGRES_ACCESS_MODE="full"
```

### Claude Desktop Configuration

Add the server to your Claude Desktop MCP configuration (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "postgres": {
      "command": "npx",
      "args": ["@tejasanik/postgres-mcp-server"],
      "env": {
        "PG_NAME_1": "dev",
        "PG_HOST_1": "your-host.com",
        "PG_PORT_1": "5432",
        "PG_USERNAME_1": "user",
        "PG_PASSWORD_1": "pass",
        "PG_DATABASE_1": "mydb",
        "PG_SSL_1": "true",
        "PG_DEFAULT_1": "true",
        "POSTGRES_ACCESS_MODE": "readonly"
      }
    }
  }
}
```

### Claude Code CLI Configuration

Add the server using the Claude Code CLI:

```bash
claude mcp add-json postgres_dbs --scope user '{
  "command": "npx",
  "args": ["-y","@tejasanik/postgres-mcp-server"],
  "env": {
    "PG_NAME_1": "dev",
    "PG_HOST_1": "dev.example.com",
    "PG_PORT_1": "5432",
    "PG_USERNAME_1": "user",
    "PG_PASSWORD_1": "pass",
    "PG_DATABASE_1": "mydb",
    "PG_SSL_1": "true",
    "PG_DEFAULT_1": "true",
    "PG_NAME_2": "staging",
    "PG_HOST_2": "staging.example.com",
    "PG_USERNAME_2": "user",
    "PG_PASSWORD_2": "pass",
    "PG_SSL_2": "true",
    "POSTGRES_ACCESS_MODE": "readonly"
  }
}'
```

## Available Tools

### Server & Database Management

#### `list_servers`

Lists all configured PostgreSQL servers. Returns server names, hosts, ports, and connection status. Use this first to discover available servers.

**Parameters:**

- `filter` (optional): Filter servers by name or host (case-insensitive partial match)

**Returns:**

- `servers`: Array of server information (name, isConnected, isDefault, defaultDatabase, defaultSchema)
- `currentServer`: Currently connected server name (or null)
- `currentDatabase`: Currently connected database (or null)
- `currentSchema`: Current schema (or null)

**Note:** Host and port are intentionally hidden from responses for security.

#### `list_databases`

Lists databases in a specific PostgreSQL server. Always provide the server name to avoid confusion.

**Parameters:**

- `serverName` (required): Name of the server to list databases from. Use `list_servers` to see available servers.
- `filter` (optional): Filter databases by name (case-insensitive partial match)
- `includeSystemDbs` (optional): Include system databases (template0, template1). Default: false
- `maxResults` (optional): Maximum number of databases to return (default: 50, max: 200)

**Returns:**

- `serverName`: The server name that was queried
- `databases`: Array of database information (name, owner, encoding, size)
- `currentDatabase`: Currently connected database on this server (or null)

#### `switch_server_db`

Switch to a different PostgreSQL server and optionally a specific database and schema.

**Parameters:**

- `server` (required): Name of the server to connect to
- `database` (optional): Name of the database to connect to (uses server's defaultDatabase or "postgres")
- `schema` (optional): Default schema to use (uses server's defaultSchema or "public")

#### `get_current_connection`

Returns details about the current database connection including server, database, schema, and access mode.

**Parameters:** None

**Returns:**

- `isConnected`: Whether currently connected to a database
- `server`: Current server name
- `database`: Current database name
- `schema`: Current schema name
- `accessMode`: "readonly" or "full"

### Schema & Object Exploration

#### `list_schemas`

Lists all database schemas in the current PostgreSQL database.

**Parameters:**

- `includeSystemSchemas` (optional): Include system schemas

#### `list_objects`

Lists database objects within a specified schema.

**Parameters:**

- `schema` (required): Schema name to list objects from
- `objectType` (optional): Type of objects to list (table, view, sequence, extension, all)
- `filter` (optional): Filter objects by name

#### `get_object_details`

Provides detailed information about a database object including columns, constraints, indexes, size, and row count.

**Parameters:**

- `schema` (required): Schema name containing the object
- `objectName` (required): Name of the object
- `objectType` (optional): Type of the object

### Query Execution

#### `execute_sql`

Executes SQL statements on the database. Supports pagination and parameterized queries. Read-only mode prevents write operations.

**Parameters:**

- `sql` (required): SQL statement to execute. Use `$1`, `$2`, etc. for parameterized queries.
- `params` (optional): Array of parameters for parameterized queries (e.g., `[123, "value"]`). Prevents SQL injection.
- `maxRows` (optional): Maximum rows to return (default: 1000, max: 100000). Use with `offset` for pagination.
- `offset` (optional): Number of rows to skip for pagination (default: 0).
- `allowLargeScript` (optional): Set to true to bypass the 100KB SQL length limit for deployment scripts.
- `includeSchemaHint` (optional): Include schema information (columns, primary keys, foreign keys) for tables referenced in the query. Helps AI agents understand table structure without separate queries.

**Returns:**

- `rows`: Result rows (paginated)
- `rowCount`: Total number of rows in the result
- `fields`: Column names
- `executionTimeMs`: Query execution time in milliseconds
- `offset`: Current offset
- `hasMore`: Whether more rows are available
- `outputFile`: (Only if output is too large) Path to temp file with full results
- `schemaHint`: (When includeSchemaHint=true) Schema information for referenced tables:
  - `tables`: Array of table schemas with columns, primary keys, foreign keys, and row count estimates

**Note:** Large outputs are automatically written to a temp file, and the file path is returned. This prevents token wastage when dealing with large result sets.

#### `execute_sql_file`

Executes a `.sql` file from the filesystem. Useful for running migration scripts, schema changes, or data imports.

**Parameters:**

- `filePath` (required): Absolute or relative path to the `.sql` file to execute
- `useTransaction` (optional): Wrap execution in a transaction (default: true). If any statement fails, all changes are rolled back.
- `stopOnError` (optional): Stop execution on first error (default: true). If false, continues with remaining statements and collects all errors.

**Returns:**

- `success`: Whether all statements executed successfully
- `filePath`: Resolved file path
- `fileSize`: File size in bytes
- `totalStatements`: Total executable statements in the file
- `statementsExecuted`: Number of successfully executed statements
- `statementsFailed`: Number of failed statements
- `executionTimeMs`: Total execution time in milliseconds
- `rowsAffected`: Total rows affected by all statements
- `errors`: (When stopOnError=false) Array of error details:
  - `statementIndex`: Which statement failed (1-based)
  - `sql`: The failing SQL (truncated to 200 chars)
  - `error`: Error message
- `rollback`: Whether a rollback was performed

**Limits:** Max file size: 50MB. Supports PostgreSQL-specific syntax including dollar-quoted strings and block comments.

#### `mutation_preview`

Preview the effect of INSERT, UPDATE, or DELETE statements without executing them. Shows estimated rows affected and a sample of rows that would be modified. Essential for verifying destructive queries before running them.

**Parameters:**

- `sql` (required): The INSERT, UPDATE, or DELETE statement to preview
- `sampleSize` (optional): Number of sample rows to show (default: 5, max: 20)

**Returns:**

- `mutationType`: Type of mutation (INSERT, UPDATE, DELETE)
- `estimatedRowsAffected`: Estimated number of rows that would be affected
- `sampleAffectedRows`: Sample of rows that would be modified (for UPDATE/DELETE)
- `targetTable`: The table being modified
- `whereClause`: The WHERE clause from the query (if present)
- `warning`: Warning message if no WHERE clause (all rows affected) or for INSERT previews

**Example:**

```
mutation_preview({ sql: "DELETE FROM orders WHERE status = 'cancelled'" })
// Returns: { mutationType: "DELETE", estimatedRowsAffected: 150, sampleAffectedRows: [...5 rows...] }
```

#### `batch_execute`

Execute multiple SQL queries in parallel. Returns all results keyed by query name. Efficient for fetching multiple independent pieces of data in a single call.

**Parameters:**

- `queries` (required): Array of queries to execute (max 20):
  - `name`: Unique name for this query (used as key in results)
  - `sql`: SQL query to execute
  - `params` (optional): Query parameters
- `stopOnError` (optional): Stop on first error (default: false, continues with all queries)

**Returns:**

- `totalQueries`: Total number of queries in the batch
- `successCount`: Number of successful queries
- `failureCount`: Number of failed queries
- `totalExecutionTimeMs`: Total execution time in milliseconds
- `results`: Object with query results keyed by name:
  - `success`: Whether the query succeeded
  - `rows`: Result rows (if successful)
  - `rowCount`: Number of rows returned
  - `error`: Error message (if failed)
  - `executionTimeMs`: Individual query execution time

**Example:**

```
batch_execute({
  queries: [
    { name: "user_count", sql: "SELECT COUNT(*) FROM users" },
    { name: "order_total", sql: "SELECT SUM(total) FROM orders" },
    { name: "recent_signups", sql: "SELECT COUNT(*) FROM users WHERE created_at > NOW() - INTERVAL '7 days'" }
  ]
})
// Returns all three results in parallel, keyed by name
```

#### `explain_query`

Gets the execution plan for a SQL query.

**Parameters:**

- `sql` (required): SQL query to explain
- `analyze` (optional): Execute query to get real timing
- `buffers` (optional): Include buffer usage statistics
- `format` (optional): Output format (text, json, yaml, xml)
- `hypotheticalIndexes` (optional): Simulate indexes (requires hypopg extension)

### Performance Analysis

#### `get_top_queries`

Reports the slowest SQL queries based on execution time.

**Parameters:**

- `limit` (optional): Number of queries to return (default: 10)
- `orderBy` (optional): Order by total_time, mean_time, or calls
- `minCalls` (optional): Minimum number of calls to include

**Requires:** `pg_stat_statements` extension

#### `analyze_workload_indexes`

Analyzes database workload and recommends optimal indexes.

**Parameters:**

- `topQueriesCount` (optional): Number of top queries to analyze
- `includeHypothetical` (optional): Include hypothetical index analysis

#### `analyze_query_indexes`

Analyzes specific SQL queries and recommends indexes.

**Parameters:**

- `queries` (required): Array of SQL queries to analyze (max 10)

### Health Monitoring

#### `analyze_db_health`

Performs comprehensive database health checks including:

- **Buffer Cache Hit Rate**: Checks cache efficiency
- **Connection Health**: Monitors connection usage
- **Invalid Indexes**: Detects broken indexes
- **Unused Indexes**: Identifies indexes that aren't being used
- **Duplicate Indexes**: Finds redundant indexes
- **Vacuum Health**: Monitors dead tuple ratios
- **Sequence Limits**: Warns about sequences approaching limits
- **Constraint Validation**: Checks for unvalidated constraints

## Usage Examples

### Connect to a Server and List Databases

```
1. Use list_servers to see available servers
2. Use list_databases with serverName="dev" to see databases in the dev server
3. Use switch_server_db with server="dev", database="myapp" to connect
```

### Explore Database Schema

```
1. Use list_schemas to see all schemas
2. Use list_objects with schema="public" to see tables
3. Use get_object_details with schema="public", objectName="users" to see table structure
```

### Analyze Query Performance

```
1. Use explain_query with your SQL to see the execution plan
2. Use get_top_queries to find slow queries
3. Use analyze_query_indexes to get index recommendations
```

### Health Check

```
1. Use analyze_db_health to run all health checks
2. Review warnings and critical issues
3. Take action on recommendations
```

### Execute SQL Migration File

```
1. Use execute_sql_file with filePath="/path/to/migration.sql"
2. By default, runs in a transaction - all changes rolled back on error
3. Set stopOnError=false to continue on errors and get a full report
4. Set useTransaction=false for DDL statements that can't run in transactions
```

## Features

### Auto-Reconnect on Connection Errors

The server automatically handles stale database connections. When a connection error occurs (e.g., server went inactive, connection reset, timeout), the server will:

1. Detect the connection error
2. Invalidate the stale connection
3. Automatically reconnect using the stored server/database/schema
4. Retry the operation once

This is particularly useful for:
- Staging/development servers that go idle
- Cloud databases with connection timeouts
- Network interruptions

Supported error patterns include: `Connection terminated`, `ECONNRESET`, `ETIMEDOUT`, `server closed the connection unexpectedly`, and PostgreSQL error codes like `57P01` (admin_shutdown), `08003` (connection_does_not_exist), etc.

### Hidden Connection Details

Host URLs, ports, and credentials are never exposed in tool responses. Only server names (aliases) are visible, preventing accidental exposure of infrastructure details.

## Security

- **Access Mode**: By default, the server runs in **full access mode**. Set `POSTGRES_ACCESS_MODE=readonly` to prevent write operations (INSERT, UPDATE, DELETE, DROP, etc.). Recommended for production environments.
- **SQL Injection Protection**: All user inputs are validated and parameterized queries are used where possible.
- **Query Timeout**: Default 30-second timeout prevents runaway queries.
- **Credentials**: Managed via environment variables and never logged or exposed through the MCP interface.
- **File Permissions**: Large output files are created with restricted permissions (0600).
- **Hidden Infrastructure**: Host URLs, ports, and passwords are never included in tool responses.

## Requirements

- Node.js 18.0.0 or higher
- PostgreSQL 11 or higher
- Optional: `pg_stat_statements` extension for query performance analysis
- Optional: `hypopg` extension for hypothetical index simulation

## License

MIT
