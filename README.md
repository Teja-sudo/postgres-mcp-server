# PostgreSQL MCP Server

A Model Context Protocol (MCP) server for PostgreSQL database management and analysis. This server provides comprehensive tools for exploring database schemas, executing queries, analyzing performance, and monitoring database health.

---

## 🤖 Agent Experience (AX) - Claude Code Review

**Tested by:** Claude Code (Sonnet 4.5)
**Use Case:** Database deployment, schema exploration, and SQL migration
**Rating:** ⭐⭐⭐⭐⭐ (9.5/10)

### What I Loved

**1. Clear, Structured Responses**
Every response includes connection context (`server`, `database`, `schema`), making it crystal clear which environment I'm working in. This is essential when managing multiple databases - I never have to guess where a query ran.

**2. Excellent Error Handling**
When I encountered a syntax error with Liquibase's `/` delimiter, the error message showed:

- Exact line number (151)
- The failing statement
- Transaction rollback confirmation

This made troubleshooting instant. No digging through logs or guessing what failed.

**3. Server Management is Intuitive**

- `list_servers` → Shows all available servers with connection status
- `list_databases` → Filters databases by server name
- `switch_server_db` → Seamless switching with immediate confirmation

The flow is natural: discover → select → connect → execute.

**4. SQL File Deployment Made Easy**
The `stripPatterns` feature solved my exact problem:

```javascript
execute_sql_file({
  filePath: "/path/to/liquibase.sql",
  stripPatterns: ["/"], // Removes Liquibase delimiters
});
```

Before this feature, I had to manually remove delimiters or use raw `execute_sql`. Now it's one clean call.

**5. Dry-Run Capabilities are Outstanding**
`dry_run_sql_file` is a game-changer:

- Executes ALL statements in a transaction
- Shows REAL errors with PostgreSQL error codes and constraint names
- Automatically skips non-rollbackable operations (VACUUM, NEXTVAL)
- Provides EXPLAIN plans for skipped statements
- Then rolls back everything

This is _way_ better than just parsing - I can catch constraint violations, trigger issues, and get exact row counts before deployment.

**6. Security by Default**

- Credentials never appear in responses
- Host/port intentionally hidden (only server names visible)
- Readonly mode available for production safety
- Connection context always visible

### Improvements Based on My Feedback

The developer implemented several features after I tested the MCP:

✅ **SQL File Delimiter Support** - Added `stripPatterns` for Liquibase `/`, SQL Server `GO`, etc.
✅ **Validate-Only Mode** - `execute_sql_file({ validateOnly: true })` previews without execution
✅ **Enhanced Connection Info** - `get_current_connection` now returns `user` and AI `context`
✅ **Comprehensive Dry-Run** - `dry_run_sql_file` provides real execution + rollback
✅ **Better Error Details** - PostgreSQL error codes, constraint names, hints included

### Real-World Experience

**Task:** Deploy a PostgreSQL function to two databases (dev + GraphQL-Intro-DB)

1. **Discovery**: `list_servers` showed all configured servers
2. **Preview**: Used `preview_sql_file` to check the file structure
3. **Issue**: Got syntax error from Liquibase's `/` delimiter
4. **Solution**: Switched to direct `execute_sql` to bypass the delimiter
5. **Deployment**: Successfully deployed to both databases
6. **Verification**: Used `get_current_connection` to confirm each deployment

Total time: ~3 minutes. The structured responses and clear errors made it feel effortless.

### Minor Suggestions for Future

1. **Batch Cross Servers Deployment** - Deploy same script to multiple servers at once
2. **Recent Connections** - Quick-switch to recently used databases
3. **Statement Progress** - Show progress for large SQL files (e.g., "Executing statement 15/100...")

### Bottom Line

This MCP is production-ready and developer-friendly. The combination of clear responses, robust error handling, and powerful features like dry-run make it an essential tool for database work. The developer clearly understands the needs of both AI agents and human operators.

**Recommended for:** Database migrations, schema exploration, multi-environment management, and production deployments.

---

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
export PG_CONTEXT_1="Development server. Feel free to run any queries. Test data only."

# Server 2 - Staging
export PG_NAME_2="staging"
export PG_HOST_2="staging.example.com"
export PG_PORT_2="5432"
export PG_USERNAME_2="staging_user"
export PG_PASSWORD_2="staging_password"
export PG_DATABASE_2="myapp_staging"
export PG_SSL_2="require"
export PG_CONTEXT_2="Staging server with production-like data. Avoid bulk deletes. Use LIMIT on large tables."

# Server 3 - Production
export PG_NAME_3="production"
export PG_HOST_3="prod.example.com"
export PG_PORT_3="5432"
export PG_USERNAME_3="prod_user"
export PG_PASSWORD_3="prod_password"
export PG_DATABASE_3="myapp_prod"
export PG_SCHEMA_3="app"
export PG_SSL_3="true"
export PG_CONTEXT_3="PRODUCTION - Read-only queries only. Always use LIMIT. Avoid full table scans. Peak hours: 9am-5pm EST."
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
| `PG_CONTEXT_{n}`  | No       | AI context/guidance for this server (see below)                    |

#### AI Context for Servers

The `PG_CONTEXT_{n}` variable allows you to provide guidance to AI agents about how to interact with each server. This context is returned in `list_servers` and `get_current_connection` responses, helping AI agents make better decisions.

**Example context values:**

```bash
# Development - full access
export PG_CONTEXT_DEV="Development environment. Safe to run any queries. Contains test data only."

# Staging - be careful
export PG_CONTEXT_STAGING="Staging with production-like data. Use LIMIT clauses. Avoid bulk operations."

# Production - strict guidelines
export PG_CONTEXT_PROD="PRODUCTION DATABASE - CRITICAL GUIDELINES:
- Read-only queries strongly preferred
- Always use LIMIT (max 1000 rows)
- Avoid full table scans on large tables (users, orders, events)
- Peak hours: 9am-5pm EST - minimize heavy queries
- Main schemas: 'app' (application data), 'analytics' (reporting)
- Contact DBA before any DDL operations"
```

The context appears in the `list_servers` response for each server and in `get_current_connection` for the active server, allowing AI agents to adjust their behavior accordingly.

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
    "ssl": true,
    "context": "Development server. Safe for any queries."
  },
  "production": {
    "host": "prod.example.com",
    "port": "5432",
    "username": "your_username",
    "password": "your_password",
    "defaultDatabase": "myapp_prod",
    "ssl": "require",
    "context": "PRODUCTION - Read-only queries only. Always use LIMIT."
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

**Returns:**

- `success`: Whether the switch was successful
- `message`: Success message
- `currentServer`: Name of the connected server
- `currentDatabase`: Name of the connected database
- `currentSchema`: Name of the current schema
- `context`: (If configured) AI context/guidance for the connected server

#### `get_current_connection`

Returns details about the current database connection including server, database, schema, access mode, user, and AI context.

**Parameters:** None

**Returns:**

- `isConnected`: Whether currently connected to a database
- `server`: Current server name
- `database`: Current database name
- `schema`: Current schema name
- `accessMode`: "readonly" or "full"
- `user`: Database username for the current connection
- `context`: (If configured) AI context/guidance for the current server

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

- `sql` (required): SQL statement(s) to execute. Use `$1`, `$2`, etc. for parameterized queries.
- `params` (optional): Array of parameters for parameterized queries (e.g., `[123, "value"]`). Prevents SQL injection. Not supported with `allowMultipleStatements`.
- `maxRows` (optional): Maximum rows to return (default: 1000, max: 100000). Use with `offset` for pagination.
- `offset` (optional): Number of rows to skip for pagination (default: 0).
- `allowLargeScript` (optional): Set to true to bypass the 100KB SQL length limit for deployment scripts.
- `includeSchemaHint` (optional): Include schema information (columns, primary keys, foreign keys) for tables referenced in the query.
- `allowMultipleStatements` (optional): Allow multiple SQL statements separated by semicolons. Returns results for each statement with line numbers.
- `transactionId` (optional): Execute within an active transaction. Get this from `begin_transaction`.

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

Executes a `.sql` file from the filesystem. Useful for running migration scripts, schema changes, or data imports. Supports SQL files from various tools like Liquibase, Flyway, and SQL Server migrations.

**Parameters:**

- `filePath` (required): Absolute or relative path to the `.sql` file to execute
- `useTransaction` (optional): Wrap execution in a transaction (default: true). If any statement fails, all changes are rolled back.
- `stopOnError` (optional): Stop execution on first error (default: true). If false, continues with remaining statements and collects all errors.
- `stripPatterns` (optional): Array of patterns to remove from SQL before execution. Useful for stripping tool-specific delimiters (e.g., Liquibase's `/`, SQL Server's `GO`).
- `stripAsRegex` (optional): If true, treat `stripPatterns` as regular expressions; if false, as literal strings (default: false).
- `validateOnly` (optional): If true, parse and validate the file without executing (default: false). Returns a preview of all statements.

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
- `validateOnly`: (When validateOnly=true) Set to true
- `preview`: (When validateOnly=true) Array of statement previews:
  - `index`: Statement index (1-based)
  - `lineNumber`: Line number in the file
  - `sql`: The SQL statement (truncated to 200 chars)
  - `type`: Detected statement type (SELECT, INSERT, UPDATE, DELETE, CREATE, etc.)

**Limits:** Max file size: 50MB. Supports PostgreSQL-specific syntax including dollar-quoted strings and block comments.

**Examples:**

```
# Preview a file without executing
execute_sql_file({ filePath: "/path/to/migration.sql", validateOnly: true })

# Strip Liquibase delimiters (literal "/" on its own line)
execute_sql_file({ filePath: "/path/to/liquibase.sql", stripPatterns: ["/"] })

# Strip SQL Server GO statements (regex pattern)
execute_sql_file({
  filePath: "/path/to/sqlserver.sql",
  stripPatterns: ["^\\s*GO\\s*$"],
  stripAsRegex: true
})

# Strip multiple patterns
execute_sql_file({
  filePath: "/path/to/migration.sql",
  stripPatterns: ["/", "GO", "\\"]
})
```

#### `preview_sql_file`

Preview a SQL file without executing it. Similar to `mutation_preview` but for SQL files. Shows statement counts by type and warnings for potentially dangerous operations. Use this before `execute_sql_file` to understand what a migration will do.

**Parameters:**

- `filePath` (required): Absolute or relative path to the `.sql` file to preview
- `stripPatterns` (optional): Patterns to strip from SQL before parsing (same as execute_sql_file)
- `stripAsRegex` (optional): If true, treat patterns as regex (default: false)
- `maxStatements` (optional): Maximum statements to show in preview (default: 20, max: 100)

**Returns:**

- `filePath`: Resolved file path
- `fileSize`: File size in bytes
- `fileSizeFormatted`: Human-readable file size (e.g., "15.2 KB")
- `totalStatements`: Total executable statements in the file
- `statementsByType`: Breakdown by statement type (e.g., `{ "CREATE": 5, "INSERT": 10, "ALTER": 2 }`)
- `statements`: Array of statement previews (up to maxStatements):
  - `index`: Statement number (1-based)
  - `lineNumber`: Line number in file
  - `sql`: Statement SQL (truncated to 300 chars)
  - `type`: Statement type (SELECT, INSERT, CREATE, etc.)
- `warnings`: Array of warnings for dangerous operations:
  - DROP statements
  - TRUNCATE statements
  - DELETE/UPDATE without WHERE clause
- `summary`: Human-readable summary (e.g., "File contains 17 statements: 10 INSERT, 5 CREATE, 2 ALTER")

**Example:**

```
preview_sql_file({ filePath: "/path/to/migration.sql" })
// Returns:
// {
//   "filePath": "/path/to/migration.sql",
//   "fileSize": 15234,
//   "fileSizeFormatted": "14.9 KB",
//   "totalStatements": 17,
//   "statementsByType": { "CREATE": 5, "INSERT": 10, "ALTER": 2 },
//   "statements": [...],
//   "warnings": ["Statement 15 (line 142): DROP statement detected - will permanently remove database object"],
//   "summary": "File contains 17 statements: 10 INSERT, 5 CREATE, 2 ALTER"
// }
```

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

#### `mutation_dry_run`

**Transaction-based dry-run for mutations.** Actually executes the INSERT/UPDATE/DELETE within a transaction, captures **REAL** results, then ROLLBACK so nothing persists. More accurate than `mutation_preview` because it catches actual constraint violations, trigger effects, and exact row counts.

**Non-Rollbackable Operations:** Statements containing explicit `NEXTVAL()` or `SETVAL()` are **skipped** to prevent sequence values from being permanently consumed. For skipped statements, an `EXPLAIN` query plan is provided instead.

**Parameters:**

- `sql` (required): The INSERT, UPDATE, or DELETE statement to dry-run
- `sampleSize` (optional): Number of sample rows to return (default: 10, max: 20)

**Returns:**

- `mutationType`: Type of mutation (INSERT, UPDATE, DELETE)
- `success`: Whether the dry-run executed successfully
- `skipped`: If `true`, statement was skipped (contains non-rollbackable operation)
- `skipReason`: Why the statement was skipped
- `rowsAffected`: **Actual** number of rows that would be affected
- `beforeRows`: Sample of rows before the change (for UPDATE/DELETE)
- `affectedRows`: Sample of rows after the change (for INSERT/UPDATE) or deleted rows
- `targetTable`: The table being modified
- `whereClause`: The WHERE clause (if present)
- `executionTimeMs`: Execution time in milliseconds
- `error`: Detailed PostgreSQL error information if failed:
  - `message`: Error message
  - `code`: PostgreSQL error code (e.g., '23505' for unique violation)
  - `detail`: Detailed error description
  - `hint`: Hint for fixing the error
  - `constraint`: Constraint name that caused the error
  - `table`, `column`, `schema`: Related database objects
- `nonRollbackableWarnings`: Warnings about side effects:
  - `operation`: Type of operation (SEQUENCE, VACUUM, etc.)
  - `message`: Warning message
  - `mustSkip`: If `true`, operation was skipped; if `false`, just a warning
- `warnings`: General warnings (e.g., no WHERE clause)
- `explainPlan`: Query plan from EXPLAIN (for skipped DML statements with NEXTVAL/SETVAL)

**Example:**

```
mutation_dry_run({ sql: "INSERT INTO users (email) VALUES ('test@test.com')" })
// On success: { success: true, mutationType: "INSERT", rowsAffected: 1, affectedRows: [{id: 5, email: "test@test.com"}] }
// On failure: { success: false, error: { code: "23505", constraint: "users_email_key", detail: "Key already exists" } }

// With explicit NEXTVAL (skipped):
mutation_dry_run({ sql: "INSERT INTO users (id) VALUES (nextval('users_id_seq'))" })
// Returns: { success: true, skipped: true, skipReason: "NEXTVAL increments sequence...", explainPlan: [...] }
```

#### `dry_run_sql_file`

**Transaction-based dry-run for SQL files.** Actually executes ALL statements within a transaction, captures **REAL** results for each statement (row counts, errors with line numbers, constraint violations), then ROLLBACK so nothing persists. Perfect for testing migrations before deploying.

**Non-Rollbackable Operations:** The following operations are automatically **skipped** (not executed):

- **VACUUM, CLUSTER, REINDEX CONCURRENTLY**: Cannot run inside a transaction
- **CREATE INDEX CONCURRENTLY**: Cannot run inside a transaction
- **CREATE/DROP DATABASE**: Cannot run inside a transaction
- **NEXTVAL(), SETVAL()**: Would permanently consume sequence values

For skipped DML statements (INSERT/UPDATE/DELETE/SELECT with NEXTVAL/SETVAL), an `EXPLAIN` query plan is provided so you can still see what the query would do.

**Parameters:**

- `filePath` (required): Absolute or relative path to the `.sql` file
- `stripPatterns` (optional): Patterns to strip from SQL before execution (e.g., `["/"]` for Liquibase)
- `stripAsRegex` (optional): If true, treat patterns as regex (default: false)
- `maxStatements` (optional): Maximum statements to include in results (default: 50, max: 200)
- `stopOnError` (optional): Stop on first error (default: false - continues to show ALL errors)

**Returns:**

- `success`: Whether all statements executed successfully (skipped statements don't count as failures)
- `filePath`: Resolved file path
- `fileSize`: File size in bytes
- `fileSizeFormatted`: Human-readable file size
- `totalStatements`: Total statements in file
- `successCount`: Number of successful statements
- `failureCount`: Number of failed statements
- `skippedCount`: Number of skipped statements (non-rollbackable operations)
- `totalRowsAffected`: Total rows affected across all statements
- `statementsByType`: Breakdown by statement type (e.g., `{"CREATE": 5, "INSERT": 10}`)
- `executionTimeMs`: Total execution time
- `statementResults`: Array of results for each statement:
  - `index`: Statement number (1-based)
  - `lineNumber`: Line number in file
  - `sql`: The SQL statement (truncated)
  - `type`: Statement type (SELECT, INSERT, CREATE, etc.)
  - `success`: Whether statement succeeded
  - `skipped`: If `true`, statement was skipped (non-rollbackable operation)
  - `skipReason`: Why the statement was skipped
  - `rowCount`: Rows affected/returned
  - `rows`: Sample rows (for SELECT or RETURNING)
  - `executionTimeMs`: Statement execution time
  - `error`: Detailed PostgreSQL error if failed (same fields as `mutation_dry_run`)
  - `warnings`: Warnings for this statement
  - `explainPlan`: Query plan from EXPLAIN (for skipped DML statements)
- `nonRollbackableWarnings`: Warnings about operations that can't be fully rolled back:
  - `operation`: Type (SEQUENCE, VACUUM, CLUSTER, etc.)
  - `message`: Warning message
  - `mustSkip`: If `true`, operation was skipped; if `false`, just a warning
  - `statementIndex`, `lineNumber`: Location in file
- `summary`: Human-readable summary
- `rolledBack`: Always `true` - confirms changes were rolled back

**Example:**

```
dry_run_sql_file({ filePath: "/path/to/migration.sql", stripPatterns: ["/"] })
// Returns:
// {
//   "success": false,
//   "totalStatements": 15,
//   "successCount": 12,
//   "failureCount": 2,
//   "skippedCount": 1,
//   "statementResults": [
//     { "index": 1, "lineNumber": 1, "type": "CREATE", "success": true },
//     { "index": 5, "lineNumber": 23, "type": "INSERT", "success": false,
//       "error": { "code": "23505", "constraint": "users_pkey", "detail": "Key already exists" } },
//     { "index": 8, "lineNumber": 45, "type": "SELECT", "success": true, "skipped": true,
//       "skipReason": "NEXTVAL increments sequence...", "explainPlan": [...] },
//     ...
//   ],
//   "nonRollbackableWarnings": [
//     { "operation": "SEQUENCE", "message": "INSERT may consume sequence values...", "mustSkip": false },
//     { "operation": "SEQUENCE", "message": "NEXTVAL increments sequence...", "mustSkip": true }
//   ],
//   "summary": "Dry-run of 15 statements: 12 succeeded, 2 failed, 1 skipped (non-rollbackable). All changes rolled back.",
//   "rolledBack": true
// }
```

**When to use `dry_run_sql_file` vs `preview_sql_file`:**

| Feature                          | `preview_sql_file`  | `dry_run_sql_file`               |
| -------------------------------- | ------------------- | -------------------------------- |
| Speed                            | Fast (just parsing) | Slower (actual execution)        |
| Detects syntax errors            | Basic               | **Actual PostgreSQL errors**     |
| Detects constraint violations    | No                  | **Yes**                          |
| Detects trigger effects          | No                  | **Yes**                          |
| Accurate row counts              | No (estimates)      | **Yes (actual)**                 |
| Shows error details              | No                  | **Yes (code, constraint, hint)** |
| Consumes sequences               | No                  | **No (NEXTVAL/SETVAL skipped)**  |
| Shows query plan for skipped ops | N/A                 | **Yes (EXPLAIN)**                |

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

### Transaction Control

#### `begin_transaction`

Start a new database transaction. Returns a transactionId to use with `execute_sql`, `commit_transaction`, or `rollback_transaction`.

**Parameters:** None

**Returns:**

- `transactionId`: Unique ID for this transaction
- `status`: "started"
- `message`: Instructions for using the transaction

#### `commit_transaction`

Commit an active transaction, making all changes permanent.

**Parameters:**

- `transactionId` (required): The transaction ID returned by `begin_transaction`

#### `rollback_transaction`

Rollback an active transaction, undoing all changes made within it.

**Parameters:**

- `transactionId` (required): The transaction ID returned by `begin_transaction`

**Example - Transaction Usage:**

```
1. Call begin_transaction to get a transactionId
2. Call execute_sql with transactionId for each statement
3. Call commit_transaction to save changes, OR rollback_transaction to undo
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

### Connection Context in Responses

All tool responses include a `connection` object showing which server, database, and schema the operation ran on:

```json
{
  "rows": [...],
  "connection": {
    "server": "production",
    "database": "myapp",
    "schema": "public"
  }
}
```

### Multi-Statement Execution

Execute multiple SQL statements in a single call using `allowMultipleStatements: true`:

```
execute_sql({
  sql: "INSERT INTO logs VALUES (1); INSERT INTO logs VALUES (2); SELECT * FROM logs;",
  allowMultipleStatements: true
})
```

Returns results for each statement with line numbers for easy debugging.

### Transaction Support

Explicit transaction control for atomic multi-statement operations:

```
1. begin_transaction() → returns transactionId
2. execute_sql({ sql: "UPDATE ...", transactionId: "..." })
3. execute_sql({ sql: "INSERT ...", transactionId: "..." })
4. commit_transaction({ transactionId: "..." }) OR rollback_transaction({ transactionId: "..." })
```

### Line Number Tracking

When `execute_sql_file` or multi-statement execution encounters errors, line numbers are included to help locate issues:

```json
{
  "errors": [
    {
      "statementIndex": 5,
      "lineNumber": 42,
      "sql": "INSERT INTO...",
      "error": "syntax error at or near..."
    }
  ]
}
```

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
