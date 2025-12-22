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

Set the `POSTGRES_SERVERS` environment variable with a JSON object containing your server configurations:

```bash
export POSTGRES_SERVERS='{
  "dev": {
    "host": "pgbouncer-server-devdb.elb.us-east-1.amazonaws.com",
    "port": "5432",
    "username": "your_username",
    "password": "your_password"
  },
  "staging": {
    "host": "pgbouncer-server-stagingdb.elb.us-east-1.amazonaws.com",
    "port": "5432",
    "username": "your_username",
    "password": "your_password"
  },
  "production": {
    "host": "pgbouncer-server-proddb.elb.us-east-1.amazonaws.com",
    "port": "5432",
    "username": "your_username",
    "password": "your_password"
  }
}'
```

### Claude Desktop Configuration

Add the server to your Claude Desktop MCP configuration (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "postgres": {
      "command": "npx",
      "args": ["postgres-mcp-server"],
      "env": {
        "POSTGRES_SERVERS": "{\"dev\":{\"host\":\"your-host.com\",\"port\":\"5432\",\"username\":\"user\",\"password\":\"pass\"}}"
      }
    }
  }
}
```

## Available Tools

### Server & Database Management

#### `list_servers_and_dbs`

Lists all configured PostgreSQL servers and their databases.

**Parameters:**

- `filter` (optional): Filter servers and databases by name
- `includeSystemDbs` (optional): Include system databases (template0, template1)
- `fetchDatabases` (optional): Fetch list of databases from connected server

#### `switch_server_db`

Switch to a different PostgreSQL server and optionally a specific database.

**Parameters:**

- `server` (required): Name of the server to connect to
- `database` (optional): Name of the database to connect to

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

Executes SQL statements on the database. Read-only mode prevents write operations.

**Parameters:**

- `sql` (required): SQL statement to execute
- `maxRows` (optional): Maximum rows to return directly (default: 1000)

**Note:** Large outputs are automatically written to a temp file, and the file path is returned. This prevents token wastage when dealing with large result sets.

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
1. Use list_servers_and_dbs to see available servers
2. Use switch_server_db with server="dev" to connect
3. Use list_servers_and_dbs with fetchDatabases=true to see databases
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

## Security

- By default, the server runs in **read-only mode**, preventing any write operations (INSERT, UPDATE, DELETE, DROP, etc.)
- Credentials are managed via environment variables
- No credentials are logged or exposed through the MCP interface

## Requirements

- Node.js 18.0.0 or higher
- PostgreSQL 11 or higher
- Optional: `pg_stat_statements` extension for query performance analysis
- Optional: `hypopg` extension for hypothetical index simulation

## License

MIT
