export interface ServerConfig {
  host: string;
  port: string;
  username: string;
  password: string;
  defaultDatabase?: string;
  defaultSchema?: string;
  isDefault?: boolean;
  ssl?: boolean | 'require' | 'prefer' | 'allow' | 'disable' | {
    rejectUnauthorized?: boolean;
    ca?: string;
    cert?: string;
    key?: string;
  };
  /**
   * AI context/guidance for this server. Can include:
   * - Server purpose (production, staging, development)
   * - Dos and don'ts (e.g., "Read-only queries preferred", "Avoid large table scans")
   * - Database-specific hints (e.g., "Main user data in 'users' schema")
   * - Any instructions to help AI agents use this server effectively
   */
  context?: string;
}

export interface ServersConfig {
  [serverName: string]: ServerConfig;
}

export interface ConnectionState {
  currentServer: string | null;
  currentDatabase: string | null;
  currentSchema: string | null;
}

export interface ConnectionInfo {
  isConnected: boolean;
  server: string | null;
  database: string | null;
  schema: string | null;
  accessMode: 'full' | 'readonly';
  /** AI context/guidance for the current server */
  context?: string;
  /** Current database user name */
  user?: string;
}

/**
 * Connection override parameters for one-time execution on a different server/database/schema.
 * When specified, the tool will temporarily connect to the specified target without
 * changing the main connection. If not specified, uses the current connection.
 */
export interface ConnectionOverride {
  /**
   * Target server name. Must match a configured server.
   * If not specified, uses the currently connected server.
   */
  server?: string;
  /**
   * Target database name.
   * If not specified, uses the server's default database or current database.
   */
  database?: string;
  /**
   * Target schema name.
   * If not specified, uses the server's default schema or 'public'.
   */
  schema?: string;
}

export interface DatabaseInfo {
  name: string;
  owner: string;
  encoding: string;
  size: string;
}

export interface SchemaInfo {
  schema_name: string;
  owner: string;
}

export interface TableInfo {
  name: string;
  type: 'table' | 'view' | 'sequence' | 'extension';
  owner: string;
  schema: string;
}

/**
 * Paginated result wrapper for list operations.
 * Provides total count, pagination metadata, and the actual items.
 */
export interface PaginatedResult<T> {
  /** The items in the current page */
  items: T[];
  /** Total number of items across all pages */
  totalCount: number;
  /** Current page offset (0-based) */
  offset: number;
  /** Maximum items per page */
  limit: number;
  /** Whether there are more items after this page */
  hasMore: boolean;
}

export interface ColumnInfo {
  column_name: string;
  data_type: string;
  is_nullable: string;
  column_default: string | null;
  character_maximum_length: number | null;
}

export interface ConstraintInfo {
  constraint_name: string;
  constraint_type: string;
  table_name: string;
  column_name: string;
  foreign_table_name: string | null;
  foreign_column_name: string | null;
}

export interface IndexInfo {
  index_name: string;
  index_definition: string;
  is_unique: boolean;
  is_primary: boolean;
}

export interface QueryPlan {
  plan: object | string;
  planning_time?: number;
  execution_time?: number;
}

export interface SlowQuery {
  query: string;
  calls: number;
  total_time: number;
  mean_time: number;
  rows: number;
}

export interface IndexRecommendation {
  table: string;
  columns: string[];
  index_type: string;
  reason: string;
  estimated_improvement: string;
}

export interface HealthCheckResult {
  category: string;
  status: 'healthy' | 'warning' | 'critical';
  message: string;
  details?: object;
}

export interface ExecuteSqlResult {
  rows: any[];
  rowCount: number;
  fields: string[];
  outputFile?: string;
  truncated?: boolean;
  executionTimeMs?: number;
  offset?: number;
  hasMore?: boolean;
  schemaHint?: SchemaHint;
}

/**
 * Schema hint for tables involved in a query
 */
export interface SchemaHint {
  tables: TableSchemaHint[];
}

export interface TableSchemaHint {
  schema: string;
  table: string;
  columns: ColumnHint[];
  primaryKey?: string[];
  foreignKeys?: ForeignKeyHint[];
  rowCountEstimate?: number;
}

export interface ColumnHint {
  name: string;
  type: string;
  nullable: boolean;
}

export interface ForeignKeyHint {
  columns: string[];
  referencedTable: string;
  referencedColumns: string[];
}

/**
 * Result of mutation preview (dry-run for INSERT/UPDATE/DELETE)
 */
export interface MutationPreviewResult {
  mutationType: 'INSERT' | 'UPDATE' | 'DELETE' | 'UNKNOWN';
  estimatedRowsAffected: number;
  sampleAffectedRows: any[];
  warning?: string;
  targetTable?: string;
  whereClause?: string;
}

/**
 * Single query in a batch
 */
export interface BatchQuery {
  name: string;
  sql: string;
  params?: any[];
}

/**
 * Result of a single query in a batch
 */
export interface BatchQueryResult {
  success: boolean;
  rows?: any[];
  rowCount?: number;
  error?: string;
  executionTimeMs: number;
}

/**
 * Result of batch execution
 */
export interface BatchExecuteResult {
  totalQueries: number;
  successCount: number;
  failureCount: number;
  totalExecutionTimeMs: number;
  results: { [name: string]: BatchQueryResult };
}

/**
 * Connection context included in tool responses
 */
export interface ConnectionContext {
  server: string | null;
  database: string | null;
  schema: string | null;
}

/**
 * Result of multi-statement execution
 */
export interface MultiStatementResult {
  statementIndex: number;
  sql: string;
  lineNumber: number;
  success: boolean;
  rows?: any[];
  rowCount?: number;
  error?: string;
}

/**
 * Extended execute_sql result with multi-statement support
 */
export interface ExecuteSqlMultiResult {
  results: MultiStatementResult[];
  totalStatements: number;
  successCount: number;
  failureCount: number;
  executionTimeMs: number;
  schemaHint?: SchemaHint;
}

/**
 * Statement with line number tracking
 */
export interface ParsedStatement {
  sql: string;
  lineNumber: number;
}

/**
 * Transaction info
 */
export interface TransactionInfo {
  transactionId: string;
  name?: string;
  server: string;
  database: string;
  schema: string;
  startedAt: Date;
}

/**
 * Transaction result
 */
export interface TransactionResult {
  transactionId: string;
  status: 'started' | 'committed' | 'rolled_back';
  message: string;
}

/**
 * Detailed error information for dry-run operations
 * Contains all PostgreSQL error details to help AI quickly identify and fix issues
 */
export interface DryRunError {
  /** Error message from PostgreSQL */
  message: string;
  /** PostgreSQL error code (e.g., '23505' for unique violation) */
  code?: string;
  /** Error severity (ERROR, FATAL, PANIC, WARNING, NOTICE) */
  severity?: string;
  /** Detailed error description */
  detail?: string;
  /** Hint for fixing the error */
  hint?: string;
  /** Character position in SQL where error occurred */
  position?: number;
  /** Internal query position if error in PL/pgSQL */
  internalPosition?: number;
  /** Internal query text */
  internalQuery?: string;
  /** Error context/stack trace from PL/pgSQL */
  where?: string;
  /** Schema name related to error */
  schema?: string;
  /** Table name related to error */
  table?: string;
  /** Column name related to error */
  column?: string;
  /** Data type related to error */
  dataType?: string;
  /** Constraint name that caused error */
  constraint?: string;
  /** File in PostgreSQL source where error originated */
  file?: string;
  /** Line in PostgreSQL source */
  line?: string;
  /** Routine that generated error */
  routine?: string;
}

/**
 * Result of executing a single statement in dry-run mode
 */
export interface DryRunStatementResult {
  /** Statement index (1-based) */
  index: number;
  /** Line number in source file/input */
  lineNumber: number;
  /** The SQL statement (may be truncated) */
  sql: string;
  /** Detected statement type (SELECT, INSERT, UPDATE, DELETE, CREATE, etc.) */
  type: string;
  /** Whether the statement executed successfully */
  success: boolean;
  /** Whether the statement was skipped (non-rollbackable operation) */
  skipped?: boolean;
  /** Reason for skipping the statement */
  skipReason?: string;
  /** Number of rows affected (for DML) or returned (for SELECT) */
  rowCount?: number;
  /** Sample of affected/returned rows */
  rows?: any[];
  /** Execution time in milliseconds */
  executionTimeMs?: number;
  /** Error details if statement failed */
  error?: DryRunError;
  /** Warnings generated during execution */
  warnings?: string[];
  /** Query plan from EXPLAIN (for skipped DML statements) */
  explainPlan?: object[];
}

/**
 * Operations that cannot be fully rolled back or have side effects
 */
export interface NonRollbackableWarning {
  /** Type of operation */
  operation: 'SEQUENCE' | 'VACUUM' | 'CLUSTER' | 'REINDEX_CONCURRENTLY' |
             'CREATE_INDEX_CONCURRENTLY' | 'CREATE_DATABASE' | 'DROP_DATABASE' |
             'NOTIFY' | 'LISTEN' | 'UNLISTEN' | 'DISCARD' | 'LOAD';
  /** Warning message explaining the limitation */
  message: string;
  /** Statement index (1-based) if applicable */
  statementIndex?: number;
  /** Line number if applicable */
  lineNumber?: number;
  /** Whether the operation must be skipped (true) or is just a warning (false) */
  mustSkip?: boolean;
}

/**
 * Enhanced mutation preview result with actual dry-run execution
 */
export interface MutationDryRunResult {
  /** Type of mutation */
  mutationType: 'INSERT' | 'UPDATE' | 'DELETE' | 'UNKNOWN';
  /** Whether the dry-run executed successfully */
  success: boolean;
  /** Whether the statement was skipped (contains non-rollbackable operation) */
  skipped?: boolean;
  /** Reason for skipping the statement */
  skipReason?: string;
  /** Actual number of rows that would be affected */
  rowsAffected: number;
  /** Sample of rows before the change (for UPDATE/DELETE) */
  beforeRows?: any[];
  /** Sample of rows after the change (for INSERT/UPDATE) or deleted rows (for DELETE) */
  affectedRows?: any[];
  /** Target table name */
  targetTable?: string;
  /** WHERE clause if present */
  whereClause?: string;
  /** Execution time in milliseconds */
  executionTimeMs?: number;
  /** Error details if execution failed */
  error?: DryRunError;
  /** Warnings about non-rollbackable side effects */
  nonRollbackableWarnings?: NonRollbackableWarning[];
  /** General warnings */
  warnings?: string[];
  /** Query plan from EXPLAIN (for skipped statements with NEXTVAL/SETVAL) */
  explainPlan?: object[];
}

/**
 * Result of dry-run execution of a SQL file
 */
export interface SqlFileDryRunResult {
  /** Whether all statements executed successfully */
  success: boolean;
  /** Resolved file path */
  filePath: string;
  /** File size in bytes */
  fileSize: number;
  /** Human-readable file size */
  fileSizeFormatted: string;
  /** Total number of statements in file */
  totalStatements: number;
  /** Number of successfully executed statements */
  successCount: number;
  /** Number of failed statements */
  failureCount: number;
  /** Number of skipped statements (non-rollbackable operations) */
  skippedCount: number;
  /** Total rows affected across all statements */
  totalRowsAffected: number;
  /** Breakdown of statements by type */
  statementsByType: { [type: string]: number };
  /** Total execution time in milliseconds */
  executionTimeMs: number;
  /** Results for each statement */
  statementResults: DryRunStatementResult[];
  /** Warnings about non-rollbackable operations */
  nonRollbackableWarnings: NonRollbackableWarning[];
  /** Summary message */
  summary: string;
  /** Note that changes were rolled back */
  rolledBack: boolean;
}
