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
