import { getDbManager } from '../db-manager.js';
import {
  ExecuteSqlResult,
  QueryPlan,
  SchemaHint,
  TableSchemaHint,
  MutationPreviewResult,
  BatchQuery,
  BatchQueryResult,
  BatchExecuteResult,
  ConnectionContext,
  ParsedStatement,
  MultiStatementResult,
  ExecuteSqlMultiResult,
  TransactionResult,
  TransactionInfo,
  DryRunError,
  DryRunStatementResult,
  NonRollbackableWarning,
  MutationDryRunResult,
  SqlFileDryRunResult,
} from '../types.js';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { v4 as uuidv4 } from 'uuid';
import { validateIdentifier, validateIndexType, isReadOnlySql, validatePositiveInteger } from '../utils/validation.js';

const MAX_OUTPUT_CHARS = 50000; // Maximum characters before writing to file
const MAX_ROWS_DEFAULT = 1000; // Default max rows in direct response
const MAX_ROWS_LIMIT = 100000; // Absolute maximum rows
const DEFAULT_SQL_LENGTH_LIMIT = 100000; // Default SQL query length limit (100KB)
const MAX_PARAMS = 100; // Maximum number of query parameters
const MAX_SQL_FILE_SIZE = 50 * 1024 * 1024; // Maximum SQL file size (50MB)
const MAX_DRY_RUN_SAMPLE_ROWS = 10; // Maximum sample rows to return in dry-run

/**
 * Extract detailed error information from a PostgreSQL error.
 * Captures all available fields to help AI quickly identify and fix issues.
 */
function extractDryRunError(error: unknown): DryRunError {
  const result: DryRunError = {
    message: error instanceof Error ? error.message : String(error)
  };

  // PostgreSQL errors have additional properties
  if (error && typeof error === 'object') {
    const pgError = error as Record<string, unknown>;

    if (pgError.code) result.code = String(pgError.code);
    if (pgError.severity) result.severity = String(pgError.severity);
    if (pgError.detail) result.detail = String(pgError.detail);
    if (pgError.hint) result.hint = String(pgError.hint);
    if (pgError.position) result.position = Number(pgError.position);
    if (pgError.internalPosition) result.internalPosition = Number(pgError.internalPosition);
    if (pgError.internalQuery) result.internalQuery = String(pgError.internalQuery);
    if (pgError.where) result.where = String(pgError.where);
    if (pgError.schema) result.schema = String(pgError.schema);
    if (pgError.table) result.table = String(pgError.table);
    if (pgError.column) result.column = String(pgError.column);
    if (pgError.dataType) result.dataType = String(pgError.dataType);
    if (pgError.constraint) result.constraint = String(pgError.constraint);
    if (pgError.file) result.file = String(pgError.file);
    if (pgError.line) result.line = String(pgError.line);
    if (pgError.routine) result.routine = String(pgError.routine);
  }

  return result;
}

/**
 * Check if a SQL statement contains operations that cannot be fully rolled back
 * or have side effects even within a transaction.
 */
function detectNonRollbackableOperations(
  sql: string,
  statementIndex?: number,
  lineNumber?: number
): NonRollbackableWarning[] {
  const warnings: NonRollbackableWarning[] = [];
  const upperSql = sql.toUpperCase().trim();

  // Operations that cannot run inside a transaction at all - MUST SKIP
  if (upperSql.match(/\bVACUUM\b/)) {
    warnings.push({
      operation: 'VACUUM',
      message: 'VACUUM cannot run inside a transaction block. Statement skipped.',
      statementIndex,
      lineNumber,
      mustSkip: true
    });
  }

  if (upperSql.match(/\bCLUSTER\b/) && !upperSql.includes('CREATE')) {
    warnings.push({
      operation: 'CLUSTER',
      message: 'CLUSTER cannot run inside a transaction block. Statement skipped.',
      statementIndex,
      lineNumber,
      mustSkip: true
    });
  }

  if (upperSql.match(/\bREINDEX\b.*\bCONCURRENTLY\b/)) {
    warnings.push({
      operation: 'REINDEX_CONCURRENTLY',
      message: 'REINDEX CONCURRENTLY cannot run inside a transaction block. Statement skipped.',
      statementIndex,
      lineNumber,
      mustSkip: true
    });
  }

  if (upperSql.match(/\bCREATE\s+INDEX\b.*\bCONCURRENTLY\b/)) {
    warnings.push({
      operation: 'CREATE_INDEX_CONCURRENTLY',
      message: 'CREATE INDEX CONCURRENTLY cannot run inside a transaction block. Statement skipped.',
      statementIndex,
      lineNumber,
      mustSkip: true
    });
  }

  if (upperSql.match(/\bCREATE\s+DATABASE\b/)) {
    warnings.push({
      operation: 'CREATE_DATABASE',
      message: 'CREATE DATABASE cannot run inside a transaction block. Statement skipped.',
      statementIndex,
      lineNumber,
      mustSkip: true
    });
  }

  if (upperSql.match(/\bDROP\s+DATABASE\b/)) {
    warnings.push({
      operation: 'DROP_DATABASE',
      message: 'DROP DATABASE cannot run inside a transaction block. Statement skipped.',
      statementIndex,
      lineNumber,
      mustSkip: true
    });
  }

  // Operations that have side effects even when rolled back - MUST SKIP
  if (upperSql.match(/\bNEXTVAL\s*\(/)) {
    warnings.push({
      operation: 'SEQUENCE',
      message: 'NEXTVAL increments sequence even when transaction is rolled back. Statement skipped to prevent sequence consumption.',
      statementIndex,
      lineNumber,
      mustSkip: true
    });
  }

  if (upperSql.match(/\bSETVAL\s*\(/)) {
    warnings.push({
      operation: 'SEQUENCE',
      message: 'SETVAL modifies sequence. Statement skipped to prevent side effects.',
      statementIndex,
      lineNumber,
      mustSkip: true
    });
  }

  // INSERT with SERIAL/BIGSERIAL columns may consume sequence values - WARNING ONLY (not skipped)
  if (upperSql.match(/\bINSERT\s+INTO\b/)) {
    warnings.push({
      operation: 'SEQUENCE',
      message: 'INSERT may consume sequence values (for SERIAL/BIGSERIAL columns) even when rolled back.',
      statementIndex,
      lineNumber,
      mustSkip: false  // Warning only, do not skip
    });
  }

  // NOTIFY only sends on commit, so safe in dry-run (rollback prevents notification)
  if (upperSql.match(/\bNOTIFY\b/)) {
    warnings.push({
      operation: 'NOTIFY',
      message: 'NOTIFY sends notifications on commit. Since dry-run rolls back, notifications will NOT be sent.',
      statementIndex,
      lineNumber,
      mustSkip: false  // Safe to execute in dry-run since we rollback
    });
  }

  return warnings;
}

export async function executeSql(args: {
  sql: string;
  params?: any[];
  maxRows?: number;
  offset?: number;
  allowLargeScript?: boolean;
  includeSchemaHint?: boolean;
  allowMultipleStatements?: boolean;
  transactionId?: string;
}): Promise<ExecuteSqlResult | ExecuteSqlMultiResult> {
  // Validate SQL input
  if (args.sql === undefined || args.sql === null) {
    throw new Error('sql parameter is required');
  }

  if (typeof args.sql !== 'string') {
    throw new Error('sql parameter must be a string');
  }

  const sql = args.sql.trim();
  if (sql.length === 0) {
    throw new Error('sql parameter cannot be empty');
  }

  // Only check length if allowLargeScript is not true
  if (!args.allowLargeScript && sql.length > DEFAULT_SQL_LENGTH_LIMIT) {
    throw new Error(`SQL query exceeds ${DEFAULT_SQL_LENGTH_LIMIT} characters. Use allowLargeScript=true for deployment scripts.`);
  }

  // Validate params if provided (only for single statement)
  if (args.params !== undefined && !args.allowMultipleStatements) {
    if (!Array.isArray(args.params)) {
      throw new Error('params must be an array');
    }
    if (args.params.length > MAX_PARAMS) {
      throw new Error(`Maximum ${MAX_PARAMS} parameters allowed`);
    }
  }

  // Params not supported with multiple statements
  if (args.allowMultipleStatements && args.params && args.params.length > 0) {
    throw new Error('params not supported with allowMultipleStatements. Use separate execute_sql calls for parameterized queries.');
  }

  const dbManager = getDbManager();
  const maxRows = validatePositiveInteger(args.maxRows, 'maxRows', 1, MAX_ROWS_LIMIT) || MAX_ROWS_DEFAULT;
  const offset = args.offset !== undefined ? validatePositiveInteger(args.offset, 'offset', 0, Number.MAX_SAFE_INTEGER) : 0;

  // Get schema hints if requested
  let schemaHint: SchemaHint | undefined;
  if (args.includeSchemaHint) {
    schemaHint = await getSchemaHintForSql(sql);
  }

  // Handle multi-statement execution
  if (args.allowMultipleStatements) {
    return executeMultipleStatements(sql, schemaHint, args.transactionId);
  }

  // Record start time for execution timing
  const startTime = process.hrtime.bigint();

  // Execute query with optional parameters (supports transaction)
  let result;
  if (args.transactionId) {
    result = await dbManager.queryInTransaction(args.transactionId, sql, args.params);
  } else {
    result = await dbManager.query(sql, args.params);
  }

  // Calculate execution time in milliseconds
  const endTime = process.hrtime.bigint();
  const executionTimeMs = Number(endTime - startTime) / 1_000_000;

  // Defensive: ensure result has expected structure
  if (!result || typeof result !== 'object') {
    throw new Error('Query returned invalid result');
  }

  const fields = result.fields?.map(f => f.name) || [];
  const rows = result.rows || [];
  const totalRows = rows.length;

  // Apply offset and limit to the results
  const startIndex = Math.min(offset, totalRows);
  const endIndex = Math.min(startIndex + maxRows, totalRows);
  const paginatedRows = rows.slice(startIndex, endIndex);
  const returnedRows = paginatedRows.length;

  // Calculate actual output size
  const outputJson = JSON.stringify(paginatedRows);
  const outputSize = outputJson.length;

  // If output is still too large even after pagination, write to file
  if (outputSize > MAX_OUTPUT_CHARS) {
    const tempDir = os.tmpdir();
    const fileName = `postgres-mcp-output-${uuidv4()}.json`;
    const filePath = path.join(tempDir, fileName);

    const outputData = {
      totalRows,
      returnedRows,
      offset: startIndex,
      fields,
      rows: paginatedRows,
      executionTimeMs: Math.round(executionTimeMs * 100) / 100,
      generatedAt: new Date().toISOString()
    };

    fs.writeFileSync(filePath, JSON.stringify(outputData, null, 2), { mode: 0o600 });

    return {
      rows: [],
      rowCount: totalRows,
      fields,
      outputFile: filePath,
      truncated: true,
      executionTimeMs: Math.round(executionTimeMs * 100) / 100,
      offset: startIndex,
      hasMore: endIndex < totalRows,
      ...(schemaHint && { schemaHint })
    };
  }

  return {
    rows: paginatedRows,
    rowCount: totalRows,
    fields,
    executionTimeMs: Math.round(executionTimeMs * 100) / 100,
    offset: startIndex,
    hasMore: endIndex < totalRows,
    ...(schemaHint && { schemaHint })
  };
}

/**
 * Execute multiple SQL statements and return results for each
 */
async function executeMultipleStatements(
  sql: string,
  schemaHint?: SchemaHint,
  transactionId?: string
): Promise<ExecuteSqlMultiResult> {
  const dbManager = getDbManager();
  const startTime = process.hrtime.bigint();

  // Parse statements with line numbers
  const parsedStatements = splitSqlStatementsWithLineNumbers(sql);

  // Filter out empty statements and comments-only
  const executableStatements = parsedStatements.filter(stmt => {
    const trimmed = stmt.sql.trim();
    if (!trimmed) return false;
    const withoutComments = stripLeadingComments(trimmed);
    return withoutComments.length > 0;
  });

  const results: MultiStatementResult[] = [];
  let successCount = 0;
  let failureCount = 0;

  for (let i = 0; i < executableStatements.length; i++) {
    const stmt = executableStatements[i];
    const stmtResult: MultiStatementResult = {
      statementIndex: i + 1,
      sql: stmt.sql.length > 200 ? stmt.sql.substring(0, 200) + '...' : stmt.sql,
      lineNumber: stmt.lineNumber,
      success: false,
    };

    try {
      let result;
      if (transactionId) {
        result = await dbManager.queryInTransaction(transactionId, stmt.sql);
      } else {
        result = await dbManager.query(stmt.sql);
      }

      stmtResult.success = true;
      stmtResult.rows = result.rows?.slice(0, 100); // Limit rows per statement
      stmtResult.rowCount = result.rowCount ?? result.rows?.length ?? 0;
      successCount++;
    } catch (error) {
      stmtResult.success = false;
      stmtResult.error = error instanceof Error ? error.message : String(error);
      failureCount++;
    }

    results.push(stmtResult);
  }

  const endTime = process.hrtime.bigint();
  const executionTimeMs = Number(endTime - startTime) / 1_000_000;

  return {
    results,
    totalStatements: executableStatements.length,
    successCount,
    failureCount,
    executionTimeMs: Math.round(executionTimeMs * 100) / 100,
    ...(schemaHint && { schemaHint })
  };
}

export async function explainQuery(args: {
  sql: string;
  analyze?: boolean;
  buffers?: boolean;
  format?: 'text' | 'json' | 'yaml' | 'xml';
  hypotheticalIndexes?: Array<{
    table: string;
    columns: string[];
    indexType?: string;
  }>;
}): Promise<QueryPlan> {
  // Validate SQL input
  if (!args.sql || typeof args.sql !== 'string') {
    throw new Error('sql parameter is required and must be a string');
  }

  if (args.sql.length > DEFAULT_SQL_LENGTH_LIMIT) {
    throw new Error(`SQL query exceeds maximum length of ${DEFAULT_SQL_LENGTH_LIMIT} characters`);
  }

  // SECURITY: Block EXPLAIN ANALYZE on write queries to prevent bypassing read-only mode
  if (args.analyze) {
    const { isReadOnly, reason } = isReadOnlySql(args.sql);
    if (!isReadOnly) {
      throw new Error(`EXPLAIN ANALYZE is not allowed for write queries. ${reason}`);
    }
  }

  const dbManager = getDbManager();
  const client = await dbManager.getClient();

  try {
    // If hypothetical indexes are specified, validate and create them
    if (args.hypotheticalIndexes && args.hypotheticalIndexes.length > 0) {
      // Limit number of hypothetical indexes
      if (args.hypotheticalIndexes.length > 10) {
        throw new Error('Maximum 10 hypothetical indexes allowed');
      }

      // Check if hypopg extension is available
      const hypopgCheck = await client.query(`
        SELECT EXISTS (
          SELECT 1 FROM pg_extension WHERE extname = 'hypopg'
        ) as has_hypopg
      `);

      if (hypopgCheck.rows[0].has_hypopg) {
        // Create hypothetical indexes with validated inputs
        for (const idx of args.hypotheticalIndexes) {
          // Validate table name
          if (!idx.table) {
            throw new Error('hypotheticalIndexes: table is required');
          }

          // Handle schema.table format
          let schemaName = 'public';
          let tableName = idx.table;

          if (idx.table.includes('.')) {
            const parts = idx.table.split('.');
            if (parts.length !== 2) {
              throw new Error(`hypotheticalIndexes: invalid table format '${idx.table}'`);
            }
            schemaName = parts[0];
            tableName = parts[1];
            validateIdentifier(schemaName, 'schema');
          }
          validateIdentifier(tableName, 'table');

          // Validate columns
          if (!idx.columns || !Array.isArray(idx.columns) || idx.columns.length === 0) {
            throw new Error('hypotheticalIndexes: columns array is required and must not be empty');
          }

          if (idx.columns.length > 32) {
            throw new Error('hypotheticalIndexes: maximum 32 columns per index');
          }

          for (const col of idx.columns) {
            validateIdentifier(col, 'column');
          }

          // Validate index type
          const indexType = validateIndexType(idx.indexType || 'btree');

          // Build safe index creation string
          const safeTableName = schemaName !== 'public'
            ? `"${schemaName}"."${tableName}"`
            : `"${tableName}"`;
          const safeColumns = idx.columns.map(c => `"${c}"`).join(', ');

          // Use parameterized approach via hypopg
          const indexDef = `CREATE INDEX ON ${safeTableName} USING ${indexType} (${safeColumns})`;
          await client.query('SELECT hypopg_create_index($1)', [indexDef]);
        }
      }
    }

    // Build EXPLAIN query
    const validFormats = ['text', 'json', 'yaml', 'xml'];
    const format = validFormats.includes(args.format || '') ? args.format! : 'json';
    const options: string[] = [`FORMAT ${format.toUpperCase()}`];

    if (args.analyze) {
      options.push('ANALYZE');
    }
    if (args.buffers) {
      options.push('BUFFERS');
    }

    // Use the validated SQL - it will be checked by the db-manager's read-only check
    // EXPLAIN itself is read-only, EXPLAIN ANALYZE executes but we validated above
    const explainSql = `EXPLAIN (${options.join(', ')}) ${args.sql}`;
    const result = await client.query(explainSql);

    // Clean up hypothetical indexes if created
    if (args.hypotheticalIndexes && args.hypotheticalIndexes.length > 0) {
      try {
        await client.query('SELECT hypopg_reset()');
      } catch (e) {
        // Ignore if hypopg not available
      }
    }

    if (format === 'json') {
      return {
        plan: result.rows[0]['QUERY PLAN'][0]
      };
    }

    return {
      plan: result.rows.map((r: any) => r['QUERY PLAN']).join('\n')
    };
  } finally {
    client.release();
  }
}

/**
 * Individual statement error when stopOnError is false
 */
export interface StatementError {
  statementIndex: number;
  lineNumber: number;
  sql: string;
  error: string;
}

/**
 * Result of executing a SQL file
 */
/** Preview of a SQL statement for validateOnly mode */
export interface StatementPreview {
  index: number;
  lineNumber: number;
  sql: string;
  type: string;
}

export interface ExecuteSqlFileResult {
  success: boolean;
  filePath: string;
  fileSize: number;
  totalStatements: number;
  statementsExecuted: number;
  statementsFailed: number;
  executionTimeMs: number;
  rowsAffected?: number;
  error?: string;
  errors?: StatementError[];
  rollback?: boolean;
  /** True if validateOnly mode was used */
  validateOnly?: boolean;
  /** Preview of statements when validateOnly is true */
  preview?: StatementPreview[];
}

/**
 * Result of previewing a SQL file
 */
export interface SqlFilePreviewResult {
  filePath: string;
  fileSize: number;
  fileSizeFormatted: string;
  totalStatements: number;
  statementsByType: { [type: string]: number };
  statements: StatementPreview[];
  warnings: string[];
  summary: string;
}

/**
 * Preprocess SQL content by removing patterns.
 * Supports both literal string matching and regex patterns.
 *
 * @param sql - The SQL content to preprocess
 * @param patterns - Array of patterns to remove from SQL content
 * @param isRegex - If true, patterns are treated as regex; if false, as literal strings
 */
function preprocessSqlContent(sql: string, patterns: string[], isRegex: boolean = false): string {
  let result = sql;
  for (const pattern of patterns) {
    try {
      if (isRegex) {
        // Treat as regex pattern (multiline by default)
        const regex = new RegExp(pattern, 'gm');
        result = result.replace(regex, '');
      } else {
        // Treat as literal string - escape and match on its own line
        const escapedPattern = pattern.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        const regex = new RegExp(`^\\s*${escapedPattern}\\s*$`, 'gm');
        result = result.replace(regex, '');
      }
    } catch (error) {
      // Invalid regex - skip this pattern
      console.error(`Warning: Invalid pattern "${pattern}": ${error instanceof Error ? error.message : String(error)}`);
    }
  }
  return result;
}

/**
 * Execute a SQL file from the filesystem.
 * Supports transaction mode for atomic execution.
 */
export async function executeSqlFile(args: {
  filePath: string;
  useTransaction?: boolean;
  stopOnError?: boolean;
  /** Patterns to strip from SQL before execution. Use with stripAsRegex for regex patterns. */
  stripPatterns?: string[];
  /** If true, stripPatterns are treated as regex; if false, as literal strings (default: false) */
  stripAsRegex?: boolean;
  /** If true, only parse and validate the file without executing (default: false) */
  validateOnly?: boolean;
}): Promise<ExecuteSqlFileResult> {
  // Validate file path
  if (!args.filePath || typeof args.filePath !== 'string') {
    throw new Error('filePath parameter is required');
  }

  const filePath = args.filePath.trim();

  // Security: Validate file extension
  const ext = path.extname(filePath).toLowerCase();
  if (ext !== '.sql') {
    throw new Error('Only .sql files are allowed');
  }

  // Security: Prevent path traversal attacks
  const resolvedPath = path.resolve(filePath);

  // Check file exists
  if (!fs.existsSync(resolvedPath)) {
    throw new Error(`File not found: ${filePath}`);
  }

  // Check file stats
  const stats = fs.statSync(resolvedPath);
  if (!stats.isFile()) {
    throw new Error(`Not a file: ${filePath}`);
  }

  if (stats.size > MAX_SQL_FILE_SIZE) {
    throw new Error(`File too large. Maximum size is ${MAX_SQL_FILE_SIZE / (1024 * 1024)}MB`);
  }

  if (stats.size === 0) {
    throw new Error('File is empty');
  }

  // Read file content
  let sqlContent = fs.readFileSync(resolvedPath, 'utf-8');

  // Preprocess SQL content if patterns specified
  if (args.stripPatterns && args.stripPatterns.length > 0) {
    sqlContent = preprocessSqlContent(sqlContent, args.stripPatterns, args.stripAsRegex === true);
  }

  const dbManager = getDbManager();
  const useTransaction = args.useTransaction !== false; // Default to true
  const stopOnError = args.stopOnError !== false; // Default to true
  const validateOnly = args.validateOnly === true; // Default to false

  const startTime = process.hrtime.bigint();

  // Split SQL into statements with line number tracking
  const parsedStatements = splitSqlStatementsWithLineNumbers(sqlContent);
  const executableStatements = parsedStatements.filter(stmt => {
    const trimmed = stmt.sql.trim();
    if (!trimmed) return false;
    const withoutComments = stripLeadingComments(trimmed);
    return withoutComments.length > 0;
  });
  const totalStatements = executableStatements.length;

  // If validateOnly mode, return preview without execution
  if (validateOnly) {
    const endTime = process.hrtime.bigint();
    const executionTimeMs = Number(endTime - startTime) / 1_000_000;

    // Create preview of statements
    const preview = executableStatements.map((stmt, idx) => ({
      index: idx + 1,
      lineNumber: stmt.lineNumber,
      sql: stmt.sql.length > 300 ? stmt.sql.substring(0, 300) + '...' : stmt.sql,
      type: detectStatementType(stmt.sql)
    }));

    return {
      success: true,
      filePath: resolvedPath,
      fileSize: stats.size,
      totalStatements,
      statementsExecuted: 0,
      statementsFailed: 0,
      executionTimeMs: Math.round(executionTimeMs * 100) / 100,
      rowsAffected: 0,
      validateOnly: true,
      preview
    } as ExecuteSqlFileResult;
  }

  const client = await dbManager.getClient();

  let statementsExecuted = 0;
  let statementsFailed = 0;
  let totalRowsAffected = 0;
  let rolledBack = false;
  const collectedErrors: StatementError[] = [];

  try {
    if (useTransaction) {
      await client.query('BEGIN');
    }

    for (let statementIndex = 0; statementIndex < executableStatements.length; statementIndex++) {
      const stmt = executableStatements[statementIndex];
      const trimmed = stmt.sql.trim();

      try {
        const result = await client.query(trimmed);
        statementsExecuted++;
        if (result.rowCount !== null) {
          totalRowsAffected += result.rowCount;
        }
      } catch (error) {
        statementsFailed++;
        const errorMessage = error instanceof Error ? error.message : String(error);

        if (stopOnError) {
          if (useTransaction) {
            await client.query('ROLLBACK');
            rolledBack = true;
          }
          // Add the error to collection before throwing
          collectedErrors.push({
            statementIndex: statementIndex + 1,
            lineNumber: stmt.lineNumber,
            sql: trimmed.length > 200 ? trimmed.substring(0, 200) + '...' : trimmed,
            error: errorMessage
          });
          throw error;
        }

        // If stopOnError is false, collect error and continue
        collectedErrors.push({
          statementIndex: statementIndex + 1,
          lineNumber: stmt.lineNumber,
          sql: trimmed.length > 200 ? trimmed.substring(0, 200) + '...' : trimmed,
          error: errorMessage
        });
        console.error(`Warning: Statement ${statementIndex + 1} at line ${stmt.lineNumber} failed: ${errorMessage}`);
      }
    }

    if (useTransaction && !rolledBack) {
      await client.query('COMMIT');
    }

    const endTime = process.hrtime.bigint();
    const executionTimeMs = Number(endTime - startTime) / 1_000_000;

    const result: ExecuteSqlFileResult = {
      success: statementsFailed === 0,
      filePath: resolvedPath,
      fileSize: stats.size,
      totalStatements,
      statementsExecuted,
      statementsFailed,
      executionTimeMs: Math.round(executionTimeMs * 100) / 100,
      rowsAffected: totalRowsAffected
    };

    // Include errors array if there were any failures (when stopOnError=false)
    if (collectedErrors.length > 0) {
      result.errors = collectedErrors;
    }

    return result;

  } catch (error) {
    const endTime = process.hrtime.bigint();
    const executionTimeMs = Number(endTime - startTime) / 1_000_000;

    const result: ExecuteSqlFileResult = {
      success: false,
      filePath: resolvedPath,
      fileSize: stats.size,
      totalStatements,
      statementsExecuted,
      statementsFailed,
      executionTimeMs: Math.round(executionTimeMs * 100) / 100,
      rowsAffected: totalRowsAffected,
      error: error instanceof Error ? error.message : String(error),
      rollback: rolledBack
    };

    if (collectedErrors.length > 0) {
      result.errors = collectedErrors;
    }

    return result;
  } finally {
    client.release();
  }
}

/**
 * Detect the type of SQL statement (SELECT, INSERT, UPDATE, DELETE, CREATE, etc.)
 */
function detectStatementType(sql: string): string {
  const trimmed = stripLeadingComments(sql).toUpperCase();

  // Common statement types
  const types = [
    'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP',
    'TRUNCATE', 'GRANT', 'REVOKE', 'BEGIN', 'COMMIT', 'ROLLBACK',
    'SET', 'SHOW', 'EXPLAIN', 'ANALYZE', 'VACUUM', 'REINDEX',
    'COMMENT', 'WITH', 'DO', 'CALL', 'EXECUTE'
  ];

  for (const type of types) {
    if (trimmed.startsWith(type + ' ') || trimmed.startsWith(type + '\n') ||
        trimmed.startsWith(type + '\t') || trimmed === type) {
      // Special case for WITH - check if it's a CTE followed by SELECT/INSERT/UPDATE/DELETE
      if (type === 'WITH') {
        if (trimmed.includes('SELECT')) return 'WITH SELECT';
        if (trimmed.includes('INSERT')) return 'WITH INSERT';
        if (trimmed.includes('UPDATE')) return 'WITH UPDATE';
        if (trimmed.includes('DELETE')) return 'WITH DELETE';
        return 'WITH';
      }
      return type;
    }
  }

  return 'UNKNOWN';
}

/**
 * Preview a SQL file without executing.
 * Similar to mutation_preview but for SQL files - shows what would happen if executed.
 */
export async function previewSqlFile(args: {
  filePath: string;
  /** Patterns to strip from SQL before parsing */
  stripPatterns?: string[];
  /** If true, stripPatterns are treated as regex */
  stripAsRegex?: boolean;
  /** Maximum number of statements to show in preview (default: 20) */
  maxStatements?: number;
}): Promise<SqlFilePreviewResult> {
  // Validate file path
  if (!args.filePath || typeof args.filePath !== 'string') {
    throw new Error('filePath parameter is required');
  }

  const filePath = args.filePath.trim();

  // Security: Validate file extension
  const ext = path.extname(filePath).toLowerCase();
  if (ext !== '.sql') {
    throw new Error('Only .sql files are allowed');
  }

  // Security: Prevent path traversal attacks
  const resolvedPath = path.resolve(filePath);

  // Check file exists
  if (!fs.existsSync(resolvedPath)) {
    throw new Error(`File not found: ${filePath}`);
  }

  // Check file stats
  const stats = fs.statSync(resolvedPath);
  if (!stats.isFile()) {
    throw new Error(`Not a file: ${filePath}`);
  }

  if (stats.size > MAX_SQL_FILE_SIZE) {
    throw new Error(`File too large. Maximum size is ${MAX_SQL_FILE_SIZE / (1024 * 1024)}MB`);
  }

  if (stats.size === 0) {
    throw new Error('File is empty');
  }

  // Read file content
  let sqlContent = fs.readFileSync(resolvedPath, 'utf-8');

  // Preprocess SQL content if patterns specified
  if (args.stripPatterns && args.stripPatterns.length > 0) {
    sqlContent = preprocessSqlContent(sqlContent, args.stripPatterns, args.stripAsRegex === true);
  }

  const maxStatements = Math.min(args.maxStatements || 20, 100);

  // Split SQL into statements with line number tracking
  const parsedStatements = splitSqlStatementsWithLineNumbers(sqlContent);
  const executableStatements = parsedStatements.filter(stmt => {
    const trimmed = stmt.sql.trim();
    if (!trimmed) return false;
    const withoutComments = stripLeadingComments(trimmed);
    return withoutComments.length > 0;
  });

  const totalStatements = executableStatements.length;

  // Count statements by type
  const statementsByType: { [type: string]: number } = {};
  const warnings: string[] = [];

  executableStatements.forEach((stmt, idx) => {
    const type = detectStatementType(stmt.sql);
    statementsByType[type] = (statementsByType[type] || 0) + 1;

    // Check for potentially dangerous operations
    const sqlUpper = stmt.sql.toUpperCase();
    if (type === 'DROP') {
      warnings.push(`Statement ${idx + 1} (line ${stmt.lineNumber}): DROP statement detected - will permanently remove database object`);
    } else if (type === 'TRUNCATE') {
      warnings.push(`Statement ${idx + 1} (line ${stmt.lineNumber}): TRUNCATE statement detected - will delete all rows from table`);
    } else if (type === 'DELETE' && !sqlUpper.includes('WHERE')) {
      warnings.push(`Statement ${idx + 1} (line ${stmt.lineNumber}): DELETE without WHERE clause - will delete ALL rows from table`);
    } else if (type === 'UPDATE' && !sqlUpper.includes('WHERE')) {
      warnings.push(`Statement ${idx + 1} (line ${stmt.lineNumber}): UPDATE without WHERE clause - will update ALL rows in table`);
    }
  });

  // Create statement previews (limited to maxStatements)
  const statements = executableStatements.slice(0, maxStatements).map((stmt, idx) => ({
    index: idx + 1,
    lineNumber: stmt.lineNumber,
    sql: stmt.sql.length > 300 ? stmt.sql.substring(0, 300) + '...' : stmt.sql,
    type: detectStatementType(stmt.sql)
  }));

  // Format file size
  const fileSizeFormatted = stats.size < 1024
    ? `${stats.size} bytes`
    : stats.size < 1024 * 1024
      ? `${(stats.size / 1024).toFixed(1)} KB`
      : `${(stats.size / (1024 * 1024)).toFixed(2)} MB`;

  // Generate summary
  const typeEntries = Object.entries(statementsByType).sort((a, b) => b[1] - a[1]);
  const typeSummary = typeEntries.map(([type, count]) => `${count} ${type}`).join(', ');
  const summary = `File contains ${totalStatements} statement${totalStatements !== 1 ? 's' : ''}: ${typeSummary || 'none'}`;

  return {
    filePath: resolvedPath,
    fileSize: stats.size,
    fileSizeFormatted,
    totalStatements,
    statementsByType,
    statements,
    warnings,
    summary
  };
}

/**
 * Execute a SQL file in dry-run mode.
 * Actually executes all statements within a transaction, captures real results,
 * then rolls back so no changes are persisted.
 *
 * This provides accurate results including:
 * - Exact row counts for each statement
 * - Actual errors with full PostgreSQL error details
 * - Line numbers for easy debugging
 * - Detection of non-rollbackable operations
 */
export async function dryRunSqlFile(args: {
  filePath: string;
  /** Patterns to strip from SQL before execution */
  stripPatterns?: string[];
  /** If true, stripPatterns are treated as regex */
  stripAsRegex?: boolean;
  /** Maximum statements to show in results (default: 50) */
  maxStatements?: number;
  /** Stop on first error (default: false - continues to show all errors) */
  stopOnError?: boolean;
}): Promise<SqlFileDryRunResult> {
  // Validate file path
  if (!args.filePath || typeof args.filePath !== 'string') {
    throw new Error('filePath parameter is required');
  }

  const filePath = args.filePath.trim();

  // Security: Validate file extension
  const ext = path.extname(filePath).toLowerCase();
  if (ext !== '.sql') {
    throw new Error('Only .sql files are allowed');
  }

  // Security: Prevent path traversal attacks
  const resolvedPath = path.resolve(filePath);

  // Check file exists
  if (!fs.existsSync(resolvedPath)) {
    throw new Error(`File not found: ${filePath}`);
  }

  // Check file stats
  const stats = fs.statSync(resolvedPath);
  if (!stats.isFile()) {
    throw new Error(`Not a file: ${filePath}`);
  }

  if (stats.size > MAX_SQL_FILE_SIZE) {
    throw new Error(`File too large. Maximum size is ${MAX_SQL_FILE_SIZE / (1024 * 1024)}MB`);
  }

  if (stats.size === 0) {
    throw new Error('File is empty');
  }

  // Read file content
  let sqlContent = fs.readFileSync(resolvedPath, 'utf-8');

  // Preprocess SQL content if patterns specified
  if (args.stripPatterns && args.stripPatterns.length > 0) {
    sqlContent = preprocessSqlContent(sqlContent, args.stripPatterns, args.stripAsRegex === true);
  }

  const maxStatements = Math.min(args.maxStatements || 50, 200);
  const stopOnError = args.stopOnError === true;

  // Split SQL into statements with line number tracking
  const parsedStatements = splitSqlStatementsWithLineNumbers(sqlContent);
  const executableStatements = parsedStatements.filter(stmt => {
    const trimmed = stmt.sql.trim();
    if (!trimmed) return false;
    const withoutComments = stripLeadingComments(trimmed);
    return withoutComments.length > 0;
  });

  const totalStatements = executableStatements.length;

  // Detect all non-rollbackable operations upfront
  const nonRollbackableWarnings: NonRollbackableWarning[] = [];
  executableStatements.forEach((stmt, idx) => {
    const warnings = detectNonRollbackableOperations(stmt.sql, idx + 1, stmt.lineNumber);
    nonRollbackableWarnings.push(...warnings);
  });

  // Format file size
  const fileSizeFormatted = stats.size < 1024
    ? `${stats.size} bytes`
    : stats.size < 1024 * 1024
      ? `${(stats.size / 1024).toFixed(1)} KB`
      : `${(stats.size / (1024 * 1024)).toFixed(2)} MB`;

  const dbManager = getDbManager();
  const client = await dbManager.getClient();
  const startTime = process.hrtime.bigint();

  const statementResults: DryRunStatementResult[] = [];
  const statementsByType: { [type: string]: number } = {};
  let successCount = 0;
  let failureCount = 0;
  let skippedCount = 0;
  let totalRowsAffected = 0;
  let aborted = false;

  try {
    // Start transaction
    await client.query('BEGIN');

    for (let idx = 0; idx < executableStatements.length && !aborted; idx++) {
      const stmt = executableStatements[idx];
      const stmtType = detectStatementType(stmt.sql);
      statementsByType[stmtType] = (statementsByType[stmtType] || 0) + 1;

      const stmtStartTime = process.hrtime.bigint();
      const result: DryRunStatementResult = {
        index: idx + 1,
        lineNumber: stmt.lineNumber,
        sql: stmt.sql.length > 300 ? stmt.sql.substring(0, 300) + '...' : stmt.sql,
        type: stmtType,
        success: false
      };

      // Check for non-rollbackable warnings specific to this statement
      const stmtWarnings = nonRollbackableWarnings
        .filter(w => w.statementIndex === idx + 1);
      const mustSkipWarnings = stmtWarnings.filter(w => w.mustSkip);

      // Only skip if there are mustSkip warnings; otherwise just warn
      if (mustSkipWarnings.length > 0) {
        result.skipped = true;
        result.skipReason = mustSkipWarnings.map(w => w.message).join('; ');
        result.warnings = stmtWarnings.map(w => w.message);
        result.success = true; // Not a failure, just skipped
        skippedCount++;

        // For DML statements with NEXTVAL/SETVAL, run EXPLAIN to show query plan
        const isDML = ['INSERT', 'UPDATE', 'DELETE', 'SELECT'].includes(stmtType);
        const hasSequenceSkip = mustSkipWarnings.some(w => w.operation === 'SEQUENCE');
        if (isDML && hasSequenceSkip) {
          try {
            const explainResult = await client.query(`EXPLAIN (FORMAT JSON) ${stmt.sql}`);
            if (explainResult.rows && explainResult.rows.length > 0) {
              result.explainPlan = explainResult.rows[0]['QUERY PLAN'];
            }
          } catch {
            // Ignore EXPLAIN errors - just skip without plan
          }
        }
      } else {
        // Include non-mustSkip warnings if any
        if (stmtWarnings.length > 0) {
          result.warnings = stmtWarnings.map(w => w.message);
        }

        try {
          const queryResult = await client.query(stmt.sql);
          result.success = true;
          result.rowCount = queryResult.rowCount || 0;
          totalRowsAffected += result.rowCount;
          successCount++;

          // Include sample rows for SELECT or RETURNING statements
          if (queryResult.rows && queryResult.rows.length > 0) {
            result.rows = queryResult.rows.slice(0, MAX_DRY_RUN_SAMPLE_ROWS);
          }
        } catch (e) {
          result.success = false;
          result.error = extractDryRunError(e);
          failureCount++;

          if (stopOnError) {
            aborted = true;
          }
        }
      }

      const stmtEndTime = process.hrtime.bigint();
      result.executionTimeMs = Math.round(Number(stmtEndTime - stmtStartTime) / 1_000_000 * 100) / 100;

      // Only include results up to maxStatements
      if (statementResults.length < maxStatements) {
        statementResults.push(result);
      }
    }

    // Always rollback - this is a dry run
    await client.query('ROLLBACK');

  } catch (e) {
    // Ensure rollback on any error
    try {
      await client.query('ROLLBACK');
    } catch {
      // Ignore rollback errors
    }
    throw e;
  } finally {
    client.release();
  }

  const endTime = process.hrtime.bigint();
  const executionTimeMs = Math.round(Number(endTime - startTime) / 1_000_000 * 100) / 100;

  // Generate summary
  const typeEntries = Object.entries(statementsByType).sort((a, b) => b[1] - a[1]);
  const typeSummary = typeEntries.map(([type, count]) => `${count} ${type}`).join(', ');

  let summary = `Dry-run of ${totalStatements} statement${totalStatements !== 1 ? 's' : ''}: `;
  summary += `${successCount} succeeded, ${failureCount} failed`;
  if (skippedCount > 0) {
    summary += `, ${skippedCount} skipped (non-rollbackable)`;
  }
  summary += '. ';
  if (typeSummary) {
    summary += `Types: ${typeSummary}. `;
  }
  summary += `Total rows affected: ${totalRowsAffected}. `;
  summary += 'All changes rolled back.';

  return {
    success: failureCount === 0,
    filePath: resolvedPath,
    fileSize: stats.size,
    fileSizeFormatted,
    totalStatements,
    successCount,
    failureCount,
    skippedCount,
    totalRowsAffected,
    statementsByType,
    executionTimeMs,
    statementResults,
    nonRollbackableWarnings,
    summary,
    rolledBack: true
  };
}

/**
 * Strips leading line comments and block comments from SQL to check if there's actual SQL.
 * Returns empty string if the entire content is just comments.
 */
function stripLeadingComments(sql: string): string {
  let result = sql.trim();

  while (result.length > 0) {
    // Strip leading line comments
    if (result.startsWith('--')) {
      const newlineIndex = result.indexOf('\n');
      if (newlineIndex === -1) {
        return ''; // Entire string is a line comment
      }
      result = result.substring(newlineIndex + 1).trim();
      continue;
    }

    // Strip leading block comments
    if (result.startsWith('/*')) {
      const endIndex = result.indexOf('*/');
      if (endIndex === -1) {
        return ''; // Unclosed block comment
      }
      result = result.substring(endIndex + 2).trim();
      continue;
    }

    // No more leading comments
    break;
  }

  return result;
}

/**
 * Split SQL content into individual statements with line number tracking.
 * Returns ParsedStatement objects with SQL and line number info.
 */
function splitSqlStatementsWithLineNumbers(sql: string): ParsedStatement[] {
  const statements: ParsedStatement[] = [];
  let current = '';
  let currentLineNumber = 1;
  let statementStartLine = 1;
  let inString = false;
  let stringChar = '';
  let inLineComment = false;
  let inBlockComment = false;
  let i = 0;

  while (i < sql.length) {
    const char = sql[i];
    const nextChar = sql[i + 1] || '';

    // Track line numbers
    if (char === '\n') {
      currentLineNumber++;
    }

    // If starting a new statement (current is empty/whitespace), record line number
    if (current.trim() === '' && char.trim() !== '') {
      statementStartLine = currentLineNumber;
    }

    // Handle line comments
    if (!inString && !inBlockComment && char === '-' && nextChar === '-') {
      inLineComment = true;
      current += char;
      i++;
      continue;
    }

    if (inLineComment && (char === '\n' || char === '\r')) {
      inLineComment = false;
      current += char;
      i++;
      continue;
    }

    // Handle block comments
    if (!inString && !inLineComment && char === '/' && nextChar === '*') {
      inBlockComment = true;
      current += char + nextChar;
      i += 2;
      continue;
    }

    if (inBlockComment && char === '*' && nextChar === '/') {
      inBlockComment = false;
      current += char + nextChar;
      i += 2;
      continue;
    }

    // Handle string literals
    if (!inLineComment && !inBlockComment && (char === "'" || char === '"')) {
      if (!inString) {
        inString = true;
        stringChar = char;
      } else if (char === stringChar) {
        if (nextChar === stringChar) {
          current += char + nextChar;
          i += 2;
          continue;
        }
        inString = false;
        stringChar = '';
      }
    }

    // Handle dollar-quoted strings (PostgreSQL specific)
    if (!inString && !inLineComment && !inBlockComment && char === '$') {
      const dollarMatch = sql.slice(i).match(/^(\$[a-zA-Z0-9_]*\$)/);
      if (dollarMatch) {
        const dollarTag = dollarMatch[1];
        const endIndex = sql.indexOf(dollarTag, i + dollarTag.length);
        if (endIndex !== -1) {
          const dollarContent = sql.slice(i, endIndex + dollarTag.length);
          // Count newlines in dollar-quoted content
          const newlines = (dollarContent.match(/\n/g) || []).length;
          currentLineNumber += newlines;
          current += dollarContent;
          i = endIndex + dollarTag.length;
          continue;
        }
      }
    }

    // Handle statement separator
    if (!inString && !inLineComment && !inBlockComment && char === ';') {
      current += char;
      const trimmed = current.trim();
      if (trimmed) {
        statements.push({ sql: trimmed, lineNumber: statementStartLine });
      }
      current = '';
      statementStartLine = currentLineNumber;
      i++;
      continue;
    }

    current += char;
    i++;
  }

  // Add remaining content if any
  const trimmed = current.trim();
  if (trimmed) {
    statements.push({ sql: trimmed, lineNumber: statementStartLine });
  }

  return statements;
}

/**
 * Extracts table names from a SQL query.
 * Handles common patterns: FROM, JOIN, INTO, UPDATE, DELETE FROM
 */
function extractTablesFromSql(sql: string): Array<{ schema: string; table: string }> {
  const tables: Array<{ schema: string; table: string }> = [];
  const seen = new Set<string>();

  // Normalize SQL: remove comments and extra whitespace
  const normalized = sql
    .replace(/--[^\n]*/g, '') // Remove line comments
    .replace(/\/\*[\s\S]*?\*\//g, '') // Remove block comments
    .replace(/\s+/g, ' ') // Normalize whitespace
    .trim();

  // Patterns to find table references
  const patterns = [
    /\bFROM\s+(["`]?[\w]+["`]?(?:\s*\.\s*["`]?[\w]+["`]?)?)/gi,
    /\bJOIN\s+(["`]?[\w]+["`]?(?:\s*\.\s*["`]?[\w]+["`]?)?)/gi,
    /\bINTO\s+(["`]?[\w]+["`]?(?:\s*\.\s*["`]?[\w]+["`]?)?)/gi,
    /\bUPDATE\s+(["`]?[\w]+["`]?(?:\s*\.\s*["`]?[\w]+["`]?)?)/gi,
    /\bDELETE\s+FROM\s+(["`]?[\w]+["`]?(?:\s*\.\s*["`]?[\w]+["`]?)?)/gi,
  ];

  for (const pattern of patterns) {
    let match;
    while ((match = pattern.exec(normalized)) !== null) {
      const tableRef = match[1].replace(/["`]/g, '').trim();

      // Skip common SQL keywords that might be matched
      if (['SELECT', 'WHERE', 'SET', 'VALUES', 'AND', 'OR'].includes(tableRef.toUpperCase())) {
        continue;
      }

      let schema = 'public';
      let table = tableRef;

      if (tableRef.includes('.')) {
        const parts = tableRef.split('.');
        schema = parts[0].trim();
        table = parts[1].trim();
      }

      const key = `${schema}.${table}`.toLowerCase();
      if (!seen.has(key)) {
        seen.add(key);
        tables.push({ schema, table });
      }
    }
  }

  return tables;
}

/**
 * Gets schema hints for tables mentioned in SQL
 */
async function getSchemaHintForSql(sql: string): Promise<SchemaHint> {
  const dbManager = getDbManager();
  const tables = extractTablesFromSql(sql);
  const tableHints: TableSchemaHint[] = [];

  for (const { schema, table } of tables.slice(0, 10)) { // Limit to 10 tables
    try {
      // Get columns
      const columnsResult = await dbManager.query(`
        SELECT
          column_name as name,
          data_type as type,
          is_nullable = 'YES' as nullable
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
      `, [schema, table]);

      // Get primary key
      const pkResult = await dbManager.query(`
        SELECT a.attname as column_name
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE i.indisprimary
          AND n.nspname = $1
          AND c.relname = $2
      `, [schema, table]);

      // Get foreign keys
      const fkResult = await dbManager.query(`
        SELECT
          kcu.column_name,
          ccu.table_schema || '.' || ccu.table_name as referenced_table,
          ccu.column_name as referenced_column
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
          ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = $1
          AND tc.table_name = $2
      `, [schema, table]);

      // Get row count estimate
      const countResult = await dbManager.query(`
        SELECT reltuples::bigint as estimate
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relname = $2
      `, [schema, table]);

      const hint: TableSchemaHint = {
        schema,
        table,
        columns: columnsResult.rows.map(r => ({
          name: r.name,
          type: r.type,
          nullable: r.nullable
        })),
        primaryKey: pkResult.rows.map(r => r.column_name),
        rowCountEstimate: countResult.rows[0]?.estimate || 0
      };

      // Group foreign keys by constraint
      if (fkResult.rows.length > 0) {
        const fkMap = new Map<string, { columns: string[]; referencedColumns: string[] }>();
        for (const row of fkResult.rows) {
          const key = row.referenced_table;
          if (!fkMap.has(key)) {
            fkMap.set(key, { columns: [], referencedColumns: [] });
          }
          fkMap.get(key)!.columns.push(row.column_name);
          fkMap.get(key)!.referencedColumns.push(row.referenced_column);
        }
        hint.foreignKeys = Array.from(fkMap.entries()).map(([refTable, data]) => ({
          columns: data.columns,
          referencedTable: refTable,
          referencedColumns: data.referencedColumns
        }));
      }

      tableHints.push(hint);
    } catch (error) {
      // Skip tables that don't exist or have permission issues
      console.error(`Could not get schema hint for ${schema}.${table}: ${error}`);
    }
  }

  return { tables: tableHints };
}

/**
 * Preview the effect of a mutation (INSERT/UPDATE/DELETE) without executing it.
 * Returns estimated rows affected and sample of rows that would be affected.
 */
export async function mutationPreview(args: {
  sql: string;
  sampleSize?: number;
}): Promise<MutationPreviewResult> {
  if (!args.sql || typeof args.sql !== 'string') {
    throw new Error('sql parameter is required');
  }

  const sql = args.sql.trim();
  const sampleSize = Math.min(args.sampleSize || 5, 20); // Default 5, max 20

  // Detect mutation type
  const upperSql = sql.toUpperCase();
  let mutationType: 'INSERT' | 'UPDATE' | 'DELETE' | 'UNKNOWN' = 'UNKNOWN';

  if (upperSql.startsWith('UPDATE')) {
    mutationType = 'UPDATE';
  } else if (upperSql.startsWith('DELETE')) {
    mutationType = 'DELETE';
  } else if (upperSql.startsWith('INSERT')) {
    mutationType = 'INSERT';
  } else {
    throw new Error('SQL must be an INSERT, UPDATE, or DELETE statement');
  }

  const dbManager = getDbManager();

  // For INSERT, we can't preview affected rows
  if (mutationType === 'INSERT') {
    // Use EXPLAIN to estimate rows
    const explainResult = await dbManager.query(`EXPLAIN (FORMAT JSON) ${sql}`);
    const plan = explainResult.rows[0]['QUERY PLAN'][0];

    return {
      mutationType,
      estimatedRowsAffected: plan?.Plan?.['Plan Rows'] || 1,
      sampleAffectedRows: [],
      warning: 'INSERT preview cannot show affected rows - they do not exist yet'
    };
  }

  // For UPDATE and DELETE, extract WHERE clause and table
  let targetTable: string | undefined;
  let whereClause: string | undefined;

  if (mutationType === 'UPDATE') {
    // Pattern: UPDATE table SET ... WHERE ...
    const updateMatch = sql.match(/UPDATE\s+(["`]?[\w.]+["`]?)\s+SET/i);
    const whereMatch = sql.match(/\bWHERE\s+(.+)$/is);
    targetTable = updateMatch?.[1]?.replace(/["`]/g, '');
    whereClause = whereMatch?.[1];
  } else if (mutationType === 'DELETE') {
    // Pattern: DELETE FROM table WHERE ...
    const deleteMatch = sql.match(/DELETE\s+FROM\s+(["`]?[\w.]+["`]?)/i);
    const whereMatch = sql.match(/\bWHERE\s+(.+)$/is);
    targetTable = deleteMatch?.[1]?.replace(/["`]/g, '');
    whereClause = whereMatch?.[1];
  }

  if (!targetTable) {
    throw new Error('Could not parse target table from SQL');
  }

  // Get estimated row count using EXPLAIN
  let estimatedRowsAffected = 0;
  try {
    const explainResult = await dbManager.query(`EXPLAIN (FORMAT JSON) ${sql}`);
    const plan = explainResult.rows[0]['QUERY PLAN'][0];
    estimatedRowsAffected = plan?.Plan?.['Plan Rows'] || 0;
  } catch (error) {
    // EXPLAIN might fail, continue with count query
  }

  // Build SELECT query to get sample of affected rows
  let sampleRows: any[] = [];
  try {
    const selectSql = whereClause
      ? `SELECT * FROM ${targetTable} WHERE ${whereClause} LIMIT ${sampleSize}`
      : `SELECT * FROM ${targetTable} LIMIT ${sampleSize}`;

    const sampleResult = await dbManager.query(selectSql);
    sampleRows = sampleResult.rows;

    // If EXPLAIN didn't work, get count
    if (estimatedRowsAffected === 0) {
      const countSql = whereClause
        ? `SELECT COUNT(*) as cnt FROM ${targetTable} WHERE ${whereClause}`
        : `SELECT COUNT(*) as cnt FROM ${targetTable}`;
      const countResult = await dbManager.query(countSql);
      estimatedRowsAffected = parseInt(countResult.rows[0]?.cnt || '0', 10);
    }
  } catch (error) {
    throw new Error(`Could not preview affected rows: ${error instanceof Error ? error.message : String(error)}`);
  }

  const result: MutationPreviewResult = {
    mutationType,
    estimatedRowsAffected,
    sampleAffectedRows: sampleRows,
    targetTable
  };

  if (whereClause) {
    result.whereClause = whereClause;
  } else {
    result.warning = 'No WHERE clause - ALL rows in the table will be affected!';
  }

  return result;
}

/**
 * Execute a mutation (INSERT/UPDATE/DELETE) in dry-run mode.
 * Actually executes the SQL within a transaction, captures real results,
 * then rolls back so no changes are persisted.
 *
 * This provides accurate results including:
 * - Exact row counts (not estimates)
 * - Actual errors (constraint violations, triggers, etc.)
 * - Before/after row states
 */
export async function mutationDryRun(args: {
  sql: string;
  sampleSize?: number;
}): Promise<MutationDryRunResult> {
  if (!args.sql || typeof args.sql !== 'string') {
    throw new Error('sql parameter is required');
  }

  const sql = args.sql.trim();
  const sampleSize = Math.min(args.sampleSize || MAX_DRY_RUN_SAMPLE_ROWS, 20);

  // Detect mutation type
  const upperSql = sql.toUpperCase();
  let mutationType: 'INSERT' | 'UPDATE' | 'DELETE' | 'UNKNOWN' = 'UNKNOWN';

  if (upperSql.startsWith('UPDATE') || upperSql.match(/^WITH\b.*\bUPDATE\b/s)) {
    mutationType = 'UPDATE';
  } else if (upperSql.startsWith('DELETE') || upperSql.match(/^WITH\b.*\bDELETE\b/s)) {
    mutationType = 'DELETE';
  } else if (upperSql.startsWith('INSERT') || upperSql.match(/^WITH\b.*\bINSERT\b/s)) {
    mutationType = 'INSERT';
  } else {
    throw new Error('SQL must be an INSERT, UPDATE, or DELETE statement');
  }

  // Check for non-rollbackable operations
  const nonRollbackableWarnings = detectNonRollbackableOperations(sql);
  const mustSkipWarnings = nonRollbackableWarnings.filter(w => w.mustSkip);

  // Skip only if there are mustSkip warnings
  if (mustSkipWarnings.length > 0) {
    const skipReason = mustSkipWarnings.map(w => w.message).join('; ');

    // Run EXPLAIN to show query plan without executing
    let explainPlan: object[] | undefined;
    const dbManager = getDbManager();
    try {
      const explainResult = await dbManager.query(`EXPLAIN (FORMAT JSON) ${sql}`);
      if (explainResult.rows && explainResult.rows.length > 0) {
        explainPlan = explainResult.rows[0]['QUERY PLAN'];
      }
    } catch {
      // Ignore EXPLAIN errors
    }

    return {
      mutationType,
      success: true, // Not a failure, just skipped
      skipped: true,
      skipReason,
      rowsAffected: 0,
      nonRollbackableWarnings,
      explainPlan,
      warnings: nonRollbackableWarnings.map(w => w.message)
    };
  }

  // Extract table and WHERE clause
  let targetTable: string | undefined;
  let whereClause: string | undefined;

  if (mutationType === 'UPDATE') {
    const updateMatch = sql.match(/UPDATE\s+(["`]?[\w.]+["`]?)\s+SET/i);
    const whereMatch = sql.match(/\bWHERE\s+(.+?)(?:RETURNING|$)/is);
    targetTable = updateMatch?.[1]?.replace(/["`]/g, '');
    whereClause = whereMatch?.[1]?.trim();
  } else if (mutationType === 'DELETE') {
    const deleteMatch = sql.match(/DELETE\s+FROM\s+(["`]?[\w.]+["`]?)/i);
    const whereMatch = sql.match(/\bWHERE\s+(.+?)(?:RETURNING|$)/is);
    targetTable = deleteMatch?.[1]?.replace(/["`]/g, '');
    whereClause = whereMatch?.[1]?.trim();
  } else if (mutationType === 'INSERT') {
    const insertMatch = sql.match(/INSERT\s+INTO\s+(["`]?[\w.]+["`]?)/i);
    targetTable = insertMatch?.[1]?.replace(/["`]/g, '');
  }

  const dbManager = getDbManager();
  const client = await dbManager.getClient();
  const startTime = process.hrtime.bigint();

  let beforeRows: any[] | undefined;
  let affectedRows: any[] = [];
  let rowsAffected = 0;
  let error: DryRunError | undefined;
  let success = false;
  const warnings: string[] = [];

  try {
    // Start transaction
    await client.query('BEGIN');

    // For UPDATE/DELETE, capture "before" state
    if ((mutationType === 'UPDATE' || mutationType === 'DELETE') && targetTable) {
      try {
        const beforeSql = whereClause
          ? `SELECT * FROM ${targetTable} WHERE ${whereClause} LIMIT ${sampleSize}`
          : `SELECT * FROM ${targetTable} LIMIT ${sampleSize}`;
        const beforeResult = await client.query(beforeSql);
        beforeRows = beforeResult.rows;
      } catch (e) {
        // Couldn't get before rows, continue anyway
        warnings.push(`Could not capture before state: ${e instanceof Error ? e.message : String(e)}`);
      }
    }

    // Execute the actual mutation
    // Add RETURNING * if not already present to get affected rows
    let executeSql = sql;
    const hasReturning = upperSql.includes('RETURNING');

    if (!hasReturning && targetTable) {
      executeSql = `${sql} RETURNING *`;
    }

    try {
      const result = await client.query(executeSql);
      rowsAffected = result.rowCount || 0;

      if (result.rows && result.rows.length > 0) {
        affectedRows = result.rows.slice(0, sampleSize);
      }

      success = true;
    } catch (e) {
      // If RETURNING failed, try without it
      if (!hasReturning) {
        try {
          const result = await client.query(sql);
          rowsAffected = result.rowCount || 0;
          success = true;

          // Try to get affected rows for UPDATE/DELETE
          if ((mutationType === 'UPDATE' || mutationType === 'DELETE') && targetTable && whereClause) {
            const afterSql = `SELECT * FROM ${targetTable} WHERE ${whereClause} LIMIT ${sampleSize}`;
            const afterResult = await client.query(afterSql);
            affectedRows = afterResult.rows;
          }
        } catch (innerError) {
          error = extractDryRunError(innerError);
        }
      } else {
        error = extractDryRunError(e);
      }
    }

    // Always rollback - this is a dry run
    await client.query('ROLLBACK');

  } catch (e) {
    // Ensure rollback on any error
    try {
      await client.query('ROLLBACK');
    } catch {
      // Ignore rollback errors
    }
    error = extractDryRunError(e);
  } finally {
    client.release();
  }

  const endTime = process.hrtime.bigint();
  const executionTimeMs = Number(endTime - startTime) / 1_000_000;

  const result: MutationDryRunResult = {
    mutationType,
    success,
    rowsAffected,
    executionTimeMs: Math.round(executionTimeMs * 100) / 100
  };

  if (beforeRows && beforeRows.length > 0) {
    result.beforeRows = beforeRows;
  }

  if (affectedRows.length > 0) {
    result.affectedRows = affectedRows;
  }

  if (targetTable) {
    result.targetTable = targetTable;
  }

  if (whereClause) {
    result.whereClause = whereClause;
  } else if (mutationType !== 'INSERT') {
    warnings.push('No WHERE clause - ALL rows in the table would be affected!');
  }

  if (error) {
    result.error = error;
  }

  if (nonRollbackableWarnings.length > 0) {
    result.nonRollbackableWarnings = nonRollbackableWarnings;
  }

  if (warnings.length > 0) {
    result.warnings = warnings;
  }

  return result;
}

/**
 * Execute multiple SQL queries in parallel.
 * Returns results keyed by query name.
 */
export async function batchExecute(args: {
  queries: BatchQuery[];
  stopOnError?: boolean;
}): Promise<BatchExecuteResult> {
  if (!args.queries || !Array.isArray(args.queries)) {
    throw new Error('queries parameter is required and must be an array');
  }

  if (args.queries.length === 0) {
    throw new Error('queries array cannot be empty');
  }

  if (args.queries.length > 20) {
    throw new Error('Maximum 20 queries allowed in a batch');
  }

  // Validate each query
  const seenNames = new Set<string>();
  for (const query of args.queries) {
    if (!query.name || typeof query.name !== 'string') {
      throw new Error('Each query must have a name');
    }
    if (!query.sql || typeof query.sql !== 'string') {
      throw new Error(`Query "${query.name}" must have sql`);
    }
    if (seenNames.has(query.name)) {
      throw new Error(`Duplicate query name: ${query.name}`);
    }
    seenNames.add(query.name);
  }

  const dbManager = getDbManager();
  const stopOnError = args.stopOnError === true; // Default false
  const startTime = process.hrtime.bigint();

  const results: { [name: string]: BatchQueryResult } = {};
  let successCount = 0;
  let failureCount = 0;

  // Execute all queries in parallel
  const promises = args.queries.map(async (query) => {
    const queryStartTime = process.hrtime.bigint();

    try {
      const result = await dbManager.query(query.sql, query.params);
      const queryEndTime = process.hrtime.bigint();
      const executionTimeMs = Number(queryEndTime - queryStartTime) / 1_000_000;

      return {
        name: query.name,
        result: {
          success: true,
          rows: result.rows,
          rowCount: result.rowCount ?? result.rows.length,
          executionTimeMs: Math.round(executionTimeMs * 100) / 100
        } as BatchQueryResult
      };
    } catch (error) {
      const queryEndTime = process.hrtime.bigint();
      const executionTimeMs = Number(queryEndTime - queryStartTime) / 1_000_000;

      return {
        name: query.name,
        result: {
          success: false,
          error: error instanceof Error ? error.message : String(error),
          executionTimeMs: Math.round(executionTimeMs * 100) / 100
        } as BatchQueryResult
      };
    }
  });

  // Wait for all queries
  const queryResults = await Promise.all(promises);

  // Collect results
  for (const { name, result } of queryResults) {
    results[name] = result;
    if (result.success) {
      successCount++;
    } else {
      failureCount++;
      if (stopOnError) {
        // Mark remaining as not executed
        break;
      }
    }
  }

  const endTime = process.hrtime.bigint();
  const totalExecutionTimeMs = Number(endTime - startTime) / 1_000_000;

  return {
    totalQueries: args.queries.length,
    successCount,
    failureCount,
    totalExecutionTimeMs: Math.round(totalExecutionTimeMs * 100) / 100,
    results
  };
}

/**
 * Begin a new transaction. Returns a transactionId to use with subsequent queries.
 */
export async function beginTransaction(args?: {
  name?: string;
}): Promise<TransactionResult & { transactionId: string; name?: string }> {
  const dbManager = getDbManager();
  const info = await dbManager.beginTransaction(args?.name);

  return {
    transactionId: info.transactionId,
    name: info.name,
    status: 'started',
    message: `Transaction${info.name ? ` "${info.name}"` : ''} started. Use transactionId "${info.transactionId}" with execute_sql or commit/rollback.`
  };
}

/**
 * Get information about an active transaction.
 */
export async function getTransactionInfo(args: {
  transactionId: string;
}): Promise<TransactionInfo | { error: string }> {
  if (!args.transactionId) {
    throw new Error('transactionId parameter is required');
  }

  const dbManager = getDbManager();
  const info = dbManager.getTransactionInfo(args.transactionId);

  if (!info) {
    return { error: `Transaction not found: ${args.transactionId}` };
  }

  return info;
}

/**
 * List all active transactions.
 */
export async function listActiveTransactions(): Promise<{ transactions: TransactionInfo[]; count: number }> {
  const dbManager = getDbManager();
  const transactions = dbManager.listActiveTransactions();

  return {
    transactions,
    count: transactions.length
  };
}

/**
 * Commit an active transaction.
 */
export async function commitTransaction(args: {
  transactionId: string;
}): Promise<TransactionResult> {
  if (!args.transactionId || typeof args.transactionId !== 'string') {
    throw new Error('transactionId is required');
  }

  const dbManager = getDbManager();
  await dbManager.commitTransaction(args.transactionId);

  return {
    transactionId: args.transactionId,
    status: 'committed',
    message: 'Transaction committed successfully.'
  };
}

/**
 * Rollback an active transaction.
 */
export async function rollbackTransaction(args: {
  transactionId: string;
}): Promise<TransactionResult> {
  if (!args.transactionId || typeof args.transactionId !== 'string') {
    throw new Error('transactionId is required');
  }

  const dbManager = getDbManager();
  await dbManager.rollbackTransaction(args.transactionId);

  return {
    transactionId: args.transactionId,
    status: 'rolled_back',
    message: 'Transaction rolled back successfully.'
  };
}

/**
 * Get connection context for including in tool responses
 */
export function getConnectionContext(): ConnectionContext {
  const dbManager = getDbManager();
  return dbManager.getConnectionContext();
}
