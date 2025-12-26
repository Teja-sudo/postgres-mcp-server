import { getDbManager } from '../db-manager.js';
import { ExecuteSqlResult, QueryPlan } from '../types.js';
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

export async function executeSql(args: {
  sql: string;
  params?: any[];
  maxRows?: number;
  offset?: number;
  allowLargeScript?: boolean;
}): Promise<ExecuteSqlResult> {
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

  // Validate params if provided
  if (args.params !== undefined) {
    if (!Array.isArray(args.params)) {
      throw new Error('params must be an array');
    }
    if (args.params.length > MAX_PARAMS) {
      throw new Error(`Maximum ${MAX_PARAMS} parameters allowed`);
    }
  }

  const dbManager = getDbManager();
  const maxRows = validatePositiveInteger(args.maxRows, 'maxRows', 1, MAX_ROWS_LIMIT) || MAX_ROWS_DEFAULT;
  const offset = args.offset !== undefined ? validatePositiveInteger(args.offset, 'offset', 0, Number.MAX_SAFE_INTEGER) : 0;

  // Record start time for execution timing
  const startTime = process.hrtime.bigint();

  // Execute query with optional parameters
  const result = await dbManager.query(sql, args.params);

  // Calculate execution time in milliseconds
  const endTime = process.hrtime.bigint();
  const executionTimeMs = Number(endTime - startTime) / 1_000_000;

  const fields = result.fields?.map(f => f.name) || [];
  const totalRows = result.rows.length;

  // Apply offset and limit to the results
  const startIndex = Math.min(offset, totalRows);
  const endIndex = Math.min(startIndex + maxRows, totalRows);
  const paginatedRows = result.rows.slice(startIndex, endIndex);
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
      hasMore: endIndex < totalRows
    };
  }

  return {
    rows: paginatedRows,
    rowCount: totalRows,
    fields,
    executionTimeMs: Math.round(executionTimeMs * 100) / 100,
    offset: startIndex,
    hasMore: endIndex < totalRows
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
  sql: string;
  error: string;
}

/**
 * Result of executing a SQL file
 */
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
}

/**
 * Execute a SQL file from the filesystem.
 * Supports transaction mode for atomic execution.
 */
export async function executeSqlFile(args: {
  filePath: string;
  useTransaction?: boolean;
  stopOnError?: boolean;
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
  const sqlContent = fs.readFileSync(resolvedPath, 'utf-8');

  const dbManager = getDbManager();
  const useTransaction = args.useTransaction !== false; // Default to true
  const stopOnError = args.stopOnError !== false; // Default to true

  const startTime = process.hrtime.bigint();
  const client = await dbManager.getClient();

  let statementsExecuted = 0;
  let statementsFailed = 0;
  let totalRowsAffected = 0;
  let rolledBack = false;
  const collectedErrors: StatementError[] = [];

  // Split SQL into statements first to get total count
  const statements = splitSqlStatements(sqlContent);
  const executableStatements = statements.filter(s => {
    const trimmed = s.trim();
    return trimmed && !trimmed.startsWith('--');
  });
  const totalStatements = executableStatements.length;

  try {
    if (useTransaction) {
      await client.query('BEGIN');
    }

    let statementIndex = 0;
    for (const statement of statements) {
      const trimmed = statement.trim();
      // Skip empty statements
      if (!trimmed) {
        continue;
      }

      // Skip pure comment-only statements (just -- comments with no SQL after)
      const withoutComments = stripLeadingComments(trimmed);
      if (!withoutComments) {
        continue;
      }

      statementIndex++;

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
            statementIndex,
            sql: trimmed.length > 200 ? trimmed.substring(0, 200) + '...' : trimmed,
            error: errorMessage
          });
          throw error;
        }

        // If stopOnError is false, collect error and continue
        collectedErrors.push({
          statementIndex,
          sql: trimmed.length > 200 ? trimmed.substring(0, 200) + '...' : trimmed,
          error: errorMessage
        });
        console.error(`Warning: Statement ${statementIndex} failed: ${errorMessage}`);
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
 * Split SQL content into individual statements.
 * Handles basic cases like semicolons, comments, and string literals.
 */
function splitSqlStatements(sql: string): string[] {
  const statements: string[] = [];
  let current = '';
  let inString = false;
  let stringChar = '';
  let inLineComment = false;
  let inBlockComment = false;
  let i = 0;

  while (i < sql.length) {
    const char = sql[i];
    const nextChar = sql[i + 1] || '';

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
        // Check for escaped quote (doubled)
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
          current += sql.slice(i, endIndex + dollarTag.length);
          i = endIndex + dollarTag.length;
          continue;
        }
      }
    }

    // Handle statement separator
    if (!inString && !inLineComment && !inBlockComment && char === ';') {
      current += char;
      statements.push(current.trim());
      current = '';
      i++;
      continue;
    }

    current += char;
    i++;
  }

  // Add remaining content if any
  if (current.trim()) {
    statements.push(current.trim());
  }

  return statements.filter(s => s.length > 0);
}
