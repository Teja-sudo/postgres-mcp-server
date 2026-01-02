import { MAX_SQL_LENGTH_FOR_REGEX } from '../tools/sql/utils/constants.js';

/** Maximum identifier length in PostgreSQL */
const MAX_IDENTIFIER_LENGTH = 63;

/** Pattern for valid unquoted PostgreSQL identifier part */
const UNQUOTED_IDENTIFIER_PATTERN = /^[a-zA-Z_][a-zA-Z0-9_$]*$/;

/** Pattern for quoted identifier content (allows Unicode and most characters) */
// Using [^"]|"" instead of [^"]+|"" to avoid potential ReDoS from nested quantifiers
const QUOTED_IDENTIFIER_PATTERN = /^"(?:[^"]|"")*"$/;

/** SQL injection patterns that should never appear in identifiers */
const SQL_INJECTION_PATTERNS = [
  /;/,           // Statement terminator
  /--/,          // Line comment start
  /\/\*/,        // Block comment start
  /\*\//,        // Block comment end
];

/**
 * Validates a single PostgreSQL identifier part (without schema).
 * Supports both unquoted and quoted identifiers.
 *
 * @param part - The identifier part to validate
 * @param fieldName - Field name for error messages
 * @returns The validated identifier part
 * @throws Error if the identifier is invalid
 */
function validateIdentifierPart(part: string, fieldName: string): string {
  if (!part || typeof part !== 'string') {
    throw new Error(`${fieldName} is required and must be a string`);
  }

  // Check for SQL injection patterns
  for (const pattern of SQL_INJECTION_PATTERNS) {
    if (pattern.test(part)) {
      throw new Error(`${fieldName} contains potentially dangerous SQL characters`);
    }
  }

  // Handle quoted identifiers (allow Unicode)
  if (part.startsWith('"') && part.endsWith('"')) {
    if (!QUOTED_IDENTIFIER_PATTERN.test(part)) {
      throw new Error(`${fieldName} has invalid quoted identifier syntax. Use "" to escape internal quotes.`);
    }
    // Extract content without quotes for length check
    const content = part.slice(1, -1).replace(/""/g, '"');
    if (content.length > MAX_IDENTIFIER_LENGTH) {
      throw new Error(`${fieldName} must be ${MAX_IDENTIFIER_LENGTH} characters or less (excluding quotes)`);
    }
    return part;
  }

  // Unquoted identifier validation
  if (part.length > MAX_IDENTIFIER_LENGTH) {
    throw new Error(`${fieldName} must be ${MAX_IDENTIFIER_LENGTH} characters or less`);
  }

  if (!UNQUOTED_IDENTIFIER_PATTERN.test(part)) {
    throw new Error(
      `${fieldName} contains invalid characters. Only letters, numbers, underscores, and dollar signs are allowed, ` +
      `and it must start with a letter or underscore. Use double quotes for special characters or Unicode.`
    );
  }

  return part;
}

/**
 * Gets the appropriate field name for a part in a qualified identifier.
 * Extracted to avoid nested ternary operators.
 *
 * @param totalParts - Total number of parts in the identifier
 * @param partIndex - Index of the current part (0-based)
 * @param fieldName - Base field name for error messages
 * @returns The appropriate part name for error messages
 */
function getPartName(totalParts: number, partIndex: number, fieldName: string): string {
  if (totalParts !== 2) {
    return fieldName;
  }
  return partIndex === 0 ? `${fieldName} schema` : `${fieldName} name`;
}

/**
 * Validates and sanitizes PostgreSQL identifiers (table names, column names, etc.)
 * Supports schema-qualified names (schema.table) and quoted identifiers.
 * Prevents SQL injection by validating identifier characters.
 *
 * @param identifier - The identifier to validate (e.g., "users", "public.users", '"My Table"')
 * @param fieldName - Field name for error messages
 * @param allowQualified - Whether to allow schema-qualified names (default: true)
 * @returns The validated identifier
 * @throws Error if the identifier is invalid
 *
 * @example
 * validateIdentifier('users', 'table')           // 'users'
 * validateIdentifier('public.users', 'table')    // 'public.users'
 * validateIdentifier('"My Table"', 'table')      // '"My Table"'
 * validateIdentifier('my_schema."Table"', 'table') // 'my_schema."Table"'
 */
export function validateIdentifier(
  identifier: string,
  fieldName: string,
  allowQualified: boolean = true
): string {
  if (!identifier || typeof identifier !== 'string') {
    throw new Error(`${fieldName} is required and must be a string`);
  }

  const trimmed = identifier.trim();

  // Split by unquoted dots (respecting quoted identifiers)
  const parts = splitQualifiedIdentifier(trimmed);

  if (parts.length === 0) {
    throw new Error(`${fieldName} is required and must be a string`);
  }

  if (parts.length > 2) {
    throw new Error(`${fieldName} has too many parts. Maximum format: schema.object`);
  }

  if (parts.length === 2 && !allowQualified) {
    throw new Error(`${fieldName} cannot be schema-qualified. Provide just the object name.`);
  }

  // Validate each part
  for (let i = 0; i < parts.length; i++) {
    const partName = getPartName(parts.length, i, fieldName);
    validateIdentifierPart(parts[i], partName);
  }

  return trimmed;
}

/**
 * Splits a qualified identifier (e.g., "schema.table" or "schema"."table")
 * into its parts, respecting quoted identifiers.
 *
 * @param identifier - The qualified identifier to split
 * @returns Array of identifier parts
 */
function splitQualifiedIdentifier(identifier: string): string[] {
  const parts: string[] = [];
  let current = '';
  let inQuotes = false;
  let i = 0;

  while (i < identifier.length) {
    const char = identifier[i];

    if (char === '"') {
      if (inQuotes && identifier[i + 1] === '"') {
        // Escaped quote inside quoted identifier
        current += '""';
        i += 2;
        continue;
      }
      inQuotes = !inQuotes;
      current += char;
    } else if (char === '.' && !inQuotes) {
      // Separator found
      if (current.trim()) {
        parts.push(current.trim());
      }
      current = '';
    } else {
      current += char;
    }
    i++;
  }

  // Add the last part
  if (current.trim()) {
    parts.push(current.trim());
  }

  return parts;
}

/**
 * Parses a qualified identifier into schema and object name.
 *
 * @param identifier - The identifier to parse (must be pre-validated)
 * @returns Object with schema (optional) and name
 */
export function parseQualifiedIdentifier(identifier: string): { schema?: string; name: string } {
  const parts = splitQualifiedIdentifier(identifier);

  if (parts.length === 2) {
    return { schema: parts[0], name: parts[1] };
  }
  return { name: parts[0] || identifier };
}

/**
 * Escapes a PostgreSQL identifier by doubling any internal double quotes
 * and wrapping in double quotes.
 */
export function escapeIdentifier(identifier: string): string {
  // Validate first
  if (!identifier || typeof identifier !== 'string') {
    throw new Error('Identifier is required and must be a string');
  }

  // Double any internal double quotes and wrap in quotes
  return `"${identifier.replace(/"/g, '""')}"`;
}

/**
 * Write operations that should be blocked in read-only mode.
 * Organized by category for maintainability.
 */
const WRITE_OPERATIONS: readonly string[] = [
  // DML Operations
  'INSERT',
  'UPDATE',
  'DELETE',
  'MERGE',              // SQL:2008 standard MERGE statement
  'UPSERT',             // PostgreSQL UPSERT pattern
  // DDL Operations
  'DROP',
  'CREATE',
  'ALTER',
  'TRUNCATE',
  'COMMENT ON',         // Modifies metadata
  // Security Operations
  'GRANT',
  'REVOKE',
  'REASSIGN OWNED',     // Ownership transfer
  'SECURITY LABEL',     // Security labels
  // Maintenance Operations (with side effects)
  'COPY',               // Can write to files
  'VACUUM',
  'REINDEX',
  'CLUSTER',
  'ANALYZE',            // Modifies statistics (can affect query plans)
  'REFRESH MATERIALIZED VIEW',
  // Lock Operations
  'LOCK',
  // Session State Operations
  'DISCARD',
  'RESET',
  'SET ',               // Note: space to avoid matching in column names
  'SET LOCAL',
  'SET SESSION',
  // Code Execution
  'DO',                 // Anonymous code blocks (no trailing space, \b handles boundary)
  'CALL',               // Stored procedures
  'EXECUTE',            // Dynamic SQL execution
  // Transaction Control (shouldn't be in read-only queries)
  'BEGIN',
  'COMMIT',
  'ROLLBACK',
  'SAVEPOINT',
  'RELEASE SAVEPOINT',
  // PostgreSQL Extensions
  'LOAD',               // Load shared library
  'IMPORT FOREIGN SCHEMA',
  'CREATE SERVER',
  'CREATE FOREIGN',
  'NOTIFY',             // Send notification
  'LISTEN',             // Subscribe to notifications
  'UNLISTEN',           // Unsubscribe from notifications
] as const;

/**
 * Functions that could have side effects or security implications.
 */
const DANGEROUS_FUNCTIONS: readonly string[] = [
  // Large Object Functions
  'LO_IMPORT',
  'LO_EXPORT',
  'LO_UNLINK',
  'LO_CREATE',
  'LO_OPEN',
  'LO_WRITE',
  'LO_PUT',
  // File System Functions
  'PG_READ_FILE',
  'PG_READ_BINARY_FILE',
  'PG_WRITE_FILE',
  'PG_FILE_WRITE',
  'PG_FILE_UNLINK',
  'PG_FILE_RENAME',
  // Remote Connection Functions
  'DBLINK_EXEC',
  'DBLINK',
  'DBLINK_CONNECT',
  'DBLINK_SEND_QUERY',
  // Copy Functions
  'COPY_TO',
  'COPY_FROM',
  // System Administration
  'PG_TERMINATE_BACKEND',
  'PG_CANCEL_BACKEND',
  'PG_RELOAD_CONF',
  'PG_ROTATE_LOGFILE',
  'PG_SWITCH_WAL',
  'PG_SWITCH_XLOG',
  // Sequence Functions (modify state even in rollback)
  'NEXTVAL',
  'SETVAL',
  'CURRVAL',            // Only dangerous if called after NEXTVAL
  // Advisory Lock Functions (can affect other sessions)
  'PG_ADVISORY_LOCK',
  'PG_ADVISORY_UNLOCK',
  'PG_TRY_ADVISORY_LOCK',
] as const;

/** Result type for read-only SQL validation */
type ReadOnlyResult = { isReadOnly: boolean; reason?: string };

/**
 * Normalizes SQL for analysis by removing comments and extra whitespace.
 */
function normalizeSqlForAnalysis(sql: string): string {
  return sql
    .replace(/--[^\n]*/g, '')           // Remove single-line comments
    .replace(/\/\*[\s\S]*?\*\//g, '')   // Remove multi-line comments (non-greedy, safe pattern)
    .replace(/\s+/g, ' ')               // Normalize whitespace
    .trim()
    .toUpperCase();
}

/**
 * Checks if SQL contains any write operations.
 * Uses ReDoS-safe patterns with bounded whitespace.
 */
function checkWriteOperations(normalizedSql: string): ReadOnlyResult | null {
  for (const op of WRITE_OPERATIONS) {
    const escapedOp = op.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const patterns = [
      new RegExp(`^${escapedOp}\\b`),            // At start
      new RegExp(`\\(\\s{0,10}${escapedOp}\\b`), // After opening paren
      new RegExp(`;\\s{0,10}${escapedOp}\\b`),   // After semicolon
    ];
    for (const pattern of patterns) {
      if (pattern.test(normalizedSql)) {
        return { isReadOnly: false, reason: `Write operation '${op.trim()}' detected` };
      }
    }
  }
  return null;
}

/**
 * Checks for data-modifying CTEs (WITH ... AS (INSERT/UPDATE/DELETE)).
 */
function checkCteWriteOperations(normalizedSql: string): ReadOnlyResult | null {
  if (!/\bWITH\b/.test(normalizedSql)) {
    return null;
  }
  for (const op of ['INSERT', 'UPDATE', 'DELETE', 'MERGE']) {
    if (new RegExp(`\\bAS\\s{0,10}\\(\\s{0,10}${op}\\b`).test(normalizedSql)) {
      return { isReadOnly: false, reason: `Write operation '${op}' in CTE detected` };
    }
  }
  return null;
}

/**
 * Checks for dangerous function calls that could have side effects.
 */
function checkDangerousFunctions(normalizedSql: string): ReadOnlyResult | null {
  for (const func of DANGEROUS_FUNCTIONS) {
    const escapedFunc = func.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    if (new RegExp(`\\b${escapedFunc}\\s{0,10}\\(`).test(normalizedSql)) {
      return { isReadOnly: false, reason: `Dangerous function '${func}' detected` };
    }
  }
  return null;
}

/**
 * Validates SQL for read-only operations more thoroughly.
 * Uses ReDoS-safe patterns and comprehensive operation detection.
 *
 * @param sql - The SQL string to validate
 * @returns Object with isReadOnly flag and optional reason
 */
export function isReadOnlySql(sql: string): ReadOnlyResult {
  if (!sql || typeof sql !== 'string') {
    return { isReadOnly: false, reason: 'SQL is required' };
  }

  if (sql.length > MAX_SQL_LENGTH_FOR_REGEX) {
    return {
      isReadOnly: false,
      reason: `SQL too large for safe validation (${sql.length} chars, max ${MAX_SQL_LENGTH_FOR_REGEX}). Review manually.`,
    };
  }

  const normalizedSql = normalizeSqlForAnalysis(sql);

  // Check each category of dangerous operations
  const writeCheck = checkWriteOperations(normalizedSql);
  if (writeCheck) return writeCheck;

  const cteCheck = checkCteWriteOperations(normalizedSql);
  if (cteCheck) return cteCheck;

  const funcCheck = checkDangerousFunctions(normalizedSql);
  if (funcCheck) return funcCheck;

  return { isReadOnly: true };
}

/**
 * Validates that a value is a positive integer within acceptable bounds.
 *
 * @param value - The value to validate (can be number, string, or undefined/null)
 * @param fieldName - Name of the field for error messages
 * @param min - Minimum allowed value (default: 1)
 * @param max - Maximum allowed value (default: 10000)
 * @returns The validated integer value
 * @throws Error if value is not a valid integer within bounds
 */
export function validatePositiveInteger(
  value: unknown,
  fieldName: string,
  min: number = 1,
  max: number = 10000
): number {
  if (value === undefined || value === null) {
    return min;
  }

  const num = typeof value === 'number' ? value : parseInt(String(value), 10);
  if (isNaN(num) || num < min || num > max) {
    throw new Error(`${fieldName} must be an integer between ${min} and ${max}`);
  }

  return num;
}

/**
 * Validates index type is one of the allowed PostgreSQL index types.
 */
export function validateIndexType(indexType: string): string {
  const validTypes = ['btree', 'hash', 'gist', 'spgist', 'gin', 'brin'];
  const normalized = (indexType || 'btree').toLowerCase();

  if (!validTypes.includes(normalized)) {
    throw new Error(`Invalid index type '${indexType}'. Must be one of: ${validTypes.join(', ')}`);
  }

  return normalized;
}
