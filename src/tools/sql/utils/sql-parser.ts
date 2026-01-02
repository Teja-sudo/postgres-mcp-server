/**
 * SQL Parser Utilities
 *
 * Functions for parsing, analyzing, and splitting SQL statements.
 * Handles PostgreSQL-specific syntax including:
 * - Dollar-quoted strings ($$ or $tag$)
 * - Nested block comments
 * - Single and double-quoted strings with escape handling
 */

import { ParsedStatement } from '../../../types.js';

/** Common SQL statement types for detection */
const STATEMENT_TYPES = [
  'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP',
  'TRUNCATE', 'GRANT', 'REVOKE', 'BEGIN', 'COMMIT', 'ROLLBACK',
  'SET', 'SHOW', 'EXPLAIN', 'ANALYZE', 'VACUUM', 'REINDEX',
  'COMMENT', 'WITH', 'DO', 'CALL', 'EXECUTE', 'MERGE', 'COPY',
] as const;

/** SQL keywords to skip when extracting table names */
const SQL_KEYWORDS_TO_SKIP = new Set([
  'SELECT', 'WHERE', 'SET', 'VALUES', 'AND', 'OR', 'NOT', 'NULL',
  'TRUE', 'FALSE', 'AS', 'ON', 'USING', 'NATURAL', 'CROSS',
  'INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER', 'LATERAL',
]);

/**
 * Strips a leading line comment from SQL and returns the remaining string.
 * Returns null if no line comment is present.
 */
function stripLineComment(sql: string): string | null {
  if (!sql.startsWith('--')) {
    return null;
  }
  const newlineIndex = sql.indexOf('\n');
  if (newlineIndex === -1) {
    return ''; // Entire string is a line comment
  }
  return sql.substring(newlineIndex + 1).trim();
}

/**
 * Finds the end position of a nested block comment starting at position 2.
 * Returns the position after the closing comment, or -1 if unclosed.
 */
function findBlockCommentEnd(sql: string): number {
  let depth = 1;
  let i = 2;
  while (i < sql.length && depth > 0) {
    if (sql[i] === '/' && sql[i + 1] === '*') {
      depth++;
      i += 2;
    } else if (sql[i] === '*' && sql[i + 1] === '/') {
      depth--;
      i += 2;
    } else {
      i++;
    }
  }
  return depth > 0 ? -1 : i;
}

/**
 * Checks if character at position matches any statement type prefix.
 */
function matchesStatementType(trimmed: string, type: string): boolean {
  return (
    trimmed.startsWith(type + ' ') ||
    trimmed.startsWith(type + '\n') ||
    trimmed.startsWith(type + '\t') ||
    trimmed === type
  );
}

/**
 * Warning information from SQL parsing.
 */
export interface ParseWarning {
  type: 'unclosed_dollar_quote' | 'unclosed_block_comment' | 'unclosed_string';
  message: string;
  lineNumber: number;
  position: number;
  tag?: string;
}

/**
 * Result of SQL parsing with optional warnings.
 */
export interface ParseResult {
  statements: ParsedStatement[];
  warnings: ParseWarning[];
}

/**
 * Strips leading line comments and block comments from SQL.
 * Properly handles nested block comments (PostgreSQL supports nesting).
 * Returns empty string if the entire content is just comments.
 *
 * IMPORTANT: This function only strips LEADING comments. Line numbers
 * in the remaining SQL are NOT affected - they still correspond to
 * the original file positions.
 *
 * @param sql - The SQL string to process
 * @returns The SQL with leading comments stripped
 */
export function stripLeadingComments(sql: string): string {
  let result = sql.trim();

  while (result.length > 0) {
    // Try stripping leading line comment
    const afterLineComment = stripLineComment(result);
    if (afterLineComment !== null) {
      if (afterLineComment === '') return '';
      result = afterLineComment;
      continue;
    }

    // Try stripping leading block comment
    if (result.startsWith('/*')) {
      const endPos = findBlockCommentEnd(result);
      if (endPos === -1) {
        return ''; // Unclosed nested block comment
      }
      result = result.substring(endPos).trim();
      continue;
    }

    // No more leading comments
    break;
  }

  return result;
}

/**
 * Detect the type of SQL statement (SELECT, INSERT, UPDATE, DELETE, CREATE, etc.)
 *
 * @param sql - The SQL statement to analyze
 * @returns The detected statement type
 */
export function detectStatementType(sql: string): string {
  const trimmed = stripLeadingComments(sql).toUpperCase();

  for (const type of STATEMENT_TYPES) {
    if (matchesStatementType(trimmed, type)) {
      // Special case for WITH - check if it's a CTE followed by SELECT/INSERT/UPDATE/DELETE
      if (type === 'WITH') {
        return detectWithCteType(trimmed);
      }
      return type;
    }
  }

  return 'UNKNOWN';
}

/**
 * Detects the type of WITH CTE statement.
 */
function detectWithCteType(trimmed: string): string {
  if (trimmed.includes('SELECT')) return 'WITH SELECT';
  if (trimmed.includes('INSERT')) return 'WITH INSERT';
  if (trimmed.includes('UPDATE')) return 'WITH UPDATE';
  if (trimmed.includes('DELETE')) return 'WITH DELETE';
  return 'WITH';
}

/**
 * Split SQL content into individual statements with line number tracking.
 * Handles PostgreSQL-specific syntax including:
 * - Single and double-quoted strings with escape handling
 * - Dollar-quoted strings ($$ or $tag$)
 * - Line comments (--)
 * - Nested block comments (PostgreSQL supports nested comments)
 *
 * Line numbers in the returned statements correspond to actual file positions.
 * Comments are NOT excluded from line counting - all newlines are counted.
 *
 * @param sql - The SQL content to split
 * @returns Array of parsed statements with accurate line numbers
 */
export function splitSqlStatementsWithLineNumbers(sql: string): ParsedStatement[] {
  const result = splitSqlStatementsWithWarnings(sql);
  return result.statements;
}

/**
 * Split SQL content into individual statements with line number tracking
 * and collect any warnings about parsing issues.
 *
 * @param sql - The SQL content to split
 * @returns Object with statements and any parsing warnings
 */
export function splitSqlStatementsWithWarnings(sql: string): ParseResult {
  const statements: ParsedStatement[] = [];
  const warnings: ParseWarning[] = [];
  let current = '';
  let currentLineNumber = 1;
  let statementStartLine = 1;
  let hasStatementContent = false; // Track if we've seen actual SQL content (not just comments)
  let inString = false;
  let stringChar = '';
  let stringStartLine = 0;
  let stringStartPos = 0;
  let inLineComment = false;
  let blockCommentDepth = 0;
  let blockCommentStartLine = 0;
  let blockCommentStartPos = 0;
  let i = 0;

  while (i < sql.length) {
    const char = sql[i];
    const nextChar = sql[i + 1] || '';

    // Track line numbers (ALL newlines count, including in comments)
    if (char === '\n') {
      currentLineNumber++;
    }

    // Check if we're starting actual SQL content (not in comment, not whitespace)
    // This determines the accurate line number for the statement
    const inComment = inLineComment || blockCommentDepth > 0;
    if (!hasStatementContent && !inComment && char.trim() !== '') {
      // Check if this is starting a new comment - if so, don't set statement start yet
      const isStartingLineComment = char === '-' && nextChar === '-';
      const isStartingBlockComment = char === '/' && nextChar === '*';
      if (!isStartingLineComment && !isStartingBlockComment) {
        hasStatementContent = true;
        statementStartLine = currentLineNumber;
      }
    }

    // Handle line comments
    if (!inString && blockCommentDepth === 0 && char === '-' && nextChar === '-') {
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

    // Handle nested block comments
    if (!inString && !inLineComment && char === '/' && nextChar === '*') {
      if (blockCommentDepth === 0) {
        blockCommentStartLine = currentLineNumber;
        blockCommentStartPos = i;
      }
      blockCommentDepth++;
      current += char + nextChar;
      i += 2;
      continue;
    }

    if (blockCommentDepth > 0 && char === '*' && nextChar === '/') {
      blockCommentDepth--;
      current += char + nextChar;
      i += 2;
      continue;
    }

    // Handle string literals
    if (!inLineComment && blockCommentDepth === 0 && (char === "'" || char === '"')) {
      if (!inString) {
        inString = true;
        stringChar = char;
        stringStartLine = currentLineNumber;
        stringStartPos = i;
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
    if (!inString && !inLineComment && blockCommentDepth === 0 && char === '$') {
      const dollarMatch = /^(\$\w*\$)/.exec(sql.slice(i));
      if (dollarMatch) {
        const dollarTag = dollarMatch[1];
        const searchStart = i + dollarTag.length;
        const endIndex = sql.indexOf(dollarTag, searchStart);

        if (endIndex !== -1) {
          const dollarContent = sql.slice(i, endIndex + dollarTag.length);
          // Count newlines in dollar-quoted content for accurate line tracking
          const newlines = (dollarContent.match(/\n/g) || []).length;
          currentLineNumber += newlines;
          current += dollarContent;
          i = endIndex + dollarTag.length;
          continue;
        } else {
          // Unclosed dollar-quote - add warning and consume to end
          warnings.push({
            type: 'unclosed_dollar_quote',
            message: `Unclosed dollar-quote ${dollarTag} starting at line ${currentLineNumber}`,
            lineNumber: currentLineNumber,
            position: i,
            tag: dollarTag,
          });
          // Consume everything to the end as part of this statement
          const remainder = sql.slice(i);
          const newlines = (remainder.match(/\n/g) || []).length;
          currentLineNumber += newlines;
          current += remainder;
          i = sql.length;
          continue;
        }
      }
    }

    // Handle statement separator
    if (!inString && !inLineComment && blockCommentDepth === 0 && char === ';') {
      current += char;
      const trimmed = current.trim();
      if (trimmed) {
        statements.push({ sql: trimmed, lineNumber: statementStartLine });
      }
      current = '';
      hasStatementContent = false; // Reset for next statement
      i++;
      continue;
    }

    current += char;
    i++;
  }

  // Check for unclosed constructs at end of input
  if (blockCommentDepth > 0) {
    warnings.push({
      type: 'unclosed_block_comment',
      message: `Unclosed block comment starting at line ${blockCommentStartLine}`,
      lineNumber: blockCommentStartLine,
      position: blockCommentStartPos,
    });
  }

  if (inString) {
    warnings.push({
      type: 'unclosed_string',
      message: `Unclosed string literal starting at line ${stringStartLine}`,
      lineNumber: stringStartLine,
      position: stringStartPos,
    });
  }

  // Add remaining content if any
  const trimmed = current.trim();
  if (trimmed) {
    statements.push({ sql: trimmed, lineNumber: statementStartLine });
  }

  return { statements, warnings };
}

/**
 * Table reference extracted from SQL.
 */
export interface TableReference {
  schema: string;
  table: string;
}

/**
 * Extracts table names from a SQL query.
 * Handles common patterns:
 * - FROM, JOIN, INTO, UPDATE, DELETE FROM
 * - MERGE INTO/USING
 * - COPY (table) FROM/TO
 * - LATERAL subquery references
 * - USING clause in JOINs
 *
 * @param sql - The SQL query to analyze
 * @returns Array of unique table references
 */
export function extractTablesFromSql(sql: string): TableReference[] {
  const tables: TableReference[] = [];
  const seen = new Set<string>();

  // Normalize SQL: remove comments and extra whitespace
  const normalized = sql
    .replace(/--[^\n]*/g, '') // Remove line comments
    .replace(/\/\*[\s\S]*?\*\//g, '') // Remove block comments
    .replace(/\s+/g, ' ') // Normalize whitespace
    .trim();

  // Pattern for SQL identifier: optional quotes, word chars, optional schema.table
  const identPattern = '(["`]?\\w+["`]?(?:\\.\\s*["`]?\\w+["`]?)?)';

  // Patterns to find table references
  const patterns = [
    // Standard patterns
    new RegExp(`\\bFROM\\s+${identPattern}`, 'gi'),
    new RegExp(`\\bJOIN\\s+${identPattern}`, 'gi'),
    new RegExp(`\\bINTO\\s+${identPattern}`, 'gi'),
    new RegExp(`\\bUPDATE\\s+${identPattern}`, 'gi'),
    new RegExp(`\\bDELETE\\s+FROM\\s+${identPattern}`, 'gi'),
    // MERGE statement patterns
    new RegExp(`\\bMERGE\\s+INTO\\s+${identPattern}`, 'gi'),
    new RegExp(`\\bUSING\\s+${identPattern}`, 'gi'),
    // COPY statement pattern
    new RegExp(`\\bCOPY\\s+${identPattern}`, 'gi'),
    // TABLE keyword (for COPY TABLE, etc.)
    new RegExp(`\\bTABLE\\s+${identPattern}`, 'gi'),
  ];

  for (const pattern of patterns) {
    let match;
    while ((match = pattern.exec(normalized)) !== null) {
      const tableRef = match[1].replace(/["`]/g, '').trim();

      // Skip common SQL keywords that might be matched
      if (SQL_KEYWORDS_TO_SKIP.has(tableRef.toUpperCase())) {
        continue;
      }

      // Skip if it looks like a function call (ends with parenthesis nearby)
      const afterMatch = normalized.slice(match.index + match[0].length).trim();
      if (afterMatch.startsWith('(')) {
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
 * Filter parsed statements to only executable ones (non-empty, non-comment).
 *
 * @param statements - Array of parsed statements
 * @returns Filtered array of executable statements
 */
export function filterExecutableStatements(statements: ParsedStatement[]): ParsedStatement[] {
  return statements.filter((stmt) => {
    const trimmed = stmt.sql.trim();
    if (!trimmed) return false;
    const withoutComments = stripLeadingComments(trimmed);
    return withoutComments.length > 0;
  });
}

/**
 * Normalize SQL by removing comments and extra whitespace.
 * Properly handles nested block comments.
 *
 * @param sql - The SQL to normalize
 * @returns Normalized SQL string
 */
export function normalizeSql(sql: string): string {
  // Remove nested block comments
  let result = sql;
  let prevLength = -1;

  // Keep removing until no more changes (handles nested comments)
  while (result.length !== prevLength) {
    prevLength = result.length;
    // Remove innermost comments first
    result = result.replace(/\/\*[^/*]*\*\//g, '');
  }

  return result
    .replace(/--[^\n]*/g, '') // Remove line comments
    .replace(/\s+/g, ' ')     // Normalize whitespace
    .trim();
}

/**
 * Checks if SQL content is empty after preprocessing (removing comments/whitespace).
 *
 * @param sql - The SQL to check
 * @returns true if SQL is effectively empty, false otherwise
 */
export function isEmptyAfterPreprocessing(sql: string): boolean {
  const stripped = stripLeadingComments(sql);
  return stripped.trim().length === 0;
}

/**
 * Detects potential schema change statements in SQL.
 * Useful for warning about schema changes during multi-statement execution.
 *
 * @param sql - The SQL statement to check
 * @returns Object with detected schema changes
 */
export function detectSchemaChanges(sql: string): {
  hasSchemaChange: boolean;
  changeType?: 'search_path' | 'schema_create' | 'schema_drop' | 'schema_alter';
  newSchema?: string;
} {
  const normalized = normalizeSql(sql).toUpperCase();

  // Check for SET search_path
  const searchPathRegex = /SET\s+(?:LOCAL\s+)?SEARCH_PATH\s*(?:TO|=)\s*(\w+)/;
  const searchPathMatch = searchPathRegex.exec(normalized);
  if (searchPathMatch) {
    return {
      hasSchemaChange: true,
      changeType: 'search_path',
      newSchema: searchPathMatch[1].toLowerCase(),
    };
  }

  // Check for CREATE SCHEMA
  if (/CREATE\s+SCHEMA\b/.test(normalized)) {
    return { hasSchemaChange: true, changeType: 'schema_create' };
  }

  // Check for DROP SCHEMA
  if (/DROP\s+SCHEMA\b/.test(normalized)) {
    return { hasSchemaChange: true, changeType: 'schema_drop' };
  }

  // Check for ALTER SCHEMA
  if (/ALTER\s+SCHEMA\b/.test(normalized)) {
    return { hasSchemaChange: true, changeType: 'schema_alter' };
  }

  return { hasSchemaChange: false };
}
