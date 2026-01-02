/**
 * SQL Parser Utilities
 *
 * Functions for parsing, analyzing, and splitting SQL statements.
 * Handles PostgreSQL-specific syntax including dollar-quoted strings.
 */

import { ParsedStatement } from '../../../types.js';

/** Common SQL statement types for detection */
const STATEMENT_TYPES = [
  'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP',
  'TRUNCATE', 'GRANT', 'REVOKE', 'BEGIN', 'COMMIT', 'ROLLBACK',
  'SET', 'SHOW', 'EXPLAIN', 'ANALYZE', 'VACUUM', 'REINDEX',
  'COMMENT', 'WITH', 'DO', 'CALL', 'EXECUTE',
] as const;

/** SQL keywords to skip when extracting table names */
const SQL_KEYWORDS_TO_SKIP = new Set([
  'SELECT', 'WHERE', 'SET', 'VALUES', 'AND', 'OR',
]);

/**
 * Strips leading line comments and block comments from SQL to check if there's actual SQL.
 * Returns empty string if the entire content is just comments.
 *
 * @param sql - The SQL string to process
 * @returns The SQL with leading comments stripped
 */
export function stripLeadingComments(sql: string): string {
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
 * Detect the type of SQL statement (SELECT, INSERT, UPDATE, DELETE, CREATE, etc.)
 *
 * @param sql - The SQL statement to analyze
 * @returns The detected statement type
 */
export function detectStatementType(sql: string): string {
  const trimmed = stripLeadingComments(sql).toUpperCase();

  for (const type of STATEMENT_TYPES) {
    if (
      trimmed.startsWith(type + ' ') ||
      trimmed.startsWith(type + '\n') ||
      trimmed.startsWith(type + '\t') ||
      trimmed === type
    ) {
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
 * Split SQL content into individual statements with line number tracking.
 * Handles PostgreSQL-specific syntax including:
 * - Single and double-quoted strings with escape handling
 * - Dollar-quoted strings ($$ or $tag$)
 * - Line comments (--)
 * - Block comments
 *
 * @param sql - The SQL content to split
 * @returns Array of parsed statements with line numbers
 */
export function splitSqlStatementsWithLineNumbers(sql: string): ParsedStatement[] {
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
      const dollarMatch = /^(\$\w*\$)/.exec(sql.slice(i));
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
 * Table reference extracted from SQL.
 */
export interface TableReference {
  schema: string;
  table: string;
}

/**
 * Extracts table names from a SQL query.
 * Handles common patterns: FROM, JOIN, INTO, UPDATE, DELETE FROM
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
    new RegExp(`\\bFROM\\s+${identPattern}`, 'gi'),
    new RegExp(`\\bJOIN\\s+${identPattern}`, 'gi'),
    new RegExp(`\\bINTO\\s+${identPattern}`, 'gi'),
    new RegExp(`\\bUPDATE\\s+${identPattern}`, 'gi'),
    new RegExp(`\\bDELETE\\s+FROM\\s+${identPattern}`, 'gi'),
  ];

  for (const pattern of patterns) {
    let match;
    while ((match = pattern.exec(normalized)) !== null) {
      const tableRef = match[1].replace(/["`]/g, '').trim();

      // Skip common SQL keywords that might be matched
      if (SQL_KEYWORDS_TO_SKIP.has(tableRef.toUpperCase())) {
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
 *
 * @param sql - The SQL to normalize
 * @returns Normalized SQL string
 */
export function normalizeSql(sql: string): string {
  return sql
    .replace(/--[^\n]*/g, '')
    .replace(/\/\*[\s\S]*?\*\//g, '')
    .replace(/\s+/g, ' ')
    .trim();
}
