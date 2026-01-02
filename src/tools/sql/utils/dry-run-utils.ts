/**
 * Dry-Run Utilities
 *
 * Helper functions for transaction-based dry-run operations.
 * Handles error extraction and detection of non-rollbackable operations.
 */

import { DryRunError, NonRollbackableWarning } from '../../../types.js';

/** Valid operation types for non-rollbackable warnings */
type OperationType = NonRollbackableWarning['operation'];

/**
 * Non-rollbackable operation patterns.
 * Operations that cannot run inside a transaction or have permanent side effects.
 */
const NON_ROLLBACKABLE_PATTERNS: Array<{
  pattern: RegExp;
  operation: OperationType;
  message: string;
  mustSkip: boolean;
}> = [
  // Operations that cannot run inside a transaction at all
  {
    pattern: /\bVACUUM\b/,
    operation: 'VACUUM',
    message: 'VACUUM cannot run inside a transaction block. Statement skipped.',
    mustSkip: true,
  },
  {
    pattern: /\bCLUSTER\b(?!.*CREATE)/,
    operation: 'CLUSTER',
    message: 'CLUSTER cannot run inside a transaction block. Statement skipped.',
    mustSkip: true,
  },
  {
    pattern: /\bREINDEX\b.*\bCONCURRENTLY\b/,
    operation: 'REINDEX_CONCURRENTLY',
    message: 'REINDEX CONCURRENTLY cannot run inside a transaction block. Statement skipped.',
    mustSkip: true,
  },
  {
    pattern: /\bCREATE\s+INDEX\b.*\bCONCURRENTLY\b/,
    operation: 'CREATE_INDEX_CONCURRENTLY',
    message: 'CREATE INDEX CONCURRENTLY cannot run inside a transaction block. Statement skipped.',
    mustSkip: true,
  },
  {
    pattern: /\bCREATE\s+DATABASE\b/,
    operation: 'CREATE_DATABASE',
    message: 'CREATE DATABASE cannot run inside a transaction block. Statement skipped.',
    mustSkip: true,
  },
  {
    pattern: /\bDROP\s+DATABASE\b/,
    operation: 'DROP_DATABASE',
    message: 'DROP DATABASE cannot run inside a transaction block. Statement skipped.',
    mustSkip: true,
  },
  // Operations with permanent side effects
  {
    pattern: /\bNEXTVAL\s*\(/,
    operation: 'SEQUENCE',
    message: 'NEXTVAL increments sequence even when transaction is rolled back. Statement skipped to prevent sequence consumption.',
    mustSkip: true,
  },
  {
    pattern: /\bSETVAL\s*\(/,
    operation: 'SEQUENCE',
    message: 'SETVAL modifies sequence. Statement skipped to prevent side effects.',
    mustSkip: true,
  },
  // Warning-only operations (still executed)
  {
    pattern: /\bINSERT\s+INTO\b/,
    operation: 'SEQUENCE',
    message: 'INSERT may consume sequence values (for SERIAL/BIGSERIAL columns) even when rolled back.',
    mustSkip: false,
  },
  {
    pattern: /\bNOTIFY\b/,
    operation: 'NOTIFY',
    message: 'NOTIFY sends notifications on commit. Since dry-run rolls back, notifications will NOT be sent.',
    mustSkip: false,
  },
];

/**
 * Extract detailed error information from a PostgreSQL error.
 * Captures all available fields to help AI quickly identify and fix issues.
 */
export function extractDryRunError(error: unknown): DryRunError {
  // Extract message - prioritize Error.message, then object.message, then String conversion
  let message: string;
  if (error instanceof Error) {
    message = error.message;
  } else if (error && typeof error === 'object' && 'message' in error) {
    message = String((error as { message: unknown }).message);
  } else {
    message = String(error);
  }

  const result: DryRunError = { message };

  if (error && typeof error === 'object') {
    const pgError = error as Record<string, unknown>;

    // Extract string fields
    if (pgError.code) result.code = String(pgError.code);
    if (pgError.severity) result.severity = String(pgError.severity);
    if (pgError.detail) result.detail = String(pgError.detail);
    if (pgError.hint) result.hint = String(pgError.hint);
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

    // Extract number fields
    if (pgError.position !== undefined) result.position = Number(pgError.position);
    if (pgError.internalPosition !== undefined) result.internalPosition = Number(pgError.internalPosition);
  }

  return result;
}

/**
 * Check if a SQL statement contains operations that cannot be fully rolled back
 * or have side effects even within a transaction.
 *
 * @param sql - The SQL statement to check
 * @param statementIndex - Optional statement index for error reporting
 * @param lineNumber - Optional line number for error reporting
 * @returns Array of warnings about non-rollbackable operations
 */
export function detectNonRollbackableOperations(
  sql: string,
  statementIndex?: number,
  lineNumber?: number
): NonRollbackableWarning[] {
  const warnings: NonRollbackableWarning[] = [];
  const upperSql = sql.toUpperCase().trim();

  const clusterPattern = /\bCLUSTER\b/;

  for (const { pattern, operation, message, mustSkip } of NON_ROLLBACKABLE_PATTERNS) {
    // Special handling for CLUSTER (must not be part of CREATE)
    if (operation === 'CLUSTER') {
      if (clusterPattern.test(upperSql) && !upperSql.includes('CREATE')) {
        warnings.push({ operation, message, statementIndex, lineNumber, mustSkip });
      }
    } else if (pattern.test(upperSql)) {
      warnings.push({ operation, message, statementIndex, lineNumber, mustSkip });
    }
  }

  return warnings;
}

/**
 * Check if any warning requires skipping the statement.
 */
export function hasMustSkipWarning(warnings: NonRollbackableWarning[]): boolean {
  return warnings.some((w) => w.mustSkip);
}

/**
 * Get skip reason from must-skip warnings.
 */
export function getSkipReason(warnings: NonRollbackableWarning[]): string {
  return warnings
    .filter((w) => w.mustSkip)
    .map((w) => w.message)
    .join('; ');
}
