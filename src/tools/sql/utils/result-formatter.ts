/**
 * Result Formatter Utilities
 *
 * Functions for formatting query results, handling large outputs,
 * and timing measurements.
 */

import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { v4 as uuidv4 } from 'uuid';
import { MAX_OUTPUT_CHARS } from './constants.js';

/**
 * Result of large output handling.
 */
export interface LargeOutputResult {
  truncated: boolean;
  outputFile?: string;
  rows: unknown[];
}

/**
 * Calculate execution time from hrtime bigints.
 *
 * @param startTime - Start time from process.hrtime.bigint()
 * @param endTime - End time from process.hrtime.bigint()
 * @returns Execution time in milliseconds (rounded to 2 decimal places)
 */
export function calculateExecutionTime(startTime: bigint, endTime: bigint): number {
  return Math.round((Number(endTime - startTime) / 1_000_000) * 100) / 100;
}

/**
 * Get current time for timing measurements.
 *
 * @returns Current high-resolution time as bigint
 */
export function getStartTime(): bigint {
  return process.hrtime.bigint();
}

/**
 * Handle potentially large output by writing to file if necessary.
 *
 * @param rows - Result rows to check
 * @param maxChars - Maximum characters before writing to file
 * @returns Object with truncated flag, optional file path, and rows
 */
export function handleLargeOutput(
  rows: unknown[],
  maxChars: number = MAX_OUTPUT_CHARS
): LargeOutputResult {
  const output = JSON.stringify(rows);

  if (output.length <= maxChars) {
    return { truncated: false, rows };
  }

  // Write to temp file
  const tempDir = os.tmpdir();
  const fileName = `sql-result-${uuidv4()}.json`;
  const filePath = path.join(tempDir, fileName);

  fs.writeFileSync(filePath, output, { mode: 0o600 });

  return {
    truncated: true,
    outputFile: filePath,
    rows: [], // Return empty rows since output is in file
  };
}

/**
 * Paginate result rows.
 *
 * @param rows - All result rows
 * @param offset - Number of rows to skip
 * @param maxRows - Maximum rows to return
 * @returns Paginated rows and metadata
 */
export function paginateRows(
  rows: unknown[],
  offset: number,
  maxRows: number
): {
  rows: unknown[];
  offset: number;
  hasMore: boolean;
  totalCount: number;
} {
  const totalCount = rows.length;
  const paginatedRows = rows.slice(offset, offset + maxRows);
  const hasMore = offset + paginatedRows.length < totalCount;

  return {
    rows: paginatedRows,
    offset,
    hasMore,
    totalCount,
  };
}

/**
 * Truncate SQL for display in error messages or previews.
 *
 * @param sql - SQL string to truncate
 * @param maxLength - Maximum length (default: 200)
 * @returns Truncated SQL with ellipsis if needed
 */
export function truncateSql(sql: string, maxLength: number = 200): string {
  const trimmed = sql.trim();
  if (trimmed.length <= maxLength) {
    return trimmed;
  }
  return trimmed.substring(0, maxLength) + '...';
}

/**
 * Format field names from query result.
 *
 * @param fields - Array of field objects from pg result
 * @returns Array of field names
 */
export function formatFieldNames(fields: Array<{ name: string }>): string[] {
  return fields.map((f) => f.name);
}

/**
 * Create a summary message for statement execution.
 *
 * @param totalStatements - Total number of statements
 * @param successCount - Number of successful statements
 * @param failureCount - Number of failed statements
 * @param skippedCount - Number of skipped statements
 * @param rolledBack - Whether changes were rolled back
 * @returns Summary message string
 */
export function createExecutionSummary(
  totalStatements: number,
  successCount: number,
  failureCount: number,
  skippedCount: number,
  rolledBack: boolean
): string {
  const parts: string[] = [];

  if (rolledBack) {
    parts.push(`Dry-run of ${totalStatements} statements:`);
  } else {
    parts.push(`Executed ${totalStatements} statements:`);
  }

  if (successCount > 0) {
    parts.push(`${successCount} succeeded`);
  }
  if (failureCount > 0) {
    parts.push(`${failureCount} failed`);
  }
  if (skippedCount > 0) {
    parts.push(`${skippedCount} skipped (non-rollbackable)`);
  }

  if (rolledBack) {
    parts.push('All changes rolled back.');
  }

  return parts.join(', ').replace('statements:,', 'statements:');
}

/**
 * Count statements by type.
 *
 * @param types - Array of statement types
 * @returns Object with counts by type
 */
export function countStatementsByType(types: string[]): Record<string, number> {
  const counts: Record<string, number> = {};

  for (const type of types) {
    counts[type] = (counts[type] || 0) + 1;
  }

  return counts;
}

/**
 * Create a human-readable file summary.
 *
 * @param statementsByType - Statement counts by type
 * @param totalStatements - Total statement count
 * @returns Summary string
 */
export function createFileSummary(
  statementsByType: Record<string, number>,
  totalStatements: number
): string {
  const parts = Object.entries(statementsByType)
    .sort(([, a], [, b]) => b - a)
    .map(([type, count]) => `${count} ${type}`)
    .join(', ');

  return `File contains ${totalStatements} statements: ${parts}`;
}
