/**
 * SQL Tools Constants
 *
 * Centralized configuration values for SQL execution tools.
 * All magic numbers are defined here for maintainability.
 */

/** Maximum characters in response before writing to temp file */
export const MAX_OUTPUT_CHARS = 50000;

/** Default maximum rows to return in direct response */
export const MAX_ROWS_DEFAULT = 1000;

/** Absolute maximum rows allowed */
export const MAX_ROWS_LIMIT = 100000;

/** Default SQL query length limit (100KB) */
export const DEFAULT_SQL_LENGTH_LIMIT = 100000;

/** Maximum number of query parameters */
export const MAX_PARAMS = 100;

/** Maximum SQL file size (50MB) */
export const MAX_SQL_FILE_SIZE = 50 * 1024 * 1024;

/** Maximum sample rows to return in dry-run */
export const MAX_DRY_RUN_SAMPLE_ROWS = 10;

/** Maximum statements to preview in file preview */
export const MAX_PREVIEW_STATEMENTS = 100;

/** Default statements to show in preview */
export const DEFAULT_PREVIEW_STATEMENTS = 20;

/** Maximum batch queries in single call */
export const MAX_BATCH_QUERIES = 20;

/** Maximum sample size for mutation preview */
export const MAX_MUTATION_SAMPLE_SIZE = 20;

/** Default sample size for mutation preview */
export const DEFAULT_MUTATION_SAMPLE_SIZE = 5;

/** Transaction timeout in milliseconds (10 minutes for active operations) */
export const TRANSACTION_TIMEOUT_MS = 10 * 60 * 1000;

/** Transaction auto-cleanup timeout in milliseconds (45 minutes) */
export const TRANSACTION_CLEANUP_TIMEOUT_MS = 45 * 60 * 1000;

/** Transaction cleanup check interval in milliseconds (5 minutes) */
export const TRANSACTION_CLEANUP_INTERVAL_MS = 5 * 60 * 1000;

/** Maximum SQL length for ReDoS-safe regex operations (100KB) */
export const MAX_SQL_LENGTH_FOR_REGEX = 100000;

/** Maximum hypothetical indexes per query */
export const MAX_HYPOTHETICAL_INDEXES = 10;

/** Maximum columns per hypothetical index */
export const MAX_HYPOTHETICAL_INDEX_COLUMNS = 10;

/** Maximum statements in dry-run file results */
export const MAX_DRY_RUN_STATEMENTS = 200;

/** Default statements in dry-run file results */
export const DEFAULT_DRY_RUN_STATEMENTS = 50;

/** Maximum rows per statement in multi-statement execution */
export const MAX_ROWS_PER_STATEMENT = 100;

/** Short SQL truncation limit for display */
export const SQL_TRUNCATION_SHORT = 200;

/** Long SQL truncation limit for detailed display */
export const SQL_TRUNCATION_LONG = 300;

/** Maximum tables to analyze for schema hints */
export const MAX_TABLES_TO_ANALYZE = 10;
