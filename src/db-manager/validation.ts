/**
 * Database Manager Validation Utilities
 *
 * Centralized validation functions for database and schema names.
 * Prevents SQL injection and ensures PostgreSQL-compatible identifiers.
 */

/**
 * Pattern for valid database names: any combination of letters, digits,
 * underscores, and hyphens.
 *
 * Hotfix-3.0.3: leading digit allowed. Database names in PostgreSQL can
 * start with a digit when quoted, and these values always pass through
 * `escapeIdentifier()` before reaching SQL — so we don't need to require
 * unquoted-identifier form here. Real-world examples of digit-leading
 * names (numeric tenant IDs, date-stamped DBs) were rejected by the
 * previous pattern.
 */
const DATABASE_NAME_PATTERN = /^[a-zA-Z0-9_][a-zA-Z0-9_-]*$/;

/** Pattern for SQL injection characters that must not appear in database names */
const SQL_INJECTION_PATTERN = /--|;|'|"|`/;

/**
 * Pattern for valid schema names: any combination of letters, digits,
 * and underscores (no hyphens — PG schemas almost never use them).
 *
 * Hotfix-3.0.3: leading digit allowed (same reasoning as
 * DATABASE_NAME_PATTERN — value is double-quoted via escapeIdentifier
 * before reaching SQL).
 */
const SCHEMA_NAME_PATTERN = /^[a-zA-Z0-9_]\w*$/;

/**
 * Validates a database name for PostgreSQL compatibility and SQL injection prevention.
 *
 * @param name - The database name to validate
 * @throws Error if the database name is invalid
 */
export function validateDatabaseName(name: string): void {
  if (!DATABASE_NAME_PATTERN.test(name) || SQL_INJECTION_PATTERN.test(name)) {
    throw new Error(
      'Invalid database name. Allowed: letters, digits, underscores, hyphens. ' +
        'Cannot contain SQL characters (;, --, quotes).'
    );
  }
}

/**
 * Validates a schema name for PostgreSQL compatibility.
 *
 * @param name - The schema name to validate
 * @throws Error if the schema name is invalid
 */
export function validateSchemaName(name: string): void {
  if (!SCHEMA_NAME_PATTERN.test(name)) {
    throw new Error('Invalid schema name. Only alphanumeric characters and underscores are allowed.');
  }
}

/**
 * Checks if a database name is valid without throwing.
 *
 * @param name - The database name to check
 * @returns true if valid, false otherwise
 */
export function isValidDatabaseName(name: string): boolean {
  return DATABASE_NAME_PATTERN.test(name) && !SQL_INJECTION_PATTERN.test(name);
}

/**
 * Checks if a schema name is valid without throwing.
 *
 * @param name - The schema name to check
 * @returns true if valid, false otherwise
 */
export function isValidSchemaName(name: string): boolean {
  return SCHEMA_NAME_PATTERN.test(name);
}
