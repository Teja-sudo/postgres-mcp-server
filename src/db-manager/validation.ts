/**
 * Database Manager Validation Utilities
 *
 * Centralized validation functions for database and schema names.
 * Prevents SQL injection and ensures PostgreSQL-compatible identifiers.
 */

/** Pattern for valid database names: start with letter/underscore, alphanumeric/underscore/hyphen */
const DATABASE_NAME_PATTERN = /^[a-zA-Z_][a-zA-Z0-9_-]*$/;

/** Pattern for SQL injection characters that must not appear in database names */
const SQL_INJECTION_PATTERN = /--|;|'|"|`/;

/** Pattern for valid schema names: start with letter/underscore, alphanumeric/underscore only */
const SCHEMA_NAME_PATTERN = /^[a-zA-Z_]\w*$/;

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
