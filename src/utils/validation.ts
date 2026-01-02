/**
 * Validates and sanitizes PostgreSQL identifiers (table names, column names, etc.)
 * Prevents SQL injection by only allowing valid identifier characters.
 */
export function validateIdentifier(identifier: string, fieldName: string): string {
  if (!identifier || typeof identifier !== 'string') {
    throw new Error(`${fieldName} is required and must be a string`);
  }

  // PostgreSQL identifier rules:
  // - Must start with a letter (a-z) or underscore
  // - Can contain letters, digits, underscores, and dollar signs
  // - Maximum 63 characters
  // We're being more restrictive for security
  const validIdentifierRegex = /^[a-zA-Z_][a-zA-Z0-9_$]*$/;

  if (identifier.length > 63) {
    throw new Error(`${fieldName} must be 63 characters or less`);
  }

  if (!validIdentifierRegex.test(identifier)) {
    throw new Error(`${fieldName} contains invalid characters. Only letters, numbers, underscores, and dollar signs are allowed, and it must start with a letter or underscore.`);
  }

  return identifier;
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
 * Validates SQL for read-only operations more thoroughly.
 * Returns true if the SQL appears to be read-only, false otherwise.
 */
export function isReadOnlySql(sql: string): { isReadOnly: boolean; reason?: string } {
  if (!sql || typeof sql !== 'string') {
    return { isReadOnly: false, reason: 'SQL is required' };
  }

  // Normalize: remove comments, extra whitespace
  const normalizedSql = sql
    // Remove single-line comments
    .replace(/--[^\n]*/g, '')
    // Remove multi-line comments
    .replace(/\/\*[\s\S]*?\*\//g, '')
    // Normalize whitespace
    .replace(/\s+/g, ' ')
    .trim()
    .toUpperCase();

  // Check for write operations at any position (not just start)
  const writeOperations = [
    'INSERT',
    'UPDATE',
    'DELETE',
    'DROP',
    'CREATE',
    'ALTER',
    'TRUNCATE',
    'GRANT',
    'REVOKE',
    'COPY',
    'VACUUM',
    'REINDEX',
    'CLUSTER',
    'REFRESH MATERIALIZED VIEW',
    'LOCK',
    'DISCARD',
    'RESET',
    'SET ',  // Note: space to avoid matching in column names
    'DO ',   // Anonymous code blocks
    'CALL',  // Stored procedures
  ];

  // Check if any write operation appears as a keyword (word boundary check)
  for (const op of writeOperations) {
    // Check at start or after common statement separators
    const patterns = [
      new RegExp(`^${op}\\b`),           // At start
      new RegExp(`\\(\\s*${op}\\b`),     // After opening paren (subquery)
      new RegExp(`;\\s*${op}\\b`),       // After semicolon
      new RegExp(`\\bWITH\\b.*\\bAS\\s*\\(\\s*${op}\\b`, 's'), // In CTE
    ];

    for (const pattern of patterns) {
      if (pattern.test(normalizedSql)) {
        return { isReadOnly: false, reason: `Write operation '${op.trim()}' detected` };
      }
    }
  }

  // Check for function calls that could have side effects
  const dangerousFunctions = [
    'LO_IMPORT',
    'LO_EXPORT',
    'LO_UNLINK',
    'PG_READ_FILE',
    'PG_READ_BINARY_FILE',
    'PG_WRITE_FILE',
    'DBLINK_EXEC',
    'DBLINK',
  ];

  for (const func of dangerousFunctions) {
    if (new RegExp(`\\b${func}\\s*\\(`).test(normalizedSql)) {
      return { isReadOnly: false, reason: `Dangerous function '${func}' detected` };
    }
  }

  return { isReadOnly: true };
}

/**
 * Validates that a number is within acceptable bounds.
 */
export function validatePositiveInteger(value: any, fieldName: string, min: number = 1, max: number = 10000): number {
  if (value === undefined || value === null) {
    return min;
  }

  const num = parseInt(value, 10);
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
