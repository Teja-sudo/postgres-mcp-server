import { getDbManager } from '../db-manager.js';
import { SchemaInfo, TableInfo, ColumnInfo, ConstraintInfo, IndexInfo, ConnectionOverride, PaginatedResult } from '../types.js';
import { validateIdentifier, validatePositiveInteger } from '../utils/validation.js';

/** Default pagination limit for listObjects */
const DEFAULT_LIST_LIMIT = 100;
/** Maximum pagination limit for listObjects */
const MAX_LIST_LIMIT = 1000;

/** Object type for listObjects */
type ObjectType = 'table' | 'view' | 'sequence' | 'extension' | 'all';

/**
 * Validates list objects filter parameter.
 */
function validateFilter(filter: string | undefined): void {
  if (!filter) return;
  if (filter.length > 128) {
    throw new Error('filter must be 128 characters or less');
  }
  if (!/^[a-zA-Z0-9_% ]+$/.test(filter)) {
    throw new Error('filter contains invalid characters');
  }
}

/**
 * Builds a single UNION query part for a specific object type.
 */
function buildObjectTypeQuery(
  type: 'table' | 'view' | 'sequence' | 'extension',
  hasFilter: boolean
): string {
  const filterClause = hasFilter ? "AND %NAME% ILIKE '%' || $2 || '%'" : '';

  switch (type) {
    case 'table':
      return `
        SELECT
          tablename as name,
          'table'::text as type,
          tableowner as owner,
          schemaname as schema
        FROM pg_catalog.pg_tables
        WHERE schemaname = $1
        ${filterClause.replace('%NAME%', 'tablename')}
      `;
    case 'view':
      return `
        SELECT
          v.table_name as name,
          'view'::text as type,
          COALESCE(c.relowner::regrole::text, '') as owner,
          v.table_schema as schema
        FROM information_schema.views v
        LEFT JOIN pg_class c ON c.relname = v.table_name
        LEFT JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = v.table_schema
        WHERE v.table_schema = $1
        ${filterClause.replace('%NAME%', 'v.table_name')}
      `;
    case 'sequence':
      return `
        SELECT
          s.sequence_name as name,
          'sequence'::text as type,
          COALESCE(c.relowner::regrole::text, '') as owner,
          s.sequence_schema as schema
        FROM information_schema.sequences s
        LEFT JOIN pg_class c ON c.relname = s.sequence_name
        LEFT JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = s.sequence_schema
        WHERE s.sequence_schema = $1
        ${filterClause.replace('%NAME%', 's.sequence_name')}
      `;
    case 'extension':
      return `
        SELECT
          extname as name,
          'extension'::text as type,
          COALESCE(extowner::regrole::text, '') as owner,
          n.nspname as schema
        FROM pg_extension e
        JOIN pg_namespace n ON e.extnamespace = n.oid
        WHERE n.nspname = $1
        ${filterClause.replace('%NAME%', 'extname')}
      `;
  }
}

/**
 * Collects union query parts based on requested object type.
 */
function collectUnionParts(objectType: ObjectType, hasFilter: boolean): string[] {
  const parts: string[] = [];
  const types: Array<'table' | 'view' | 'sequence' | 'extension'> = ['table', 'view', 'sequence', 'extension'];

  for (const type of types) {
    if (objectType === 'all' || objectType === type) {
      parts.push(buildObjectTypeQuery(type, hasFilter));
    }
  }

  return parts;
}

export async function listSchemas(args: {
  includeSystemSchemas?: boolean;
  // Connection override parameters
  server?: string;
  database?: string;
  schema?: string;
}): Promise<SchemaInfo[]> {
  const dbManager = getDbManager();

  // Build connection override if specified
  const hasOverride = args.server || args.database || args.schema;
  const override: ConnectionOverride | undefined = hasOverride
    ? { server: args.server, database: args.database, schema: args.schema }
    : undefined;

  let query = `
    SELECT
      schema_name,
      schema_owner as owner
    FROM information_schema.schemata
  `;

  if (!args.includeSystemSchemas) {
    query += `
      WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
      AND schema_name NOT LIKE 'pg_%'
    `;
  }

  query += ' ORDER BY schema_name';

  const result = await dbManager.queryWithOverride<SchemaInfo>(query, undefined, override);
  return result.rows;
}

/**
 * Lists database objects (tables, views, sequences, extensions) in a schema.
 * Supports filtering by object type, name pattern, and pagination.
 *
 * @param args - Query parameters including schema, filters, and pagination
 * @returns Paginated result with objects and metadata
 */
export async function listObjects(args: {
  /** Schema name to list objects from (required) */
  schema: string;
  /** Filter by object type (default: 'all') */
  objectType?: 'table' | 'view' | 'sequence' | 'extension' | 'all';
  /** Filter objects by name pattern (ILIKE matching) */
  filter?: string;
  /** Maximum number of objects to return (default: 100, max: 1000) */
  limit?: number;
  /** Number of objects to skip for pagination (default: 0) */
  offset?: number;
  // Connection override parameters
  server?: string;
  database?: string;
  targetSchema?: string; // Use targetSchema to avoid confusion with the required schema param
}): Promise<PaginatedResult<TableInfo>> {
  // Validate required parameters
  if (!args.schema) {
    throw new Error('schema parameter is required');
  }

  // Validate schema name to prevent SQL injection
  validateIdentifier(args.schema, 'schema');
  validateFilter(args.filter);

  // Validate and set pagination parameters
  const limit = args.limit !== undefined
    ? validatePositiveInteger(args.limit, 'limit', 1, MAX_LIST_LIMIT)
    : DEFAULT_LIST_LIMIT;
  const offset = args.offset !== undefined
    ? validatePositiveInteger(args.offset, 'offset', 0, 1000000)
    : 0;

  const dbManager = getDbManager();
  const objectType: ObjectType = args.objectType || 'all';

  // Build connection override if specified
  const hasOverride = args.server || args.database || args.targetSchema;
  const override: ConnectionOverride | undefined = hasOverride
    ? { server: args.server, database: args.database, schema: args.targetSchema }
    : undefined;

  // Build union query parts using helper
  const unionParts = collectUnionParts(objectType, !!args.filter);
  const baseParams = args.filter ? [args.schema, args.filter] : [args.schema];

  if (unionParts.length === 0) {
    // No valid object types requested
    return {
      items: [],
      totalCount: 0,
      offset,
      limit,
      hasMore: false,
    };
  }

  // Build the combined query with UNION ALL
  const unionQuery = unionParts.join('\nUNION ALL\n');

  // First, get the total count (without pagination)
  const countQuery = `SELECT COUNT(*) as total FROM (${unionQuery}) as combined`;
  const countResult = await dbManager.queryWithOverride<{ total: string }>(countQuery, baseParams, override);
  const totalCount = parseInt(countResult.rows[0]?.total || '0', 10);

  // Then get the paginated results
  const paginatedQuery = `
    SELECT * FROM (${unionQuery}) as combined
    ORDER BY type, name
    LIMIT ${limit} OFFSET ${offset}
  `;
  const objectsResult = await dbManager.queryWithOverride<TableInfo>(paginatedQuery, baseParams, override);

  return {
    items: objectsResult.rows,
    totalCount,
    offset,
    limit,
    hasMore: offset + objectsResult.rows.length < totalCount,
  };
}

/**
 * Lists database objects without pagination (legacy compatibility).
 * Returns all objects matching the criteria.
 *
 * @deprecated Use listObjects with pagination for better performance on large schemas
 * @param args - Query parameters including schema and filters
 * @returns Array of all matching objects
 */
export async function listObjectsUnpaginated(args: {
  schema: string;
  objectType?: 'table' | 'view' | 'sequence' | 'extension' | 'all';
  filter?: string;
  server?: string;
  database?: string;
  targetSchema?: string;
}): Promise<TableInfo[]> {
  const result = await listObjects({
    ...args,
    limit: MAX_LIST_LIMIT,
    offset: 0,
  });
  return result.items;
}

export async function getObjectDetails(args: {
  schema: string;
  objectName: string;
  objectType?: 'table' | 'view' | 'sequence';
  // Connection override parameters
  server?: string;
  database?: string;
  targetSchema?: string; // Use targetSchema to avoid confusion with the required schema param
}): Promise<{
  columns?: ColumnInfo[];
  constraints?: ConstraintInfo[];
  indexes?: IndexInfo[];
  rowCount?: number;
  size?: string;
  definition?: string;
}> {
  // Validate required parameters
  if (!args.schema) {
    throw new Error('schema parameter is required');
  }
  if (!args.objectName) {
    throw new Error('objectName parameter is required');
  }

  // Validate identifiers to prevent SQL injection
  validateIdentifier(args.schema, 'schema');
  validateIdentifier(args.objectName, 'objectName');

  const dbManager = getDbManager();

  // Build connection override if specified
  const hasOverride = args.server || args.database || args.targetSchema;
  const override: ConnectionOverride | undefined = hasOverride
    ? { server: args.server, database: args.database, schema: args.targetSchema }
    : undefined;

  const result: {
    columns?: ColumnInfo[];
    constraints?: ConstraintInfo[];
    indexes?: IndexInfo[];
    rowCount?: number;
    size?: string;
    definition?: string;
  } = {};

  // Get columns - using parameterized query
  const columnsQuery = `
    SELECT
      column_name,
      data_type,
      is_nullable,
      column_default,
      character_maximum_length
    FROM information_schema.columns
    WHERE table_schema = $1 AND table_name = $2
    ORDER BY ordinal_position
  `;
  const columns = await dbManager.queryWithOverride<ColumnInfo>(columnsQuery, [args.schema, args.objectName], override);
  result.columns = columns.rows;

  // Get constraints - using parameterized query
  const constraintsQuery = `
    SELECT
      tc.constraint_name,
      tc.constraint_type,
      tc.table_name,
      kcu.column_name,
      ccu.table_name as foreign_table_name,
      ccu.column_name as foreign_column_name
    FROM information_schema.table_constraints tc
    LEFT JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    LEFT JOIN information_schema.constraint_column_usage ccu
      ON tc.constraint_name = ccu.constraint_name
      AND tc.table_schema = ccu.table_schema
    WHERE tc.table_schema = $1 AND tc.table_name = $2
    ORDER BY tc.constraint_type, tc.constraint_name
  `;
  const constraints = await dbManager.queryWithOverride<ConstraintInfo>(constraintsQuery, [args.schema, args.objectName], override);
  result.constraints = constraints.rows;

  // Get indexes - using parameterized query
  const indexesQuery = `
    SELECT
      i.relname as index_name,
      pg_get_indexdef(i.oid) as index_definition,
      ix.indisunique as is_unique,
      ix.indisprimary as is_primary
    FROM pg_class t
    JOIN pg_index ix ON t.oid = ix.indrelid
    JOIN pg_class i ON i.oid = ix.indexrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = $1 AND t.relname = $2
    ORDER BY i.relname
  `;
  const indexes = await dbManager.queryWithOverride<IndexInfo>(indexesQuery, [args.schema, args.objectName], override);
  result.indexes = indexes.rows;

  // Get table size and row count using safe approach
  try {
    const sizeQuery = `
      SELECT
        pg_size_pretty(pg_total_relation_size(c.oid)) as size,
        c.reltuples::bigint as row_count
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = $1 AND c.relname = $2
    `;
    const sizeResult = await dbManager.queryWithOverride<{ size: string; row_count: number }>(sizeQuery, [args.schema, args.objectName], override);
    if (sizeResult.rows.length > 0) {
      result.size = sizeResult.rows[0].size;
      result.rowCount = sizeResult.rows[0].row_count;
    }
  } catch (error) {
    // Size query might fail for views or non-existent objects
    console.debug('Could not get object size:', error);
  }

  // Get view definition if it's a view - using safe parameterized approach
  if (args.objectType === 'view') {
    try {
      const viewDefQuery = `
        SELECT pg_get_viewdef(c.oid, true) as definition
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'v'
      `;
      const viewDef = await dbManager.queryWithOverride<{ definition: string }>(viewDefQuery, [args.schema, args.objectName], override);
      if (viewDef.rows.length > 0) {
        result.definition = viewDef.rows[0].definition;
      }
    } catch (error) {
      // Might fail if not a view
      console.debug('Could not get view definition:', error);
    }
  }

  return result;
}
