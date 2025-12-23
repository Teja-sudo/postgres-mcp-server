import { getDbManager } from '../db-manager.js';
import { SchemaInfo, TableInfo, ColumnInfo, ConstraintInfo, IndexInfo } from '../types.js';
import { validateIdentifier } from '../utils/validation.js';

export async function listSchemas(args: {
  includeSystemSchemas?: boolean;
}): Promise<SchemaInfo[]> {
  const dbManager = getDbManager();

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

  const result = await dbManager.query<SchemaInfo>(query);
  return result.rows;
}

export async function listObjects(args: {
  schema: string;
  objectType?: 'table' | 'view' | 'sequence' | 'extension' | 'all';
  filter?: string;
}): Promise<TableInfo[]> {
  // Validate required parameters
  if (!args.schema) {
    throw new Error('schema parameter is required');
  }

  // Validate schema name to prevent SQL injection
  validateIdentifier(args.schema, 'schema');

  // Validate filter if provided
  if (args.filter) {
    // Allow more characters in filter since it's used with ILIKE and parameterized
    if (args.filter.length > 128) {
      throw new Error('filter must be 128 characters or less');
    }
    // Only allow safe characters in filter
    if (!/^[a-zA-Z0-9_% ]+$/.test(args.filter)) {
      throw new Error('filter contains invalid characters');
    }
  }

  const dbManager = getDbManager();
  const objectType = args.objectType || 'all';
  const objects: TableInfo[] = [];

  // List tables
  if (objectType === 'all' || objectType === 'table') {
    const tablesQuery = `
      SELECT
        tablename as name,
        'table' as type,
        tableowner as owner,
        schemaname as schema
      FROM pg_catalog.pg_tables
      WHERE schemaname = $1
      ${args.filter ? "AND tablename ILIKE '%' || $2 || '%'" : ''}
      ORDER BY tablename
    `;
    const params = args.filter ? [args.schema, args.filter] : [args.schema];
    const tables = await dbManager.query<TableInfo>(tablesQuery, params);
    objects.push(...tables.rows);
  }

  // List views
  if (objectType === 'all' || objectType === 'view') {
    const viewsQuery = `
      SELECT
        v.table_name as name,
        'view' as type,
        v.table_schema as schema,
        COALESCE(c.relowner::regrole::text, '') as owner
      FROM information_schema.views v
      LEFT JOIN pg_class c ON c.relname = v.table_name
      LEFT JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = v.table_schema
      WHERE v.table_schema = $1
      ${args.filter ? "AND v.table_name ILIKE '%' || $2 || '%'" : ''}
      ORDER BY v.table_name
    `;
    const params = args.filter ? [args.schema, args.filter] : [args.schema];
    const views = await dbManager.query<TableInfo>(viewsQuery, params);
    objects.push(...views.rows);
  }

  // List sequences
  if (objectType === 'all' || objectType === 'sequence') {
    const sequencesQuery = `
      SELECT
        s.sequence_name as name,
        'sequence' as type,
        s.sequence_schema as schema,
        COALESCE(c.relowner::regrole::text, '') as owner
      FROM information_schema.sequences s
      LEFT JOIN pg_class c ON c.relname = s.sequence_name
      LEFT JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = s.sequence_schema
      WHERE s.sequence_schema = $1
      ${args.filter ? "AND s.sequence_name ILIKE '%' || $2 || '%'" : ''}
      ORDER BY s.sequence_name
    `;
    const params = args.filter ? [args.schema, args.filter] : [args.schema];
    const sequences = await dbManager.query<TableInfo>(sequencesQuery, params);
    objects.push(...sequences.rows);
  }

  // List extensions (schema-independent but we can filter)
  if (objectType === 'all' || objectType === 'extension') {
    const extensionsQuery = `
      SELECT
        extname as name,
        'extension' as type,
        n.nspname as schema,
        COALESCE(extowner::regrole::text, '') as owner
      FROM pg_extension e
      JOIN pg_namespace n ON e.extnamespace = n.oid
      WHERE n.nspname = $1
      ${args.filter ? "AND extname ILIKE '%' || $2 || '%'" : ''}
      ORDER BY extname
    `;
    const params = args.filter ? [args.schema, args.filter] : [args.schema];
    const extensions = await dbManager.query<TableInfo>(extensionsQuery, params);
    objects.push(...extensions.rows);
  }

  return objects;
}

export async function getObjectDetails(args: {
  schema: string;
  objectName: string;
  objectType?: 'table' | 'view' | 'sequence';
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
  const columns = await dbManager.query<ColumnInfo>(columnsQuery, [args.schema, args.objectName]);
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
  const constraints = await dbManager.query<ConstraintInfo>(constraintsQuery, [args.schema, args.objectName]);
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
  const indexes = await dbManager.query<IndexInfo>(indexesQuery, [args.schema, args.objectName]);
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
    const sizeResult = await dbManager.query<{ size: string; row_count: number }>(sizeQuery, [args.schema, args.objectName]);
    if (sizeResult.rows.length > 0) {
      result.size = sizeResult.rows[0].size;
      result.rowCount = sizeResult.rows[0].row_count;
    }
  } catch (error) {
    // Size query might fail for views or non-existent objects
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
      const viewDef = await dbManager.query<{ definition: string }>(viewDefQuery, [args.schema, args.objectName]);
      if (viewDef.rows.length > 0) {
        result.definition = viewDef.rows[0].definition;
      }
    } catch (error) {
      // Might fail if not a view
    }
  }

  return result;
}
