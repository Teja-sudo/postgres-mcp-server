import { getDbManager } from '../db-manager.js';
import { SchemaInfo, TableInfo, ColumnInfo, ConstraintInfo, IndexInfo } from '../types.js';

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
  const dbManager = getDbManager();
  const objectType = args.objectType || 'all';
  const objects: TableInfo[] = [];

  // List tables
  if (objectType === 'all' || objectType === 'table') {
    const tablesQuery = `
      SELECT
        table_name as name,
        'table' as type,
        tableowner as owner,
        schemaname as schema
      FROM pg_catalog.pg_tables
      WHERE schemaname = $1
      ${args.filter ? "AND table_name ILIKE '%' || $2 || '%'" : ''}
      ORDER BY table_name
    `;
    const params = args.filter ? [args.schema, args.filter] : [args.schema];
    const tables = await dbManager.query<TableInfo>(tablesQuery, params);
    objects.push(...tables.rows);
  }

  // List views
  if (objectType === 'all' || objectType === 'view') {
    const viewsQuery = `
      SELECT
        table_name as name,
        'view' as type,
        table_schema as schema,
        '' as owner
      FROM information_schema.views
      WHERE table_schema = $1
      ${args.filter ? "AND table_name ILIKE '%' || $2 || '%'" : ''}
      ORDER BY table_name
    `;
    const params = args.filter ? [args.schema, args.filter] : [args.schema];
    const views = await dbManager.query<TableInfo>(viewsQuery, params);
    objects.push(...views.rows);
  }

  // List sequences
  if (objectType === 'all' || objectType === 'sequence') {
    const sequencesQuery = `
      SELECT
        sequence_name as name,
        'sequence' as type,
        sequence_schema as schema,
        '' as owner
      FROM information_schema.sequences
      WHERE sequence_schema = $1
      ${args.filter ? "AND sequence_name ILIKE '%' || $2 || '%'" : ''}
      ORDER BY sequence_name
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
        '' as owner
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
  const dbManager = getDbManager();
  const result: {
    columns?: ColumnInfo[];
    constraints?: ConstraintInfo[];
    indexes?: IndexInfo[];
    rowCount?: number;
    size?: string;
    definition?: string;
  } = {};

  // Get columns
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

  // Get constraints
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

  // Get indexes
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

  // Get table size and row count (approximate)
  try {
    const sizeQuery = `
      SELECT
        pg_size_pretty(pg_total_relation_size($1::regclass)) as size,
        (SELECT reltuples::bigint FROM pg_class WHERE oid = $1::regclass) as row_count
    `;
    const fullName = `"${args.schema}"."${args.objectName}"`;
    const sizeResult = await dbManager.query<{ size: string; row_count: number }>(sizeQuery, [fullName]);
    if (sizeResult.rows.length > 0) {
      result.size = sizeResult.rows[0].size;
      result.rowCount = sizeResult.rows[0].row_count;
    }
  } catch (error) {
    // Size query might fail for views
  }

  // Get view definition if it's a view
  if (args.objectType === 'view') {
    try {
      const viewDefQuery = `
        SELECT pg_get_viewdef($1::regclass, true) as definition
      `;
      const fullName = `"${args.schema}"."${args.objectName}"`;
      const viewDef = await dbManager.query<{ definition: string }>(viewDefQuery, [fullName]);
      if (viewDef.rows.length > 0) {
        result.definition = viewDef.rows[0].definition;
      }
    } catch (error) {
      // Might fail if not a view
    }
  }

  return result;
}
