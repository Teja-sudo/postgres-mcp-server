import { getDbManager } from '../db-manager.js';
import { ExecuteSqlResult, QueryPlan } from '../types.js';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { v4 as uuidv4 } from 'uuid';
import { validateIdentifier, validateIndexType, isReadOnlySql, validatePositiveInteger } from '../utils/validation.js';

const MAX_OUTPUT_CHARS = 50000; // Maximum characters before writing to file
const MAX_ROWS_IN_RESPONSE = 1000; // Maximum rows to include in direct response
const MAX_SQL_LENGTH = 100000; // Maximum SQL query length

export async function executeSql(args: {
  sql: string;
  maxRows?: number;
}): Promise<ExecuteSqlResult> {
  // Validate SQL input
  if (!args.sql || typeof args.sql !== 'string') {
    throw new Error('sql parameter is required and must be a string');
  }

  if (args.sql.length > MAX_SQL_LENGTH) {
    throw new Error(`SQL query exceeds maximum length of ${MAX_SQL_LENGTH} characters`);
  }

  const dbManager = getDbManager();
  const maxRows = validatePositiveInteger(args.maxRows, 'maxRows', 1, 100000) || MAX_ROWS_IN_RESPONSE;

  const result = await dbManager.query(args.sql);

  const fields = result.fields?.map(f => f.name) || [];
  const totalRows = result.rows.length;

  // Check if output is too large - estimate size first without full serialization
  const estimatedSize = totalRows * (fields.length * 50); // rough estimate

  if (estimatedSize > MAX_OUTPUT_CHARS || totalRows > maxRows) {
    // Write to temp file
    const tempDir = os.tmpdir();
    const fileName = `postgres-mcp-output-${uuidv4()}.json`;
    const filePath = path.join(tempDir, fileName);

    const outputData = {
      totalRows,
      fields,
      rows: result.rows,
      generatedAt: new Date().toISOString(),
      // Don't include the query in output file for security
    };

    fs.writeFileSync(filePath, JSON.stringify(outputData, null, 2), { mode: 0o600 });

    return {
      rows: [],
      rowCount: totalRows,
      fields,
      outputFile: filePath,
      truncated: true
    };
  }

  return {
    rows: result.rows,
    rowCount: totalRows,
    fields
  };
}

export async function explainQuery(args: {
  sql: string;
  analyze?: boolean;
  buffers?: boolean;
  format?: 'text' | 'json' | 'yaml' | 'xml';
  hypotheticalIndexes?: Array<{
    table: string;
    columns: string[];
    indexType?: string;
  }>;
}): Promise<QueryPlan> {
  // Validate SQL input
  if (!args.sql || typeof args.sql !== 'string') {
    throw new Error('sql parameter is required and must be a string');
  }

  if (args.sql.length > MAX_SQL_LENGTH) {
    throw new Error(`SQL query exceeds maximum length of ${MAX_SQL_LENGTH} characters`);
  }

  // SECURITY: Block EXPLAIN ANALYZE on write queries to prevent bypassing read-only mode
  if (args.analyze) {
    const { isReadOnly, reason } = isReadOnlySql(args.sql);
    if (!isReadOnly) {
      throw new Error(`EXPLAIN ANALYZE is not allowed for write queries. ${reason}`);
    }
  }

  const dbManager = getDbManager();
  const client = await dbManager.getClient();

  try {
    // If hypothetical indexes are specified, validate and create them
    if (args.hypotheticalIndexes && args.hypotheticalIndexes.length > 0) {
      // Limit number of hypothetical indexes
      if (args.hypotheticalIndexes.length > 10) {
        throw new Error('Maximum 10 hypothetical indexes allowed');
      }

      // Check if hypopg extension is available
      const hypopgCheck = await client.query(`
        SELECT EXISTS (
          SELECT 1 FROM pg_extension WHERE extname = 'hypopg'
        ) as has_hypopg
      `);

      if (hypopgCheck.rows[0].has_hypopg) {
        // Create hypothetical indexes with validated inputs
        for (const idx of args.hypotheticalIndexes) {
          // Validate table name
          if (!idx.table) {
            throw new Error('hypotheticalIndexes: table is required');
          }

          // Handle schema.table format
          let schemaName = 'public';
          let tableName = idx.table;

          if (idx.table.includes('.')) {
            const parts = idx.table.split('.');
            if (parts.length !== 2) {
              throw new Error(`hypotheticalIndexes: invalid table format '${idx.table}'`);
            }
            schemaName = parts[0];
            tableName = parts[1];
            validateIdentifier(schemaName, 'schema');
          }
          validateIdentifier(tableName, 'table');

          // Validate columns
          if (!idx.columns || !Array.isArray(idx.columns) || idx.columns.length === 0) {
            throw new Error('hypotheticalIndexes: columns array is required and must not be empty');
          }

          if (idx.columns.length > 32) {
            throw new Error('hypotheticalIndexes: maximum 32 columns per index');
          }

          for (const col of idx.columns) {
            validateIdentifier(col, 'column');
          }

          // Validate index type
          const indexType = validateIndexType(idx.indexType || 'btree');

          // Build safe index creation string
          const safeTableName = schemaName !== 'public'
            ? `"${schemaName}"."${tableName}"`
            : `"${tableName}"`;
          const safeColumns = idx.columns.map(c => `"${c}"`).join(', ');

          // Use parameterized approach via hypopg
          const indexDef = `CREATE INDEX ON ${safeTableName} USING ${indexType} (${safeColumns})`;
          await client.query('SELECT hypopg_create_index($1)', [indexDef]);
        }
      }
    }

    // Build EXPLAIN query
    const validFormats = ['text', 'json', 'yaml', 'xml'];
    const format = validFormats.includes(args.format || '') ? args.format! : 'json';
    const options: string[] = [`FORMAT ${format.toUpperCase()}`];

    if (args.analyze) {
      options.push('ANALYZE');
    }
    if (args.buffers) {
      options.push('BUFFERS');
    }

    // Use the validated SQL - it will be checked by the db-manager's read-only check
    // EXPLAIN itself is read-only, EXPLAIN ANALYZE executes but we validated above
    const explainSql = `EXPLAIN (${options.join(', ')}) ${args.sql}`;
    const result = await client.query(explainSql);

    // Clean up hypothetical indexes if created
    if (args.hypotheticalIndexes && args.hypotheticalIndexes.length > 0) {
      try {
        await client.query('SELECT hypopg_reset()');
      } catch (e) {
        // Ignore if hypopg not available
      }
    }

    if (format === 'json') {
      return {
        plan: result.rows[0]['QUERY PLAN'][0]
      };
    }

    return {
      plan: result.rows.map((r: any) => r['QUERY PLAN']).join('\n')
    };
  } finally {
    client.release();
  }
}
