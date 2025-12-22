import { getDbManager } from '../db-manager.js';
import { ExecuteSqlResult, QueryPlan } from '../types.js';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { v4 as uuidv4 } from 'uuid';

const MAX_OUTPUT_CHARS = 50000; // Maximum characters before writing to file
const MAX_ROWS_IN_RESPONSE = 1000; // Maximum rows to include in direct response

export async function executeSql(args: {
  sql: string;
  maxRows?: number;
}): Promise<ExecuteSqlResult> {
  const dbManager = getDbManager();
  const maxRows = args.maxRows || MAX_ROWS_IN_RESPONSE;

  const result = await dbManager.query(args.sql);

  const fields = result.fields?.map(f => f.name) || [];
  const totalRows = result.rows.length;

  // Serialize the result to check size
  const serialized = JSON.stringify(result.rows, null, 2);

  if (serialized.length > MAX_OUTPUT_CHARS || totalRows > maxRows) {
    // Write to temp file
    const tempDir = os.tmpdir();
    const fileName = `postgres-mcp-output-${uuidv4()}.json`;
    const filePath = path.join(tempDir, fileName);

    const outputData = {
      totalRows,
      fields,
      rows: result.rows,
      generatedAt: new Date().toISOString(),
      query: args.sql
    };

    fs.writeFileSync(filePath, JSON.stringify(outputData, null, 2));

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
  const dbManager = getDbManager();
  const client = await dbManager.getClient();

  try {
    // If hypothetical indexes are specified, create them as hypothetical
    if (args.hypotheticalIndexes && args.hypotheticalIndexes.length > 0) {
      // Check if hypopg extension is available
      const hypopgCheck = await client.query(`
        SELECT EXISTS (
          SELECT 1 FROM pg_extension WHERE extname = 'hypopg'
        ) as has_hypopg
      `);

      if (hypopgCheck.rows[0].has_hypopg) {
        // Create hypothetical indexes
        for (const idx of args.hypotheticalIndexes) {
          const indexType = idx.indexType || 'btree';
          const columns = idx.columns.join(', ');
          await client.query(`SELECT hypopg_create_index('CREATE INDEX ON ${idx.table} USING ${indexType} (${columns})')`);
        }
      }
    }

    // Build EXPLAIN query
    const format = args.format || 'json';
    const options: string[] = [`FORMAT ${format.toUpperCase()}`];

    if (args.analyze) {
      options.push('ANALYZE');
    }
    if (args.buffers) {
      options.push('BUFFERS');
    }

    const explainQuery = `EXPLAIN (${options.join(', ')}) ${args.sql}`;
    const result = await client.query(explainQuery);

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
