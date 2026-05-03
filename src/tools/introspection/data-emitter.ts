/**
 * Data emission
 *
 * Reads rows from a source table and emits portable INSERT statements
 * (or PG `COPY ... FROM stdin` blocks). Used by export_to_sql_file.
 *
 * SP-2 v1: SELECT-and-emit-INSERT only. Streaming COPY format will be
 * added in SP-3 when pg-copy-streams is brought in for cross-server
 * transfer.
 */

import { PoolClient } from 'pg';
import { Writable } from 'stream';

/** Synchronous write callback - used by the export pipeline so we don't
 *  need a Writable wrapper (which buffers asynchronously and can write
 *  after the underlying stream closes). */
export type WriteFn = (chunk: string) => void;

/** Format SQL literal value from JS value, handling NULL, numbers,
 *  strings, booleans, dates, JSON, byte arrays. Best-effort — not a
 *  full PG type-aware encoder, but covers the common cases for
 *  exported INSERTs. */
export function formatSqlLiteral(value: unknown): string {
  if (value === null || value === undefined) return 'NULL';
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) return `'${String(value)}'`;
    return String(value);
  }
  if (typeof value === 'boolean') return value ? 'TRUE' : 'FALSE';
  if (typeof value === 'bigint') return value.toString();
  if (value instanceof Date) {
    return `'${value.toISOString()}'::timestamptz`;
  }
  if (Buffer.isBuffer(value)) {
    return `'\\x${value.toString('hex')}'::bytea`;
  }
  if (Array.isArray(value)) {
    // Audit-iteration-1 SP-2 P0 fix: empty arrays must include a
    // type cast — bare `ARRAY[]` makes PG complain "cannot
    // determine type of empty array". `'{}'::text[]` is the safest
    // universal literal because PG will implicit-cast it to the
    // target column's element type during INSERT regardless of
    // whether the column is text[]/int[]/uuid[]/etc.
    if (value.length === 0) return `'{}'::text[]`;
    // PG array literal with elements
    const inner = value.map((v) => formatSqlLiteral(v)).join(', ');
    return `ARRAY[${inner}]`;
  }
  if (typeof value === 'object') {
    // JSON-ish
    return `'${JSON.stringify(value).replace(/'/g, "''")}'::jsonb`;
  }
  // string
  const s = String(value);
  return `'${s.replace(/'/g, "''")}'`;
}

function qident(name: string): string {
  return '"' + name.replace(/"/g, '""') + '"';
}

function qualify(schema: string, name: string): string {
  return `${qident(schema)}.${qident(name)}`;
}

/**
 * Emit INSERT statements for every row in a table. Writes to the
 * provided sink. Uses a server-side cursor (via the pg client cursor
 * cap) to avoid loading large tables fully into memory.
 */
export interface EmitRowsOptions {
  /** Maximum rows to emit. 0 / undefined = no limit. */
  limit?: number;
  /** Statement batch size. Multiple rows per INSERT for compactness. */
  batchSize?: number;
  /** ORDER BY clause (without the keyword). For deterministic output. */
  orderBy?: string;
  /** Where clause (without WHERE). */
  where?: string;
}

export async function emitTableRowsAsInsert(
  client: PoolClient,
  schema: string,
  table: string,
  sink: WriteFn | Writable,
  opts: EmitRowsOptions = {}
): Promise<{ rowsEmitted: number }> {
  const batchSize = opts.batchSize ?? 100;
  const qualified = qualify(schema, table);

  // Normalize sink: accept both a synchronous WriteFn and a Node Writable.
  // The synchronous form is preferred because it avoids the "write after
  // end" race when the export pipeline closes the underlying stream
  // before the Writable's buffered chunks are drained.
  const writeChunk: WriteFn = typeof sink === 'function'
    ? sink
    : (chunk: string) => { sink.write(chunk); };

  // Audit-iteration-1 SP-2/SP-3 P0 fix: exclude IDENTITY (`a`/`d`)
  // and GENERATED-STORED (`s`) columns from the INSERT column list.
  // PG rejects explicit values for these:
  //   `cannot insert a non-DEFAULT value into column "id"` (IDENTITY)
  //   `cannot insert into generated column "x"` (STORED)
  // For IDENTITY columns the auto-generated value will be assigned;
  // for STORED columns the expression will be re-evaluated on the
  // target. Sequence state for SERIAL-style IDENTITY is synced
  // separately by the export/transfer caller.
  const colsRes = await client.query(
    `SELECT a.attname
     FROM pg_attribute a
     JOIN pg_class c ON c.oid = a.attrelid
     JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = $1 AND c.relname = $2
       AND a.attnum > 0 AND NOT a.attisdropped
       AND a.attidentity = ''
       AND a.attgenerated = ''
     ORDER BY a.attnum`,
    [schema, table]
  );
  const columnNames = colsRes.rows.map((r) => r.attname as string);
  const columnList = columnNames.map(qident).join(', ');

  // Build SELECT
  const whereClause = opts.where ? ` WHERE ${opts.where}` : '';
  const orderClause = opts.orderBy ? ` ORDER BY ${opts.orderBy}` : '';
  const limitClause = opts.limit && opts.limit > 0 ? ` LIMIT ${opts.limit}` : '';
  const sql = `SELECT ${columnList} FROM ${qualified}${whereClause}${orderClause}${limitClause}`;

  const result = await client.query(sql);
  const rows = result.rows;

  let emitted = 0;
  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    const valueRows = batch.map((row) => {
      const vals = columnNames.map((c) => formatSqlLiteral(row[c]));
      return `  (${vals.join(', ')})`;
    });
    writeChunk(
      `INSERT INTO ${qualified} (${columnList}) VALUES\n` +
      valueRows.join(',\n') + ';\n'
    );
    emitted += batch.length;
  }

  // Audit-iteration-1 SP-2 P0 fix: emit setval for any serial-backed
  // sequence on this table so a replay file leaves the sequence
  // pointing past the inserted rows. Without this, the replayed DB
  // gets the inserted rows AND a sequence still at 1; next nextval()
  // collides with the loaded data.
  if (emitted > 0) {
    const seqRes = await client.query(
      `SELECT
         a.attname::text AS col,
         pg_get_serial_sequence($1, a.attname) AS seq
       FROM pg_attribute a
       JOIN pg_class c ON c.oid = a.attrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = $2 AND c.relname = $3
         AND a.attnum > 0 AND NOT a.attisdropped
         AND pg_get_serial_sequence($1, a.attname) IS NOT NULL`,
      [qualified, schema, table]
    );
    for (const seqRow of seqRes.rows) {
      const seq = String(seqRow.seq);
      const col = String(seqRow.col);
      // Use literal in the dump because we don't know the value
      // server-side until replay; SELECT setval is portable.
      writeChunk(
        `SELECT setval('${seq.replace(/'/g, "''")}', COALESCE((SELECT MAX(${qident(col)})::bigint FROM ${qualified}), 1), true);\n`
      );
    }
  }

  return { rowsEmitted: emitted };
}
