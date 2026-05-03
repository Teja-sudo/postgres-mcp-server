/**
 * SP-2 export_to_sql_file tool
 *
 * Exports schema and/or data from the connected database to a .sql
 * file. Append (default) or overwrite modes. Four content variants:
 *
 *   - { kind: 'objects', objects }       — DDL of a list of objects
 *   - { kind: 'data', tables, format }   — INSERT statements for tables
 *   - { kind: 'schema_dump', schema, include_data } — full schema +/- data
 *   - { kind: 'query_result', sql, target_table } — SELECT → INSERTs
 *
 * Uses the introspection module for DDL extraction and dependency
 * ordering. Header banner is always prepended; source server alias
 * (not host) and timestamp are recorded.
 */

import * as fs from 'fs';
import * as path from 'path';
import { Writable } from 'stream';
import { getDbManager, OverrideClientResult } from '../db-manager.js';
import { ConnectionOverride } from '../types.js';
import {
  ObjectKind,
  ObjectDescriptor,
  ExtractedDDL,
  listObjectsInScope,
  extractObjectDDL,
  buildDependencyGraph,
  topologicallyOrder,
  emitTableRowsAsInsert,
  formatSqlLiteral,
} from './introspection/index.js';

/** Maximum size of file we will append to (50MB) before refusing. */
const MAX_APPEND_FILE_SIZE = 50 * 1024 * 1024;

/** Object reference for `kind: 'objects'`. */
export interface ObjectRef {
  kind: ObjectKind;
  /** Schema-qualified name OR just the object name (uses scope schema). */
  name: string;
  /** Optional schema override for this object (default: scope). */
  schema?: string;
}

/** Discriminated union of what to export. */
export type ExportSpec =
  | {
      kind: 'objects';
      objects: ObjectRef[];
    }
  | {
      kind: 'data';
      /** Schema-qualified or scope-relative table names. */
      tables: string[];
      /** v1: 'insert' only. 'copy' format reserved for SP-3. */
      format?: 'insert';
      /** Per-table options applied to all tables in the list. */
      where?: string;
      orderBy?: string;
      limit?: number;
    }
  | {
      kind: 'schema_dump';
      /** Schema to dump. Default: scope's schema. */
      schema?: string;
      include_data?: boolean;
    }
  | {
      kind: 'query_result';
      sql: string;
      target_table: string;
    };

export interface ExportToSqlFileArgs {
  filePath: string;
  mode?: 'append' | 'overwrite';
  what: ExportSpec;
  /** Allow emitting CREATE … IF NOT EXISTS for object DDL (default true). */
  include_create_if_not_exists?: boolean;
  /** Connection override (optional). */
  server?: string;
  database?: string;
  schema?: string;
  /** Caller confirmation when mode='overwrite' on a recently-modified file. */
  confirm_overwrite?: boolean;
}

export interface ExportToSqlFileResult {
  filePath: string;
  mode: 'append' | 'overwrite';
  bytesWritten: number;
  totalBytes: number;
  objectsExported: number;
  rowsExported: number;
  warnings: string[];
  /** Source server alias and database (no host/port — security). */
  source: { server: string; database: string; schema: string };
}

function safeIdent(name: string): string {
  return '"' + name.replace(/"/g, '""') + '"';
}

function qualify(schema: string, name: string): string {
  return `${safeIdent(schema)}.${safeIdent(name)}`;
}

/**
 * Resolve a possibly schema-qualified object name into { schema, name }.
 */
function splitQualified(
  raw: string,
  defaultSchema: string
): { schema: string; name: string } {
  // Use indexOf instead of regex to avoid the slow-regex lint
  // (and keep parsing predictable). Strip wrapping double quotes
  // from each part, but only at the outermost positions.
  const trimmed = raw.trim();
  const dot = trimmed.indexOf('.');
  if (dot > 0 && dot < trimmed.length - 1) {
    const left = trimmed.slice(0, dot).replace(/^"|"$/g, '').trim();
    const right = trimmed.slice(dot + 1).replace(/^"|"$/g, '').trim();
    if (left && right) return { schema: left, name: right };
  }
  return { schema: defaultSchema, name: trimmed.replace(/^"|"$/g, '') };
}

function buildBanner(
  source: { server: string; database: string; schema: string },
  spec: ExportSpec
): string {
  const ts = new Date().toISOString();
  const kindDescription = (() => {
    switch (spec.kind) {
      case 'objects':
        return `objects (${spec.objects.length} listed)`;
      case 'data':
        return `data (tables: ${spec.tables.join(', ')})`;
      case 'schema_dump':
        return `schema_dump (${spec.schema ?? source.schema}${spec.include_data ? ' + data' : ''})`;
      case 'query_result':
        return `query_result (target_table: ${spec.target_table})`;
    }
  })();
  return [
    '-- ────────────────────────────────────────────────────────────',
    '-- postgres-mcp-server export',
    `-- timestamp: ${ts}`,
    `-- source:    server="${source.server}" database="${source.database}" schema="${source.schema}"`,
    `-- kind:      ${kindDescription}`,
    '-- ────────────────────────────────────────────────────────────',
    '',
  ].join('\n');
}

/**
 * Resolve the target file: existence check, size check, mode handling,
 * append-newline prep. Returns the open WriteStream and starting size.
 */
async function openTarget(
  filePath: string,
  mode: 'append' | 'overwrite',
  confirmOverwrite: boolean
): Promise<{ stream: Writable; existingSize: number }> {
  const resolved = path.resolve(filePath);
  if (!filePath.endsWith('.sql')) {
    throw new Error('filePath must end with .sql');
  }

  // Refuse dangerous paths
  if (
    /[/\\](node_modules|\.git)([/\\]|$)/.test(resolved) ||
    /[/\\]\.env(\..*)?$/.test(resolved)
  ) {
    throw new Error(`Refusing to write to sensitive path: ${resolved}`);
  }

  let existingSize = 0;
  if (fs.existsSync(resolved)) {
    const stats = fs.statSync(resolved);
    if (!stats.isFile()) {
      throw new Error(`Target exists but is not a file: ${resolved}`);
    }
    existingSize = stats.size;
    if (mode === 'append') {
      if (existingSize > MAX_APPEND_FILE_SIZE) {
        throw new Error(
          `Target file is ${(existingSize / 1024 / 1024).toFixed(1)} MB, ` +
          `exceeds append limit of ${MAX_APPEND_FILE_SIZE / 1024 / 1024} MB. ` +
          `Use mode='overwrite' explicitly to replace.`
        );
      }
      // If file doesn't end with newline, prepend one for clean append
      if (existingSize > 0) {
        const tail = fs.readFileSync(resolved, { encoding: 'utf-8', flag: 'r' });
        if (!tail.endsWith('\n')) {
          fs.appendFileSync(resolved, '\n');
        }
      }
    } else {
      // overwrite mode
      const ageMs = Date.now() - stats.mtimeMs;
      if (ageMs < 60_000 && !confirmOverwrite) {
        throw new Error(
          `Target file was modified ${Math.round(ageMs / 1000)}s ago. ` +
          `Pass confirm_overwrite: true to proceed (foot-gun guard).`
        );
      }
    }
  }

  const stream = fs.createWriteStream(resolved, {
    flags: mode === 'append' ? 'a' : 'w',
    mode: 0o600,
    encoding: 'utf-8',
  });

  return { stream, existingSize };
}

async function endStream(s: Writable): Promise<void> {
  return new Promise((resolve, reject) => {
    s.end((err: unknown) => {
      if (err) reject(err);
      else resolve();
    });
  });
}

/**
 * SP-2 export_to_sql_file.
 */
export async function exportToSqlFile(
  args: ExportToSqlFileArgs
): Promise<ExportToSqlFileResult> {
  if (!args.filePath || typeof args.filePath !== 'string') {
    throw new Error('filePath is required');
  }
  if (!args.what || typeof args.what !== 'object') {
    throw new Error('what is required');
  }
  const mode: 'append' | 'overwrite' = args.mode ?? 'append';
  if (mode !== 'append' && mode !== 'overwrite') {
    throw new Error("mode must be 'append' or 'overwrite'");
  }

  const dbManager = getDbManager();
  const hasOverride = args.server || args.database || args.schema;
  const override: ConnectionOverride | undefined = hasOverride
    ? { server: args.server, database: args.database, schema: args.schema }
    : undefined;

  let clientResult: OverrideClientResult | null = null;
  let client;
  let server: string;
  let database: string;
  let scopeSchema: string;

  if (override) {
    clientResult = await dbManager.getClientWithOverride(override);
    client = clientResult.client;
    server = clientResult.server;
    database = clientResult.database;
    scopeSchema = clientResult.schema;
  } else {
    client = await dbManager.getClient();
    const ctx = dbManager.getConnectionInfo();
    server = ctx.server || '';
    database = ctx.database || '';
    scopeSchema = ctx.schema || 'public';
  }

  const { stream } = await openTarget(args.filePath, mode, args.confirm_overwrite === true);

  const source = { server, database, schema: scopeSchema };
  const warnings: string[] = [];
  let objectsExported = 0;
  let rowsExported = 0;
  let bytesWritten = 0;

  // Single synchronous write function: count bytes + push to underlying
  // stream. We deliberately avoid wrapping in a Node Writable because
  // the Writable's internal buffering can flush callbacks AFTER the
  // outer stream has been ended in the finally block, producing
  // "write after end" errors. Synchronous push-through has none of
  // that complexity and is correct for our small batched writes.
  const countingWrite = (s: string): void => {
    bytesWritten += Buffer.byteLength(s, 'utf-8');
    stream.write(s);
  };

  try {
    countingWrite(buildBanner(source, args.what));

    if (args.what.kind === 'objects') {
      const { count, warns } = await emitObjectDDLs(
        client,
        scopeSchema,
        args.what.objects,
        countingWrite
      );
      objectsExported = count;
      warnings.push(...warns);
    } else if (args.what.kind === 'data') {
      const { rows, warns } = await emitDataForTables(
        client,
        scopeSchema,
        args.what.tables,
        countingWrite,
        { where: args.what.where, orderBy: args.what.orderBy, limit: args.what.limit }
      );
      rowsExported = rows;
      warnings.push(...warns);
    } else if (args.what.kind === 'schema_dump') {
      const result = await emitSchemaDump(
        client,
        args.what.schema ?? scopeSchema,
        countingWrite,
        args.what.include_data === true
      );
      objectsExported = result.objects;
      rowsExported = result.rows;
      warnings.push(...result.warns);
    } else if (args.what.kind === 'query_result') {
      const rows = await emitQueryAsInserts(
        client,
        args.what.sql,
        args.what.target_table,
        scopeSchema,
        countingWrite
      );
      rowsExported = rows;
    }
  } finally {
    await endStream(stream);
    if (clientResult) clientResult.release();
    else client.release();
  }

  const totalBytes = fs.statSync(path.resolve(args.filePath)).size;

  return {
    filePath: path.resolve(args.filePath),
    mode,
    bytesWritten,
    totalBytes,
    objectsExported,
    rowsExported,
    warnings,
    source,
  };
}

async function emitObjectDDLs(
  client: any,
  defaultSchema: string,
  refs: ObjectRef[],
  write: (s: string) => void
): Promise<{ count: number; warns: string[] }> {
  // Discover descriptor for each ref
  const descriptors: ObjectDescriptor[] = [];
  const ddls: ExtractedDDL[] = [];
  const warns: string[] = [];

  for (const ref of refs) {
    const schema = ref.schema ?? defaultSchema;
    // Find the OID for this kind+schema+name
    const list = await listObjectsInScope(client, { schema }, ref.kind);
    const match = list.find((d) => d.name === ref.name);
    if (!match) {
      warns.push(`Object not found: ${ref.kind} ${schema}.${ref.name}`);
      continue;
    }
    descriptors.push(match);
    const ddl = await extractObjectDDL(client, match);
    ddls.push(ddl);
    warns.push(...ddl.warnings);
  }

  // Topological order
  const graph = buildDependencyGraph({ descriptors, ddls });
  const { ordered, cycles } = topologicallyOrder(graph);
  if (cycles.length > 0) {
    warns.push(
      `Dependency cycle detected involving ${cycles[0].length} object(s); ` +
      `FK constraints between tables are emitted as ALTER TABLE statements ` +
      `to break cycles.`
    );
  }

  for (const node of ordered) {
    const idx = descriptors.findIndex((d) => d.oid === node.oid);
    if (idx === -1) continue;
    const ddl = ddls[idx];
    if (!ddl.sql) continue;
    write(`-- ${ddl.kind}: ${ddl.qualifiedName}\n`);
    write(ddl.sql + '\n\n');
  }

  return { count: ordered.length, warns };
}

async function emitDataForTables(
  client: any,
  defaultSchema: string,
  tables: string[],
  write: (s: string) => void,
  opts: { where?: string; orderBy?: string; limit?: number }
): Promise<{ rows: number; warns: string[] }> {
  let rows = 0;
  const warns: string[] = [];
  for (const ref of tables) {
    const { schema, name } = splitQualified(ref, defaultSchema);
    write(`-- data: ${qualify(schema, name)}\n`);
    try {
      const result = await emitTableRowsAsInsert(client, schema, name, write, opts);
      rows += result.rowsEmitted;
      write('\n');
    } catch (e) {
      warns.push(
        `Failed to emit data for ${qualify(schema, name)}: ` +
        (e instanceof Error ? e.message : String(e))
      );
    }
  }
  return { rows, warns };
}

async function emitSchemaDump(
  client: any,
  schema: string,
  write: (s: string) => void,
  includeData: boolean
): Promise<{ objects: number; rows: number; warns: string[] }> {
  const all = await listObjectsInScope(client, { schema }, 'all');
  const ddls: ExtractedDDL[] = [];
  const warns: string[] = [];
  for (const d of all) {
    try {
      const ddl = await extractObjectDDL(client, d);
      ddls.push(ddl);
      warns.push(...ddl.warnings);
    } catch (e) {
      warns.push(
        `Failed to extract DDL for ${d.kind} ${d.schema}.${d.name}: ` +
        (e instanceof Error ? e.message : String(e))
      );
      ddls.push({ kind: d.kind, qualifiedName: d.name, sql: '', warnings: [], dependencies: [] });
    }
  }

  const graph = buildDependencyGraph({ descriptors: all, ddls });
  const { ordered } = topologicallyOrder(graph);

  for (const node of ordered) {
    const idx = all.findIndex((d) => d.oid === node.oid);
    if (idx === -1) continue;
    const ddl = ddls[idx];
    if (!ddl.sql) continue;
    write(`-- ${ddl.kind}: ${ddl.qualifiedName}\n`);
    write(ddl.sql + '\n\n');
  }

  let rows = 0;
  if (includeData) {
    write('-- Data ----------------------------------------\n');
    const tables = all.filter((d) => d.kind === 'table');
    // Audit-iteration-1 SP-2 P0 fix: disable user triggers around
    // the data load so re-INSERTing source rows does NOT fire the
    // dst triggers (e.g., audit_user_change writing to audit_log
    // again, doubling the audit_log row count post-replay). The
    // SESSION_REPLICATION_ROLE switch only affects this connection;
    // user-defined triggers still fire when applications connect
    // normally after replay.
    write(`-- disable user triggers during data load to prevent\n`);
    write(`-- duplicate side-effects from triggers re-firing on\n`);
    write(`-- already-source-side-recorded events\n`);
    write(`SET session_replication_role = 'replica';\n\n`);
    for (const t of tables) {
      write(`-- data: ${qualify(t.schema, t.name)}\n`);
      try {
        const result = await emitTableRowsAsInsert(client, t.schema, t.name, write);
        rows += result.rowsEmitted;
        write('\n');
      } catch (e) {
        warns.push(
          `Failed to emit data for ${qualify(t.schema, t.name)}: ` +
          (e instanceof Error ? e.message : String(e))
        );
      }
    }
    write(`SET session_replication_role = 'origin';\n\n`);

    // Audit-iteration-1 SP-2 P1 fix: refresh materialized views
    // after data replay so they're populated.
    const matviews = all.filter((d) => d.kind === 'matview');
    if (matviews.length > 0) {
      write('-- Matview refresh ------------------------------\n');
      for (const mv of matviews) {
        write(`REFRESH MATERIALIZED VIEW ${qualify(mv.schema, mv.name)};\n`);
      }
      write('\n');
    }
  }

  return { objects: ordered.length, rows, warns };
}

async function emitQueryAsInserts(
  client: any,
  sql: string,
  targetTable: string,
  defaultSchema: string,
  write: (s: string) => void
): Promise<number> {
  const { schema, name } = splitQualified(targetTable, defaultSchema);
  const result = await client.query(sql);
  if (!result.rows || result.rows.length === 0) {
    write(`-- query_result: 0 rows\n`);
    return 0;
  }
  const fields = result.fields.map((f: { name: string }) => f.name);
  const columnList = fields.map(safeIdent).join(', ');
  write(`-- query_result → ${qualify(schema, name)}: ${result.rows.length} rows\n`);

  const batchSize = 100;
  for (let i = 0; i < result.rows.length; i += batchSize) {
    const batch = result.rows.slice(i, i + batchSize);
    const valueRows = batch.map((row: Record<string, unknown>) => {
      const vals = fields.map((c: string) => formatSqlLiteral(row[c]));
      return `  (${vals.join(', ')})`;
    });
    write(
      `INSERT INTO ${qualify(schema, name)} (${columnList}) VALUES\n` +
      valueRows.join(',\n') + ';\n'
    );
  }
  write('\n');
  return result.rows.length;
}
