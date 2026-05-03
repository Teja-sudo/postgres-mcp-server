/**
 * SP-3 transfer_objects tool
 *
 * Releases / moves schema and/or data between databases (same or
 * different server). Builds on the SP-2 introspection module:
 *   - extract DDL from source
 *   - topologically order
 *   - apply to target inside a transaction
 *   - optionally stream data via SELECT + INSERT batches
 *
 * Cross-server is the common case; same-server-different-db works the
 * same way (PG can't query across DBs in a single connection without
 * dblink/FDW, so we always use two pool clients in parallel).
 *
 * dry_run: true → emits the would-be SQL to a file (or returns it),
 * does not touch the target. Reuses the SP-2 export emission code.
 */

import * as fs from 'fs';
import * as path from 'path';
import { PoolClient } from 'pg';
import { Writable } from 'stream';
import { getDbManager, OverrideClientResult } from '../db-manager.js';
import {
  ObjectDescriptor,
  ExtractedDDL,
  ObjectKind,
  listObjectsInScope,
  extractObjectDDL,
  buildDependencyGraph,
  topologicallyOrder,
  emitTableRowsAsInsert,
} from './introspection/index.js';

export interface TransferEndpoint {
  server: string;
  database?: string;
  schema?: string;
}

export interface ObjectRefForTransfer {
  kind: ObjectKind;
  name: string;
  schema?: string;
}

export interface TransferObjectsArgs {
  from: TransferEndpoint;
  to: TransferEndpoint;
  /** List of objects to transfer, or '*' for all in source schema. */
  objects: ObjectRefForTransfer[] | '*';
  /** What to transfer: ddl only, data only, or both. */
  include?: 'ddl' | 'data' | 'both';
  /** If target object already exists. */
  if_exists?: 'skip' | 'replace' | 'error';
  /** v1: 'insert_batches' only. Streaming COPY format reserved for v2. */
  data_strategy?: 'insert_batches';
  /** Don't apply - emit the SQL to output_file (or return inline). */
  dry_run?: boolean;
  /** When dry_run is true, file path to write generated SQL to. */
  output_file?: string;
}

export interface TransferObjectsResult {
  applied: boolean;
  dryRun: boolean;
  outputFile?: string;
  source: { server: string; database: string; schema: string };
  target: { server: string; database: string; schema: string };
  objectsTransferred: number;
  rowsTransferred: number;
  warnings: string[];
  /** SQL that was applied (or would be applied, in dry-run). Empty
   *  string when output_file was used to avoid duplication. */
  generatedSql?: string;
}

function safeIdent(n: string): string {
  return '"' + n.replace(/"/g, '""') + '"';
}

function qualify(schema: string, name: string): string {
  return `${safeIdent(schema)}.${safeIdent(name)}`;
}

/**
 * SP-3 transfer_objects.
 */
export async function transferObjects(
  args: TransferObjectsArgs
): Promise<TransferObjectsResult> {
  if (!args.from || !args.from.server) {
    throw new Error('from.server is required');
  }
  if (!args.to || !args.to.server) {
    throw new Error('to.server is required');
  }
  if (!args.objects) {
    throw new Error('objects is required (use "*" for all in source schema)');
  }

  const include = args.include ?? 'both';
  const ifExists = args.if_exists ?? 'error';
  const dryRun = args.dry_run === true;

  const dbManager = getDbManager();

  // Acquire source client (read-only, can be readonly server)
  const sourceClient = await dbManager.getClientWithOverride({
    server: args.from.server,
    database: args.from.database,
    schema: args.from.schema,
  });

  // Preflight check on target access mode (can't transfer to readonly)
  if (!dryRun) {
    const targetServer = args.to.server;
    const targetDb = args.to.database;
    if (dbManager.isReadOnlyFor(targetServer, targetDb)) {
      sourceClient.release();
      const dbPart = targetDb ? ` database '${targetDb}'` : '';
      throw new Error(
        `Target server '${targetServer}'${dbPart} is in readonly access mode. ` +
        `Cannot apply transferred objects. Use dry_run: true to generate the SQL only, ` +
        `or change the target's effective access mode.`
      );
    }
  }

  // Acquire target client (skipped in dry-run if we're emitting only)
  let targetClient: OverrideClientResult | null = null;
  if (!dryRun) {
    targetClient = await dbManager.getClientWithOverride({
      server: args.to.server,
      database: args.to.database,
      schema: args.to.schema,
    });
  }

  const warnings: string[] = [];
  const generatedSqlChunks: string[] = [];
  let objectsTransferred = 0;
  let rowsTransferred = 0;
  let outputStream: Writable | null = null;

  try {
    // Open output file if dry-run and output_file specified
    if (dryRun && args.output_file) {
      const resolved = path.resolve(args.output_file);
      if (!resolved.endsWith('.sql')) {
        throw new Error('output_file must end with .sql');
      }
      outputStream = fs.createWriteStream(resolved, {
        flags: 'w',
        mode: 0o600,
        encoding: 'utf-8',
      });
      const banner = [
        '-- ────────────────────────────────────────────────────────────',
        '-- postgres-mcp-server transfer_objects (dry-run)',
        `-- timestamp: ${new Date().toISOString()}`,
        `-- source:    server="${sourceClient.server}" database="${sourceClient.database}" schema="${sourceClient.schema}"`,
        `-- target:    server="${args.to.server}" database="${args.to.database ?? '(default)'}" schema="${args.to.schema ?? '(default)'}"`,
        `-- include:   ${include}`,
        `-- if_exists: ${ifExists}`,
        '-- ────────────────────────────────────────────────────────────',
        '',
      ].join('\n');
      outputStream.write(banner);
    }

    const emitSql = (sql: string): void => {
      generatedSqlChunks.push(sql);
      if (outputStream) outputStream.write(sql);
    };

    // === Discover source objects ===
    const sourceDescriptors = await resolveSourceObjects(
      sourceClient.client,
      sourceClient.schema,
      args.objects
    );

    // === Extract DDL ===
    const ddls: ExtractedDDL[] = [];
    if (include === 'ddl' || include === 'both') {
      for (const d of sourceDescriptors) {
        try {
          const ddl = await extractObjectDDL(sourceClient.client, d);
          ddls.push(ddl);
          warnings.push(...ddl.warnings);
        } catch (e) {
          warnings.push(
            `Failed DDL extraction for ${d.kind} ${d.schema}.${d.name}: ` +
            (e instanceof Error ? e.message : String(e))
          );
          ddls.push({
            kind: d.kind, qualifiedName: d.name, sql: '',
            warnings: [], dependencies: [],
          });
        }
      }
    }

    // === Topologically order ===
    const orderedDescriptors: ObjectDescriptor[] =
      include === 'ddl' || include === 'both'
        ? orderDescriptors(sourceDescriptors, ddls)
        : sourceDescriptors;

    // === Apply DDL ===
    if ((include === 'ddl' || include === 'both') && ddls.length > 0) {
      if (dryRun) {
        // Just emit
        for (let i = 0; i < orderedDescriptors.length; i++) {
          const idx = sourceDescriptors.findIndex((d) => d.oid === orderedDescriptors[i].oid);
          if (idx === -1) continue;
          const ddl = ddls[idx];
          if (!ddl.sql) continue;
          emitSql(`-- ${ddl.kind}: ${ddl.qualifiedName}\n`);
          emitSql(prepDDLForIfExists(ddl, ifExists));
          emitSql('\n\n');
          objectsTransferred++;
        }
      } else {
        // Apply to target inside a transaction
        await targetClient!.client.query('BEGIN');
        try {
          for (let i = 0; i < orderedDescriptors.length; i++) {
            const idx = sourceDescriptors.findIndex((d) => d.oid === orderedDescriptors[i].oid);
            if (idx === -1) continue;
            const ddl = ddls[idx];
            if (!ddl.sql) continue;

            const desc = sourceDescriptors[idx];
            const exists = await checkExists(targetClient!.client, desc, args.to.schema ?? sourceClient.schema);

            if (exists) {
              if (ifExists === 'error') {
                throw new Error(
                  `Target object already exists: ${desc.kind} ${desc.schema}.${desc.name}. ` +
                  `Set if_exists to 'skip' or 'replace'.`
                );
              }
              if (ifExists === 'skip') {
                warnings.push(`Skipped existing ${desc.kind}: ${qualify(desc.schema, desc.name)}`);
                continue;
              }
              if (ifExists === 'replace') {
                // Audit-iteration-1 SP-3 P0 fix #6: for functions /
                // procedures we need full argument signatures (PG can
                // have multiple overloads); for triggers we need the
                // table name. Resolve those from the source side
                // before emitting the drop.
                const dropSql = await buildDropStatementWithSignature(
                  sourceClient.client, desc
                );
                if (dropSql) {
                  await targetClient!.client.query(dropSql);
                }
              }
            }

            await targetClient!.client.query(ddl.sql);
            objectsTransferred++;
          }
          await targetClient!.client.query('COMMIT');
        } catch (e) {
          await targetClient!.client.query('ROLLBACK').catch(() => {});
          throw e;
        }
      }
    }

    // === Transfer data ===
    if (include === 'data' || include === 'both') {
      const tables = orderedDescriptors.filter((d) => d.kind === 'table');
      if (tables.length === 0 && (include === 'data')) {
        warnings.push('No tables in source scope; nothing to transfer for data');
      }
      const targetSchema = args.to.schema ?? sourceClient.schema;
      for (const t of tables) {
        if (dryRun) {
          if (outputStream) {
            const writeChunk = (s: string): void => { outputStream!.write(s); };
            writeChunk(`-- data: ${qualify(t.schema, t.name)}\n`);
            const result = await emitTableRowsAsInsert(
              sourceClient.client, t.schema, t.name, writeChunk
            );
            rowsTransferred += result.rowsEmitted;
            writeChunk('\n');
          }
        } else {
          // Audit-iteration-1 SP-3 P0 fix #5: when if_exists='skip',
          // also skip data transfer for tables that already have rows
          // on the target. The previous code unconditionally re-INSERTed,
          // which either errored on PK violations or silently duplicated.
          if (ifExists === 'skip') {
            const existingR = await targetClient!.client.query(
              `SELECT COUNT(*)::bigint AS c FROM ${qualify(targetSchema, t.name)}`
            );
            const existing = Number(existingR.rows[0]?.c ?? 0);
            if (existing > 0) {
              warnings.push(
                `Skipped data for ${qualify(targetSchema, t.name)}: target has ${existing} rows already.`
              );
              continue;
            }
          }
          const rows = await transferTableData(
            sourceClient.client,
            targetClient!.client,
            t.schema,
            t.name,
            targetSchema
          );
          rowsTransferred += rows;
        }
      }
    }
  } finally {
    sourceClient.release();
    if (targetClient) targetClient.release();
    if (outputStream) {
      await new Promise<void>((resolve, reject) => {
        outputStream!.end((err: unknown) => err ? reject(err) : resolve());
      });
    }
  }

  return {
    applied: !dryRun,
    dryRun,
    outputFile: args.output_file ? path.resolve(args.output_file) : undefined,
    source: {
      server: sourceClient.server,
      database: sourceClient.database,
      schema: sourceClient.schema,
    },
    target: {
      server: args.to.server,
      database: args.to.database ?? sourceClient.database,
      schema: args.to.schema ?? sourceClient.schema,
    },
    objectsTransferred,
    rowsTransferred,
    warnings,
    ...(args.output_file ? {} : { generatedSql: generatedSqlChunks.join('') }),
  };
}

async function resolveSourceObjects(
  client: PoolClient,
  schema: string,
  refs: ObjectRefForTransfer[] | '*'
): Promise<ObjectDescriptor[]> {
  if (refs === '*') {
    const all = await listObjectsInScope(client, { schema }, 'all');
    // The 'public' schema exists by default on every PG database. Don't
    // try to recreate it - the target already has it. Same for the
    // language plpgsql which is always pre-installed (we already
    // exclude it from listObjectsInScope, but defense-in-depth).
    return all.filter((d) => !(d.kind === 'schema' && d.name === 'public'));
  }
  const out: ObjectDescriptor[] = [];
  for (const ref of refs) {
    const list = await listObjectsInScope(
      client,
      { schema: ref.schema ?? schema },
      ref.kind
    );
    const match = list.find((d) => d.name === ref.name);
    if (match) out.push(match);
  }
  return out;
}

function orderDescriptors(
  descriptors: ObjectDescriptor[],
  ddls: ExtractedDDL[]
): ObjectDescriptor[] {
  const graph = buildDependencyGraph({ descriptors, ddls });
  const { ordered } = topologicallyOrder(graph);
  return ordered.map((node) => descriptors.find((d) => d.oid === node.oid)!).filter(Boolean);
}

async function checkExists(
  client: PoolClient,
  desc: ObjectDescriptor,
  targetSchema: string
): Promise<boolean> {
  const schema = targetSchema;
  switch (desc.kind) {
    case 'table':
    case 'view':
    case 'matview':
    case 'sequence':
    case 'index': {
      const r = await client.query(
        `SELECT to_regclass($1) AS reg`,
        [`${schema}.${desc.name}`]
      );
      return r.rows[0].reg !== null;
    }
    case 'extension': {
      const r = await client.query(
        `SELECT 1 FROM pg_extension WHERE extname = $1`,
        [desc.name]
      );
      return r.rowCount! > 0;
    }
    case 'schema': {
      const r = await client.query(
        `SELECT 1 FROM pg_namespace WHERE nspname = $1`,
        [desc.name]
      );
      return r.rowCount! > 0;
    }
    case 'function':
    case 'procedure': {
      const r = await client.query(
        `SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = $1 AND p.proname = $2`,
        [schema, desc.name]
      );
      return r.rowCount! > 0;
    }
    case 'type': {
      const r = await client.query(
        `SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
         WHERE n.nspname = $1 AND t.typname = $2`,
        [schema, desc.name]
      );
      return r.rowCount! > 0;
    }
    case 'trigger': {
      const r = await client.query(
        `SELECT 1 FROM pg_trigger t WHERE t.tgname = $1`,
        [desc.name]
      );
      return r.rowCount! > 0;
    }
    default:
      return false;
  }
}

/**
 * Audit-iteration-1 SP-3 P0 fix #6: build a DROP that PG can actually
 * execute on overloaded functions and on triggers (which need the
 * table name).
 *
 * For functions/procedures, fetch the full identity arg list via
 * `pg_get_function_identity_arguments` so the drop targets the
 * specific overload. For triggers, fetch the owning table.
 */
async function buildDropStatementWithSignature(
  client: PoolClient,
  desc: ObjectDescriptor
): Promise<string | null> {
  if (desc.kind === 'function' || desc.kind === 'procedure') {
    const r = await client.query(
      `SELECT pg_get_function_identity_arguments($1::oid) AS args`,
      [desc.oid]
    );
    const args = r.rows[0]?.args ?? '';
    const kw = desc.kind === 'procedure' ? 'PROCEDURE' : 'FUNCTION';
    return `DROP ${kw} IF EXISTS ${qualify(desc.schema, desc.name)}(${args}) CASCADE;`;
  }
  if (desc.kind === 'trigger') {
    const r = await client.query(
      `SELECT n.nspname AS sch, c.relname AS tbl
       FROM pg_trigger t
       JOIN pg_class c ON c.oid = t.tgrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE t.oid = $1::oid`,
      [desc.oid]
    );
    if (r.rows.length === 0) return null;
    const sch = String(r.rows[0].sch);
    const tbl = String(r.rows[0].tbl);
    return `DROP TRIGGER IF EXISTS ${safeIdent(desc.name)} ON ${qualify(sch, tbl)};`;
  }
  return buildDropStatement(desc);
}

function buildDropStatement(desc: ObjectDescriptor): string | null {
  switch (desc.kind) {
    case 'table': return `DROP TABLE IF EXISTS ${qualify(desc.schema, desc.name)} CASCADE;`;
    case 'view': return `DROP VIEW IF EXISTS ${qualify(desc.schema, desc.name)} CASCADE;`;
    case 'matview': return `DROP MATERIALIZED VIEW IF EXISTS ${qualify(desc.schema, desc.name)} CASCADE;`;
    case 'sequence': return `DROP SEQUENCE IF EXISTS ${qualify(desc.schema, desc.name)} CASCADE;`;
    case 'index': return `DROP INDEX IF EXISTS ${qualify(desc.schema, desc.name)};`;
    case 'extension': return `DROP EXTENSION IF EXISTS ${safeIdent(desc.name)} CASCADE;`;
    case 'function':
    case 'procedure':
      // Function signatures matter; use CASCADE to drop overloads matching
      return `DROP ${desc.kind === 'procedure' ? 'PROCEDURE' : 'FUNCTION'} IF EXISTS ${qualify(desc.schema, desc.name)} CASCADE;`;
    case 'type': return `DROP TYPE IF EXISTS ${qualify(desc.schema, desc.name)} CASCADE;`;
    case 'trigger':
      // Triggers need the table; we'd need to introspect to get it.
      // Return null to skip auto-drop; user can replace manually.
      return null;
    case 'schema': return `DROP SCHEMA IF EXISTS ${safeIdent(desc.name)} CASCADE;`;
  }
}

function prepDDLForIfExists(ddl: ExtractedDDL, ifExists: 'skip' | 'replace' | 'error'): string {
  // The DDL strings already use IF NOT EXISTS for tables/sequences/etc.
  // For 'replace' mode we'd need to prepend a DROP, but we don't know
  // the descriptor here; the apply path handles drops. For dry-run
  // emission, we'll just include a comment when replace is requested.
  if (ifExists === 'replace') {
    return `-- if_exists=replace: target will be dropped before apply\n${ddl.sql}`;
  }
  return ddl.sql;
}

async function transferTableData(
  source: PoolClient,
  target: PoolClient,
  sourceSchema: string,
  tableName: string,
  targetSchema: string
): Promise<number> {
  // Audit-iteration-1 SP-3 P0 fix: exclude IDENTITY/GENERATED
  // columns from data transfer column list - PG rejects explicit
  // values for either. See data-emitter.ts for full reasoning.
  const colsRes = await source.query(
    `SELECT a.attname
     FROM pg_attribute a
     JOIN pg_class c ON c.oid = a.attrelid
     JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = $1 AND c.relname = $2
       AND a.attnum > 0 AND NOT a.attisdropped
       AND a.attidentity = ''
       AND a.attgenerated = ''
     ORDER BY a.attnum`,
    [sourceSchema, tableName]
  );
  const columnNames = colsRes.rows.map((r) => r.attname as string);
  const columnList = columnNames.map(safeIdent).join(', ');

  const sourceQualified = qualify(sourceSchema, tableName);
  const targetQualified = qualify(targetSchema, tableName);

  // SELECT all rows
  const result = await source.query(
    `SELECT ${columnList} FROM ${sourceQualified}`
  );

  if (result.rows.length === 0) return 0;

  // Insert in batches with parameterized queries (safer than literal-formatting)
  const batchSize = 100;
  let total = 0;
  for (let i = 0; i < result.rows.length; i += batchSize) {
    const batch = result.rows.slice(i, i + batchSize);
    // Build VALUES placeholders
    const valuesSql: string[] = [];
    const params: unknown[] = [];
    let p = 1;
    for (const row of batch) {
      const placeholders = columnNames.map(() => `$${p++}`).join(', ');
      valuesSql.push(`(${placeholders})`);
      for (const c of columnNames) params.push(row[c]);
    }
    await target.query(
      `INSERT INTO ${targetQualified} (${columnList}) VALUES ${valuesSql.join(', ')}`,
      params
    );
    total += batch.length;
  }

  // Audit-iteration-1 SP-2/SP-3 P0 fix: sync sequence state after
  // data load. Source rows came in with explicit serial values; the
  // target's auto-created sequence still points at 1, so the next
  // nextval() will collide with our just-loaded data. Walk every
  // serial-backed column on the target and call setval(seq, max(col))
  // so the next insert advances past the loaded rows.
  const seqRes = await target.query(
    `SELECT
       a.attname AS col,
       pg_get_serial_sequence($1, a.attname) AS seq
     FROM pg_attribute a
     JOIN pg_class c ON c.oid = a.attrelid
     JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = $2 AND c.relname = $3
       AND a.attnum > 0 AND NOT a.attisdropped
       AND pg_get_serial_sequence($1, a.attname) IS NOT NULL`,
    [targetQualified, targetSchema, tableName]
  );
  for (const seqRow of seqRes.rows) {
    const col = String(seqRow.col);
    const seq = String(seqRow.seq);
    // setval to MAX(col); coalesce to 1 to handle empty tables.
    // is_called=true so next nextval() advances past max.
    await target.query(
      `SELECT setval($1, COALESCE((SELECT MAX(${safeIdent(col)})::bigint FROM ${targetQualified}), 1), true)`,
      [seq]
    );
  }

  return total;
}
