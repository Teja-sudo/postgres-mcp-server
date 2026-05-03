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
      throw new Error(
        `Target server '${targetServer}'${targetDb ? ` database '${targetDb}'` : ''} is in readonly access mode. ` +
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
                const dropSql = buildDropStatement(desc);
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
      for (const t of tables) {
        if (dryRun) {
          if (outputStream) {
            outputStream.write(`-- data: ${qualify(t.schema, t.name)}\n`);
            const result = await emitTableRowsAsInsert(
              sourceClient.client, t.schema, t.name, outputStream
            );
            rowsTransferred += result.rowsEmitted;
            outputStream.write('\n');
          }
        } else {
          const rows = await transferTableData(
            sourceClient.client,
            targetClient!.client,
            t.schema,
            t.name,
            args.to.schema ?? sourceClient.schema
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
    return listObjectsInScope(client, { schema }, 'all');
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
  // Discover columns
  const colsRes = await source.query(
    `SELECT a.attname
     FROM pg_attribute a
     JOIN pg_class c ON c.oid = a.attrelid
     JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = $1 AND c.relname = $2
       AND a.attnum > 0 AND NOT a.attisdropped
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
  return total;
}
