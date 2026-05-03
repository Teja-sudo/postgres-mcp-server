/**
 * SP-4 schema awareness pack
 *
 * Three read-only tools, all built on the SP-2 introspection module:
 *
 *   describe_table  — single rich call: columns, FK in/out, indexes,
 *                     size, row count estimate, sample rows, per-column
 *                     null/distinct rate.
 *   find_dependents — walk pg_depend recursively, classify dependents
 *                     (views, FKs, functions, materialized views).
 *   schema_diff     — DDL delta between two { server, database, schema }
 *                     triples; emits SQL that migrates target to match
 *                     source.
 */

import { PoolClient } from 'pg';
import { getDbManager } from '../db-manager.js';
import { ConnectionOverride } from '../types.js';
import {
  ObjectKind,
  listObjectsInScope,
  extractObjectDDL,
} from './introspection/index.js';

function safeIdent(n: string): string {
  return '"' + n.replace(/"/g, '""') + '"';
}

function qualify(schema: string, name: string): string {
  return `${safeIdent(schema)}.${safeIdent(name)}`;
}

// ============================================================
// describe_table
// ============================================================

export interface DescribeTableArgs {
  schema?: string;
  table: string;
  /** Sample rows to fetch. 0 to skip. Default 5. */
  sample_size?: number;
  /** Columns to compute null %/distinct ratio for (default: all up to 20). */
  profile_columns?: string[];
  server?: string;
  database?: string;
  /** One-time schema override - kept distinct from `schema` so the
   *  required arg has a clear name. */
  override_schema?: string;
}

export interface DescribeTableResult {
  schema: string;
  table: string;
  exists: boolean;
  size?: string;
  rowCountEstimate?: number;
  columns: Array<{
    name: string;
    type: string;
    nullable: boolean;
    default?: string;
    nullPercent?: number;
    distinctRatio?: number;
  }>;
  primaryKey: string[];
  foreignKeysOut: Array<{
    constraintName: string;
    columns: string[];
    referencedTable: string;
    referencedColumns: string[];
    onDelete: string;
    onUpdate: string;
  }>;
  foreignKeysIn: Array<{
    constraintName: string;
    sourceTable: string;
    sourceColumns: string[];
    referencedColumns: string[];
  }>;
  indexes: Array<{
    name: string;
    definition: string;
    unique: boolean;
    primary: boolean;
  }>;
  sampleRows: any[];
  comment?: string;
}

export async function describeTable(args: DescribeTableArgs): Promise<DescribeTableResult> {
  if (!args.table) throw new Error('table is required');

  const dbManager = getDbManager();
  const hasOverride = args.server || args.database || args.override_schema;
  const override: ConnectionOverride | undefined = hasOverride
    ? { server: args.server, database: args.database, schema: args.override_schema }
    : undefined;

  const sampleSize = args.sample_size ?? 5;

  const { client, release } = await acquireClient(dbManager, override);

  try {
    const schema = args.schema ?? (override?.schema ?? 'public');

    // Existence + size + row count estimate
    const sizeRes = await client.query(
      `SELECT pg_size_pretty(pg_total_relation_size(c.oid)) AS size,
              c.reltuples::bigint AS row_estimate,
              pg_catalog.obj_description(c.oid, 'pg_class') AS comment
       FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r','m','v')`,
      [schema, args.table]
    );

    if (sizeRes.rows.length === 0) {
      return {
        schema, table: args.table, exists: false,
        columns: [], primaryKey: [], foreignKeysOut: [], foreignKeysIn: [],
        indexes: [], sampleRows: [],
      };
    }

    const sizeRow = sizeRes.rows[0];

    // Columns
    const colsRes = await client.query(
      `SELECT a.attname AS name,
              pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
              NOT a.attnotnull AS nullable,
              pg_get_expr(d.adbin, d.adrelid) AS "default"
       FROM pg_attribute a
       LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
       JOIN pg_class c ON c.oid = a.attrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = $1 AND c.relname = $2
         AND a.attnum > 0 AND NOT a.attisdropped
       ORDER BY a.attnum`,
      [schema, args.table]
    );
    const columns: DescribeTableResult['columns'] = colsRes.rows.map((r) => ({
      name: r.name,
      type: r.type,
      nullable: r.nullable,
      default: r.default ?? undefined,
    }));

    // Primary key
    const pkRes = await client.query(
      `SELECT a.attname
       FROM pg_index i
       JOIN pg_class c ON c.oid = i.indrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
       JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
       WHERE i.indisprimary AND n.nspname = $1 AND c.relname = $2
       ORDER BY array_position(i.indkey::int[], a.attnum::int)`,
      [schema, args.table]
    );
    const primaryKey = pkRes.rows.map((r) => r.attname);

    // FKs going OUT (this table -> other tables)
    const fkOutRes = await client.query(
      `SELECT con.conname,
              pg_get_constraintdef(con.oid, true) AS def,
              array_agg(att.attname ORDER BY u.ord) AS cols,
              ref_n.nspname || '.' || ref_c.relname AS ref_table,
              array_agg(ref_att.attname ORDER BY u.ord) AS ref_cols,
              CASE con.confdeltype WHEN 'a' THEN 'NO ACTION' WHEN 'r' THEN 'RESTRICT'
                                   WHEN 'c' THEN 'CASCADE' WHEN 'n' THEN 'SET NULL'
                                   WHEN 'd' THEN 'SET DEFAULT' END AS on_delete,
              CASE con.confupdtype WHEN 'a' THEN 'NO ACTION' WHEN 'r' THEN 'RESTRICT'
                                   WHEN 'c' THEN 'CASCADE' WHEN 'n' THEN 'SET NULL'
                                   WHEN 'd' THEN 'SET DEFAULT' END AS on_update
       FROM pg_constraint con
       JOIN pg_class c ON c.oid = con.conrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
       JOIN pg_class ref_c ON ref_c.oid = con.confrelid
       JOIN pg_namespace ref_n ON ref_n.oid = ref_c.relnamespace
       JOIN unnest(con.conkey) WITH ORDINALITY u(attnum, ord) ON TRUE
       JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = u.attnum
       JOIN unnest(con.confkey) WITH ORDINALITY ru(attnum, ord) ON ru.ord = u.ord
       JOIN pg_attribute ref_att ON ref_att.attrelid = con.confrelid AND ref_att.attnum = ru.attnum
       WHERE con.contype = 'f' AND n.nspname = $1 AND c.relname = $2
       GROUP BY con.oid, con.conname, ref_n.nspname, ref_c.relname,
                con.confdeltype, con.confupdtype, con.oid`,
      [schema, args.table]
    );
    const foreignKeysOut = fkOutRes.rows.map((r) => ({
      constraintName: r.conname,
      columns: r.cols as string[],
      referencedTable: r.ref_table,
      referencedColumns: r.ref_cols as string[],
      onDelete: r.on_delete,
      onUpdate: r.on_update,
    }));

    // FKs coming IN (other tables -> this table)
    const fkInRes = await client.query(
      `SELECT con.conname,
              src_n.nspname || '.' || src_c.relname AS src_table,
              array_agg(src_att.attname ORDER BY u.ord) AS src_cols,
              array_agg(ref_att.attname ORDER BY u.ord) AS ref_cols
       FROM pg_constraint con
       JOIN pg_class c ON c.oid = con.confrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
       JOIN pg_class src_c ON src_c.oid = con.conrelid
       JOIN pg_namespace src_n ON src_n.oid = src_c.relnamespace
       JOIN unnest(con.conkey) WITH ORDINALITY u(attnum, ord) ON TRUE
       JOIN pg_attribute src_att ON src_att.attrelid = con.conrelid AND src_att.attnum = u.attnum
       JOIN unnest(con.confkey) WITH ORDINALITY ru(attnum, ord) ON ru.ord = u.ord
       JOIN pg_attribute ref_att ON ref_att.attrelid = con.confrelid AND ref_att.attnum = ru.attnum
       WHERE con.contype = 'f' AND n.nspname = $1 AND c.relname = $2
       GROUP BY con.oid, con.conname, src_n.nspname, src_c.relname`,
      [schema, args.table]
    );
    const foreignKeysIn = fkInRes.rows.map((r) => ({
      constraintName: r.conname,
      sourceTable: r.src_table,
      sourceColumns: r.src_cols as string[],
      referencedColumns: r.ref_cols as string[],
    }));

    // Indexes
    const idxRes = await client.query(
      `SELECT i.relname AS name,
              pg_get_indexdef(i.oid) AS def,
              ix.indisunique AS is_unique,
              ix.indisprimary AS is_primary
       FROM pg_class t
       JOIN pg_index ix ON ix.indrelid = t.oid
       JOIN pg_class i ON i.oid = ix.indexrelid
       JOIN pg_namespace n ON n.oid = t.relnamespace
       WHERE n.nspname = $1 AND t.relname = $2
       ORDER BY i.relname`,
      [schema, args.table]
    );
    const indexes = idxRes.rows.map((r) => ({
      name: r.name,
      definition: r.def,
      unique: r.is_unique,
      primary: r.is_primary,
    }));

    // Sample rows (with safe identifier)
    let sampleRows: any[] = [];
    if (sampleSize > 0 && columns.length > 0) {
      try {
        const r = await client.query(
          `SELECT * FROM ${qualify(schema, args.table)} LIMIT $1`,
          [sampleSize]
        );
        sampleRows = r.rows;
      } catch {
        // table might be a view that fails to sample; ignore
      }
    }

    // Per-column null %/distinct ratio (best-effort, capped to 20 cols
    // to avoid blowing up on wide tables).
    const profileCols = (args.profile_columns ?? columns.map((c) => c.name)).slice(0, 20);
    const totalRows = Number(sizeRow.row_estimate ?? 0);
    if (totalRows > 0 && profileCols.length > 0) {
      // Use pg_stats for null fraction + n_distinct (free, no scan)
      const stats = await client.query(
        `SELECT attname, null_frac, n_distinct
         FROM pg_stats
         WHERE schemaname = $1 AND tablename = $2
           AND attname = ANY($3::text[])`,
        [schema, args.table, profileCols]
      );
      const statsByCol = new Map<string, { nf: number; nd: number }>();
      for (const row of stats.rows) {
        statsByCol.set(row.attname, { nf: Number(row.null_frac), nd: Number(row.n_distinct) });
      }
      for (const c of columns) {
        const s = statsByCol.get(c.name);
        if (!s) continue;
        c.nullPercent = Math.round(s.nf * 10000) / 100;
        // n_distinct: positive = absolute count, negative = -fraction-of-rows
        if (s.nd >= 0) {
          c.distinctRatio = Math.round((s.nd / totalRows) * 10000) / 10000;
        } else {
          c.distinctRatio = Math.round(-s.nd * 10000) / 10000;
        }
      }
    }

    return {
      schema,
      table: args.table,
      exists: true,
      size: sizeRow.size,
      rowCountEstimate: Number(sizeRow.row_estimate ?? 0),
      columns,
      primaryKey,
      foreignKeysOut,
      foreignKeysIn,
      indexes,
      sampleRows,
      comment: sizeRow.comment ?? undefined,
    };
  } finally {
    release();
  }
}

// ============================================================
// find_dependents
// ============================================================

export interface FindDependentsArgs {
  schema?: string;
  /** Object name. */
  name: string;
  /** Object kind. Default 'table'. */
  kind?: ObjectKind;
  /** Recursion depth limit. Default 5. */
  max_depth?: number;
  server?: string;
  database?: string;
  override_schema?: string;
}

export interface FindDependentsResult {
  target: { schema: string; name: string; kind: ObjectKind };
  totalDependents: number;
  dependents: Array<{
    kind: string;
    schema: string;
    name: string;
    depth: number;
    via: string;
  }>;
  truncatedAtDepth: boolean;
}

export async function findDependents(args: FindDependentsArgs): Promise<FindDependentsResult> {
  if (!args.name) throw new Error('name is required');

  const kind = args.kind ?? 'table';
  const maxDepth = args.max_depth ?? 5;

  const dbManager = getDbManager();
  const hasOverride = args.server || args.database || args.override_schema;
  const override: ConnectionOverride | undefined = hasOverride
    ? { server: args.server, database: args.database, schema: args.override_schema }
    : undefined;
  const { client, release } = await acquireClient(dbManager, override);

  try {
    const schema = args.schema ?? (override?.schema ?? 'public');

    // Resolve target OID
    const oidRow = await resolveOid(client, kind, schema, args.name);
    if (!oidRow) {
      return {
        target: { schema, name: args.name, kind },
        totalDependents: 0,
        dependents: [],
        truncatedAtDepth: false,
      };
    }

    // BFS over pg_depend
    const visited = new Set<number>();
    visited.add(oidRow.oid);
    const queue: Array<{ oid: number; classid: number; depth: number; via: string }> = [
      { oid: oidRow.oid, classid: oidRow.classid, depth: 0, via: 'self' },
    ];
    const dependents: FindDependentsResult['dependents'] = [];
    let truncatedAtDepth = false;

    while (queue.length > 0) {
      const { oid, classid, depth, via } = queue.shift()!;
      if (depth > maxDepth) {
        truncatedAtDepth = true;
        continue;
      }

      // Find what depends on this OID
      const r = await client.query(
        `SELECT DISTINCT
                d.classid,
                d.objid,
                d.deptype
         FROM pg_depend d
         WHERE d.refclassid = $1 AND d.refobjid = $2
           AND d.classid <> 0`,
        [classid, oid]
      );

      for (const row of r.rows) {
        const depOid = Number(row.objid);
        const depClass = Number(row.classid);
        if (visited.has(depOid)) continue;
        visited.add(depOid);

        const info = await describeOid(client, depClass, depOid);
        if (!info) continue;
        // Skip internal / extension-owned objects unless they're at depth 0
        if (info.kindLabel === 'extension') continue;

        dependents.push({
          kind: info.kindLabel,
          schema: info.schema,
          name: info.name,
          depth: depth + 1,
          via: via === 'self' ? `pg_depend (${depthLabel(row.deptype)})` : via + ` → pg_depend`,
        });
        if (depth + 1 < maxDepth) {
          queue.push({ oid: depOid, classid: depClass, depth: depth + 1, via: 'pg_depend' });
        }
      }
    }

    return {
      target: { schema, name: args.name, kind },
      totalDependents: dependents.length,
      dependents,
      truncatedAtDepth,
    };
  } finally {
    release();
  }
}

function depthLabel(deptype: string): string {
  switch (deptype) {
    case 'n': return 'normal';
    case 'a': return 'auto';
    case 'i': return 'internal';
    case 'e': return 'extension';
    case 'p': return 'pin';
    default: return deptype;
  }
}

interface OidInfo {
  oid: number;
  classid: number;
}

async function resolveOid(
  client: PoolClient,
  kind: ObjectKind,
  schema: string,
  name: string
): Promise<OidInfo | null> {
  switch (kind) {
    case 'table':
    case 'view':
    case 'matview':
    case 'sequence':
    case 'index': {
      const r = await client.query(
        `SELECT c.oid::int AS oid, 'pg_class'::regclass::oid::int AS classid
         FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = $1 AND c.relname = $2`,
        [schema, name]
      );
      return r.rows[0] ?? null;
    }
    case 'function':
    case 'procedure': {
      const r = await client.query(
        `SELECT p.oid::int AS oid, 'pg_proc'::regclass::oid::int AS classid
         FROM pg_proc p
         JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = $1 AND p.proname = $2 LIMIT 1`,
        [schema, name]
      );
      return r.rows[0] ?? null;
    }
    case 'type': {
      const r = await client.query(
        `SELECT t.oid::int AS oid, 'pg_type'::regclass::oid::int AS classid
         FROM pg_type t
         JOIN pg_namespace n ON n.oid = t.typnamespace
         WHERE n.nspname = $1 AND t.typname = $2`,
        [schema, name]
      );
      return r.rows[0] ?? null;
    }
    case 'extension': {
      const r = await client.query(
        `SELECT e.oid::int AS oid, 'pg_extension'::regclass::oid::int AS classid
         FROM pg_extension e WHERE e.extname = $1`,
        [name]
      );
      return r.rows[0] ?? null;
    }
    case 'schema': {
      const r = await client.query(
        `SELECT n.oid::int AS oid, 'pg_namespace'::regclass::oid::int AS classid
         FROM pg_namespace n WHERE n.nspname = $1`,
        [name]
      );
      return r.rows[0] ?? null;
    }
    default:
      return null;
  }
}

async function describeOid(
  client: PoolClient,
  classid: number,
  objid: number
): Promise<{ kindLabel: string; schema: string; name: string } | null> {
  // Look up classid -> table name to know which catalog to query
  const classNameRes = await client.query(
    `SELECT relname FROM pg_class WHERE oid = $1`,
    [classid]
  );
  if (classNameRes.rows.length === 0) return null;
  const className = classNameRes.rows[0].relname;

  if (className === 'pg_class') {
    const r = await client.query(
      `SELECT n.nspname AS schema, c.relname AS name,
              CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view'
                             WHEN 'm' THEN 'matview' WHEN 'S' THEN 'sequence'
                             WHEN 'i' THEN 'index' ELSE c.relkind::text END AS kind
       FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.oid = $1`,
      [objid]
    );
    if (r.rows.length === 0) return null;
    return { kindLabel: r.rows[0].kind, schema: r.rows[0].schema, name: r.rows[0].name };
  }
  if (className === 'pg_proc') {
    const r = await client.query(
      `SELECT n.nspname AS schema, p.proname AS name,
              CASE p.prokind WHEN 'p' THEN 'procedure' ELSE 'function' END AS kind
       FROM pg_proc p
       JOIN pg_namespace n ON n.oid = p.pronamespace
       WHERE p.oid = $1`,
      [objid]
    );
    if (r.rows.length === 0) return null;
    return { kindLabel: r.rows[0].kind, schema: r.rows[0].schema, name: r.rows[0].name };
  }
  if (className === 'pg_type') {
    const r = await client.query(
      `SELECT n.nspname AS schema, t.typname AS name
       FROM pg_type t
       JOIN pg_namespace n ON n.oid = t.typnamespace
       WHERE t.oid = $1`,
      [objid]
    );
    if (r.rows.length === 0) return null;
    return { kindLabel: 'type', schema: r.rows[0].schema, name: r.rows[0].name };
  }
  if (className === 'pg_constraint') {
    const r = await client.query(
      `SELECT n.nspname AS schema, c.relname AS table_name, con.conname AS name, con.contype
       FROM pg_constraint con
       JOIN pg_class c ON c.oid = con.conrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE con.oid = $1`,
      [objid]
    );
    if (r.rows.length === 0) return null;
    const ct = r.rows[0].contype;
    const label = ct === 'f' ? 'foreign-key' : ct === 'p' ? 'primary-key' : ct === 'u' ? 'unique' : 'constraint';
    return {
      kindLabel: label,
      schema: r.rows[0].schema,
      name: `${r.rows[0].table_name}.${r.rows[0].name}`,
    };
  }
  if (className === 'pg_rewrite') {
    // Rules - typically views' rewrite rules; resolve to the underlying view
    const r = await client.query(
      `SELECT c.relname AS name, n.nspname AS schema
       FROM pg_rewrite r
       JOIN pg_class c ON c.oid = r.ev_class
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE r.oid = $1`,
      [objid]
    );
    if (r.rows.length === 0) return null;
    return { kindLabel: 'rule', schema: r.rows[0].schema, name: r.rows[0].name };
  }
  return null;
}

// ============================================================
// schema_diff
// ============================================================

export interface SchemaDiffEndpoint {
  server?: string;
  database?: string;
  schema?: string;
}

export interface SchemaDiffArgs {
  /** Source of truth. */
  source: SchemaDiffEndpoint;
  /** Target to be migrated. */
  target: SchemaDiffEndpoint;
}

export interface SchemaDiffResult {
  source: { server: string; database: string; schema: string };
  target: { server: string; database: string; schema: string };
  /** Objects present in source but absent in target → CREATE statements */
  toCreate: Array<{ kind: ObjectKind; name: string; sql: string }>;
  /** Objects present in target but absent in source → DROP statements */
  toDrop: Array<{ kind: ObjectKind; name: string; sql: string }>;
  /** Objects in both, with differing DDL → REPLACE/ALTER statements */
  toModify: Array<{
    kind: ObjectKind;
    name: string;
    sourceDdl: string;
    targetDdl: string;
    suggestedSql: string;
  }>;
  /** Combined SQL script: ordered DROPs (reverse), then CREATEs (forward),
   *  then REPLACE statements where applicable. Run on target to converge. */
  migrationSql: string;
}

export async function schemaDiff(args: SchemaDiffArgs): Promise<SchemaDiffResult> {
  if (!args.source?.server) throw new Error('source.server is required');
  if (!args.target?.server) throw new Error('target.server is required');

  const dbManager = getDbManager();
  const sourceClient = await dbManager.getClientWithOverride({
    server: args.source.server,
    database: args.source.database,
    schema: args.source.schema,
  });
  const targetClient = await dbManager.getClientWithOverride({
    server: args.target.server,
    database: args.target.database,
    schema: args.target.schema,
  });

  try {
    const sourceObjs = await listObjectsInScope(sourceClient.client, { schema: sourceClient.schema }, 'all');
    const targetObjs = await listObjectsInScope(targetClient.client, { schema: targetClient.schema }, 'all');

    // Key by kind+name
    const keyOf = (o: typeof sourceObjs[number]): string => `${o.kind}:${o.name}`;
    const sourceMap = new Map(sourceObjs.map((o) => [keyOf(o), o]));
    const targetMap = new Map(targetObjs.map((o) => [keyOf(o), o]));

    const toCreate: SchemaDiffResult['toCreate'] = [];
    const toDrop: SchemaDiffResult['toDrop'] = [];
    const toModify: SchemaDiffResult['toModify'] = [];

    // Source - Target = CREATE
    for (const [key, src] of sourceMap) {
      if (!targetMap.has(key)) {
        const ddl = await extractObjectDDL(sourceClient.client, src);
        toCreate.push({ kind: src.kind, name: src.name, sql: ddl.sql });
      }
    }

    // Target - Source = DROP
    for (const [key, tgt] of targetMap) {
      if (!sourceMap.has(key)) {
        const drop = buildDropStatement(tgt.kind, tgt.schema, tgt.name);
        if (drop) toDrop.push({ kind: tgt.kind, name: tgt.name, sql: drop });
      }
    }

    // Intersection: compare DDL
    for (const [key, src] of sourceMap) {
      const tgt = targetMap.get(key);
      if (!tgt) continue;
      const srcDdl = await extractObjectDDL(sourceClient.client, src);
      const tgtDdl = await extractObjectDDL(targetClient.client, tgt);
      if (normalize(srcDdl.sql) !== normalize(tgtDdl.sql)) {
        // Suggest a CREATE OR REPLACE for views/functions; DROP+CREATE for others
        let suggested: string;
        if (src.kind === 'view' || src.kind === 'function' || src.kind === 'procedure') {
          suggested = srcDdl.sql; // CREATE OR REPLACE
        } else {
          const drop = buildDropStatement(src.kind, src.schema, src.name);
          suggested = (drop ?? '') + '\n' + srcDdl.sql;
        }
        toModify.push({
          kind: src.kind,
          name: src.name,
          sourceDdl: srcDdl.sql,
          targetDdl: tgtDdl.sql,
          suggestedSql: suggested,
        });
      }
    }

    // Build migration SQL
    const lines: string[] = [
      '-- ────────────────────────────────────────────────────────────',
      '-- schema_diff migration script',
      `-- source: server="${args.source.server}" db="${args.source.database ?? '(default)'}" schema="${sourceClient.schema}"`,
      `-- target: server="${args.target.server}" db="${args.target.database ?? '(default)'}" schema="${targetClient.schema}"`,
      `-- timestamp: ${new Date().toISOString()}`,
      `-- run on TARGET to converge schema with SOURCE`,
      '-- ────────────────────────────────────────────────────────────',
      '',
    ];
    if (toDrop.length > 0) {
      lines.push('-- DROPs (target objects not in source)');
      for (const d of toDrop) lines.push(`-- ${d.kind}: ${d.name}`, d.sql, '');
    }
    if (toModify.length > 0) {
      lines.push('-- MODIFIES (DDL drift between source and target)');
      for (const m of toModify) lines.push(`-- ${m.kind}: ${m.name}`, m.suggestedSql, '');
    }
    if (toCreate.length > 0) {
      lines.push('-- CREATEs (source objects not in target)');
      for (const c of toCreate) lines.push(`-- ${c.kind}: ${c.name}`, c.sql, '');
    }

    return {
      source: { server: sourceClient.server, database: sourceClient.database, schema: sourceClient.schema },
      target: { server: targetClient.server, database: targetClient.database, schema: targetClient.schema },
      toCreate,
      toDrop,
      toModify,
      migrationSql: lines.join('\n'),
    };
  } finally {
    sourceClient.release();
    targetClient.release();
  }
}

function buildDropStatement(kind: ObjectKind, schema: string, name: string): string | null {
  switch (kind) {
    case 'table': return `DROP TABLE IF EXISTS ${qualify(schema, name)} CASCADE;`;
    case 'view': return `DROP VIEW IF EXISTS ${qualify(schema, name)} CASCADE;`;
    case 'matview': return `DROP MATERIALIZED VIEW IF EXISTS ${qualify(schema, name)} CASCADE;`;
    case 'sequence': return `DROP SEQUENCE IF EXISTS ${qualify(schema, name)} CASCADE;`;
    case 'index': return `DROP INDEX IF EXISTS ${qualify(schema, name)};`;
    case 'extension': return `DROP EXTENSION IF EXISTS ${safeIdent(name)} CASCADE;`;
    case 'function': return `DROP FUNCTION IF EXISTS ${qualify(schema, name)} CASCADE;`;
    case 'procedure': return `DROP PROCEDURE IF EXISTS ${qualify(schema, name)} CASCADE;`;
    case 'type': return `DROP TYPE IF EXISTS ${qualify(schema, name)} CASCADE;`;
    case 'trigger': return null; // would need underlying table
    case 'schema': return `DROP SCHEMA IF EXISTS ${safeIdent(name)} CASCADE;`;
  }
}

function normalize(sql: string): string {
  // Strip comments + collapse whitespace for comparison
  return sql
    .replace(/--[^\n]*/g, '')
    .replace(/\/\*[\s\S]*?\*\//g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

// ============================================================
// shared helper
// ============================================================

async function acquireClient(
  dbManager: ReturnType<typeof getDbManager>,
  override: ConnectionOverride | undefined
): Promise<{ client: PoolClient; release: () => void }> {
  if (override) {
    const r = await dbManager.getClientWithOverride(override);
    return { client: r.client, release: r.release };
  }
  const c = await dbManager.getClient();
  return { client: c, release: () => c.release() };
}
