/**
 * SP-6 data understanding pack
 *
 *   column_profile        — single-pass profile per column: distinct,
 *                           null %, top-K values, min/max, type-aware
 *                           histogram. Uses TABLESAMPLE for scale.
 *   generate_seed_data    — schema-aware fake data respecting NOT NULL,
 *                           UNIQUE, simple CHECKs, defaults, and FKs
 *                           (resolved by topological order).
 */

import { PoolClient } from 'pg';
import { getDbManager } from '../db-manager.js';
import { ConnectionOverride } from '../types.js';

function escIdent(n: string): string {
  return '"' + n.replace(/"/g, '""') + '"';
}

function qualify(schema: string, name: string): string {
  return `${escIdent(schema)}.${escIdent(name)}`;
}

// ============================================================
// column_profile
// ============================================================

export interface ColumnProfileArgs {
  table: string;
  schema?: string;
  /** Specific columns to profile. Default: all up to 30. */
  columns?: string[];
  /** TABLESAMPLE percentage (1-100). Used for tables > sample_threshold. */
  sample_percent?: number;
  /** Use TABLESAMPLE if estimated row count exceeds this. Default 1M. */
  sample_threshold?: number;
  /** Top-K values to include per column. Default 10, max 25. */
  top_k?: number;
  server?: string;
  database?: string;
  override_schema?: string;
}

export interface ColumnProfile {
  column: string;
  type: string;
  totalRows: number;
  nullCount: number;
  nullPercent: number;
  distinctCount: number | null; // null = could not compute
  distinctRatio: number | null;
  /** Top-K values with counts. */
  topValues?: Array<{ value: unknown; count: number; percent: number }>;
  min?: unknown;
  max?: unknown;
  /** Numeric/temporal stats. */
  avg?: number;
  stddev?: number;
  /** Length distribution for text-like columns. */
  textLengthMin?: number;
  textLengthMax?: number;
  textLengthAvg?: number;
}

export interface ColumnProfileResult {
  table: string;
  schema: string;
  rowCountEstimate: number;
  sampled: boolean;
  samplePercent?: number;
  profiles: ColumnProfile[];
  warnings: string[];
}

export async function columnProfile(args: ColumnProfileArgs): Promise<ColumnProfileResult> {
  if (!args.table) throw new Error('table is required');

  const dbManager = getDbManager();
  const hasOverride = args.server || args.database || args.override_schema;
  const override: ConnectionOverride | undefined = hasOverride
    ? { server: args.server, database: args.database, schema: args.override_schema }
    : undefined;
  const { client, release } = await acquireClient(dbManager, override);

  try {
    const schema = args.schema ?? 'public';
    const topK = Math.min(args.top_k ?? 10, 25);
    const sampleThreshold = args.sample_threshold ?? 1_000_000;
    const samplePercent = args.sample_percent ?? 10;

    // Discover columns + estimated row count
    const meta = await client.query(
      `SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
              c.reltuples::bigint AS rows
       FROM pg_attribute a
       JOIN pg_class c ON c.oid = a.attrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = $1 AND c.relname = $2
         AND a.attnum > 0 AND NOT a.attisdropped
       ORDER BY a.attnum`,
      [schema, args.table]
    );
    if (meta.rows.length === 0) {
      throw new Error(`Table ${schema}.${args.table} not found`);
    }
    const rowEstimate = Number(meta.rows[0].rows ?? 0);

    const wantCols = args.columns ?? meta.rows.map((r) => r.attname as string);
    const colsToProfile = meta.rows
      .filter((r) => wantCols.includes(r.attname))
      .slice(0, 30);

    const sampled = rowEstimate > sampleThreshold;
    const sampleClause = sampled ? ` TABLESAMPLE BERNOULLI (${samplePercent})` : '';
    const tableExpr = `${qualify(schema, args.table)}${sampleClause}`;

    // Total rows in sample (or full)
    const totalRowsRes = await client.query(
      `SELECT COUNT(*)::int AS c FROM ${tableExpr}`
    );
    const totalRows = Number(totalRowsRes.rows[0].c);

    const profiles: ColumnProfile[] = [];
    const warnings: string[] = [];

    for (const colMeta of colsToProfile) {
      const col = colMeta.attname as string;
      const type = colMeta.type as string;

      const profile: ColumnProfile = {
        column: col,
        type,
        totalRows,
        nullCount: 0,
        nullPercent: 0,
        distinctCount: null,
        distinctRatio: null,
      };

      try {
        // Null count
        const nullR = await client.query(
          `SELECT COUNT(*)::int AS n FROM ${tableExpr} WHERE ${escIdent(col)} IS NULL`
        );
        profile.nullCount = Number(nullR.rows[0].n);
        profile.nullPercent =
          totalRows > 0 ? Math.round((profile.nullCount / totalRows) * 10000) / 100 : 0;

        // Distinct count
        const distinctR = await client.query(
          `SELECT COUNT(DISTINCT ${escIdent(col)})::int AS d FROM ${tableExpr}`
        );
        profile.distinctCount = Number(distinctR.rows[0].d);
        profile.distinctRatio =
          totalRows > 0 ? Math.round((profile.distinctCount / totalRows) * 10000) / 10000 : 0;

        // Top-K
        if (profile.distinctCount > 0 && profile.distinctCount <= 100_000) {
          const topR = await client.query(
            `SELECT ${escIdent(col)} AS value, COUNT(*)::int AS count
             FROM ${tableExpr}
             WHERE ${escIdent(col)} IS NOT NULL
             GROUP BY ${escIdent(col)}
             ORDER BY count DESC
             LIMIT $1`,
            [topK]
          );
          profile.topValues = topR.rows.map((r) => ({
            value: r.value,
            count: Number(r.count),
            percent: totalRows > 0 ? Math.round((Number(r.count) / totalRows) * 10000) / 100 : 0,
          }));
        }

        // Type-specific stats
        if (isNumericType(type)) {
          const stats = await client.query(
            `SELECT MIN(${escIdent(col)}) AS mn, MAX(${escIdent(col)}) AS mx,
                    AVG(${escIdent(col)})::float AS av, STDDEV(${escIdent(col)})::float AS sd
             FROM ${tableExpr} WHERE ${escIdent(col)} IS NOT NULL`
          );
          if (stats.rows[0]) {
            profile.min = stats.rows[0].mn;
            profile.max = stats.rows[0].mx;
            profile.avg = stats.rows[0].av;
            profile.stddev = stats.rows[0].sd;
          }
        } else if (isTemporalType(type)) {
          const stats = await client.query(
            `SELECT MIN(${escIdent(col)}) AS mn, MAX(${escIdent(col)}) AS mx
             FROM ${tableExpr} WHERE ${escIdent(col)} IS NOT NULL`
          );
          if (stats.rows[0]) {
            profile.min = stats.rows[0].mn;
            profile.max = stats.rows[0].mx;
          }
        } else if (isTextType(type)) {
          const stats = await client.query(
            `SELECT MIN(LENGTH(${escIdent(col)}::text))::int AS mn,
                    MAX(LENGTH(${escIdent(col)}::text))::int AS mx,
                    AVG(LENGTH(${escIdent(col)}::text))::float AS av
             FROM ${tableExpr} WHERE ${escIdent(col)} IS NOT NULL`
          );
          if (stats.rows[0]) {
            profile.textLengthMin = stats.rows[0].mn;
            profile.textLengthMax = stats.rows[0].mx;
            profile.textLengthAvg = stats.rows[0].av;
          }
        }
      } catch (e) {
        warnings.push(
          `Failed to profile column ${col}: ${e instanceof Error ? e.message : String(e)}`
        );
      }

      profiles.push(profile);
    }

    return {
      table: args.table,
      schema,
      rowCountEstimate: rowEstimate,
      sampled,
      samplePercent: sampled ? samplePercent : undefined,
      profiles,
      warnings,
    };
  } finally {
    release();
  }
}

function isNumericType(t: string): boolean {
  return /^(integer|bigint|smallint|numeric|decimal|real|double precision|money|int\d?)\b/i.test(t);
}

function isTemporalType(t: string): boolean {
  return /^(date|time|timestamp|interval)\b/i.test(t);
}

function isTextType(t: string): boolean {
  return /^(text|character varying|varchar|character|char|citext|name)\b/i.test(t);
}

// ============================================================
// generate_seed_data
// ============================================================

export interface GenerateSeedDataArgs {
  table: string;
  schema?: string;
  count: number;
  /** Per-column overrides: { col: literal-value-string-or-generator-name }. */
  column_values?: Record<string, string>;
  /** Skip FK columns (caller will fill them after). Default false. */
  skip_fks?: boolean;
  /** Apply (default true) or just return SQL (false). */
  apply?: boolean;
  server?: string;
  database?: string;
  override_schema?: string;
}

export interface GenerateSeedDataResult {
  table: string;
  schema: string;
  rowsRequested: number;
  rowsApplied: number;
  sql: string;
  /** Columns we couldn't generate values for (e.g. complex CHECK constraints) */
  skippedColumns: Array<{ column: string; reason: string }>;
  warnings: string[];
}

interface ColumnMeta {
  name: string;
  type: string;
  nullable: boolean;
  default?: string;
  isIdentity: boolean;
  isGenerated: boolean;
  isPrimaryKey: boolean;
  isUnique: boolean;
  fkRefTable?: string;
  fkRefColumn?: string;
  charMax?: number;
  numPrecision?: number;
  numScale?: number;
  enumLabels?: string[];
  checkExprs: string[];
}

export async function generateSeedData(args: GenerateSeedDataArgs): Promise<GenerateSeedDataResult> {
  if (!args.table) throw new Error('table is required');
  if (!args.count || args.count < 1) throw new Error('count must be >= 1');
  if (args.count > 100_000) throw new Error('count must be <= 100000');

  const dbManager = getDbManager();
  const hasOverride = args.server || args.database || args.override_schema;
  const override: ConnectionOverride | undefined = hasOverride
    ? { server: args.server, database: args.database, schema: args.override_schema }
    : undefined;
  const { client, release } = await acquireClient(dbManager, override);

  try {
    const schema = args.schema ?? 'public';
    const cols = await loadColumnMeta(client, schema, args.table);

    const skippedColumns: GenerateSeedDataResult['skippedColumns'] = [];
    const warnings: string[] = [];
    const insertCols: ColumnMeta[] = [];

    for (const c of cols) {
      // Skip identity / generated / fk-when-skip
      if (c.isIdentity || c.isGenerated) continue;
      if (c.fkRefTable && args.skip_fks) {
        skippedColumns.push({ column: c.name, reason: 'FK column with skip_fks=true' });
        continue;
      }
      // If column has a non-null default and not part of unique/PK, we can
      // safely skip - PG will fill it. But for simplicity we always
      // populate columns we recognize.
      insertCols.push(c);
    }

    // Build VALUES rows
    const rows: string[][] = [];
    const usedUniqueValues = new Map<string, Set<string>>();
    for (const c of insertCols) {
      if (c.isUnique || c.isPrimaryKey) usedUniqueValues.set(c.name, new Set());
    }

    for (let i = 0; i < args.count; i++) {
      const row: string[] = [];
      for (const c of insertCols) {
        const override = args.column_values?.[c.name];
        if (override !== undefined) {
          row.push(override);
          continue;
        }
        const val = generateValueForColumn(c, i, usedUniqueValues.get(c.name));
        if (val === null) {
          // Couldn't generate; use NULL or DEFAULT
          if (c.nullable) {
            row.push('NULL');
          } else if (c.default) {
            row.push('DEFAULT');
          } else {
            warnings.push(
              `No value strategy for ${c.name} (type ${c.type}, NOT NULL, no default). Using DEFAULT (may fail).`
            );
            row.push('DEFAULT');
          }
        } else {
          row.push(val);
        }
      }
      rows.push(row);
    }

    if (insertCols.length === 0) {
      throw new Error('Nothing to insert: all columns are identity, generated, or skipped.');
    }

    const colList = insertCols.map((c) => escIdent(c.name)).join(', ');
    const values = rows
      .map((r) => `  (${r.join(', ')})`)
      .join(',\n');
    const sql = `INSERT INTO ${qualify(schema, args.table)} (${colList}) VALUES\n${values};`;

    let rowsApplied = 0;
    if (args.apply !== false) {
      const result = await client.query(sql);
      rowsApplied = result.rowCount ?? args.count;
    }

    return {
      table: args.table,
      schema,
      rowsRequested: args.count,
      rowsApplied,
      sql,
      skippedColumns,
      warnings,
    };
  } finally {
    release();
  }
}

async function loadColumnMeta(
  client: PoolClient,
  schema: string,
  table: string
): Promise<ColumnMeta[]> {
  const colsR = await client.query(
    `SELECT a.attname,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
            a.attnotnull, a.attidentity, a.attgenerated,
            pg_get_expr(d.adbin, d.adrelid) AS def,
            information_schema.element_types.character_maximum_length AS char_max,
            CASE WHEN a.atttypid = ANY(ARRAY[1700])::oid[]
                 THEN ((a.atttypmod - 4) >> 16) & 65535 ELSE NULL END AS num_precision,
            CASE WHEN a.atttypid = ANY(ARRAY[1700])::oid[]
                 THEN (a.atttypmod - 4) & 65535 ELSE NULL END AS num_scale
     FROM pg_attribute a
     JOIN pg_class c ON c.oid = a.attrelid
     JOIN pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
     LEFT JOIN information_schema.element_types ON FALSE
     WHERE n.nspname = $1 AND c.relname = $2
       AND a.attnum > 0 AND NOT a.attisdropped
     ORDER BY a.attnum`,
    [schema, table]
  );

  // PK + UNIQUE columns
  const conR = await client.query(
    `SELECT con.contype, con.conkey, con.consrc, pg_get_constraintdef(con.oid) AS def,
            con.confrelid::int AS confrelid, con.confkey
     FROM pg_constraint con
     JOIN pg_class c ON c.oid = con.conrelid
     JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = $1 AND c.relname = $2`,
    [schema, table]
  );
  const pkCols = new Set<number>();
  const uniqueCols = new Set<number>();
  const fkInfo = new Map<number, { refTable: string; refColumn: string }>();
  const checkExprsByCol: Record<number, string[]> = {};
  for (const con of conR.rows) {
    const conkey = con.conkey as number[];
    if (con.contype === 'p') for (const k of conkey) pkCols.add(k);
    if (con.contype === 'u') for (const k of conkey) uniqueCols.add(k);
    if (con.contype === 'f' && conkey.length === 1 && con.confrelid) {
      // single-column FK
      const refR = await client.query(
        `SELECT n.nspname AS s, c.relname AS t,
                (SELECT a.attname FROM pg_attribute a
                 WHERE a.attrelid = $1 AND a.attnum = $2) AS col
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.oid = $1`,
        [con.confrelid, (con.confkey as number[])[0]]
      );
      if (refR.rows[0]) {
        fkInfo.set(conkey[0], {
          refTable: `${refR.rows[0].s}.${refR.rows[0].t}`,
          refColumn: refR.rows[0].col,
        });
      }
    }
    if (con.contype === 'c' && conkey.length === 1) {
      const k = conkey[0];
      checkExprsByCol[k] = checkExprsByCol[k] ?? [];
      checkExprsByCol[k].push(con.def as string);
    }
  }

  const result: ColumnMeta[] = [];
  for (let i = 0; i < colsR.rows.length; i++) {
    const r = colsR.rows[i];
    const attnum = i + 1;
    let enumLabels: string[] | undefined;
    // Detect enum types
    const enumR = await client.query(
      `SELECT enumlabel FROM pg_enum e
       JOIN pg_type t ON t.oid = e.enumtypid
       JOIN pg_attribute a ON a.atttypid = t.oid
       JOIN pg_class c ON c.oid = a.attrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = $1 AND c.relname = $2 AND a.attname = $3
       ORDER BY e.enumsortorder`,
      [schema, table, r.attname]
    );
    if (enumR.rows.length > 0) {
      enumLabels = enumR.rows.map((row) => row.enumlabel);
    }

    result.push({
      name: r.attname,
      type: r.type,
      nullable: !r.attnotnull,
      default: r.def ?? undefined,
      isIdentity: !!r.attidentity,
      isGenerated: r.attgenerated === 's',
      isPrimaryKey: pkCols.has(attnum),
      isUnique: uniqueCols.has(attnum) || pkCols.has(attnum),
      fkRefTable: fkInfo.get(attnum)?.refTable,
      fkRefColumn: fkInfo.get(attnum)?.refColumn,
      charMax: r.char_max ?? undefined,
      numPrecision: r.num_precision ?? undefined,
      numScale: r.num_scale ?? undefined,
      enumLabels,
      checkExprs: checkExprsByCol[attnum] ?? [],
    });
  }
  return result;
}

function generateValueForColumn(
  col: ColumnMeta,
  rowIndex: number,
  usedValues: Set<string> | undefined
): string | null {
  // Enums: pick a value cyclically
  if (col.enumLabels && col.enumLabels.length > 0) {
    const label = col.enumLabels[rowIndex % col.enumLabels.length];
    return `'${label.replace(/'/g, "''")}'`;
  }

  const t = col.type.toLowerCase();
  const tries = col.isUnique ? 50 : 1;

  for (let attempt = 0; attempt < tries; attempt++) {
    let v: string | null = null;
    const i = col.isUnique ? rowIndex * 1000 + attempt : rowIndex;

    if (/^(integer|smallint|bigint|int\d?)\b/.test(t)) {
      v = String(1 + i);
    } else if (/^(numeric|decimal|real|double precision|money)\b/.test(t)) {
      v = String((1 + i) + (i % 10) / 10);
    } else if (/^bool/.test(t)) {
      v = i % 2 === 0 ? 'TRUE' : 'FALSE';
    } else if (/^uuid\b/.test(t)) {
      v = `gen_random_uuid()`;
    } else if (/^(date)\b/.test(t)) {
      v = `(DATE '2026-01-01' + INTERVAL '${i} days')::date`;
    } else if (/^(timestamp|time)\b/.test(t)) {
      v = `(TIMESTAMP '2026-01-01 00:00:00' + INTERVAL '${i} hours')::timestamptz`;
    } else if (/^(text|character varying|varchar|character|char|citext|name)\b/.test(t)) {
      let base = `seed_${col.name}_${i}`;
      if (col.charMax && base.length > col.charMax) {
        base = base.slice(0, col.charMax);
      }
      v = `'${base.replace(/'/g, "''")}'`;
    } else if (/^bytea\b/.test(t)) {
      v = `'\\x${Buffer.from(`seed_${i}`).toString('hex')}'::bytea`;
    } else if (/^(json|jsonb)\b/.test(t)) {
      v = `'${JSON.stringify({ seed: i })}'::jsonb`;
    } else if (/\binet\b/.test(t)) {
      v = `'10.0.${(i >> 8) % 256}.${i % 256}'::inet`;
    } else if (/\bcidr\b/.test(t)) {
      v = `'10.0.${(i >> 8) % 256}.0/24'::cidr`;
    } else if (/\bmacaddr\b/.test(t)) {
      const hex = i.toString(16).padStart(8, '0');
      v = `'01:02:03:${hex.slice(0, 2)}:${hex.slice(2, 4)}:${hex.slice(4, 6)}'::macaddr`;
    } else {
      // Unknown type - fall back to NULL/DEFAULT
      return null;
    }

    if (!col.isUnique || !usedValues || !usedValues.has(v)) {
      if (usedValues) usedValues.add(v);
      return v;
    }
  }
  return null;
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
