/**
 * SP-5 migration safety pack
 *
 *   lock_check               — static SQL analysis → required lock
 *                              level + duration estimate from target
 *                              table size.
 *   detect_migration_state   — probe for Liquibase, Flyway, Alembic,
 *                              Prisma, Knex, Sequelize, Django, Rails
 *                              migration tracker tables.
 *   safe_alter_table         — high-level intent → zero-downtime DDL
 *                              recipe (multi-step plan).
 */

import { PoolClient } from 'pg';
import { getDbManager } from '../db-manager.js';
import { ConnectionOverride } from '../types.js';

// ============================================================
// lock_check
// ============================================================

/**
 * PostgreSQL lock levels, ordered from least → most restrictive.
 *
 * AccessShareLock (1)            — SELECT
 * RowShareLock (2)               — SELECT FOR UPDATE/SHARE
 * RowExclusiveLock (3)           — INSERT/UPDATE/DELETE
 * ShareUpdateExclusiveLock (4)   — VACUUM, ANALYZE, CREATE INDEX CONCURRENTLY
 * ShareLock (5)                  — CREATE INDEX (non-concurrent)
 * ShareRowExclusiveLock (6)      — CREATE COLLATION etc
 * ExclusiveLock (7)              — REFRESH MATERIALIZED VIEW CONCURRENTLY
 * AccessExclusiveLock (8)        — DROP TABLE, ALTER TABLE (most variants)
 *
 * AccessShareLock and AccessExclusiveLock conflict; AccessExclusiveLock
 * blocks ALL other locks (including SELECT). This is the dangerous
 * one for production.
 */
type LockLevel =
  | 'AccessShareLock'
  | 'RowShareLock'
  | 'RowExclusiveLock'
  | 'ShareUpdateExclusiveLock'
  | 'ShareLock'
  | 'ShareRowExclusiveLock'
  | 'ExclusiveLock'
  | 'AccessExclusiveLock';

const LOCK_RANK: Record<LockLevel, number> = {
  AccessShareLock: 1,
  RowShareLock: 2,
  RowExclusiveLock: 3,
  ShareUpdateExclusiveLock: 4,
  ShareLock: 5,
  ShareRowExclusiveLock: 6,
  ExclusiveLock: 7,
  AccessExclusiveLock: 8,
};

/** Mapping from DDL statement pattern → required lock level. */
const LOCK_MAPPING: Array<{
  match: RegExp;
  lock: LockLevel;
  forcesRewrite: boolean;
  notes: string;
}> = [
  { match: /^DROP\s+TABLE\b/i, lock: 'AccessExclusiveLock', forcesRewrite: false,
    notes: 'DROP TABLE blocks all access until commit.' },
  { match: /^TRUNCATE\b/i, lock: 'AccessExclusiveLock', forcesRewrite: true,
    notes: 'TRUNCATE rewrites the table.' },
  { match: /^DROP\s+INDEX\b(?!.*CONCURRENTLY)/i, lock: 'AccessExclusiveLock', forcesRewrite: false,
    notes: 'DROP INDEX takes ACCESS EXCLUSIVE on the table; use DROP INDEX CONCURRENTLY for production.' },
  { match: /^DROP\s+INDEX\s+CONCURRENTLY\b/i, lock: 'ShareUpdateExclusiveLock', forcesRewrite: false,
    notes: 'DROP INDEX CONCURRENTLY takes SHARE UPDATE EXCLUSIVE; allows concurrent SELECT/INSERT/UPDATE/DELETE.' },
  { match: /^CREATE\s+INDEX\s+CONCURRENTLY\b/i, lock: 'ShareUpdateExclusiveLock', forcesRewrite: false,
    notes: 'CREATE INDEX CONCURRENTLY allows concurrent reads + writes; safe for production.' },
  { match: /^CREATE\s+INDEX\b/i, lock: 'ShareLock', forcesRewrite: false,
    notes: 'Plain CREATE INDEX takes SHARE lock — blocks writes (UPDATE/INSERT/DELETE) but not reads.' },
  { match: /^REINDEX\s+\w+\s+CONCURRENTLY\b/i, lock: 'ShareUpdateExclusiveLock', forcesRewrite: false,
    notes: 'REINDEX CONCURRENTLY: safe for production.' },
  { match: /^REINDEX\b/i, lock: 'AccessExclusiveLock', forcesRewrite: false,
    notes: 'REINDEX (non-concurrent) takes ACCESS EXCLUSIVE on the table.' },
  { match: /^VACUUM\s+FULL\b/i, lock: 'AccessExclusiveLock', forcesRewrite: true,
    notes: 'VACUUM FULL rewrites the entire table - very long for big tables. Avoid in production.' },
  { match: /^VACUUM\b/i, lock: 'ShareUpdateExclusiveLock', forcesRewrite: false,
    notes: 'VACUUM takes SHARE UPDATE EXCLUSIVE; safe for normal load. VACUUM FULL takes ACCESS EXCLUSIVE — beware.' },
  { match: /^CLUSTER\b/i, lock: 'AccessExclusiveLock', forcesRewrite: true,
    notes: 'CLUSTER rewrites the table. Heavy.' },
  { match: /^REFRESH\s+MATERIALIZED\s+VIEW\s+CONCURRENTLY\b/i, lock: 'ExclusiveLock', forcesRewrite: false,
    notes: 'REFRESH MATERIALIZED VIEW CONCURRENTLY allows reads but blocks other refreshes; requires unique index.' },
  { match: /^REFRESH\s+MATERIALIZED\s+VIEW\b/i, lock: 'AccessExclusiveLock', forcesRewrite: false,
    notes: 'REFRESH MATERIALIZED VIEW (non-concurrent) blocks all access during refresh.' },
];

const ALTER_PATTERNS: Array<{
  match: RegExp;
  lock: LockLevel;
  forcesRewrite: boolean;
  notes: string;
}> = [
  // PG 11+ recipes that reduce locking.
  // The `(?!.*...)` lookahead is flagged by sonarjs/slow-regex; in
  // practice the input is bounded (SQL DDL ≤ 100KB by upstream
  // limit elsewhere) and we accept the worst-case backtracking.
  // eslint-disable-next-line sonarjs/slow-regex
  { match: /ADD\s+COLUMN\s+\w+\s+\w+(?:\([^)]+\))?\s+(?!.*(?:NOT\s+NULL|DEFAULT))/i,
    lock: 'AccessExclusiveLock', forcesRewrite: false,
    notes: 'ADD COLUMN without DEFAULT or NOT NULL: ACCESS EXCLUSIVE briefly (metadata only on PG 11+).' },
  { match: /ADD\s+COLUMN.*DEFAULT.*\b(now\(\)|nextval|gen_random_uuid|random|clock_timestamp)/i,
    lock: 'AccessExclusiveLock', forcesRewrite: true,
    notes: 'ADD COLUMN with VOLATILE DEFAULT (now(), random(), etc.): forces full table rewrite. Heavy for large tables. Recipe: add nullable, backfill in batches, set NOT NULL last.' },
  { match: /ADD\s+COLUMN.*\bNOT\s+NULL\b/i,
    lock: 'AccessExclusiveLock', forcesRewrite: false,
    notes: 'ADD COLUMN NOT NULL with constant DEFAULT: PG 11+ avoids rewrite. Without DEFAULT, fails on existing rows. Use safe_alter_table for the multi-step recipe.' },
  { match: /ADD\s+COLUMN.*DEFAULT/i,
    lock: 'AccessExclusiveLock', forcesRewrite: false,
    notes: 'ADD COLUMN with non-volatile DEFAULT: PG 11+ avoids rewrite (constant default). Otherwise full rewrite.' },
  { match: /DROP\s+COLUMN\b/i,
    lock: 'AccessExclusiveLock', forcesRewrite: false,
    notes: 'DROP COLUMN: ACCESS EXCLUSIVE (metadata only - column is hidden, not physically removed until VACUUM).' },
  { match: /ALTER\s+COLUMN\s+\w+\s+TYPE\b/i,
    lock: 'AccessExclusiveLock', forcesRewrite: true,
    notes: 'ALTER COLUMN TYPE: usually rewrites the table. Some compatible casts (e.g. varchar→text) avoid rewrite. Heavy.' },
  { match: /ALTER\s+COLUMN\s+\w+\s+SET\s+NOT\s+NULL\b/i,
    lock: 'AccessExclusiveLock', forcesRewrite: false,
    notes: 'SET NOT NULL: scans the table to verify (no rewrite). Use a CHECK NOT VALID + VALIDATE recipe to reduce locking on PG 12+.' },
  { match: /ALTER\s+COLUMN\s+\w+\s+DROP\s+NOT\s+NULL\b/i,
    lock: 'AccessExclusiveLock', forcesRewrite: false,
    notes: 'DROP NOT NULL: ACCESS EXCLUSIVE briefly, no scan.' },
  { match: /ALTER\s+COLUMN\s+\w+\s+SET\s+DEFAULT\b/i,
    lock: 'AccessExclusiveLock', forcesRewrite: false,
    notes: 'SET DEFAULT: metadata-only.' },
  { match: /ADD\s+CONSTRAINT.*FOREIGN\s+KEY/i,
    lock: 'ShareRowExclusiveLock', forcesRewrite: false,
    notes: 'ADD FOREIGN KEY: scans the table to validate (blocks writes, allows reads). Use NOT VALID + VALIDATE CONSTRAINT to skip the scan initially.' },
  { match: /ADD\s+CONSTRAINT.*CHECK/i,
    lock: 'AccessExclusiveLock', forcesRewrite: false,
    notes: 'ADD CHECK: scans table to validate. Use NOT VALID + VALIDATE CONSTRAINT to skip initial scan.' },
  { match: /VALIDATE\s+CONSTRAINT\b/i,
    lock: 'ShareUpdateExclusiveLock', forcesRewrite: false,
    notes: 'VALIDATE CONSTRAINT: SHARE UPDATE EXCLUSIVE only; safer than blanket constraint addition.' },
  { match: /RENAME\s+(?:COLUMN|TO)\b/i,
    lock: 'AccessExclusiveLock', forcesRewrite: false,
    notes: 'RENAME: ACCESS EXCLUSIVE briefly, metadata-only.' },
  { match: /SET\s+(?:STORAGE|STATISTICS|TABLESPACE)\b/i,
    lock: 'AccessExclusiveLock', forcesRewrite: false,
    notes: 'SET storage/statistics/tablespace: metadata-only (tablespace is heavier).' },
];

export interface LockCheckArgs {
  sql: string;
  /** Get table size estimate to compute duration. Default true. */
  estimate_duration?: boolean;
  server?: string;
  database?: string;
  schema?: string;
}

export interface LockCheckResult {
  sql: string;
  detectedLockLevel: LockLevel | 'unknown';
  forcesTableRewrite: boolean;
  notes: string;
  table?: string;
  tableSize?: string;
  estimatedRowCount?: number;
  estimatedDuration?: string;
  warnings: string[];
  recommendations: string[];
}

export async function lockCheck(args: LockCheckArgs): Promise<LockCheckResult> {
  if (!args.sql) throw new Error('sql is required');

  let stripped = args.sql
    .replace(/--[^\n]*/g, '')
    .replace(/\/\*[\s\S]*?\*\//g, '')
    .trim();

  // Audit-iteration-1 SP-5 P0 fix: peel off outer transaction-control
  // wrappers (`BEGIN; <DDL>; COMMIT;`, `START TRANSACTION; <DDL>; END;`).
  // Without this peel, the anchored ^DDL regex never matches because
  // the SQL begins with BEGIN — lockCheck silently returned `unknown`
  // for any DDL the caller wrapped in a transaction block.
  // bounded DDL input (caller-supplied SQL); accept worst-case backtracking
  /* eslint-disable sonarjs/slow-regex */
  const STRIP_LEADING_TX = /^\s*(?:BEGIN|START\s+TRANSACTION|COMMIT|END|ROLLBACK|ABORT)\s*(?:[A-Z\s]+)?;\s*/i;
  const STRIP_TRAILING_TX = /;\s*(?:COMMIT|END|ROLLBACK|ABORT)\s*;?\s*$/i;
  /* eslint-enable sonarjs/slow-regex */
  for (let i = 0; i < 3; i++) {
    const before = stripped;
    stripped = stripped.replace(STRIP_LEADING_TX, '').replace(STRIP_TRAILING_TX, '').trim();
    if (stripped === before) break;
  }

  let lock: LockLevel | 'unknown' = 'unknown';
  let forcesRewrite = false;
  let notes = '';

  // Check ALTER TABLE patterns first (they have the richest matching)
  const isAlter = /^ALTER\s+TABLE\b/i.test(stripped);
  if (isAlter) {
    let matched = false;
    for (const p of ALTER_PATTERNS) {
      if (p.match.test(stripped)) {
        lock = p.lock;
        forcesRewrite = p.forcesRewrite;
        notes = p.notes;
        matched = true;
        break;
      }
    }
    if (!matched) {
      lock = 'AccessExclusiveLock';
      notes = 'ALTER TABLE: defaults to ACCESS EXCLUSIVE for unknown variants.';
    }
  } else {
    for (const p of LOCK_MAPPING) {
      if (p.match.test(stripped)) {
        lock = p.lock;
        forcesRewrite = p.forcesRewrite;
        notes = p.notes;
        break;
      }
    }
  }

  const result: LockCheckResult = {
    sql: stripped,
    detectedLockLevel: lock,
    forcesTableRewrite: forcesRewrite,
    notes,
    warnings: [],
    recommendations: [],
  };

  // Extract table name (best-effort)
  const tableMatch =
    /^ALTER\s+TABLE\s+(?:ONLY\s+)?(?:IF\s+EXISTS\s+)?([\w".]+)/i.exec(stripped) ||
    /^DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([\w".]+)/i.exec(stripped) ||
    /^TRUNCATE\s+(?:TABLE\s+)?([\w".]+)/i.exec(stripped) ||
    /^CREATE\s+INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?\w+\s+ON\s+([\w".]+)/i.exec(stripped) ||
    /^VACUUM\s+(?:\([^)]+\)\s+)?([\w".]+)/i.exec(stripped) ||
    /^CLUSTER\s+(?:VERBOSE\s+)?([\w".]+)/i.exec(stripped);

  let tableName: string | undefined;
  if (tableMatch) {
    tableName = tableMatch[1].replace(/"/g, '');
    result.table = tableName;
  }

  // Estimate duration based on table size
  if (args.estimate_duration !== false && tableName) {
    const dbManager = getDbManager();
    const hasOverride = args.server || args.database || args.schema;
    const override: ConnectionOverride | undefined = hasOverride
      ? { server: args.server, database: args.database, schema: args.schema }
      : undefined;

    const { client, release } = await acquireClient(dbManager, override);
    try {
      const sizeQuery = await client.query(
        `SELECT pg_size_pretty(pg_total_relation_size(c.oid)) AS size,
                c.reltuples::bigint AS rows
         FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relkind IN ('r', 'm') AND
               (n.nspname || '.' || c.relname = $1 OR c.relname = $1)`,
        [tableName]
      );
      if (sizeQuery.rows.length > 0) {
        result.tableSize = sizeQuery.rows[0].size;
        result.estimatedRowCount = Number(sizeQuery.rows[0].rows);
        // Rough duration estimate: 100K rows/sec for rewrite ops, 1M rows/sec for scans
        if (result.estimatedRowCount && result.estimatedRowCount > 0) {
          const rowsPerSec = forcesRewrite ? 100_000 : 1_000_000;
          const secs = result.estimatedRowCount / rowsPerSec;
          if (secs < 1) result.estimatedDuration = '< 1 second';
          else if (secs < 60) result.estimatedDuration = `${Math.round(secs)} seconds`;
          else if (secs < 3600) result.estimatedDuration = `${Math.round(secs / 60)} minutes`;
          else result.estimatedDuration = `${Math.round(secs / 3600)} hours`;
        }
      }
    } catch {
      // table not found or permission issue - skip
    } finally {
      release();
    }
  }

  // Severity warnings
  const rank = lock === 'unknown' ? 0 : LOCK_RANK[lock];
  if (rank >= LOCK_RANK.AccessExclusiveLock) {
    result.warnings.push(
      'ACCESS EXCLUSIVE LOCK: blocks ALL other access (including SELECT) until commit. ' +
      'For production-busy tables, consider an alternative recipe.'
    );
  }
  if (forcesRewrite && result.estimatedRowCount && result.estimatedRowCount > 1_000_000) {
    result.warnings.push(
      `Forces table rewrite on ${result.estimatedRowCount.toLocaleString()} rows. ` +
      `Long downtime risk. Use safe_alter_table for incremental recipes.`
    );
  }

  // Audit-iteration-1 SP-5 P0 fix: warn that `ADD COLUMN ... NOT NULL`
  // without a DEFAULT will FAIL on any table that already has rows -
  // PG cannot synthesize a value for existing rows. Caller likely
  // wants safe_alter_table's add_not_null_column_with_default recipe.
  if (
    // eslint-disable-next-line sonarjs/slow-regex -- bounded DDL input
    /ADD\s+COLUMN\s+\w+\s+\S+.*\bNOT\s+NULL\b/i.test(stripped) &&
    !/\bDEFAULT\b/i.test(stripped) &&
    !/\bGENERATED\b/i.test(stripped)
  ) {
    result.warnings.push(
      'ADD COLUMN NOT NULL without DEFAULT fails immediately if the table has any rows ' +
      '("column \\"x\\" of relation \\"...\\" contains null values"). For non-empty tables ' +
      'use safe_alter_table({ kind: "add_not_null_column_with_default", ... }) which ' +
      'emits a 4-step safe recipe (add nullable → backfill → set default → set NOT NULL).'
    );
  }

  // Recommendations
  if (/CREATE\s+INDEX\b(?!.*CONCURRENTLY)/i.test(stripped)) {
    result.recommendations.push('Use CREATE INDEX CONCURRENTLY to avoid blocking writes.');
  }
  if (/DROP\s+INDEX\b(?!.*CONCURRENTLY)/i.test(stripped)) {
    result.recommendations.push('Use DROP INDEX CONCURRENTLY to avoid ACCESS EXCLUSIVE lock.');
  }
  if (/ADD\s+CONSTRAINT.*(?:FOREIGN\s+KEY|CHECK)/i.test(stripped) && !/NOT\s+VALID/i.test(stripped)) {
    result.recommendations.push(
      'Use NOT VALID + VALIDATE CONSTRAINT to skip the initial scan: ' +
      'ALTER TABLE ... ADD CONSTRAINT ... NOT VALID; ALTER TABLE ... VALIDATE CONSTRAINT ...'
    );
  }
  if (/ALTER\s+COLUMN\s+\w+\s+SET\s+NOT\s+NULL\b/i.test(stripped)) {
    result.recommendations.push(
      'On PG 12+: add a CHECK (col IS NOT NULL) NOT VALID, validate it, ' +
      'then SET NOT NULL — avoids the long scan inside an ACCESS EXCLUSIVE.'
    );
  }

  return result;
}

// ============================================================
// detect_migration_state
// ============================================================

/**
 * Each probe lists the table name AND a set of "signature" columns —
 * names that must all be present in the table for it to count as that
 * tool. Audit-iteration-1 SP-5 P0 fix: previously the probe was
 * to_regclass-only, so any user table named `migrations` was reported
 * as TypeORM (high false-positive rate). Column shape verification
 * eliminates this collision.
 *
 * Use a generous-but-distinctive subset: enough columns that a casual
 * user wouldn't have all of them by accident, few enough that minor
 * version drift in the migration tool doesn't cause us to miss
 * detection.
 */
const MIGRATION_TOOL_PROBES: Array<{
  tool: string;
  table: string;
  schema?: string;
  versionColumn: string;
  /** Lowercase column names that must all be present. */
  signatureColumns: string[];
}> = [
  { tool: 'Liquibase', table: 'databasechangelog', versionColumn: 'id',
    signatureColumns: ['id', 'author', 'filename', 'dateexecuted', 'md5sum'] },
  { tool: 'Flyway', table: 'flyway_schema_history', versionColumn: 'version',
    signatureColumns: ['installed_rank', 'version', 'description', 'checksum', 'installed_on'] },
  { tool: 'Alembic', table: 'alembic_version', versionColumn: 'version_num',
    signatureColumns: ['version_num'] },
  { tool: 'Prisma', table: '_prisma_migrations', versionColumn: 'migration_name',
    signatureColumns: ['id', 'checksum', 'finished_at', 'migration_name'] },
  { tool: 'Knex', table: 'knex_migrations', versionColumn: 'name',
    signatureColumns: ['id', 'name', 'batch', 'migration_time'] },
  { tool: 'Sequelize', table: 'SequelizeMeta', versionColumn: 'name',
    signatureColumns: ['name'] },
  { tool: 'Django', table: 'django_migrations', versionColumn: 'name',
    signatureColumns: ['id', 'app', 'name', 'applied'] },
  { tool: 'Rails', table: 'schema_migrations', versionColumn: 'version',
    signatureColumns: ['version'] },
  { tool: 'Goose', table: 'goose_db_version', versionColumn: 'version_id',
    signatureColumns: ['id', 'version_id', 'is_applied', 'tstamp'] },
  { tool: 'TypeORM', table: 'migrations', versionColumn: 'name',
    signatureColumns: ['id', 'timestamp', 'name'] },
];

export interface DetectMigrationStateArgs {
  /** Schemas to probe. Default: all non-system schemas. */
  schemas?: string[];
  server?: string;
  database?: string;
  schema?: string;
}

export interface DetectMigrationStateResult {
  detectedTools: Array<{
    tool: string;
    schema: string;
    table: string;
    appliedCount: number;
    latestVersion?: string;
  }>;
  notDetected: string[];
}

export async function detectMigrationState(
  args: DetectMigrationStateArgs = {}
): Promise<DetectMigrationStateResult> {
  const dbManager = getDbManager();
  const hasOverride = args.server || args.database || args.schema;
  const override: ConnectionOverride | undefined = hasOverride
    ? { server: args.server, database: args.database, schema: args.schema }
    : undefined;
  const { client, release } = await acquireClient(dbManager, override);

  try {
    let schemas = args.schemas;
    if (!schemas) {
      const r = await client.query(
        `SELECT nspname FROM pg_namespace
         WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
           AND nspname NOT LIKE 'pg_%'
         ORDER BY nspname`
      );
      schemas = r.rows.map((row) => row.nspname);
    }

    const detected: DetectMigrationStateResult['detectedTools'] = [];
    const notDetected: string[] = [];

    for (const probe of MIGRATION_TOOL_PROBES) {
      let foundIn: string | null = null;
      for (const s of schemas!) {
        const exists = await client.query(
          `SELECT to_regclass($1) AS reg`,
          [`${s}.${probe.table}`]
        );
        if (exists.rows[0].reg === null) continue;
        // Audit-iteration-1 SP-5 P0 fix: verify column shape. Without
        // this, any user table happening to share a probe name (e.g.
        // a business `migrations` table) is misreported as TypeORM.
        const colsR = await client.query(
          `SELECT lower(column_name) AS col
           FROM information_schema.columns
           WHERE table_schema = $1 AND table_name = $2`,
          [s, probe.table]
        );
        const cols = new Set(colsR.rows.map((row) => String(row.col)));
        const missingSig = probe.signatureColumns.filter((c) => !cols.has(c.toLowerCase()));
        if (missingSig.length === 0) {
          foundIn = s;
          break;
        }
      }
      if (!foundIn) {
        notDetected.push(probe.tool);
        continue;
      }
      // Get applied count + latest
      try {
        const countR = await client.query(
          `SELECT COUNT(*)::int AS c FROM ${escapeIdent(foundIn)}.${escapeIdent(probe.table)}`
        );
        const count = countR.rows[0].c;
        let latest: string | undefined;
        try {
          const latestR = await client.query(
            `SELECT ${escapeIdent(probe.versionColumn)} AS v
             FROM ${escapeIdent(foundIn)}.${escapeIdent(probe.table)}
             ORDER BY ${escapeIdent(probe.versionColumn)} DESC LIMIT 1`
          );
          latest = latestR.rows[0]?.v ? String(latestR.rows[0].v) : undefined;
        } catch {
          // ordering by version column might fail (different types)
        }
        detected.push({
          tool: probe.tool,
          schema: foundIn,
          table: probe.table,
          appliedCount: count,
          latestVersion: latest,
        });
      } catch {
        notDetected.push(probe.tool);
      }
    }

    return { detectedTools: detected, notDetected };
  } finally {
    release();
  }
}

function escapeIdent(name: string): string {
  return '"' + name.replace(/"/g, '""') + '"';
}

// ============================================================
// safe_alter_table
// ============================================================

export type SafeAlterIntent =
  | { kind: 'add_not_null_column_with_default'; table: string; column: string; type: string; default_expr: string }
  | { kind: 'add_not_null'; table: string; column: string }
  | { kind: 'add_foreign_key'; table: string; constraint_name: string; columns: string[]; references_table: string; references_columns: string[] }
  | { kind: 'add_check'; table: string; constraint_name: string; check_expr: string }
  | { kind: 'create_index'; table: string; index_name: string; columns: string[]; index_type?: string; unique?: boolean }
  | { kind: 'drop_index'; index_name: string; schema?: string };

export interface SafeAlterTableArgs {
  intent: SafeAlterIntent;
}

export interface SafeAlterTableResult {
  intent: SafeAlterIntent;
  recipe: Array<{
    step: number;
    description: string;
    sql: string;
    locking: string;
    notes?: string;
  }>;
  /** Concatenated SQL ready for `dry_run_sql_file` or manual review. */
  scriptSql: string;
  notes: string[];
}

function quote(name: string): string {
  return name.includes('.')
    ? name.split('.').map(escapeIdent).join('.')
    : escapeIdent(name);
}

export async function safeAlterTable(args: SafeAlterTableArgs): Promise<SafeAlterTableResult> {
  if (!args.intent) throw new Error('intent is required');

  const intent = args.intent;
  const recipe: SafeAlterTableResult['recipe'] = [];
  const notes: string[] = [];

  switch (intent.kind) {
    case 'add_not_null_column_with_default': {
      // 4-step zero-downtime recipe
      recipe.push({
        step: 1,
        description: `Add ${intent.column} as nullable column without default`,
        sql: `ALTER TABLE ${quote(intent.table)} ADD COLUMN ${escapeIdent(intent.column)} ${intent.type};`,
        locking: 'AccessExclusiveLock (brief, metadata-only on PG 11+)',
      });
      recipe.push({
        step: 2,
        description: `Backfill existing rows with the default value (UPDATE in batches in production)`,
        sql: `UPDATE ${quote(intent.table)} SET ${escapeIdent(intent.column)} = ${intent.default_expr} WHERE ${escapeIdent(intent.column)} IS NULL;`,
        locking: 'RowExclusiveLock',
        notes: 'For large tables, batch this UPDATE: e.g. WHERE id BETWEEN 1 AND 10000.',
      });
      recipe.push({
        step: 3,
        description: `Set the column DEFAULT for future inserts`,
        sql: `ALTER TABLE ${quote(intent.table)} ALTER COLUMN ${escapeIdent(intent.column)} SET DEFAULT ${intent.default_expr};`,
        locking: 'AccessExclusiveLock (brief, metadata-only)',
      });
      recipe.push({
        step: 4,
        description: `Add NOT NULL constraint via NOT VALID + VALIDATE for minimal locking`,
        sql:
          `ALTER TABLE ${quote(intent.table)} ADD CONSTRAINT ${escapeIdent(intent.column + '_not_null_chk')} CHECK (${escapeIdent(intent.column)} IS NOT NULL) NOT VALID;\n` +
          `ALTER TABLE ${quote(intent.table)} VALIDATE CONSTRAINT ${escapeIdent(intent.column + '_not_null_chk')};\n` +
          `ALTER TABLE ${quote(intent.table)} ALTER COLUMN ${escapeIdent(intent.column)} SET NOT NULL;\n` +
          `ALTER TABLE ${quote(intent.table)} DROP CONSTRAINT ${escapeIdent(intent.column + '_not_null_chk')};`,
        locking: 'ShareUpdateExclusiveLock for VALIDATE (allows reads + writes)',
      });
      notes.push(
        'Steps 1-4 should be applied as separate transactions (one per migration ' +
        'release ideally) to avoid holding locks for too long. Backfill in batches ' +
        'for tables > 1M rows.'
      );
      break;
    }

    case 'add_not_null': {
      recipe.push({
        step: 1,
        description: `Add CHECK constraint NOT VALID (skips the initial scan)`,
        sql: `ALTER TABLE ${quote(intent.table)} ADD CONSTRAINT ${escapeIdent(intent.column + '_not_null_chk')} CHECK (${escapeIdent(intent.column)} IS NOT NULL) NOT VALID;`,
        locking: 'AccessExclusiveLock (brief, metadata-only)',
      });
      recipe.push({
        step: 2,
        description: `Validate the constraint (scans the table with SHARE UPDATE EXCLUSIVE — allows reads + writes)`,
        sql: `ALTER TABLE ${quote(intent.table)} VALIDATE CONSTRAINT ${escapeIdent(intent.column + '_not_null_chk')};`,
        locking: 'ShareUpdateExclusiveLock',
      });
      recipe.push({
        step: 3,
        description: `On PG 12+: SET NOT NULL is now fast (uses the validated CHECK)`,
        sql: `ALTER TABLE ${quote(intent.table)} ALTER COLUMN ${escapeIdent(intent.column)} SET NOT NULL;`,
        locking: 'AccessExclusiveLock (brief)',
      });
      recipe.push({
        step: 4,
        description: `Drop the redundant CHECK (NOT NULL implies it)`,
        sql: `ALTER TABLE ${quote(intent.table)} DROP CONSTRAINT ${escapeIdent(intent.column + '_not_null_chk')};`,
        locking: 'AccessExclusiveLock (brief)',
      });
      notes.push('Without the CHECK NOT VALID trick, SET NOT NULL holds AccessExclusive while scanning the entire table.');
      break;
    }

    case 'add_foreign_key': {
      const cols = intent.columns.map(escapeIdent).join(', ');
      const refCols = intent.references_columns.map(escapeIdent).join(', ');
      recipe.push({
        step: 1,
        description: `Add FK constraint NOT VALID (skips the validation scan)`,
        sql: `ALTER TABLE ${quote(intent.table)} ADD CONSTRAINT ${escapeIdent(intent.constraint_name)} FOREIGN KEY (${cols}) REFERENCES ${quote(intent.references_table)} (${refCols}) NOT VALID;`,
        locking: 'ShareRowExclusiveLock',
      });
      recipe.push({
        step: 2,
        description: `Validate when ready (scans the table without ACCESS EXCLUSIVE)`,
        sql: `ALTER TABLE ${quote(intent.table)} VALIDATE CONSTRAINT ${escapeIdent(intent.constraint_name)};`,
        locking: 'ShareUpdateExclusiveLock',
      });
      break;
    }

    case 'add_check': {
      recipe.push({
        step: 1,
        description: `Add CHECK constraint NOT VALID`,
        sql: `ALTER TABLE ${quote(intent.table)} ADD CONSTRAINT ${escapeIdent(intent.constraint_name)} CHECK (${intent.check_expr}) NOT VALID;`,
        locking: 'AccessExclusiveLock (brief, metadata-only)',
      });
      recipe.push({
        step: 2,
        description: `Validate when ready`,
        sql: `ALTER TABLE ${quote(intent.table)} VALIDATE CONSTRAINT ${escapeIdent(intent.constraint_name)};`,
        locking: 'ShareUpdateExclusiveLock',
      });
      break;
    }

    case 'create_index': {
      const cols = intent.columns.map(escapeIdent).join(', ');
      const indexType = intent.index_type ?? 'btree';
      const unique = intent.unique ? 'UNIQUE ' : '';
      recipe.push({
        step: 1,
        description: `CREATE ${unique}INDEX CONCURRENTLY (allows reads and writes during build)`,
        sql: `CREATE ${unique}INDEX CONCURRENTLY ${escapeIdent(intent.index_name)} ON ${quote(intent.table)} USING ${indexType} (${cols});`,
        locking: 'ShareUpdateExclusiveLock',
        notes: 'CONCURRENTLY cannot run inside a transaction. Run as a standalone statement (useTransaction=false in executeSqlFile).',
      });
      notes.push(
        'If CREATE INDEX CONCURRENTLY fails (e.g. due to unique violation), ' +
        'the resulting INVALID index must be dropped: DROP INDEX CONCURRENTLY <name>; ' +
        'then retry.'
      );
      break;
    }

    case 'drop_index': {
      const ref = intent.schema
        ? `${escapeIdent(intent.schema)}.${escapeIdent(intent.index_name)}`
        : escapeIdent(intent.index_name);
      recipe.push({
        step: 1,
        description: `DROP INDEX CONCURRENTLY (avoids ACCESS EXCLUSIVE on the table)`,
        sql: `DROP INDEX CONCURRENTLY IF EXISTS ${ref};`,
        locking: 'ShareUpdateExclusiveLock',
        notes: 'CONCURRENTLY cannot run inside a transaction.',
      });
      break;
    }
  }

  const scriptSql = recipe
    .map((r) => `-- Step ${r.step}: ${r.description}\n${r.sql}`)
    .join('\n\n');

  return { intent, recipe, scriptSql, notes };
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
