/**
 * DDL extraction
 *
 * Builds a CREATE-style DDL statement for each supported object kind
 * by composing PG's built-in pg_get_*def() functions and information
 * schema queries. Pure SQL — no shelling out to pg_dump.
 *
 * Each extractor returns the DDL plus a list of OIDs the object
 * depends on (for the dependency graph) and warnings for any
 * features that could not be exported (RLS, exclusion constraints,
 * partition relationships, etc).
 */

import { PoolClient } from 'pg';
import {
  ExtractedDDL,
  ObjectDescriptor,
  UNSUPPORTED_FEATURES,
} from './types.js';

/** Quote a PG identifier safely. */
function qident(name: string): string {
  return '"' + name.replace(/"/g, '""') + '"';
}

/** Build "schema.name" with both parts quoted. */
function qualify(schema: string, name: string): string {
  return `${qident(schema)}.${qident(name)}`;
}

/**
 * Top-level dispatch: extract DDL for any supported object kind.
 */
export async function extractObjectDDL(
  client: PoolClient,
  obj: ObjectDescriptor
): Promise<ExtractedDDL> {
  switch (obj.kind) {
    case 'extension':
      return extractExtensionDDL(obj);
    case 'schema':
      return extractSchemaDDL(obj);
    case 'sequence':
      return extractSequenceDDL(client, obj);
    case 'type':
      return extractTypeDDL(client, obj);
    case 'table':
      return extractTableDDL(client, obj);
    case 'index':
      return extractIndexDDL(client, obj);
    case 'view':
      return extractViewDDL(client, obj);
    case 'matview':
      return extractMatViewDDL(client, obj);
    case 'function':
    case 'procedure':
      return extractFunctionDDL(client, obj);
    case 'trigger':
      return extractTriggerDDL(client, obj);
    default:
      throw new Error(`Unsupported object kind: ${(obj as ObjectDescriptor).kind}`);
  }
}

function extractExtensionDDL(obj: ObjectDescriptor): ExtractedDDL {
  const sql = `CREATE EXTENSION IF NOT EXISTS ${qident(obj.name)};`;
  return {
    kind: 'extension',
    qualifiedName: obj.name,
    sql,
    warnings: [],
    dependencies: [],
  };
}

function extractSchemaDDL(obj: ObjectDescriptor): ExtractedDDL {
  const sql = `CREATE SCHEMA IF NOT EXISTS ${qident(obj.name)};`;
  return {
    kind: 'schema',
    qualifiedName: obj.name,
    sql,
    warnings: [],
    dependencies: [],
  };
}

async function extractSequenceDDL(
  client: PoolClient,
  obj: ObjectDescriptor
): Promise<ExtractedDDL> {
  const r = await client.query(
    `SELECT seqstart, seqincrement, seqmin, seqmax, seqcache, seqcycle,
            (SELECT pg_catalog.format_type(s.seqtypid, NULL)) AS data_type
     FROM pg_sequence s WHERE seqrelid = $1`,
    [obj.oid]
  );
  if (r.rows.length === 0) {
    throw new Error(`Sequence ${obj.name} not found in pg_sequence`);
  }
  const s = r.rows[0];
  const parts = [
    `CREATE SEQUENCE IF NOT EXISTS ${qualify(obj.schema, obj.name)}`,
    `  AS ${s.data_type}`,
    `  START WITH ${s.seqstart}`,
    `  INCREMENT BY ${s.seqincrement}`,
    `  MINVALUE ${s.seqmin}`,
    `  MAXVALUE ${s.seqmax}`,
    `  CACHE ${s.seqcache}`,
    `  ${s.seqcycle ? 'CYCLE' : 'NO CYCLE'};`,
  ];
  return {
    kind: 'sequence',
    qualifiedName: qualify(obj.schema, obj.name),
    sql: parts.join('\n'),
    warnings: [],
    dependencies: [],
  };
}

async function extractTypeDDL(
  client: PoolClient,
  obj: ObjectDescriptor
): Promise<ExtractedDDL> {
  // Determine kind: enum vs composite
  const r = await client.query(
    `SELECT t.typtype FROM pg_type t WHERE t.oid = $1`,
    [obj.oid]
  );
  if (r.rows.length === 0) {
    throw new Error(`Type ${obj.name} not found`);
  }
  const typtype = r.rows[0].typtype;

  if (typtype === 'e') {
    const labels = await client.query(
      `SELECT enumlabel FROM pg_enum
       WHERE enumtypid = $1 ORDER BY enumsortorder`,
      [obj.oid]
    );
    const labelList = labels.rows
      .map((row) => `'${row.enumlabel.replace(/'/g, "''")}'`)
      .join(', ');
    return {
      kind: 'type',
      qualifiedName: qualify(obj.schema, obj.name),
      sql: `CREATE TYPE ${qualify(obj.schema, obj.name)} AS ENUM (${labelList});`,
      warnings: [],
      dependencies: [],
    };
  }
  if (typtype === 'c') {
    // Composite type: introspect attributes
    const attrs = await client.query(
      `SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type
       FROM pg_attribute a
       JOIN pg_type t ON t.typrelid = a.attrelid
       WHERE t.oid = $1 AND a.attnum > 0 AND NOT a.attisdropped
       ORDER BY a.attnum`,
      [obj.oid]
    );
    const cols = attrs.rows
      .map((row) => `  ${qident(row.attname)} ${row.data_type}`)
      .join(',\n');
    return {
      kind: 'type',
      qualifiedName: qualify(obj.schema, obj.name),
      sql: `CREATE TYPE ${qualify(obj.schema, obj.name)} AS (\n${cols}\n);`,
      warnings: [],
      dependencies: [],
    };
  }
  return {
    kind: 'type',
    qualifiedName: qualify(obj.schema, obj.name),
    sql: '',
    warnings: [`Type ${obj.name} has unsupported typtype '${typtype}' (domain/range/pseudo). Skipped.`],
    dependencies: [],
  };
}

async function extractTableDDL(
  client: PoolClient,
  obj: ObjectDescriptor
): Promise<ExtractedDDL> {
  const warnings: string[] = [];
  const dependencies: number[] = [];

  // Columns
  const cols = await client.query(
    `SELECT a.attname,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
            a.attnotnull,
            pg_get_expr(d.adbin, d.adrelid) AS default_expr,
            a.attidentity,
            a.attgenerated
     FROM pg_attribute a
     LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
     WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
     ORDER BY a.attnum`,
    [obj.oid]
  );

  if (cols.rows.length === 0) {
    throw new Error(`Table ${obj.name} has no columns or does not exist`);
  }

  const colDefs = cols.rows.map((row) => {
    let def = `  ${qident(row.attname)} ${row.data_type}`;
    if (row.attidentity) {
      // GENERATED ... AS IDENTITY
      const kind = row.attidentity === 'a' ? 'ALWAYS' : 'BY DEFAULT';
      def += ` GENERATED ${kind} AS IDENTITY`;
      warnings.push(`Column ${row.attname}: ${UNSUPPORTED_FEATURES.IDENTITY_COLUMN}`);
    } else if (row.attgenerated === 's') {
      def += ` GENERATED ALWAYS AS (${row.default_expr}) STORED`;
      warnings.push(`Column ${row.attname}: ${UNSUPPORTED_FEATURES.GENERATED_COLUMN}`);
    } else if (row.default_expr) {
      def += ` DEFAULT ${row.default_expr}`;
    }
    if (row.attnotnull) def += ' NOT NULL';
    return def;
  });

  // Primary key + unique + check constraints (FKs deferred)
  const constraints = await client.query(
    `SELECT con.conname,
            con.contype,
            pg_get_constraintdef(con.oid, true) AS def,
            con.confrelid::int AS confrelid
     FROM pg_constraint con
     WHERE con.conrelid = $1
     ORDER BY con.contype DESC, con.conname`,
    [obj.oid]
  );

  const inlineConstraints: string[] = [];
  const fkConstraints: string[] = [];
  for (const row of constraints.rows) {
    if (row.contype === 'x') {
      warnings.push(`Constraint ${row.conname}: ${UNSUPPORTED_FEATURES.EXCLUSION_CONSTRAINT}`);
      continue;
    }
    if (row.contype === 'f') {
      // Foreign keys handled separately as a post-table ALTER so cycles
      // between tables don't break ordering.
      fkConstraints.push(
        `ALTER TABLE ${qualify(obj.schema, obj.name)} ` +
        `ADD CONSTRAINT ${qident(row.conname)} ${row.def};`
      );
      if (row.confrelid && row.confrelid !== 0) {
        dependencies.push(row.confrelid);
      }
      continue;
    }
    // Primary key, unique, check, foreign-key all have a def
    inlineConstraints.push(`  CONSTRAINT ${qident(row.conname)} ${row.def}`);
  }

  // Check for partitioning - we don't support exporting partition info
  const part = await client.query(
    `SELECT pg_get_partkeydef($1) AS partkey`,
    [obj.oid]
  );
  if (part.rows[0].partkey) {
    warnings.push(`Table ${obj.name}: ${UNSUPPORTED_FEATURES.PARTITION_HIERARCHY}`);
  }

  // Check for RLS
  const rls = await client.query(
    `SELECT relrowsecurity FROM pg_class WHERE oid = $1`,
    [obj.oid]
  );
  if (rls.rows[0]?.relrowsecurity) {
    warnings.push(`Table ${obj.name}: ${UNSUPPORTED_FEATURES.RLS_POLICY}`);
  }

  const allDefs = [...colDefs, ...inlineConstraints].join(',\n');
  let sql =
    `CREATE TABLE IF NOT EXISTS ${qualify(obj.schema, obj.name)} (\n${allDefs}\n);`;

  if (obj.comment) {
    sql += `\nCOMMENT ON TABLE ${qualify(obj.schema, obj.name)} IS '${obj.comment.replace(/'/g, "''")}';`;
  }

  // Append FKs to the SQL output (they'll be ordered AFTER all tables
  // by the topological sort).
  if (fkConstraints.length > 0) {
    sql += '\n-- Foreign keys (applied after all tables exist)\n' + fkConstraints.join('\n');
  }

  return {
    kind: 'table',
    qualifiedName: qualify(obj.schema, obj.name),
    sql,
    warnings,
    dependencies,
  };
}

async function extractIndexDDL(
  client: PoolClient,
  obj: ObjectDescriptor
): Promise<ExtractedDDL> {
  const r = await client.query(
    `SELECT pg_get_indexdef($1) AS def, indrelid::int AS table_oid
     FROM pg_index WHERE indexrelid = $1`,
    [obj.oid]
  );
  if (r.rows.length === 0) {
    throw new Error(`Index ${obj.name} not found`);
  }
  return {
    kind: 'index',
    qualifiedName: qualify(obj.schema, obj.name),
    sql: r.rows[0].def + ';',
    warnings: [],
    dependencies: [r.rows[0].table_oid],
  };
}

async function extractViewDDL(
  client: PoolClient,
  obj: ObjectDescriptor
): Promise<ExtractedDDL> {
  const r = await client.query(`SELECT pg_get_viewdef($1, true) AS def`, [obj.oid]);
  if (r.rows.length === 0) {
    throw new Error(`View ${obj.name} not found`);
  }
  let sql = `CREATE OR REPLACE VIEW ${qualify(obj.schema, obj.name)} AS\n${r.rows[0].def}`;
  if (!sql.trim().endsWith(';')) sql += ';';
  return {
    kind: 'view',
    qualifiedName: qualify(obj.schema, obj.name),
    sql,
    warnings: [],
    dependencies: [],
  };
}

async function extractMatViewDDL(
  client: PoolClient,
  obj: ObjectDescriptor
): Promise<ExtractedDDL> {
  const r = await client.query(`SELECT pg_get_viewdef($1, true) AS def`, [obj.oid]);
  if (r.rows.length === 0) {
    throw new Error(`Materialized view ${obj.name} not found`);
  }
  let sql = `CREATE MATERIALIZED VIEW IF NOT EXISTS ${qualify(obj.schema, obj.name)} AS\n${r.rows[0].def}`;
  if (!sql.trim().endsWith(';')) sql += ';';
  return {
    kind: 'matview',
    qualifiedName: qualify(obj.schema, obj.name),
    sql,
    warnings: [],
    dependencies: [],
  };
}

async function extractFunctionDDL(
  client: PoolClient,
  obj: ObjectDescriptor
): Promise<ExtractedDDL> {
  const r = await client.query(`SELECT pg_get_functiondef($1) AS def`, [obj.oid]);
  if (r.rows.length === 0) {
    throw new Error(`Function/procedure ${obj.name} not found`);
  }
  let sql = r.rows[0].def;
  if (!sql.trim().endsWith(';')) sql += ';';
  return {
    kind: obj.kind,
    qualifiedName: qualify(obj.schema, obj.name),
    sql,
    warnings: [],
    dependencies: [],
  };
}

async function extractTriggerDDL(
  client: PoolClient,
  obj: ObjectDescriptor
): Promise<ExtractedDDL> {
  const r = await client.query(
    `SELECT pg_get_triggerdef($1) AS def, tgrelid::int AS table_oid
     FROM pg_trigger WHERE oid = $1`,
    [obj.oid]
  );
  if (r.rows.length === 0) {
    throw new Error(`Trigger ${obj.name} not found`);
  }
  return {
    kind: 'trigger',
    qualifiedName: obj.name,
    sql: r.rows[0].def + ';',
    warnings: [],
    dependencies: [r.rows[0].table_oid],
  };
}
