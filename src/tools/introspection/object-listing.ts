/**
 * Object listing
 *
 * Discovers objects in a given scope (schema). Returns lightweight
 * descriptors used by extractObjectDDL and buildDependencyGraph.
 */

import { PoolClient } from 'pg';
import { ObjectDescriptor, ObjectKind, ObjectKindFilter, IntrospectionScope } from './types.js';

/**
 * List all known objects in the given scope.
 *
 * For schema-scoped kinds (table, view, matview, sequence, function,
 * procedure, type, index, trigger), only objects in `scope.schema` are
 * returned.
 *
 * For cluster-wide kinds (extension, schema), the scope is informational —
 * all extensions / all schemas are returned.
 */
export async function listObjectsInScope(
  client: PoolClient,
  scope: IntrospectionScope,
  kindFilter: ObjectKindFilter = 'all'
): Promise<ObjectDescriptor[]> {
  const schema = scope.schema ?? 'public';
  const want = (k: ObjectKind): boolean =>
    kindFilter === 'all' || kindFilter === k;

  const out: ObjectDescriptor[] = [];

  if (want('extension')) {
    const r = await client.query(
      `SELECT e.oid::int AS oid,
              e.extname AS name,
              n.nspname AS schema,
              pg_catalog.pg_get_userbyid(e.extowner) AS owner,
              pg_catalog.obj_description(e.oid, 'pg_extension') AS comment
       FROM pg_extension e
       JOIN pg_namespace n ON n.oid = e.extnamespace
       WHERE e.extname NOT IN ('plpgsql')
       ORDER BY e.extname`
    );
    out.push(...r.rows.map((row) => ({ ...row, kind: 'extension' as const })));
  }

  if (want('schema')) {
    const r = await client.query(
      `SELECT n.oid::int AS oid,
              n.nspname AS name,
              '' AS schema,
              pg_catalog.pg_get_userbyid(n.nspowner) AS owner,
              pg_catalog.obj_description(n.oid, 'pg_namespace') AS comment
       FROM pg_namespace n
       WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
         AND n.nspname NOT LIKE 'pg_temp_%'
         AND n.nspname NOT LIKE 'pg_toast_temp_%'
       ORDER BY n.nspname`
    );
    out.push(...r.rows.map((row) => ({ ...row, kind: 'schema' as const })));
  }

  if (want('sequence')) {
    // Audit-iteration-1 SP-2 P0 fix: exclude sequences that are
    // owned by a SERIAL/identity column (pg_depend.deptype='a').
    // The owning table's CREATE TABLE already emits `SERIAL`/
    // `BIGSERIAL` which auto-creates the sequence, so listing those
    // here would result in duplicate `CREATE SEQUENCE` statements
    // on replay (we'd end up with both users_id_seq and
    // users_id_seq1, half orphaned).
    const r = await client.query(
      `SELECT c.oid::int AS oid,
              c.relname AS name,
              n.nspname AS schema,
              pg_catalog.pg_get_userbyid(c.relowner) AS owner,
              pg_catalog.obj_description(c.oid, 'pg_class') AS comment
       FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relkind = 'S' AND n.nspname = $1
         AND NOT EXISTS (
           SELECT 1 FROM pg_depend d
           WHERE d.classid = 'pg_class'::regclass
             AND d.objid = c.oid
             AND d.refclassid = 'pg_class'::regclass
             AND d.deptype = 'a'
         )
       ORDER BY c.relname`,
      [schema]
    );
    out.push(...r.rows.map((row) => ({ ...row, kind: 'sequence' as const })));
  }

  if (want('type')) {
    // Composite (c) and enum (e) types only, excluding auto-generated row
    // types for tables (typtype='c' AND relkind='c' must check via join).
    // Audit-iteration-1 fix: exclude extension-owned types.
    const r = await client.query(
      `SELECT t.oid::int AS oid,
              t.typname AS name,
              n.nspname AS schema,
              pg_catalog.pg_get_userbyid(t.typowner) AS owner,
              pg_catalog.obj_description(t.oid, 'pg_type') AS comment
       FROM pg_type t
       JOIN pg_namespace n ON n.oid = t.typnamespace
       LEFT JOIN pg_class c ON c.oid = t.typrelid
       WHERE n.nspname = $1
         AND (
           (t.typtype = 'e')
           OR (t.typtype = 'c' AND (c.oid IS NULL OR c.relkind = 'c'))
         )
         AND NOT EXISTS (
           SELECT 1 FROM pg_depend d
           WHERE d.classid = 'pg_type'::regclass
             AND d.objid = t.oid
             AND d.deptype = 'e'
         )
       ORDER BY t.typname`,
      [schema]
    );
    out.push(...r.rows.map((row) => ({ ...row, kind: 'type' as const })));
  }

  if (want('table')) {
    const r = await client.query(
      `SELECT c.oid::int AS oid,
              c.relname AS name,
              n.nspname AS schema,
              pg_catalog.pg_get_userbyid(c.relowner) AS owner,
              pg_catalog.obj_description(c.oid, 'pg_class') AS comment
       FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relkind = 'r' AND n.nspname = $1
       ORDER BY c.relname`,
      [schema]
    );
    out.push(...r.rows.map((row) => ({ ...row, kind: 'table' as const })));
  }

  if (want('view')) {
    const r = await client.query(
      `SELECT c.oid::int AS oid,
              c.relname AS name,
              n.nspname AS schema,
              pg_catalog.pg_get_userbyid(c.relowner) AS owner,
              pg_catalog.obj_description(c.oid, 'pg_class') AS comment
       FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relkind = 'v' AND n.nspname = $1
       ORDER BY c.relname`,
      [schema]
    );
    out.push(...r.rows.map((row) => ({ ...row, kind: 'view' as const })));
  }

  if (want('matview')) {
    const r = await client.query(
      `SELECT c.oid::int AS oid,
              c.relname AS name,
              n.nspname AS schema,
              pg_catalog.pg_get_userbyid(c.relowner) AS owner,
              pg_catalog.obj_description(c.oid, 'pg_class') AS comment
       FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relkind = 'm' AND n.nspname = $1
       ORDER BY c.relname`,
      [schema]
    );
    out.push(...r.rows.map((row) => ({ ...row, kind: 'matview' as const })));
  }

  if (want('function') || want('procedure')) {
    // Audit-iteration-1 SP-2/SP-3 P0 fix: exclude extension-owned
    // functions. With pgcrypto installed in `public`, the unfiltered
    // listing previously returned all 38 of its functions (armor,
    // digest, gen_random_uuid, etc.) as user-owned, then the
    // export/transfer would emit `CREATE OR REPLACE FUNCTION ...`
    // statements that collide on replay (or fail on overloads).
    const r = await client.query(
      `SELECT p.oid::int AS oid,
              p.proname AS name,
              n.nspname AS schema,
              pg_catalog.pg_get_userbyid(p.proowner) AS owner,
              pg_catalog.obj_description(p.oid, 'pg_proc') AS comment,
              p.prokind AS prokind
       FROM pg_proc p
       JOIN pg_namespace n ON n.oid = p.pronamespace
       WHERE n.nspname = $1
         AND p.prokind IN ('f', 'p')
         AND NOT EXISTS (
           SELECT 1 FROM pg_depend d
           WHERE d.classid = 'pg_proc'::regclass
             AND d.objid = p.oid
             AND d.deptype = 'e'
         )
       ORDER BY p.proname`,
      [schema]
    );
    for (const row of r.rows) {
      const kind: ObjectKind = row.prokind === 'p' ? 'procedure' : 'function';
      if (!want(kind)) continue;
      // Strip the prokind discriminator (only used for kind selection)
      const rest = { ...row };
      delete rest.prokind;
      out.push({ ...rest, kind });
    }
  }

  if (want('index')) {
    // Non-PK / non-unique-constraint-backing indexes only. PK indexes
    // are emitted as part of CREATE TABLE; UNIQUE constraint indexes
    // come with the constraint definition.
    const r = await client.query(
      `SELECT c.oid::int AS oid,
              c.relname AS name,
              n.nspname AS schema,
              pg_catalog.pg_get_userbyid(c.relowner) AS owner,
              pg_catalog.obj_description(c.oid, 'pg_class') AS comment
       FROM pg_class c
       JOIN pg_namespace n ON n.oid = c.relnamespace
       JOIN pg_index i ON i.indexrelid = c.oid
       WHERE c.relkind = 'i' AND n.nspname = $1
         AND NOT i.indisprimary
         AND NOT EXISTS (
           SELECT 1 FROM pg_constraint con
           WHERE con.conindid = c.oid AND con.contype IN ('u', 'p', 'x')
         )
       ORDER BY c.relname`,
      [schema]
    );
    out.push(...r.rows.map((row) => ({ ...row, kind: 'index' as const })));
  }

  if (want('trigger')) {
    const r = await client.query(
      `SELECT t.oid::int AS oid,
              t.tgname AS name,
              n.nspname AS schema,
              pg_catalog.pg_get_userbyid(c.relowner) AS owner,
              pg_catalog.obj_description(t.oid, 'pg_trigger') AS comment
       FROM pg_trigger t
       JOIN pg_class c ON c.oid = t.tgrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE NOT t.tgisinternal AND n.nspname = $1
       ORDER BY t.tgname`,
      [schema]
    );
    out.push(...r.rows.map((row) => ({ ...row, kind: 'trigger' as const })));
  }

  return out;
}
