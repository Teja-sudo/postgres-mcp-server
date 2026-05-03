/**
 * Introspection types
 *
 * Shared types for the SP-2 introspection module: object descriptors,
 * dependency graph nodes, and the supported feature matrix.
 */

/** Object kinds the introspection module knows how to extract DDL for. */
export type ObjectKind =
  | 'extension'
  | 'schema'
  | 'sequence'
  | 'type'
  | 'table'
  | 'index'
  | 'view'
  | 'matview'
  | 'function'
  | 'procedure'
  | 'trigger';

/** Filter for listing objects: a specific kind or 'all'. */
export type ObjectKindFilter = ObjectKind | 'all';

/** Connection scope: where to look for objects. */
export interface IntrospectionScope {
  /** Schema name (default: 'public'). For extensions and schemas, the
   *  scope's schema is informational only — those objects are
   *  cluster-wide / cross-schema. */
  schema?: string;
}

/** Lightweight descriptor of a database object discovered by listing. */
export interface ObjectDescriptor {
  /** PostgreSQL OID — primary key in pg_class / pg_proc / etc. */
  oid: number;
  /** Object kind. */
  kind: ObjectKind;
  /** Object's namespace/schema (or empty for cluster-wide objects). */
  schema: string;
  /** Object name (unqualified). */
  name: string;
  /** Owner role name. */
  owner: string;
  /** Optional human-friendly description (from pg_description). */
  comment?: string;
}

/** Result of extracting DDL for one object. */
export interface ExtractedDDL {
  /** Object kind we extracted. */
  kind: ObjectKind;
  /** Schema-qualified name where applicable. */
  qualifiedName: string;
  /** The DDL statement(s) to recreate the object. May contain multiple
   *  semicolon-separated statements (e.g. CREATE TABLE + COMMENT ON). */
  sql: string;
  /** Warnings about features that could not be exported (e.g. RLS
   *  policies, partition relationships, exclusion constraints). */
  warnings: string[];
  /** OIDs of objects this DDL depends on. Used to build the dependency
   *  graph for topological ordering. */
  dependencies: number[];
}

/** A node in the dependency graph. */
export interface DependencyNode {
  oid: number;
  kind: ObjectKind;
  schema: string;
  name: string;
  /** OIDs this node depends on (must be ordered before this in output). */
  dependsOn: Set<number>;
}

/** Result of topological ordering. */
export interface OrderedObjects {
  /** Objects in dependency-safe creation order. */
  ordered: DependencyNode[];
  /** Cycles detected during ordering (object groups that depend on each
   *  other). FKs are typically split out and applied last to break
   *  cycles between tables. */
  cycles: number[][];
}

/** What features are supported by the SP-2 introspection in v1. */
export const SUPPORTED_FEATURES: Record<ObjectKind, true | string> = {
  extension: true,
  schema: true,
  sequence: true,
  type: true, // limited: enums + composite, not domains/ranges
  table: true,
  index: true,
  view: true,
  matview: true,
  function: true,
  procedure: true,
  trigger: true,
};

/** Features intentionally NOT supported in SP-2 v1.
 *  Each entry is an enum-style identifier we surface in warnings so AI
 *  agents can recognize the gap. */
export const UNSUPPORTED_FEATURES = {
  RLS_POLICY: 'Row-level security policies (CREATE POLICY)',
  EXCLUSION_CONSTRAINT: 'EXCLUDE constraints',
  PARTITION_HIERARCHY: 'Table partitioning (PARTITION BY / ATTACH PARTITION)',
  GENERATED_COLUMN: 'GENERATED columns (computed values)',
  IDENTITY_COLUMN: 'IDENTITY columns (GENERATED AS IDENTITY) — exported as SERIAL approximation',
  CUSTOM_COLLATION: 'CREATE COLLATION definitions',
  TEXT_SEARCH_CONFIG: 'Custom text search configurations',
  OPERATOR_CLASS: 'Custom operator classes / families',
  DOMAIN_TYPE: 'CREATE DOMAIN definitions',
  RANGE_TYPE: 'CREATE TYPE ... AS RANGE',
  AGGREGATE: 'CREATE AGGREGATE definitions',
  RULE: 'CREATE RULE definitions',
  LARGE_OBJECT: 'Large object data (lo_*)',
  FOREIGN_TABLE: 'Foreign tables / FDW server / user mappings',
} as const;

export type UnsupportedFeature = keyof typeof UNSUPPORTED_FEATURES;
