/**
 * Introspection module barrel export.
 *
 * Public surface used by export-tools.ts (SP-2), transfer-tools.ts
 * (SP-3), and the schema-awareness pack (SP-4: describe_table,
 * find_dependents, schema_diff).
 */

export * from './types.js';
export * from './object-listing.js';
export * from './object-ddl.js';
export * from './dependency-graph.js';
export * from './data-emitter.js';
