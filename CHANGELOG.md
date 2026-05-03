# Changelog

All notable changes to `@tejasanik/postgres-mcp-server` are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/), versioning
follows [Semantic Versioning](https://semver.org/).

## [2.7.0] — 2026-05-03

### Added (SP-5 — migration safety pack)

- **`lock_check` MCP tool** — static analysis of a SQL DDL statement to
  determine the PG lock level it requires, whether it forces a full
  table rewrite, and (with table-size lookup) an estimated duration.
  Returns warnings for ACCESS EXCLUSIVE on busy tables and concrete
  recommendations: use CREATE INDEX CONCURRENTLY, NOT VALID + VALIDATE
  CONSTRAINT, etc. Knows lock semantics for ALTER TABLE variants
  (ADD/DROP COLUMN, ADD/DROP NOT NULL, ALTER TYPE, ADD CONSTRAINT,
  RENAME, SET STORAGE/STATISTICS/TABLESPACE), CREATE/DROP INDEX
  (CONCURRENTLY vs not), VACUUM (FULL vs not), CLUSTER, REFRESH
  MATERIALIZED VIEW (CONCURRENTLY vs not).
- **`detect_migration_state` MCP tool** — probes for 10 common
  migration tracker tables (Liquibase databasechangelog, Flyway
  flyway_schema_history, Alembic alembic_version, Prisma
  _prisma_migrations, Knex knex_migrations, Sequelize SequelizeMeta,
  Django django_migrations, Rails schema_migrations, Goose
  goose_db_version, TypeORM migrations). Reports schema, table,
  applied count, and latest version per detected tool.
- **`safe_alter_table` MCP tool** — high-level intent → multi-step
  zero-downtime DDL recipe. Six intents covered:
  - `add_not_null_column_with_default`: 4-step recipe (add nullable,
    backfill, set default, add NOT NULL via NOT VALID + VALIDATE).
  - `add_not_null`: 4-step (CHECK NOT VALID → VALIDATE → SET NOT NULL
    → DROP redundant CHECK).
  - `add_foreign_key` / `add_check`: NOT VALID + VALIDATE recipe to
    avoid the initial scan under heavy lock.
  - `create_index` / `drop_index`: CONCURRENTLY recipe with note
    that CONCURRENTLY cannot run inside a transaction.
  Each step has its own SQL, expected lock level, notes. The combined
  `scriptSql` is suitable for `dry_run_sql_file` review followed by
  `executeSqlFile(useTransaction=false)` for production rollout.

## [2.6.0] — 2026-05-03

### Added (SP-4 — schema awareness pack)

- **`describe_table` MCP tool** — single rich call replacing ~5 separate
  ones (get_object_details + sample SELECT + COUNT + pg_stats lookup).
  Returns columns (with null %/distinct ratio from pg_stats), primary
  key, foreign keys going OUT (this table → others) AND coming IN
  (others → this table), indexes (with definitions), table size,
  row-count estimate, sample rows, comment.
- **`find_dependents` MCP tool** — recursive walk of `pg_depend` to
  find every object that depends on a target. Classifies dependents
  (tables, views, matviews, foreign-keys, indexes, functions, types,
  rules) with depth and dependency reason ('normal', 'auto',
  'internal', 'extension', 'pin'). Use BEFORE running DROP CASCADE.
  Configurable max_depth (default 5).
- **`schema_diff` MCP tool** — DDL delta between two
  `{ server, database, schema }` endpoints. Returns:
  - `toCreate`: in source but not in target
  - `toDrop`: in target but not in source
  - `toModify`: in both, but DDL drifted (CREATE OR REPLACE for
    views/functions/procedures; DROP+CREATE for everything else)
  - `migrationSql`: single script that, when applied to the TARGET,
    converges its schema with the SOURCE.
  Source is the source-of-truth. All comparisons happen at the DDL-string
  level after comment stripping + whitespace normalization.

## [2.5.0] — 2026-05-03

### Added (SP-3)

- **`transfer_objects` MCP tool** — releases / moves schema and/or data
  between two configured servers (same server, different DB, or fully
  remote). Builds on the SP-2 introspection module:
  - `from` and `to` endpoints, each `{ server, database, schema }`. Both
    must be configured servers (`PG_NAME_*`); ad-hoc connection strings
    are not accepted (security).
  - `objects: '*' | ObjectRef[]` — transfer everything in source schema
    or a specific list.
  - `include: 'ddl' | 'data' | 'both'`.
  - `if_exists: 'skip' | 'replace' | 'error'` — handles target-side
    conflicts. `replace` issues a `DROP … CASCADE` before recreating.
  - `dry_run: true` + `output_file` — emits the would-be SQL without
    touching the target. Useful for review-before-apply.
  - Refuses if target's effective access mode is `readonly`.
  - Apply happens inside a target-side transaction (atomic-or-rollback
    on the destination).
  - FK constraints between tables emitted as `ALTER TABLE` statements
    appended after tables, breaking inter-table dependency cycles.
  - Data transferred via parameterized INSERT batches (100 rows per
    statement). Streaming COPY format reserved for v2.

## [2.4.0] — 2026-05-03

### Added (SP-2)

- **`export_to_sql_file` MCP tool** — exports schema and/or data from
  the connected database to a `.sql` file. Four content variants:
  - `{ kind: 'objects', objects }` — DDL of a list of objects, ordered
    topologically by dependency.
  - `{ kind: 'data', tables, where, orderBy, limit }` — INSERT statements
    for the listed tables, with optional WHERE / ORDER BY / LIMIT.
  - `{ kind: 'schema_dump', schema, include_data }` — full schema (and
    optionally data) for a schema.
  - `{ kind: 'query_result', sql, target_table }` — SELECT result emitted
    as INSERTs into a named target table.
  - Modes: `append` (default, preserves existing content with separator
    banner) or `overwrite`. Foot-gun guard refuses overwrite of
    files modified <60s ago unless `confirm_overwrite: true`.
  - Header banner records timestamp + source server alias (host/port
    intentionally hidden, consistent with `list_servers` policy).
  - Refuses writes to sensitive paths (`.env*`, `node_modules/`,
    `.git/`).
- **`introspection/` shared module** — building block for SP-3
  (transfer_objects), SP-4 (describe_table, find_dependents,
  schema_diff). Public surface:
  - `listObjectsInScope(client, scope, kind)` — discover objects
  - `extractObjectDDL(client, descriptor)` — DDL string + dependencies +
    warnings (per object)
  - `buildDependencyGraph(input)` / `topologicallyOrder(graph)` —
    creation-order resolution
  - `emitTableRowsAsInsert(...)` / `formatSqlLiteral(value)` — data emit
- **Supported object kinds** (DDL extraction): extension, schema,
  sequence, type (enum + composite), table, index, view, materialized
  view, function, procedure, trigger.
- **Documented unsupported features** (SP-2 v1) — RLS policies,
  exclusion constraints, partition hierarchies, generated/identity
  columns (rendered approximately), domains, range types, custom
  collations / text-search configs / operator classes / aggregates,
  rules, large objects, foreign tables. Each surface a warning rather
  than silently dropping.
- **Foreign-key handling** — FKs are emitted as `ALTER TABLE ... ADD
  CONSTRAINT` statements appended after all tables, so cyclic FKs
  between tables don't break creation order.

## [2.3.1] — 2026-05-03

### Fixed (SP-1)

- **`dry_run_sql_file`, `mutationDryRun`, `executeSqlFile(useTransaction=true)`,
  and `execute_sql` with `transactionId` no longer silently persist changes
  when the input SQL contains transaction-control statements (`COMMIT`,
  `ROLLBACK`, `BEGIN`, `END`, `ABORT`, `SAVEPOINT`, `RELEASE`, `START
  TRANSACTION`, `ROLLBACK TO`).** Previously, an embedded `COMMIT` would
  close our outer transaction, persist everything before it, and leave the
  final `ROLLBACK` to run against "no transaction in progress". The result
  was misleading: tools claimed `rolledBack: true` while changes were visible
  on the live database.

  A two-layer defense now prevents this:

  1. **Static analysis** (`dry-run-utils.ts`): transaction-control
     statements are detected at parse time and skipped, surfacing as
     `nonRollbackableWarnings.operation: "TRANSACTION_CONTROL"` with
     `mustSkip: true` and the line number of the offending statement.
  2. **Runtime sentinel** (`transaction-guard.ts`): a uniquely-named
     `SAVEPOINT` is installed immediately after our outer `BEGIN` and
     verified after every suspect statement (DO blocks, EXECUTE strings,
     CALL into procedures) plus once before the final `ROLLBACK`. If the
     savepoint is gone, the result includes `dryRunCompromised: true`
     with `compromisedAt` indicating where the outer transaction was
     closed.

### Changed

- `executeSqlFile` now refuses the combination
  `useTransaction=true, stopOnError=false` at validation time. PostgreSQL
  aborts the entire transaction on the first error, ignoring all subsequent
  statements, so this combination never produced useful output. Use
  `useTransaction=false` for per-statement isolation, or `stopOnError=true`
  to roll back atomically on first failure.

- `commitTransaction` and `rollbackTransaction` now return
  `status: 'compromised'` (not `'committed'` / `'rolled_back'`) when the
  transaction was hijacked by an embedded transaction-control statement
  earlier in the session. The accompanying message tells callers to
  inspect the live DB for partial persistence.

### Added

- `testcontainers`-backed integration suite. Opt-in locally via
  `POSTGRES_INTEGRATION_TESTS=1`; always on in CI (`ubuntu-latest`).
- `transaction-guard.ts` exporting `TransactionGuard` and
  `stripCommentsAndStrings` helpers (re-exported from
  `tools/sql/utils/index.ts`).
- New result-shape fields (additive, optional):
  - `SqlFileDryRunResult.dryRunCompromised`, `compromisedAt`
  - `MutationDryRunResult.dryRunCompromised`, `compromisedAt`
  - `NonRollbackableWarning.operation: 'TRANSACTION_CONTROL'`
  - `TransactionInfo.compromised`, `compromisedReason`
  - `TransactionResult.status: 'compromised'`
- `DatabaseManager` gains `verifyTransactionIntact`,
  `markTransactionCompromised`, `isTransactionCompromised` methods.

### Test infrastructure

- Replaced hardcoded `/tmp/postgres-mcp-*-` paths in `sql-tools.test.ts`
  with `path.join(os.tmpdir(), ...)` for cross-platform compatibility.
  All 56 previously-Windows-failing tests now pass on Windows.

### Security note

- A `moderate` `npm audit` finding flags `uuid <14.0.0` (a buffer bounds
  bug in `uuid.v3/v5/v6` when `buf` is provided), reaching us only via
  `dockerode → testcontainers` (devDep). The runtime never imports
  `uuid <14`; this is dev-only and the affected APIs are not used.

---

## [2.3.0] — 2026-04-29

- Initial release of the post-package-upgrade baseline. (See git history
  for prior changes; this CHANGELOG begins with v2.3.1.)
