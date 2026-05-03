# Changelog

All notable changes to `@tejasanik/postgres-mcp-server` are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/), versioning
follows [Semantic Versioning](https://semver.org/).

## [3.0.1] — 2026-05-04

Audit-loop iteration 3: all 36 MCP tools reviewed against a real PG 17
cluster with complex schemas (FKs, partial indexes, materialized views,
cycles). Fixes are bug-only; no new tools and no breaking API changes.

### Fixed

- **`switch_server_db` validates inputs before tearing down the live
  pool.** Previously, a malformed `dbName` or `schema` killed the
  current connection on its way to throwing. Validation now runs first;
  the pool is only closed once inputs are accepted.
- **`detect_migration_state` recognizes quoted-identifier tracker
  tables.** Replaced `to_regclass($1)` with
  `to_regclass(format('%I.%I', $1, $2))`, so Sequelize's mixed-case
  `"SequelizeMeta"` (and any other tool that uses quoted identifiers)
  is detected.
- **`list_databases` / `list_servers` `maxResults` clamped non-negative**
  via `Math.max(requested, 0)` (was `Math.min(maxResults || 50, 200)`,
  which let `-1` slip through as the default).
- **`find_dependents.truncatedAtDepth` actually trips at the boundary.**
  Off-by-one in the depth check meant the flag never reflected real
  truncation. The walker now enqueues up to and including `max_depth`
  and sets the flag exactly when the next layer is suppressed.
- **EXPLAIN-plan column-reference parser handles qualified names.**
  PG 17's EXPLAIN output emits `(orders.status = ...)` for joined
  predicates; the analyzer now matches both `col` and `tbl.col` forms
  when extracting referenced columns.
- **`safe_alter_table.create_index` rejects unknown `index_type`.**
  Allowlist is now `{btree, hash, gist, spgist, gin, brin}` — anything
  else throws before SQL is constructed.
- **`batch_execute` with `stopOnError: true` short-circuits.**
  Previous implementation pre-fired all queries via `Promise.all` and
  only filtered failures afterwards; remaining queries can now never
  observe side-effects of an earlier failure.
- **`mutation_dry_run` preserves the real PG error code.** The
  no-RETURNING fallback now only fires for the two error codes that
  actually indicate a RETURNING-clause issue (`42703`
  undefined_column / `0A000` feature_not_supported). Other failures
  (e.g. `23505` unique_violation, `23503` foreign_key_violation)
  surface their original code, detail, and constraint name to the
  caller — instead of being masked as `25P02 in_failed_sql_transaction`.
- **`dry_run_sql_file` per-statement savepoints.** Each user statement
  now runs inside its own `SAVEPOINT psm_stmt_<idx>` when
  `stopOnError: false`, with `ROLLBACK TO SAVEPOINT` on failure. Two
  benefits: (a) cascading `25P02` errors are gone — every failing
  statement reports its own real error code with line number;
  (b) `COMMIT`/`ROLLBACK` issued from inside a `DO` block is now
  contained — PG refuses transaction termination inside a savepoint
  subtransaction, so the dry-run is no longer compromised when a
  migration script DO-blocks an embedded `COMMIT`.
- **`get_top_queries` runtime probes `pg_stat_statements`.** Catalog
  membership is no longer trusted; we issue a
  `SELECT 1 FROM pg_stat_statements LIMIT 0` and distinguish
  `42P01 undefined_table` (not installed) from `55000 object_not_in_prerequisite_state`
  (installed but `shared_preload_libraries` missing) with clear
  remediation messages. Legacy `total_time / mean_time` columns are
  only attempted on PG < 13.
- **`list_objects` + `get_object_details` materialized-view support.**
  `relkind='m'` is now a first-class object kind (queried via
  `pg_class` directly since `information_schema.views` doesn't expose
  it). `get_object_details` auto-detects view / matview / sequence
  from `pg_class.relkind` regardless of the caller-supplied
  `objectType`, returns `exists: false` early when the object isn't
  found, surfaces sequence-specific metadata, and includes CHECK
  constraint expressions via `pg_get_constraintdef`.
- **`explain_query` validates `hypotheticalIndexes` even without
  hypopg.** Validation moved out of the `if (has_hypopg)` branch.
  `hypopg_reset()` now only runs when at least one hypothetical index
  was actually registered (was previously called unconditionally —
  harmless but noisy).
- **`analyze_db_health` Invalid Indexes failure surfaces as
  `warning`.** Earlier code swallowed the error and reported
  `status: 'healthy'`, which lied about cases where the catalog query
  itself failed (permissions, etc.). The check now reports
  `status: 'warning'` with the actual error text in `message`.

### Changed

- **`TableInfo.type` widened** to include `'matview'`. Additive — no
  caller change required.
- `MEMORY.md`, `audit-iteration-3.md` design / fix doc added under
  `docs/superpowers/program/` for future audit-loop iterations.

### Tests

- New `src/__tests__/audit/iteration-3-fixes.test.ts` (14 tests),
  one per P0/P1 fix above. Exercised against a real PG 17 cluster
  via `AUDIT_PG_URL`.
- `821 tests pass across 28 suites` against real PG 17 (no mocks for
  any of the fixed behaviors).

### Documentation

- README: documented all 13 v3 tools (`describe_table`,
  `find_dependents`, `lock_check`, `safe_alter_table`,
  `detect_migration_state`, `export_to_sql_file`, `transfer_objects`,
  `schema_diff`, `column_profile`, `generate_seed_data`,
  `find_blocking_queries`, `kill_query`, plus
  `maxEstimatedRows` / `maxEstimatedCost` query-budget flags on
  `execute_sql`). Refreshed the connection-override list. Added a
  Development & Testing section explaining the
  `AUDIT_PG_URL` / `testcontainers` / skipped flow for contributors.

## [3.0.0] — 2026-05-03

### Added (SP-7 — operations & safety pack)

- **`find_blocking_queries` MCP tool** — friendly tree of blocker →
  blocked sessions. Returns each session's pid, user, database,
  application name, state, current query (truncated), time spent in
  the current state, and PG wait_event / wait_event_type. Uses
  `pg_blocking_pids()` (PG 9.6+) — replaces the gnarly
  `pg_stat_activity ⨝ pg_locks` join an AI agent struggles to write.
- **`kill_query` MCP tool** — `pg_cancel_backend` (soft, mode='cancel')
  or `pg_terminate_backend` (hard, mode='terminate'). Requires
  `confirm: true` (foot-gun guard). Refused if the target server's
  effective access mode is readonly. Returns a pre-kill snapshot of
  the target session.
- **`maxEstimatedRows` / `maxEstimatedCost` flags on `execute_sql`** —
  SP-7 query budget. Pre-EXPLAIN check on read-only queries only;
  refuses to execute if the planner's estimate exceeds the budget.
  Backstop for AI-generated queries against production. Silently
  ignored on write queries (we don't EXPLAIN ANALYZE writes — that
  would defeat read-only mode).

### v3.0.0 rollup

This is the bundled v3 release of the toolkit assembled across
SP-1 through SP-7. The full surface added since v2.3.0:

| SP | Tools / Changes |
|----|-----------------|
| SP-1 | dry-run trust restoration: 4 tools fixed, transaction-guard sentinel + TRANSACTION_CONTROL static skip |
| SP-2 | introspection module + `export_to_sql_file` (4 variants) |
| SP-3 | `transfer_objects` (cross-DB / cross-server release) |
| SP-4 | `describe_table`, `find_dependents`, `schema_diff` |
| SP-5 | `lock_check`, `detect_migration_state`, `safe_alter_table` |
| SP-6 | `column_profile`, `generate_seed_data` |
| SP-7 | `find_blocking_queries`, `kill_query`, `query_budget` flag |

**13 new MCP tools added; 0 existing tools removed; all changes
additive at the API level. The only behavior change is SP-1's
silent-persistence bug fix in dry-run tools.**

## [2.8.0] — 2026-05-03

### Added (SP-6 — data understanding pack)

- **`column_profile` MCP tool** — single-pass profile per column:
  null count + percent, distinct count + ratio, top-K values with
  frequencies (default 10, max 25), and type-aware stats (min/max/
  avg/stddev for numeric, length min/max/avg for text, range for
  temporal). Uses `TABLESAMPLE BERNOULLI` for tables larger than
  `sample_threshold` (default 1M rows) to bound latency. Replaces
  ~10 separate exploratory queries.
- **`generate_seed_data` MCP tool** — schema-aware fake seed data
  generation. Respects NOT NULL (uses DEFAULT for unknown types),
  UNIQUE/PK (retry-with-suffix to avoid collisions), enum types
  (cycles through labels), text length limits, generated/identity
  columns (skipped). Generates type-appropriate values for numeric
  (integer/bigint/numeric/decimal), text (text/varchar/char/citext),
  boolean, uuid (`gen_random_uuid()`), date/timestamp (epoch +
  rowIndex), bytea, JSON, inet, cidr, macaddr. Per-column overrides
  via `column_values` (caller supplies the SQL literal). Apply mode
  default; `apply: false` returns SQL only for review.

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
