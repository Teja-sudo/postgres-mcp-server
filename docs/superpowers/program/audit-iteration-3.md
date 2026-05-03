# Audit iteration 3 — all-tools findings

Real PG 17 cluster on 127.0.0.1:5433. Six per-tool-group subagents
running in parallel against the complex-schema fixture.

Awaiting: server/connection (group 1), SQL execution (3), dry-run+tx
(4), perf+health (5), v3 re-exercise (6).

## Group 2 — schema exploration (3 tools)

46 integration tests against the audit cluster; all passed. Findings
describe ACTUAL current behavior of `src/tools/schema-tools.ts`.

### P0 correctness

1. **`list_objects` cannot list materialized views.** `ObjectType`
   enumeration in `schema-tools.ts:11` is
   `'table' | 'view' | 'sequence' | 'extension' | 'all'` — no
   `'matview'`. The view branch (`:48-59`) queries
   `information_schema.views`, which excludes matviews (relkind 'm').
   Fixture's `tenant_revenue` matview is invisible. **Fix:** add a
   `'matview'` branch using `pg_class WHERE relkind='m'`.

2. **`get_object_details` view definition gated on caller-supplied
   `objectType`** (`schema-tools.ts:372`). Auto-detection path never
   fetches `pg_get_viewdef`. **Fix:** look up the relkind once
   (cheap), then fetch view def whenever it's `v` or `m`.

3. **Quoted non-ASCII identifiers don't reach catalogs.**
   `validateIdentifier` accepts `'"日本語"'` but the catalog queries
   bind `$1` against tables that store the unquoted name. Caller
   gets 0 results. The unquoted form is rejected outright by
   `UNQUOTED_IDENTIFIER_PATTERN`. Net: non-ASCII schemas/objects
   are unreachable. **Fix:** strip surrounding quotes (unescape
   `""`) before binding; OR relax pattern to allow Unicode letters.

### P1 missing

1. **`get_object_details` for sequences returns no sequence-specific
   info** (no start, min, max, increment, last_value, cycle, cache).
   None of the queries hit `pg_sequence`/`pg_sequences`. Affected:
   `schema-tools.ts:252-391`.

2. **Non-existent objects silently return empty.** No `notFound`
   flag, no error. Caller can't distinguish "FK-less table" from
   "missing table". **Fix:** existence check via `pg_class`.

3. **CHECK constraints returned without `column_name` or
   expression** (`:313-330`). The kcu join only fires for keyed
   constraints. **Fix:** join `information_schema.check_constraints`
   or use `pg_get_constraintdef`.

4. **`list_objects` doesn't expose triggers, functions, types,
   indexes, foreign tables.** Fixture has 2 triggers, 2 functions,
   1 procedure, 1 enum, 1 composite — none discoverable.

### P2 UX

- Pagination math correct (verified at all edges)
- Identifier validation security solid (SQL injection rejected)
- `targetSchema` parameter naming awkward; arguably unused since
  catalog queries are fully qualified
- View `owner` may be empty when name collides across schemas
  (LEFT JOIN by name only)

### Verified correct

- `list_schemas` system-schema filter, override, empty-DB
- `list_objects` filter ILIKE, all overrides
- `get_object_details` composite PK/FK, partial/expression/GIN/BRIN
- `rowCount`/`size` byte-for-byte match catalog values

---

## Group 3 — SQL execution (4 tools)

Static analysis only (subagent's bash invocations were sandbox-denied;
they couldn't run the temp test file). Citations are file:line in
`src/tools/sql-tools.ts` unless noted.

### P0 correctness

1. **`batch_execute` `stopOnError: true` does NOT actually stop.**
   `sql-tools.ts:2010-2052`. Every query is dispatched in a single
   `Promise.all(queries.map(...))` before the result-collection
   loop runs. The `if (stopOnError) break` only stops *counting*,
   not *executing* — by the time the loop sees the first failure,
   all queries (including those after it) have already hit the
   database. Side-effect writes from later queries persist; the
   `results` map omits them, so `successCount + failureCount !==
   queries.length`. **Fix:** change to a sequential `for`-await
   loop that respects the flag.

### P1 missing / wrong

1. **`query_budget` false-negative on aggregates and LIMIT** (known
   from iteration 1). `sql-tools.ts:154-187`. Reads only the top
   plan's `Plan Rows` and `Total Cost`. `SELECT count(*) FROM users`
   reports `Plan Rows = 1`, lets a full-table aggregate through.
   `SELECT … LIMIT 1` same. **Fix:** recurse into child plan nodes,
   take the max `Plan Rows` from leaf scans.

2. **`previewSqlFile` WHERE detection is naive uppercased substring**
   (`sql-tools.ts:1002-1011`). Matches `where_id` (column name),
   `nowhere`, strings, comments. False positives + false negatives.
   **Fix:** strip strings/comments first using the existing
   `stripCommentsAndStrings` helper.

3. **`execute_sql` connection-override + transactionId check** is
   over-broad: schema-only override is refused even though
   `SET search_path` works fine inside an active tx
   (`sql-tools.ts:124-128`). Minor footgun.

4. **`outputFile` temp files never auto-deleted.** `os.tmpdir()/
   postgres-mcp-output-*.json` accumulates. The MCP server has no
   cleanup hook for old output files. **Fix:** age-based cleanup
   on startup, or self-deleting tempfiles via `unref` + finalizer.

5. **`execute_sql_file` `validateOnly: true` preview NOT
   equivalent to `previewSqlFile`.** Different return shape.
   validateOnly omits `statementsByType`, `warnings`, `summary`,
   `fileSizeFormatted`. validateOnly also returns ALL statements
   regardless of count (no `maxStatements` cap). Document the
   difference, or align them.

### P2

- `maxRows: 0` rejected with confusing error ("between 1 and
  100000") despite `|| MAX_ROWS_DEFAULT` fallback that's now dead
  code.
- Multi-stmt `getSchemaHintForSql` runs once on the whole concat,
  so `MAX_TABLES_TO_ANALYZE=10` silently drops tables from later
  statements.
- `foreignKeys` undefined vs `[]` inconsistency on FK-less tables.
- `executeSqlFile` SP-1 layer-1 skipped statements push synthetic
  errors but don't increment `statementsFailed` — there's a third
  "skipped" bucket the result schema doesn't expose.

### Verified correct (static)

- MAX_PARAMS=100, non-array params, allowMultipleStatements+params
  rejection
- `allowLargeScript` bypass of 100KB cap
- `execute_sql_file` extension/.sql, 50MB size, empty-file checks
- `batch_execute` empty/>20/duplicate-name rejection
- transaction-guard sentinel placement in execute_sql multi-stmt
  and executeSqlFile

---

## Group 1 — server + connection (4 tools)

58/58 tests passed against the audit cluster. 4 bugs identified.

### P0 correctness/security

1. **Schema-name validation bypassed in `switch_server_db`.**
   `db-manager.ts:619-680` (`switchServer`) calls
   `validateDatabaseName(dbName)` at line 639 but never validates
   the user-supplied schema. Line 668-669 assigns it into
   `connectionState.currentSchema` raw. Repro:
   `switch_server_db({server:'srvA', schema:'public; DROP TABLE
   users--'})` returns `{success:true}`. SQL-injection at execute
   time is mitigated only by `escapeIdentifier()` so it's not an
   active exploit, but it (a) violates the documented validation
   contract, (b) leaves nonsensical schema names in state, (c) is
   inconsistent with `setCurrentSchema()` which DOES validate.
   **Fix:** call `validateSchemaName(schema)` early in
   `switchServer`. The validation function already exists at
   `src/db-manager/validation.ts:38-42`.

### P1 missing / surprising

2. **`includeSystemDbs:true` is a no-op for template DBs.**
   `server-tools.ts:60-69`. SQL hardcodes `WHERE datistemplate =
   false`, so `template0`/`template1` never reach the JS filter
   at line 198-200. The user-facing flag implies they become
   visible when `true`; they don't. The `systemDbs` JS filter is
   dead code. **Fix:** drop the SQL `WHERE` when flag is true, OR
   document that templates are always hidden.

3. **Negative `maxResults` silently truncates results.**
   `server-tools.ts:178`. `Math.min(args.maxResults || 50, 200)`
   does not clamp to ≥ 0. Then `databases.slice(0, -100)` drops
   the last 100. Repro:
   `listDatabases({serverName:'srvA', maxResults:-100})` returns
   `[]`. **Fix:** clamp to `[1, 200]`.

4. **`maxResults: 0` silently becomes 50.** Same line, the `||`
   replaces a falsy `0` with the default. A caller asking for
   "zero" gets 50. **Fix:** use `?? 50` and validate input.

### P2 UX

5. `filter` arg in `list_servers` and `list_databases` doesn't
   trim whitespace; `'  srv  '` matches nothing.
6. `switch_server_db` error wrap doubles "Error:" prefix: template-
   coercing the inner Error to string yields the redundant text
   `Failed to switch: Error: Failed to connect ...`.
7. `switch_server_db` success message doesn't mention which DB/
   schema were defaulted to, only the server.

### Verified correct

- Credential hiding (host/port/user/password never appear in
  list_servers / list_databases responses)
- Default-server fallback when no `PG_DEFAULT_*=true`
- Reconnect after failure (no stale pool)
- accessMode priority: database > server > global, all aliases
  (ro/rw/readonly/full/read-only) work
- `context` and `user` fields surface correctly

---

## Group 6 — v3 surface re-exercise (12 tools)

39/41 sub-tests passed. All iteration-1 P0 fixes verified holding.
Two new issues iteration-1 missed; three known-but-unfixed
iteration-1 issues confirmed still present.

### NEW P0 (iteration-1 missed)

1. **`detectMigrationState` cannot detect Sequelize** despite the
   probe being listed. `safety-tools.ts:381,440-443` calls
   `to_regclass(${s}.${probe.table})` with the literal
   `public.SequelizeMeta`. PG's identifier parser folds the
   unquoted mixed-case `SequelizeMeta` → `sequelizemeta`, then
   looks up that name. Real Sequelize tables are stored with
   the case-preserving `"SequelizeMeta"` identifier, so the
   probe always returns NULL. Reproduced: 9/10 trackers
   detected, Sequelize missing. **Fix:** pass quoted identifiers
   via `to_regclass(format('%I.%I', schema, table))`.

### NEW P1 (iteration-1 missed)

2. **`columnProfile` re-samples per query → internally
   inconsistent results.** `data-tools.ts:114-153` interpolates
   `<table> TABLESAMPLE BERNOULLI (n)` into multiple separate
   queries (null count, distinct count, top-K, min/max). Each
   query draws its OWN fresh Bernoulli sample. Repro:
   `sample_threshold=10, sample_percent=50` on 200-row table
   → `distinctCount=96, totalRows=84` (impossible by
   construction). Iteration-1 noted "sample math is sample-
   local" but didn't flag this multi-sample inconsistency.
   **Fix:** materialize the sample once into a CTE
   (`WITH s AS (SELECT * FROM tbl TABLESAMPLE BERNOULLI(n))`)
   and run all aggregates against `s`.

### Iteration-1 issues STILL PRESENT (known but unfixed)

- **`safe_alter_table.create_index` SQL-injection footgun**
  (iteration-1 SP-5 P1#5). `safety-tools.ts:652,657` interpolates
  `intent.index_type` unvalidated. `index_type: 'malicious-
  injection'` produced raw `USING malicious-injection (...)`.
- **`generateSeedData` violates CHECK constraints**
  (iteration-1 SP-6 P1#1). `email LIKE '%@%'` and
  `currency ~ '^[A-Z]{3}$'` both fail at INSERT time. CHECK
  expressions are collected but unused by `generateValueForColumn`.
- **`findDependents.truncatedAtDepth` is dead code**
  (iteration-1 SP-4 P2). Off-by-one in `awareness-tools.ts:490`
  (`if (depth + 1 < maxDepth)`) prevents the flag from ever
  flipping.

### Iteration-1 fixes confirmed HOLDING

- export_to_sql_file: schema_dump round-trip clean,
  session_replication_role replica/origin emitted, matview
  REFRESH, sequence setval, empty arrays
- transfer_objects: pgcrypto fns not duplicated, IDENTITY
  tables, if_exists skip+replace, post-load setval
- describeTable: composite PK/FK arrays, GENERATED + IDENTITY
  flags, column comments
- findDependents: transitive FK reach, TOAST/self-array filter
- schemaDiff: identical empty, column drift uses ALTER not DROP
- lockCheck: BEGIN/COMMIT wrapper, ADD COLUMN NOT NULL warning
- detectMigrationState: false-positive on `migrations` blocked
- safeAlterTable: all 6 intents, applied recipes work
- findBlockingQueries: empty + blocker scenario both correct
- killQuery: confirm/non-existent/cancel/terminate all correct

---

## Group 4 — dry-run + transactions (8 tools)

59/59 tests passed (3 encode the bugs explicitly so they fail-loud
when fixed).

### P0 correctness

1. **`mutation_dry_run` masks real PG error codes with `25P02`
   (in_failed_sql_transaction).** `sql-tools.ts:1842-1871`. When
   the RETURNING-augmented query fails, PG poisons the outer tx;
   the no-RETURNING fallback then errors with 25P02 and that
   becomes `result.error.code`. Real codes (`23505` unique,
   `23503` FK) plus `constraint`/`detail`/`hint` are lost.
   Reproduced for both unique and FK violations. AI agents rely
   on these codes to choose remediation. **Fix:** capture the
   FIRST error; only run the no-RETURNING fallback if the first
   error is genuinely about RETURNING (e.g., `42703`
   undefined_column).

2. **`dry_run_sql_file` with `stopOnError: false` emits cascading
   `25P02` errors after the first failure.** `sql-tools.ts:1232-
   1251`. Single outer tx, no per-statement savepoints. After the
   first failing statement PG aborts the tx and every subsequent
   statement fails with 25P02. The "continue and collect" promise
   degrades to "report every later statement as 25P02". Repro:
   4 statements with errors at #2 and #3 → operator can't see
   the real error of statement 3 or 4. **Fix:** wrap each
   statement in a per-statement `SAVEPOINT`, `ROLLBACK TO`
   savepoint on failure, then continue.

### P1 missing / wrong

1. **`transaction-guard.SUSPECT_PATTERN` fires on plain EXECUTE/
   CALL/DO** even when no transaction control happens.
   `transaction-guard.ts:45-46`. Correctness intact; perf cost is
   one extra `RELEASE+SAVEPOINT` round-trip per such statement,
   visible on the 1000-statement test. **Fix:** narrower regex,
   or accept the cost as defense-in-depth.

2. **`mutation_preview` rejects `WITH ... UPDATE/INSERT/DELETE`
   despite `mutation_dry_run` accepting it.** `sql-tools.ts:1567-
   1578`. Inconsistent surface. Also uses `lastIndexOf('WHERE')`
   which grabs WHERE inside subqueries in
   `UPDATE ... SET col = (SELECT ... WHERE ...)`. **Fix:** add
   the WITH-prefix branch + safer WHERE extraction.

3. **CLUSTER static pattern uses `!upperSql.includes('CREATE')`**
   which falsely matches CREATE inside SQL comments
   (`dry-run-utils.ts:32, 205-216`). **Fix:** strip comments
   first (consistent with TRANSACTION_CONTROL detection).

### P2

- Sequence-side-effect on plain INSERT into SERIAL → row count
  rolled back, sequence value still consumed. Warning fires
  (`mustSkip:false`) but should be louder.
- Read-only mode enforcement (server-level + db-level) correct
  for transactions
- Compromised-flag propagation correct across commit + rollback
- DO-block COMMIT in dryRunSqlFile caught by final pre-rollback
  verify (not per-statement)

### Verified correct

- 7-statement TC-variant skip with accurate line numbers
- DO-block COMMIT → `dryRunCompromised=true`, reason captured
- NEXTVAL skip with explainPlan populated for DML
- VACUUM, CLUSTER, REINDEX CONCURRENTLY, CREATE INDEX CONCURRENTLY,
  CREATE/DROP DATABASE skipped correctly
- `maxStatements` truncates results, totals reflect file
- 1000-statement file completes in well under 30s
- begin_transaction uuid7, list_transactions, get_transaction_info
  all correct
- Trigger fires + rolls back correctly in mutation_dry_run
- Composite-PK UPDATE works
- sampleSize capped at MAX_MUTATION_SAMPLE_SIZE=20

---

## Group 5 — performance + health (5 tools)

42 tests passed against the audit cluster. The cluster has
`pg_stat_statements` in `pg_extension` but NOT in
`shared_preload_libraries`, so reading the view fails with 55000.
`hypopg` is not available on the cluster.

### P0 correctness

1. **`get_top_queries` legacy fallback hits a dead column on
   PG 17.** `analysis-tools.ts:21-29` checks only
   `pg_extension`, not runtime usability. The primary query fails
   with 55000 ("must be loaded via shared_preload_libraries"), the
   catch at `:55` swallows it and runs the legacy SELECT. PG 13+
   removed `total_time`/`mean_time` columns, so the legacy query
   throws `column "total_time" does not exist` — masking the real
   error. **Fix:** runtime usability probe (`SELECT 1 FROM
   pg_stat_statements LIMIT 0`); detect SQLSTATE 55000 in catch
   and surface the original message; gate legacy fallback on
   `server_version_num < 130000`.

2. **`analyze_workload_indexes` inherits the same broken path**
   via `getTopQueries` (`analysis-tools.ts:92`). Same misleading
   error.

### P1 missing / wrong

1. **`explain_query` calls `hypopg_reset()` unconditionally**
   when `args.hypotheticalIndexes.length > 0` even if hypopg
   wasn't present (`sql-tools.ts:533-540`). The catch swallows
   the error but pollutes logs and wastes a round-trip. **Fix:**
   gate the reset on `has_hypopg`.

2. **`explain_query` validation of `hypotheticalIndexes` is
   gated on hypopg presence** — when hypopg isn't installed, junk
   payloads like `{ table: "users; DROP TABLE x", columns: ["id"]
   }` are silently accepted and the call returns a normal plan.
   The `MAX_HYPOTHETICAL_INDEXES` check is outside; identifier
   validation isn't. **Fix:** move all input validation out of
   the hypopg branch.

3. **`analyze_query_indexes` regex misses qualified column
   refs.** `analysis-tools.ts:281` uses `/\((\w+)\s*[=<>!]+/g`.
   PG 17 EXPLAIN renders filters as `(orders.status = 'paid')`
   — `\w+` after `(` matches the alias `orders`, not the column.
   **Fix:** `/\(?(?:[a-zA-Z_]\w*\.)?(\w+)\s*[=<>!]+/g`.

4. **`analyze_db_health` Invalid Indexes check hides permission
   errors.** `analysis-tools.ts:418`: when the query throws (e.g.
   permission denied), the catch reports
   `'No invalid indexes found'` — masking a genuine failure.
   **Fix:** mark the category as `'warning'` with the error
   message in details.

### P2

- `get_top_queries` `orderBy` silently coerces invalid values to
  `total_time`; typos like `'total-time'` go undetected
- `analyze_query_indexes` deduplication keyed correctly on
  `table:sortedCols:type`

### Verified correct

- EXPLAIN ANALYZE write-block: refuses INSERT, UPDATE, DELETE,
  TRUNCATE, AND `WITH x AS (INSERT ... RETURNING) SELECT *` (CTE
  write path)
- analyze_db_health resilience: each of 8 checks wrapped in own
  try/catch
- Synthetic dirty states detected: duplicate indexes, NOT VALID
  constraints, high-dead-tuple tables
- explain_query 100KB rejection, > 10 hypotheticalIndexes
  rejection, schema-qualified `public.tbl` table parsing
- analyze_query_indexes empty/>10/per-query rejections all clean

---

# Final tier-ordered fix list

## Tier 0 — single-line bug fixes (do first)

| # | Where | Issue |
|---|---|---|
| T0-1 | `db-manager.ts:619-680` | `validateSchemaName(schema)` missing in `switchServer` |
| T0-2 | `safety-tools.ts:440-443` | `to_regclass` on mixed-case `SequelizeMeta` folds to lowercase; use `format('%I.%I', ...)` |
| T0-3 | `server-tools.ts:178` | `maxResults` clamping (`||` swallows 0; no negative-number guard) |
| T0-4 | `awareness-tools.ts:490` | `truncatedAtDepth` off-by-one — flag never trips |
| T0-5 | `analysis-tools.ts:281` | regex column extraction misses qualified refs |
| T0-6 | `safety-tools.ts:652,657` | `index_type` allowlist missing |

## Tier 1 — multi-line correctness

| # | Where | Issue |
|---|---|---|
| T1-1 | `sql-tools.ts:2010-2052` | `batch_execute stopOnError:true` doesn't short-circuit (Promise.all) |
| T1-2 | `sql-tools.ts:1842-1871` | `mutation_dry_run` masks real PG error codes with 25P02 |
| T1-3 | `sql-tools.ts:1232-1251` | `dry_run_sql_file stopOnError:false` cascading 25P02 |
| T1-4 | `analysis-tools.ts:21-50` | runtime usability probe for pg_stat_statements |
| T1-5 | `schema-tools.ts:11,35-86` | matview missing from `list_objects` |
| T1-6 | `schema-tools.ts:372` | view definition auto-detection |
| T1-7 | `schema-tools.ts:309-365` | non-existent object → existence check + error |
| T1-8 | `schema-tools.ts:313-330` | CHECK constraints expression via `pg_get_constraintdef` |
| T1-9 | `sql-tools.ts:533-540` | `hypopg_reset()` only when hypopg present |
| T1-10 | `sql-tools.ts:463-497` | hypotheticalIndexes validation outside hypopg branch |
| T1-11 | `analysis-tools.ts:418` | Invalid Indexes check exposes permission error |

## Tier 2 — featureful fixes

- T2-1: `data-tools.ts:114-153` — `column_profile` materialize sample once via CTE
- T2-2: `dry-run-utils.ts:32` — strip comments before CLUSTER pattern check
- T2-3: `data-tools.ts:520+` — `generateSeedData` CHECK-aware generation (fixture-relevant subset)
- T2-4: `sql-tools.ts:1567-1578` — `mutation_preview` accept WITH-prefixed mutations
- T2-5: `server-tools.ts:60-69` — `includeSystemDbs:true` actually shows templates
- T2-6: `sql-tools.ts:236-251` — output-file cleanup hook

## Tier 3 — UX polish

- Filter whitespace trimming
- `switch_server_db` error/message clarity
- `targetSchema` parameter rename (defer; breaking change)
- `executeSqlFile` skipped-statement count surface
- `validateOnly` ↔ `previewSqlFile` shape alignment

