# Audit iteration 1 — findings

Real PG 17 audit cluster on 127.0.0.1:5433.
Per-pack subagents exercised tools against complex-schema fixture
(10+ tables, FKs, views, matview, plpgsql funcs/procs, triggers,
enum + composite types, partial/expression/GIN/BRIN indexes,
~5–22K rows of seed data).

## Pre-audit baseline fixes (already applied)

- `pg_get_*def($1)` text-overload trap → cast all to `$1::oid` in
  `src/tools/introspection/object-ddl.ts` (5 sites)
- `write after end` from buffered `Writable` wrapper → refactored
  `data-emitter.ts` to use synchronous `WriteFn`
- SERIAL columns failed transfer → emit `SERIAL`/`BIGSERIAL`
  literally so PG auto-creates the sequence (no separate transfer
  needed)
- `public` schema collision on `*` transfer → filtered out in
  `transfer-tools.ts:resolveSourceObjects`
- `executeSqlFile(useTransaction=true)` missing static
  TRANSACTION_CONTROL skip → added
- PG error code `25P02` (in_failed_sql_transaction) wasn't
  recognized by the savepoint sentinel → added to
  `transaction-guard.ts`

After these, all 24 test suites + 783 tests pass against real PG.

---

## SP-6 (data-tools): findings

### P0 — Correctness / Crashes

1. **`generateSeedData` is non-functional on real PG.** Every call
   fails with `cannot cast type boolean to oid[]`. Root cause:
   precedence in `loadColumnMeta` SQL — `a.atttypid = ANY(ARRAY[1700])::oid[]`
   evaluates the equality first (returning bool), THEN tries to cast
   bool → oid[]. **Fix:** `a.atttypid = ANY(ARRAY[1700]::oid[])`
   (move the cast inside the ANY).
   Source: `src/tools/data-tools.ts:402-405`.

2. **Sampling math is sample-local.** `topValues.percent`,
   `nullPercent`, `distinctCount` are computed against the Bernoulli
   sample size, not the population. Result schema does not flag
   that the values are sample-derived. Comparing two profiles
   (one sampled, one not) silently produces drift.

3. **`distinctCount` has no extrapolation under sampling.** No
   Horvitz–Thompson / GEE estimator; ignores cheap
   `pg_stats.n_distinct` entirely. High-cardinality columns
   under-report dramatically.

### P1 — Missing features

1. **No CHECK-aware generation.** `users.email CHECK (email LIKE '%@%')`
   → generator emits `'seed_email_0'` (no `@`). `orders.currency CHECK`
   regex similarly violated. `checkExprs` is collected on `ColumnMeta`
   but never read by `generateValueForColumn`. Tool would produce
   constraint-violating data even if the P0 crash were fixed.

2. **No FK auto-resolution** despite docstring claim.
   `skip_fks: false` just emits `1+i` for int FK columns — works
   only by chance for tiny tables.

3. **Composite types not supported.** Generator returns null →
   then NULL or DEFAULT. No emission strategy for record types.

4. **`bytea` all-NULL column reports `distinctCount: 0`** (legit
   meaning) but conflates with "column has 0 distinct non-null
   values". Should be `null` for "undefined".

### P2 — UX / perf

1. **`sample_threshold=1_000_000` default is too high** for
   realistic dev databases — sampling never kicks in. Either lower
   to ~100K or surface "did/didn't sample" more prominently.

2. **`distinctRatio` ignores `pg_stats.n_distinct`** which is
   already computed cheaply (no scan needed). Tool runs its own
   `COUNT(DISTINCT col)` instead.

3. **N+1 query in `loadColumnMeta`** — separate enum-lookup
   fired per column even for non-enum columns.

4. **`information_schema.element_types ON FALSE`** in column meta
   query is dead code. `charMax` therefore always undefined →
   varchar truncation logic in `generateValueForColumn` never
   exercised.

5. **`isIdentity = !!r.attidentity`** — `attidentity` is
   `char(1)` (`'a'`, `'d'`, or `''`); the JS double-bang happens
   to work because empty string is falsy, but it's brittle.

### Confirmed correct

- `columnProfile` null counts, distinct counts, top-K, percent
  match cross-checks exactly when not sampled. UUID
  `distinctRatio=1.0`. Numeric stats match raw aggregates byte-for-byte.
- `count:0` and `count:100001` validators reject correctly.
- Nonexistent-table errors are clear.

---

## SP-5 (safety-tools): findings

36 tests against `audit_sp5_a` — all passed but several real bugs.

### P0 correctness

1. **`lockCheck` returns `unknown` on `BEGIN; <DDL>; COMMIT;` blocks.**
   Anchored regex never looks past leading `BEGIN;`. Returns the
   bare-string `unknown` lock + empty notes — silently. Needs to
   strip outer transaction-control statements before matching.

2. **`detectMigrationState` false-positives on user tables named
   `migrations`.** Probe is `to_regclass(schema.migrations)` only —
   any business table named `migrations` is reported as TypeORM
   with `appliedCount=N`. Need column-shape verification (TypeORM
   has `id`, `timestamp`, `name`). Same vulnerability for any
   probe-name collision.

3. **`add_column ... NOT NULL` (no default) reported as
   `forcesRewrite=false` with no warning that it will fail on
   existing non-empty tables** — real PG rejects with
   `column "x" of relation "users" contains null values`.
   Lock_check should at least surface "fails if rows exist" for
   this combination.

4. **`add_check` reports `AccessExclusiveLock` even though PG 14+
   only takes ACCESS EXCLUSIVE briefly for metadata; the validation
   phase is ShareRowExclusive.** Misleading severity.

### P1 missing

1. **No column-shape validation in `detectMigrationState`** —
   any matching table name detects, regardless of whether the
   columns look like the migration tool's actual schema. Affects
   reliability of `latestVersion` lookup too (we read columns that
   may not exist on a collision).

2. **`lockCheck` returns `unknown` for many real DDL variants** —
   `DROP MATERIALIZED VIEW`, `ALTER TYPE`, `ALTER SCHEMA`,
   `ALTER SEQUENCE`, `LOCK TABLE`, `ATTACH PARTITION`/
   `DETACH PARTITION`, `SET LOCAL lock_timeout`.

3. **`safe_alter_table.add_not_null_column_with_default` step 2
   emits one big UPDATE.** The notes mention "batch this for
   large tables" but the SQL itself is a single statement. For
   the > 1M row case the recipe is supposed to address, the user
   has to manually rewrite.

4. **`add_not_null` recipe doesn't probe PG version.** Notes say
   "On PG 12+: SET NOT NULL is now fast" but emits the same SQL
   regardless. On PG ≤ 11 the recipe still does a full-table scan
   under ACCESS EXCLUSIVE.

5. **`create_index.index_type` is interpolated unquoted/unvalidated**
   — caller-controlled input lands in the emitted DDL. Minor
   SQL-injection / footgun surface. Should validate against the
   known set: btree/hash/gist/spgist/gin/brin.

### P2 UX/perf

- Duration estimate uses table-rewrite math regardless of op kind;
  DROP INDEX gets a wrong "< 1 second" by luck on small fixtures.
- `lockCheck` silently swallows the size-query exception
  (`catch {}`); permission-denied looks like missing-table.
- `detectMigrationState` orders by `version_num DESC`
  lexicographically — Flyway `1.10.0` vs `1.2.0` returns the
  wrong one as "latest".

### Verified correct

- All 14 tested DDL → lock-level mappings except the BEGIN block.
- Detection of Flyway, Alembic, Liquibase including in non-public
  schema.
- All 6 `safe_alter_table` recipes apply cleanly to fresh fixture.
- CONCURRENTLY recipes work outside transactions via direct pool.

---

## SP-3 (transfer-tools): findings — SEVERE

The SP-3 audit found 7 P0 bugs against the complex schema. The
tool currently does NOT support `objects: '*'` against a database
with extensions, IDENTITY columns, GENERATED columns, or
serial-backed PKs — i.e. essentially every realistic schema.

### P0 correctness

1. **Extension-owned functions re-listed and collide with
   `CREATE EXTENSION`.** `listObjectsInScope` returns all 38
   pgcrypto functions (`armor`, `digest`, `gen_random_uuid`, etc.)
   as user-owned functions. With pgcrypto installed first, the
   transfer loop hits "already exists" (or "name not unique" for
   overloads). **Fix:** filter `pg_proc` rows whose
   `pg_depend` shows extension membership (`deptype='e'`).
   Same risk applies to extension-owned types/operators/casts/etc.

2. **IDENTITY column data INSERT fails.** Both `transferTableData`
   and `emitTableRowsAsInsert` build column lists from every
   non-dropped attribute — `attidentity='a'` (GENERATED ALWAYS)
   columns get explicit values, which PG rejects unless
   `OVERRIDING SYSTEM VALUE` is on the INSERT. Affected: `products.id`
   in our fixture. **Fix:** filter `attidentity` columns from the
   INSERT column list, OR emit `OVERRIDING SYSTEM VALUE`.

3. **GENERATED STORED columns included in INSERT column list.**
   Same code path; `attgenerated <> ''` columns also need
   filtering. Affected: `products.price_with_tax`,
   `order_items.line_total`. **Fix:** filter `attgenerated` columns.

4. **Sequence state not synced after data transfer — silent PK
   time-bomb.** Source rows transferred with explicit `id` values;
   target's auto-created sequence (`serial`) starts at 1. Next
   `nextval()` collides with existing rows. The audit run actually
   reproduced this: trigger-induced INSERT into `audit_log` after
   data load failed with duplicate-key violation. **Fix:** after
   data load for any serial-backed table, emit
   `SELECT setval(pg_get_serial_sequence('schema.table', 'col'),
   max(col)) FROM schema.table` per affected column.

5. **`if_exists: 'skip'` does not skip data.** The data-transfer
   loop unconditionally re-INSERTs every row, regardless of
   `if_exists` mode. Re-running a "skip" transfer either errors
   on PK violations (observed) or silently duplicates on tables
   without unique constraints. **Fix:** when `if_exists='skip'`
   AND target table has rows, skip the data INSERT for that table.

6. **`if_exists: 'replace'` cannot DROP overloaded functions or
   trigger objects.** `buildDropStatement` emits
   `DROP FUNCTION IF EXISTS schema.name CASCADE` with no
   arg-signature → fails on any overload. `buildDropStatement`
   returns `null` for triggers (skipped silently). **Fix:** for
   functions, fetch full signature via `pg_get_function_identity_arguments`.
   For triggers, drop via `DROP TRIGGER IF EXISTS name ON table`.

7. **Single-object transfer of dependent objects doesn't auto-
   include dependencies.** Transferring just `orders` table fails
   with "type order_status does not exist" — the enum, the
   composite type, AND the FK referenced tables aren't pulled in.
   `resolveSourceObjects` only returns explicit refs; the
   dependency graph is built but only used for ordering, never
   for closure expansion. **Fix:** add an optional
   `include_dependencies: boolean` flag (default true) that walks
   `pg_depend` from each requested object and pulls in
   transitively required types/sequences/tables/functions.

### P1 missing

- `if_exists:'skip'` is name-only for DDL — body-different objects
  silently skipped without warning
- No `SET CONSTRAINTS ALL DEFERRED` for data load — FK ordering
  burden falls on user
- COMMENT ON column/view/function/index lost (only TABLE comments
  survive)
- GRANT/REVOKE not transferred (documented as expected)
- Extension transfer name-only — no `WITH SCHEMA`/`VERSION`
- Standalone sequences (not serial-owned) reset to start values
  on transfer

### P2 UX/perf

- `transferTableData` loads `SELECT *` fully into Node memory
  then batches INSERTs — OOM risk for large tables. The
  `data_strategy` field allows only `'insert_batches'`; the
  comment promises streaming COPY in v2.
- Default `if_exists: 'error'` is nearly always wrong against a
  target that has any prior state.
- Error messages don't disambiguate DDL-phase failures from
  data-phase failures — users see PG errors with no context.
- `dry_run` with `if_exists='replace'` emits a comment line
  instead of the actual DROP statements that the apply path runs
  — the preview is not a faithful representation of the apply.

### Verified correct

- DDL extraction for table/view/matview/function/procedure/trigger/
  index correct on the happy path
- Topological ordering correct (types → tables → indexes → views →
  matviews → functions → triggers)
- FK constraints correctly broken out as ALTER TABLE statements
- Composite type values, arrays, JSONB, inet, uuid, partial/
  expression/GIN/BRIN indexes, enum, composite-PK tables, CHECK
  constraints all round-trip when the data path is allowed to run

---

## SP-2 (export-tools): findings — SEVERE

Round-trip (`schema_dump(include_data=true)` → fresh DB → replay)
is **broken**. Schema portion replays cleanly (104 stmts) but data
portion fails on first GENERATED-column table.

### P0 correctness

1. **GENERATED column in INSERT column list.** Same root cause as
   SP-3 P0#3. `emitTableRowsAsInsert` builds column list from
   every `pg_attribute` row without filtering `attgenerated <> ''`.
   `INSERT INTO order_items (..., line_total) VALUES (..., '262.02')`
   → `cannot insert a non-DEFAULT value into column "line_total"`.

2. **Empty arrays emitted as bare `ARRAY[]`.** `formatSqlLiteral([])`
   returns the unparseable `ARRAY[]` (PG: `cannot determine type of
   empty array`). Repro: `products.categories text[]` rows that
   have empty arrays. **Fix:** emit `'{}'::text[]` or
   `ARRAY[]::text[]` based on column type.

3. **Duplicate sequences after replay.** `object-listing.ts` lists
   sequences AND `extractTableDDL` emits `serial`/`bigserial`
   columns that auto-create sequences. After replay, source's 5
   sequences become 10 on target — half orphaned, half wired. Plus
   sequence ownership/setval state lost. **Fix:** filter
   serial-owned sequences (`pg_depend.deptype='a'`) from
   `listObjectsInScope`.

4. **Extension-owned functions re-exported as user functions.**
   Same bug as SP-3 P0#1. With pgcrypto installed in `public`,
   schema_dump emits 37 spurious `CREATE OR REPLACE FUNCTION
   public.armor(bytea) ... AS '$libdir/pgcrypto'` lines.
   **Fix:** filter `pg_proc` rows with extension membership
   (`pg_depend deptype='e'`).

5. **Triggers fire during data replay → duplicate trigger-side
   rows.** `audit_log` count is 2× source after replay because
   `trg_audit_users_after_insert` fires on the replayed user
   INSERTs. **Fix:** emit `ALTER TABLE ... DISABLE TRIGGER ALL`
   around the data section, OR sequence triggers AFTER data load.

### P1 missing

- Materialized views NOT refreshed after replay (`REFRESH
  MATERIALIZED VIEW` not emitted) → matview empty post-replay
- Matview's UNIQUE index (`tenant_revenue_pk`) NOT bundled with
  `kind: matview` — required for `REFRESH CONCURRENTLY`
- `COMMENT ON COLUMN` never exported (table comments survive)
- Cross-object FK deps silently stripped without warning when
  the referenced table isn't in the export list
- Sequence `setval` position not preserved (always restarts at 1)
- `schema_dump(schema='public')` emits ALL schemas + extensions
  (cluster-wide); too broad for a single-schema dump

### P2 UX/perf

- `bigint`/`numeric` rendered as quoted strings (PG accepts via
  coercion, but ugly)
- Composite-type values rendered as raw strings without `::type`
  cast — breaks for `query_result` to a fresh target
- `inet` rendered as bare string without `::inet` cast
- `emitTableRowsAsInsert` loads ENTIRE table into memory — comment
  promises server-side cursor / batchSize but neither is
  implemented; OOM on large tables
- Single-table object export emits unqualified `REFERENCES tenants`
  — search-path dependent

### Verified correct

- Dependency ordering, FK extraction, mode handling
  (append/overwrite + foot-gun guard)
- Identifier quoting incl. weird names
- Unicode round-trip byte-for-byte
- jsonb with embedded quotes, bytea, long text
- BRIN/GIN/partial/expression indexes round-trip
- IDENTITY columns emit correct GENERATED ALWAYS AS IDENTITY

---

## SP-4 (awareness-tools): findings

### P0 correctness

1. **FK columns returned as PG-array literal strings, not JS
   arrays.** `array_agg(att.attname ORDER BY u.ord)` on
   `pg_attribute.attname` (PG type `name`) — node-pg's default
   parser doesn't parse `name[]`. Values come back as raw
   `"{tenant_id}"` and `"{order_id,tenant_id}"` strings, the TS
   cast `r.cols as string[]` lies. Composite-FK consumers using
   `.length`/`.map`/etc. break. **Fix:** cast in SQL —
   `array_agg(att.attname::text ORDER BY u.ord)`.

2. **`findDependents` doesn't recurse through FK constraints to
   dependent TABLES.** From `users`: misses `orders`, `audit_log`,
   `tenant_revenue`, `audit_user_change` trigger fn — the constraint
   row is reported but the BFS enqueues with `classid=pg_constraint`
   so the next pass searches what depends on the CONSTRAINT, not
   on the dependent table. Net: "what tables transitively depend
   on this one" is broken. **Fix:** when a constraint row is
   discovered, also enqueue its `conrelid` table.

3. **`schema_diff` produces unsafe `DROP TABLE ... CASCADE; CREATE
   TABLE ...` for ANY column-type drift.** Verified: changing
   `tenants.name` from `text` → `varchar(255)` produces
   `DROP TABLE IF EXISTS tenants CASCADE` followed by CREATE.
   Cascade destroys all rows AND every dependent FK, view, matview.
   **Fix:** emit `ALTER TABLE ... ALTER COLUMN ... TYPE ...` for
   column-type-only diffs; full DROP+CREATE only as last resort
   or gated behind a flag.

4. **`findDependents` reports the target's array type and TOAST
   table as "dependents".** `pg_toast_27331` shows up at depth 1
   with `kind: "t"` (literal char from `relkind`). Plus internal
   indexes at depth 2. Inflate `totalDependents` and confuse
   callers. **Fix:** filter out `pg_toast_*` and array-type
   self-references.

### P1 missing

- **Column comments not surfaced.** `describeTable` columns lack
  a `comment` field even though `COMMENT ON COLUMN` data is
  available
- **GENERATED columns not flagged.** `products.price_with_tax`
  shows as `nullable:true, default:"round(price * (1 + tax_rate),2)"`
  — indistinguishable from a regular column with default
- **IDENTITY columns indistinguishable from plain NOT NULL int**
- **CHECK constraints, exclusion constraints, partition info, RLS
  policies** not surfaced in describe_table

### P2 UX/perf

- `findDependents` issues O(N) sequential queries per row
  (`describeOid` per OID, separate `pg_class` lookup per
  `classid`) — wide schema would N+1
- `truncatedAtDepth` is effectively dead code due to off-by-one
  (queue filter `depth + 1 < maxDepth` means children at depth
  `maxDepth` are never enqueued, flag never trips)

### Verified correct

- Identical-fixture diff is empty (`create=0 drop=0 modify=0`)
- COMMENT-only diffs detected
- Index storage-param diffs detected
- Round-trip works for a dropped view (drop on B → diff reports
  toCreate → migrationSql replays → diff empty)
- `rowCountEstimate` accurate after ANALYZE (1500/1500)

---

## SP-7 + SP-1-deep: findings — mostly clean

31/31 audit tests passed. No P0 findings.

### SP-7 P1

- **`query_budget` (`maxEstimatedRows`) false-negative for
  aggregates and LIMIT.** `SELECT count(*) FROM users` with
  `maxEstimatedRows: 10` PASSES because EXPLAIN reports OUTPUT
  rows (1) not rows scanned. Anything aggregated/LIMITed slips
  through. Need either a leaf-scan-aware check or a doc tweak
  pointing callers at `maxEstimatedCost` for full-scan
  protection.

### SP-7 P2

- `find_blocking_queries.tree` only contains nodes whose blocker
  is itself in the result set; if blocker is filtered by LIMIT
  or `include_idle: false`, blocked sessions surface in
  `blockedBy` but not in `tree`.
- `kill_query` against an override server with PID belonging to
  current server silently returns `signaled: false` — confused
  user could blame "PID gone".
- Writes (INSERT/UPDATE/DELETE) bypass `query_budget` entirely.
  Reasonable default but bulk DELETE of millions of rows slips
  through unbudgeted; could cheaply EXPLAIN (non-ANALYZE) writes.

### SP-1 deeper

All variants behave correctly:
- Mixed `BEGIN; ... COMMIT;` with DDL — caught
- Multi-line `BEGIN \n READ COMMITTED;` — caught
- `SAVEPOINT` + `RELEASE SAVEPOINT` — both skipped
- `ROLLBACK TO SAVEPOINT` — caught
- `CREATE INDEX CONCURRENTLY` warning surfaces
- `mutationDryRun` with `WITH … INSERT` (CTE write) — works
- `execute_sql` multi-stmt + `transactionId` + embedded COMMIT —
  `commit_transaction` returns `'compromised'`
- Edge files: `COMMIT;`-only, whitespace-only, comments-only — all OK
- 10,000-statement file dry-run completes in ~1.4s
- COMMIT inside `--`/`/* */` comments — not flagged (correct)
- `DO $$ ... END $$` block — runtime sentinel verifies; not falsely
  flagged

## SP-1 deeper — confirmed correct, no findings.

---

# Fix priority — consolidated

## Tier 0: trivial fixes (single-line scope)

1. **`generateSeedData` precedence bug** — `ARRAY[1700])::oid[]` →
   `ARRAY[1700]::oid[]`. SP-6 P0.

2. **`array_agg(att.attname)` returns name[]** that node-pg
   doesn't parse → cast `::text` in SQL. SP-4 P0.

## Tier 1: shared-root fixes (multi-pack)

3. **Filter extension-owned objects in `listObjectsInScope`**
   (functions, types). SP-2 P0, SP-3 P0.

4. **Filter IDENTITY/GENERATED columns from INSERT column lists**
   in `emitTableRowsAsInsert` AND `transferTableData`. SP-2 P0,
   SP-3 P0.

5. **Filter serial-owned sequences from `listObjectsInScope`**
   (don't double-emit `CREATE SEQUENCE` when SERIAL already does).
   SP-2 P0.

6. **Sync sequence state after data load** — emit
   `setval(pg_get_serial_sequence(...), max(col))` per affected
   column. SP-2 P0, SP-3 P0.

## Tier 2: important single-pack P0s

7. `lockCheck` strip outer `BEGIN; ... COMMIT;`. SP-5 P0.
8. `detectMigrationState` column-shape verification. SP-5 P0.
9. `schemaDiff` emit `ALTER TABLE ... ALTER COLUMN TYPE` instead
   of `DROP TABLE CASCADE`. SP-4 P0.
10. `findDependents` recurse through constraints to tables. SP-4 P0.
11. `findDependents` filter TOAST + self-array-types. SP-4 P0.
12. `if_exists:'skip'` in transfer skip data when rows exist. SP-3 P0.
13. `if_exists:'replace'` proper drop for overloads + triggers. SP-3 P0.
14. Empty arrays in `formatSqlLiteral` need column-type cast.
    SP-2 P0.
15. Triggers fire during data replay → disable during data section.
    SP-2 P0.
16. `lockCheck` warn about `ADD COLUMN NOT NULL no default` failing
    on existing rows. SP-5 P0.

## Tier 3: P1 features + selected P2

17. Auto-include dependencies for single-object transfer. SP-3 P0.
18. `detectMigrationState` semver-aware version sort. SP-5 P2.
19. `describeTable` surface GENERATED + IDENTITY column flags +
    column comments. SP-4 P1.
20. Matview REFRESH after replay. SP-2 P1.
21. Matview UNIQUE index bundled with matview. SP-2 P1.
22. `COMMENT ON COLUMN` export. SP-2 P1, SP-4 P1.
23. More DDL variants in `lockCheck`. SP-5 P1.
24. Validate `index_type` allowlist in `safe_alter_table`. SP-5 P1.

## Tier 4: defer to next iteration

- Streaming COPY transfer (SP-3 P2)
- TABLESAMPLE threshold (SP-6 P2)
- describeOid CTE rewrite (SP-4 P2)
- Cross-pack auto-deps semantics

