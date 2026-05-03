# postgres-mcp-server v3.0 program checklist

The seven sub-projects below decompose the v3 toolkit into independently
shippable releases. Each line is ticked at three stages: spec approved (S),
implementation merged (I), released to npm (R). Nothing in this list ships
without all three ticks.

## SP-1 — dry-run trust restoration (v2.3.1)
- [x] (S/I) `dry_run_sql_file` outer-tx leak fixed
- [x] (S/I) `mutationDryRun` outer-tx leak fixed
- [x] (S/I) `executeSqlFile(useTransaction=true)` outer-tx leak fixed
- [x] (S/I) `executeMultipleStatements(transactionId)` outer-tx leak fixed
- [x] (S/I) testcontainers harness added
- [x] (S/I) `/tmp` Windows portability fixed in `sql-tools.test.ts`

## SP-2 — introspection module + export_to_sql_file (v2.4.0)
- [x] (S/I) shared `introspection/` module shipped
- [x] (S/I) `export_to_sql_file` tool shipped (append + overwrite modes)

## SP-3 — transfer_objects (v2.5.0)
- [x] (S/I) `transfer_objects` tool shipped (ddl/data/both, cross-server)

## SP-4 — schema awareness pack (v2.6.0)
- [x] (S/I) `describe_table` shipped
- [x] (S/I) `find_dependents` shipped
- [x] (S/I) `schema_diff` shipped

## SP-5 — migration safety pack (v2.7.0)
- [x] (S/I) `lock_check` shipped
- [x] (S/I) `detect_migration_state` shipped
- [x] (S/I) `safe_alter_table` shipped

## SP-6 — data understanding pack (v2.8.0)
- [x] (S/I) `column_profile` shipped
- [x] (S/I) `generate_seed_data` shipped

## SP-7 — operations & safety pack (v2.9.0)
- [x] (S/I) `find_blocking_queries` shipped
- [x] (S/I) `kill_query` shipped
- [x] (S/I) `query_budget` flag on `execute_sql` shipped

## v3.0.0 rollup
- [x] All SPs implemented (S/I across all 19 lines)
- [ ] Consolidated docs under `docs/tools/` (deferred — README still
      hosts per-tool docs; can split later if README grows further)
- [x] CHANGELOG migration notes finalized
- [ ] Tagged & published as `3.0.0` (release left to user — workflow
      auto-publishes on GitHub release creation)
