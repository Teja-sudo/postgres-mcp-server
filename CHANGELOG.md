# Changelog

All notable changes to `@tejasanik/postgres-mcp-server` are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/), versioning
follows [Semantic Versioning](https://semver.org/).

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
