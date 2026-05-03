# SP-1 — Dry-run trust restoration

| | |
|---|---|
| **Sub-project** | SP-1 of the v3 toolkit program |
| **Status** | Approved (design) |
| **Target release** | v2.3.1 (patch) |
| **Author** | Teja + Claude |
| **Date** | 2026-05-03 |

---

## 1. Why

### 1.1 The bug

The user ran `dry_run_sql_file` against a migration file
(`trim_entity_hierarchy_to_top_level.sql`) and observed that **all
migration changes were visible on the live database afterwards**, despite
the response containing `rolledBack: true`. The dry-run silently persisted
data.

### 1.2 Root cause

`src/tools/sql-tools.ts:dryRunSqlFile` wraps execution in a hand-rolled
transaction:

```
client.query('BEGIN');
for (stmt of statements) { client.query(stmt.sql); }
client.query('ROLLBACK');
```

If any statement is a transaction-control statement (`COMMIT`, `END`,
`ROLLBACK`, `SAVEPOINT`, `RELEASE`, `BEGIN`, `START TRANSACTION`,
`ABORT`, `ROLLBACK TO`), the outer transaction's lifecycle gets
hijacked:

- `COMMIT` / `END` mid-stream **persists** everything before it. PG
  returns to autocommit; subsequent statements run as auto-committed
  individual transactions and also persist.
- The final `ROLLBACK` runs against "no transaction in progress". PG
  emits a `WARNING`, not an error. The Node `pg` driver swallows the
  warning. Our code reports `rolledBack: true`.

The result: data persists, the API claims rollback, the user trusts
the response and moves on. **A silent correctness bug in a tool whose
single purpose is "show me what would happen without persisting."**

### 1.3 Same vulnerability lives in

| Tool | File:line | Mechanism |
|---|---|---|
| `mutationDryRun` | `sql-tools.ts:1498` | Same outer `BEGIN`/`ROLLBACK`. User-supplied SQL can contain `;COMMIT`. |
| `executeSqlFile` with `useTransaction=true` | `sql-tools.ts:678` | Same shape. Compounded by D4 below. |
| `executeMultipleStatements` invoked under `transactionId` | `sql-tools.ts:290` | The user's "outer" transaction is the one held by `dbManager.activeTransactions[transactionId]`. A `COMMIT` in any statement closes it; subsequent statements run autocommit; `commit_transaction` / `rollback_transaction` later fails. |

### 1.4 Why this matters more than a typical bug

Every other tool in this server is built on top of these dry-run /
transaction primitives. AI agents pattern on `dry_run_sql_file` to
de-risk migrations. If dry-run lies, so does every "let me preview
this for you" workflow downstream. **Trust in the entire toolkit
hinges on this being airtight.**

---

## 2. What

### 2.1 Goal

Make `dry_run_sql_file`, `mutationDryRun`,
`executeSqlFile(useTransaction=true)`, and
`executeMultipleStatements(transactionId)` **provably never persist
changes when the response indicates rollback**.

### 2.2 Public API changes (all additive — no breaking changes)

#### Result-shape additions

```typescript
// src/types.ts
interface SqlFileDryRunResult {
  // ...existing fields unchanged...

  /** True if a transaction-control statement closed our outer transaction
   *  during execution. When true, some changes may have persisted despite
   *  rolledBack: true. compromisedAt indicates where it happened. */
  dryRunCompromised?: boolean;
  compromisedAt?: {
    statementIndex: number;
    lineNumber: number;
    reason: 'tx_closed' | 'tx_diverged';
  };
}

interface MutationDryRunResult {
  // ...existing fields unchanged...
  dryRunCompromised?: boolean;
  compromisedAt?: { reason: 'tx_closed' | 'tx_diverged' };
}

type NonRollbackableWarning['operation'] =
  | ...existing variants...
  | 'TRANSACTION_CONTROL';   // NEW
```

#### Validation added to `executeSqlFile`

`useTransaction=true` combined with `stopOnError=false` is now refused
at validation time with:

> `useTransaction=true` and `stopOnError=false` cannot be combined.
> PostgreSQL aborts the entire transaction on the first error,
> ignoring subsequent statements. Use `useTransaction=false` for
> per-statement isolation, or `stopOnError=true` to roll back on
> first failure.

This combination was always broken (it produced one real error
followed by `current transaction is aborted, commands ignored until
end of transaction block` for every subsequent statement). Refusing
it explicitly is more honest than continuing to ship the broken
behavior.

### 2.3 No tools added or removed

SP-1 is purely a correctness fix. New tools start in SP-2.

---

## 3. How

### 3.1 Two-layer defense

```
        ┌─────────────────────────┐
        │ user-supplied SQL file  │
        └────────────┬────────────┘
                     │
                     ▼
   ┌──────────────────────────────────────┐
   │ LAYER 1: static analysis             │
   │ dry-run-utils.ts                     │
   │                                      │
   │ Match transaction-control patterns,  │
   │ tag with mustSkip:true               │
   └────────────┬─────────────────────────┘
                │
                │ (statements with mustSkip=true → not executed,
                │  emitted as nonRollbackableWarnings)
                ▼
   ┌──────────────────────────────────────┐
   │ LAYER 2: runtime sentinel            │
   │ transaction-guard.ts (NEW)           │
   │                                      │
   │ SAVEPOINT psm_outer_<uuid> after     │
   │ outer BEGIN. After "suspect"         │
   │ statements + always before final     │
   │ ROLLBACK, run                        │
   │ RELEASE+SAVEPOINT cycle. If RELEASE  │
   │ errors → outer tx is gone → abort.   │
   └──────────────────────────────────────┘
```

### 3.2 Design decisions (all approved)

#### D1 — Sentinel mechanism: savepoint-based

After outer `BEGIN`, run `SAVEPOINT psm_outer_<uuidv7>`. Verification
is `RELEASE SAVEPOINT psm_outer_<uuidv7>; SAVEPOINT psm_outer_<uuidv7>`
in one round-trip. If RELEASE errors with PG code `3B001`
(invalid_savepoint_specification) or `25P01`
(no_active_sql_transaction), the outer transaction was closed.

Why not `pg_current_xact_id_if_assigned()`: only works PG 13+; returns
NULL for read-only outer txs that haven't done writes yet
(false-negative). Savepoints work back to PG 7.x and have a clean
failure mode tied to specific PG error codes.

UUID v7 suffix collision-proofs the savepoint name against any
SAVEPOINT statement in the user's SQL (already very unlikely after
Layer 1 strips them).

#### D2 — When to verify: heuristic + always-at-end

Verify after a statement only if it matches the pattern
`\b(COMMIT|ROLLBACK|END|BEGIN|START|SAVEPOINT|RELEASE|ABORT|DO|EXECUTE|CALL)\b`
(after stripping comments and string literals — reuse existing
`stripLeadingComments` + the dollar-quote-aware splitter).

Plus an unconditional verification immediately before the final
`ROLLBACK`, so the rollback is provably operating on our outer
transaction.

Net cost on typical migrations: 0–3 extra round-trips. On worst-case
files where every statement is a DO block, ≤ N round-trips (same as
naïve "verify-every-statement"). Trade-off lands cleanly.

#### D3 — Test infrastructure: testcontainers

Add `testcontainers` (npm `testcontainers`, latest 11.14.0) as a
devDep. Spin up `postgres:16-alpine` for the integration suite.

- **Local**: opt-in via env var `POSTGRES_INTEGRATION_TESTS=1`.
  Without it, integration suite is `describe.skip(...)`.
- **CI**: `.github/workflows/npm-publish.yml` already runs on
  `ubuntu-latest`, which has Docker pre-installed. The workflow gets
  `POSTGRES_INTEGRATION_TESTS=1` set in the test step.
- Single PG version (16) for SP-1. Matrix testing across 13/14/15/16/17
  is deferred until a version-specific bug surfaces.

#### D4 — Refuse `useTransaction=true && stopOnError=false`

Validation error at the start of `executeSqlFile`. Documented in the
tool description. CHANGELOG explicitly notes this combination
previously appeared to work but actually didn't.

### 3.3 New module: `src/tools/sql/utils/transaction-guard.ts`

```typescript
import { PoolClient } from 'pg';
import { v7 as uuidv7 } from 'uuid';

/** Result of a transaction-state verification call. */
export type TxGuardResult =
  | { ok: true }
  | {
      ok: false;
      reason: 'tx_closed' | 'tx_diverged';
      pgCode?: string;
      pgMessage?: string;
    };

/** Pattern used to decide whether a statement warrants verification. */
const SUSPECT_PATTERN =
  /\b(COMMIT|ROLLBACK|END|BEGIN|START|SAVEPOINT|RELEASE|ABORT|DO|EXECUTE|CALL)\b/i;

export class TransactionGuard {
  readonly savepointName: string;

  constructor() {
    // 'psm_' prefix + uuidv7 dashes stripped → valid PG identifier
    this.savepointName = 'psm_outer_' + uuidv7().replace(/-/g, '');
  }

  /** Install the sentinel savepoint. Call immediately after outer BEGIN. */
  async arm(client: PoolClient): Promise<void> {
    await client.query(`SAVEPOINT ${this.savepointName}`);
  }

  /** Decide whether to run verify after a given statement. */
  shouldVerifyAfter(sql: string): boolean {
    // Strip comments and string literals to avoid false positives on
    // -- COMMIT or 'COMMIT' inside string literals.
    const stripped = stripCommentsAndStrings(sql);
    return SUSPECT_PATTERN.test(stripped);
  }

  /** Verify the outer tx still exists, then re-arm the savepoint. */
  async verify(client: PoolClient): Promise<TxGuardResult> {
    try {
      // Single round-trip: release-and-recreate
      await client.query(
        `RELEASE SAVEPOINT ${this.savepointName};
         SAVEPOINT ${this.savepointName}`
      );
      return { ok: true };
    } catch (err: any) {
      if (err?.code === '3B001') {
        return {
          ok: false,
          reason: 'tx_diverged',
          pgCode: err.code,
          pgMessage: err.message,
        };
      }
      if (err?.code === '25P01') {
        return {
          ok: false,
          reason: 'tx_closed',
          pgCode: err.code,
          pgMessage: err.message,
        };
      }
      // Some other error - re-throw so the outer handler sees it
      throw err;
    }
  }
}

/** Strip line comments, block comments, single/double-quoted strings,
 *  and dollar-quoted strings before regex matching. Built by reusing
 *  the existing parser primitives in src/tools/sql/utils/sql-parser.ts:
 *  the splitter already walks character-by-character with awareness of
 *  comment / string / dollar-quote state. We expose a small helper that
 *  walks the same state machine but emits a sanitized string instead
 *  of splitting at semicolons. No new parsing logic — pure refactor of
 *  existing internal logic into an exported helper. */
function stripCommentsAndStrings(sql: string): string;
```

### 3.4 Extension to `dry-run-utils.ts`

Add `'TRANSACTION_CONTROL'` to the operation union. Append eight
patterns to `NON_ROLLBACKABLE_PATTERNS`:

```typescript
{ pattern: /\bBEGIN\b/, operation: 'TRANSACTION_CONTROL', mustSkip: true,
  message: 'BEGIN inside a dry-run wrapper closes the outer transaction. Skipped.' },
{ pattern: /\bSTART\s+TRANSACTION\b/, operation: 'TRANSACTION_CONTROL', mustSkip: true,
  message: 'START TRANSACTION inside a dry-run wrapper closes the outer transaction. Skipped.' },
{ pattern: /\bCOMMIT\b/, operation: 'TRANSACTION_CONTROL', mustSkip: true,
  message: 'COMMIT inside a dry-run wrapper persists changes and closes the outer transaction. Skipped.' },
{ pattern: /\bROLLBACK\b(?!\s+TO)/, operation: 'TRANSACTION_CONTROL', mustSkip: true,
  message: 'ROLLBACK inside a dry-run wrapper closes the outer transaction. Skipped.' },
{ pattern: /\bABORT\b/, operation: 'TRANSACTION_CONTROL', mustSkip: true,
  message: 'ABORT (synonym for ROLLBACK) inside a dry-run wrapper closes the outer transaction. Skipped.' },
{ pattern: /\bSAVEPOINT\b/, operation: 'TRANSACTION_CONTROL', mustSkip: true,
  message: 'SAVEPOINT inside a dry-run wrapper interferes with the sentinel. Skipped.' },
{ pattern: /\bRELEASE\s+SAVEPOINT\b/, operation: 'TRANSACTION_CONTROL', mustSkip: true,
  message: 'RELEASE SAVEPOINT inside a dry-run wrapper interferes with the sentinel. Skipped.' },
{ pattern: /\bROLLBACK\s+TO\b/, operation: 'TRANSACTION_CONTROL', mustSkip: true,
  message: 'ROLLBACK TO inside a dry-run wrapper interferes with the sentinel. Skipped.' },
```

**Why `END` is intentionally NOT in the static patterns:** PG accepts
`END` as a synonym for `COMMIT` at the top level, but the bare keyword
appears in many other contexts (`CASE...END`, `END IF`, `END LOOP`,
plpgsql block terminators) where it is NOT transaction control.
Matching `\bEND\b` produces false positives that silently skip valid
SQL. Layer 2 (the runtime sentinel) catches a top-level `END;` if it
ever fires — the savepoint disappears just like with COMMIT — so
nothing is lost by omitting it from Layer 1.

The eight remaining patterns are statement-leading keywords (`BEGIN`,
`COMMIT`, etc.) that don't ambiguously appear inside other constructs
the way `END` does. Inside dollar-quoted function bodies the existing
parser already keeps the whole `CREATE FUNCTION ... $$ ... $$` as
one statement, so a `COMMIT` inside a function body matches the
static pattern; this is a deliberate over-match (a function whose
body contains `COMMIT` would error at runtime anyway under PG's
transaction rules).

### 3.5 Call-site changes (4 primary sites + 2 supporting sites)

The four primary sites are the dry-run / transaction tools listed in
§1.3. Two supporting changes also land in `src/db-manager.ts` to
honor the `compromised` flag that §3.5.4 sets on transaction records.

#### 3.5.1 `sql-tools.ts:dryRunSqlFile`

Currently lines 1031–1129. Changes:

```typescript
// after acquiring client, before BEGIN:
const guard = new TransactionGuard();

// existing: await client.query('BEGIN');
await guard.arm(client);

for (let idx = 0; idx < executableStatements.length && !aborted; idx++) {
  const stmt = executableStatements[idx];
  // ...existing skip logic...

  // after each non-skipped statement that succeeds or fails:
  if (guard.shouldVerifyAfter(stmt.sql)) {
    const check = await guard.verify(client);
    if (!check.ok) {
      result.dryRunCompromised = true;
      result.compromisedAt = {
        statementIndex: idx + 1,
        lineNumber: stmt.lineNumber,
        reason: check.reason,
      };
      aborted = true;
      break;
    }
  }
}

// always verify before ROLLBACK
const finalCheck = await guard.verify(client);
if (!finalCheck.ok && !result.dryRunCompromised) {
  result.dryRunCompromised = true;
  result.compromisedAt = {
    statementIndex: -1,
    lineNumber: -1,
    reason: finalCheck.reason,
  };
}

// best-effort ROLLBACK (may no-op if tx closed)
try { await client.query('ROLLBACK'); } catch { /* ignored */ }
```

#### 3.5.2 `sql-tools.ts:mutationDryRun`

Currently lines 1618–1697. Changes: same pattern, but the single
user-supplied statement is always treated as suspect (no point
running the heuristic for one statement we don't trust).

#### 3.5.3 `sql-tools.ts:executeSqlFile`

Currently lines 553–774. Two changes:

1. At the top of the function, after argument validation:

```typescript
if (args.useTransaction !== false && args.stopOnError === false) {
  throw new Error(
    "useTransaction=true and stopOnError=false cannot be combined. " +
    "PostgreSQL aborts the entire transaction on the first error, " +
    "ignoring subsequent statements. Use useTransaction=false for " +
    "per-statement isolation, or stopOnError=true to roll back on " +
    "first failure."
  );
}
```

2. When `useTransaction` is true, install the guard around the loop
   identically to dryRunSqlFile, but on guard failure return:

```typescript
return {
  success: false,
  filePath,
  fileSize,
  totalStatements,
  statementsExecuted,
  statementsFailed,
  executionTimeMs,
  rowsAffected: totalRowsAffected,
  error: 'Transaction-control statement detected — outer transaction was closed. ' +
         `See line ${guard.compromisedAt.lineNumber}.`,
  rollback: false,  // we cannot guarantee rollback at this point
  errors: collectedErrors,
};
```

#### 3.5.4 `sql-tools.ts:executeMultipleStatements` (transactionId branch)

Currently lines 290–320. The user's transaction is held by
`dbManager.activeTransactions[transactionId].client`. We arm a guard
on that client immediately. Failure puts the transaction into a
"compromised" state — `commit_transaction` and `rollback_transaction`
on the same `transactionId` are still callable but operate on a
closed/different tx. We need to:

```typescript
// On guard failure, mark the transaction record as compromised:
const txRecord = dbManager.activeTransactions.get(transactionId);
if (txRecord) {
  (txRecord.info as TransactionInfo & { compromised: boolean }).compromised = true;
}

// Subsequent commitTransaction / rollbackTransaction calls check this flag
// and return: { status: 'compromised', message: '...' } instead of pretending
// to commit/rollback.
```

This is a tiny addition to `db-manager.ts:commitTransaction` and
`rollbackTransaction`: if the info record carries `compromised: true`,
return immediately with a status of `'compromised'` and a clear
message. Don't issue any SQL.

### 3.6 Side fixes folded into SP-1

#### 3.6.1 `/tmp` Windows portability

`src/__tests__/sql-tools.test.ts` lines 412, 685, 1259 use
`fs.mkdtempSync('/tmp/postgres-mcp-*-')`. Replace with
`fs.mkdtempSync(path.join(os.tmpdir(), 'postgres-mcp-*-'))`.
Tests then pass on Windows + Linux + macOS without additional
config.

#### 3.6.2 testcontainers harness

New file: `src/__tests__/integration/postgres-container.ts`.
Spins up `postgres:16-alpine` once per test suite via
`@testcontainers/postgresql`. Exposes `getTestPool()` and
`resetDatabase()` helpers. All integration test files start with:

```typescript
const integration = process.env.POSTGRES_INTEGRATION_TESTS === '1' ? describe : describe.skip;

integration('SP-1 dry-run trust', () => { /* ... */ });
```

#### 3.6.3 CI workflow update

`.github/workflows/npm-publish.yml` `build` job gains:

```yaml
- name: Run integration tests
  run: npm test
  env:
    POSTGRES_INTEGRATION_TESTS: '1'
```

(Existing `npm test` line is replaced or supplemented; testcontainers
auto-discovers Docker on `ubuntu-latest`.)

---

## 4. Test plan

### 4.1 Unit tests (no PG required)

`src/__tests__/sql-utils/transaction-guard.test.ts`

| Test | Asserts |
|---|---|
| `shouldVerifyAfter` returns true for `COMMIT;` | basic match |
| `shouldVerifyAfter` returns true for `DO $$ BEGIN COMMIT; END $$` | DO blocks flagged |
| `shouldVerifyAfter` returns true for `EXECUTE 'COMMIT'` | dynamic SQL flagged |
| `shouldVerifyAfter` returns true for `CALL my_proc()` | procedures flagged |
| `shouldVerifyAfter` returns false for `SELECT 1` | no false positive |
| `shouldVerifyAfter` returns false for `INSERT INTO t VALUES (1)` | normal DML |
| `shouldVerifyAfter` returns false for `-- COMMIT in comment` | strips comments |
| `shouldVerifyAfter` returns false for `INSERT INTO log VALUES ('COMMIT')` | strips strings |
| Constructor produces unique savepoint names across instances | no collision |
| Savepoint name is a valid PG identifier (letters/digits/underscore, starts with letter) | format |

`src/__tests__/sql-utils/dry-run-utils.test.ts` extensions

| Test | Asserts |
|---|---|
| `detectNonRollbackableOperations('COMMIT;')` flags TRANSACTION_CONTROL | static catch |
| Same for `BEGIN;`, `START TRANSACTION;`, `END;`, `ROLLBACK;`, `ABORT;`, `SAVEPOINT a;`, `RELEASE SAVEPOINT a;`, `ROLLBACK TO a;` | full coverage |
| All have `mustSkip: true` | enforcement |

### 4.2 Integration tests (testcontainers, opt-in)

`src/__tests__/integration/sp1-dry-run-trust.test.ts`

| Test | Setup | Assert |
|---|---|---|
| **Bug repro: `dry_run_sql_file` with embedded COMMIT does not persist** | SQL: `CREATE TABLE t(x int); INSERT INTO t VALUES (1); COMMIT; INSERT INTO t VALUES (2);` against a fresh DB with no pre-existing `t` | Static layer skips the COMMIT; both INSERTs run inside our outer tx; final ROLLBACK undoes the CREATE TABLE and both inserts. After the call: `SELECT to_regclass('public.t')` returns NULL (table does not exist). |
| **Static skip surfaces TRANSACTION_CONTROL warning** | Same SQL | `result.nonRollbackableWarnings` contains entry with `operation: 'TRANSACTION_CONTROL'` and the COMMIT line |
| **Runtime sentinel catches DO block COMMIT (PG 11+)** | SQL: `DO $$ BEGIN COMMIT; END $$;` | `result.dryRunCompromised === true`, `compromisedAt.reason === 'tx_closed'` |
| **mutationDryRun with semicolon-injected COMMIT does not persist** | `INSERT INTO t (x) VALUES (1); COMMIT; INSERT INTO t (x) VALUES (2)` (must use `allowMultipleStatements`-equivalent path) | Either refused at validation or no rows persist after call |
| **executeSqlFile(useTransaction=true) with embedded COMMIT** | Same as bug repro | No rows persist; result reports `success: false`, `error` mentions transaction-control statement detected |
| **executeSqlFile refuses (useTransaction=true, stopOnError=false)** | Both flags set | Throws validation error; no SQL executed |
| **executeSqlFile(useTransaction=false) — embedded COMMIT works as before** | Same SQL | Rows persist (this is the autocommit / per-statement mode; user opted in) |
| **Transaction with transactionId: COMMIT inside execute_sql** | `begin_transaction` → `execute_sql` with COMMIT in SQL | Transaction marked compromised; subsequent `commit_transaction` returns `status: 'compromised'` instead of pretending success |
| **Heuristic does not over-fire on plain DML** | 100 INSERTs, no transaction control | Zero verify round-trips fired (instrument the guard with a counter) |
| **Always-at-end verification fires once** | Any successful run | Counter ends at exactly 1 + (number of suspect stmts) |

### 4.3 Existing tests

All 603 currently-passing unit tests must continue to pass. The 56
Windows-failing tests will now pass on Windows after the `/tmp` fix.

### 4.4 Performance budget

Integration test asserts: a 100-statement migration with no
transaction control runs in ≤ 1.1× the time it took before SP-1
(measured against testcontainers PG, three runs averaged). Heuristic
must not introduce material overhead on the common case.

---

## 5. Out of scope

- Auto-savepointing every statement in `executeSqlFile(useTransaction=true)`
  to make `stopOnError=false` work cleanly. (Tracked separately;
  candidate for a future SP if user demand surfaces.)
- PG version matrix testing (13/14/15/17). Single PG 16 in CI is
  sufficient until a version-specific bug appears.
- Rewriting plpgsql DO blocks to remove embedded COMMIT/ROLLBACK
  before execution. Not feasible without a real plpgsql parser; the
  runtime sentinel catches this anyway.
- Detecting transaction control inside `pg_exec()`/`dblink_exec()`
  calls. Out of scope. Layer 2 catches dblink but only after the
  damage is done.
- Per-savepoint nesting strategies (running each statement under its
  own savepoint so individual statement errors are isolated). Belongs
  to the deferred follow-up above.

---

## 6. Release & migration

### 6.1 Versioning

v2.3.1 (patch). Justification:

- **Result-shape additions are purely additive.** New optional fields
  (`dryRunCompromised`, `compromisedAt`) and a new
  `NonRollbackableWarning.operation` variant. Existing consumers that
  ignore unknown fields are unaffected; consumers that exhaustively
  switch on the operation union should treat new variants as warnings
  to surface, which is the spec's intent.
- **The `executeSqlFile` validation refusal IS a behavior change**,
  but the prior behavior was demonstrably broken: PG aborts the
  transaction on first error and ignores subsequent statements,
  producing one real error followed by N spurious "current transaction
  is aborted" errors. No real workflow could rely on it doing
  something useful. Surfacing the limitation explicitly with a clear
  error message is a fix, not a removal.
- **The dry-run trust fix IS a behavior change**: previously persisted
  changes will now be correctly rolled back. That's the bug fix.
  Consumers who (incorrectly) depended on the bug to commit changes
  should switch to `executeSqlFile(useTransaction=false)` or explicit
  `commit_transaction`.

CHANGELOG (§6.2) calls these out clearly so users grepping the
release notes find them.

### 6.2 CHANGELOG (text to ship)

```
### Fixed

- **dry_run_sql_file, mutationDryRun, executeSqlFile(useTransaction=true),
  and execute_sql with transactionId no longer silently persist changes
  when the input SQL contains transaction-control statements (COMMIT,
  ROLLBACK, BEGIN, etc).** Previously, an embedded COMMIT would close
  our outer transaction, persist everything before it, and leave the
  final ROLLBACK to run against "no transaction in progress". The result
  was misleading: tools claimed `rolledBack: true` while changes were
  visible on the live database.

  A new two-layer defense detects transaction-control statements at parse
  time (skipping them with `nonRollbackableWarnings.operation:
  "TRANSACTION_CONTROL"`) and verifies the outer transaction's integrity
  via a sentinel savepoint after each suspect statement. When the
  sentinel detects the outer transaction was closed, results now include
  `dryRunCompromised: true` with `compromisedAt` indicating where it
  happened.

### Changed

- `executeSqlFile` now refuses the combination
  `useTransaction=true, stopOnError=false`. PostgreSQL aborts the
  whole transaction on first error, so "continue with remaining
  statements" never actually worked under transaction mode. Callers
  must pick: `useTransaction=false` for per-statement isolation, or
  `stopOnError=true` for atomic-or-rollback.

### Added

- testcontainers-based integration tests (opt-in via
  `POSTGRES_INTEGRATION_TESTS=1` locally; always on in CI).
- Cross-platform fix for tests using hardcoded `/tmp/` paths on
  Windows.
```

### 6.3 Rollout plan

1. Land SP-1 changes on `main` via PR
2. CI runs full unit + integration suite
3. Tag `v2.3.1`
4. `npm publish` (existing OIDC-authenticated workflow)
5. Update master checklist (mark SP-1 boxes as I → R)

---

## 7. Definition of done

All of the following must be true before SP-1 is closed:

- [ ] `transaction-guard.ts` shipped with full unit-test coverage
- [ ] `dry-run-utils.ts` extended with `TRANSACTION_CONTROL` patterns
- [ ] All four call-sites updated; each has at least one integration
      test asserting "embedded COMMIT does not persist"
- [ ] `executeSqlFile` refuses the broken combination
- [ ] testcontainers harness exists; integration suite passes locally
      (with `POSTGRES_INTEGRATION_TESTS=1`) and in CI
- [ ] Existing 603 unit tests pass
- [ ] 56 previously-Windows-failing tests pass on Windows
- [ ] CHANGELOG updated
- [ ] v2.3.1 tagged and published to npm
- [ ] Master program checklist (SP-1 lines) updated

---

## 8. Open questions

None at design time. All four design decisions (D1–D4) are resolved
and approved.

---

## Appendix A — example: bug repro file

```sql
-- File: trim_entity_hierarchy_to_top_level.sql (paraphrased)
CREATE TABLE t (id int);
INSERT INTO t VALUES (1);
COMMIT;                       -- ← THIS LINE breaks the dry-run today
INSERT INTO t VALUES (2);
```

Today's behavior: file persists `(1)` and `(2)` to the live DB; tool
returns `rolledBack: true`.

After SP-1: COMMIT is matched by Layer 1 and skipped; both INSERTs
execute under the outer transaction; final ROLLBACK undoes them;
`nonRollbackableWarnings` contains an entry with
`operation: 'TRANSACTION_CONTROL'` and the line number of the COMMIT.
Live DB is unchanged. `dryRunCompromised` is false (Layer 1 caught
it; Layer 2 was never tripped).
