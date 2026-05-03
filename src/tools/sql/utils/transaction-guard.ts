/**
 * Transaction Guard
 *
 * Runtime sentinel for the dry-run / transaction wrappers. Installs a
 * named savepoint immediately after the outer BEGIN; verifies the
 * savepoint still exists after suspect statements and just before the
 * final ROLLBACK. If the savepoint disappears, the outer transaction
 * was closed mid-flight (e.g. by an embedded COMMIT that escaped the
 * static analysis layer in dry-run-utils.ts).
 *
 * This is layer 2 of a two-layer defense:
 *   layer 1 (static): dry-run-utils.ts NON_ROLLBACKABLE_PATTERNS skip
 *                     transaction-control statements at parse time.
 *   layer 2 (runtime): this module catches anything layer 1 missed
 *                      (DO blocks with COMMIT, EXECUTE 'COMMIT',
 *                      stored procedures that COMMIT internally).
 *
 * See docs/superpowers/specs/2026-05-03-sp1-dry-run-trust-restoration-design.md
 */

import { PoolClient } from 'pg';
import { v7 as uuidv7 } from 'uuid';

/**
 * Result of a transaction-state verification call.
 *  - ok: true → outer transaction still alive, sentinel re-armed.
 *  - ok: false → outer transaction was closed or replaced; the
 *                caller must abort and report dryRunCompromised.
 */
export type TxGuardResult =
  | { ok: true }
  | {
      ok: false;
      reason: 'tx_closed' | 'tx_diverged';
      pgCode?: string;
      pgMessage?: string;
    };

/**
 * Pattern used to decide whether a statement warrants verification.
 * Matches statement-level transaction control keywords plus constructs
 * that can dynamically issue them (DO blocks, EXECUTE strings, CALL
 * into procedures that COMMIT internally on PG 11+).
 */
const SUSPECT_PATTERN =
  /\b(COMMIT|ROLLBACK|END|BEGIN|START|SAVEPOINT|RELEASE|ABORT|DO|EXECUTE|CALL)\b/i;

/**
 * Strip line comments, block comments, single/double-quoted strings,
 * and dollar-quoted strings from SQL before regex matching. Avoids
 * false positives where SUSPECT_PATTERN keywords appear inside text
 * the SQL parser would not treat as SQL (e.g.
 * `INSERT INTO log VALUES ('COMMIT')`).
 *
 * Walks the same character-by-character state machine as
 * splitSqlStatementsWithLineNumbers. Whitespace replaces stripped
 * regions so word boundaries elsewhere in the SQL remain valid.
 */
export function stripCommentsAndStrings(sql: string): string {
  let result = '';
  let i = 0;
  let inLineComment = false;
  let blockCommentDepth = 0;
  let inString = false;
  let stringChar = '';

  while (i < sql.length) {
    const ch = sql[i];
    const next = sql[i + 1] || '';

    if (inLineComment) {
      if (ch === '\n' || ch === '\r') {
        inLineComment = false;
        result += ch;
      } else {
        result += ' ';
      }
      i++;
      continue;
    }

    if (blockCommentDepth > 0) {
      if (ch === '*' && next === '/') {
        blockCommentDepth--;
        result += '  ';
        i += 2;
        continue;
      }
      if (ch === '/' && next === '*') {
        blockCommentDepth++;
        result += '  ';
        i += 2;
        continue;
      }
      result += ch === '\n' ? '\n' : ' ';
      i++;
      continue;
    }

    if (inString) {
      if (ch === stringChar) {
        if (next === stringChar) {
          // Doubled quote = escaped, stays in string
          result += '  ';
          i += 2;
          continue;
        }
        inString = false;
        stringChar = '';
        result += ' ';
        i++;
        continue;
      }
      result += ch === '\n' ? '\n' : ' ';
      i++;
      continue;
    }

    // Not currently in any quoted/comment context

    if (ch === '-' && next === '-') {
      inLineComment = true;
      result += '  ';
      i += 2;
      continue;
    }

    if (ch === '/' && next === '*') {
      blockCommentDepth = 1;
      result += '  ';
      i += 2;
      continue;
    }

    if (ch === "'" || ch === '"') {
      inString = true;
      stringChar = ch;
      result += ' ';
      i++;
      continue;
    }

    if (ch === '$') {
      // Detect dollar-quoted string $tag$ ... $tag$
      const dollarMatch = /^(\$\w*\$)/.exec(sql.slice(i));
      if (dollarMatch) {
        const tag = dollarMatch[1];
        const searchStart = i + tag.length;
        const endIdx = sql.indexOf(tag, searchStart);
        if (endIdx !== -1) {
          // Replace dollar-quoted body with whitespace, preserving newlines
          const body = sql.slice(i, endIdx + tag.length);
          for (const c of body) {
            result += c === '\n' ? '\n' : ' ';
          }
          i = endIdx + tag.length;
          continue;
        }
        // Unclosed dollar-quote - consume to end as if it were a string
        for (let j = i; j < sql.length; j++) {
          result += sql[j] === '\n' ? '\n' : ' ';
        }
        i = sql.length;
        continue;
      }
    }

    result += ch;
    i++;
  }

  return result;
}

/**
 * Sentinel that protects an outer dry-run transaction from being
 * silently closed by transaction-control statements inside user-
 * supplied SQL. One TransactionGuard per outer transaction.
 */
export class TransactionGuard {
  /**
   * The savepoint name installed in the outer transaction. Built from
   * a UUID v7 to avoid colliding with any user SAVEPOINT statement.
   */
  readonly savepointName: string;

  constructor() {
    // 'psm_outer_' prefix + uuidv7 with dashes stripped → valid PG identifier.
    // PG identifiers max 63 chars; 'psm_outer_' (10) + 32 hex = 42. Safe.
    this.savepointName = 'psm_outer_' + uuidv7().replace(/-/g, '');
  }

  /**
   * Install the sentinel savepoint. Must be called immediately after the
   * outer BEGIN; the client must already be inside a transaction.
   */
  async arm(client: PoolClient): Promise<void> {
    await client.query(`SAVEPOINT ${this.savepointName}`);
  }

  /**
   * Decide whether the statement that just executed is likely to have
   * tampered with transaction state. Strips comments and strings first
   * so we don't false-fire on `INSERT INTO log VALUES ('COMMIT')` etc.
   */
  shouldVerifyAfter(sql: string): boolean {
    const stripped = stripCommentsAndStrings(sql);
    return SUSPECT_PATTERN.test(stripped);
  }

  /**
   * Verify the outer transaction's sentinel savepoint still exists, then
   * re-arm it (release-and-recreate cycle in a single round-trip).
   *
   * Three outcomes:
   *  - { ok: true }                  outer tx healthy; sentinel re-armed.
   *  - { ok: false, reason: 'tx_closed' }
   *                                  PG error 25P01 - we are not in any
   *                                  transaction (outer was closed by
   *                                  COMMIT/ROLLBACK).
   *  - { ok: false, reason: 'tx_diverged' }
   *                                  PG error 3B001 - savepoint does not
   *                                  exist (outer tx was closed and a
   *                                  new one started, or RELEASE removed
   *                                  the savepoint).
   *
   * Any other PG error is re-thrown for the caller to handle.
   */
  async verify(client: PoolClient): Promise<TxGuardResult> {
    try {
      // Single round-trip: release the sentinel and immediately recreate
      // it. If RELEASE fails, the outer tx is gone or a different one
      // is active.
      await client.query(
        `RELEASE SAVEPOINT ${this.savepointName}; SAVEPOINT ${this.savepointName}`
      );
      return { ok: true };
    } catch (err: unknown) {
      const pgErr = err as { code?: string; message?: string };
      // 3B001 invalid_savepoint_specification: savepoint vanished
      // 25P01 no_active_sql_transaction: outer tx was closed
      // 25P02 in_failed_sql_transaction: a previous statement aborted the
      //       tx (e.g. a DO block with COMMIT that PG refuses mid-tx).
      //       We treat this as tx_closed for our purposes - the dry-run
      //       can't trust subsequent queries on this connection.
      if (pgErr?.code === '3B001') {
        return {
          ok: false,
          reason: 'tx_diverged',
          pgCode: pgErr.code,
          pgMessage: pgErr.message,
        };
      }
      if (pgErr?.code === '25P01' || pgErr?.code === '25P02') {
        return {
          ok: false,
          reason: 'tx_closed',
          pgCode: pgErr.code,
          pgMessage: pgErr.message,
        };
      }
      // Unknown error - re-throw
      throw err;
    }
  }
}
