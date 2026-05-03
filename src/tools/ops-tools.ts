/**
 * SP-7 operations & safety pack
 *
 *   find_blocking_queries — friendly tree of blocker → blocked from
 *                            pg_stat_activity ⨝ pg_locks. Replaces a
 *                            gnarly join an AI agent struggles to write.
 *   kill_query             — pg_cancel_backend (soft) or
 *                            pg_terminate_backend (hard) with confirm
 *                            gate. Refused on readonly access mode.
 *
 * (query_budget is added as an optional flag on execute_sql, not a
 * separate tool — see executeSql in sql-tools.ts.)
 */

import { PoolClient } from 'pg';
import { getDbManager } from '../db-manager.js';
import { ConnectionOverride } from '../types.js';

// ============================================================
// find_blocking_queries
// ============================================================

export interface FindBlockingQueriesArgs {
  /** Include idle-in-transaction sessions (default: true). */
  include_idle: boolean;
  /** Maximum queries to return per side. Default 50. */
  limit?: number;
  server?: string;
  database?: string;
  schema?: string;
}

export interface BlockingSession {
  pid: number;
  user: string;
  database: string;
  applicationName?: string;
  state: string;
  query: string;
  /** Time spent in current state, milliseconds. */
  stateAgeMs: number;
  /** Wait event (e.g. "Lock", "BufferPin", null if not waiting). */
  waitEvent?: string;
  waitEventType?: string;
}

export interface FindBlockingQueriesResult {
  blockers: BlockingSession[];
  blockedBy: Record<number, number[]>;
  /** Tree-structured view: each blocker with its blocked sessions nested. */
  tree: Array<{ blocker: BlockingSession; blocked: BlockingSession[] }>;
  totalBlockers: number;
  totalBlocked: number;
}

export async function findBlockingQueries(
  args: FindBlockingQueriesArgs = { include_idle: true }
): Promise<FindBlockingQueriesResult> {
  const dbManager = getDbManager();
  const hasOverride = args.server || args.database || args.schema;
  const override: ConnectionOverride | undefined = hasOverride
    ? { server: args.server, database: args.database, schema: args.schema }
    : undefined;
  const { client, release } = await acquireClient(dbManager, override);

  try {
    const limit = args.limit ?? 50;

    // pg_blocking_pids() returns the PIDs blocking a given PID.
    // Available since PG 9.6 - safe baseline for our supported versions.
    const r = await client.query(
      `SELECT a.pid, a.usename AS "user", a.datname AS database,
              a.application_name, a.state,
              COALESCE(a.query, '') AS query,
              EXTRACT(EPOCH FROM (now() - a.state_change)) * 1000 AS state_age_ms,
              a.wait_event, a.wait_event_type,
              pg_blocking_pids(a.pid) AS blocking_pids
       FROM pg_stat_activity a
       WHERE a.backend_type = 'client backend'
         AND a.pid <> pg_backend_pid()
         ${args.include_idle === false ? "AND a.state <> 'idle'" : ''}
       ORDER BY a.state_change
       LIMIT $1`,
      [limit]
    );

    const sessionsByPid = new Map<number, BlockingSession>();
    const blockedByMap: Record<number, number[]> = {};

    for (const row of r.rows) {
      const session: BlockingSession = {
        pid: Number(row.pid),
        user: row.user,
        database: row.database,
        applicationName: row.application_name ?? undefined,
        state: row.state,
        query: row.query,
        stateAgeMs: Math.round(Number(row.state_age_ms) || 0),
        waitEvent: row.wait_event ?? undefined,
        waitEventType: row.wait_event_type ?? undefined,
      };
      sessionsByPid.set(session.pid, session);
      const blockingPids = (row.blocking_pids as number[] | null) ?? [];
      if (blockingPids.length > 0) {
        blockedByMap[session.pid] = blockingPids.map(Number);
      }
    }

    // Build blocker → blocked tree
    const blockerPids = new Set<number>();
    for (const blockers of Object.values(blockedByMap)) {
      for (const b of blockers) blockerPids.add(b);
    }

    const tree: FindBlockingQueriesResult['tree'] = [];
    for (const blockerPid of blockerPids) {
      const blocker = sessionsByPid.get(blockerPid);
      if (!blocker) continue;
      const blocked: BlockingSession[] = [];
      for (const [blockedPidStr, blockers] of Object.entries(blockedByMap)) {
        if (blockers.includes(blockerPid)) {
          const b = sessionsByPid.get(Number(blockedPidStr));
          if (b) blocked.push(b);
        }
      }
      tree.push({ blocker, blocked });
    }

    return {
      blockers: Array.from(sessionsByPid.values()).filter((s) => blockerPids.has(s.pid)),
      blockedBy: blockedByMap,
      tree,
      totalBlockers: tree.length,
      totalBlocked: Object.keys(blockedByMap).length,
    };
  } finally {
    release();
  }
}

// ============================================================
// kill_query
// ============================================================

export interface KillQueryArgs {
  pid: number;
  /** 'cancel' = soft (pg_cancel_backend); 'terminate' = hard. */
  mode: 'cancel' | 'terminate';
  /** Required confirmation. Acts as foot-gun guard. */
  confirm: boolean;
  server?: string;
  database?: string;
  schema?: string;
}

export interface KillQueryResult {
  pid: number;
  mode: 'cancel' | 'terminate';
  signaled: boolean;
  /** Pre-kill snapshot of the target session. */
  target?: {
    user: string;
    database: string;
    state: string;
    query: string;
    stateAgeMs: number;
  };
  message: string;
}

export async function killQuery(args: KillQueryArgs): Promise<KillQueryResult> {
  if (!args.pid) throw new Error('pid is required');
  if (!['cancel', 'terminate'].includes(args.mode)) {
    throw new Error("mode must be 'cancel' or 'terminate'");
  }
  if (args.confirm !== true) {
    throw new Error(
      'confirm must be set to true. kill_query requires explicit confirmation. ' +
      "Pass { pid, mode, confirm: true }."
    );
  }

  const dbManager = getDbManager();
  const hasOverride = args.server || args.database || args.schema;

  // Refuse on readonly access mode (target server is the one we'd
  // signal, not the source - they can be the same for ops use).
  if (dbManager.isReadOnlyFor(args.server, args.database)) {
    throw new Error(
      `Server '${args.server ?? '(current)'}' is in readonly access mode. ` +
      `kill_query requires write access (it issues administrative function calls).`
    );
  }

  const override: ConnectionOverride | undefined = hasOverride
    ? { server: args.server, database: args.database, schema: args.schema }
    : undefined;
  const { client, release } = await acquireClient(dbManager, override);

  try {
    // Snapshot target session before killing
    let target: KillQueryResult['target'];
    try {
      const snap = await client.query(
        `SELECT usename AS "user", datname AS database, state,
                COALESCE(query, '') AS query,
                EXTRACT(EPOCH FROM (now() - state_change)) * 1000 AS state_age_ms
         FROM pg_stat_activity WHERE pid = $1`,
        [args.pid]
      );
      if (snap.rows[0]) {
        target = {
          user: snap.rows[0].user,
          database: snap.rows[0].database,
          state: snap.rows[0].state,
          query: snap.rows[0].query,
          stateAgeMs: Math.round(Number(snap.rows[0].state_age_ms) || 0),
        };
      }
    } catch {
      // ignore snapshot failure
    }

    const fn = args.mode === 'cancel' ? 'pg_cancel_backend' : 'pg_terminate_backend';
    const r = await client.query(`SELECT ${fn}($1) AS signaled`, [args.pid]);
    const signaled = r.rows[0]?.signaled === true;

    const message = signaled
      ? args.mode === 'cancel'
        ? `Sent cancellation signal to PID ${args.pid}. Query may take a moment to actually stop.`
        : `Sent termination signal to PID ${args.pid}. Backend will be killed.`
      : `Failed to ${args.mode} PID ${args.pid}. Process may have already exited or you lack permission.`;

    return { pid: args.pid, mode: args.mode, signaled, target, message };
  } finally {
    release();
  }
}

// ============================================================
// shared helper
// ============================================================

async function acquireClient(
  dbManager: ReturnType<typeof getDbManager>,
  override: ConnectionOverride | undefined
): Promise<{ client: PoolClient; release: () => void }> {
  if (override) {
    const r = await dbManager.getClientWithOverride(override);
    return { client: r.client, release: r.release };
  }
  const c = await dbManager.getClient();
  return { client: c, release: () => c.release() };
}
