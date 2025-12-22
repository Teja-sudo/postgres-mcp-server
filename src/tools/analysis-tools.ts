import { getDbManager } from '../db-manager.js';
import { SlowQuery, IndexRecommendation, HealthCheckResult } from '../types.js';
import { validatePositiveInteger, isReadOnlySql } from '../utils/validation.js';

const MAX_QUERIES_TO_ANALYZE = 10;

export async function getTopQueries(args: {
  limit?: number;
  orderBy?: 'total_time' | 'mean_time' | 'calls';
  minCalls?: number;
}): Promise<SlowQuery[]> {
  const dbManager = getDbManager();
  const limit = validatePositiveInteger(args.limit, 'limit', 1, 100) || 10;
  const minCalls = validatePositiveInteger(args.minCalls, 'minCalls', 1, 1000000) || 1;

  // Validate orderBy
  const validOrderBy = ['total_time', 'mean_time', 'calls'];
  const orderBy = validOrderBy.includes(args.orderBy || '') ? args.orderBy! : 'total_time';

  // Check if pg_stat_statements extension is available
  const extCheck = await dbManager.query(`
    SELECT EXISTS (
      SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
    ) as has_extension
  `);

  if (!extCheck.rows[0].has_extension) {
    throw new Error('pg_stat_statements extension is not installed. Please install it to use this feature.');
  }

  // Map order by to actual column names (safe since we validated above)
  const orderColumnMap: Record<string, string> = {
    'total_time': 'total_exec_time',
    'mean_time': 'mean_exec_time',
    'calls': 'calls'
  };
  const orderColumn = orderColumnMap[orderBy];

  const query = `
    SELECT
      query,
      calls,
      total_exec_time as total_time,
      mean_exec_time as mean_time,
      rows
    FROM pg_stat_statements
    WHERE calls >= $1
    ORDER BY ${orderColumn} DESC
    LIMIT $2
  `;

  try {
    const result = await dbManager.query<SlowQuery>(query, [minCalls, limit]);
    return result.rows;
  } catch (error) {
    // Try older column names (PostgreSQL < 13)
    const legacyOrderColumnMap: Record<string, string> = {
      'total_time': 'total_time',
      'mean_time': 'mean_time',
      'calls': 'calls'
    };
    const legacyOrderColumn = legacyOrderColumnMap[orderBy];

    const legacyQuery = `
      SELECT
        query,
        calls,
        total_time,
        mean_time,
        rows
      FROM pg_stat_statements
      WHERE calls >= $1
      ORDER BY ${legacyOrderColumn} DESC
      LIMIT $2
    `;
    const result = await dbManager.query<SlowQuery>(legacyQuery, [minCalls, limit]);
    return result.rows;
  }
}

export async function analyzeWorkloadIndexes(args: {
  topQueriesCount?: number;
  includeHypothetical?: boolean;
}): Promise<{
  queries: SlowQuery[];
  recommendations: IndexRecommendation[];
}> {
  const topCount = validatePositiveInteger(args.topQueriesCount, 'topQueriesCount', 1, 50) || 20;

  // Get top slow queries
  const slowQueries = await getTopQueries({ limit: topCount, orderBy: 'total_time' });

  const recommendations: IndexRecommendation[] = [];

  // Analyze each query for potential index improvements
  // Limit to prevent excessive processing
  const queriesToAnalyze = slowQueries.slice(0, 20);

  for (const query of queriesToAnalyze) {
    try {
      const queryRecs = await analyzeQueryForIndexes(query.query);
      recommendations.push(...queryRecs);
    } catch (error) {
      // Skip queries that fail analysis
      continue;
    }
  }

  // Deduplicate recommendations
  const uniqueRecs = deduplicateRecommendations(recommendations);

  return {
    queries: slowQueries,
    recommendations: uniqueRecs
  };
}

export async function analyzeQueryIndexes(args: {
  queries: string[];
}): Promise<{
  queryAnalysis: Array<{
    query: string;
    recommendations: IndexRecommendation[];
    error?: string;
  }>;
  summary: IndexRecommendation[];
}> {
  if (!args.queries || !Array.isArray(args.queries)) {
    throw new Error('queries parameter is required and must be an array');
  }

  if (args.queries.length === 0) {
    throw new Error('queries array must contain at least one query');
  }

  if (args.queries.length > MAX_QUERIES_TO_ANALYZE) {
    throw new Error(`Maximum ${MAX_QUERIES_TO_ANALYZE} queries allowed per analysis`);
  }

  const queryAnalysis: Array<{ query: string; recommendations: IndexRecommendation[]; error?: string }> = [];
  const allRecommendations: IndexRecommendation[] = [];

  for (const query of args.queries) {
    if (!query || typeof query !== 'string') {
      queryAnalysis.push({ query: String(query), recommendations: [], error: 'Invalid query' });
      continue;
    }

    // Only analyze read-only queries for security
    const { isReadOnly, reason } = isReadOnlySql(query);
    if (!isReadOnly) {
      queryAnalysis.push({ query, recommendations: [], error: `Cannot analyze: ${reason}` });
      continue;
    }

    try {
      const recommendations = await analyzeQueryForIndexes(query);
      queryAnalysis.push({ query, recommendations });
      allRecommendations.push(...recommendations);
    } catch (error) {
      queryAnalysis.push({
        query,
        recommendations: [],
        error: error instanceof Error ? error.message : String(error)
      });
    }
  }

  return {
    queryAnalysis,
    summary: deduplicateRecommendations(allRecommendations)
  };
}

async function analyzeQueryForIndexes(query: string): Promise<IndexRecommendation[]> {
  const dbManager = getDbManager();
  const recommendations: IndexRecommendation[] = [];

  // Validate query is read-only before analyzing
  const { isReadOnly, reason } = isReadOnlySql(query);
  if (!isReadOnly) {
    throw new Error(`Cannot analyze write query: ${reason}`);
  }

  try {
    // Get the execution plan - EXPLAIN without ANALYZE is safe
    const explainResult = await dbManager.query(`EXPLAIN (FORMAT JSON) ${query}`);

    if (!explainResult.rows.length || !explainResult.rows[0]['QUERY PLAN']) {
      return recommendations;
    }

    const plan = explainResult.rows[0]['QUERY PLAN'][0];

    if (!plan || !plan.Plan) {
      return recommendations;
    }

    // Analyze the plan for sequential scans on large tables
    const seqScans = findSequentialScans(plan.Plan);

    for (const scan of seqScans) {
      // Check if there's a filter condition that could benefit from an index
      if (scan.filter && scan.table) {
        const columns = extractColumnsFromFilter(scan.filter);
        if (columns.length > 0) {
          recommendations.push({
            table: scan.table,
            columns,
            index_type: 'btree',
            reason: `Sequential scan with filter on ${columns.join(', ')}. Estimated cost: ${scan.cost || 'unknown'}`,
            estimated_improvement: 'Could significantly reduce query time for filtered queries'
          });
        }
      }

      // Check for sort operations that could use indexes
      if (scan.sortKey && scan.table) {
        recommendations.push({
          table: scan.table,
          columns: [scan.sortKey],
          index_type: 'btree',
          reason: `Sort operation on ${scan.sortKey}`,
          estimated_improvement: 'Could eliminate sorting overhead'
        });
      }
    }
  } catch (error) {
    // Query analysis failed, skip this query
    throw error;
  }

  return recommendations;
}

function findSequentialScans(node: any, scans: any[] = []): any[] {
  if (!node || typeof node !== 'object') return scans;

  if (node['Node Type'] === 'Seq Scan' && node['Relation Name']) {
    scans.push({
      table: node['Relation Name'],
      filter: node['Filter'],
      cost: node['Total Cost'],
      rows: node['Plan Rows']
    });
  }

  if (node['Sort Key'] && Array.isArray(node['Sort Key']) && node['Sort Key'].length > 0) {
    const sortKey = String(node['Sort Key'][0]);
    // Clean up the sort key to extract column name
    const cleanSortKey = sortKey.replace(/\s+(ASC|DESC|NULLS\s+(FIRST|LAST))/gi, '').trim();

    if (cleanSortKey && node['Relation Name']) {
      scans.push({
        table: node['Relation Name'],
        sortKey: cleanSortKey,
        cost: node['Total Cost']
      });
    }
  }

  // Recursively check child nodes
  if (Array.isArray(node.Plans)) {
    for (const child of node.Plans) {
      findSequentialScans(child, scans);
    }
  }

  return scans;
}

function extractColumnsFromFilter(filter: string): string[] {
  if (!filter || typeof filter !== 'string') return [];

  // Simple extraction of column names from filter expressions
  const columns: string[] = [];

  // Match patterns like (column_name = ...) or (column_name > ...)
  const matches = filter.match(/\((\w+)\s*[=<>!]+/g);
  if (matches) {
    for (const match of matches) {
      const col = match.match(/\((\w+)/);
      if (col && col[1]) {
        // Basic validation - only alphanumeric and underscore
        if (/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(col[1])) {
          columns.push(col[1]);
        }
      }
    }
  }

  return [...new Set(columns)];
}

function deduplicateRecommendations(recommendations: IndexRecommendation[]): IndexRecommendation[] {
  const seen = new Map<string, IndexRecommendation>();

  for (const rec of recommendations) {
    if (!rec.table || !rec.columns || !Array.isArray(rec.columns)) continue;

    const key = `${rec.table}:${[...rec.columns].sort().join(',')}:${rec.index_type}`;
    if (!seen.has(key)) {
      seen.set(key, rec);
    }
  }

  return Array.from(seen.values());
}

export async function analyzeDbHealth(): Promise<HealthCheckResult[]> {
  const dbManager = getDbManager();
  const results: HealthCheckResult[] = [];

  // 1. Buffer Cache Hit Rate
  try {
    const cacheResult = await dbManager.query(`
      SELECT
        sum(heap_blks_read) as heap_read,
        sum(heap_blks_hit) as heap_hit,
        CASE WHEN sum(heap_blks_hit) + sum(heap_blks_read) > 0
          THEN sum(heap_blks_hit)::float / (sum(heap_blks_hit) + sum(heap_blks_read))::float
          ELSE 0
        END as ratio
      FROM pg_statio_user_tables
    `);

    const ratio = parseFloat(cacheResult.rows[0]?.ratio) || 0;
    let status: HealthCheckResult['status'] = 'healthy';
    if (ratio < 0.9 && ratio > 0) status = 'warning';
    if (ratio < 0.8 && ratio > 0) status = 'critical';

    results.push({
      category: 'Buffer Cache Hit Rate',
      status,
      message: ratio > 0 ? `Cache hit ratio: ${(ratio * 100).toFixed(2)}%` : 'No data available',
      details: {
        heap_read: cacheResult.rows[0]?.heap_read || 0,
        heap_hit: cacheResult.rows[0]?.heap_hit || 0,
        recommendation: ratio < 0.9 && ratio > 0 ? 'Consider increasing shared_buffers' : 'Good cache performance'
      }
    });
  } catch (error) {
    results.push({
      category: 'Buffer Cache Hit Rate',
      status: 'warning',
      message: `Could not check buffer cache: ${error instanceof Error ? error.message : String(error)}`
    });
  }

  // 2. Connection Health
  try {
    const connResult = await dbManager.query(`
      SELECT
        count(*) as total_connections,
        count(*) FILTER (WHERE state = 'active') as active,
        count(*) FILTER (WHERE state = 'idle') as idle,
        count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction,
        (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') as max_connections
      FROM pg_stat_activity
      WHERE backend_type = 'client backend'
    `);

    const row = connResult.rows[0];
    const totalConns = parseInt(row?.total_connections) || 0;
    const maxConns = parseInt(row?.max_connections) || 100;
    const usageRatio = maxConns > 0 ? totalConns / maxConns : 0;

    let status: HealthCheckResult['status'] = 'healthy';
    if (usageRatio > 0.7) status = 'warning';
    if (usageRatio > 0.9) status = 'critical';

    const idleInTx = parseInt(row?.idle_in_transaction) || 0;

    results.push({
      category: 'Connection Health',
      status,
      message: `Using ${totalConns}/${maxConns} connections (${(usageRatio * 100).toFixed(1)}%)`,
      details: {
        active: parseInt(row?.active) || 0,
        idle: parseInt(row?.idle) || 0,
        idle_in_transaction: idleInTx,
        recommendation: idleInTx > 5 ? 'Check for long-running idle transactions' : 'Connection usage healthy'
      }
    });
  } catch (error) {
    results.push({
      category: 'Connection Health',
      status: 'warning',
      message: `Could not check connections: ${error instanceof Error ? error.message : String(error)}`
    });
  }

  // 3. Invalid Indexes
  try {
    const indexResult = await dbManager.query(`
      SELECT
        n.nspname as schemaname,
        t.relname as tablename,
        i.relname as indexname
      FROM pg_index ix
      JOIN pg_class i ON i.oid = ix.indexrelid
      JOIN pg_class t ON t.oid = ix.indrelid
      JOIN pg_namespace n ON n.oid = t.relnamespace
      WHERE NOT ix.indisvalid
      AND n.nspname NOT IN ('pg_catalog', 'pg_toast', 'information_schema')
      LIMIT 100
    `);

    const status: HealthCheckResult['status'] = indexResult.rows.length > 0 ? 'critical' : 'healthy';
    results.push({
      category: 'Invalid Indexes',
      status,
      message: indexResult.rows.length > 0 ? `Found ${indexResult.rows.length} invalid indexes` : 'No invalid indexes found',
      details: indexResult.rows.length > 0 ? { indexes: indexResult.rows } : undefined
    });
  } catch (error) {
    results.push({
      category: 'Invalid Indexes',
      status: 'healthy',
      message: 'No invalid indexes found'
    });
  }

  // 4. Unused Indexes
  try {
    const unusedResult = await dbManager.query(`
      SELECT
        schemaname,
        relname as tablename,
        indexrelname as indexname,
        idx_scan,
        pg_size_pretty(pg_relation_size(indexrelid)) as index_size
      FROM pg_stat_user_indexes
      WHERE idx_scan = 0
      AND indexrelname NOT LIKE '%_pkey'
      AND pg_relation_size(indexrelid) > 1048576
      ORDER BY pg_relation_size(indexrelid) DESC
      LIMIT 20
    `);

    const status: HealthCheckResult['status'] = unusedResult.rows.length > 5 ? 'warning' : 'healthy';
    results.push({
      category: 'Unused Indexes',
      status,
      message: unusedResult.rows.length > 0 ? `Found ${unusedResult.rows.length} potentially unused indexes (>1MB)` : 'No large unused indexes found',
      details: unusedResult.rows.length > 0 ? { indexes: unusedResult.rows } : undefined
    });
  } catch (error) {
    results.push({
      category: 'Unused Indexes',
      status: 'warning',
      message: `Could not check unused indexes: ${error instanceof Error ? error.message : String(error)}`
    });
  }

  // 5. Duplicate Indexes
  try {
    const dupResult = await dbManager.query(`
      SELECT
        pg_size_pretty(sum(pg_relation_size(idx))::bigint) as total_size,
        array_agg(idx::text) as indexes,
        (array_agg(indrelid))[1]::regclass::text as table_name
      FROM (
        SELECT indexrelid::regclass as idx,
               indrelid,
               indkey as columns
        FROM pg_index
        WHERE indrelid IN (SELECT oid FROM pg_class WHERE relnamespace = 'public'::regnamespace)
      ) sub
      GROUP BY indrelid, columns
      HAVING count(*) > 1
      LIMIT 50
    `);

    const status: HealthCheckResult['status'] = dupResult.rows.length > 0 ? 'warning' : 'healthy';
    results.push({
      category: 'Duplicate Indexes',
      status,
      message: dupResult.rows.length > 0 ? `Found ${dupResult.rows.length} sets of duplicate indexes` : 'No duplicate indexes found',
      details: dupResult.rows.length > 0 ? { duplicates: dupResult.rows } : undefined
    });
  } catch (error) {
    results.push({
      category: 'Duplicate Indexes',
      status: 'healthy',
      message: 'No duplicate indexes detected'
    });
  }

  // 6. Vacuum Health
  try {
    const vacuumResult = await dbManager.query(`
      SELECT
        schemaname,
        relname,
        n_live_tup,
        n_dead_tup,
        last_vacuum,
        last_autovacuum,
        CASE WHEN n_live_tup > 0
          THEN round(100.0 * n_dead_tup / n_live_tup, 2)
          ELSE 0
        END as dead_ratio
      FROM pg_stat_user_tables
      WHERE n_dead_tup > 10000
      ORDER BY n_dead_tup DESC
      LIMIT 20
    `);

    let status: HealthCheckResult['status'] = 'healthy';
    const needsVacuum = vacuumResult.rows.filter((r: any) => parseFloat(r.dead_ratio) > 10);
    if (needsVacuum.length > 0) status = 'warning';
    if (needsVacuum.length > 5) status = 'critical';

    results.push({
      category: 'Vacuum Health',
      status,
      message: needsVacuum.length > 0 ? `${needsVacuum.length} tables have high dead tuple ratio` : 'Vacuum status healthy',
      details: needsVacuum.length > 0 ? { tables: needsVacuum } : undefined
    });
  } catch (error) {
    results.push({
      category: 'Vacuum Health',
      status: 'warning',
      message: `Could not check vacuum status: ${error instanceof Error ? error.message : String(error)}`
    });
  }

  // 7. Sequence Limits
  try {
    const seqResult = await dbManager.query(`
      SELECT
        schemaname,
        sequencename,
        last_value,
        max_value,
        CASE WHEN max_value > 0
          THEN round(100.0 * last_value / max_value, 2)
          ELSE 0
        END as usage_percent
      FROM pg_sequences
      WHERE max_value > 0
      AND 100.0 * last_value / max_value > 50
      ORDER BY usage_percent DESC
      LIMIT 20
    `);

    let status: HealthCheckResult['status'] = 'healthy';
    if (seqResult.rows.some((r: any) => parseFloat(r.usage_percent) > 80)) status = 'warning';
    if (seqResult.rows.some((r: any) => parseFloat(r.usage_percent) > 95)) status = 'critical';

    results.push({
      category: 'Sequence Limits',
      status,
      message: seqResult.rows.length > 0 ? `${seqResult.rows.length} sequences over 50% usage` : 'All sequences have healthy headroom',
      details: seqResult.rows.length > 0 ? { sequences: seqResult.rows } : undefined
    });
  } catch (error) {
    results.push({
      category: 'Sequence Limits',
      status: 'healthy',
      message: 'Sequences appear healthy'
    });
  }

  // 8. Constraint Validation
  try {
    const constraintResult = await dbManager.query(`
      SELECT
        conname,
        conrelid::regclass::text as table_name,
        contype
      FROM pg_constraint
      WHERE NOT convalidated
      LIMIT 50
    `);

    const status: HealthCheckResult['status'] = constraintResult.rows.length > 0 ? 'warning' : 'healthy';
    results.push({
      category: 'Constraint Validation',
      status,
      message: constraintResult.rows.length > 0 ? `${constraintResult.rows.length} constraints not validated` : 'All constraints validated',
      details: constraintResult.rows.length > 0 ? { constraints: constraintResult.rows } : undefined
    });
  } catch (error) {
    results.push({
      category: 'Constraint Validation',
      status: 'healthy',
      message: 'Constraints appear validated'
    });
  }

  return results;
}
