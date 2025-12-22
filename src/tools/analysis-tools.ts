import { getDbManager } from '../db-manager.js';
import { SlowQuery, IndexRecommendation, HealthCheckResult } from '../types.js';

export async function getTopQueries(args: {
  limit?: number;
  orderBy?: 'total_time' | 'mean_time' | 'calls';
  minCalls?: number;
}): Promise<SlowQuery[]> {
  const dbManager = getDbManager();
  const limit = args.limit || 10;
  const orderBy = args.orderBy || 'total_time';
  const minCalls = args.minCalls || 1;

  // Check if pg_stat_statements extension is available
  const extCheck = await dbManager.query(`
    SELECT EXISTS (
      SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
    ) as has_extension
  `);

  if (!extCheck.rows[0].has_extension) {
    throw new Error('pg_stat_statements extension is not installed. Please install it to use this feature.');
  }

  const orderColumn = orderBy === 'total_time' ? 'total_exec_time' :
                      orderBy === 'mean_time' ? 'mean_exec_time' : 'calls';

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
    const legacyQuery = `
      SELECT
        query,
        calls,
        total_time,
        mean_time,
        rows
      FROM pg_stat_statements
      WHERE calls >= $1
      ORDER BY ${orderBy === 'total_time' ? 'total_time' : orderBy === 'mean_time' ? 'mean_time' : 'calls'} DESC
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
  const dbManager = getDbManager();
  const topCount = args.topQueriesCount || 20;

  // Get top slow queries
  const slowQueries = await getTopQueries({ limit: topCount, orderBy: 'total_time' });

  const recommendations: IndexRecommendation[] = [];

  // Analyze each query for potential index improvements
  for (const query of slowQueries) {
    const queryRecs = await analyzeQueryForIndexes(query.query);
    recommendations.push(...queryRecs);
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
  }>;
  summary: IndexRecommendation[];
}> {
  if (args.queries.length > 10) {
    throw new Error('Maximum 10 queries allowed per analysis');
  }

  const queryAnalysis: Array<{ query: string; recommendations: IndexRecommendation[] }> = [];
  const allRecommendations: IndexRecommendation[] = [];

  for (const query of args.queries) {
    const recommendations = await analyzeQueryForIndexes(query);
    queryAnalysis.push({ query, recommendations });
    allRecommendations.push(...recommendations);
  }

  return {
    queryAnalysis,
    summary: deduplicateRecommendations(allRecommendations)
  };
}

async function analyzeQueryForIndexes(query: string): Promise<IndexRecommendation[]> {
  const dbManager = getDbManager();
  const recommendations: IndexRecommendation[] = [];

  try {
    // Get the execution plan
    const explainResult = await dbManager.query(`EXPLAIN (FORMAT JSON) ${query}`);
    const plan = explainResult.rows[0]['QUERY PLAN'][0];

    // Analyze the plan for sequential scans on large tables
    const seqScans = findSequentialScans(plan.Plan);

    for (const scan of seqScans) {
      // Check if there's a filter condition that could benefit from an index
      if (scan.filter) {
        const columns = extractColumnsFromFilter(scan.filter);
        if (columns.length > 0) {
          recommendations.push({
            table: scan.table,
            columns,
            index_type: 'btree',
            reason: `Sequential scan with filter on ${columns.join(', ')}. Query cost: ${scan.cost}`,
            estimated_improvement: 'Could significantly reduce query time for filtered queries'
          });
        }
      }

      // Check for sort operations that could use indexes
      if (scan.sortKey) {
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
  }

  return recommendations;
}

function findSequentialScans(node: any, scans: any[] = []): any[] {
  if (!node) return scans;

  if (node['Node Type'] === 'Seq Scan') {
    scans.push({
      table: node['Relation Name'],
      filter: node['Filter'],
      cost: node['Total Cost'],
      rows: node['Plan Rows']
    });
  }

  if (node['Sort Key']) {
    scans.push({
      table: node['Relation Name'] || 'unknown',
      sortKey: node['Sort Key'][0],
      cost: node['Total Cost']
    });
  }

  // Recursively check child nodes
  if (node.Plans) {
    for (const child of node.Plans) {
      findSequentialScans(child, scans);
    }
  }

  return scans;
}

function extractColumnsFromFilter(filter: string): string[] {
  // Simple extraction of column names from filter expressions
  const columns: string[] = [];
  const matches = filter.match(/\((\w+)\s*[=<>!]+/g);
  if (matches) {
    for (const match of matches) {
      const col = match.match(/\((\w+)/);
      if (col) {
        columns.push(col[1]);
      }
    }
  }
  return [...new Set(columns)];
}

function deduplicateRecommendations(recommendations: IndexRecommendation[]): IndexRecommendation[] {
  const seen = new Map<string, IndexRecommendation>();

  for (const rec of recommendations) {
    const key = `${rec.table}:${rec.columns.sort().join(',')}:${rec.index_type}`;
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
        sum(heap_blks_hit) / NULLIF(sum(heap_blks_hit) + sum(heap_blks_read), 0) as ratio
      FROM pg_statio_user_tables
    `);

    const ratio = parseFloat(cacheResult.rows[0].ratio) || 0;
    let status: HealthCheckResult['status'] = 'healthy';
    if (ratio < 0.9) status = 'warning';
    if (ratio < 0.8) status = 'critical';

    results.push({
      category: 'Buffer Cache Hit Rate',
      status,
      message: `Cache hit ratio: ${(ratio * 100).toFixed(2)}%`,
      details: {
        heap_read: cacheResult.rows[0].heap_read,
        heap_hit: cacheResult.rows[0].heap_hit,
        recommendation: ratio < 0.9 ? 'Consider increasing shared_buffers' : 'Good cache performance'
      }
    });
  } catch (error) {
    results.push({
      category: 'Buffer Cache Hit Rate',
      status: 'warning',
      message: `Could not check buffer cache: ${error}`
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
    const usageRatio = row.total_connections / row.max_connections;
    let status: HealthCheckResult['status'] = 'healthy';
    if (usageRatio > 0.7) status = 'warning';
    if (usageRatio > 0.9) status = 'critical';

    results.push({
      category: 'Connection Health',
      status,
      message: `Using ${row.total_connections}/${row.max_connections} connections (${(usageRatio * 100).toFixed(1)}%)`,
      details: {
        active: row.active,
        idle: row.idle,
        idle_in_transaction: row.idle_in_transaction,
        recommendation: row.idle_in_transaction > 5 ? 'Check for long-running idle transactions' : 'Connection usage healthy'
      }
    });
  } catch (error) {
    results.push({
      category: 'Connection Health',
      status: 'warning',
      message: `Could not check connections: ${error}`
    });
  }

  // 3. Invalid Indexes
  try {
    const indexResult = await dbManager.query(`
      SELECT
        schemaname,
        tablename,
        indexname,
        pg_get_indexdef(i.indexrelid) as indexdef
      FROM pg_indexes
      JOIN pg_index i ON i.indexrelid = (schemaname || '.' || indexname)::regclass
      WHERE NOT i.indisvalid
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
      message: `Could not check unused indexes: ${error}`
    });
  }

  // 5. Duplicate Indexes
  try {
    const dupResult = await dbManager.query(`
      SELECT
        pg_size_pretty(sum(pg_relation_size(idx))::bigint) as total_size,
        array_agg(idx) as indexes,
        (array_agg(indrelid))[1]::regclass as table_name
      FROM (
        SELECT indexrelid::regclass as idx,
               indrelid,
               indkey as columns
        FROM pg_index
      ) sub
      GROUP BY indrelid, columns
      HAVING count(*) > 1
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
    const needsVacuum = vacuumResult.rows.filter((r: any) => r.dead_ratio > 10);
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
      message: `Could not check vacuum status: ${error}`
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
      WHERE CASE WHEN max_value > 0
        THEN 100.0 * last_value / max_value > 50
        ELSE false
      END
      ORDER BY usage_percent DESC
    `);

    let status: HealthCheckResult['status'] = 'healthy';
    if (seqResult.rows.some((r: any) => r.usage_percent > 80)) status = 'warning';
    if (seqResult.rows.some((r: any) => r.usage_percent > 95)) status = 'critical';

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
        conrelid::regclass as table_name,
        contype
      FROM pg_constraint
      WHERE NOT convalidated
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
