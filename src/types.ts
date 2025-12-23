export interface ServerConfig {
  host: string;
  port: string;
  username: string;
  password: string;
  defaultDatabase?: string;
  defaultSchema?: string;
  isDefault?: boolean;
  ssl?: boolean | 'require' | 'prefer' | 'allow' | 'disable' | {
    rejectUnauthorized?: boolean;
    ca?: string;
    cert?: string;
    key?: string;
  };
}

export interface ServersConfig {
  [serverName: string]: ServerConfig;
}

export interface ConnectionState {
  currentServer: string | null;
  currentDatabase: string | null;
  currentSchema: string | null;
}

export interface ConnectionInfo {
  isConnected: boolean;
  server: string | null;
  database: string | null;
  schema: string | null;
  host: string | null;
  port: string | null;
  accessMode: 'full' | 'readonly';
}

export interface DatabaseInfo {
  name: string;
  owner: string;
  encoding: string;
  size: string;
}

export interface SchemaInfo {
  schema_name: string;
  owner: string;
}

export interface TableInfo {
  name: string;
  type: 'table' | 'view' | 'sequence' | 'extension';
  owner: string;
  schema: string;
}

export interface ColumnInfo {
  column_name: string;
  data_type: string;
  is_nullable: string;
  column_default: string | null;
  character_maximum_length: number | null;
}

export interface ConstraintInfo {
  constraint_name: string;
  constraint_type: string;
  table_name: string;
  column_name: string;
  foreign_table_name: string | null;
  foreign_column_name: string | null;
}

export interface IndexInfo {
  index_name: string;
  index_definition: string;
  is_unique: boolean;
  is_primary: boolean;
}

export interface QueryPlan {
  plan: object | string;
  planning_time?: number;
  execution_time?: number;
}

export interface SlowQuery {
  query: string;
  calls: number;
  total_time: number;
  mean_time: number;
  rows: number;
}

export interface IndexRecommendation {
  table: string;
  columns: string[];
  index_type: string;
  reason: string;
  estimated_improvement: string;
}

export interface HealthCheckResult {
  category: string;
  status: 'healthy' | 'warning' | 'critical';
  message: string;
  details?: object;
}

export interface ExecuteSqlResult {
  rows: any[];
  rowCount: number;
  fields: string[];
  outputFile?: string;
  truncated?: boolean;
  executionTimeMs?: number;
  offset?: number;
  hasMore?: boolean;
}
