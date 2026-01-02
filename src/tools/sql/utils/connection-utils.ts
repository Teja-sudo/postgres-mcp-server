/**
 * Connection Utilities
 *
 * Helper functions for connection management and override handling.
 * Centralizes connection override logic to reduce duplication.
 */

import { getDbManager } from '../../../db-manager.js';
import { ConnectionOverride } from '../../../types.js';
import { PoolClient } from 'pg';

/**
 * Parameters for connection override.
 */
export interface ConnectionOverrideParams {
  server?: string;
  database?: string;
  schema?: string;
}

/**
 * Result of client acquisition.
 */
export interface AcquiredClient {
  client: PoolClient;
  release: () => void;
  isOverride: boolean;
  connectionInfo?: {
    server: string;
    database: string;
    schema: string;
  };
}

/**
 * Build a ConnectionOverride object from optional parameters.
 * Returns undefined if no override parameters are provided.
 *
 * @param params - Optional connection override parameters
 * @returns ConnectionOverride object or undefined
 */
export function buildConnectionOverride(
  params: ConnectionOverrideParams
): ConnectionOverride | undefined {
  const hasOverride = params.server || params.database || params.schema;
  if (!hasOverride) {
    return undefined;
  }
  return {
    server: params.server,
    database: params.database,
    schema: params.schema,
  };
}

/**
 * Check if connection override parameters conflict with transaction usage.
 *
 * @param params - Connection override parameters
 * @param transactionId - Transaction ID if any
 * @throws Error if override is used with transaction
 */
export function validateOverrideWithTransaction(
  params: ConnectionOverrideParams,
  transactionId?: string
): void {
  const hasOverride = params.server || params.database || params.schema;
  if (hasOverride && transactionId) {
    throw new Error(
      'Connection override (server/database/schema) cannot be used with transactions. ' +
      'Transactions are bound to the main connection.'
    );
  }
}

/**
 * Acquire a client with optional connection override.
 * Handles both regular and override client acquisition.
 *
 * @param override - Optional connection override
 * @returns Acquired client with release function
 */
export async function acquireClient(
  override?: ConnectionOverride
): Promise<AcquiredClient> {
  const dbManager = getDbManager();

  if (override) {
    const result = await dbManager.getClientWithOverride(override);
    return {
      client: result.client,
      release: result.release,
      isOverride: true,
      connectionInfo: {
        server: result.server,
        database: result.database,
        schema: result.schema,
      },
    };
  }

  const client = await dbManager.getClient();
  return {
    client,
    release: () => client.release(),
    isOverride: false,
  };
}

/**
 * Execute a function with an acquired client, ensuring proper cleanup.
 *
 * @param override - Optional connection override
 * @param fn - Function to execute with the client
 * @returns Result of the function
 */
export async function withClient<T>(
  override: ConnectionOverride | undefined,
  fn: (client: PoolClient) => Promise<T>
): Promise<T> {
  const { client, release } = await acquireClient(override);
  try {
    return await fn(client);
  } finally {
    release();
  }
}

/**
 * Execute a function with a transaction, ensuring proper rollback on error.
 *
 * @param override - Optional connection override
 * @param fn - Function to execute within transaction
 * @param rollbackOnComplete - If true, rollback even on success (for dry-run)
 * @returns Result of the function
 */
export async function withTransaction<T>(
  override: ConnectionOverride | undefined,
  fn: (client: PoolClient) => Promise<T>,
  rollbackOnComplete: boolean = false
): Promise<T> {
  const { client, release } = await acquireClient(override);
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    if (rollbackOnComplete) {
      await client.query('ROLLBACK');
    } else {
      await client.query('COMMIT');
    }
    return result;
  } catch (error) {
    try {
      await client.query('ROLLBACK');
    } catch {
      // Ignore rollback errors
    }
    throw error;
  } finally {
    release();
  }
}

/**
 * Execute a dry-run transaction (always rollback).
 *
 * @param override - Optional connection override
 * @param fn - Function to execute within transaction
 * @returns Result of the function
 */
export async function withDryRunTransaction<T>(
  override: ConnectionOverride | undefined,
  fn: (client: PoolClient) => Promise<T>
): Promise<T> {
  return withTransaction(override, fn, true);
}
