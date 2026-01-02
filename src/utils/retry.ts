import { getDbManager, DatabaseManager } from '../db-manager.js';

/**
 * Wraps a tool handler function with automatic retry logic for connection errors.
 * If a connection error is detected (e.g., server went inactive), it will:
 * 1. Invalidate the current connection
 * 2. Attempt to reconnect
 * 3. Retry the operation once
 *
 * @param handler - The async function to wrap
 * @param maxRetries - Maximum number of retries (default: 1)
 * @returns Wrapped function with retry logic
 */
export function withConnectionRetry<T extends unknown[], R>(
  handler: (...args: T) => Promise<R>,
  maxRetries: number = 1
): (...args: T) => Promise<R> {
  return async (...args: T): Promise<R> => {
    let lastError: unknown;

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        return await handler(...args);
      } catch (error) {
        lastError = error;

        // Check if this is a connection error that warrants retry
        if (attempt < maxRetries && DatabaseManager.isConnectionError(error)) {
          console.error(`Connection error detected (attempt ${attempt + 1}/${maxRetries + 1}), attempting reconnect...`);

          const dbManager = getDbManager();
          const reconnected = await dbManager.reconnect();

          if (reconnected) {
            console.error('Reconnection successful, retrying operation...');
            continue; // Retry the operation
          } else {
            console.error('Reconnection failed, will not retry');
            break;
          }
        }

        // Not a connection error or max retries reached
        break;
      }
    }

    // Re-throw the last error
    throw lastError;
  };
}

/**
 * Creates a tool handler wrapper that includes connection retry logic.
 * Use this for tools that require an active database connection.
 *
 * @param handler - The tool handler function
 * @returns Wrapped handler with retry logic
 */
export function createRetryableToolHandler<TArgs, TResult>(
  handler: (args: TArgs) => Promise<TResult>
): (args: TArgs) => Promise<TResult> {
  return withConnectionRetry(handler);
}
