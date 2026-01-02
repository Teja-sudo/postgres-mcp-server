/**
 * File Handler Utilities
 *
 * Functions for validating and processing SQL files.
 * Handles file validation, reading, and preprocessing.
 */

import * as fs from 'fs';
import * as path from 'path';
import { MAX_SQL_FILE_SIZE } from './constants.js';

/**
 * Result of file validation.
 */
export interface FileValidationResult {
  isValid: boolean;
  resolvedPath: string;
  fileSize: number;
  error?: string;
}

/**
 * Validates a SQL file path and returns file information.
 *
 * @param filePath - Path to the SQL file
 * @returns Validation result with resolved path and file size
 */
export function validateSqlFile(filePath: string): FileValidationResult {
  // Check file extension
  const ext = path.extname(filePath).toLowerCase();
  if (ext !== '.sql') {
    return {
      isValid: false,
      resolvedPath: '',
      fileSize: 0,
      error: 'Only .sql files are allowed. Received file extension: ' + ext,
    };
  }

  // Resolve path
  const resolvedPath = path.resolve(filePath);

  // Check file exists
  if (!fs.existsSync(resolvedPath)) {
    return {
      isValid: false,
      resolvedPath,
      fileSize: 0,
      error: `File not found: ${filePath}`,
    };
  }

  // Get file stats
  const stats = fs.statSync(resolvedPath);

  // Check it's a file
  if (!stats.isFile()) {
    return {
      isValid: false,
      resolvedPath,
      fileSize: 0,
      error: `Not a file: ${filePath}`,
    };
  }

  // Check file size
  if (stats.size > MAX_SQL_FILE_SIZE) {
    return {
      isValid: false,
      resolvedPath,
      fileSize: stats.size,
      error: `File too large: ${formatFileSize(stats.size)}. Maximum allowed: ${formatFileSize(MAX_SQL_FILE_SIZE)}`,
    };
  }

  // Check file not empty
  if (stats.size === 0) {
    return {
      isValid: false,
      resolvedPath,
      fileSize: 0,
      error: 'File is empty',
    };
  }

  return {
    isValid: true,
    resolvedPath,
    fileSize: stats.size,
  };
}

/**
 * Reads a SQL file and optionally preprocesses it.
 *
 * @param resolvedPath - Resolved path to the SQL file
 * @param stripPatterns - Optional patterns to remove from content
 * @param stripAsRegex - If true, patterns are regex; if false, literal strings
 * @returns Preprocessed SQL content
 */
export function readSqlFile(
  resolvedPath: string,
  stripPatterns?: string[],
  stripAsRegex: boolean = false
): string {
  let content = fs.readFileSync(resolvedPath, 'utf-8');

  if (stripPatterns && stripPatterns.length > 0) {
    content = preprocessSqlContent(content, stripPatterns, stripAsRegex);
  }

  return content;
}

/**
 * Preprocess SQL content by removing patterns.
 * Supports both literal string matching and regex patterns.
 *
 * @param sql - The SQL content to preprocess
 * @param patterns - Array of patterns to remove from SQL content
 * @param isRegex - If true, patterns are treated as regex; if false, as literal strings
 * @returns Preprocessed SQL content
 */
export function preprocessSqlContent(
  sql: string,
  patterns: string[],
  isRegex: boolean = false
): string {
  let result = sql;

  for (const pattern of patterns) {
    try {
      if (isRegex) {
        // Treat as regex pattern (multiline by default)
        const regex = new RegExp(pattern, 'gm');
        result = result.replace(regex, '');
      } else {
        // Treat as literal string - escape and match on its own line
        const escapedPattern = pattern.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        const regex = new RegExp(`^\\s*${escapedPattern}\\s*$`, 'gm');
        result = result.replace(regex, '');
      }
    } catch (error) {
      // Invalid regex - skip this pattern silently in production
      // Log only in non-production for debugging
      if (process.env.NODE_ENV !== 'production') {
        console.error(
          `Warning: Invalid pattern "${pattern}": ${error instanceof Error ? error.message : String(error)}`
        );
      }
    }
  }

  return result;
}

/**
 * Format file size in human-readable format.
 *
 * @param bytes - File size in bytes
 * @returns Human-readable file size (e.g., "1.5 MB")
 */
export function formatFileSize(bytes: number): string {
  if (bytes === 0) return '0 B';

  const units = ['B', 'KB', 'MB', 'GB'];
  const base = 1024;
  const unitIndex = Math.min(
    Math.floor(Math.log(bytes) / Math.log(base)),
    units.length - 1
  );

  const size = bytes / Math.pow(base, unitIndex);
  return `${size.toFixed(unitIndex > 0 ? 1 : 0)} ${units[unitIndex]}`;
}

/**
 * Ensure a file path is safe (no path traversal attacks).
 *
 * @param basePath - Base directory path
 * @param filePath - File path to validate
 * @returns True if the file path is within the base path
 */
export function isPathSafe(basePath: string, filePath: string): boolean {
  const resolvedBase = path.resolve(basePath);
  const resolvedFile = path.resolve(filePath);
  return resolvedFile.startsWith(resolvedBase);
}
