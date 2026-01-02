/**
 * Result Formatter Utilities Tests
 */

import {
  calculateExecutionTime,
  getStartTime,
  handleLargeOutput,
  paginateRows,
  truncateSql,
  createExecutionSummary,
  countStatementsByType,
  createFileSummary,
  formatFieldNames,
} from '../../tools/sql/utils/result-formatter.js';
import * as fs from 'fs';

describe('result-formatter', () => {
  describe('calculateExecutionTime', () => {
    it('should calculate execution time in milliseconds', () => {
      const start = BigInt(1000000000); // 1ms in nanoseconds
      const end = BigInt(5000000000); // 5ms in nanoseconds
      const result = calculateExecutionTime(start, end);
      expect(result).toBe(4000); // 4ms
    });

    it('should round to 2 decimal places', () => {
      const start = BigInt(0);
      const end = BigInt(1234567); // ~1.234567ms
      const result = calculateExecutionTime(start, end);
      expect(result).toBe(1.23);
    });
  });

  describe('getStartTime', () => {
    it('should return a bigint', () => {
      const start = getStartTime();
      expect(typeof start).toBe('bigint');
    });

    it('should return increasing values', () => {
      const start1 = getStartTime();
      const start2 = getStartTime();
      expect(start2).toBeGreaterThanOrEqual(start1);
    });
  });

  describe('handleLargeOutput', () => {
    it('should return rows unchanged when under limit', () => {
      const rows = [{ id: 1 }, { id: 2 }];
      const result = handleLargeOutput(rows);
      expect(result.truncated).toBe(false);
      expect(result.rows).toEqual(rows);
      expect(result.outputFile).toBeUndefined();
    });

    it('should write to file when over limit', () => {
      const largeRows = Array.from({ length: 10000 }, (_, i) => ({
        id: i,
        data: 'x'.repeat(100),
      }));
      const result = handleLargeOutput(largeRows, 1000); // Low limit

      expect(result.truncated).toBe(true);
      expect(result.outputFile).toBeDefined();
      expect(result.rows).toEqual([]);

      // Clean up
      if (result.outputFile) {
        fs.unlinkSync(result.outputFile);
      }
    });

    it('should use custom maxChars limit', () => {
      const rows = [{ data: 'a'.repeat(100) }];
      const result = handleLargeOutput(rows, 50);
      expect(result.truncated).toBe(true);
    });
  });

  describe('paginateRows', () => {
    const rows = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

    it('should return first page correctly', () => {
      const result = paginateRows(rows, 0, 3);
      expect(result.rows).toEqual([1, 2, 3]);
      expect(result.offset).toBe(0);
      expect(result.hasMore).toBe(true);
      expect(result.totalCount).toBe(10);
    });

    it('should return middle page correctly', () => {
      const result = paginateRows(rows, 3, 3);
      expect(result.rows).toEqual([4, 5, 6]);
      expect(result.offset).toBe(3);
      expect(result.hasMore).toBe(true);
    });

    it('should return last page correctly', () => {
      const result = paginateRows(rows, 9, 3);
      expect(result.rows).toEqual([10]);
      expect(result.hasMore).toBe(false);
    });

    it('should handle offset beyond array', () => {
      const result = paginateRows(rows, 20, 3);
      expect(result.rows).toEqual([]);
      expect(result.hasMore).toBe(false);
    });
  });

  describe('truncateSql', () => {
    it('should not truncate short SQL', () => {
      const sql = 'SELECT * FROM users';
      expect(truncateSql(sql)).toBe(sql);
    });

    it('should truncate long SQL with ellipsis', () => {
      const sql = 'SELECT ' + 'a'.repeat(300) + ' FROM users';
      const result = truncateSql(sql, 200);
      expect(result.length).toBe(203); // 200 + '...'
      expect(result.endsWith('...')).toBe(true);
    });

    it('should respect custom maxLength', () => {
      const sql = 'SELECT * FROM users';
      const result = truncateSql(sql, 10);
      expect(result).toBe('SELECT * F...');
    });

    it('should trim whitespace', () => {
      const sql = '  SELECT * FROM users  ';
      expect(truncateSql(sql)).toBe('SELECT * FROM users');
    });
  });

  describe('createExecutionSummary', () => {
    it('should create summary for all successful', () => {
      const summary = createExecutionSummary(5, 5, 0, 0, false);
      expect(summary).toContain('Executed 5 statements');
      expect(summary).toContain('5 succeeded');
    });

    it('should include failure count', () => {
      const summary = createExecutionSummary(5, 3, 2, 0, false);
      expect(summary).toContain('3 succeeded');
      expect(summary).toContain('2 failed');
    });

    it('should include skipped count', () => {
      const summary = createExecutionSummary(5, 3, 0, 2, false);
      expect(summary).toContain('3 succeeded');
      expect(summary).toContain('2 skipped (non-rollbackable)');
    });

    it('should indicate dry-run with rollback', () => {
      const summary = createExecutionSummary(5, 5, 0, 0, true);
      expect(summary).toContain('Dry-run of 5 statements');
      expect(summary).toContain('All changes rolled back');
    });
  });

  describe('countStatementsByType', () => {
    it('should count statement types correctly', () => {
      const types = ['SELECT', 'SELECT', 'INSERT', 'UPDATE', 'SELECT'];
      const counts = countStatementsByType(types);
      expect(counts['SELECT']).toBe(3);
      expect(counts['INSERT']).toBe(1);
      expect(counts['UPDATE']).toBe(1);
    });

    it('should return empty object for empty array', () => {
      const counts = countStatementsByType([]);
      expect(counts).toEqual({});
    });
  });

  describe('createFileSummary', () => {
    it('should create readable summary', () => {
      const counts = { SELECT: 5, INSERT: 3, UPDATE: 2 };
      const summary = createFileSummary(counts, 10);
      expect(summary).toContain('File contains 10 statements');
      expect(summary).toContain('5 SELECT');
      expect(summary).toContain('3 INSERT');
      expect(summary).toContain('2 UPDATE');
    });

    it('should sort by count descending', () => {
      const counts = { UPDATE: 1, SELECT: 10, INSERT: 5 };
      const summary = createFileSummary(counts, 16);
      // SELECT should appear first
      const selectIndex = summary.indexOf('SELECT');
      const insertIndex = summary.indexOf('INSERT');
      expect(selectIndex).toBeLessThan(insertIndex);
    });
  });

  describe('formatFieldNames', () => {
    it('should extract field names', () => {
      const fields = [{ name: 'id' }, { name: 'name' }, { name: 'email' }];
      const names = formatFieldNames(fields);
      expect(names).toEqual(['id', 'name', 'email']);
    });

    it('should return empty array for empty input', () => {
      const names = formatFieldNames([]);
      expect(names).toEqual([]);
    });
  });
});
