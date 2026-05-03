/**
 * Transaction Guard Unit Tests
 *
 * Pure unit tests - no PG required. Validates the static heuristic
 * (shouldVerifyAfter), the comment/string-stripping helper, and the
 * savepoint-name generation. Real-PG verify() behavior is covered
 * by the integration suite under __tests__/integration/.
 */

import { describe, it, expect } from '@jest/globals';
import {
  TransactionGuard,
  stripCommentsAndStrings,
} from '../../tools/sql/utils/transaction-guard.js';

describe('TransactionGuard', () => {
  describe('savepointName', () => {
    it('starts with the psm_outer_ prefix', () => {
      const g = new TransactionGuard();
      expect(g.savepointName).toMatch(/^psm_outer_/);
    });

    it('produces a valid PG identifier (≤63 chars, [A-Za-z_][A-Za-z0-9_]*)', () => {
      const g = new TransactionGuard();
      expect(g.savepointName.length).toBeLessThanOrEqual(63);
      expect(g.savepointName).toMatch(/^[A-Za-z_][A-Za-z0-9_]*$/);
    });

    it('generates unique names across instances', () => {
      const names = new Set<string>();
      for (let i = 0; i < 50; i++) {
        names.add(new TransactionGuard().savepointName);
      }
      expect(names.size).toBe(50);
    });
  });

  describe('shouldVerifyAfter', () => {
    const guard = new TransactionGuard();

    it.each([
      ['COMMIT;', true],
      ['ROLLBACK;', true],
      ['BEGIN;', true],
      ['START TRANSACTION;', true],
      ['SAVEPOINT s1;', true],
      ['RELEASE SAVEPOINT s1;', true],
      ['ABORT;', true],
      ['END;', true],
      ['DO $$ BEGIN COMMIT; END $$;', true],
      ["EXECUTE 'COMMIT'", true],
      ['CALL my_proc()', true],
    ])('returns true for transaction-control / dynamic SQL: %s', (sql, expected) => {
      expect(guard.shouldVerifyAfter(sql)).toBe(expected);
    });

    it.each([
      ['SELECT 1', false],
      ['SELECT * FROM users WHERE id = 1', false],
      ['INSERT INTO t VALUES (1)', false],
      ['UPDATE t SET x = 1 WHERE y = 2', false],
      ['DELETE FROM t WHERE id = 1', false],
      ['CREATE TABLE t (id int)', false],
      ['ALTER TABLE t ADD COLUMN x int', false],
      ['DROP TABLE t', false],
    ])('returns false for normal DML/DDL: %s', (sql, expected) => {
      expect(guard.shouldVerifyAfter(sql)).toBe(expected);
    });

    it('returns false when COMMIT appears only inside a line comment', () => {
      expect(guard.shouldVerifyAfter('-- COMMIT this later\nSELECT 1')).toBe(false);
    });

    it('returns false when COMMIT appears only inside a block comment', () => {
      expect(guard.shouldVerifyAfter('/* COMMIT */ SELECT 1')).toBe(false);
    });

    it('returns false when COMMIT appears only inside a string literal', () => {
      expect(guard.shouldVerifyAfter("INSERT INTO log VALUES ('COMMIT')")).toBe(false);
    });

    it('returns false when COMMIT appears only inside a double-quoted identifier', () => {
      expect(guard.shouldVerifyAfter('SELECT * FROM "COMMIT_LOG"')).toBe(false);
    });

    it('returns false when COMMIT appears only inside a dollar-quoted string body', () => {
      const sql = 'CREATE FUNCTION f() RETURNS void AS $$ SELECT 1 $$ LANGUAGE sql';
      expect(guard.shouldVerifyAfter(sql)).toBe(false);
    });

    it('returns true when COMMIT keyword genuinely appears outside strings/comments', () => {
      // Real top-level COMMIT after some SQL
      expect(guard.shouldVerifyAfter("INSERT INTO log VALUES ('hi'); COMMIT")).toBe(true);
    });

    it('handles tagged dollar-quotes correctly', () => {
      const sql = 'SELECT $tag$ this is a $tag$ test';
      // 'tag' is not a control keyword; nothing to flag
      expect(guard.shouldVerifyAfter(sql)).toBe(false);
    });
  });
});

describe('stripCommentsAndStrings', () => {
  it('strips a leading line comment', () => {
    const out = stripCommentsAndStrings('-- ignore me\nSELECT 1');
    expect(out).not.toContain('ignore');
    expect(out).toContain('SELECT 1');
  });

  it('strips a block comment', () => {
    const out = stripCommentsAndStrings('SELECT /* ignore */ 1');
    expect(out).not.toContain('ignore');
    expect(out).toMatch(/SELECT\s+1/);
  });

  it('strips nested block comments', () => {
    const out = stripCommentsAndStrings('SELECT /* a /* b */ c */ 1');
    expect(out).not.toContain('a');
    expect(out).not.toContain('b');
    expect(out).not.toContain('c');
    expect(out).toMatch(/SELECT\s+1/);
  });

  it('strips a single-quoted string literal', () => {
    const out = stripCommentsAndStrings("SELECT 'hello world'");
    expect(out).not.toContain('hello');
    expect(out).not.toContain('world');
    expect(out).toContain('SELECT');
  });

  it('strips a double-quoted identifier', () => {
    const out = stripCommentsAndStrings('SELECT "weird name"');
    expect(out).not.toContain('weird');
    expect(out).not.toContain('name');
  });

  it('handles doubled quotes inside string as escapes', () => {
    const out = stripCommentsAndStrings("SELECT 'it''s fine'");
    expect(out).not.toContain('fine');
    expect(out).toContain('SELECT');
  });

  it('strips dollar-quoted strings', () => {
    const out = stripCommentsAndStrings('SELECT $$secret COMMIT$$');
    expect(out).not.toContain('secret');
    expect(out).not.toContain('COMMIT');
  });

  it('strips tagged dollar-quoted strings', () => {
    const out = stripCommentsAndStrings('SELECT $body$inside COMMIT$body$');
    expect(out).not.toContain('inside');
    expect(out).not.toContain('COMMIT');
  });

  it('preserves newlines inside stripped regions for line-number stability', () => {
    const out = stripCommentsAndStrings("SELECT '\n\n' FROM t");
    // Two newlines should still be in the output
    expect((out.match(/\n/g) || []).length).toBe(2);
  });

  it('leaves SQL outside comments/strings unchanged', () => {
    const out = stripCommentsAndStrings('SELECT 1 FROM t WHERE x = 2');
    expect(out).toMatch(/SELECT\s+1\s+FROM\s+t\s+WHERE\s+x\s+=\s+2/);
  });

  it('handles unclosed dollar-quote without throwing', () => {
    expect(() => stripCommentsAndStrings('SELECT $$ unclosed')).not.toThrow();
  });

  it('handles unclosed string without throwing', () => {
    expect(() => stripCommentsAndStrings("SELECT 'unclosed")).not.toThrow();
  });

  it('handles unclosed block comment without throwing', () => {
    expect(() => stripCommentsAndStrings('SELECT /* unclosed')).not.toThrow();
  });
});
