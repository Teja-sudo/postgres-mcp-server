/**
 * SQL literal formatter unit tests.
 */

import { describe, it, expect } from '@jest/globals';
import { formatSqlLiteral } from '../../tools/introspection/index.js';

describe('formatSqlLiteral', () => {
  it('renders NULL for null/undefined', () => {
    expect(formatSqlLiteral(null)).toBe('NULL');
    expect(formatSqlLiteral(undefined)).toBe('NULL');
  });

  it('renders numbers raw', () => {
    expect(formatSqlLiteral(42)).toBe('42');
    expect(formatSqlLiteral(3.14)).toBe('3.14');
    expect(formatSqlLiteral(0)).toBe('0');
    expect(formatSqlLiteral(-1)).toBe('-1');
  });

  it('renders Infinity/NaN as quoted strings', () => {
    expect(formatSqlLiteral(Number.POSITIVE_INFINITY)).toBe("'Infinity'");
    expect(formatSqlLiteral(Number.NEGATIVE_INFINITY)).toBe("'-Infinity'");
    expect(formatSqlLiteral(Number.NaN)).toBe("'NaN'");
  });

  it('renders booleans as TRUE/FALSE', () => {
    expect(formatSqlLiteral(true)).toBe('TRUE');
    expect(formatSqlLiteral(false)).toBe('FALSE');
  });

  it('renders bigints as raw', () => {
    expect(formatSqlLiteral(BigInt('9999999999'))).toBe('9999999999');
  });

  it('renders Date as ISO timestamptz', () => {
    const d = new Date('2026-05-03T07:00:00Z');
    expect(formatSqlLiteral(d)).toBe("'2026-05-03T07:00:00.000Z'::timestamptz");
  });

  it('renders Buffer as bytea hex', () => {
    const b = Buffer.from('hello');
    expect(formatSqlLiteral(b)).toMatch(/^'\\x[0-9a-f]+'::bytea$/);
  });

  it('escapes single quotes in strings', () => {
    expect(formatSqlLiteral("it's fine")).toBe("'it''s fine'");
  });

  it('renders arrays as ARRAY[…]', () => {
    expect(formatSqlLiteral([1, 2, 3])).toBe('ARRAY[1, 2, 3]');
    expect(formatSqlLiteral(['a', 'b'])).toBe("ARRAY['a', 'b']");
  });

  it('renders nested arrays', () => {
    expect(formatSqlLiteral([[1, 2], [3, 4]])).toBe('ARRAY[ARRAY[1, 2], ARRAY[3, 4]]');
  });

  it('renders objects as JSONB', () => {
    expect(formatSqlLiteral({ a: 1, b: 'hi' })).toBe(`'{"a":1,"b":"hi"}'::jsonb`);
  });

  it('escapes single quotes inside JSON', () => {
    expect(formatSqlLiteral({ msg: "it's broken" })).toMatch(/it''s/);
  });
});
