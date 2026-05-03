/**
 * Dependency graph + topological sort unit tests.
 */

import { describe, it, expect } from '@jest/globals';
import {
  buildDependencyGraph,
  topologicallyOrder,
  ObjectDescriptor,
  ExtractedDDL,
} from '../../tools/introspection/index.js';

function desc(oid: number, kind: ObjectDescriptor['kind'], name: string, schema = 'public'): ObjectDescriptor {
  return { oid, kind, name, schema, owner: 'test' };
}

function ddl(deps: number[]): ExtractedDDL {
  return { kind: 'table', qualifiedName: 't', sql: '...', warnings: [], dependencies: deps };
}

describe('introspection: dependency graph', () => {
  it('orders extension before table', () => {
    const descriptors = [desc(1, 'table', 'users'), desc(2, 'extension', 'pgcrypto')];
    const ddls = [ddl([]), { ...ddl([]), kind: 'extension' as const }];
    const graph = buildDependencyGraph({ descriptors, ddls });
    const { ordered } = topologicallyOrder(graph);
    expect(ordered.map((n) => n.kind)).toEqual(['extension', 'table']);
  });

  it('respects explicit dependency between objects', () => {
    // index depends on table
    const descriptors = [desc(10, 'index', 'idx_users_email'), desc(20, 'table', 'users')];
    const ddls = [ddl([20]), ddl([])];
    const graph = buildDependencyGraph({ descriptors, ddls });
    const { ordered } = topologicallyOrder(graph);
    const oids = ordered.map((n) => n.oid);
    expect(oids.indexOf(20)).toBeLessThan(oids.indexOf(10));
  });

  it('orders types before tables (kind priority)', () => {
    const descriptors = [desc(1, 'table', 'orders'), desc(2, 'type', 'order_status')];
    const ddls = [ddl([]), ddl([])];
    const graph = buildDependencyGraph({ descriptors, ddls });
    const { ordered } = topologicallyOrder(graph);
    expect(ordered.map((n) => n.kind)).toEqual(['type', 'table']);
  });

  it('detects cycles', () => {
    // a depends on b; b depends on a
    const descriptors = [desc(1, 'table', 'a'), desc(2, 'table', 'b')];
    const ddls = [ddl([2]), ddl([1])];
    const graph = buildDependencyGraph({ descriptors, ddls });
    const { cycles, ordered } = topologicallyOrder(graph);
    expect(cycles.length).toBeGreaterThanOrEqual(1);
    // Cyclic nodes are still emitted (so user can see them)
    expect(ordered.length).toBe(2);
  });

  it('produces deterministic order via name tiebreaker', () => {
    const descriptors = [
      desc(1, 'table', 'zebra'),
      desc(2, 'table', 'apple'),
      desc(3, 'table', 'mango'),
    ];
    const ddls = [ddl([]), ddl([]), ddl([])];
    const graph = buildDependencyGraph({ descriptors, ddls });
    const { ordered } = topologicallyOrder(graph);
    expect(ordered.map((n) => n.name)).toEqual(['apple', 'mango', 'zebra']);
  });

  it('prunes dependencies that are out of scope', () => {
    // Table depends on OID 999, which is NOT in our descriptor list
    const descriptors = [desc(1, 'table', 'users')];
    const ddls = [ddl([999])];
    const graph = buildDependencyGraph({ descriptors, ddls });
    expect(graph.get(1)!.dependsOn.size).toBe(0);
    const { ordered, cycles } = topologicallyOrder(graph);
    expect(ordered.length).toBe(1);
    expect(cycles).toEqual([]);
  });

  it('handles empty input', () => {
    const graph = buildDependencyGraph({ descriptors: [], ddls: [] });
    const { ordered, cycles } = topologicallyOrder(graph);
    expect(ordered).toEqual([]);
    expect(cycles).toEqual([]);
  });
});
