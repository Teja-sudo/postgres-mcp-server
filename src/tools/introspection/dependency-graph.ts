/**
 * Dependency graph + topological sort
 *
 * Given a list of ExtractedDDL with dependency OIDs, produce a creation
 * order such that each object's dependencies come before it. Cycles
 * (typically tables with mutual FKs) are surfaced separately and
 * handled by the export layer (FKs are emitted as ALTER TABLE
 * statements appended after all tables).
 */

import {
  DependencyNode,
  ExtractedDDL,
  ObjectDescriptor,
  ObjectKind,
  OrderedObjects,
} from './types.js';

/**
 * Built-in kind ordering used as a tiebreaker / first-pass sort.
 * Earlier kinds are emitted before later ones when there's no explicit
 * dependency relationship.
 */
const KIND_PRIORITY: Record<ObjectKind, number> = {
  extension: 0,
  schema: 1,
  type: 2,
  sequence: 3,
  table: 4,       // FKs separated to ALTER TABLEs - see object-ddl.ts
  index: 5,
  matview: 6,
  view: 7,
  function: 8,
  procedure: 8,
  trigger: 9,
};

export interface BuildGraphInput {
  descriptors: ObjectDescriptor[];
  ddls: ExtractedDDL[];
}

/**
 * Build a graph keyed by OID where edges represent "must be created
 * before". Returns the graph as a Map plus the descriptor lookup.
 */
export function buildDependencyGraph(input: BuildGraphInput): Map<number, DependencyNode> {
  const byOid = new Map<number, ObjectDescriptor>();
  for (const d of input.descriptors) byOid.set(d.oid, d);

  const graph = new Map<number, DependencyNode>();

  for (let i = 0; i < input.descriptors.length; i++) {
    const desc = input.descriptors[i];
    const ddl = input.ddls[i];
    const node: DependencyNode = {
      oid: desc.oid,
      kind: desc.kind,
      schema: desc.schema,
      name: desc.name,
      dependsOn: new Set<number>(),
    };
    if (ddl) {
      for (const depOid of ddl.dependencies) {
        // Only count deps that are within our scope
        if (byOid.has(depOid)) {
          node.dependsOn.add(depOid);
        }
      }
    }
    graph.set(desc.oid, node);
  }

  return graph;
}

/**
 * Topologically sort the graph. Cycles are detected and reported in
 * the result; affected nodes are still included in the ordered list
 * (with their dependencies broken arbitrarily) so callers can choose
 * how to handle them.
 *
 * Within a single dependency level, nodes are ordered by kind priority
 * (extensions first, triggers last) and then by qualified name for
 * deterministic output.
 */
export function topologicallyOrder(graph: Map<number, DependencyNode>): OrderedObjects {
  const ordered: DependencyNode[] = [];
  const cycles: number[][] = [];
  const inDegree = new Map<number, number>();
  const dependents = new Map<number, number[]>();

  for (const [oid, node] of graph) {
    inDegree.set(oid, node.dependsOn.size);
    for (const dep of node.dependsOn) {
      const list = dependents.get(dep) ?? [];
      list.push(oid);
      dependents.set(dep, list);
    }
  }

  // Kahn's algorithm with kind-priority tiebreaker
  const ready: number[] = [];
  for (const [oid, deg] of inDegree) {
    if (deg === 0) ready.push(oid);
  }

  const compareNodes = (a: number, b: number): number => {
    const na = graph.get(a)!;
    const nb = graph.get(b)!;
    const pa = KIND_PRIORITY[na.kind] ?? 100;
    const pb = KIND_PRIORITY[nb.kind] ?? 100;
    if (pa !== pb) return pa - pb;
    const qa = `${na.schema}.${na.name}`;
    const qb = `${nb.schema}.${nb.name}`;
    return qa.localeCompare(qb);
  };

  while (ready.length > 0) {
    ready.sort(compareNodes);
    const oid = ready.shift()!;
    ordered.push(graph.get(oid)!);
    for (const dep of dependents.get(oid) ?? []) {
      const newDeg = (inDegree.get(dep) ?? 0) - 1;
      inDegree.set(dep, newDeg);
      if (newDeg === 0) ready.push(dep);
    }
  }

  // Detect cycles: any node still with non-zero in-degree
  const remaining: number[] = [];
  for (const [oid, deg] of inDegree) {
    if (deg > 0) remaining.push(oid);
  }
  if (remaining.length > 0) {
    // Group cyclic nodes - simple approach: all remaining are in
    // (potentially overlapping) cycles. For our use case (mostly FK
    // cycles between tables), surfacing them as one group is enough.
    cycles.push(remaining);
    // Append them in deterministic order to ordered output anyway
    remaining.sort(compareNodes);
    for (const oid of remaining) ordered.push(graph.get(oid)!);
  }

  return { ordered, cycles };
}
