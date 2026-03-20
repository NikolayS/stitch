// src/plan/sort.ts — Dependency-aware topological sort for deploy ordering
//
// Implements Kahn's algorithm for topological sort, cycle detection,
// conflict checking, and filtering for pending/target changes.
//
// Changes declare dependencies via `requires` (must be deployed first)
// and `conflicts` (must NOT be deployed). The sort produces a valid
// deploy order that respects all dependency constraints.

import type { Change } from "./types";

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

/** Thrown when a dependency cycle is detected among changes. */
export class CycleError extends Error {
  /** The change names forming the cycle, e.g. ["A", "B", "C", "A"]. */
  readonly cycle: string[];

  constructor(cycle: string[]) {
    super(`Dependency cycle detected: ${cycle.join(" -> ")}`);
    this.name = "CycleError";
    this.cycle = cycle;
  }
}

/** Thrown when a required dependency is missing (not in changes or deployed). */
export class MissingDependencyError extends Error {
  /** The change that has the unsatisfied dependency. */
  readonly change: string;
  /** The dependency that is missing. */
  readonly dependency: string;

  constructor(change: string, dependency: string) {
    super(
      `Change "${change}" requires "${dependency}", which is not in the plan or deployed`,
    );
    this.name = "MissingDependencyError";
    this.change = change;
    this.dependency = dependency;
  }
}

/** Thrown when a conflicting change is already deployed. */
export class ConflictError extends Error {
  /** The change that declares the conflict. */
  readonly change: string;
  /** The deployed change that conflicts. */
  readonly conflictsWith: string;

  constructor(change: string, conflictsWith: string) {
    super(
      `Change "${change}" conflicts with "${conflictsWith}", which is already deployed`,
    );
    this.name = "ConflictError";
    this.change = change;
    this.conflictsWith = conflictsWith;
  }
}

// ---------------------------------------------------------------------------
// Core functions
// ---------------------------------------------------------------------------

/**
 * Topological sort using Kahn's algorithm.
 *
 * Returns changes in a valid deploy order: every change appears after
 * all of its `requires` dependencies. For changes with no dependency
 * relationship, the original plan-file order is preserved (stable sort).
 *
 * Throws CycleError if a dependency cycle is detected.
 *
 * Dependencies referencing changes not in the input set are silently
 * skipped (they are assumed to be already deployed). Use
 * validateDependencies() to verify all dependencies are satisfiable
 * before calling this function.
 */
export function topologicalSort(changes: Change[]): Change[] {
  if (changes.length === 0) return [];

  // Build name -> index map for O(1) lookup
  const nameToIndex = new Map<string, number>();
  for (let i = 0; i < changes.length; i++) {
    const c = changes[i]!;
    nameToIndex.set(c.name, i);
  }

  // Compute in-degree and adjacency (forward edges: dep -> dependant)
  const n = changes.length;
  const inDegree = new Array<number>(n).fill(0);
  const adj = new Array<number[]>(n);
  for (let i = 0; i < n; i++) {
    adj[i] = [];
  }

  for (let i = 0; i < n; i++) {
    const c = changes[i]!;
    for (const req of c.requires) {
      const depIdx = nameToIndex.get(req);
      if (depIdx === undefined) {
        // External dependency (already deployed or validated separately)
        continue;
      }
      adj[depIdx]!.push(i);
      inDegree[i]!++;
    }
  }

  // Kahn's algorithm with stable ordering:
  // Use a queue that always picks the node with the lowest original index
  // among those with in-degree 0, preserving plan-file order for
  // independent changes.
  const queue: number[] = [];
  for (let i = 0; i < n; i++) {
    if (inDegree[i] === 0) {
      queue.push(i);
    }
  }
  // Sort initially by original index (ascending) so we process in plan order
  queue.sort((a, b) => a - b);

  const result: Change[] = [];
  let processed = 0;

  while (queue.length > 0) {
    const idx = queue.shift()!;
    result.push(changes[idx]!);
    processed++;

    // Collect newly freed nodes
    const newlyFree: number[] = [];
    for (const neighbor of adj[idx]!) {
      inDegree[neighbor]!--;
      if (inDegree[neighbor] === 0) {
        newlyFree.push(neighbor);
      }
    }
    // Insert newly freed nodes in sorted position to maintain stability
    if (newlyFree.length > 0) {
      newlyFree.sort((a, b) => a - b);
      // Merge into queue maintaining sorted order
      const merged: number[] = [];
      let qi = 0;
      let ni = 0;
      while (qi < queue.length && ni < newlyFree.length) {
        if (queue[qi]! <= newlyFree[ni]!) {
          merged.push(queue[qi]!);
          qi++;
        } else {
          merged.push(newlyFree[ni]!);
          ni++;
        }
      }
      while (qi < queue.length) {
        merged.push(queue[qi]!);
        qi++;
      }
      while (ni < newlyFree.length) {
        merged.push(newlyFree[ni]!);
        ni++;
      }
      queue.length = 0;
      queue.push(...merged);
    }
  }

  if (processed < n) {
    // Cycle detected — find and report it
    const cyclePath = findCyclePath(changes, nameToIndex, inDegree);
    throw new CycleError(cyclePath);
  }

  return result;
}

/**
 * Find a cycle path among remaining nodes (those with non-zero in-degree).
 * Returns the cycle as [A, B, C, A] for reporting.
 */
function findCyclePath(
  changes: Change[],
  nameToIndex: Map<string, number>,
  inDegree: number[],
): string[] {
  // DFS from nodes still in the graph (in-degree > 0)
  const n = changes.length;
  const visited = new Set<number>();
  const onStack = new Set<number>();
  const parent = new Map<number, number>();

  for (let start = 0; start < n; start++) {
    if (inDegree[start] === 0 || visited.has(start)) continue;

    const stack: Array<{ node: number; reqIdx: number }> = [
      { node: start, reqIdx: 0 },
    ];
    onStack.add(start);
    visited.add(start);

    while (stack.length > 0) {
      const frame = stack[stack.length - 1]!;
      const c = changes[frame.node]!;

      if (frame.reqIdx >= c.requires.length) {
        // Done with this node
        onStack.delete(frame.node);
        stack.pop();
        continue;
      }

      const reqName = c.requires[frame.reqIdx]!;
      frame.reqIdx++;

      const neighbor = nameToIndex.get(reqName);
      if (neighbor === undefined || inDegree[neighbor] === 0) continue;

      if (onStack.has(neighbor)) {
        // Found cycle: trace back from current node to neighbor
        const cycle: string[] = [changes[neighbor]!.name];
        for (let i = stack.length - 1; i >= 0; i--) {
          cycle.push(changes[stack[i]!.node]!.name);
          if (stack[i]!.node === neighbor) break;
        }
        cycle.reverse();
        return cycle;
      }

      if (!visited.has(neighbor)) {
        visited.add(neighbor);
        onStack.add(neighbor);
        parent.set(neighbor, frame.node);
        stack.push({ node: neighbor, reqIdx: 0 });
      }
    }
  }

  // Fallback: shouldn't reach here if there's truly a cycle,
  // but return something useful
  const remaining = [];
  for (let i = 0; i < n; i++) {
    if (inDegree[i]! > 0) remaining.push(changes[i]!.name);
  }
  return [...remaining, remaining[0]!];
}

/**
 * Validate that all dependencies are satisfiable.
 *
 * A dependency is satisfiable if it exists in `changes` (will be deployed)
 * or in `deployed` (already deployed). Also checks conflict constraints:
 * if a change declares a conflict with a deployed change, throws ConflictError.
 *
 * @param changes - The changes to validate
 * @param deployed - Names of already-deployed changes
 */
export function validateDependencies(
  changes: Change[],
  deployed: string[],
): void {
  const deployedSet = new Set(deployed);
  const changeNames = new Set(changes.map((c) => c.name));

  for (const change of changes) {
    // Check requires
    for (const req of change.requires) {
      if (!changeNames.has(req) && !deployedSet.has(req)) {
        throw new MissingDependencyError(change.name, req);
      }
    }

    // Check conflicts
    for (const conflict of change.conflicts) {
      if (deployedSet.has(conflict)) {
        throw new ConflictError(change.name, conflict);
      }
    }
  }
}

/**
 * Detect dependency cycles among changes.
 *
 * Returns null if no cycle exists, or a CycleError with the cycle path
 * if one is found. Does not throw — the caller decides how to handle it.
 */
export function detectCycles(changes: Change[]): CycleError | null {
  if (changes.length === 0) return null;

  const nameToIndex = new Map<string, number>();
  for (let i = 0; i < changes.length; i++) {
    nameToIndex.set(changes[i]!.name, i);
  }

  // DFS-based cycle detection
  const n = changes.length;
  const WHITE = 0; // unvisited
  const GRAY = 1; // on current DFS stack
  const BLACK = 2; // fully processed
  const color = new Array<number>(n).fill(WHITE);

  for (let start = 0; start < n; start++) {
    if (color[start] !== WHITE) continue;

    const stack: Array<{ node: number; reqIdx: number }> = [
      { node: start, reqIdx: 0 },
    ];
    color[start] = GRAY;

    while (stack.length > 0) {
      const frame = stack[stack.length - 1]!;
      const c = changes[frame.node]!;

      if (frame.reqIdx >= c.requires.length) {
        color[frame.node] = BLACK;
        stack.pop();
        continue;
      }

      const reqName = c.requires[frame.reqIdx]!;
      frame.reqIdx++;

      const neighbor = nameToIndex.get(reqName);
      if (neighbor === undefined) continue; // external dep, skip

      if (color[neighbor] === GRAY) {
        // Found cycle — build the path
        const cycle: string[] = [changes[neighbor]!.name];
        for (let i = stack.length - 1; i >= 0; i--) {
          cycle.push(changes[stack[i]!.node]!.name);
          if (stack[i]!.node === neighbor) break;
        }
        cycle.reverse();
        return new CycleError(cycle);
      }

      if (color[neighbor] === WHITE) {
        color[neighbor] = GRAY;
        stack.push({ node: neighbor, reqIdx: 0 });
      }
    }
  }

  return null;
}

/**
 * Filter to only pending (undeployed) changes.
 *
 * Returns changes from `allChanges` whose `change_id` is not in
 * `deployedIds`. Preserves the original order.
 */
export function filterPending(
  allChanges: Change[],
  deployedIds: string[],
): Change[] {
  const deployed = new Set(deployedIds);
  return allChanges.filter((c) => !deployed.has(c.change_id));
}

/**
 * Filter changes up to and including a target change.
 *
 * Returns the subset of `changes` from the beginning up to and including
 * the change whose name matches `target`. Preserves order.
 *
 * Throws if the target change is not found.
 */
export function filterToTarget(changes: Change[], target: string): Change[] {
  const idx = changes.findIndex((c) => c.name === target);
  if (idx === -1) {
    throw new Error(`Target change "${target}" not found in plan`);
  }
  return changes.slice(0, idx + 1);
}
