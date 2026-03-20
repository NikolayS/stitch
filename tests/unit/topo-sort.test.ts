// tests/unit/topo-sort.test.ts — Tests for topological sort and dependency handling
//
// Validates deploy ordering, cycle detection, conflict checking,
// filtering, and edge cases for the topological sort module.

import { describe, expect, it } from "bun:test";

import {
  topologicalSort,
  validateDependencies,
  detectCycles,
  filterPending,
  filterToTarget,
  CycleError,
  MissingDependencyError,
  ConflictError,
} from "../../src/plan/sort";

import type { Change } from "../../src/plan/types";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Create a minimal Change object for testing. */
function makeChange(
  name: string,
  opts?: {
    requires?: string[];
    conflicts?: string[];
    change_id?: string;
    project?: string;
  },
): Change {
  return {
    change_id: opts?.change_id ?? `id-${name}`,
    name,
    project: opts?.project ?? "test",
    note: "",
    planner_name: "Test User",
    planner_email: "test@example.com",
    planned_at: "2025-01-01T00:00:00Z",
    requires: opts?.requires ?? [],
    conflicts: opts?.conflicts ?? [],
  };
}

/** Extract just the names from a Change array, for easy assertion. */
function names(changes: Change[]): string[] {
  return changes.map((c) => c.name);
}

// ---------------------------------------------------------------------------
// topologicalSort
// ---------------------------------------------------------------------------

describe("topologicalSort", () => {
  it("returns empty array for empty input", () => {
    expect(topologicalSort([])).toEqual([]);
  });

  it("returns single change unchanged", () => {
    const changes = [makeChange("A")];
    expect(names(topologicalSort(changes))).toEqual(["A"]);
  });

  it("preserves plan order for sequential (no deps) changes", () => {
    const changes = [
      makeChange("A"),
      makeChange("B"),
      makeChange("C"),
      makeChange("D"),
    ];
    expect(names(topologicalSort(changes))).toEqual(["A", "B", "C", "D"]);
  });

  it("sorts linear dependencies: A -> B -> C", () => {
    const changes = [
      makeChange("C", { requires: ["B"] }),
      makeChange("B", { requires: ["A"] }),
      makeChange("A"),
    ];
    expect(names(topologicalSort(changes))).toEqual(["A", "B", "C"]);
  });

  it("sorts diamond dependencies: D -> B+C, B -> A, C -> A", () => {
    // Diamond: A is at the base, B and C depend on A, D depends on B and C
    const changes = [
      makeChange("D", { requires: ["B", "C"] }),
      makeChange("B", { requires: ["A"] }),
      makeChange("C", { requires: ["A"] }),
      makeChange("A"),
    ];
    const sorted = names(topologicalSort(changes));
    // A must come first
    expect(sorted[0]).toBe("A");
    // B and C must come before D
    expect(sorted.indexOf("B")).toBeLessThan(sorted.indexOf("D"));
    expect(sorted.indexOf("C")).toBeLessThan(sorted.indexOf("D"));
    // D must be last
    expect(sorted[3]).toBe("D");
  });

  it("handles fan-out: A -> B, A -> C, A -> D", () => {
    const changes = [
      makeChange("A"),
      makeChange("B", { requires: ["A"] }),
      makeChange("C", { requires: ["A"] }),
      makeChange("D", { requires: ["A"] }),
    ];
    const sorted = names(topologicalSort(changes));
    expect(sorted[0]).toBe("A");
    // B, C, D can be in any stable order (plan order)
    expect(sorted.slice(1)).toEqual(["B", "C", "D"]);
  });

  it("handles fan-in: B -> D, C -> D", () => {
    const changes = [
      makeChange("B"),
      makeChange("C"),
      makeChange("D", { requires: ["B", "C"] }),
    ];
    const sorted = names(topologicalSort(changes));
    expect(sorted.indexOf("B")).toBeLessThan(sorted.indexOf("D"));
    expect(sorted.indexOf("C")).toBeLessThan(sorted.indexOf("D"));
  });

  it("preserves stable order among independent changes", () => {
    // E has no deps, placed between dependent changes
    const changes = [
      makeChange("A"),
      makeChange("E"),
      makeChange("B", { requires: ["A"] }),
      makeChange("F"),
      makeChange("C", { requires: ["B"] }),
    ];
    const sorted = names(topologicalSort(changes));
    // A < B < C must hold
    expect(sorted.indexOf("A")).toBeLessThan(sorted.indexOf("B"));
    expect(sorted.indexOf("B")).toBeLessThan(sorted.indexOf("C"));
    // E and F should maintain relative plan order among independents
    expect(sorted.indexOf("E")).toBeLessThan(sorted.indexOf("F"));
  });

  it("throws CycleError for direct cycle: A -> B -> A", () => {
    const changes = [
      makeChange("A", { requires: ["B"] }),
      makeChange("B", { requires: ["A"] }),
    ];
    expect(() => topologicalSort(changes)).toThrow(CycleError);
  });

  it("throws CycleError for indirect cycle: A -> B -> C -> A", () => {
    const changes = [
      makeChange("A", { requires: ["C"] }),
      makeChange("B", { requires: ["A"] }),
      makeChange("C", { requires: ["B"] }),
    ];
    expect(() => topologicalSort(changes)).toThrow(CycleError);
  });

  it("cycle error includes the cycle path", () => {
    const changes = [
      makeChange("A", { requires: ["B"] }),
      makeChange("B", { requires: ["A"] }),
    ];
    try {
      topologicalSort(changes);
      expect(true).toBe(false); // should not reach
    } catch (e) {
      expect(e).toBeInstanceOf(CycleError);
      const err = e as CycleError;
      // Cycle path should start and end with the same node
      expect(err.cycle[0]).toBe(err.cycle[err.cycle.length - 1]);
      expect(err.cycle.length).toBeGreaterThanOrEqual(3); // at least A -> B -> A
      expect(err.message).toContain("->");
    }
  });

  it("skips external dependencies not in input set", () => {
    // "ext" is not in changes — treated as already deployed
    const changes = [
      makeChange("A", { requires: ["ext"] }),
      makeChange("B", { requires: ["A"] }),
    ];
    const sorted = names(topologicalSort(changes));
    expect(sorted).toEqual(["A", "B"]);
  });

  it("self-referencing change throws CycleError", () => {
    const changes = [makeChange("A", { requires: ["A"] })];
    expect(() => topologicalSort(changes)).toThrow(CycleError);
  });

  it("handles complex graph correctly", () => {
    // A -> B -> D
    // A -> C -> D
    // E (independent)
    // F -> D
    const changes = [
      makeChange("E"),
      makeChange("A"),
      makeChange("B", { requires: ["A"] }),
      makeChange("C", { requires: ["A"] }),
      makeChange("F"),
      makeChange("D", { requires: ["B", "C", "F"] }),
    ];
    const sorted = names(topologicalSort(changes));
    // A before B and C
    expect(sorted.indexOf("A")).toBeLessThan(sorted.indexOf("B"));
    expect(sorted.indexOf("A")).toBeLessThan(sorted.indexOf("C"));
    // B, C, F before D
    expect(sorted.indexOf("B")).toBeLessThan(sorted.indexOf("D"));
    expect(sorted.indexOf("C")).toBeLessThan(sorted.indexOf("D"));
    expect(sorted.indexOf("F")).toBeLessThan(sorted.indexOf("D"));
  });
});

// ---------------------------------------------------------------------------
// validateDependencies
// ---------------------------------------------------------------------------

describe("validateDependencies", () => {
  it("passes when all requires are in changes", () => {
    const changes = [
      makeChange("A"),
      makeChange("B", { requires: ["A"] }),
    ];
    expect(() => validateDependencies(changes, [])).not.toThrow();
  });

  it("passes when requires are in deployed", () => {
    const changes = [makeChange("B", { requires: ["A"] })];
    expect(() => validateDependencies(changes, ["A"])).not.toThrow();
  });

  it("passes when requires are split between changes and deployed", () => {
    const changes = [
      makeChange("B"),
      makeChange("C", { requires: ["A", "B"] }),
    ];
    expect(() => validateDependencies(changes, ["A"])).not.toThrow();
  });

  it("throws MissingDependencyError for missing require", () => {
    const changes = [makeChange("B", { requires: ["A"] })];
    expect(() => validateDependencies(changes, [])).toThrow(
      MissingDependencyError,
    );
  });

  it("missing dependency error has correct fields", () => {
    const changes = [makeChange("B", { requires: ["A"] })];
    try {
      validateDependencies(changes, []);
      expect(true).toBe(false);
    } catch (e) {
      expect(e).toBeInstanceOf(MissingDependencyError);
      const err = e as MissingDependencyError;
      expect(err.change).toBe("B");
      expect(err.dependency).toBe("A");
    }
  });

  it("throws ConflictError when conflict is deployed", () => {
    const changes = [makeChange("B", { conflicts: ["A"] })];
    expect(() => validateDependencies(changes, ["A"])).toThrow(ConflictError);
  });

  it("conflict error has correct fields", () => {
    const changes = [makeChange("B", { conflicts: ["A"] })];
    try {
      validateDependencies(changes, ["A"]);
      expect(true).toBe(false);
    } catch (e) {
      expect(e).toBeInstanceOf(ConflictError);
      const err = e as ConflictError;
      expect(err.change).toBe("B");
      expect(err.conflictsWith).toBe("A");
    }
  });

  it("does not throw for conflict that is NOT deployed", () => {
    const changes = [makeChange("B", { conflicts: ["A"] })];
    expect(() => validateDependencies(changes, [])).not.toThrow();
  });

  it("does not throw for conflict that is in changes (not deployed)", () => {
    // A conflict with an undeployed change in the same batch is okay
    // (it means the change hasn't been deployed yet, so no conflict)
    const changes = [
      makeChange("A"),
      makeChange("B", { conflicts: ["A"] }),
    ];
    expect(() => validateDependencies(changes, [])).not.toThrow();
  });

  it("handles changes with no deps", () => {
    const changes = [makeChange("A"), makeChange("B")];
    expect(() => validateDependencies(changes, [])).not.toThrow();
  });
});

// ---------------------------------------------------------------------------
// detectCycles
// ---------------------------------------------------------------------------

describe("detectCycles", () => {
  it("returns null for empty input", () => {
    expect(detectCycles([])).toBeNull();
  });

  it("returns null for acyclic graph", () => {
    const changes = [
      makeChange("A"),
      makeChange("B", { requires: ["A"] }),
      makeChange("C", { requires: ["B"] }),
    ];
    expect(detectCycles(changes)).toBeNull();
  });

  it("returns null for independent changes", () => {
    const changes = [makeChange("A"), makeChange("B"), makeChange("C")];
    expect(detectCycles(changes)).toBeNull();
  });

  it("detects direct cycle", () => {
    const changes = [
      makeChange("A", { requires: ["B"] }),
      makeChange("B", { requires: ["A"] }),
    ];
    const err = detectCycles(changes);
    expect(err).toBeInstanceOf(CycleError);
    expect(err!.cycle[0]).toBe(err!.cycle[err!.cycle.length - 1]);
  });

  it("detects indirect cycle of length 3", () => {
    const changes = [
      makeChange("A", { requires: ["C"] }),
      makeChange("B", { requires: ["A"] }),
      makeChange("C", { requires: ["B"] }),
    ];
    const err = detectCycles(changes);
    expect(err).toBeInstanceOf(CycleError);
    expect(err!.cycle.length).toBe(4); // A -> B -> C -> A or similar
  });

  it("detects self-referencing cycle", () => {
    const changes = [makeChange("A", { requires: ["A"] })];
    const err = detectCycles(changes);
    expect(err).toBeInstanceOf(CycleError);
  });

  it("ignores external dependencies when checking cycles", () => {
    // "ext" is not in changes — it's an external/deployed dep
    const changes = [makeChange("A", { requires: ["ext"] })];
    const err = detectCycles(changes);
    expect(err).toBeNull();
  });

  it("returns CycleError with meaningful message", () => {
    const changes = [
      makeChange("X", { requires: ["Y"] }),
      makeChange("Y", { requires: ["X"] }),
    ];
    const err = detectCycles(changes);
    expect(err).not.toBeNull();
    expect(err!.message).toContain("->");
    expect(err!.message).toContain("cycle");
  });
});

// ---------------------------------------------------------------------------
// filterPending
// ---------------------------------------------------------------------------

describe("filterPending", () => {
  it("returns all changes when none deployed", () => {
    const changes = [makeChange("A"), makeChange("B")];
    expect(names(filterPending(changes, []))).toEqual(["A", "B"]);
  });

  it("filters out deployed changes by change_id", () => {
    const changes = [
      makeChange("A", { change_id: "id-1" }),
      makeChange("B", { change_id: "id-2" }),
      makeChange("C", { change_id: "id-3" }),
    ];
    expect(names(filterPending(changes, ["id-1", "id-3"]))).toEqual(["B"]);
  });

  it("returns empty when all deployed", () => {
    const changes = [
      makeChange("A", { change_id: "id-1" }),
      makeChange("B", { change_id: "id-2" }),
    ];
    expect(filterPending(changes, ["id-1", "id-2"])).toEqual([]);
  });

  it("preserves order of pending changes", () => {
    const changes = [
      makeChange("A", { change_id: "id-1" }),
      makeChange("B", { change_id: "id-2" }),
      makeChange("C", { change_id: "id-3" }),
      makeChange("D", { change_id: "id-4" }),
    ];
    expect(names(filterPending(changes, ["id-2", "id-4"]))).toEqual([
      "A",
      "C",
    ]);
  });

  it("handles empty changes", () => {
    expect(filterPending([], ["id-1"])).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// filterToTarget
// ---------------------------------------------------------------------------

describe("filterToTarget", () => {
  it("returns all changes up to target", () => {
    const changes = [
      makeChange("A"),
      makeChange("B"),
      makeChange("C"),
      makeChange("D"),
    ];
    expect(names(filterToTarget(changes, "C"))).toEqual(["A", "B", "C"]);
  });

  it("returns single change when target is first", () => {
    const changes = [makeChange("A"), makeChange("B"), makeChange("C")];
    expect(names(filterToTarget(changes, "A"))).toEqual(["A"]);
  });

  it("returns all changes when target is last", () => {
    const changes = [makeChange("A"), makeChange("B"), makeChange("C")];
    expect(names(filterToTarget(changes, "C"))).toEqual(["A", "B", "C"]);
  });

  it("throws when target not found", () => {
    const changes = [makeChange("A"), makeChange("B")];
    expect(() => filterToTarget(changes, "Z")).toThrow(
      'Target change "Z" not found in plan',
    );
  });

  it("handles single-element list", () => {
    const changes = [makeChange("A")];
    expect(names(filterToTarget(changes, "A"))).toEqual(["A"]);
  });
});

// ---------------------------------------------------------------------------
// Integration: sort + validate + filter
// ---------------------------------------------------------------------------

describe("integration: sort + validate + filter", () => {
  it("full pipeline: filter pending, validate, sort", () => {
    const all = [
      makeChange("schema", { change_id: "deployed-1" }),
      makeChange("users", { change_id: "deployed-2", requires: ["schema"] }),
      makeChange("orders", { change_id: "pending-1", requires: ["users"] }),
      makeChange("items", { change_id: "pending-2", requires: ["orders"] }),
    ];
    const pending = filterPending(all, ["deployed-1", "deployed-2"]);
    expect(names(pending)).toEqual(["orders", "items"]);

    // Validate with deployed names
    validateDependencies(pending, ["schema", "users"]);

    const sorted = topologicalSort(pending);
    expect(names(sorted)).toEqual(["orders", "items"]);
  });

  it("filter to target then sort", () => {
    const changes = [
      makeChange("A"),
      makeChange("B", { requires: ["A"] }),
      makeChange("C", { requires: ["B"] }),
      makeChange("D", { requires: ["C"] }),
    ];
    const subset = filterToTarget(changes, "C");
    const sorted = topologicalSort(subset);
    expect(names(sorted)).toEqual(["A", "B", "C"]);
  });

  it("conflict blocks deploy even when other deps satisfied", () => {
    const changes = [
      makeChange("new_feature", {
        requires: ["schema"],
        conflicts: ["old_feature"],
      }),
    ];
    expect(() =>
      validateDependencies(changes, ["schema", "old_feature"]),
    ).toThrow(ConflictError);
  });
});
