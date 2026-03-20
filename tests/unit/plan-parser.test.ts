// tests/unit/plan-parser.test.ts — Tests for the sqitch.plan parser
//
// Validates parsePlan(), parseDependencies(), and PlanParseError.
// Includes customer-zero fixture test with 255 changes.

import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";

import { parsePlan, parseDependencies, PlanParseError } from "../../src/plan/parser";
import { computeChangeId, computeTagId } from "../../src/plan/types";
// Types are used via inference from parsePlan() return type

const FIXTURES_DIR = join(import.meta.dir, "..", "fixtures");

// ---------------------------------------------------------------------------
// Helper: build a minimal valid plan string
// ---------------------------------------------------------------------------
function minimalPlan(lines: string[] = []): string {
  return [
    "%syntax-version=1.0.0",
    "%project=testproject",
    "",
    ...lines,
  ].join("\n");
}

function planWithUri(lines: string[] = []): string {
  return [
    "%syntax-version=1.0.0",
    "%project=testproject",
    "%uri=https://example.com/testproject",
    "",
    ...lines,
  ].join("\n");
}

// ---------------------------------------------------------------------------
// parseDependencies
// ---------------------------------------------------------------------------
describe("parseDependencies", () => {
  it("parses empty dependency list", () => {
    const deps = parseDependencies("");
    expect(deps).toEqual([]);
  });

  it("parses single require dependency", () => {
    const deps = parseDependencies("create_schema");
    expect(deps).toEqual([{ type: "require", name: "create_schema" }]);
  });

  it("parses multiple require dependencies", () => {
    const deps = parseDependencies("dep1 dep2 dep3");
    expect(deps).toHaveLength(3);
    expect(deps[0]).toEqual({ type: "require", name: "dep1" });
    expect(deps[1]).toEqual({ type: "require", name: "dep2" });
    expect(deps[2]).toEqual({ type: "require", name: "dep3" });
  });

  it("parses conflict dependency with ! prefix", () => {
    const deps = parseDependencies("!old_users");
    expect(deps).toEqual([{ type: "conflict", name: "old_users" }]);
  });

  it("parses mixed requires and conflicts", () => {
    const deps = parseDependencies("dep1 !conflict1 dep2 !conflict2");
    expect(deps).toHaveLength(4);
    expect(deps[0]).toEqual({ type: "require", name: "dep1" });
    expect(deps[1]).toEqual({ type: "conflict", name: "conflict1" });
    expect(deps[2]).toEqual({ type: "require", name: "dep2" });
    expect(deps[3]).toEqual({ type: "conflict", name: "conflict2" });
  });

  it("parses cross-project dependency with project:change syntax", () => {
    const deps = parseDependencies("otherproject:create_schema");
    expect(deps).toEqual([
      { type: "require", name: "create_schema", project: "otherproject" },
    ]);
  });

  it("parses cross-project conflict", () => {
    const deps = parseDependencies("!otherproject:old_thing");
    expect(deps).toEqual([
      { type: "conflict", name: "old_thing", project: "otherproject" },
    ]);
  });

  it("parses reworked change reference with @tag syntax", () => {
    const deps = parseDependencies("add_users@v1.0");
    expect(deps).toEqual([{ type: "require", name: "add_users@v1.0" }]);
  });

  it("handles extra whitespace between dependencies", () => {
    const deps = parseDependencies("  dep1   dep2   ");
    expect(deps).toHaveLength(2);
    expect(deps[0]!.name).toBe("dep1");
    expect(deps[1]!.name).toBe("dep2");
  });
});

// ---------------------------------------------------------------------------
// parsePlan — pragmas
// ---------------------------------------------------------------------------
describe("parsePlan — pragmas", () => {
  it("parses required %project pragma", () => {
    const plan = parsePlan(minimalPlan());
    expect(plan.project.name).toBe("testproject");
  });

  it("parses %uri pragma into project.uri", () => {
    const plan = parsePlan(planWithUri());
    expect(plan.project.uri).toBe("https://example.com/testproject");
  });

  it("parses %syntax-version pragma", () => {
    const plan = parsePlan(minimalPlan());
    expect(plan.pragmas.get("syntax-version")).toBe("1.0.0");
  });

  it("stores all pragmas in the pragmas map", () => {
    const plan = parsePlan(planWithUri());
    expect(plan.pragmas.get("project")).toBe("testproject");
    expect(plan.pragmas.get("uri")).toBe("https://example.com/testproject");
    expect(plan.pragmas.get("syntax-version")).toBe("1.0.0");
  });

  it("throws when %project pragma is missing", () => {
    const content = "%syntax-version=1.0.0\n";
    expect(() => parsePlan(content)).toThrow(PlanParseError);
    expect(() => parsePlan(content)).toThrow("Missing required %project pragma");
  });

  it("project.uri is undefined when %uri pragma is absent", () => {
    const plan = parsePlan(minimalPlan());
    expect(plan.project.uri).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// parsePlan — empty and minimal plans
// ---------------------------------------------------------------------------
describe("parsePlan — empty and minimal plans", () => {
  it("parses plan with only pragmas (no changes)", () => {
    const plan = parsePlan(minimalPlan());
    expect(plan.changes).toHaveLength(0);
    expect(plan.tags).toHaveLength(0);
  });

  it("parses plan with pragmas and blank lines", () => {
    const plan = parsePlan(minimalPlan(["", "", ""]));
    expect(plan.changes).toHaveLength(0);
  });

  it("parses plan with comments only", () => {
    const plan = parsePlan(
      minimalPlan(["# This is a comment", "# Another comment"]),
    );
    expect(plan.changes).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// parsePlan — single change
// ---------------------------------------------------------------------------
describe("parsePlan — change parsing", () => {
  it("parses a single change entry", () => {
    const plan = parsePlan(
      minimalPlan([
        "first_change 2024-01-15T10:30:00Z Test User <test@example.com> # initial change",
      ]),
    );
    expect(plan.changes).toHaveLength(1);
    const change = plan.changes[0]!;
    expect(change.name).toBe("first_change");
    expect(change.planned_at).toBe("2024-01-15T10:30:00Z");
    expect(change.planner_name).toBe("Test User");
    expect(change.planner_email).toBe("test@example.com");
    expect(change.note).toBe("initial change");
    expect(change.project).toBe("testproject");
  });

  it("parses change without note", () => {
    const plan = parsePlan(
      minimalPlan([
        "first_change 2024-01-15T10:30:00Z Test User <test@example.com>",
      ]),
    );
    expect(plan.changes).toHaveLength(1);
    expect(plan.changes[0]!.note).toBe("");
  });

  it("parses change with dependencies", () => {
    const plan = parsePlan(
      minimalPlan([
        "first_change 2024-01-15T10:30:00Z A <a@b.com> # first",
        "second_change [first_change] 2024-01-15T10:31:00Z A <a@b.com> # depends on first",
      ]),
    );
    expect(plan.changes).toHaveLength(2);
    const second = plan.changes[1]!;
    expect(second.requires).toEqual(["first_change"]);
    expect(second.conflicts).toEqual([]);
  });

  it("parses change with conflict dependencies", () => {
    const plan = parsePlan(
      minimalPlan([
        "new_thing [dep1 !old_thing] 2024-01-15T10:30:00Z A <a@b.com> # new",
      ]),
    );
    const change = plan.changes[0]!;
    expect(change.requires).toEqual(["dep1"]);
    expect(change.conflicts).toEqual(["old_thing"]);
  });

  it("parses change with cross-project dependency", () => {
    const plan = parsePlan(
      minimalPlan([
        "my_change [other_project:base_schema] 2024-01-15T10:30:00Z A <a@b.com> # cross-project",
      ]),
    );
    const change = plan.changes[0]!;
    expect(change.requires).toEqual(["other_project:base_schema"]);
  });

  it("handles commas in planner name", () => {
    const plan = parsePlan(
      minimalPlan([
        "my_change 2024-01-15T10:30:00Z Dmitry,Udalov,, <dmius@dev># note",
      ]),
    );
    const change = plan.changes[0]!;
    expect(change.planner_name).toBe("Dmitry,Udalov,,");
    expect(change.planner_email).toBe("dmius@dev");
  });

  it("handles no space before # in note", () => {
    const plan = parsePlan(
      minimalPlan([
        "my_change 2024-01-15T10:30:00Z User <u@e.com># no space note",
      ]),
    );
    expect(plan.changes[0]!.note).toBe("no space note");
  });

  it("handles double space before <email>", () => {
    const plan = parsePlan(
      minimalPlan([
        "my_change 2024-01-15T10:30:00Z User Name  <u@e.com> # note",
      ]),
    );
    expect(plan.changes[0]!.planner_name).toBe("User Name");
    expect(plan.changes[0]!.planner_email).toBe("u@e.com");
  });

  it("handles planner name that is just whitespace", () => {
    // Edge case from customer-zero: "  <sqitch@01157dfe3b0b>"
    const plan = parsePlan(
      minimalPlan([
        "my_change 2024-01-15T10:30:00Z   <sqitch@host> # note",
      ]),
    );
    expect(plan.changes[0]!.planner_name).toBe("");
    expect(plan.changes[0]!.planner_email).toBe("sqitch@host");
  });
});

// ---------------------------------------------------------------------------
// parsePlan — parent chain
// ---------------------------------------------------------------------------
describe("parsePlan — parent chain", () => {
  it("first change has no parent", () => {
    const plan = parsePlan(
      minimalPlan([
        "first 2024-01-15T10:30:00Z A <a@b.com> # first",
      ]),
    );
    expect(plan.changes[0]!.parent).toBeUndefined();
  });

  it("second change has first change as parent", () => {
    const plan = parsePlan(
      minimalPlan([
        "first 2024-01-15T10:30:00Z A <a@b.com> # first",
        "second 2024-01-15T10:31:00Z A <a@b.com> # second",
      ]),
    );
    expect(plan.changes[1]!.parent).toBe(plan.changes[0]!.change_id);
  });

  it("parent chain threads through all changes", () => {
    const plan = parsePlan(
      minimalPlan([
        "first 2024-01-15T10:30:00Z A <a@b.com> # 1",
        "second 2024-01-15T10:31:00Z A <a@b.com> # 2",
        "third 2024-01-15T10:32:00Z A <a@b.com> # 3",
      ]),
    );
    expect(plan.changes[0]!.parent).toBeUndefined();
    expect(plan.changes[1]!.parent).toBe(plan.changes[0]!.change_id);
    expect(plan.changes[2]!.parent).toBe(plan.changes[1]!.change_id);
  });
});

// ---------------------------------------------------------------------------
// parsePlan — change IDs
// ---------------------------------------------------------------------------
describe("parsePlan — change ID computation", () => {
  it("computes correct change_id for first change (no parent, no URI)", () => {
    const plan = parsePlan(
      minimalPlan([
        "first_change 2024-01-15T12:00:00Z Test User <test@example.com> #",
      ]),
    );
    const change = plan.changes[0]!;

    // Manually compute expected ID
    const expected = computeChangeId({
      project: "testproject",
      change: "first_change",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      requires: [],
      conflicts: [],
      note: "",
    });

    expect(change.change_id).toBe(expected);
  });

  it("computes correct change_id with URI", () => {
    const plan = parsePlan(
      planWithUri([
        "first_change 2024-01-15T12:00:00Z Test User <test@example.com> # a note",
      ]),
    );
    const change = plan.changes[0]!;

    const expected = computeChangeId({
      project: "testproject",
      uri: "https://example.com/testproject",
      change: "first_change",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T12:00:00Z",
      requires: [],
      conflicts: [],
      note: "a note",
    });

    expect(change.change_id).toBe(expected);
  });

  it("change IDs are deterministic", () => {
    const content = minimalPlan([
      "first 2024-01-15T12:00:00Z A <a@b.com> # note",
    ]);
    const plan1 = parsePlan(content);
    const plan2 = parsePlan(content);
    expect(plan1.changes[0]!.change_id).toBe(plan2.changes[0]!.change_id);
  });

  it("change IDs are unique across changes", () => {
    const plan = parsePlan(
      minimalPlan([
        "first 2024-01-15T12:00:00Z A <a@b.com> # 1",
        "second 2024-01-15T12:01:00Z A <a@b.com> # 2",
        "third 2024-01-15T12:02:00Z A <a@b.com> # 3",
      ]),
    );
    const ids = plan.changes.map((c) => c.change_id);
    const uniqueIds = new Set(ids);
    expect(uniqueIds.size).toBe(ids.length);
  });

  it("all change_ids are 40-character hex strings", () => {
    const plan = parsePlan(
      minimalPlan([
        "first 2024-01-15T12:00:00Z A <a@b.com> # 1",
        "second 2024-01-15T12:01:00Z A <a@b.com> # 2",
      ]),
    );
    for (const change of plan.changes) {
      expect(change.change_id).toMatch(/^[0-9a-f]{40}$/);
    }
  });
});

// ---------------------------------------------------------------------------
// parsePlan — tags
// ---------------------------------------------------------------------------
describe("parsePlan — tags", () => {
  it("parses a tag entry after a change", () => {
    const plan = parsePlan(
      minimalPlan([
        "first_change 2024-01-15T10:30:00Z A <a@b.com> # first",
        "@v1.0 2024-01-15T10:31:00Z A <a@b.com> # tag note",
      ]),
    );
    expect(plan.tags).toHaveLength(1);
    const tag = plan.tags[0]!;
    expect(tag.name).toBe("v1.0");
    expect(tag.note).toBe("tag note");
    expect(tag.planner_name).toBe("A");
    expect(tag.planner_email).toBe("a@b.com");
    expect(tag.planned_at).toBe("2024-01-15T10:31:00Z");
    expect(tag.project).toBe("testproject");
  });

  it("tag links to preceding change via change_id", () => {
    const plan = parsePlan(
      minimalPlan([
        "first_change 2024-01-15T10:30:00Z A <a@b.com> # first",
        "@v1.0 2024-01-15T10:31:00Z A <a@b.com> # tag",
      ]),
    );
    expect(plan.tags[0]!.change_id).toBe(plan.changes[0]!.change_id);
  });

  it("computes correct tag_id", () => {
    const plan = parsePlan(
      minimalPlan([
        "first_change 2024-01-15T10:30:00Z A <a@b.com> # first",
        "@v1.0 2024-01-15T10:31:00Z A <a@b.com> # tag note",
      ]),
    );
    const tag = plan.tags[0]!;

    const expected = computeTagId({
      project: "testproject",
      tag: "v1.0",
      change_id: plan.changes[0]!.change_id,
      planner_name: "A",
      planner_email: "a@b.com",
      planned_at: "2024-01-15T10:31:00Z",
      note: "tag note",
    });

    expect(tag.tag_id).toBe(expected);
  });

  it("tag_id is a 40-character hex string", () => {
    const plan = parsePlan(
      minimalPlan([
        "first_change 2024-01-15T10:30:00Z A <a@b.com> # first",
        "@v1.0 2024-01-15T10:31:00Z A <a@b.com> # tag",
      ]),
    );
    expect(plan.tags[0]!.tag_id).toMatch(/^[0-9a-f]{40}$/);
  });

  it("throws when tag appears before any change", () => {
    expect(() =>
      parsePlan(minimalPlan(["@v1.0 2024-01-15T10:31:00Z A <a@b.com> # tag"])),
    ).toThrow("Tag before any change");
  });

  it("multiple tags can appear after a single change", () => {
    const plan = parsePlan(
      minimalPlan([
        "first 2024-01-15T10:30:00Z A <a@b.com> # first",
        "@v1.0 2024-01-15T10:31:00Z A <a@b.com> # tag 1",
        "@v1.1 2024-01-15T10:32:00Z A <a@b.com> # tag 2",
      ]),
    );
    expect(plan.tags).toHaveLength(2);
    expect(plan.tags[0]!.name).toBe("v1.0");
    expect(plan.tags[1]!.name).toBe("v1.1");
    // Both attach to the same change
    expect(plan.tags[0]!.change_id).toBe(plan.changes[0]!.change_id);
    expect(plan.tags[1]!.change_id).toBe(plan.changes[0]!.change_id);
  });
});

// ---------------------------------------------------------------------------
// parsePlan — reworked changes
// ---------------------------------------------------------------------------
describe("parsePlan — reworked changes", () => {
  it("parses reworked change (same name, different entry)", () => {
    const plan = parsePlan(
      minimalPlan([
        "add_users 2024-01-01T00:00:00Z User <user@example.com> # add users table",
        "@v1.0 2024-01-01T00:01:00Z User <user@example.com> # tag v1.0",
        "add_users [add_users@v1.0] 2024-02-01T00:00:00Z User <user@example.com> # rework users",
      ]),
    );
    expect(plan.changes).toHaveLength(2);
    expect(plan.changes[0]!.name).toBe("add_users");
    expect(plan.changes[1]!.name).toBe("add_users");
    // Different IDs
    expect(plan.changes[0]!.change_id).not.toBe(plan.changes[1]!.change_id);
    // Second change depends on first via @v1.0
    expect(plan.changes[1]!.requires).toEqual(["add_users@v1.0"]);
  });

  it("reworked change has correct parent (previous change in plan order)", () => {
    const plan = parsePlan(
      minimalPlan([
        "add_users 2024-01-01T00:00:00Z User <user@example.com> # first",
        "@v1.0 2024-01-01T00:01:00Z User <user@example.com> # tag",
        "add_users [add_users@v1.0] 2024-02-01T00:00:00Z User <user@example.com> # rework",
      ]),
    );
    // Parent of reworked change is the first change's ID
    expect(plan.changes[1]!.parent).toBe(plan.changes[0]!.change_id);
  });
});

// ---------------------------------------------------------------------------
// parsePlan — comments and blank lines
// ---------------------------------------------------------------------------
describe("parsePlan — comments and blank lines", () => {
  it("skips comment lines starting with #", () => {
    const plan = parsePlan(
      minimalPlan([
        "# This is a comment",
        "first 2024-01-15T10:30:00Z A <a@b.com> # note",
        "# Another comment",
        "second 2024-01-15T10:31:00Z A <a@b.com> # note",
      ]),
    );
    expect(plan.changes).toHaveLength(2);
  });

  it("skips blank lines anywhere in the plan", () => {
    const plan = parsePlan(
      minimalPlan([
        "",
        "first 2024-01-15T10:30:00Z A <a@b.com> # note",
        "",
        "",
        "second 2024-01-15T10:31:00Z A <a@b.com> # note",
        "",
      ]),
    );
    expect(plan.changes).toHaveLength(2);
  });

  it("blank lines between changes do not break parent chain", () => {
    const plan = parsePlan(
      minimalPlan([
        "first 2024-01-15T10:30:00Z A <a@b.com> # 1",
        "",
        "second 2024-01-15T10:31:00Z A <a@b.com> # 2",
      ]),
    );
    expect(plan.changes[1]!.parent).toBe(plan.changes[0]!.change_id);
  });
});

// ---------------------------------------------------------------------------
// parsePlan — edge cases
// ---------------------------------------------------------------------------
describe("parsePlan — edge cases", () => {
  it("handles unicode in change names and notes", () => {
    const plan = parsePlan(
      minimalPlan([
        "add_ñoño 2024-01-15T10:30:00Z José García <jose@example.com> # Añadir tabla de usuarios",
      ]),
    );
    const change = plan.changes[0]!;
    expect(change.name).toBe("add_ñoño");
    expect(change.planner_name).toBe("José García");
    expect(change.note).toBe("Añadir tabla de usuarios");
  });

  it("handles trailing whitespace on lines", () => {
    const plan = parsePlan(
      minimalPlan([
        "first 2024-01-15T10:30:00Z A <a@b.com> # note   ",
      ]),
    );
    expect(plan.changes[0]!.note).toBe("note");
  });

  it("handles empty note after #", () => {
    const plan = parsePlan(
      minimalPlan([
        "first 2024-01-15T10:30:00Z A <a@b.com> #",
      ]),
    );
    expect(plan.changes[0]!.note).toBe("");
  });

  it("handles note with colons and special characters", () => {
    const plan = parsePlan(
      minimalPlan([
        "my_change 2024-01-15T10:30:00Z A <a@b.com> # API improvements: update clone.",
      ]),
    );
    expect(plan.changes[0]!.note).toBe("API improvements: update clone.");
  });

  it("handles escaped newline in note from customer-zero", () => {
    // This is the raw literal from customer-zero line 44:
    // The note contains literal \n characters (not actual newlines)
    const plan = parsePlan(
      minimalPlan([
        String.raw`my_change 2024-01-15T10:30:00Z A <a@b.com> # Logging imrovements, some fixes.\nPlease enter a note`,
      ]),
    );
    expect(plan.changes[0]!.note).toBe(
      String.raw`Logging imrovements, some fixes.\nPlease enter a note`,
    );
  });

  it("throws PlanParseError on missing timestamp", () => {
    expect(() =>
      parsePlan(minimalPlan(["bad_line no_timestamp A <a@b.com> # note"])),
    ).toThrow(PlanParseError);
    expect(() =>
      parsePlan(minimalPlan(["bad_line no_timestamp A <a@b.com> # note"])),
    ).toThrow("Missing timestamp");
  });

  it("throws PlanParseError on missing email", () => {
    expect(() =>
      parsePlan(
        minimalPlan(["bad_line 2024-01-15T10:30:00Z no_email_here # note"]),
      ),
    ).toThrow(PlanParseError);
    expect(() =>
      parsePlan(
        minimalPlan(["bad_line 2024-01-15T10:30:00Z no_email_here # note"]),
      ),
    ).toThrow("Missing planner email");
  });
});

// ---------------------------------------------------------------------------
// parsePlan — full plan round-trip
// ---------------------------------------------------------------------------
describe("parsePlan — complex plan", () => {
  it("parses a complete plan with changes, tags, deps, and reworks", () => {
    const content = [
      "%syntax-version=1.0.0",
      "%project=myproject",
      "%uri=https://example.com/",
      "",
      "create_schema 2024-01-01T00:00:00Z Dev <dev@example.com> # Create schema",
      "add_users [create_schema] 2024-01-02T00:00:00Z Dev <dev@example.com> # Add users table",
      "add_posts [add_users] 2024-01-03T00:00:00Z Dev <dev@example.com> # Add posts table",
      "@v1.0 2024-01-03T00:01:00Z Dev <dev@example.com> # Version 1.0",
      "# Rework add_users for v2",
      "add_users [add_users@v1.0] 2024-02-01T00:00:00Z Dev <dev@example.com> # Rework users with email validation",
      "@v2.0 2024-02-01T00:01:00Z Dev <dev@example.com> # Version 2.0",
    ].join("\n");

    const plan = parsePlan(content);

    // 4 changes total (create_schema, add_users, add_posts, reworked add_users)
    expect(plan.changes).toHaveLength(4);
    expect(plan.tags).toHaveLength(2);

    // Project
    expect(plan.project.name).toBe("myproject");
    expect(plan.project.uri).toBe("https://example.com/");

    // Tags link to correct changes
    expect(plan.tags[0]!.name).toBe("v1.0");
    expect(plan.tags[0]!.change_id).toBe(plan.changes[2]!.change_id); // add_posts
    expect(plan.tags[1]!.name).toBe("v2.0");
    expect(plan.tags[1]!.change_id).toBe(plan.changes[3]!.change_id); // reworked add_users

    // Parent chain
    expect(plan.changes[0]!.parent).toBeUndefined();
    expect(plan.changes[1]!.parent).toBe(plan.changes[0]!.change_id);
    expect(plan.changes[2]!.parent).toBe(plan.changes[1]!.change_id);
    expect(plan.changes[3]!.parent).toBe(plan.changes[2]!.change_id);

    // Reworked change
    expect(plan.changes[1]!.name).toBe("add_users"); // original
    expect(plan.changes[3]!.name).toBe("add_users"); // reworked
    expect(plan.changes[3]!.requires).toEqual(["add_users@v1.0"]);
  });
});

// ---------------------------------------------------------------------------
// PlanParseError
// ---------------------------------------------------------------------------
describe("PlanParseError", () => {
  it("includes line number in error message", () => {
    try {
      parsePlan(
        minimalPlan(["bad_line_no_timestamp A <a@b.com> # note"]),
      );
      expect(true).toBe(false); // should not reach
    } catch (e) {
      expect(e).toBeInstanceOf(PlanParseError);
      if (e instanceof PlanParseError) {
        expect(e.line).toBeGreaterThan(0);
        expect(e.message).toContain("line");
      }
    }
  });

  it("includes offending line content", () => {
    try {
      parsePlan(
        minimalPlan(["bad_line_no_timestamp A <a@b.com> # note"]),
      );
      expect(true).toBe(false);
    } catch (e) {
      expect(e).toBeInstanceOf(PlanParseError);
      if (e instanceof PlanParseError) {
        expect(e.content).toContain("bad_line_no_timestamp");
      }
    }
  });
});

// ---------------------------------------------------------------------------
// Customer-zero fixture
// ---------------------------------------------------------------------------
describe("customer-zero fixture", () => {
  const content = readFileSync(
    join(FIXTURES_DIR, "customer-zero.plan"),
    "utf-8",
  );

  it("parses all 255 changes from PostgresAI Console plan", () => {
    const plan = parsePlan(content);
    expect(plan.changes).toHaveLength(255);
  });

  it("has correct project name and URI", () => {
    const plan = parsePlan(content);
    expect(plan.project.name).toBe("postgres_ai");
    expect(plan.project.uri).toBe("https://gitlab.com/postgres-ai/platform/");
  });

  it("has three pragmas", () => {
    const plan = parsePlan(content);
    expect(plan.pragmas.size).toBe(3);
    expect(plan.pragmas.get("syntax-version")).toBe("1.0.0");
  });

  it("first change is 20190726_init_api", () => {
    const plan = parsePlan(content);
    const first = plan.changes[0]!;
    expect(first.name).toBe("20190726_init_api");
    expect(first.planned_at).toBe("2019-07-26T13:24:32Z");
    expect(first.planner_name).toBe("Dmitry,Udalov,,");
    expect(first.planner_email).toBe("dmius@dev");
    expect(first.note).toBe("Init Rest API");
    expect(first.parent).toBeUndefined();
  });

  it("last change is 20260311_fix_telemetry_usage_billing_cycle_anchor_cast", () => {
    const plan = parsePlan(content);
    const last = plan.changes[plan.changes.length - 1]!;
    expect(last.name).toBe(
      "20260311_fix_telemetry_usage_billing_cycle_anchor_cast",
    );
    expect(last.planned_at).toBe("2026-03-11T19:00:00Z");
  });

  it("handles commas in planner names from early entries", () => {
    const plan = parsePlan(content);
    // Lines 5, 6, 7 have Dmitry,Udalov,,
    expect(plan.changes[0]!.planner_name).toBe("Dmitry,Udalov,,");
    expect(plan.changes[1]!.planner_name).toBe("Dmitry,Udalov,,");
    expect(plan.changes[2]!.planner_name).toBe("Dmitry,Udalov,,");
  });

  it("handles empty planner names (just whitespace)", () => {
    const plan = parsePlan(content);
    // Line 157: "20251013_toggle_ai_models ... <sqitch@01157dfe3b0b>"
    // Planner name is "  " which trims to ""
    const change = plan.changes.find(
      (c) => c.name === "20251013_toggle_ai_models",
    );
    expect(change).toBeDefined();
    expect(change!.planner_name).toBe("");
    expect(change!.planner_email).toBe("sqitch@01157dfe3b0b");
  });

  it("handles double space before email angle bracket", () => {
    const plan = parsePlan(content);
    // Line 14: Dmitry Udalov  <dmius@postgres.ai>
    const change = plan.changes.find((c) => c.name === "20191114_dblab");
    expect(change).toBeDefined();
    expect(change!.planner_name).toBe("Dmitry Udalov");
  });

  it("handles note without space after #", () => {
    const plan = parsePlan(content);
    // Line 5: <dmius@dev># Init Rest API
    expect(plan.changes[0]!.note).toBe("Init Rest API");
  });

  it("handles blank line in middle of plan (line 216)", () => {
    // The blank line at line 216 should be silently skipped
    const plan = parsePlan(content);
    expect(plan.changes).toHaveLength(255);
  });

  it("all change IDs are valid 40-char hex strings", () => {
    const plan = parsePlan(content);
    for (const change of plan.changes) {
      expect(change.change_id).toMatch(/^[0-9a-f]{40}$/);
    }
  });

  it("all change IDs are unique", () => {
    const plan = parsePlan(content);
    const ids = plan.changes.map((c) => c.change_id);
    const uniqueIds = new Set(ids);
    expect(uniqueIds.size).toBe(255);
  });

  it("parent chain is continuous — each change references the previous", () => {
    const plan = parsePlan(content);
    for (let i = 0; i < plan.changes.length; i++) {
      const change = plan.changes[i]!;
      if (i === 0) {
        expect(change.parent).toBeUndefined();
      } else {
        expect(change.parent).toBe(plan.changes[i - 1]!.change_id);
      }
    }
  });

  it("re-parsing produces identical results", () => {
    const plan1 = parsePlan(content);
    const plan2 = parsePlan(content);

    expect(plan1.changes.length).toBe(plan2.changes.length);
    for (let i = 0; i < plan1.changes.length; i++) {
      expect(plan1.changes[i]!.change_id).toBe(plan2.changes[i]!.change_id);
      expect(plan1.changes[i]!.name).toBe(plan2.changes[i]!.name);
      expect(plan1.changes[i]!.parent).toBe(plan2.changes[i]!.parent);
    }
  });

  it("has no tags (customer-zero plan has no @tags)", () => {
    const plan = parsePlan(content);
    expect(plan.tags).toHaveLength(0);
  });

  it("parses the literal backslash-n in note on line 44", () => {
    const plan = parsePlan(content);
    const change = plan.changes.find(
      (c) => c.name === "20200907_billing_impovements",
    );
    expect(change).toBeDefined();
    expect(change!.note).toContain(String.raw`\n`);
    expect(change!.note).toContain("Please enter a note");
  });
});
