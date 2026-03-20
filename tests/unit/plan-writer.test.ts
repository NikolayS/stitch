// tests/unit/plan-writer.test.ts — Tests for sqitch.plan writer
//
// Validates serializePlan, serializeChange, serializeTag, appendChange,
// and appendTag produce Sqitch-compatible output.

import { describe, expect, it, beforeEach, afterEach } from "bun:test";
import { mkdtempSync, writeFileSync, readFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

import {
  serializeChange,
  serializeTag,
  serializePlan,
  appendChange,
  appendTag,
} from "../../src/plan/writer";

import type { Change, Tag, Plan } from "../../src/plan/types";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeChange(overrides: Partial<Change> = {}): Change {
  return {
    change_id: "abc123def456",
    name: "add_users",
    project: "myproject",
    note: "Add users table",
    planner_name: "Test User",
    planner_email: "test@example.com",
    planned_at: "2024-01-15T10:30:00Z",
    requires: [],
    conflicts: [],
    ...overrides,
  };
}

function makeTag(overrides: Partial<Tag> = {}): Tag {
  return {
    tag_id: "tag123",
    name: "v1.0",
    project: "myproject",
    change_id: "abc123def456",
    note: "First release",
    planner_name: "Test User",
    planner_email: "test@example.com",
    planned_at: "2024-01-15T10:31:00Z",
    ...overrides,
  };
}

function makePlan(overrides: Partial<Plan> = {}): Plan {
  return {
    project: { name: "myproject" },
    pragmas: new Map([
      ["syntax-version", "1.0.0"],
      ["project", "myproject"],
    ]),
    changes: [],
    tags: [],
    ...overrides,
  };
}

let tmpDir: string;

// ---------------------------------------------------------------------------
// serializeChange
// ---------------------------------------------------------------------------

describe("serializeChange", () => {
  it("formats a minimal change (no deps, no note)", () => {
    const change = makeChange({ note: "", requires: [], conflicts: [] });
    const line = serializeChange(change);
    expect(line).toBe(
      "add_users 2024-01-15T10:30:00Z Test User <test@example.com>",
    );
  });

  it("formats a change with a note", () => {
    const change = makeChange({ note: "Add users table" });
    const line = serializeChange(change);
    expect(line).toBe(
      "add_users 2024-01-15T10:30:00Z Test User <test@example.com> # Add users table",
    );
  });

  it("formats a change with requires dependencies", () => {
    const change = makeChange({
      requires: ["create_schema", "add_roles"],
      note: "",
    });
    const line = serializeChange(change);
    expect(line).toBe(
      "add_users [create_schema add_roles] 2024-01-15T10:30:00Z Test User <test@example.com>",
    );
  });

  it("formats a change with conflict dependencies", () => {
    const change = makeChange({
      conflicts: ["old_users"],
      note: "",
    });
    const line = serializeChange(change);
    expect(line).toBe(
      "add_users [!old_users] 2024-01-15T10:30:00Z Test User <test@example.com>",
    );
  });

  it("formats a change with both requires and conflicts", () => {
    const change = makeChange({
      requires: ["create_schema"],
      conflicts: ["old_users", "legacy_auth"],
      note: "Replace legacy",
    });
    const line = serializeChange(change);
    expect(line).toBe(
      "add_users [create_schema !old_users !legacy_auth] 2024-01-15T10:30:00Z Test User <test@example.com> # Replace legacy",
    );
  });

  it("omits dependency brackets when no deps exist", () => {
    const change = makeChange({ requires: [], conflicts: [], note: "" });
    const line = serializeChange(change);
    // Should NOT contain brackets at all
    expect(line).not.toContain("[");
    expect(line).not.toContain("]");
  });

  it("handles unicode in change name, planner, and note", () => {
    const change = makeChange({
      name: "add_ñoño",
      planner_name: "José García",
      planner_email: "jose@example.com",
      note: "Añadir tabla",
    });
    const line = serializeChange(change);
    expect(line).toBe(
      "add_ñoño 2024-01-15T10:30:00Z José García <jose@example.com> # Añadir tabla",
    );
  });
});

// ---------------------------------------------------------------------------
// serializeTag
// ---------------------------------------------------------------------------

describe("serializeTag", () => {
  it("formats a tag with a note", () => {
    const tag = makeTag();
    const line = serializeTag(tag);
    expect(line).toBe(
      "@v1.0 2024-01-15T10:31:00Z Test User <test@example.com> # First release",
    );
  });

  it("formats a tag without a note", () => {
    const tag = makeTag({ note: "" });
    const line = serializeTag(tag);
    expect(line).toBe(
      "@v1.0 2024-01-15T10:31:00Z Test User <test@example.com>",
    );
  });

  it("tag name is prefixed with @", () => {
    const tag = makeTag({ name: "release-2.0" });
    const line = serializeTag(tag);
    expect(line).toMatch(/^@release-2\.0 /);
  });

  it("handles unicode in tag planner and note", () => {
    const tag = makeTag({
      name: "v2.0",
      planner_name: "José García",
      planner_email: "jose@example.com",
      note: "Versión dos",
    });
    const line = serializeTag(tag);
    expect(line).toBe(
      "@v2.0 2024-01-15T10:31:00Z José García <jose@example.com> # Versión dos",
    );
  });
});

// ---------------------------------------------------------------------------
// serializePlan
// ---------------------------------------------------------------------------

describe("serializePlan", () => {
  it("serializes a plan with pragmas only (no entries)", () => {
    const plan = makePlan();
    const output = serializePlan(plan);
    expect(output).toBe(
      "%syntax-version=1.0.0\n" +
        "%project=myproject\n" +
        "\n",
    );
  });

  it("serializes pragmas in canonical order: syntax-version, project, uri", () => {
    const plan = makePlan({
      pragmas: new Map([
        ["uri", "https://example.com/"],
        ["project", "myproject"],
        ["syntax-version", "1.0.0"],
      ]),
      project: { name: "myproject", uri: "https://example.com/" },
    });
    const output = serializePlan(plan);
    const lines = output.split("\n");
    expect(lines[0]).toBe("%syntax-version=1.0.0");
    expect(lines[1]).toBe("%project=myproject");
    expect(lines[2]).toBe("%uri=https://example.com/");
  });

  it("serializes a plan with one change, no tags", () => {
    const change = makeChange({ note: "" });
    const plan = makePlan({ changes: [change] });
    const output = serializePlan(plan);
    expect(output).toContain(serializeChange(change));
    expect(output).toEndWith("\n");
  });

  it("serializes a plan with changes followed by their tags", () => {
    const change = makeChange({ change_id: "cid1", note: "First change" });
    const tag = makeTag({ change_id: "cid1", note: "Tag v1" });
    const plan = makePlan({
      changes: [change],
      tags: [tag],
    });
    const output = serializePlan(plan);
    const lines = output.split("\n").filter((l) => l.length > 0);
    // Pragmas, then change, then tag
    const changeIdx = lines.findIndex((l) => l.startsWith("add_users"));
    const tagIdx = lines.findIndex((l) => l.startsWith("@v1.0"));
    expect(changeIdx).toBeGreaterThan(-1);
    expect(tagIdx).toBeGreaterThan(changeIdx);
  });

  it("interleaves multiple changes and tags correctly", () => {
    const c1 = makeChange({
      change_id: "cid1",
      name: "first",
      note: "",
    });
    const c2 = makeChange({
      change_id: "cid2",
      name: "second",
      note: "",
    });
    const t1 = makeTag({
      name: "v1.0",
      change_id: "cid1",
      note: "",
    });
    const plan = makePlan({
      changes: [c1, c2],
      tags: [t1],
    });
    const output = serializePlan(plan);
    const entryLines = output
      .split("\n")
      .filter((l) => l.length > 0 && !l.startsWith("%"));
    // Expected order: first, @v1.0, second
    expect(entryLines[0]).toMatch(/^first /);
    expect(entryLines[1]).toMatch(/^@v1\.0 /);
    expect(entryLines[2]).toMatch(/^second /);
  });

  it("handles extra/custom pragmas", () => {
    const plan = makePlan({
      pragmas: new Map([
        ["syntax-version", "1.0.0"],
        ["project", "myproject"],
        ["custom-key", "custom-value"],
      ]),
    });
    const output = serializePlan(plan);
    expect(output).toContain("%custom-key=custom-value");
  });

  it("output ends with trailing newline", () => {
    const plan = makePlan({ changes: [makeChange({ note: "" })] });
    const output = serializePlan(plan);
    expect(output).toEndWith("\n");
  });

  it("round-trip: serializePlan produces parseable plan format", () => {
    const change1 = makeChange({
      change_id: "aaa",
      name: "create_schema",
      note: "Create app schema",
      requires: [],
      conflicts: [],
    });
    const change2 = makeChange({
      change_id: "bbb",
      name: "add_users",
      note: "Add users table",
      requires: ["create_schema"],
      conflicts: [],
    });
    const tag = makeTag({
      name: "v1.0",
      change_id: "bbb",
      note: "Release v1.0",
    });
    const plan = makePlan({
      project: { name: "myproject", uri: "https://example.com/" },
      pragmas: new Map([
        ["syntax-version", "1.0.0"],
        ["project", "myproject"],
        ["uri", "https://example.com/"],
      ]),
      changes: [change1, change2],
      tags: [tag],
    });

    const output = serializePlan(plan);

    // Verify exact expected output
    const expected =
      "%syntax-version=1.0.0\n" +
      "%project=myproject\n" +
      "%uri=https://example.com/\n" +
      "\n" +
      "create_schema 2024-01-15T10:30:00Z Test User <test@example.com> # Create app schema\n" +
      "add_users [create_schema] 2024-01-15T10:30:00Z Test User <test@example.com> # Add users table\n" +
      "@v1.0 2024-01-15T10:31:00Z Test User <test@example.com> # Release v1.0\n";

    expect(output).toBe(expected);
  });
});

// ---------------------------------------------------------------------------
// appendChange
// ---------------------------------------------------------------------------

describe("appendChange", () => {
  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), "sqlever-writer-test-"));
  });

  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it("appends a change line to an existing plan file", async () => {
    const planPath = join(tmpDir, "sqitch.plan");
    const initial =
      "%syntax-version=1.0.0\n" +
      "%project=myproject\n" +
      "\n";
    writeFileSync(planPath, initial, "utf-8");

    const change = makeChange({ note: "Added" });
    await appendChange(planPath, change);

    const result = readFileSync(planPath, "utf-8");
    expect(result).toBe(initial + serializeChange(change) + "\n");
  });

  it("preserves existing file content exactly", async () => {
    const planPath = join(tmpDir, "sqitch.plan");
    const initial =
      "%syntax-version=1.0.0\n" +
      "%project=myproject\n" +
      "\n" +
      "existing_change 2024-01-01T00:00:00Z User <u@e.com> # old\n";
    writeFileSync(planPath, initial, "utf-8");

    const change = makeChange({ name: "new_change", note: "" });
    await appendChange(planPath, change);

    const result = readFileSync(planPath, "utf-8");
    // Original content must be untouched
    expect(result.startsWith(initial)).toBe(true);
    // New line appended
    expect(result).toBe(initial + serializeChange(change) + "\n");
  });

  it("adds newline before entry if file does not end with newline", async () => {
    const planPath = join(tmpDir, "sqitch.plan");
    const initial = "%syntax-version=1.0.0\n%project=myproject";
    writeFileSync(planPath, initial, "utf-8");

    const change = makeChange({ note: "" });
    await appendChange(planPath, change);

    const result = readFileSync(planPath, "utf-8");
    // Should have inserted a newline before the change
    expect(result).toBe(initial + "\n" + serializeChange(change) + "\n");
  });
});

// ---------------------------------------------------------------------------
// appendTag
// ---------------------------------------------------------------------------

describe("appendTag", () => {
  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), "sqlever-writer-test-"));
  });

  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it("appends a tag line to an existing plan file", async () => {
    const planPath = join(tmpDir, "sqitch.plan");
    const initial =
      "%syntax-version=1.0.0\n" +
      "%project=myproject\n" +
      "\n" +
      "add_users 2024-01-15T10:30:00Z User <u@e.com>\n";
    writeFileSync(planPath, initial, "utf-8");

    const tag = makeTag({ note: "Tag it" });
    await appendTag(planPath, tag);

    const result = readFileSync(planPath, "utf-8");
    expect(result).toBe(initial + serializeTag(tag) + "\n");
  });

  it("preserves existing file content exactly when appending tag", async () => {
    const planPath = join(tmpDir, "sqitch.plan");
    const initial =
      "%syntax-version=1.0.0\n" +
      "%project=myproject\n" +
      "\n" +
      "change1 2024-01-15T10:30:00Z User <u@e.com> # first\n";
    writeFileSync(planPath, initial, "utf-8");

    const tag = makeTag({ note: "" });
    await appendTag(planPath, tag);

    const result = readFileSync(planPath, "utf-8");
    expect(result.startsWith(initial)).toBe(true);
  });

  it("adds newline before tag if file does not end with newline", async () => {
    const planPath = join(tmpDir, "sqitch.plan");
    const initial = "change1 2024-01-15T10:30:00Z User <u@e.com>";
    writeFileSync(planPath, initial, "utf-8");

    const tag = makeTag({ note: "" });
    await appendTag(planPath, tag);

    const result = readFileSync(planPath, "utf-8");
    expect(result).toBe(initial + "\n" + serializeTag(tag) + "\n");
  });

  it("multiple appends produce correct multi-line output", async () => {
    const planPath = join(tmpDir, "sqitch.plan");
    const initial =
      "%syntax-version=1.0.0\n" +
      "%project=myproject\n" +
      "\n";
    writeFileSync(planPath, initial, "utf-8");

    const c1 = makeChange({ name: "step1", note: "First" });
    const c2 = makeChange({ name: "step2", note: "Second" });
    const tag = makeTag({ name: "v1.0", note: "Release" });

    await appendChange(planPath, c1);
    await appendTag(planPath, tag);
    await appendChange(planPath, c2);

    const result = readFileSync(planPath, "utf-8");
    const expected =
      initial +
      serializeChange(c1) + "\n" +
      serializeTag(tag) + "\n" +
      serializeChange(c2) + "\n";
    expect(result).toBe(expected);
  });
});
