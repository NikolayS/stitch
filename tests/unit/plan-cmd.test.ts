// tests/unit/plan-cmd.test.ts — Tests for the `sqlever plan` command
//
// Validates parsePlanArgs(), filterPlan(), formatDeps(), buildPlanJson(),
// printPlanText(), and the CLI subprocess integration.
// Covers: argument parsing, filtering by --change and --tag, text output,
// JSON output, error handling, and quiet mode.

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { ParsedArgs } from "../../src/cli";
import {
  parsePlanArgs,
  filterPlan,
  formatDeps,
  buildPlanJson,
  printPlanText,
} from "../../src/commands/plan";
import type { PlanOptions } from "../../src/commands/plan";
import { parsePlan } from "../../src/plan/parser";
import type { Plan, Change, Tag } from "../../src/plan/types";
import { resetConfig, setConfig } from "../../src/output";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const CWD = import.meta.dir + "/../..";

/** Build a minimal ParsedArgs, overriding specific fields. */
function makeArgs(overrides: Partial<ParsedArgs> = {}): ParsedArgs {
  return {
    command: "plan",
    rest: [],
    help: false,
    version: false,
    format: "text",
    quiet: false,
    verbose: false,
    dbUri: undefined,
    planFile: undefined,
    topDir: undefined,
    registry: undefined,
    target: undefined,
    ...overrides,
  };
}

/** Build a minimal valid plan string. */
function minimalPlan(lines: string[] = []): string {
  return [
    "%syntax-version=1.0.0",
    "%project=testproject",
    "",
    ...lines,
  ].join("\n");
}

/** Plan with URI and some changes/tags for filtering tests. */
function complexPlan(): string {
  return [
    "%syntax-version=1.0.0",
    "%project=myproject",
    "%uri=https://example.com/",
    "",
    "create_schema 2024-01-01T00:00:00Z Dev <dev@example.com> # Create schema",
    "add_users [create_schema] 2024-01-02T00:00:00Z Dev <dev@example.com> # Add users table",
    "add_posts [add_users] 2024-01-03T00:00:00Z Dev <dev@example.com> # Add posts table",
    "@v1.0 2024-01-03T00:01:00Z Dev <dev@example.com> # Version 1.0",
    "add_users [add_users@v1.0] 2024-02-01T00:00:00Z Dev <dev@example.com> # Rework users with email validation",
    "@v2.0 2024-02-01T00:01:00Z Dev <dev@example.com> # Version 2.0",
  ].join("\n");
}

/** Create a fresh temp directory for each test. */
async function makeTempDir(): Promise<string> {
  return mkdtemp(join(tmpdir(), "sqlever-plan-test-"));
}

/** Spawn the CLI and capture output. */
async function run(
  ...args: string[]
): Promise<{ stdout: string; stderr: string; exitCode: number }> {
  const proc = Bun.spawn(["bun", "run", "src/cli.ts", ...args], {
    cwd: CWD,
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  return { stdout, stderr, exitCode };
}

// ---------------------------------------------------------------------------
// Tests: parsePlanArgs (unit, no filesystem)
// ---------------------------------------------------------------------------

describe("parsePlanArgs", () => {
  test("defaults: plan file resolved from cwd/sqitch.plan", () => {
    const args = makeArgs();
    const opts = parsePlanArgs(args);
    expect(opts.planFile).toBe(resolve("sqitch.plan"));
    expect(opts.change).toBeUndefined();
    expect(opts.tag).toBeUndefined();
  });

  test("--change flag parsed from rest", () => {
    const args = makeArgs({ rest: ["--change", "add_users"] });
    const opts = parsePlanArgs(args);
    expect(opts.change).toBe("add_users");
  });

  test("--tag flag parsed from rest", () => {
    const args = makeArgs({ rest: ["--tag", "v1.0"] });
    const opts = parsePlanArgs(args);
    expect(opts.tag).toBe("v1.0");
  });

  test("--plan-file in rest overrides global", () => {
    const args = makeArgs({
      planFile: "global.plan",
      rest: ["--plan-file", "local.plan"],
    });
    const opts = parsePlanArgs(args);
    expect(opts.planFile).toContain("local.plan");
  });

  test("global --plan-file used when not in rest", () => {
    const args = makeArgs({ planFile: "custom.plan" });
    const opts = parsePlanArgs(args);
    expect(opts.planFile).toContain("custom.plan");
  });

  test("--top-dir affects plan file resolution", () => {
    const args = makeArgs({ topDir: "/my/project" });
    const opts = parsePlanArgs(args);
    expect(opts.planFile).toBe("/my/project/sqitch.plan");
  });

  test("both --change and --tag can be set", () => {
    const args = makeArgs({
      rest: ["--change", "add_users", "--tag", "v1.0"],
    });
    const opts = parsePlanArgs(args);
    expect(opts.change).toBe("add_users");
    expect(opts.tag).toBe("v1.0");
  });
});

// ---------------------------------------------------------------------------
// Tests: formatDeps (unit)
// ---------------------------------------------------------------------------

describe("formatDeps", () => {
  test("empty deps returns empty string", () => {
    expect(formatDeps([], [])).toBe("");
  });

  test("requires only", () => {
    expect(formatDeps(["dep1", "dep2"], [])).toBe("dep1, dep2");
  });

  test("conflicts only", () => {
    expect(formatDeps([], ["old_thing"])).toBe("!old_thing");
  });

  test("mixed requires and conflicts", () => {
    expect(formatDeps(["dep1"], ["old_thing"])).toBe("dep1, !old_thing");
  });
});

// ---------------------------------------------------------------------------
// Tests: filterPlan (unit)
// ---------------------------------------------------------------------------

describe("filterPlan", () => {
  let plan: Plan;

  beforeEach(() => {
    plan = parsePlan(complexPlan());
  });

  test("no filters returns all changes and tags", () => {
    const opts: PlanOptions = { planFile: "sqitch.plan" };
    const result = filterPlan(plan, opts);
    expect(result.changes).toHaveLength(4);
    expect(result.tags).toHaveLength(2);
  });

  test("--change filters to matching changes only", () => {
    const opts: PlanOptions = { planFile: "sqitch.plan", change: "add_users" };
    const result = filterPlan(plan, opts);
    // There are two add_users changes (original + rework)
    expect(result.changes).toHaveLength(2);
    for (const c of result.changes) {
      expect(c.name).toBe("add_users");
    }
  });

  test("--change with nonexistent name returns empty", () => {
    const opts: PlanOptions = {
      planFile: "sqitch.plan",
      change: "nonexistent",
    };
    const result = filterPlan(plan, opts);
    expect(result.changes).toHaveLength(0);
    expect(result.tags).toHaveLength(0);
  });

  test("--tag filters changes up to tagged change", () => {
    const opts: PlanOptions = { planFile: "sqitch.plan", tag: "v1.0" };
    const result = filterPlan(plan, opts);
    // v1.0 is attached to add_posts (3rd change), so should include first 3
    expect(result.changes).toHaveLength(3);
    expect(result.changes[0]!.name).toBe("create_schema");
    expect(result.changes[1]!.name).toBe("add_users");
    expect(result.changes[2]!.name).toBe("add_posts");
    // Only v1.0 tag should be included (v2.0 is outside range)
    expect(result.tags).toHaveLength(1);
    expect(result.tags[0]!.name).toBe("v1.0");
  });

  test("--tag with nonexistent tag returns empty", () => {
    const opts: PlanOptions = { planFile: "sqitch.plan", tag: "nonexistent" };
    const result = filterPlan(plan, opts);
    expect(result.changes).toHaveLength(0);
    expect(result.tags).toHaveLength(0);
  });

  test("--tag and --change combined: tag sets range, change filters", () => {
    const opts: PlanOptions = {
      planFile: "sqitch.plan",
      tag: "v1.0",
      change: "add_users",
    };
    const result = filterPlan(plan, opts);
    // v1.0 limits to first 3 changes, then filter by name "add_users"
    // Only the original add_users (2nd change) should match
    expect(result.changes).toHaveLength(1);
    expect(result.changes[0]!.name).toBe("add_users");
  });

  test("--change filters tags to only those on matching changes", () => {
    const opts: PlanOptions = {
      planFile: "sqitch.plan",
      change: "create_schema",
    };
    const result = filterPlan(plan, opts);
    expect(result.changes).toHaveLength(1);
    // create_schema has no tags attached to it
    expect(result.tags).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// Tests: buildPlanJson (unit)
// ---------------------------------------------------------------------------

describe("buildPlanJson", () => {
  test("produces correct JSON structure", () => {
    const plan = parsePlan(complexPlan());
    const result = buildPlanJson(plan, plan.changes, plan.tags) as {
      project: { name: string; uri: string | null };
      changes: Array<{
        name: string;
        change_id: string;
        tags: Array<{ name: string; tag_id: string }>;
        requires: string[];
        conflicts: string[];
        parent: string | null;
      }>;
    };

    expect(result.project.name).toBe("myproject");
    expect(result.project.uri).toBe("https://example.com/");
    expect(result.changes).toHaveLength(4);

    // First change has no parent
    expect(result.changes[0]!.parent).toBeNull();
    expect(result.changes[0]!.name).toBe("create_schema");

    // Third change (add_posts) has v1.0 tag
    const addPosts = result.changes[2]!;
    expect(addPosts.tags).toHaveLength(1);
    expect(addPosts.tags[0]!.name).toBe("v1.0");
  });

  test("null uri when project has no URI", () => {
    const plan = parsePlan(minimalPlan([
      "first 2024-01-15T10:30:00Z A <a@b.com> # note",
    ]));
    const result = buildPlanJson(plan, plan.changes, plan.tags) as {
      project: { uri: string | null };
    };
    expect(result.project.uri).toBeNull();
  });

  test("includes requires and conflicts in change entries", () => {
    const plan = parsePlan(complexPlan());
    const result = buildPlanJson(plan, plan.changes, plan.tags) as {
      changes: Array<{ requires: string[]; conflicts: string[] }>;
    };

    // Second change (add_users) requires create_schema
    expect(result.changes[1]!.requires).toEqual(["create_schema"]);
    expect(result.changes[1]!.conflicts).toEqual([]);
  });

  test("empty changes array when no changes", () => {
    const plan = parsePlan(minimalPlan());
    const result = buildPlanJson(plan, [], []) as {
      changes: unknown[];
    };
    expect(result.changes).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// Tests: printPlanText (unit, capturing stdout)
// ---------------------------------------------------------------------------

describe("printPlanText", () => {
  beforeEach(() => {
    resetConfig();
  });

  test("prints project header", () => {
    const plan = parsePlan(complexPlan());
    const chunks: string[] = [];
    const origWrite = process.stdout.write;
    process.stdout.write = (chunk: string | Uint8Array) => {
      chunks.push(String(chunk));
      return true;
    };

    try {
      printPlanText(plan, plan.changes, plan.tags);
    } finally {
      process.stdout.write = origWrite;
    }

    const output = chunks.join("");
    expect(output).toContain("Project: myproject");
    expect(output).toContain("URI:     https://example.com/");
  });

  test("prints 'No changes.' when changes array is empty", () => {
    const plan = parsePlan(minimalPlan());
    const chunks: string[] = [];
    const origWrite = process.stdout.write;
    process.stdout.write = (chunk: string | Uint8Array) => {
      chunks.push(String(chunk));
      return true;
    };

    try {
      printPlanText(plan, [], []);
    } finally {
      process.stdout.write = origWrite;
    }

    const output = chunks.join("");
    expect(output).toContain("No changes.");
  });

  test("prints table with change names", () => {
    const plan = parsePlan(complexPlan());
    const chunks: string[] = [];
    const origWrite = process.stdout.write;
    process.stdout.write = (chunk: string | Uint8Array) => {
      chunks.push(String(chunk));
      return true;
    };

    try {
      printPlanText(plan, plan.changes, plan.tags);
    } finally {
      process.stdout.write = origWrite;
    }

    const output = chunks.join("");
    expect(output).toContain("create_schema");
    expect(output).toContain("add_users");
    expect(output).toContain("add_posts");
    // Table headers
    expect(output).toContain("Name");
    expect(output).toContain("Deps");
    expect(output).toContain("Tags");
  });

  test("quiet mode suppresses all output", () => {
    setConfig({ quiet: true });
    const plan = parsePlan(complexPlan());
    const chunks: string[] = [];
    const origWrite = process.stdout.write;
    process.stdout.write = (chunk: string | Uint8Array) => {
      chunks.push(String(chunk));
      return true;
    };

    try {
      printPlanText(plan, plan.changes, plan.tags);
    } finally {
      process.stdout.write = origWrite;
      resetConfig();
    }

    const output = chunks.join("");
    expect(output).toBe("");
  });
});

// ---------------------------------------------------------------------------
// Tests: CLI subprocess integration
// ---------------------------------------------------------------------------

describe("sqlever plan (subprocess)", () => {
  let tempDir: string;
  let planPath: string;

  beforeEach(async () => {
    tempDir = await makeTempDir();
    planPath = join(tempDir, "sqitch.plan");
    resetConfig();
  });

  afterEach(async () => {
    await rm(tempDir, { recursive: true, force: true });
  });

  test("displays plan contents and exits 0", async () => {
    await writeFile(planPath, complexPlan(), "utf-8");

    const { stdout, exitCode } = await run(
      "plan",
      "--plan-file",
      planPath,
    );
    expect(exitCode).toBe(0);
    expect(stdout).toContain("Project: myproject");
    expect(stdout).toContain("create_schema");
    expect(stdout).toContain("add_users");
    expect(stdout).toContain("add_posts");
  });

  test("--format json outputs valid JSON", async () => {
    await writeFile(planPath, complexPlan(), "utf-8");

    const { stdout, exitCode } = await run(
      "--format",
      "json",
      "plan",
      "--plan-file",
      planPath,
    );
    expect(exitCode).toBe(0);
    const data = JSON.parse(stdout);
    expect(data.project.name).toBe("myproject");
    expect(data.changes).toHaveLength(4);
    expect(data.changes[0].name).toBe("create_schema");
  });

  test("--change filter works via CLI", async () => {
    await writeFile(planPath, complexPlan(), "utf-8");

    const { stdout, exitCode } = await run(
      "--format",
      "json",
      "plan",
      "--plan-file",
      planPath,
      "--change",
      "create_schema",
    );
    expect(exitCode).toBe(0);
    const data = JSON.parse(stdout);
    expect(data.changes).toHaveLength(1);
    expect(data.changes[0].name).toBe("create_schema");
  });

  test("--tag filter works via CLI", async () => {
    await writeFile(planPath, complexPlan(), "utf-8");

    const { stdout, exitCode } = await run(
      "--format",
      "json",
      "plan",
      "--plan-file",
      planPath,
      "--tag",
      "v1.0",
    );
    expect(exitCode).toBe(0);
    const data = JSON.parse(stdout);
    expect(data.changes).toHaveLength(3);
  });

  test("nonexistent plan file exits 1 with error", async () => {
    const { stderr, exitCode } = await run(
      "plan",
      "--plan-file",
      join(tempDir, "nope.plan"),
    );
    expect(exitCode).toBe(1);
    expect(stderr).toContain("cannot read plan file");
  });

  test("--quiet suppresses text output", async () => {
    await writeFile(planPath, complexPlan(), "utf-8");

    const { stdout, exitCode } = await run(
      "--quiet",
      "plan",
      "--plan-file",
      planPath,
    );
    expect(exitCode).toBe(0);
    expect(stdout).toBe("");
  });

  test("empty plan shows 'No changes.'", async () => {
    await writeFile(planPath, minimalPlan(), "utf-8");

    const { stdout, exitCode } = await run(
      "plan",
      "--plan-file",
      planPath,
    );
    expect(exitCode).toBe(0);
    expect(stdout).toContain("No changes.");
  });

  test("JSON output includes tags on changes", async () => {
    await writeFile(planPath, complexPlan(), "utf-8");

    const { stdout, exitCode } = await run(
      "--format",
      "json",
      "plan",
      "--plan-file",
      planPath,
    );
    expect(exitCode).toBe(0);
    const data = JSON.parse(stdout);
    // add_posts (index 2) should have v1.0 tag
    const addPosts = data.changes[2];
    expect(addPosts.tags).toHaveLength(1);
    expect(addPosts.tags[0].name).toBe("v1.0");
    // reworked add_users (index 3) should have v2.0 tag
    const reworked = data.changes[3];
    expect(reworked.tags).toHaveLength(1);
    expect(reworked.tags[0].name).toBe("v2.0");
  });

  test("text output shows deps and tags columns", async () => {
    await writeFile(planPath, complexPlan(), "utf-8");

    const { stdout, exitCode } = await run(
      "plan",
      "--plan-file",
      planPath,
    );
    expect(exitCode).toBe(0);
    // Check that dependency info appears
    expect(stdout).toContain("create_schema");
    // Check that tag info appears (the @v1.0 tag)
    expect(stdout).toContain("@v1.0");
  });
});
