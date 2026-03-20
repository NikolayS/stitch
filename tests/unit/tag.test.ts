// tests/unit/tag.test.ts — Tests for sqlever tag command
//
// Validates tag command: argument parsing, tag name validation, plan
// appending, duplicate detection, planner identity, tag ID computation,
// and CLI integration.

import { describe, expect, it, beforeEach, afterEach } from "bun:test";
import {
  mkdtempSync,
  writeFileSync,
  readFileSync,
  rmSync,
} from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

import {
  parseTagArgs,
  runTag,
  isValidTagName,
} from "../../src/commands/tag";
import { computeTagId, computeChangeId } from "../../src/plan/types";
import { parsePlan } from "../../src/plan/parser";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let tmpDir: string;

function createTmpDir(): string {
  return mkdtempSync(join(tmpdir(), "sqlever-tag-test-"));
}

/** Create a minimal project directory with sqitch.plan containing one change. */
function setupProject(
  dir: string,
  planContent?: string,
): { planPath: string } {
  const planPath = join(dir, "sqitch.plan");

  writeFileSync(
    planPath,
    planContent ??
      "%syntax-version=1.0.0\n" +
      "%project=myproject\n" +
      "\n" +
      "create_schema 2024-01-15T10:30:00Z Test User <test@example.com> # first change\n",
    "utf-8",
  );

  return { planPath };
}

/** Mock environment with planner identity to avoid git dependency. */
const TEST_ENV: Record<string, string | undefined> = {
  SQLEVER_USER_NAME: "Test User",
  SQLEVER_USER_EMAIL: "test@example.com",
};

/** Create a minimal MergedConfig for testing. */
function testConfig(topDir: string) {
  return {
    core: {
      engine: undefined,
      top_dir: topDir,
      deploy_dir: "deploy",
      revert_dir: "revert",
      verify_dir: "verify",
      plan_file: "sqitch.plan",
    },
    deploy: {
      verify: true,
      mode: "change" as const,
      lock_retries: 0,
      lock_timeout: "5s",
      idle_in_transaction_session_timeout: "10min",
      search_path: undefined,
    },
    engines: {},
    targets: {},
    analysis: {},
    sqitchConf: { entries: [], rawLines: [], sections: new Set<string>() },
    sqleverToml: null,
  };
}

// ---------------------------------------------------------------------------
// parseTagArgs
// ---------------------------------------------------------------------------

describe("parseTagArgs", () => {
  it("parses a simple tag name", () => {
    const opts = parseTagArgs(["v1.0"]);
    expect(opts.name).toBe("v1.0");
    expect(opts.note).toBe("");
  });

  it("parses -n / --note", () => {
    const opts1 = parseTagArgs(["v1.0", "-n", "Release version 1.0"]);
    expect(opts1.name).toBe("v1.0");
    expect(opts1.note).toBe("Release version 1.0");

    const opts2 = parseTagArgs(["v1.0", "--note", "Release version 1.0"]);
    expect(opts2.name).toBe("v1.0");
    expect(opts2.note).toBe("Release version 1.0");
  });

  it("returns empty name when no positional arg given", () => {
    const opts = parseTagArgs(["-n", "some note"]);
    expect(opts.name).toBe("");
  });

  it("parses tag name with note before name", () => {
    const opts = parseTagArgs(["-n", "my note", "v2.0"]);
    expect(opts.name).toBe("v2.0");
    expect(opts.note).toBe("my note");
  });
});

// ---------------------------------------------------------------------------
// isValidTagName
// ---------------------------------------------------------------------------

describe("isValidTagName", () => {
  it("accepts simple names", () => {
    expect(isValidTagName("v1")).toBe(true);
    expect(isValidTagName("release")).toBe(true);
    expect(isValidTagName("_private")).toBe(true);
  });

  it("accepts names with dots, hyphens, underscores", () => {
    expect(isValidTagName("v1.0.0")).toBe(true);
    expect(isValidTagName("release-candidate")).toBe(true);
    expect(isValidTagName("v1_beta")).toBe(true);
    expect(isValidTagName("v1.0-rc.1")).toBe(true);
  });

  it("rejects names starting with digits", () => {
    expect(isValidTagName("1invalid")).toBe(false);
    expect(isValidTagName("123")).toBe(false);
  });

  it("rejects empty name", () => {
    expect(isValidTagName("")).toBe(false);
  });

  it("rejects names with spaces or special chars", () => {
    expect(isValidTagName("v 1.0")).toBe(false);
    expect(isValidTagName("v1.0!")).toBe(false);
    expect(isValidTagName("tag@name")).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// runTag — full integration (with temp dirs)
// ---------------------------------------------------------------------------

describe("runTag", () => {
  beforeEach(() => {
    tmpDir = createTmpDir();
  });

  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it("appends a tag line to the plan file", async () => {
    const { planPath } = setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    await runTag(
      { name: "v1.0", note: "First release" },
      cfg,
      TEST_ENV,
    );

    const plan = readFileSync(planPath, "utf-8");
    expect(plan).toContain("@v1.0");
    expect(plan).toContain("# First release");
    expect(plan).toContain("Test User <test@example.com>");
  });

  it("computes the correct tag ID", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    const tag = await runTag(
      { name: "v1.0", note: "Release" },
      cfg,
      TEST_ENV,
    );

    // Compute expected change_id for the change the tag attaches to
    const expectedChangeId = computeChangeId({
      project: "myproject",
      change: "create_schema",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T10:30:00Z",
      requires: [],
      conflicts: [],
      note: "first change",
    });

    expect(tag.change_id).toBe(expectedChangeId);

    // Verify tag_id is correctly computed
    const expectedTagId = computeTagId({
      project: "myproject",
      tag: "v1.0",
      change_id: expectedChangeId,
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: tag.planned_at,
      note: "Release",
    });

    expect(tag.tag_id).toBe(expectedTagId);
  });

  it("returns a complete Tag object", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    const tag = await runTag(
      { name: "v1.0", note: "My tag note" },
      cfg,
      TEST_ENV,
    );

    expect(tag.name).toBe("v1.0");
    expect(tag.project).toBe("myproject");
    expect(tag.note).toBe("My tag note");
    expect(tag.planner_name).toBe("Test User");
    expect(tag.planner_email).toBe("test@example.com");
    expect(tag.planned_at).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/);
    expect(tag.tag_id).toBeTruthy();
    expect(tag.change_id).toBeTruthy();
  });

  it("strips leading @ from tag name", async () => {
    const { planPath } = setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    const tag = await runTag(
      { name: "@v1.0", note: "" },
      cfg,
      TEST_ENV,
    );

    expect(tag.name).toBe("v1.0");
    const plan = readFileSync(planPath, "utf-8");
    // Should have @v1.0, not @@v1.0
    expect(plan).toContain("@v1.0");
    expect(plan).not.toContain("@@v1.0");
  });

  it("creates a tag with empty note", async () => {
    const { planPath } = setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    await runTag(
      { name: "v1.0", note: "" },
      cfg,
      TEST_ENV,
    );

    const plan = readFileSync(planPath, "utf-8");
    expect(plan).toContain("@v1.0");
    // No "# " should appear for empty note
    const tagLine = plan.split("\n").find((l) => l.startsWith("@v1.0"));
    expect(tagLine).toBeTruthy();
    expect(tagLine).not.toContain("#");
  });

  it("errors on duplicate tag name", async () => {
    // Set up a plan with an existing tag
    setupProject(
      tmpDir,
      "%syntax-version=1.0.0\n" +
      "%project=myproject\n" +
      "\n" +
      "create_schema 2024-01-15T10:30:00Z Test User <test@example.com> # first\n" +
      "@v1.0 2024-01-15T10:31:00Z Test User <test@example.com> # existing tag\n",
    );
    const cfg = testConfig(tmpDir);

    const origExit = process.exit;
    let exitCode: number | undefined;
    process.exit = ((code: number) => {
      exitCode = code;
      throw new Error(`process.exit(${code})`);
    }) as never;

    try {
      await runTag(
        { name: "v1.0", note: "duplicate" },
        cfg,
        TEST_ENV,
      );
    } catch {
      // Expected
    } finally {
      process.exit = origExit;
    }

    expect(exitCode).toBe(1);
  });

  it("errors when plan has no changes", async () => {
    // Plan with no changes
    setupProject(
      tmpDir,
      "%syntax-version=1.0.0\n" +
      "%project=myproject\n" +
      "\n",
    );
    const cfg = testConfig(tmpDir);

    const origExit = process.exit;
    let exitCode: number | undefined;
    process.exit = ((code: number) => {
      exitCode = code;
      throw new Error(`process.exit(${code})`);
    }) as never;

    try {
      await runTag(
        { name: "v1.0", note: "" },
        cfg,
        TEST_ENV,
      );
    } catch {
      // Expected
    } finally {
      process.exit = origExit;
    }

    expect(exitCode).toBe(1);
  });

  it("errors when plan file does not exist", async () => {
    // Don't create a plan file
    const cfg = testConfig(tmpDir);

    const origExit = process.exit;
    let exitCode: number | undefined;
    process.exit = ((code: number) => {
      exitCode = code;
      throw new Error(`process.exit(${code})`);
    }) as never;

    try {
      await runTag(
        { name: "v1.0", note: "" },
        cfg,
        TEST_ENV,
      );
    } catch {
      // Expected
    } finally {
      process.exit = origExit;
    }

    expect(exitCode).toBe(1);
  });

  it("errors when no tag name provided", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    const origExit = process.exit;
    let exitCode: number | undefined;
    process.exit = ((code: number) => {
      exitCode = code;
      throw new Error(`process.exit(${code})`);
    }) as never;

    try {
      await runTag(
        { name: "", note: "" },
        cfg,
        TEST_ENV,
      );
    } catch {
      // Expected
    } finally {
      process.exit = origExit;
    }

    expect(exitCode).toBe(1);
  });

  it("errors on invalid tag name", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    const origExit = process.exit;
    let exitCode: number | undefined;
    process.exit = ((code: number) => {
      exitCode = code;
      throw new Error(`process.exit(${code})`);
    }) as never;

    try {
      await runTag(
        { name: "123-invalid", note: "" },
        cfg,
        TEST_ENV,
      );
    } catch {
      // Expected
    } finally {
      process.exit = origExit;
    }

    expect(exitCode).toBe(1);
  });

  it("tags the last change in a multi-change plan", async () => {
    setupProject(
      tmpDir,
      "%syntax-version=1.0.0\n" +
      "%project=myproject\n" +
      "\n" +
      "first 2024-01-15T10:30:00Z Test User <test@example.com>\n" +
      "second 2024-01-15T10:31:00Z Test User <test@example.com>\n",
    );
    const cfg = testConfig(tmpDir);

    const tag = await runTag(
      { name: "v1.0", note: "" },
      cfg,
      TEST_ENV,
    );

    // The tag should attach to "second" (the last change)
    const firstId = computeChangeId({
      project: "myproject",
      change: "first",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T10:30:00Z",
      requires: [],
      conflicts: [],
      note: "",
    });
    const secondId = computeChangeId({
      project: "myproject",
      change: "second",
      parent: firstId,
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T10:31:00Z",
      requires: [],
      conflicts: [],
      note: "",
    });

    expect(tag.change_id).toBe(secondId);
  });

  it("handles plan with URI pragma", async () => {
    setupProject(
      tmpDir,
      "%syntax-version=1.0.0\n" +
      "%project=myproject\n" +
      "%uri=https://example.com/myproject\n" +
      "\n" +
      "create_schema 2024-01-15T10:30:00Z Test User <test@example.com>\n",
    );
    const cfg = testConfig(tmpDir);

    const tag = await runTag(
      { name: "v1.0", note: "" },
      cfg,
      TEST_ENV,
    );

    // Tag ID should include URI in computation
    const changeId = computeChangeId({
      project: "myproject",
      uri: "https://example.com/myproject",
      change: "create_schema",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T10:30:00Z",
      requires: [],
      conflicts: [],
      note: "",
    });

    const expectedTagId = computeTagId({
      project: "myproject",
      uri: "https://example.com/myproject",
      tag: "v1.0",
      change_id: changeId,
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: tag.planned_at,
      note: "",
    });

    expect(tag.tag_id).toBe(expectedTagId);
    expect(tag.change_id).toBe(changeId);
  });

  it("plan is parseable after adding tag", async () => {
    const { planPath } = setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    await runTag(
      { name: "v1.0", note: "First release" },
      cfg,
      TEST_ENV,
    );

    // Re-parse the plan to make sure it's valid
    const content = readFileSync(planPath, "utf-8");
    const plan = parsePlan(content);

    expect(plan.changes.length).toBe(1);
    expect(plan.tags.length).toBe(1);
    expect(plan.tags[0]!.name).toBe("v1.0");
    expect(plan.tags[0]!.note).toBe("First release");
    expect(plan.tags[0]!.change_id).toBe(plan.changes[0]!.change_id);
  });

  it("allows multiple distinct tags", async () => {
    const { planPath } = setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    await runTag(
      { name: "v1.0", note: "First" },
      cfg,
      TEST_ENV,
    );

    await runTag(
      { name: "v1.1", note: "Second" },
      cfg,
      TEST_ENV,
    );

    const content = readFileSync(planPath, "utf-8");
    const plan = parsePlan(content);

    expect(plan.tags.length).toBe(2);
    expect(plan.tags[0]!.name).toBe("v1.0");
    expect(plan.tags[1]!.name).toBe("v1.1");
  });
});

// ---------------------------------------------------------------------------
// CLI integration (subprocess)
// ---------------------------------------------------------------------------

describe("sqlever tag (subprocess)", () => {
  const CWD = import.meta.dir + "/../..";

  beforeEach(() => {
    tmpDir = createTmpDir();
  });

  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true });
  });

  async function runCli(
    ...args: string[]
  ): Promise<{ stdout: string; stderr: string; exitCode: number }> {
    const proc = Bun.spawn(["bun", "run", join(CWD, "src/cli.ts"), ...args], {
      cwd: tmpDir,
      stdout: "pipe",
      stderr: "pipe",
      env: {
        ...process.env,
        SQLEVER_USER_NAME: "CLI Test",
        SQLEVER_USER_EMAIL: "cli@test.com",
      },
    });
    const [stdout, stderr, exitCode] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
      proc.exited,
    ]);
    return { stdout, stderr, exitCode };
  }

  it("creates a tag via CLI", async () => {
    setupProject(tmpDir);

    const { stdout, exitCode } = await runCli(
      "tag", "v1.0", "-n", "First release",
    );
    expect(exitCode).toBe(0);
    expect(stdout).toContain("Tagged");
    expect(stdout).toContain("@v1.0");

    const plan = readFileSync(join(tmpDir, "sqitch.plan"), "utf-8");
    expect(plan).toContain("@v1.0");
    expect(plan).toContain("# First release");
  });

  it("exits 1 when no tag name is provided", async () => {
    setupProject(tmpDir);

    const { exitCode, stderr } = await runCli("tag");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("tag name is required");
  });

  it("exits 1 when plan file is missing", async () => {
    const { exitCode, stderr } = await runCli("tag", "v1.0");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("plan file not found");
  });
});
