// tests/unit/rework.test.ts — Tests for sqlever rework command
//
// Validates rework command: argument parsing, plan analysis, script backup,
// fresh file creation, plan appending, error cases, and CLI integration.

import { describe, expect, it, beforeEach, afterEach } from "bun:test";
import {
  mkdtempSync,
  writeFileSync,
  readFileSync,
  existsSync,
  mkdirSync,
} from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { rmSync } from "node:fs";

import {
  parseReworkArgs,
  runRework,
  findReworkContext,
  ReworkError,
} from "../../src/commands/rework";
import { computeChangeId } from "../../src/plan/types";
import { parsePlan } from "../../src/plan/parser";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let tmpDir: string;

function createTmpDir(): string {
  return mkdtempSync(join(tmpdir(), "sqlever-rework-test-"));
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

/** Standard plan with one change and a tag after it. */
const PLAN_WITH_TAG =
  "%syntax-version=1.0.0\n" +
  "%project=myproject\n" +
  "\n" +
  "add_users 2024-01-01T00:00:00Z Test User <test@example.com> # add users table\n" +
  "@v1.0 2024-01-01T00:01:00Z Test User <test@example.com> # tag v1.0\n";

/** Plan with two changes, tag after first only. */
const PLAN_TWO_CHANGES_TAG_AFTER_FIRST =
  "%syntax-version=1.0.0\n" +
  "%project=myproject\n" +
  "\n" +
  "add_users 2024-01-01T00:00:00Z Test User <test@example.com> # add users table\n" +
  "@v1.0 2024-01-01T00:01:00Z Test User <test@example.com> # tag v1.0\n" +
  "add_roles 2024-01-02T00:00:00Z Test User <test@example.com> # add roles table\n";

/** Plan with no tag after the change. */
const PLAN_NO_TAG =
  "%syntax-version=1.0.0\n" +
  "%project=myproject\n" +
  "\n" +
  "add_users 2024-01-01T00:00:00Z Test User <test@example.com> # add users table\n";

/** Set up a project with plan and existing scripts. */
function setupReworkProject(
  dir: string,
  planContent: string,
  changeName: string = "add_users",
): { planPath: string } {
  const planPath = join(dir, "sqitch.plan");
  writeFileSync(planPath, planContent, "utf-8");

  // Create directories and script files
  const deployDir = join(dir, "deploy");
  const revertDir = join(dir, "revert");
  const verifyDir = join(dir, "verify");
  mkdirSync(deployDir, { recursive: true });
  mkdirSync(revertDir, { recursive: true });
  mkdirSync(verifyDir, { recursive: true });

  writeFileSync(
    join(deployDir, `${changeName}.sql`),
    `-- Deploy ${changeName}\nCREATE TABLE users (id int);\n`,
    "utf-8",
  );
  writeFileSync(
    join(revertDir, `${changeName}.sql`),
    `-- Revert ${changeName}\nDROP TABLE users;\n`,
    "utf-8",
  );
  writeFileSync(
    join(verifyDir, `${changeName}.sql`),
    `-- Verify ${changeName}\nSELECT 1 FROM users;\n`,
    "utf-8",
  );

  return { planPath };
}

// ---------------------------------------------------------------------------
// parseReworkArgs
// ---------------------------------------------------------------------------

describe("parseReworkArgs", () => {
  it("parses a simple change name", () => {
    const opts = parseReworkArgs(["add_users"]);
    expect(opts.name).toBe("add_users");
    expect(opts.note).toBe("");
  });

  it("parses -n / --note", () => {
    const opts1 = parseReworkArgs(["add_users", "-n", "reworked users"]);
    expect(opts1.name).toBe("add_users");
    expect(opts1.note).toBe("reworked users");

    const opts2 = parseReworkArgs(["add_users", "--note", "reworked users"]);
    expect(opts2.name).toBe("add_users");
    expect(opts2.note).toBe("reworked users");
  });

  it("returns empty name when no positional arg given", () => {
    const opts = parseReworkArgs(["-n", "some note"]);
    expect(opts.name).toBe("");
  });
});

// ---------------------------------------------------------------------------
// findReworkContext
// ---------------------------------------------------------------------------

describe("findReworkContext", () => {
  it("finds change and tag for a simple rework scenario", () => {
    const ctx = findReworkContext(PLAN_WITH_TAG, "add_users");
    expect(ctx.lastChange.name).toBe("add_users");
    expect(ctx.tagAfterChange.name).toBe("v1.0");
    expect(ctx.projectName).toBe("myproject");
  });

  it("throws ReworkError when change does not exist", () => {
    expect(() => findReworkContext(PLAN_WITH_TAG, "nonexistent")).toThrow(
      ReworkError,
    );
    expect(() => findReworkContext(PLAN_WITH_TAG, "nonexistent")).toThrow(
      /Unknown change/,
    );
  });

  it("throws ReworkError when no tag exists after the change", () => {
    expect(() => findReworkContext(PLAN_NO_TAG, "add_users")).toThrow(
      ReworkError,
    );
    expect(() => findReworkContext(PLAN_NO_TAG, "add_users")).toThrow(
      /no tag exists/,
    );
  });

  it("finds the correct tag when change is not the last in the plan", () => {
    const ctx = findReworkContext(PLAN_TWO_CHANGES_TAG_AFTER_FIRST, "add_users");
    expect(ctx.tagAfterChange.name).toBe("v1.0");
    // lastPlanChangeId should be the ID of add_roles (the last change)
    expect(ctx.lastPlanChangeId).toBeTruthy();
    expect(ctx.lastChange.name).toBe("add_users");
  });

  it("uses the last occurrence when a change name appears multiple times", () => {
    // Plan with already-reworked change
    const plan =
      "%syntax-version=1.0.0\n" +
      "%project=myproject\n" +
      "\n" +
      "add_users 2024-01-01T00:00:00Z Test User <test@example.com> # original\n" +
      "@v1.0 2024-01-01T00:01:00Z Test User <test@example.com> # tag v1.0\n" +
      "add_users [add_users@v1.0] 2024-02-01T00:00:00Z Test User <test@example.com> # rework 1\n" +
      "@v2.0 2024-02-01T00:01:00Z Test User <test@example.com> # tag v2.0\n";

    const ctx = findReworkContext(plan, "add_users");
    // Should find the second (reworked) occurrence
    expect(ctx.lastChange.note).toBe("rework 1");
    // Should find the tag after the second occurrence
    expect(ctx.tagAfterChange.name).toBe("v2.0");
  });

  it("throws when last occurrence of a reworked change has no tag", () => {
    const plan =
      "%syntax-version=1.0.0\n" +
      "%project=myproject\n" +
      "\n" +
      "add_users 2024-01-01T00:00:00Z Test User <test@example.com> # original\n" +
      "@v1.0 2024-01-01T00:01:00Z Test User <test@example.com> # tag v1.0\n" +
      "add_users [add_users@v1.0] 2024-02-01T00:00:00Z Test User <test@example.com> # rework 1\n";

    expect(() => findReworkContext(plan, "add_users")).toThrow(
      /no tag exists/,
    );
  });

  it("returns correct project URI when present", () => {
    const plan =
      "%syntax-version=1.0.0\n" +
      "%project=myproject\n" +
      "%uri=https://example.com/myproject\n" +
      "\n" +
      "add_users 2024-01-01T00:00:00Z Test User <test@example.com> # add users\n" +
      "@v1.0 2024-01-01T00:01:00Z Test User <test@example.com>\n";

    const ctx = findReworkContext(plan, "add_users");
    expect(ctx.projectUri).toBe("https://example.com/myproject");
  });
});

// ---------------------------------------------------------------------------
// runRework — full integration (with temp dirs)
// ---------------------------------------------------------------------------

describe("runRework", () => {
  beforeEach(() => {
    tmpDir = createTmpDir();
  });

  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it("copies existing scripts to @tag backup files", async () => {
    setupReworkProject(tmpDir, PLAN_WITH_TAG);
    const cfg = testConfig(tmpDir);

    await runRework(
      { name: "add_users", note: "rework users" },
      cfg,
      TEST_ENV,
    );

    // Backup files should exist with original content
    const deployBackup = readFileSync(
      join(tmpDir, "deploy", "add_users@v1.0.sql"),
      "utf-8",
    );
    expect(deployBackup).toContain("CREATE TABLE users");

    const revertBackup = readFileSync(
      join(tmpDir, "revert", "add_users@v1.0.sql"),
      "utf-8",
    );
    expect(revertBackup).toContain("DROP TABLE users");

    const verifyBackup = readFileSync(
      join(tmpDir, "verify", "add_users@v1.0.sql"),
      "utf-8",
    );
    expect(verifyBackup).toContain("SELECT 1 FROM users");
  });

  it("creates fresh deploy/revert/verify scripts", async () => {
    setupReworkProject(tmpDir, PLAN_WITH_TAG);
    const cfg = testConfig(tmpDir);

    await runRework(
      { name: "add_users", note: "rework users" },
      cfg,
      TEST_ENV,
    );

    // Fresh scripts should be template content (not original)
    const deploySql = readFileSync(
      join(tmpDir, "deploy", "add_users.sql"),
      "utf-8",
    );
    expect(deploySql).toContain("-- Deploy add_users");
    expect(deploySql).toContain("-- requires: add_users@v1.0");
    expect(deploySql).not.toContain("CREATE TABLE users");

    const revertSql = readFileSync(
      join(tmpDir, "revert", "add_users.sql"),
      "utf-8",
    );
    expect(revertSql).toContain("-- Revert add_users");
    expect(revertSql).not.toContain("DROP TABLE users");

    const verifySql = readFileSync(
      join(tmpDir, "verify", "add_users.sql"),
      "utf-8",
    );
    expect(verifySql).toContain("-- Verify add_users");
    expect(verifySql).not.toContain("SELECT 1 FROM users");
  });

  it("appends reworked change to plan with correct dependency", async () => {
    setupReworkProject(tmpDir, PLAN_WITH_TAG);
    const cfg = testConfig(tmpDir);

    await runRework(
      { name: "add_users", note: "rework users" },
      cfg,
      TEST_ENV,
    );

    const plan = readFileSync(join(tmpDir, "sqitch.plan"), "utf-8");
    const lines = plan.split("\n");

    // Find the reworked change line (the second add_users entry)
    const addUserLines = lines.filter((l) => l.startsWith("add_users"));
    expect(addUserLines.length).toBe(2);

    const reworkedLine = addUserLines[1]!;
    expect(reworkedLine).toContain("[add_users@v1.0]");
    expect(reworkedLine).toContain("Test User <test@example.com>");
    expect(reworkedLine).toContain("# rework users");
  });

  it("sets correct parent for reworked change", async () => {
    setupReworkProject(tmpDir, PLAN_WITH_TAG);
    const cfg = testConfig(tmpDir);

    await runRework(
      { name: "add_users", note: "rework users" },
      cfg,
      TEST_ENV,
    );

    // Parse the resulting plan
    const planContent = readFileSync(join(tmpDir, "sqitch.plan"), "utf-8");
    const plan = parsePlan(planContent);

    expect(plan.changes.length).toBe(2);
    // The reworked change should have the original as parent
    const reworked = plan.changes[1]!;
    expect(reworked.name).toBe("add_users");
    expect(reworked.parent).toBe(plan.changes[0]!.change_id);
    expect(reworked.requires).toEqual(["add_users@v1.0"]);
  });

  it("errors when change name not provided", async () => {
    setupReworkProject(tmpDir, PLAN_WITH_TAG);
    const cfg = testConfig(tmpDir);

    const origExit = process.exit;
    let exitCode: number | undefined;
    process.exit = ((code: number) => {
      exitCode = code;
      throw new Error(`process.exit(${code})`);
    }) as never;

    try {
      await runRework({ name: "", note: "" }, cfg, TEST_ENV);
    } catch {
      // Expected
    } finally {
      process.exit = origExit;
    }

    expect(exitCode).toBe(1);
  });

  it("errors when change does not exist in plan", async () => {
    setupReworkProject(tmpDir, PLAN_WITH_TAG);
    const cfg = testConfig(tmpDir);

    const origExit = process.exit;
    let exitCode: number | undefined;
    process.exit = ((code: number) => {
      exitCode = code;
      throw new Error(`process.exit(${code})`);
    }) as never;

    try {
      await runRework(
        { name: "nonexistent_change", note: "" },
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

  it("errors when no tag exists after the change", async () => {
    setupReworkProject(tmpDir, PLAN_NO_TAG);
    const cfg = testConfig(tmpDir);

    const origExit = process.exit;
    let exitCode: number | undefined;
    process.exit = ((code: number) => {
      exitCode = code;
      throw new Error(`process.exit(${code})`);
    }) as never;

    try {
      await runRework({ name: "add_users", note: "" }, cfg, TEST_ENV);
    } catch {
      // Expected
    } finally {
      process.exit = origExit;
    }

    expect(exitCode).toBe(1);
  });

  it("errors when plan file does not exist", async () => {
    // Don't create plan file
    const cfg = testConfig(tmpDir);

    const origExit = process.exit;
    let exitCode: number | undefined;
    process.exit = ((code: number) => {
      exitCode = code;
      throw new Error(`process.exit(${code})`);
    }) as never;

    try {
      await runRework({ name: "add_users", note: "" }, cfg, TEST_ENV);
    } catch {
      // Expected
    } finally {
      process.exit = origExit;
    }

    expect(exitCode).toBe(1);
  });

  it("handles rework when change is not the last change in plan", async () => {
    // Setup project with two changes, tag after first
    const planPath = join(tmpDir, "sqitch.plan");
    writeFileSync(planPath, PLAN_TWO_CHANGES_TAG_AFTER_FIRST, "utf-8");

    // Create script directories and files for add_users
    const deployDir = join(tmpDir, "deploy");
    const revertDir = join(tmpDir, "revert");
    const verifyDir = join(tmpDir, "verify");
    mkdirSync(deployDir, { recursive: true });
    mkdirSync(revertDir, { recursive: true });
    mkdirSync(verifyDir, { recursive: true });

    writeFileSync(
      join(deployDir, "add_users.sql"),
      "-- Deploy add_users\nCREATE TABLE users (id int);\n",
      "utf-8",
    );
    writeFileSync(
      join(revertDir, "add_users.sql"),
      "-- Revert add_users\nDROP TABLE users;\n",
      "utf-8",
    );
    writeFileSync(
      join(verifyDir, "add_users.sql"),
      "-- Verify add_users\nSELECT 1 FROM users;\n",
      "utf-8",
    );

    const cfg = testConfig(tmpDir);

    await runRework(
      { name: "add_users", note: "rework users" },
      cfg,
      TEST_ENV,
    );

    // The reworked entry should be appended at the end
    const plan = readFileSync(planPath, "utf-8");
    const lines = plan.split("\n").filter((l) => l.trim() !== "" && !l.startsWith("%"));
    const lastNonEmpty = lines[lines.length - 1]!;
    expect(lastNonEmpty).toMatch(/^add_users /);
    expect(lastNonEmpty).toContain("[add_users@v1.0]");

    // Parent should be the last change (add_roles), not add_users
    const parsedPlan = parsePlan(plan);
    const reworkedChange = parsedPlan.changes[parsedPlan.changes.length - 1]!;
    expect(reworkedChange.name).toBe("add_users");
    // Parent should be add_roles (the preceding change in the plan)
    const addRolesChange = parsedPlan.changes.find(c => c.name === "add_roles");
    expect(reworkedChange.parent).toBe(addRolesChange!.change_id);
  });

  it("handles rework with empty note", async () => {
    setupReworkProject(tmpDir, PLAN_WITH_TAG);
    const cfg = testConfig(tmpDir);

    await runRework(
      { name: "add_users", note: "" },
      cfg,
      TEST_ENV,
    );

    const plan = readFileSync(join(tmpDir, "sqitch.plan"), "utf-8");
    const reworkedLine = plan.split("\n").filter(l => l.startsWith("add_users"))[1];
    expect(reworkedLine).toBeTruthy();
    // Should not have a note comment
    expect(reworkedLine).not.toContain("#");
  });

  it("handles missing scripts gracefully (no error if scripts don't exist)", async () => {
    // Set up plan but don't create script files
    const planPath = join(tmpDir, "sqitch.plan");
    writeFileSync(planPath, PLAN_WITH_TAG, "utf-8");

    const cfg = testConfig(tmpDir);

    // Should not throw even without existing scripts
    await runRework(
      { name: "add_users", note: "rework" },
      cfg,
      TEST_ENV,
    );

    // Fresh scripts should still be created
    expect(existsSync(join(tmpDir, "deploy", "add_users.sql"))).toBe(true);
    expect(existsSync(join(tmpDir, "revert", "add_users.sql"))).toBe(true);
    expect(existsSync(join(tmpDir, "verify", "add_users.sql"))).toBe(true);

    // Backup files should NOT exist (nothing to copy)
    expect(existsSync(join(tmpDir, "deploy", "add_users@v1.0.sql"))).toBe(false);
  });

  it("produces a plan that parsePlan can re-parse cleanly", async () => {
    setupReworkProject(tmpDir, PLAN_WITH_TAG);
    const cfg = testConfig(tmpDir);

    await runRework(
      { name: "add_users", note: "rework users" },
      cfg,
      TEST_ENV,
    );

    const planContent = readFileSync(join(tmpDir, "sqitch.plan"), "utf-8");
    // Should parse without errors
    const plan = parsePlan(planContent);
    expect(plan.changes.length).toBe(2);
    expect(plan.tags.length).toBe(1);
    expect(plan.changes[0]!.name).toBe("add_users");
    expect(plan.changes[1]!.name).toBe("add_users");
  });

  it("uses custom directory names from config", async () => {
    const planPath = join(tmpDir, "sqitch.plan");
    writeFileSync(planPath, PLAN_WITH_TAG, "utf-8");

    // Create custom dirs with scripts
    mkdirSync(join(tmpDir, "custom-deploy"), { recursive: true });
    mkdirSync(join(tmpDir, "custom-revert"), { recursive: true });
    mkdirSync(join(tmpDir, "custom-verify"), { recursive: true });
    writeFileSync(
      join(tmpDir, "custom-deploy", "add_users.sql"),
      "-- custom deploy\n",
      "utf-8",
    );
    writeFileSync(
      join(tmpDir, "custom-revert", "add_users.sql"),
      "-- custom revert\n",
      "utf-8",
    );
    writeFileSync(
      join(tmpDir, "custom-verify", "add_users.sql"),
      "-- custom verify\n",
      "utf-8",
    );

    const cfg = testConfig(tmpDir);
    cfg.core.deploy_dir = "custom-deploy";
    cfg.core.revert_dir = "custom-revert";
    cfg.core.verify_dir = "custom-verify";

    await runRework(
      { name: "add_users", note: "rework" },
      cfg,
      TEST_ENV,
    );

    expect(existsSync(join(tmpDir, "custom-deploy", "add_users@v1.0.sql"))).toBe(true);
    expect(existsSync(join(tmpDir, "custom-revert", "add_users@v1.0.sql"))).toBe(true);
    expect(existsSync(join(tmpDir, "custom-verify", "add_users@v1.0.sql"))).toBe(true);
  });

  it("computes a valid change ID for the reworked change", async () => {
    setupReworkProject(tmpDir, PLAN_WITH_TAG);
    const cfg = testConfig(tmpDir);

    await runRework(
      { name: "add_users", note: "rework users" },
      cfg,
      TEST_ENV,
    );

    const planContent = readFileSync(join(tmpDir, "sqitch.plan"), "utf-8");
    const plan = parsePlan(planContent);

    // Both changes should have different IDs
    expect(plan.changes[0]!.change_id).not.toBe(plan.changes[1]!.change_id);

    // The reworked change's ID should be recomputable
    const reworked = plan.changes[1]!;
    const recomputed = computeChangeId({
      project: "myproject",
      change: "add_users",
      parent: plan.changes[0]!.change_id,
      planner_name: reworked.planner_name,
      planner_email: reworked.planner_email,
      planned_at: reworked.planned_at,
      requires: ["add_users@v1.0"],
      conflicts: [],
      note: "rework users",
    });
    expect(reworked.change_id).toBe(recomputed);
  });
});

// ---------------------------------------------------------------------------
// CLI integration (subprocess)
// ---------------------------------------------------------------------------

describe("sqlever rework (subprocess)", () => {
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

  it("reworks a change via CLI", async () => {
    setupReworkProject(tmpDir, PLAN_WITH_TAG);

    const { stdout, exitCode } = await runCli(
      "rework", "add_users", "-n", "rework users",
    );
    expect(exitCode).toBe(0);
    expect(stdout).toContain("Reworked");
    expect(existsSync(join(tmpDir, "deploy", "add_users@v1.0.sql"))).toBe(true);
  });

  it("exits 1 when no change name is provided", async () => {
    setupReworkProject(tmpDir, PLAN_WITH_TAG);

    const { exitCode, stderr } = await runCli("rework");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("change name is required");
  });

  it("exits 1 when change does not exist", async () => {
    setupReworkProject(tmpDir, PLAN_WITH_TAG);

    const { exitCode, stderr } = await runCli("rework", "nonexistent");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("Unknown change");
  });

  it("exits 1 when no tag exists after the change", async () => {
    setupReworkProject(tmpDir, PLAN_NO_TAG);

    const { exitCode, stderr } = await runCli("rework", "add_users");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("no tag exists");
  });
});
