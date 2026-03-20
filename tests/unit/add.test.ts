// tests/unit/add.test.ts — Tests for sqlever add command
//
// Validates add command: argument parsing, file creation, plan appending,
// duplicate detection, planner identity, and config integration.

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
  parseAddArgs,
  runAdd,
  getPlannerIdentity,
  deployTemplate,
  revertTemplate,
  verifyTemplate,
  readPlanInfo,
  nowTimestamp,
} from "../../src/commands/add";
import { computeChangeId } from "../../src/plan/types";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let tmpDir: string;

function createTmpDir(): string {
  return mkdtempSync(join(tmpdir(), "sqlever-add-test-"));
}

/** Create a minimal project directory with sqitch.plan. */
function setupProject(
  dir: string,
  planContent?: string,
): { planPath: string; deployDir: string; revertDir: string; verifyDir: string } {
  const planPath = join(dir, "sqitch.plan");
  const deployDir = join(dir, "deploy");
  const revertDir = join(dir, "revert");
  const verifyDir = join(dir, "verify");

  writeFileSync(
    planPath,
    planContent ??
      "%syntax-version=1.0.0\n" +
      "%project=myproject\n" +
      "\n",
    "utf-8",
  );

  return { planPath, deployDir, revertDir, verifyDir };
}

/** Mock environment with planner identity to avoid git dependency. */
const TEST_ENV: Record<string, string | undefined> = {
  SQLEVER_USER_NAME: "Test User",
  SQLEVER_USER_EMAIL: "test@example.com",
};

/** Create a minimal MergedConfig for testing. */
function testConfig(topDir: string) {
  // We import loadConfig lazily to avoid circular issues.
  // Instead, create a minimal mock config.
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
// parseAddArgs
// ---------------------------------------------------------------------------

describe("parseAddArgs", () => {
  it("parses a simple change name", () => {
    const opts = parseAddArgs(["create_users"]);
    expect(opts.name).toBe("create_users");
    expect(opts.note).toBe("");
    expect(opts.requires).toEqual([]);
    expect(opts.conflicts).toEqual([]);
    expect(opts.noVerify).toBe(false);
  });

  it("parses -n / --note", () => {
    const opts1 = parseAddArgs(["create_users", "-n", "Add users table"]);
    expect(opts1.note).toBe("Add users table");

    const opts2 = parseAddArgs(["create_users", "--note", "Add users table"]);
    expect(opts2.note).toBe("Add users table");
  });

  it("parses -r / --requires (single)", () => {
    const opts = parseAddArgs(["add_users", "-r", "create_schema"]);
    expect(opts.requires).toEqual(["create_schema"]);
  });

  it("parses -r / --requires (multiple)", () => {
    const opts = parseAddArgs([
      "add_users",
      "-r", "create_schema",
      "--requires", "add_roles",
    ]);
    expect(opts.requires).toEqual(["create_schema", "add_roles"]);
  });

  it("parses -c / --conflicts (single)", () => {
    const opts = parseAddArgs(["add_users", "-c", "old_users"]);
    expect(opts.conflicts).toEqual(["old_users"]);
  });

  it("parses -c / --conflicts (multiple)", () => {
    const opts = parseAddArgs([
      "add_users",
      "-c", "old_users",
      "--conflicts", "legacy_auth",
    ]);
    expect(opts.conflicts).toEqual(["old_users", "legacy_auth"]);
  });

  it("parses --no-verify", () => {
    const opts = parseAddArgs(["add_users", "--no-verify"]);
    expect(opts.noVerify).toBe(true);
  });

  it("parses all flags together", () => {
    const opts = parseAddArgs([
      "add_users",
      "-n", "Add users table",
      "-r", "create_schema",
      "-r", "add_roles",
      "-c", "old_users",
      "--no-verify",
    ]);
    expect(opts.name).toBe("add_users");
    expect(opts.note).toBe("Add users table");
    expect(opts.requires).toEqual(["create_schema", "add_roles"]);
    expect(opts.conflicts).toEqual(["old_users"]);
    expect(opts.noVerify).toBe(true);
  });

  it("returns empty name when no positional arg given", () => {
    const opts = parseAddArgs(["-n", "some note"]);
    expect(opts.name).toBe("");
  });
});

// ---------------------------------------------------------------------------
// File templates
// ---------------------------------------------------------------------------

describe("deployTemplate", () => {
  it("generates deploy script with no requires", () => {
    const content = deployTemplate("create_users", []);
    expect(content).toBe(
      "-- Deploy create_users\n" +
      "-- requires:\n" +
      "\n" +
      "BEGIN;\n" +
      "\n" +
      "-- XXX Add DDL here.\n" +
      "\n" +
      "COMMIT;\n",
    );
  });

  it("generates deploy script with requires", () => {
    const content = deployTemplate("add_users", ["create_schema", "add_roles"]);
    expect(content).toContain("-- requires: create_schema, add_roles");
  });
});

describe("revertTemplate", () => {
  it("generates revert script", () => {
    const content = revertTemplate("create_users");
    expect(content).toBe(
      "-- Revert create_users\n" +
      "\n" +
      "BEGIN;\n" +
      "\n" +
      "-- XXX Add revert DDL here.\n" +
      "\n" +
      "COMMIT;\n",
    );
  });
});

describe("verifyTemplate", () => {
  it("generates verify script with ROLLBACK", () => {
    const content = verifyTemplate("create_users");
    expect(content).toBe(
      "-- Verify create_users\n" +
      "\n" +
      "BEGIN;\n" +
      "\n" +
      "-- XXX Add verification here.\n" +
      "\n" +
      "ROLLBACK;\n",
    );
  });
});

// ---------------------------------------------------------------------------
// nowTimestamp
// ---------------------------------------------------------------------------

describe("nowTimestamp", () => {
  it("returns ISO 8601 format without milliseconds", () => {
    const ts = nowTimestamp();
    expect(ts).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/);
  });
});

// ---------------------------------------------------------------------------
// getPlannerIdentity
// ---------------------------------------------------------------------------

describe("getPlannerIdentity", () => {
  it("uses SQLEVER_USER_NAME and SQLEVER_USER_EMAIL env vars", () => {
    const identity = getPlannerIdentity({
      SQLEVER_USER_NAME: "Jane Doe",
      SQLEVER_USER_EMAIL: "jane@example.com",
    });
    expect(identity.name).toBe("Jane Doe");
    expect(identity.email).toBe("jane@example.com");
  });

  it("falls back to git config when env vars not set", () => {
    // This test depends on git being configured; if not, it falls back
    const identity = getPlannerIdentity({});
    expect(identity.name).toBeTruthy();
    expect(identity.email).toBeTruthy();
  });

  it("provides default fallback when nothing is configured", () => {
    // We can't easily remove git config, so just check the types
    const identity = getPlannerIdentity({
      SQLEVER_USER_NAME: "",
      SQLEVER_USER_EMAIL: "",
    });
    // With empty strings, should fall through to git or fallback
    expect(typeof identity.name).toBe("string");
    expect(typeof identity.email).toBe("string");
  });
});

// ---------------------------------------------------------------------------
// readPlanInfo
// ---------------------------------------------------------------------------

describe("readPlanInfo", () => {
  beforeEach(() => {
    tmpDir = createTmpDir();
  });

  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it("reads project name from pragma", () => {
    const planPath = join(tmpDir, "sqitch.plan");
    writeFileSync(
      planPath,
      "%syntax-version=1.0.0\n%project=testproject\n\n",
      "utf-8",
    );
    const info = readPlanInfo(planPath);
    expect(info.projectName).toBe("testproject");
  });

  it("reads project URI from pragma", () => {
    const planPath = join(tmpDir, "sqitch.plan");
    writeFileSync(
      planPath,
      "%syntax-version=1.0.0\n%project=testproject\n%uri=https://example.com/\n\n",
      "utf-8",
    );
    const info = readPlanInfo(planPath);
    expect(info.projectUri).toBe("https://example.com/");
  });

  it("detects existing change names", () => {
    const planPath = join(tmpDir, "sqitch.plan");
    writeFileSync(
      planPath,
      "%syntax-version=1.0.0\n" +
      "%project=myproject\n" +
      "\n" +
      "create_schema 2024-01-15T10:30:00Z Test User <test@example.com> # first\n" +
      "add_users 2024-01-15T10:31:00Z Test User <test@example.com> # second\n",
      "utf-8",
    );
    const info = readPlanInfo(planPath);
    expect(info.existingNames.has("create_schema")).toBe(true);
    expect(info.existingNames.has("add_users")).toBe(true);
    expect(info.existingNames.has("nonexistent")).toBe(false);
  });

  it("computes lastChangeId correctly for a single change", () => {
    const planPath = join(tmpDir, "sqitch.plan");
    writeFileSync(
      planPath,
      "%syntax-version=1.0.0\n" +
      "%project=myproject\n" +
      "\n" +
      "create_schema 2024-01-15T10:30:00Z Test User <test@example.com> # first\n",
      "utf-8",
    );
    const info = readPlanInfo(planPath);

    // Compute expected ID manually
    const expectedId = computeChangeId({
      project: "myproject",
      change: "create_schema",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T10:30:00Z",
      requires: [],
      conflicts: [],
      note: "first",
    });
    expect(info.lastChangeId).toBe(expectedId);
  });

  it("computes lastChangeId with parent chaining for multiple changes", () => {
    const planPath = join(tmpDir, "sqitch.plan");
    writeFileSync(
      planPath,
      "%syntax-version=1.0.0\n" +
      "%project=myproject\n" +
      "\n" +
      "first 2024-01-15T10:30:00Z Test User <test@example.com>\n" +
      "second 2024-01-15T10:31:00Z Test User <test@example.com>\n",
      "utf-8",
    );
    const info = readPlanInfo(planPath);

    // Compute expected IDs
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
    expect(info.lastChangeId).toBe(secondId);
  });

  it("handles empty plan (no changes)", () => {
    const planPath = join(tmpDir, "sqitch.plan");
    writeFileSync(
      planPath,
      "%syntax-version=1.0.0\n%project=myproject\n\n",
      "utf-8",
    );
    const info = readPlanInfo(planPath);
    expect(info.existingNames.size).toBe(0);
    expect(info.lastChangeId).toBeUndefined();
  });

  it("skips tag lines", () => {
    const planPath = join(tmpDir, "sqitch.plan");
    writeFileSync(
      planPath,
      "%syntax-version=1.0.0\n" +
      "%project=myproject\n" +
      "\n" +
      "first 2024-01-15T10:30:00Z Test User <test@example.com>\n" +
      "@v1.0 2024-01-15T10:31:00Z Test User <test@example.com> # tag\n",
      "utf-8",
    );
    const info = readPlanInfo(planPath);
    expect(info.existingNames.has("first")).toBe(true);
    expect(info.existingNames.has("@v1.0")).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// runAdd — full integration (with temp dirs)
// ---------------------------------------------------------------------------

describe("runAdd", () => {
  beforeEach(() => {
    tmpDir = createTmpDir();
  });

  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it("creates deploy, revert, verify files and appends to plan", async () => {
    const { planPath } = setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    await runAdd(
      {
        name: "create_users",
        note: "Add users table",
        requires: [],
        conflicts: [],
        noVerify: false,
      },
      cfg,
      TEST_ENV,
    );

    // Check files were created
    expect(existsSync(join(tmpDir, "deploy", "create_users.sql"))).toBe(true);
    expect(existsSync(join(tmpDir, "revert", "create_users.sql"))).toBe(true);
    expect(existsSync(join(tmpDir, "verify", "create_users.sql"))).toBe(true);

    // Check deploy script content
    const deploySql = readFileSync(join(tmpDir, "deploy", "create_users.sql"), "utf-8");
    expect(deploySql).toContain("-- Deploy create_users");
    expect(deploySql).toContain("BEGIN;");
    expect(deploySql).toContain("COMMIT;");

    // Check revert script content
    const revertSql = readFileSync(join(tmpDir, "revert", "create_users.sql"), "utf-8");
    expect(revertSql).toContain("-- Revert create_users");

    // Check verify script content
    const verifySql = readFileSync(join(tmpDir, "verify", "create_users.sql"), "utf-8");
    expect(verifySql).toContain("-- Verify create_users");
    expect(verifySql).toContain("ROLLBACK;");

    // Check plan was updated
    const plan = readFileSync(planPath, "utf-8");
    expect(plan).toContain("create_users");
    expect(plan).toContain("Test User <test@example.com>");
    expect(plan).toContain("# Add users table");
  });

  it("creates files with correct templates", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    await runAdd(
      {
        name: "add_roles",
        note: "",
        requires: ["create_schema"],
        conflicts: [],
        noVerify: false,
      },
      cfg,
      TEST_ENV,
    );

    const deploySql = readFileSync(join(tmpDir, "deploy", "add_roles.sql"), "utf-8");
    expect(deploySql).toContain("-- requires: create_schema");
  });

  it("appends change with dependencies to plan", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    await runAdd(
      {
        name: "add_users",
        note: "users",
        requires: ["create_schema"],
        conflicts: ["old_users"],
        noVerify: false,
      },
      cfg,
      TEST_ENV,
    );

    const plan = readFileSync(join(tmpDir, "sqitch.plan"), "utf-8");
    expect(plan).toContain("[create_schema !old_users]");
  });

  it("computes correct change ID", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    await runAdd(
      {
        name: "create_users",
        note: "Add users table",
        requires: [],
        conflicts: [],
        noVerify: false,
      },
      cfg,
      TEST_ENV,
    );

    const plan = readFileSync(join(tmpDir, "sqitch.plan"), "utf-8");

    // Parse the timestamp from the plan to compute expected ID
    const changeLine = plan.split("\n").find((l) => l.startsWith("create_users"));
    expect(changeLine).toBeTruthy();

    // Extract timestamp from the line
    const tsMatch = changeLine!.match(/(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)/);
    expect(tsMatch).toBeTruthy();

    const expectedId = computeChangeId({
      project: "myproject",
      change: "create_users",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: tsMatch![1]!,
      requires: [],
      conflicts: [],
      note: "Add users table",
    });

    // The change ID doesn't appear in the plan line itself
    // (it's computed on read), but we verify it via readPlanInfo
    const info = readPlanInfo(join(tmpDir, "sqitch.plan"));
    expect(info.lastChangeId).toBe(expectedId);
  });

  it("errors on duplicate change name", async () => {
    setupProject(
      tmpDir,
      "%syntax-version=1.0.0\n" +
      "%project=myproject\n" +
      "\n" +
      "create_users 2024-01-15T10:30:00Z Test User <test@example.com> # existing\n",
    );
    const cfg = testConfig(tmpDir);

    // Mock process.exit to capture exit
    const origExit = process.exit;
    let exitCode: number | undefined;
    process.exit = ((code: number) => {
      exitCode = code;
      throw new Error(`process.exit(${code})`);
    }) as never;

    try {
      await runAdd(
        {
          name: "create_users",
          note: "duplicate",
          requires: [],
          conflicts: [],
          noVerify: false,
        },
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
    // Don't create sqitch.plan
    const cfg = testConfig(tmpDir);

    const origExit = process.exit;
    let exitCode: number | undefined;
    process.exit = ((code: number) => {
      exitCode = code;
      throw new Error(`process.exit(${code})`);
    }) as never;

    try {
      await runAdd(
        {
          name: "create_users",
          note: "",
          requires: [],
          conflicts: [],
          noVerify: false,
        },
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

  it("errors on invalid change name", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    const origExit = process.exit;
    let exitCode: number | undefined;
    process.exit = ((code: number) => {
      exitCode = code;
      throw new Error(`process.exit(${code})`);
    }) as never;

    try {
      await runAdd(
        {
          name: "123-invalid",
          note: "",
          requires: [],
          conflicts: [],
          noVerify: false,
        },
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

  it("errors when no change name provided", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    const origExit = process.exit;
    let exitCode: number | undefined;
    process.exit = ((code: number) => {
      exitCode = code;
      throw new Error(`process.exit(${code})`);
    }) as never;

    try {
      await runAdd(
        {
          name: "",
          note: "",
          requires: [],
          conflicts: [],
          noVerify: false,
        },
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

  it("skips verify file when --no-verify is set", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    await runAdd(
      {
        name: "create_users",
        note: "",
        requires: [],
        conflicts: [],
        noVerify: true,
      },
      cfg,
      TEST_ENV,
    );

    expect(existsSync(join(tmpDir, "deploy", "create_users.sql"))).toBe(true);
    expect(existsSync(join(tmpDir, "revert", "create_users.sql"))).toBe(true);
    expect(existsSync(join(tmpDir, "verify", "create_users.sql"))).toBe(false);
  });

  it("creates directories if they don't exist", async () => {
    // Only create plan file, not the dirs
    const planPath = join(tmpDir, "sqitch.plan");
    writeFileSync(
      planPath,
      "%syntax-version=1.0.0\n%project=myproject\n\n",
      "utf-8",
    );
    const cfg = testConfig(tmpDir);

    await runAdd(
      {
        name: "create_users",
        note: "",
        requires: [],
        conflicts: [],
        noVerify: false,
      },
      cfg,
      TEST_ENV,
    );

    expect(existsSync(join(tmpDir, "deploy"))).toBe(true);
    expect(existsSync(join(tmpDir, "revert"))).toBe(true);
    expect(existsSync(join(tmpDir, "verify"))).toBe(true);
  });

  it("uses custom directory names from config", async () => {
    const planPath = join(tmpDir, "sqitch.plan");
    writeFileSync(
      planPath,
      "%syntax-version=1.0.0\n%project=myproject\n\n",
      "utf-8",
    );
    const cfg = testConfig(tmpDir);
    cfg.core.deploy_dir = "migrations/deploy";
    cfg.core.revert_dir = "migrations/revert";
    cfg.core.verify_dir = "migrations/verify";

    await runAdd(
      {
        name: "create_users",
        note: "",
        requires: [],
        conflicts: [],
        noVerify: false,
      },
      cfg,
      TEST_ENV,
    );

    expect(existsSync(join(tmpDir, "migrations", "deploy", "create_users.sql"))).toBe(true);
    expect(existsSync(join(tmpDir, "migrations", "revert", "create_users.sql"))).toBe(true);
    expect(existsSync(join(tmpDir, "migrations", "verify", "create_users.sql"))).toBe(true);
  });

  it("sets parent ID when adding to a plan with existing changes", async () => {
    // Set up a plan with one existing change
    const planContent =
      "%syntax-version=1.0.0\n" +
      "%project=myproject\n" +
      "\n" +
      "first 2024-01-15T10:30:00Z Test User <test@example.com> # first change\n";
    setupProject(tmpDir, planContent);
    const cfg = testConfig(tmpDir);

    await runAdd(
      {
        name: "second",
        note: "second change",
        requires: [],
        conflicts: [],
        noVerify: false,
      },
      cfg,
      TEST_ENV,
    );

    // Read back the plan info and verify the second change has the first as parent
    const info = readPlanInfo(join(tmpDir, "sqitch.plan"));

    // The first change's ID
    const firstId = computeChangeId({
      project: "myproject",
      change: "first",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T10:30:00Z",
      requires: [],
      conflicts: [],
      note: "first change",
    });

    // Parse the second change's timestamp from the plan
    const plan = readFileSync(join(tmpDir, "sqitch.plan"), "utf-8");
    const secondLine = plan.split("\n").find((l) => l.startsWith("second"));
    expect(secondLine).toBeTruthy();
    const tsMatch = secondLine!.match(/(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)/);

    const expectedSecondId = computeChangeId({
      project: "myproject",
      change: "second",
      parent: firstId,
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: tsMatch![1]!,
      requires: [],
      conflicts: [],
      note: "second change",
    });

    expect(info.lastChangeId).toBe(expectedSecondId);
  });

  it("handles plan with URI pragma", async () => {
    setupProject(
      tmpDir,
      "%syntax-version=1.0.0\n" +
      "%project=myproject\n" +
      "%uri=https://example.com/myproject\n" +
      "\n",
    );
    const cfg = testConfig(tmpDir);

    await runAdd(
      {
        name: "create_users",
        note: "users",
        requires: [],
        conflicts: [],
        noVerify: false,
      },
      cfg,
      TEST_ENV,
    );

    // Verify it was added (the URI affects change ID computation)
    const info = readPlanInfo(join(tmpDir, "sqitch.plan"));
    expect(info.existingNames.has("create_users")).toBe(true);
    expect(info.projectUri).toBe("https://example.com/myproject");
  });

  it("errors if deploy script already exists", async () => {
    setupProject(tmpDir);
    mkdirSync(join(tmpDir, "deploy"), { recursive: true });
    writeFileSync(join(tmpDir, "deploy", "create_users.sql"), "existing", "utf-8");
    const cfg = testConfig(tmpDir);

    const origExit = process.exit;
    let exitCode: number | undefined;
    process.exit = ((code: number) => {
      exitCode = code;
      throw new Error(`process.exit(${code})`);
    }) as never;

    try {
      await runAdd(
        {
          name: "create_users",
          note: "",
          requires: [],
          conflicts: [],
          noVerify: false,
        },
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

  it("correctly formats plan entry with multiple deps", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    await runAdd(
      {
        name: "add_users",
        note: "users table",
        requires: ["create_schema", "add_roles"],
        conflicts: ["old_users"],
        noVerify: false,
      },
      cfg,
      TEST_ENV,
    );

    const plan = readFileSync(join(tmpDir, "sqitch.plan"), "utf-8");
    const changeLine = plan.split("\n").find((l) => l.startsWith("add_users"));
    expect(changeLine).toBeTruthy();
    expect(changeLine).toContain("[create_schema add_roles !old_users]");
    expect(changeLine).toContain("# users table");
  });

  it("handles change name with hyphens and underscores", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    await runAdd(
      {
        name: "add-user_accounts",
        note: "",
        requires: [],
        conflicts: [],
        noVerify: false,
      },
      cfg,
      TEST_ENV,
    );

    expect(existsSync(join(tmpDir, "deploy", "add-user_accounts.sql"))).toBe(true);
    const plan = readFileSync(join(tmpDir, "sqitch.plan"), "utf-8");
    expect(plan).toContain("add-user_accounts");
  });

  it("adds multiple changes sequentially", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    await runAdd(
      {
        name: "first_change",
        note: "first",
        requires: [],
        conflicts: [],
        noVerify: false,
      },
      cfg,
      TEST_ENV,
    );

    await runAdd(
      {
        name: "second_change",
        note: "second",
        requires: ["first_change"],
        conflicts: [],
        noVerify: false,
      },
      cfg,
      TEST_ENV,
    );

    const plan = readFileSync(join(tmpDir, "sqitch.plan"), "utf-8");
    const lines = plan.split("\n").filter((l) => !l.startsWith("%") && l.trim() !== "");
    expect(lines.length).toBe(2);
    expect(lines[0]).toMatch(/^first_change /);
    expect(lines[1]).toMatch(/^second_change /);
    expect(lines[1]).toContain("[first_change]");
  });
});

// ---------------------------------------------------------------------------
// CLI integration (subprocess)
// ---------------------------------------------------------------------------

describe("sqlever add (subprocess)", () => {
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

  it("creates migration files via CLI", async () => {
    setupProject(tmpDir);

    const { stdout, exitCode } = await runCli(
      "add", "create_users", "-n", "Add users table",
    );
    expect(exitCode).toBe(0);
    expect(stdout).toContain("Added");
    expect(existsSync(join(tmpDir, "deploy", "create_users.sql"))).toBe(true);
    expect(existsSync(join(tmpDir, "revert", "create_users.sql"))).toBe(true);
    expect(existsSync(join(tmpDir, "verify", "create_users.sql"))).toBe(true);
  });

  it("exits 1 when no change name is provided", async () => {
    setupProject(tmpDir);

    const { exitCode, stderr } = await runCli("add");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("change name is required");
  });

  it("exits 1 when plan file is missing", async () => {
    // No project setup
    const { exitCode, stderr } = await runCli("add", "test_change");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("plan file not found");
  });
});
