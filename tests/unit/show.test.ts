// tests/unit/show.test.ts — Tests for sqlever show command
//
// Validates show command: argument parsing, script display, change/tag
// metadata lookup, error handling, and JSON output mode.

import { describe, expect, it, beforeEach, afterEach } from "bun:test";
import {
  mkdtempSync,
  writeFileSync,
  mkdirSync,
} from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { rmSync } from "node:fs";

import {
  parseShowArgs,
  runShow,
  resolveScriptPath,
  readScript,
  findChange,
  findTag,
  formatChange,
  formatTag,
} from "../../src/commands/show";
import { computeChangeId } from "../../src/plan/types";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let tmpDir: string;

function createTmpDir(): string {
  return mkdtempSync(join(tmpdir(), "sqlever-show-test-"));
}

/** Plan content with one change and one tag. */
const PLAN_CONTENT =
  "%syntax-version=1.0.0\n" +
  "%project=myproject\n" +
  "\n" +
  "create_schema 2024-01-15T10:30:00Z Test User <test@example.com> # Bootstrap schema\n" +
  "add_users [create_schema] 2024-01-15T10:31:00Z Test User <test@example.com> # Users table\n" +
  "@v1.0 2024-01-15T10:32:00Z Test User <test@example.com> # First release\n";

/** Create a project directory with plan file and script files. */
function setupProject(dir: string, planContent?: string): void {
  const planPath = join(dir, "sqitch.plan");
  writeFileSync(planPath, planContent ?? PLAN_CONTENT, "utf-8");

  // Create script directories and files
  for (const sub of ["deploy", "revert", "verify"]) {
    mkdirSync(join(dir, sub), { recursive: true });
  }

  writeFileSync(
    join(dir, "deploy", "create_schema.sql"),
    "-- Deploy create_schema\nBEGIN;\nCREATE SCHEMA app;\nCOMMIT;\n",
    "utf-8",
  );
  writeFileSync(
    join(dir, "revert", "create_schema.sql"),
    "-- Revert create_schema\nBEGIN;\nDROP SCHEMA app;\nCOMMIT;\n",
    "utf-8",
  );
  writeFileSync(
    join(dir, "verify", "create_schema.sql"),
    "-- Verify create_schema\nSELECT 1/COUNT(*) FROM information_schema.schemata WHERE schema_name = 'app';\n",
    "utf-8",
  );
  writeFileSync(
    join(dir, "deploy", "add_users.sql"),
    "-- Deploy add_users\nBEGIN;\nCREATE TABLE app.users (id serial PRIMARY KEY);\nCOMMIT;\n",
    "utf-8",
  );
  writeFileSync(
    join(dir, "revert", "add_users.sql"),
    "-- Revert add_users\nBEGIN;\nDROP TABLE app.users;\nCOMMIT;\n",
    "utf-8",
  );
  writeFileSync(
    join(dir, "verify", "add_users.sql"),
    "-- Verify add_users\nSELECT 1/COUNT(*) FROM information_schema.tables WHERE table_name = 'users';\n",
    "utf-8",
  );
}

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
// parseShowArgs
// ---------------------------------------------------------------------------

describe("parseShowArgs", () => {
  it("parses type and name from positional args", () => {
    const opts = parseShowArgs(["deploy", "create_schema"]);
    expect(opts.type).toBe("deploy");
    expect(opts.name).toBe("create_schema");
  });

  it("parses all valid types", () => {
    for (const type of ["deploy", "revert", "verify", "change", "tag"] as const) {
      const opts = parseShowArgs([type, "my_change"]);
      expect(opts.type).toBe(type);
      expect(opts.name).toBe("my_change");
    }
  });

  it("returns empty type and name when no args", () => {
    const opts = parseShowArgs([]);
    expect(opts.type as string).toBe("");
    expect(opts.name).toBe("");
  });

  it("returns empty name when only type given", () => {
    const opts = parseShowArgs(["deploy"]);
    expect(opts.name).toBe("");
  });
});

// ---------------------------------------------------------------------------
// resolveScriptPath
// ---------------------------------------------------------------------------

describe("resolveScriptPath", () => {
  it("resolves script path under topDir", () => {
    const path = resolveScriptPath("/project", "deploy", "create_schema");
    expect(path).toBe("/project/deploy/create_schema.sql");
  });

  it("handles nested script directories", () => {
    const path = resolveScriptPath("/project", "migrations/deploy", "add_users");
    expect(path).toBe("/project/migrations/deploy/add_users.sql");
  });
});

// ---------------------------------------------------------------------------
// readScript
// ---------------------------------------------------------------------------

describe("readScript", () => {
  beforeEach(() => {
    tmpDir = createTmpDir();
  });

  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it("reads an existing script file", () => {
    const scriptPath = join(tmpDir, "test.sql");
    writeFileSync(scriptPath, "SELECT 1;\n", "utf-8");
    const content = readScript(scriptPath);
    expect(content).toBe("SELECT 1;\n");
  });

  it("returns null for nonexistent file", () => {
    const content = readScript(join(tmpDir, "nonexistent.sql"));
    expect(content).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// findChange
// ---------------------------------------------------------------------------

describe("findChange", () => {
  beforeEach(() => {
    tmpDir = createTmpDir();
  });

  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it("finds a change by name", () => {
    const planPath = join(tmpDir, "sqitch.plan");
    writeFileSync(planPath, PLAN_CONTENT, "utf-8");
    const change = findChange(planPath, "create_schema");
    expect(change).not.toBeNull();
    expect(change!.name).toBe("create_schema");
    expect(change!.planner_name).toBe("Test User");
    expect(change!.planner_email).toBe("test@example.com");
    expect(change!.note).toBe("Bootstrap schema");
  });

  it("finds a change with dependencies", () => {
    const planPath = join(tmpDir, "sqitch.plan");
    writeFileSync(planPath, PLAN_CONTENT, "utf-8");
    const change = findChange(planPath, "add_users");
    expect(change).not.toBeNull();
    expect(change!.requires).toEqual(["create_schema"]);
    expect(change!.note).toBe("Users table");
  });

  it("returns null for nonexistent change", () => {
    const planPath = join(tmpDir, "sqitch.plan");
    writeFileSync(planPath, PLAN_CONTENT, "utf-8");
    const change = findChange(planPath, "nonexistent");
    expect(change).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// findTag
// ---------------------------------------------------------------------------

describe("findTag", () => {
  beforeEach(() => {
    tmpDir = createTmpDir();
  });

  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it("finds a tag by name (without @ prefix)", () => {
    const planPath = join(tmpDir, "sqitch.plan");
    writeFileSync(planPath, PLAN_CONTENT, "utf-8");
    const tag = findTag(planPath, "v1.0");
    expect(tag).not.toBeNull();
    expect(tag!.name).toBe("v1.0");
    expect(tag!.planner_name).toBe("Test User");
    expect(tag!.note).toBe("First release");
  });

  it("returns null for nonexistent tag", () => {
    const planPath = join(tmpDir, "sqitch.plan");
    writeFileSync(planPath, PLAN_CONTENT, "utf-8");
    const tag = findTag(planPath, "v2.0");
    expect(tag).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// formatChange
// ---------------------------------------------------------------------------

describe("formatChange", () => {
  it("formats change metadata with all fields", () => {
    const firstId = computeChangeId({
      project: "myproject",
      change: "create_schema",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T10:30:00Z",
      requires: [],
      conflicts: [],
      note: "Bootstrap schema",
    });

    const output = formatChange({
      change_id: firstId,
      name: "add_users",
      project: "myproject",
      note: "Users table",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T10:31:00Z",
      requires: ["create_schema"],
      conflicts: ["old_users"],
      parent: firstId,
    });

    expect(output).toContain("Change:    add_users");
    expect(output).toContain(`ID:        ${firstId}`);
    expect(output).toContain("Project:   myproject");
    expect(output).toContain("Planner:   Test User <test@example.com>");
    expect(output).toContain("Planned:   2024-01-15T10:31:00Z");
    expect(output).toContain(`Parent:    ${firstId}`);
    expect(output).toContain("Requires:  create_schema");
    expect(output).toContain("Conflicts: old_users");
    expect(output).toContain("Note:      Users table");
  });

  it("omits optional fields when empty", () => {
    const output = formatChange({
      change_id: "abc123",
      name: "first",
      project: "myproject",
      note: "",
      planner_name: "Test",
      planner_email: "t@t.com",
      planned_at: "2024-01-01T00:00:00Z",
      requires: [],
      conflicts: [],
    });

    expect(output).not.toContain("Parent:");
    expect(output).not.toContain("Requires:");
    expect(output).not.toContain("Conflicts:");
    expect(output).not.toContain("Note:");
  });
});

// ---------------------------------------------------------------------------
// formatTag
// ---------------------------------------------------------------------------

describe("formatTag", () => {
  it("formats tag metadata with all fields", () => {
    const output = formatTag({
      tag_id: "tag123",
      name: "v1.0",
      project: "myproject",
      change_id: "change123",
      note: "First release",
      planner_name: "Test User",
      planner_email: "test@example.com",
      planned_at: "2024-01-15T10:32:00Z",
    });

    expect(output).toContain("Tag:       @v1.0");
    expect(output).toContain("ID:        tag123");
    expect(output).toContain("Project:   myproject");
    expect(output).toContain("Change:    change123");
    expect(output).toContain("Planner:   Test User <test@example.com>");
    expect(output).toContain("Planned:   2024-01-15T10:32:00Z");
    expect(output).toContain("Note:      First release");
  });

  it("omits note when empty", () => {
    const output = formatTag({
      tag_id: "tag123",
      name: "v1.0",
      project: "myproject",
      change_id: "change123",
      note: "",
      planner_name: "Test",
      planner_email: "t@t.com",
      planned_at: "2024-01-01T00:00:00Z",
    });

    expect(output).not.toContain("Note:");
  });
});

// ---------------------------------------------------------------------------
// runShow — integration tests
// ---------------------------------------------------------------------------

describe("runShow", () => {
  beforeEach(() => {
    tmpDir = createTmpDir();
  });

  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it("errors on invalid show type", () => {
    const origExit = process.exit;
    let exitCode: number | undefined;
    process.exit = ((code: number) => {
      exitCode = code;
      throw new Error(`process.exit(${code})`);
    }) as never;

    try {
      runShow({ type: "invalid" as any, name: "foo" });
    } catch {
      // Expected
    } finally {
      process.exit = origExit;
    }

    expect(exitCode).toBe(1);
  });

  it("errors when no name is provided", () => {
    const origExit = process.exit;
    let exitCode: number | undefined;
    process.exit = ((code: number) => {
      exitCode = code;
      throw new Error(`process.exit(${code})`);
    }) as never;

    try {
      runShow({ type: "deploy", name: "" });
    } catch {
      // Expected
    } finally {
      process.exit = origExit;
    }

    expect(exitCode).toBe(1);
  });

  it("errors when deploy script not found", () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    const origExit = process.exit;
    let exitCode: number | undefined;
    process.exit = ((code: number) => {
      exitCode = code;
      throw new Error(`process.exit(${code})`);
    }) as never;

    try {
      runShow({ type: "deploy", name: "nonexistent", topDir: tmpDir }, cfg);
    } catch {
      // Expected
    } finally {
      process.exit = origExit;
    }

    expect(exitCode).toBe(1);
  });

  it("errors when change not found in plan", () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    const origExit = process.exit;
    let exitCode: number | undefined;
    process.exit = ((code: number) => {
      exitCode = code;
      throw new Error(`process.exit(${code})`);
    }) as never;

    try {
      runShow({ type: "change", name: "nonexistent", topDir: tmpDir }, cfg);
    } catch {
      // Expected
    } finally {
      process.exit = origExit;
    }

    expect(exitCode).toBe(1);
  });

  it("errors when tag not found in plan", () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    const origExit = process.exit;
    let exitCode: number | undefined;
    process.exit = ((code: number) => {
      exitCode = code;
      throw new Error(`process.exit(${code})`);
    }) as never;

    try {
      runShow({ type: "tag", name: "nonexistent", topDir: tmpDir }, cfg);
    } catch {
      // Expected
    } finally {
      process.exit = origExit;
    }

    expect(exitCode).toBe(1);
  });

  it("prints deploy script to stdout", () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    // Capture stdout
    const chunks: string[] = [];
    const origWrite = process.stdout.write;
    process.stdout.write = ((chunk: string) => {
      chunks.push(chunk);
      return true;
    }) as typeof process.stdout.write;

    try {
      runShow(
        { type: "deploy", name: "create_schema", topDir: tmpDir },
        cfg,
      );
    } finally {
      process.stdout.write = origWrite;
    }

    const output = chunks.join("");
    expect(output).toContain("-- Deploy create_schema");
    expect(output).toContain("CREATE SCHEMA app;");
  });

  it("prints revert script to stdout", () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    const chunks: string[] = [];
    const origWrite = process.stdout.write;
    process.stdout.write = ((chunk: string) => {
      chunks.push(chunk);
      return true;
    }) as typeof process.stdout.write;

    try {
      runShow(
        { type: "revert", name: "create_schema", topDir: tmpDir },
        cfg,
      );
    } finally {
      process.stdout.write = origWrite;
    }

    const output = chunks.join("");
    expect(output).toContain("-- Revert create_schema");
    expect(output).toContain("DROP SCHEMA app;");
  });

  it("prints verify script to stdout", () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    const chunks: string[] = [];
    const origWrite = process.stdout.write;
    process.stdout.write = ((chunk: string) => {
      chunks.push(chunk);
      return true;
    }) as typeof process.stdout.write;

    try {
      runShow(
        { type: "verify", name: "create_schema", topDir: tmpDir },
        cfg,
      );
    } finally {
      process.stdout.write = origWrite;
    }

    const output = chunks.join("");
    expect(output).toContain("-- Verify create_schema");
    expect(output).toContain("schema_name = 'app'");
  });

  it("prints change metadata to stdout", () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    const chunks: string[] = [];
    const origWrite = process.stdout.write;
    process.stdout.write = ((chunk: string) => {
      chunks.push(chunk);
      return true;
    }) as typeof process.stdout.write;

    try {
      runShow(
        { type: "change", name: "add_users", topDir: tmpDir },
        cfg,
      );
    } finally {
      process.stdout.write = origWrite;
    }

    const output = chunks.join("");
    expect(output).toContain("Change:    add_users");
    expect(output).toContain("Requires:  create_schema");
    expect(output).toContain("Note:      Users table");
  });

  it("prints tag metadata to stdout", () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    const chunks: string[] = [];
    const origWrite = process.stdout.write;
    process.stdout.write = ((chunk: string) => {
      chunks.push(chunk);
      return true;
    }) as typeof process.stdout.write;

    try {
      runShow(
        { type: "tag", name: "v1.0", topDir: tmpDir },
        cfg,
      );
    } finally {
      process.stdout.write = origWrite;
    }

    const output = chunks.join("");
    expect(output).toContain("Tag:       @v1.0");
    expect(output).toContain("Note:      First release");
    expect(output).toContain("Planner:   Test User <test@example.com>");
  });
});

// ---------------------------------------------------------------------------
// CLI integration (subprocess)
// ---------------------------------------------------------------------------

describe("sqlever show (subprocess)", () => {
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

  it("displays deploy script via CLI", async () => {
    setupProject(tmpDir);
    const { stdout, exitCode } = await runCli(
      "show", "deploy", "create_schema",
    );
    expect(exitCode).toBe(0);
    expect(stdout).toContain("-- Deploy create_schema");
    expect(stdout).toContain("CREATE SCHEMA app;");
  });

  it("displays change metadata via CLI", async () => {
    setupProject(tmpDir);
    const { stdout, exitCode } = await runCli(
      "show", "change", "add_users",
    );
    expect(exitCode).toBe(0);
    expect(stdout).toContain("Change:    add_users");
    expect(stdout).toContain("Requires:  create_schema");
  });

  it("displays tag metadata via CLI", async () => {
    setupProject(tmpDir);
    const { stdout, exitCode } = await runCli(
      "show", "tag", "v1.0",
    );
    expect(exitCode).toBe(0);
    expect(stdout).toContain("Tag:       @v1.0");
    expect(stdout).toContain("First release");
  });

  it("exits 1 for missing script", async () => {
    setupProject(tmpDir);
    const { exitCode, stderr } = await runCli(
      "show", "deploy", "nonexistent",
    );
    expect(exitCode).toBe(1);
    expect(stderr).toContain("script not found");
  });

  it("exits 1 for missing change", async () => {
    setupProject(tmpDir);
    const { exitCode, stderr } = await runCli(
      "show", "change", "nonexistent",
    );
    expect(exitCode).toBe(1);
    expect(stderr).toContain("not found in plan");
  });

  it("exits 1 when no type is given", async () => {
    setupProject(tmpDir);
    const { exitCode, stderr } = await runCli("show");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("invalid show type");
  });

  it("exits 1 when no name is given", async () => {
    setupProject(tmpDir);
    const { exitCode, stderr } = await runCli("show", "deploy");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("name is required");
  });

  it("outputs JSON with --format json for deploy script", async () => {
    setupProject(tmpDir);
    const { stdout, exitCode } = await runCli(
      "--format", "json", "show", "deploy", "create_schema",
    );
    expect(exitCode).toBe(0);
    const parsed = JSON.parse(stdout);
    expect(parsed.type).toBe("deploy");
    expect(parsed.name).toBe("create_schema");
    expect(parsed.content).toContain("CREATE SCHEMA app;");
    expect(parsed.path).toBeTruthy();
  });

  it("outputs JSON with --format json for change metadata", async () => {
    setupProject(tmpDir);
    const { stdout, exitCode } = await runCli(
      "--format", "json", "show", "change", "add_users",
    );
    expect(exitCode).toBe(0);
    const parsed = JSON.parse(stdout);
    expect(parsed.name).toBe("add_users");
    expect(parsed.change_id).toBeTruthy();
    expect(parsed.requires).toEqual(["create_schema"]);
    expect(parsed.note).toBe("Users table");
  });
});
