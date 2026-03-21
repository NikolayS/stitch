import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { mkdtemp, writeFile, mkdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import {
  computeStatus,
  formatStatusText,
  resolveTargetUri,
  parseStatusOptions,
  type StatusResult,
} from "../../src/commands/status";
import type { Plan } from "../../src/plan/types";
import type { Change as RegistryChange } from "../../src/db/registry";
import type { MergedConfig } from "../../src/config/index";
import type { ParsedArgs } from "../../src/cli";
import { resetConfig } from "../../src/output";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Build a minimal ParsedArgs for status, overriding specific fields. */
function makeArgs(overrides: Partial<ParsedArgs> = {}): ParsedArgs {
  return {
    command: "status",
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

/** Build a minimal plan with the given changes. */
function makePlan(
  projectName: string,
  changes: Array<{ name: string; change_id: string }>,
): Plan {
  return {
    project: { name: projectName },
    pragmas: new Map([
      ["syntax-version", "1.0.0"],
      ["project", projectName],
    ]),
    changes: changes.map((c) => ({
      change_id: c.change_id,
      name: c.name,
      project: projectName,
      note: "",
      planner_name: "Test",
      planner_email: "test@test.com",
      planned_at: "2024-01-01T00:00:00Z",
      requires: [],
      conflicts: [],
    })),
    tags: [],
  };
}

/** Build a minimal registry Change record. */
function makeRegistryChange(
  name: string,
  change_id: string,
  overrides: Partial<RegistryChange> = {},
): RegistryChange {
  return {
    change_id,
    script_hash: null,
    change: name,
    project: "myproject",
    note: "",
    committed_at: new Date("2024-01-15T10:30:00Z"),
    committer_name: "Deployer",
    committer_email: "deploy@test.com",
    planned_at: new Date("2024-01-01T00:00:00Z"),
    planner_name: "Test",
    planner_email: "test@test.com",
    ...overrides,
  };
}

/** Build a minimal MergedConfig for resolveTargetUri tests. */
function makeConfig(overrides: Partial<MergedConfig> = {}): MergedConfig {
  return {
    core: {
      engine: "pg",
      top_dir: ".",
      deploy_dir: "deploy",
      revert_dir: "revert",
      verify_dir: "verify",
      plan_file: "sqitch.plan",
    },
    deploy: {
      verify: true,
      mode: "change",
      lock_retries: 0,
      lock_timeout: "5s",
      idle_in_transaction_session_timeout: "10min",
    },
    engines: {},
    targets: {},
    analysis: {},
    sqitchConf: { entries: [], rawLines: [] },
    sqleverToml: null,
    ...overrides,
  };
}

/** Compute SHA-1 of a buffer (matches computeScriptHash behavior). */
function sha1(content: string): string {
  return createHash("sha1").update(Buffer.from(content, "utf-8")).digest("hex");
}

// ---------------------------------------------------------------------------
// Tests: parseStatusOptions
// ---------------------------------------------------------------------------

describe("parseStatusOptions", () => {
  test("defaults to topDir='.', format='text', no overrides", () => {
    const opts = parseStatusOptions(makeArgs());
    expect(opts.topDir).toBe(".");
    expect(opts.format).toBe("text");
    expect(opts.dbUri).toBeUndefined();
    expect(opts.target).toBeUndefined();
    expect(opts.planFile).toBeUndefined();
  });

  test("passes through topDir, format, dbUri, target, planFile", () => {
    const opts = parseStatusOptions(
      makeArgs({
        topDir: "/my/project",
        format: "json",
        dbUri: "postgresql://host/db",
        target: "prod",
        planFile: "custom.plan",
      }),
    );
    expect(opts.topDir).toBe("/my/project");
    expect(opts.format).toBe("json");
    expect(opts.dbUri).toBe("postgresql://host/db");
    expect(opts.target).toBe("prod");
    expect(opts.planFile).toBe("custom.plan");
  });
});

// ---------------------------------------------------------------------------
// Tests: resolveTargetUri
// ---------------------------------------------------------------------------

describe("resolveTargetUri", () => {
  test("returns --db-uri when provided", () => {
    const config = makeConfig();
    const result = resolveTargetUri(config, "postgresql://host/db");
    expect(result).toBe("postgresql://host/db");
  });

  test("--db-uri takes precedence over --target", () => {
    const config = makeConfig({
      targets: { prod: { name: "prod", uri: "postgresql://prod/db" } },
    });
    const result = resolveTargetUri(
      config,
      "postgresql://override/db",
      "prod",
    );
    expect(result).toBe("postgresql://override/db");
  });

  test("looks up --target name in config targets", () => {
    const config = makeConfig({
      targets: { prod: { name: "prod", uri: "postgresql://prod/db" } },
    });
    const result = resolveTargetUri(config, undefined, "prod");
    expect(result).toBe("postgresql://prod/db");
  });

  test("treats --target as URI if it contains ://", () => {
    const config = makeConfig();
    const result = resolveTargetUri(
      config,
      undefined,
      "postgresql://inline/db",
    );
    expect(result).toBe("postgresql://inline/db");
  });

  test("returns null for unknown --target name without ://", () => {
    const config = makeConfig();
    const result = resolveTargetUri(config, undefined, "nonexistent");
    expect(result).toBeNull();
  });

  test("falls back to default engine target", () => {
    const config = makeConfig({
      engines: { pg: { name: "pg", target: "dev" } },
      targets: { dev: { name: "dev", uri: "postgresql://dev/db" } },
    });
    const result = resolveTargetUri(config);
    expect(result).toBe("postgresql://dev/db");
  });

  test("engine target can be a direct URI", () => {
    const config = makeConfig({
      engines: { pg: { name: "pg", target: "postgresql://engine-direct/db" } },
    });
    const result = resolveTargetUri(config);
    expect(result).toBe("postgresql://engine-direct/db");
  });

  test("returns null when no target is configured anywhere", () => {
    const config = makeConfig();
    const result = resolveTargetUri(config);
    expect(result).toBeNull();
  });

  test("returns null when engine has no target", () => {
    const config = makeConfig({
      engines: { pg: { name: "pg" } },
    });
    const result = resolveTargetUri(config);
    expect(result).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Tests: computeStatus
// ---------------------------------------------------------------------------

describe("computeStatus", () => {
  let tempDir: string;
  let deployDir: string;

  beforeEach(async () => {
    resetConfig();
    tempDir = await mkdtemp(join(tmpdir(), "sqlever-status-test-"));
    deployDir = join(tempDir, "deploy");
    await mkdir(deployDir, { recursive: true });
  });

  afterEach(async () => {
    await rm(tempDir, { recursive: true, force: true });
  });

  test("returns correct counts when all changes are deployed", () => {
    const plan = makePlan("myproject", [
      { name: "change_a", change_id: "aaa" },
      { name: "change_b", change_id: "bbb" },
    ]);
    const deployed = [
      makeRegistryChange("change_a", "aaa"),
      makeRegistryChange("change_b", "bbb"),
    ];

    const result = computeStatus(plan, deployed, "pg://host/db", deployDir);

    expect(result.project).toBe("myproject");
    expect(result.target).toBe("pg://host/db");
    expect(result.deployed_count).toBe(2);
    expect(result.pending_count).toBe(0);
    expect(result.pending_changes).toEqual([]);
  });

  test("returns correct counts when some changes are pending", () => {
    const plan = makePlan("myproject", [
      { name: "change_a", change_id: "aaa" },
      { name: "change_b", change_id: "bbb" },
      { name: "change_c", change_id: "ccc" },
    ]);
    const deployed = [makeRegistryChange("change_a", "aaa")];

    const result = computeStatus(plan, deployed, null, deployDir);

    expect(result.deployed_count).toBe(1);
    expect(result.pending_count).toBe(2);
    expect(result.pending_changes).toEqual(["change_b", "change_c"]);
  });

  test("returns correct counts when no changes are deployed", () => {
    const plan = makePlan("myproject", [
      { name: "change_a", change_id: "aaa" },
      { name: "change_b", change_id: "bbb" },
    ]);

    const result = computeStatus(plan, [], null, deployDir);

    expect(result.deployed_count).toBe(0);
    expect(result.pending_count).toBe(2);
    expect(result.pending_changes).toEqual(["change_a", "change_b"]);
    expect(result.last_deployed).toBeNull();
  });

  test("returns correct counts for empty plan and no deployments", () => {
    const plan = makePlan("myproject", []);

    const result = computeStatus(plan, [], null, deployDir);

    expect(result.deployed_count).toBe(0);
    expect(result.pending_count).toBe(0);
    expect(result.pending_changes).toEqual([]);
    expect(result.last_deployed).toBeNull();
  });

  test("populates last_deployed from the last deployed change", () => {
    const plan = makePlan("myproject", [
      { name: "change_a", change_id: "aaa" },
      { name: "change_b", change_id: "bbb" },
    ]);
    const deployed = [
      makeRegistryChange("change_a", "aaa", {
        committed_at: new Date("2024-01-10T08:00:00Z"),
        committer_name: "Alice",
      }),
      makeRegistryChange("change_b", "bbb", {
        committed_at: new Date("2024-01-15T10:30:00Z"),
        committer_name: "Bob",
      }),
    ];

    const result = computeStatus(plan, deployed, null, deployDir);

    expect(result.last_deployed).not.toBeNull();
    expect(result.last_deployed!.change).toBe("change_b");
    expect(result.last_deployed!.change_id).toBe("bbb");
    expect(result.last_deployed!.committed_at).toBe(
      "2024-01-15T10:30:00.000Z",
    );
    expect(result.last_deployed!.committer_name).toBe("Bob");
  });

  test("detects modified scripts via script_hash comparison", async () => {
    const originalContent = "CREATE TABLE foo (id int);";
    const originalHash = sha1(originalContent);
    const modifiedContent = "CREATE TABLE foo (id int, name text);";

    // Write the modified file on disk
    await writeFile(join(deployDir, "change_a.sql"), modifiedContent);

    const plan = makePlan("myproject", [
      { name: "change_a", change_id: "aaa" },
    ]);
    const deployed = [
      makeRegistryChange("change_a", "aaa", {
        script_hash: originalHash,
      }),
    ];

    const result = computeStatus(plan, deployed, null, deployDir);

    expect(result.modified_scripts).toHaveLength(1);
    expect(result.modified_scripts[0]!.change).toBe("change_a");
    expect(result.modified_scripts[0]!.registry_hash).toBe(originalHash);
    expect(result.modified_scripts[0]!.current_hash).toBe(
      sha1(modifiedContent),
    );
  });

  test("does not flag unmodified scripts", async () => {
    const content = "CREATE TABLE bar (id int);";
    const hash = sha1(content);

    await writeFile(join(deployDir, "change_a.sql"), content);

    const plan = makePlan("myproject", [
      { name: "change_a", change_id: "aaa" },
    ]);
    const deployed = [
      makeRegistryChange("change_a", "aaa", { script_hash: hash }),
    ];

    const result = computeStatus(plan, deployed, null, deployDir);
    expect(result.modified_scripts).toHaveLength(0);
  });

  test("skips script_hash check when registry has null hash", async () => {
    await writeFile(join(deployDir, "change_a.sql"), "anything");

    const plan = makePlan("myproject", [
      { name: "change_a", change_id: "aaa" },
    ]);
    const deployed = [
      makeRegistryChange("change_a", "aaa", { script_hash: null }),
    ];

    const result = computeStatus(plan, deployed, null, deployDir);
    expect(result.modified_scripts).toHaveLength(0);
  });

  test("skips script_hash check when deploy file does not exist", () => {
    const plan = makePlan("myproject", [
      { name: "change_a", change_id: "aaa" },
    ]);
    const deployed = [
      makeRegistryChange("change_a", "aaa", {
        script_hash: "abc123",
      }),
    ];

    const result = computeStatus(plan, deployed, null, deployDir);
    expect(result.modified_scripts).toHaveLength(0);
  });

  test("preserves pending change order from plan", () => {
    const plan = makePlan("myproject", [
      { name: "alpha", change_id: "aaa" },
      { name: "bravo", change_id: "bbb" },
      { name: "charlie", change_id: "ccc" },
      { name: "delta", change_id: "ddd" },
    ]);
    // Deploy only alpha and charlie (out of plan order)
    const deployed = [
      makeRegistryChange("alpha", "aaa"),
      makeRegistryChange("charlie", "ccc"),
    ];

    const result = computeStatus(plan, deployed, null, deployDir);
    expect(result.pending_changes).toEqual(["bravo", "delta"]);
  });

  test("includes target in result", () => {
    const plan = makePlan("myproject", []);
    const result = computeStatus(
      plan,
      [],
      "postgresql://host/mydb",
      deployDir,
    );
    expect(result.target).toBe("postgresql://host/mydb");
  });

  test("target is null when not provided", () => {
    const plan = makePlan("myproject", []);
    const result = computeStatus(plan, [], null, deployDir);
    expect(result.target).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Tests: formatStatusText
// ---------------------------------------------------------------------------

describe("formatStatusText", () => {
  test("shows project name", () => {
    const result: StatusResult = {
      project: "myproject",
      target: null,
      deployed_count: 0,
      pending_count: 0,
      pending_changes: [],
      last_deployed: null,
      modified_scripts: [],
      expand_contract_operations: [],
    };
    const text = formatStatusText(result);
    expect(text).toContain("# Project: myproject");
  });

  test("shows target when present", () => {
    const result: StatusResult = {
      project: "myproject",
      target: "postgresql://host/db",
      deployed_count: 0,
      pending_count: 0,
      pending_changes: [],
      last_deployed: null,
      modified_scripts: [],
      expand_contract_operations: [],
    };
    const text = formatStatusText(result);
    expect(text).toContain("# Target:  postgresql://host/db");
  });

  test("omits target line when null", () => {
    const result: StatusResult = {
      project: "myproject",
      target: null,
      deployed_count: 0,
      pending_count: 0,
      pending_changes: [],
      last_deployed: null,
      modified_scripts: [],
      expand_contract_operations: [],
    };
    const text = formatStatusText(result);
    expect(text).not.toContain("Target:");
  });

  test("shows deployed and pending counts", () => {
    const result: StatusResult = {
      project: "myproject",
      target: null,
      deployed_count: 5,
      pending_count: 3,
      pending_changes: ["a", "b", "c"],
      last_deployed: null,
      modified_scripts: [],
      expand_contract_operations: [],
    };
    const text = formatStatusText(result);
    expect(text).toContain("Deployed: 5");
    expect(text).toContain("Pending:  3");
  });

  test("lists pending changes with bullet marker", () => {
    const result: StatusResult = {
      project: "myproject",
      target: null,
      deployed_count: 1,
      pending_count: 2,
      pending_changes: ["add_users", "add_orders"],
      last_deployed: null,
      modified_scripts: [],
      expand_contract_operations: [],
    };
    const text = formatStatusText(result);
    expect(text).toContain("  * add_users");
    expect(text).toContain("  * add_orders");
  });

  test("shows last deployed change info", () => {
    const result: StatusResult = {
      project: "myproject",
      target: null,
      deployed_count: 2,
      pending_count: 0,
      pending_changes: [],
      last_deployed: {
        change: "create_tables",
        change_id: "abc123",
        committed_at: "2024-01-15T10:30:00.000Z",
        committer_name: "Alice",
      },
      modified_scripts: [],
      expand_contract_operations: [],
    };
    const text = formatStatusText(result);
    expect(text).toContain("# Last deployed change:");
    expect(text).toContain("#   create_tables");
    expect(text).toContain("#   deployed at: 2024-01-15T10:30:00.000Z");
    expect(text).toContain("#   by: Alice");
  });

  test("shows modified scripts with warning marker", () => {
    const result: StatusResult = {
      project: "myproject",
      target: null,
      deployed_count: 2,
      pending_count: 0,
      pending_changes: [],
      last_deployed: null,
      modified_scripts: [
        {
          change: "add_users",
          change_id: "aaa",
          registry_hash: "old_hash",
          current_hash: "new_hash",
        },
      ],
      expand_contract_operations: [],
    };
    const text = formatStatusText(result);
    expect(text).toContain("Modified scripts (hash mismatch):");
    expect(text).toContain("  ! add_users");
  });

  test("shows up-to-date message when nothing pending and no modifications", () => {
    const result: StatusResult = {
      project: "myproject",
      target: null,
      deployed_count: 3,
      pending_count: 0,
      pending_changes: [],
      last_deployed: null,
      modified_scripts: [],
      expand_contract_operations: [],
    };
    const text = formatStatusText(result);
    expect(text).toContain("Nothing to deploy. Everything is up-to-date.");
  });

  test("does not show up-to-date message when changes are pending", () => {
    const result: StatusResult = {
      project: "myproject",
      target: null,
      deployed_count: 1,
      pending_count: 1,
      pending_changes: ["change_b"],
      last_deployed: null,
      modified_scripts: [],
      expand_contract_operations: [],
    };
    const text = formatStatusText(result);
    expect(text).not.toContain("Nothing to deploy");
  });

  test("does not show up-to-date message when scripts are modified", () => {
    const result: StatusResult = {
      project: "myproject",
      target: null,
      deployed_count: 1,
      pending_count: 0,
      pending_changes: [],
      last_deployed: null,
      modified_scripts: [
        {
          change: "x",
          change_id: "id",
          registry_hash: "a",
          current_hash: "b",
        },
      ],
      expand_contract_operations: [],
    };
    const text = formatStatusText(result);
    expect(text).not.toContain("Nothing to deploy");
  });
});

// ---------------------------------------------------------------------------
// Tests: CLI integration (subprocess)
// ---------------------------------------------------------------------------

describe("status CLI integration", () => {
  const CWD = import.meta.dir + "/../..";
  let tempDir: string;

  beforeEach(async () => {
    resetConfig();
    tempDir = await mkdtemp(join(tmpdir(), "sqlever-status-cli-"));
    await mkdir(join(tempDir, "deploy"), { recursive: true });
  });

  afterEach(async () => {
    await rm(tempDir, { recursive: true, force: true });
  });

  async function run(
    ...args: string[]
  ): Promise<{ stdout: string; stderr: string; exitCode: number }> {
    const proc = Bun.spawn(
      ["bun", "run", "src/cli.ts", ...args],
      {
        cwd: CWD,
        stdout: "pipe",
        stderr: "pipe",
      },
    );
    const [stdout, stderr, exitCode] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
      proc.exited,
    ]);
    return { stdout, stderr, exitCode };
  }

  test("exits with error when no plan file exists", async () => {
    const { stderr, exitCode } = await run(
      "status",
      "--top-dir",
      tempDir,
    );
    expect(exitCode).not.toBe(0);
    expect(stderr).toContain("Plan file not found");
  });

  test("shows plan-only status (no DB) when plan exists but no target", async () => {
    // Write a sqitch.conf (no target configured)
    await writeFile(
      join(tempDir, "sqitch.conf"),
      "[core]\n\tengine = pg\n",
    );
    // Write a plan file with two changes
    await writeFile(
      join(tempDir, "sqitch.plan"),
      `%syntax-version=1.0.0
%project=testproj

init_schema 2024-01-01T00:00:00Z Dev <dev@test.com> # first
add_users 2024-01-02T00:00:00Z Dev <dev@test.com> # second
`,
    );

    const { stdout, exitCode } = await run(
      "status",
      "--top-dir",
      tempDir,
    );
    expect(exitCode).toBe(0);
    expect(stdout).toContain("# Project: testproj");
    expect(stdout).toContain("Deployed: 0");
    expect(stdout).toContain("Pending:  2");
    expect(stdout).toContain("  * init_schema");
    expect(stdout).toContain("  * add_users");
  });

  test("--format json outputs valid JSON", async () => {
    await writeFile(
      join(tempDir, "sqitch.conf"),
      "[core]\n\tengine = pg\n",
    );
    await writeFile(
      join(tempDir, "sqitch.plan"),
      `%syntax-version=1.0.0
%project=jsontest

change_one 2024-01-01T00:00:00Z Dev <dev@test.com> # note
`,
    );

    const { stdout, exitCode } = await run(
      "status",
      "--top-dir",
      tempDir,
      "--format",
      "json",
    );
    expect(exitCode).toBe(0);

    const data = JSON.parse(stdout);
    expect(data.project).toBe("jsontest");
    expect(data.deployed_count).toBe(0);
    expect(data.pending_count).toBe(1);
    expect(data.pending_changes).toEqual(["change_one"]);
    expect(data.target).toBeNull();
    expect(data.last_deployed).toBeNull();
    expect(data.modified_scripts).toEqual([]);
  });
});
