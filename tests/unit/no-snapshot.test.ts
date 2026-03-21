// tests/unit/no-snapshot.test.ts — Tests for --no-snapshot flag
//
// Verifies that:
//  - The --no-snapshot flag is correctly parsed by parseDeployOptions
//  - Default behavior (no flag) resolves includes from git history
//  - --no-snapshot resolves includes from HEAD/current files
//  - The flag is wired through the deploy flow correctly
//
// Issue #83: C2 — --no-snapshot flag + snapshot include tests

import { describe, it, expect, beforeEach, afterEach, mock } from "bun:test";
import { EventEmitter } from "events";
import { mkdirSync, writeFileSync, rmSync, mkdtempSync } from "fs";
import { execSync } from "child_process";
import { join } from "path";
import { tmpdir } from "os";
import { resetConfig, setConfig } from "../../src/output";

// ---------------------------------------------------------------------------
// Mock pg/lib/client — same approach as deploy-failures.test.ts
// ---------------------------------------------------------------------------

let mockInstances: MockPgClient[] = [];

class MockPgClient {
  options: Record<string, unknown>;
  queries: Array<{ text: string; values?: unknown[] }> = [];
  connected = false;
  ended = false;

  constructor(options: Record<string, unknown>) {
    this.options = options;
    mockInstances.push(this);
  }

  async connect() {
    this.connected = true;
  }

  async query(text: string, values?: unknown[]) {
    this.queries.push({ text, values });
    if (text.includes("pg_try_advisory_lock")) {
      return { rows: [{ pg_try_advisory_lock: true }], rowCount: 1, command: "SELECT" };
    }
    if (text.includes("pg_advisory_unlock")) {
      return { rows: [{ pg_advisory_unlock: true }], rowCount: 1, command: "SELECT" };
    }
    if (text.includes("SELECT") && text.includes("sqitch.projects") && !text.includes("INSERT")) {
      return { rows: [], rowCount: 0, command: "SELECT" };
    }
    if (text.includes("INSERT INTO sqitch.projects")) {
      return {
        rows: [{ project: "test", uri: null, created_at: new Date(), creator_name: "Test", creator_email: "test@x.com" }],
        rowCount: 1,
        command: "INSERT",
      };
    }
    if (text.includes("SELECT") && text.includes("sqitch.changes")) {
      return { rows: [], rowCount: 0, command: "SELECT" };
    }
    return { rows: [], rowCount: 0, command: "SELECT" };
  }

  async end() {
    this.ended = true;
    this.connected = false;
  }
}

mock.module("pg/lib/client", () => ({
  default: MockPgClient,
  __esModule: true,
}));

// Type imports
import type { DeployOptions, DeployDeps } from "../../src/commands/deploy";
import type { SpawnFn } from "../../src/psql";

// Import after mocking
const { DatabaseClient } = await import("../../src/db/client");
const { Registry } = await import("../../src/db/registry");
const {
  executeDeploy,
  parseDeployOptions,
} = await import("../../src/commands/deploy");
const { loadConfig } = await import("../../src/config/index");
const { PsqlRunner } = await import("../../src/psql");
const { ShutdownManager } = await import("../../src/signals");

// Snapshot imports for direct testing
const {
  resolveDeployIncludes,
} = await import("../../src/includes/snapshot");

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let testDir: string;
let testDirCounter = 0;

function createTestDir(): string {
  testDirCounter++;
  const dir = join(tmpdir(), `sqlever-nosnapshot-test-${Date.now()}-${testDirCounter}`);
  mkdirSync(dir, { recursive: true });
  mkdirSync(join(dir, "deploy"), { recursive: true });
  mkdirSync(join(dir, "revert"), { recursive: true });
  mkdirSync(join(dir, "verify"), { recursive: true });
  return dir;
}

function makeTempDir(): string {
  return mkdtempSync(join(tmpdir(), "sqlever-test-"));
}

function initGitRepo(): string {
  const dir = makeTempDir();
  execSync("git init", { cwd: dir, stdio: "ignore" });
  execSync('git config user.email "test@sqlever.dev"', { cwd: dir, stdio: "ignore" });
  execSync('git config user.name "Test User"', { cwd: dir, stdio: "ignore" });
  writeFileSync(join(dir, ".gitkeep"), "");
  execSync("git add .gitkeep", { cwd: dir, stdio: "ignore" });
  execSync('git commit -m "initial"', { cwd: dir, stdio: "ignore" });
  return dir;
}

function commitFile(repoRoot: string, filePath: string, content: string, message?: string): string {
  const absolutePath = join(repoRoot, filePath);
  const dir = absolutePath.substring(0, absolutePath.lastIndexOf("/"));
  mkdirSync(dir, { recursive: true });
  writeFileSync(absolutePath, content);
  execSync(`git add "${filePath}"`, { cwd: repoRoot, stdio: "ignore" });
  execSync(`git commit -m "${message ?? `add ${filePath}`}"`, { cwd: repoRoot, stdio: "ignore" });
  return execSync("git rev-parse HEAD", { cwd: repoRoot }).toString().trim();
}

function cleanupDir(dir: string): void {
  try {
    rmSync(dir, { recursive: true, force: true });
  } catch {
    // Best effort
  }
}

function writePlan(dir: string, content: string): void {
  writeFileSync(join(dir, "sqitch.plan"), content, "utf-8");
}

function writeDeployScript(dir: string, name: string, content: string): void {
  writeFileSync(join(dir, "deploy", `${name}.sql`), content, "utf-8");
}

function writeSqitchConf(dir: string, content: string): void {
  writeFileSync(join(dir, "sqitch.conf"), content, "utf-8");
}

function makeArgs(rest: string[], overrides?: Partial<Record<string, unknown>>) {
  return {
    command: "deploy",
    rest,
    help: false,
    version: false,
    format: "text" as const,
    quiet: false,
    verbose: false,
    dbUri: "postgresql://localhost/testdb",
    planFile: undefined,
    topDir: testDir,
    registry: undefined,
    target: undefined,
    ...overrides,
  };
}

const SINGLE_CHANGE_PLAN = `%syntax-version=1.0.0
%project=myproject

create_schema 2025-01-01T00:00:00Z Test User <test@example.com> # Create schema
`;

const TWO_CHANGE_PLAN = `%syntax-version=1.0.0
%project=myproject

create_schema 2025-01-01T00:00:00Z Test User <test@example.com> # Create schema
add_users [create_schema] 2025-01-02T00:00:00Z Test User <test@example.com> # Add users table
`;

/**
 * Create a tracking PsqlRunner that records calls and their arguments/content.
 */
function createTrackingPsqlRunner(failOnScripts: string[] = []): {
  runner: PsqlRunner;
  calls: Array<{ scriptFile: string; args: string[]; content?: string }>;
} {
  const calls: Array<{ scriptFile: string; args: string[]; content?: string }> = [];
  const mockSpawn: SpawnFn = (_cmd, args, _opts) => {
    const scriptFile = args.find((a: string) => a.endsWith(".sql")) ?? "";
    calls.push({ scriptFile, args: [...args] });
    const child = Object.assign(new EventEmitter(), {
      stdout: new EventEmitter(),
      stderr: new EventEmitter(),
      stdin: {
        write(data: string) {
          // Record content passed via stdin
          const lastCall = calls[calls.length - 1];
          if (lastCall) lastCall.content = data;
        },
        end() {},
      },
    });
    const shouldFail = failOnScripts.some((s) => scriptFile.includes(s));
    queueMicrotask(() => {
      if (shouldFail) {
        child.stderr.emit("data", Buffer.from(`psql:${scriptFile}:1: ERROR: simulated failure`));
      }
      child.emit("close", shouldFail ? 1 : 0);
    });
    return child as ReturnType<typeof import("child_process").spawn>;
  };
  return { runner: new PsqlRunner("psql", mockSpawn), calls };
}

function defaultOptions(dir: string): DeployOptions {
  return {
    mode: "change",
    dryRun: false,
    verify: false,
    variables: {},
    dbUri: "postgresql://localhost/testdb",
    projectDir: dir,
    committerName: "Test User",
    committerEmail: "test@example.com",
    noTui: true,
    noSnapshot: false,
  };
}

async function createDeps(): Promise<DeployDeps> {
  const db = new DatabaseClient("postgresql://localhost/testdb");
  const registry = new Registry(db);
  const psqlRunner = new PsqlRunner("psql");
  const config = loadConfig(testDir);
  const shutdownMgr = new ShutdownManager();
  return { db, registry, psqlRunner, config, shutdownMgr };
}

// ---------------------------------------------------------------------------
// Tests: parseDeployOptions — --no-snapshot flag parsing
// ---------------------------------------------------------------------------

describe("parseDeployOptions: --no-snapshot flag", () => {
  beforeEach(() => {
    mockInstances = [];
    resetConfig();
    setConfig({ quiet: true });
    testDir = createTestDir();
    writeSqitchConf(testDir, `[core]\n    engine = pg\n`);
  });

  afterEach(() => {
    cleanupDir(testDir);
  });

  it("parses --no-snapshot as noSnapshot=true", () => {
    const options = parseDeployOptions(makeArgs(["--no-snapshot"]));
    expect(options.noSnapshot).toBe(true);
  });

  it("defaults noSnapshot to false when --no-snapshot is not provided", () => {
    const options = parseDeployOptions(makeArgs([]));
    expect(options.noSnapshot).toBe(false);
  });

  it("parses --no-snapshot alongside other flags", () => {
    const options = parseDeployOptions(makeArgs(["--no-snapshot", "--dry-run", "--verify"]));
    expect(options.noSnapshot).toBe(true);
    expect(options.dryRun).toBe(true);
    expect(options.verify).toBe(true);
  });

  it("parses --no-snapshot when placed after --to", () => {
    const options = parseDeployOptions(makeArgs(["--to", "create_schema", "--no-snapshot"]));
    expect(options.noSnapshot).toBe(true);
    expect(options.to).toBe("create_schema");
  });

  it("parses --no-snapshot when placed before --to", () => {
    const options = parseDeployOptions(makeArgs(["--no-snapshot", "--to", "add_users"]));
    expect(options.noSnapshot).toBe(true);
    expect(options.to).toBe("add_users");
  });
});

// ---------------------------------------------------------------------------
// Tests: resolveDeployIncludes — noSnapshot parameter
// ---------------------------------------------------------------------------

describe("resolveDeployIncludes: noSnapshot behavior", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("resolves includes from historical commit when noSnapshot is false", () => {
    // Commit v1 of shared file
    const hash1 = commitFile(
      repoRoot,
      "shared/funcs.sql",
      "CREATE FUNCTION greet_v1() RETURNS void AS $$ $$ LANGUAGE sql;\n",
    );
    // Update to v2
    commitFile(
      repoRoot,
      "shared/funcs.sql",
      "CREATE FUNCTION greet_v2() RETURNS void AS $$ $$ LANGUAGE sql;\n",
    );
    // Create deploy script that includes the shared file
    commitFile(
      repoRoot,
      "deploy/001.sql",
      "\\i shared/funcs.sql\nSELECT 1;\n",
    );

    const result = resolveDeployIncludes(
      join(repoRoot, "deploy/001.sql"),
      "2025-01-01T00:00:00Z",
      repoRoot,
      hash1,
      false, // noSnapshot = false
    );

    expect(result).toBeDefined();
    expect(result!.content).toContain("greet_v1");
    expect(result!.content).not.toContain("greet_v2");
  });

  it("resolves includes from HEAD when noSnapshot is true", () => {
    // Commit v1 of shared file
    commitFile(
      repoRoot,
      "shared/funcs.sql",
      "CREATE FUNCTION greet_v1() RETURNS void AS $$ $$ LANGUAGE sql;\n",
    );
    // Update to v2
    commitFile(
      repoRoot,
      "shared/funcs.sql",
      "CREATE FUNCTION greet_v2() RETURNS void AS $$ $$ LANGUAGE sql;\n",
    );
    // Create deploy script
    commitFile(
      repoRoot,
      "deploy/001.sql",
      "\\i shared/funcs.sql\nSELECT 1;\n",
    );

    const result = resolveDeployIncludes(
      join(repoRoot, "deploy/001.sql"),
      "2025-01-01T00:00:00Z",
      repoRoot,
      undefined,
      true, // noSnapshot = true
    );

    expect(result).toBeDefined();
    expect(result!.content).toContain("greet_v2");
    expect(result!.content).not.toContain("greet_v1");
  });

  it("noSnapshot ignores explicit commitHash and uses HEAD instead", () => {
    // Commit v1
    const hash1 = commitFile(
      repoRoot,
      "shared/funcs.sql",
      "-- old version\n",
    );
    // Commit v2
    commitFile(
      repoRoot,
      "shared/funcs.sql",
      "-- current version\n",
    );
    commitFile(
      repoRoot,
      "deploy/001.sql",
      "\\i shared/funcs.sql\n",
    );

    // Pass hash1 explicitly, but noSnapshot=true should override
    const result = resolveDeployIncludes(
      join(repoRoot, "deploy/001.sql"),
      "2025-01-01T00:00:00Z",
      repoRoot,
      hash1,
      true,
    );

    expect(result).toBeDefined();
    expect(result!.content).toContain("current version");
    expect(result!.content).not.toContain("old version");
  });

  it("returns undefined for scripts with no includes regardless of noSnapshot", () => {
    commitFile(
      repoRoot,
      "deploy/simple.sql",
      "CREATE TABLE users (id int);\n",
    );

    const resultWithSnapshot = resolveDeployIncludes(
      join(repoRoot, "deploy/simple.sql"),
      "2025-01-01T00:00:00Z",
      repoRoot,
      undefined,
      false,
    );
    expect(resultWithSnapshot).toBeUndefined();

    const resultNoSnapshot = resolveDeployIncludes(
      join(repoRoot, "deploy/simple.sql"),
      "2025-01-01T00:00:00Z",
      repoRoot,
      undefined,
      true,
    );
    expect(resultNoSnapshot).toBeUndefined();
  });

  it("snapshot resolution uses planned_at timestamp to find correct commit", () => {
    // Commit v1 of shared file (this is the only version at planned_at)
    commitFile(
      repoRoot,
      "shared/funcs.sql",
      "-- planned version\n",
    );
    commitFile(
      repoRoot,
      "deploy/001.sql",
      "\\i shared/funcs.sql\n",
    );

    // Use far-future planned_at to match the latest commit
    const result = resolveDeployIncludes(
      join(repoRoot, "deploy/001.sql"),
      "2099-12-31T23:59:59Z",
      repoRoot,
      undefined,
      false, // snapshot mode
    );

    expect(result).toBeDefined();
    expect(result!.content).toContain("planned version");
  });
});

// ---------------------------------------------------------------------------
// Tests: Customer-zero scenario — migration with \i, modify included file
// ---------------------------------------------------------------------------

describe("customer-zero scenario: migration with \\i and modified included file", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("snapshot resolves historical version, --no-snapshot uses current", () => {
    // Step 1: Create shared helper v1
    const hash1 = commitFile(
      repoRoot,
      "shared/helpers.sql",
      "CREATE FUNCTION helper_v1() RETURNS text AS $$ SELECT 'v1' $$ LANGUAGE sql;\n",
    );

    // Step 2: Create migration that includes the helper
    commitFile(
      repoRoot,
      "deploy/001-init.sql",
      "-- Migration 001\n\\i shared/helpers.sql\nCREATE TABLE main_table (id int);\n",
    );

    // Step 3: Later, modify the helper (simulating ongoing development)
    commitFile(
      repoRoot,
      "shared/helpers.sql",
      "CREATE FUNCTION helper_v2() RETURNS text AS $$ SELECT 'v2' $$ LANGUAGE sql;\n",
    );

    // With snapshot (default): should get v1 (the version at hash1)
    const snapshotResult = resolveDeployIncludes(
      join(repoRoot, "deploy/001-init.sql"),
      "2025-01-01T00:00:00Z",
      repoRoot,
      hash1,
      false,
    );
    expect(snapshotResult).toBeDefined();
    expect(snapshotResult!.content).toContain("helper_v1");
    expect(snapshotResult!.content).not.toContain("helper_v2");

    // With --no-snapshot: should get v2 (current HEAD)
    const noSnapshotResult = resolveDeployIncludes(
      join(repoRoot, "deploy/001-init.sql"),
      "2025-01-01T00:00:00Z",
      repoRoot,
      hash1, // even with explicit hash, noSnapshot overrides
      true,
    );
    expect(noSnapshotResult).toBeDefined();
    expect(noSnapshotResult!.content).toContain("helper_v2");
    expect(noSnapshotResult!.content).not.toContain("helper_v1");
  });

  it("nested includes are also resolved from the correct version", () => {
    // v1: types, functions, and deploy script all committed together
    commitFile(
      repoRoot,
      "shared/types.sql",
      "CREATE TYPE status_v1 AS ENUM ('active');\n",
    );
    commitFile(
      repoRoot,
      "shared/funcs.sql",
      "\\i shared/types.sql\nCREATE FUNCTION get_status_v1() RETURNS status_v1 AS $$ SELECT 'active'::status_v1 $$ LANGUAGE sql;\n",
    );
    // Capture the commit where everything is at v1
    const hashV1 = commitFile(
      repoRoot,
      "deploy/001.sql",
      "\\i shared/funcs.sql\n",
    );

    // v2: updated types and functions
    commitFile(
      repoRoot,
      "shared/types.sql",
      "CREATE TYPE status_v2 AS ENUM ('active', 'archived');\n",
    );
    commitFile(
      repoRoot,
      "shared/funcs.sql",
      "\\i shared/types.sql\nCREATE FUNCTION get_status_v2() RETURNS status_v2 AS $$ SELECT 'active'::status_v2 $$ LANGUAGE sql;\n",
    );

    // Snapshot at hashV1: should get v1 of both types and funcs
    const snapshotResult = resolveDeployIncludes(
      join(repoRoot, "deploy/001.sql"),
      "2025-01-01T00:00:00Z",
      repoRoot,
      hashV1,
      false,
    );
    expect(snapshotResult).toBeDefined();
    expect(snapshotResult!.content).toContain("status_v1");
    expect(snapshotResult!.content).toContain("get_status_v1");
    expect(snapshotResult!.content).not.toContain("status_v2");
    expect(snapshotResult!.content).not.toContain("get_status_v2");

    // --no-snapshot: should get v2
    const noSnapshotResult = resolveDeployIncludes(
      join(repoRoot, "deploy/001.sql"),
      "2025-01-01T00:00:00Z",
      repoRoot,
      undefined,
      true,
    );
    expect(noSnapshotResult).toBeDefined();
    expect(noSnapshotResult!.content).toContain("status_v2");
    expect(noSnapshotResult!.content).toContain("get_status_v2");
  });
});

// ---------------------------------------------------------------------------
// Tests: Deploy flow integration with --no-snapshot
// ---------------------------------------------------------------------------

describe("deploy flow integration with --no-snapshot", () => {
  beforeEach(() => {
    mockInstances = [];
    resetConfig();
    setConfig({ quiet: true });
    testDir = createTestDir();
    writeSqitchConf(testDir, `[core]\n    engine = pg\n`);
  });

  afterEach(() => {
    cleanupDir(testDir);
  });

  it("with --no-snapshot, passes original script file to psql (no includes)", async () => {
    writePlan(testDir, SINGLE_CHANGE_PLAN);
    writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;\n");

    const { runner, calls } = createTrackingPsqlRunner();
    const deps = await createDeps();
    deps.psqlRunner = runner;

    const options = defaultOptions(testDir);
    options.noSnapshot = true;

    const result = await executeDeploy(options, deps);
    expect(result.deployed).toBe(1);

    // Script with no includes: psql should receive the original script file
    expect(calls.length).toBe(1);
    expect(calls[0]!.scriptFile).toContain("create_schema.sql");
    expect(calls[0]!.content).toBeUndefined();
  });

  it("without --no-snapshot, passes original script file to psql when no includes", async () => {
    writePlan(testDir, SINGLE_CHANGE_PLAN);
    writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;\n");

    const { runner, calls } = createTrackingPsqlRunner();
    const deps = await createDeps();
    deps.psqlRunner = runner;

    const options = defaultOptions(testDir);
    options.noSnapshot = false;

    const result = await executeDeploy(options, deps);
    expect(result.deployed).toBe(1);

    // No includes: should still pass file path regardless of snapshot setting
    expect(calls.length).toBe(1);
    expect(calls[0]!.scriptFile).toContain("create_schema.sql");
    expect(calls[0]!.content).toBeUndefined();
  });

  it("with --no-snapshot and includes, passes original script file to psql", async () => {
    writePlan(testDir, SINGLE_CHANGE_PLAN);
    // Write a deploy script with an include (the included file doesn't need to
    // exist in git since --no-snapshot means psql handles \i natively)
    mkdirSync(join(testDir, "shared"), { recursive: true });
    writeFileSync(join(testDir, "shared", "helpers.sql"), "SELECT 1;\n");
    writeDeployScript(testDir, "create_schema", "\\i shared/helpers.sql\nCREATE SCHEMA myapp;\n");

    const { runner, calls } = createTrackingPsqlRunner();
    const deps = await createDeps();
    deps.psqlRunner = runner;

    const options = defaultOptions(testDir);
    options.noSnapshot = true;

    const result = await executeDeploy(options, deps);
    expect(result.deployed).toBe(1);

    // With --no-snapshot: psql receives the original file, handles \i itself
    expect(calls.length).toBe(1);
    expect(calls[0]!.scriptFile).toContain("create_schema.sql");
    expect(calls[0]!.content).toBeUndefined();
  });

  it("noSnapshot defaults to false in DeployOptions from parseDeployOptions", () => {
    const options = parseDeployOptions(makeArgs([]));
    expect(options.noSnapshot).toBe(false);
  });
});
