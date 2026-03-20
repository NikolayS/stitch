import { describe, it, expect, beforeEach, mock, afterEach } from "bun:test";
import { EventEmitter } from "events";
import { mkdirSync, writeFileSync, rmSync, existsSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";
import { resetConfig, setConfig } from "../../src/output";

// ---------------------------------------------------------------------------
// Mock pg/lib/client — same approach as registry.test.ts
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
    // Default: advisory lock returns true
    if (text.includes("pg_try_advisory_lock")) {
      return { rows: [{ pg_try_advisory_lock: true }], rowCount: 1, command: "SELECT" };
    }
    if (text.includes("pg_advisory_unlock")) {
      return { rows: [{ pg_advisory_unlock: true }], rowCount: 1, command: "SELECT" };
    }
    // SELECT from sqitch.projects — not found (triggers INSERT)
    if (text.includes("SELECT") && text.includes("sqitch.projects") && !text.includes("INSERT")) {
      return { rows: [], rowCount: 0, command: "SELECT" };
    }
    // INSERT INTO sqitch.projects
    if (text.includes("INSERT INTO sqitch.projects")) {
      return {
        rows: [{ project: "test", uri: null, created_at: new Date(), creator_name: "Test", creator_email: "test@x.com" }],
        rowCount: 1,
        command: "INSERT",
      };
    }
    // SELECT deployed changes — return empty by default (nothing deployed)
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

// Type imports (these work statically)
import type { DeployOptions, DeployDeps } from "../../src/commands/deploy";
import type { SpawnFn, PsqlRunResult } from "../../src/psql";

// Import after mocking
const { DatabaseClient } = await import("../../src/db/client");
const { Registry } = await import("../../src/db/registry");
const {
  executeDeploy,
  runDeploy,
  projectLockKey,
  isNonTransactional,
  parseDeployOptions,
  ADVISORY_LOCK_NAMESPACE,
  EXIT_CONCURRENT_DEPLOY,
  EXIT_DEPLOY_FAILED,
  EXIT_LOCK_TIMEOUT,
  EXIT_DB_UNREACHABLE,
} = await import("../../src/commands/deploy");
const { loadConfig } = await import("../../src/config/index");
const { PsqlRunner } = await import("../../src/psql");
const { ShutdownManager } = await import("../../src/signals");

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let testDir: string;
let testDirCounter = 0;

function createTestDir(): string {
  testDirCounter++;
  const dir = join(tmpdir(), `sqlever-deploy-test-${Date.now()}-${testDirCounter}`);
  mkdirSync(dir, { recursive: true });
  mkdirSync(join(dir, "deploy"), { recursive: true });
  mkdirSync(join(dir, "revert"), { recursive: true });
  mkdirSync(join(dir, "verify"), { recursive: true });
  return dir;
}

function writePlan(dir: string, content: string): void {
  writeFileSync(join(dir, "sqitch.plan"), content, "utf-8");
}

function writeDeployScript(dir: string, name: string, content: string): void {
  writeFileSync(join(dir, "deploy", `${name}.sql`), content, "utf-8");
}

function writeVerifyScript(dir: string, name: string, content: string): void {
  writeFileSync(join(dir, "verify", `${name}.sql`), content, "utf-8");
}

function writeSqitchConf(dir: string, content: string): void {
  writeFileSync(join(dir, "sqitch.conf"), content, "utf-8");
}

/** A simple plan with two changes */
const SIMPLE_PLAN = `%syntax-version=1.0.0
%project=myproject

create_schema 2025-01-01T00:00:00Z Test User <test@example.com> # Create schema
add_users [create_schema] 2025-01-02T00:00:00Z Test User <test@example.com> # Add users table
`;

/** A plan with a tag */
const TAGGED_PLAN = `%syntax-version=1.0.0
%project=myproject

create_schema 2025-01-01T00:00:00Z Test User <test@example.com> # Create schema
add_users [create_schema] 2025-01-02T00:00:00Z Test User <test@example.com> # Add users table
@v1.0 2025-01-03T00:00:00Z Test User <test@example.com> # Release v1.0
add_posts [add_users] 2025-01-04T00:00:00Z Test User <test@example.com> # Add posts table
`;

/**
 * Create a mock PsqlRunner that succeeds (exit code 0).
 */
function createMockPsqlRunner(exitCode = 0, stderr = ""): PsqlRunner {
  const mockSpawn: SpawnFn = (_cmd, _args, _opts) => {
    const child = Object.assign(new EventEmitter(), {
      stdout: new EventEmitter(),
      stderr: new EventEmitter(),
    });
    queueMicrotask(() => {
      if (stderr) child.stderr.emit("data", Buffer.from(stderr));
      child.emit("close", exitCode);
    });
    return child as ReturnType<typeof import("child_process").spawn>;
  };
  return new PsqlRunner("psql", mockSpawn);
}

/**
 * Create a mock PsqlRunner that fails on a specific script.
 */
function createFailingPsqlRunner(failOnScript: string, errorMsg = "ERROR: relation does not exist"): PsqlRunner {
  const mockSpawn: SpawnFn = (_cmd, args, _opts) => {
    const child = Object.assign(new EventEmitter(), {
      stdout: new EventEmitter(),
      stderr: new EventEmitter(),
    });
    const scriptFile = args.find((a: string) => a.endsWith(".sql")) ?? "";
    const shouldFail = scriptFile.includes(failOnScript);
    queueMicrotask(() => {
      if (shouldFail) {
        child.stderr.emit("data", Buffer.from(`psql:${scriptFile}:1: ${errorMsg}`));
      }
      child.emit("close", shouldFail ? 1 : 0);
    });
    return child as ReturnType<typeof import("child_process").spawn>;
  };
  return new PsqlRunner("psql", mockSpawn);
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
  };
}

async function createDeps(opts?: Partial<{ psqlExitCode: number; psqlStderr: string; failOnScript: string }>): Promise<DeployDeps> {
  const db = new DatabaseClient("postgresql://localhost/testdb");
  const registry = new Registry(db);
  let psqlRunner: PsqlRunner;
  if (opts?.failOnScript) {
    psqlRunner = createFailingPsqlRunner(opts.failOnScript);
  } else {
    psqlRunner = createMockPsqlRunner(opts?.psqlExitCode ?? 0, opts?.psqlStderr ?? "");
  }
  const config = loadConfig(testDir);
  const shutdownMgr = new ShutdownManager();

  return { db, registry, psqlRunner, config, shutdownMgr };
}

function getPgClient(): MockPgClient {
  return mockInstances[mockInstances.length - 1]!;
}

function queryTexts(pgClient: MockPgClient): string[] {
  return pgClient.queries.map((q) => q.text);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("deploy", () => {
  beforeEach(() => {
    mockInstances = [];
    resetConfig();
    setConfig({ quiet: true }); // suppress output during tests
    testDir = createTestDir();
    writeSqitchConf(testDir, `[core]\n    engine = pg\n`);
  });

  afterEach(() => {
    try {
      if (testDir && existsSync(testDir)) {
        rmSync(testDir, { recursive: true, force: true });
      }
    } catch {
      // ignore cleanup errors
    }
  });

  // -----------------------------------------------------------------------
  // projectLockKey
  // -----------------------------------------------------------------------

  describe("projectLockKey()", () => {
    it("returns a positive 32-bit integer", () => {
      const key = projectLockKey("myproject");
      expect(key).toBeGreaterThan(0);
      expect(key).toBeLessThanOrEqual(0x7FFFFFFF);
    });

    it("produces different keys for different projects", () => {
      const k1 = projectLockKey("project_a");
      const k2 = projectLockKey("project_b");
      expect(k1).not.toBe(k2);
    });

    it("produces the same key for the same project name", () => {
      const k1 = projectLockKey("myproject");
      const k2 = projectLockKey("myproject");
      expect(k1).toBe(k2);
    });

    it("handles empty string", () => {
      const key = projectLockKey("");
      expect(key).toBeGreaterThanOrEqual(0);
    });
  });

  // -----------------------------------------------------------------------
  // isNonTransactional
  // -----------------------------------------------------------------------

  describe("isNonTransactional()", () => {
    it("returns true for scripts with no-transaction comment", () => {
      expect(isNonTransactional("-- sqlever:no-transaction\nCREATE INDEX CONCURRENTLY ...")).toBe(true);
    });

    it("returns true with varied spacing", () => {
      expect(isNonTransactional("--  sqlever:no-transaction\nSELECT 1")).toBe(true);
    });

    it("returns true case-insensitively", () => {
      expect(isNonTransactional("-- SQLEVER:NO-TRANSACTION\nSELECT 1")).toBe(true);
    });

    it("returns false for normal scripts", () => {
      expect(isNonTransactional("CREATE TABLE foo (id int);\n")).toBe(false);
    });

    it("returns false when comment is not on first line", () => {
      expect(isNonTransactional("CREATE TABLE foo;\n-- sqlever:no-transaction\n")).toBe(false);
    });
  });

  // -----------------------------------------------------------------------
  // ADVISORY_LOCK_NAMESPACE
  // -----------------------------------------------------------------------

  describe("ADVISORY_LOCK_NAMESPACE", () => {
    it("is a positive integer (ASCII sqlv)", () => {
      expect(ADVISORY_LOCK_NAMESPACE).toBe(0x73716C76);
      expect(ADVISORY_LOCK_NAMESPACE).toBeGreaterThan(0);
    });
  });

  // -----------------------------------------------------------------------
  // Exit codes
  // -----------------------------------------------------------------------

  describe("exit codes", () => {
    it("EXIT_DEPLOY_FAILED is 1", () => {
      expect(EXIT_DEPLOY_FAILED).toBe(1);
    });
    it("EXIT_CONCURRENT_DEPLOY is 4", () => {
      expect(EXIT_CONCURRENT_DEPLOY).toBe(4);
    });
    it("EXIT_LOCK_TIMEOUT is 5", () => {
      expect(EXIT_LOCK_TIMEOUT).toBe(5);
    });
    it("EXIT_DB_UNREACHABLE is 10", () => {
      expect(EXIT_DB_UNREACHABLE).toBe(10);
    });
  });

  // -----------------------------------------------------------------------
  // executeDeploy — no plan file
  // -----------------------------------------------------------------------

  describe("executeDeploy() — error cases", () => {
    it("returns error when plan file is missing", async () => {
      // Don't write a plan file
      const deps = await createDeps();
      const options = defaultOptions(testDir);
      const result = await executeDeploy(options, deps);

      expect(result.error).toContain("Plan file not found");
      expect(result.deployed).toBe(0);
    });

    it("returns error when no DB URI is specified", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps();
      const options = defaultOptions(testDir);
      options.dbUri = undefined;

      const result = await executeDeploy(options, deps);
      expect(result.error).toContain("No database URI specified");
    });
  });

  // -----------------------------------------------------------------------
  // executeDeploy — successful deploy
  // -----------------------------------------------------------------------

  describe("executeDeploy() — success", () => {
    it("deploys all pending changes in order", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps();
      const options = defaultOptions(testDir);
      const result = await executeDeploy(options, deps);

      expect(result.error).toBeUndefined();
      expect(result.deployed).toBe(2);
      expect(result.dryRun).toBe(false);
    });

    it("acquires advisory lock before deploying", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps();
      const options = defaultOptions(testDir);
      await executeDeploy(options, deps);

      const pgClient = getPgClient();
      const lockQuery = pgClient.queries.find((q) => q.text.includes("pg_try_advisory_lock"));
      expect(lockQuery).toBeDefined();
      expect(lockQuery!.values).toEqual([ADVISORY_LOCK_NAMESPACE, projectLockKey("myproject")]);
    });

    it("releases advisory lock after successful deploy", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps();
      const options = defaultOptions(testDir);
      await executeDeploy(options, deps);

      const pgClient = getPgClient();
      // Find the deploy advisory unlock (two-argument form), not the registry schema lock
      const unlockQuery = pgClient.queries.find((q) =>
        q.text.includes("pg_advisory_unlock") && q.values?.length === 2
      );
      expect(unlockQuery).toBeDefined();
      expect(unlockQuery!.values).toEqual([ADVISORY_LOCK_NAMESPACE, projectLockKey("myproject")]);
    });

    it("creates registry schema", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps();
      const options = defaultOptions(testDir);
      await executeDeploy(options, deps);

      const pgClient = getPgClient();
      const registryDdl = pgClient.queries.find((q) => q.text.includes("CREATE SCHEMA IF NOT EXISTS sqitch"));
      expect(registryDdl).toBeDefined();
    });

    it("records deploy in tracking tables for each change", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps();
      const options = defaultOptions(testDir);
      await executeDeploy(options, deps);

      const pgClient = getPgClient();
      const changeInserts = pgClient.queries.filter((q) =>
        q.text.includes("INSERT INTO sqitch.changes"),
      );
      expect(changeInserts.length).toBe(2);

      // Verify the change names
      expect(changeInserts[0]!.values![2]).toBe("create_schema");
      expect(changeInserts[1]!.values![2]).toBe("add_users");
    });

    it("records deploy events for each change", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps();
      const options = defaultOptions(testDir);
      await executeDeploy(options, deps);

      const pgClient = getPgClient();
      const eventInserts = pgClient.queries.filter((q) =>
        q.text.includes("INSERT INTO sqitch.events"),
      );
      // 2 deploy events
      expect(eventInserts.length).toBe(2);
      expect(eventInserts[0]!.values![0]).toBe("deploy");
      expect(eventInserts[1]!.values![0]).toBe("deploy");
    });

    it("records dependencies for changes with requires", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps();
      const options = defaultOptions(testDir);
      await executeDeploy(options, deps);

      const pgClient = getPgClient();
      const depInserts = pgClient.queries.filter((q) =>
        q.text.includes("INSERT INTO sqitch.dependencies"),
      );
      // add_users requires create_schema
      expect(depInserts.length).toBe(1);
      expect(depInserts[0]!.values![1]).toBe("require");
      expect(depInserts[0]!.values![2]).toBe("create_schema");
    });
  });

  // -----------------------------------------------------------------------
  // executeDeploy — nothing to deploy
  // -----------------------------------------------------------------------

  describe("executeDeploy() — nothing to deploy", () => {
    it("reports 'up to date' when all changes are deployed", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      // Mock the DB to return all changes as already deployed
      const deps = await createDeps();
      const pgClient = getPgClient();
      const origQuery = pgClient.query.bind(pgClient);
      pgClient.query = async (text: string, values?: unknown[]) => {
        if (text.includes("SELECT") && text.includes("sqitch.changes") && text.includes("ORDER BY committed_at ASC")) {
          // Need to return changes with matching IDs from the plan
          // Parse the plan to get change IDs
          const { parsePlan } = await import("../../src/plan/parser");
          const plan = parsePlan(SIMPLE_PLAN);
          const rows = plan.changes.map((c) => ({
            change_id: c.change_id,
            script_hash: "dummy",
            change: c.name,
            project: "myproject",
            note: c.note,
            committed_at: new Date(),
            committer_name: "Test",
            committer_email: "test@x.com",
            planned_at: new Date(c.planned_at),
            planner_name: c.planner_name,
            planner_email: c.planner_email,
          }));
          return { rows, rowCount: rows.length, command: "SELECT" };
        }
        return origQuery(text, values);
      };

      const options = defaultOptions(testDir);
      const result = await executeDeploy(options, deps);

      expect(result.deployed).toBe(0);
      expect(result.error).toBeUndefined();
    });
  });

  // -----------------------------------------------------------------------
  // executeDeploy — dry-run
  // -----------------------------------------------------------------------

  describe("executeDeploy() — dry-run", () => {
    it("does not execute scripts in dry-run mode", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps();
      const options = defaultOptions(testDir);
      options.dryRun = true;

      const result = await executeDeploy(options, deps);

      expect(result.dryRun).toBe(true);
      expect(result.deployed).toBe(0);

      // Should NOT have any INSERT INTO sqitch.changes
      const pgClient = getPgClient();
      const changeInserts = pgClient.queries.filter((q) =>
        q.text.includes("INSERT INTO sqitch.changes"),
      );
      expect(changeInserts.length).toBe(0);
    });

    it("makes zero DB changes — no lock, no registry, no project record", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps();
      const options = defaultOptions(testDir);
      options.dryRun = true;

      await executeDeploy(options, deps);

      const pgClient = getPgClient();
      // No advisory lock acquired (no DB connection at all)
      const lockQuery = pgClient.queries.find((q) => q.text.includes("pg_try_advisory_lock"));
      expect(lockQuery).toBeUndefined();
      // No registry schema creation
      const registryDdl = pgClient.queries.find((q) => q.text.includes("CREATE SCHEMA IF NOT EXISTS sqitch"));
      expect(registryDdl).toBeUndefined();
      // No project INSERT
      const projectInsert = pgClient.queries.find((q) => q.text.includes("INSERT INTO sqitch.projects"));
      expect(projectInsert).toBeUndefined();
    });
  });

  // -----------------------------------------------------------------------
  // executeDeploy — --to flag
  // -----------------------------------------------------------------------

  describe("executeDeploy() — --to flag", () => {
    it("deploys only up to the specified change", async () => {
      writePlan(testDir, TAGGED_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");
      writeDeployScript(testDir, "add_posts", "CREATE TABLE posts (id int);");

      const deps = await createDeps();
      const options = defaultOptions(testDir);
      options.to = "add_users";

      const result = await executeDeploy(options, deps);

      expect(result.deployed).toBe(2); // create_schema + add_users
      expect(result.error).toBeUndefined();

      // Verify add_posts was NOT deployed
      const pgClient = getPgClient();
      const changeInserts = pgClient.queries.filter((q) =>
        q.text.includes("INSERT INTO sqitch.changes"),
      );
      const deployedNames = changeInserts.map((q) => q.values![2]);
      expect(deployedNames).toContain("create_schema");
      expect(deployedNames).toContain("add_users");
      expect(deployedNames).not.toContain("add_posts");
    });
  });

  // -----------------------------------------------------------------------
  // executeDeploy — advisory lock contention
  // -----------------------------------------------------------------------

  describe("executeDeploy() — concurrent deploy", () => {
    it("returns error when advisory lock cannot be acquired", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps();

      // Override the mock to return false for advisory lock
      const pgClient = getPgClient();
      const origQuery = pgClient.query.bind(pgClient);
      pgClient.query = async (text: string, values?: unknown[]) => {
        if (text.includes("pg_try_advisory_lock")) {
          pgClient.queries.push({ text, values });
          return { rows: [{ pg_try_advisory_lock: false }], rowCount: 1, command: "SELECT" };
        }
        return origQuery(text, values);
      };

      const options = defaultOptions(testDir);
      const result = await executeDeploy(options, deps);

      expect(result.error).toBe("Concurrent deploy detected");
      expect(result.deployed).toBe(0);
    });

    it("does not attempt to release lock if never acquired", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps();

      // Override the mock to return false for advisory lock
      const pgClient = getPgClient();
      const origQuery = pgClient.query.bind(pgClient);
      pgClient.query = async (text: string, values?: unknown[]) => {
        if (text.includes("pg_try_advisory_lock")) {
          pgClient.queries.push({ text, values });
          return { rows: [{ pg_try_advisory_lock: false }], rowCount: 1, command: "SELECT" };
        }
        return origQuery(text, values);
      };

      const options = defaultOptions(testDir);
      await executeDeploy(options, deps);

      const unlockQueries = pgClient.queries.filter((q) => q.text.includes("pg_advisory_unlock"));
      expect(unlockQueries.length).toBe(0);
    });
  });

  // -----------------------------------------------------------------------
  // executeDeploy — deploy script failure
  // -----------------------------------------------------------------------

  describe("executeDeploy() — script failure", () => {
    it("stops on first failing script and reports the change", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps({ failOnScript: "add_users" });
      const options = defaultOptions(testDir);
      const result = await executeDeploy(options, deps);

      // First change succeeds, second fails
      expect(result.deployed).toBe(1);
      expect(result.failedChange).toBe("add_users");
      expect(result.error).toBeDefined();
    });

    it("releases advisory lock after failure", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps({ failOnScript: "add_users" });
      const options = defaultOptions(testDir);
      await executeDeploy(options, deps);

      const pgClient = getPgClient();
      const unlockQuery = pgClient.queries.find((q) => q.text.includes("pg_advisory_unlock"));
      expect(unlockQuery).toBeDefined();
    });

    it("records a fail event when deploy script fails", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps({ failOnScript: "add_users" });
      const options = defaultOptions(testDir);
      await executeDeploy(options, deps);

      const pgClient = getPgClient();
      const failEvent = pgClient.queries.find((q) =>
        q.text.includes("INSERT INTO sqitch.events") && q.values?.[0] === "fail",
      );
      expect(failEvent).toBeDefined();
      expect(failEvent!.values![2]).toBe("add_users");
    });
  });

  // -----------------------------------------------------------------------
  // executeDeploy — missing deploy script
  // -----------------------------------------------------------------------

  describe("executeDeploy() — missing deploy script", () => {
    it("returns error when deploy script does not exist", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      // Only write the first script — second is missing
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");

      const deps = await createDeps();
      const options = defaultOptions(testDir);
      const result = await executeDeploy(options, deps);

      expect(result.deployed).toBe(1); // first succeeds
      expect(result.failedChange).toBe("add_users");
      expect(result.error).toContain("Deploy script not found");
    });
  });

  // -----------------------------------------------------------------------
  // executeDeploy — verify
  // -----------------------------------------------------------------------

  describe("executeDeploy() — verify", () => {
    it("runs verify scripts when --verify is set", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");
      writeVerifyScript(testDir, "create_schema", "SELECT 1 FROM information_schema.schemata WHERE schema_name = 'myapp';");
      writeVerifyScript(testDir, "add_users", "SELECT 1 FROM users LIMIT 0;");

      const deps = await createDeps();
      const options = defaultOptions(testDir);
      options.verify = true;

      const result = await executeDeploy(options, deps);
      expect(result.deployed).toBe(2);
      expect(result.error).toBeUndefined();
    });

    it("reports error when verify script fails", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");
      writeVerifyScript(testDir, "create_schema", "SELECT 1;");
      writeVerifyScript(testDir, "add_users", "SELECT 1 FROM nonexistent_table;");

      // Use a psql runner that fails on verify scripts
      const mockSpawn: SpawnFn = (_cmd, args, _opts) => {
        const child = Object.assign(new EventEmitter(), {
          stdout: new EventEmitter(),
          stderr: new EventEmitter(),
        });
        const scriptFile = args.find((a: string) => a.endsWith(".sql")) ?? "";
        const isVerifyFail = scriptFile.includes("verify/add_users");
        queueMicrotask(() => {
          if (isVerifyFail) {
            child.stderr.emit("data", Buffer.from("ERROR: relation nonexistent_table does not exist"));
          }
          child.emit("close", isVerifyFail ? 1 : 0);
        });
        return child as ReturnType<typeof import("child_process").spawn>;
      };

      const deps = await createDeps();
      deps.psqlRunner = new PsqlRunner("psql", mockSpawn);

      const options = defaultOptions(testDir);
      options.verify = true;

      const result = await executeDeploy(options, deps);
      expect(result.failedChange).toBe("add_users");
      expect(result.error).toContain("Verify failed");
    });
  });

  // -----------------------------------------------------------------------
  // executeDeploy — non-transactional changes
  // -----------------------------------------------------------------------

  describe("executeDeploy() — non-transactional changes", () => {
    it("detects no-transaction marker and deploys without --single-transaction", async () => {
      const plan = `%syntax-version=1.0.0
%project=myproject

create_schema 2025-01-01T00:00:00Z Test User <test@example.com> # Schema
add_index 2025-01-02T00:00:00Z Test User <test@example.com> # Concurrent index
`;
      writePlan(testDir, plan);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_index", "-- sqlever:no-transaction\nCREATE INDEX CONCURRENTLY idx_users_email ON users(email);");

      // Track psql invocations
      const calls: Array<{ args: string[] }> = [];
      const mockSpawn: SpawnFn = (_cmd, args, _opts) => {
        calls.push({ args: [...args] });
        const child = Object.assign(new EventEmitter(), {
          stdout: new EventEmitter(),
          stderr: new EventEmitter(),
        });
        queueMicrotask(() => {
          child.emit("close", 0);
        });
        return child as ReturnType<typeof import("child_process").spawn>;
      };

      const deps = await createDeps();
      deps.psqlRunner = new PsqlRunner("psql", mockSpawn);

      const options = defaultOptions(testDir);
      const result = await executeDeploy(options, deps);

      expect(result.deployed).toBe(2);

      // First script (transactional) should have --single-transaction
      const firstCall = calls[0]!;
      expect(firstCall.args).toContain("--single-transaction");

      // Second script (non-transactional) should NOT have --single-transaction
      const secondCall = calls[1]!;
      expect(secondCall.args).not.toContain("--single-transaction");
    });
  });

  // -----------------------------------------------------------------------
  // executeDeploy — lock timeout
  // -----------------------------------------------------------------------

  describe("executeDeploy() — lock timeout guard", () => {
    it("passes lock timeout to psql when specified and script doesn't set its own", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const calls: Array<{ args: string[] }> = [];
      const mockSpawn: SpawnFn = (_cmd, args, _opts) => {
        calls.push({ args: [...args] });
        const child = Object.assign(new EventEmitter(), {
          stdout: new EventEmitter(),
          stderr: new EventEmitter(),
        });
        queueMicrotask(() => child.emit("close", 0));
        return child as ReturnType<typeof import("child_process").spawn>;
      };

      const deps = await createDeps();
      deps.psqlRunner = new PsqlRunner("psql", mockSpawn);

      const options = defaultOptions(testDir);
      options.lockTimeout = 5000;

      await executeDeploy(options, deps);

      // Each psql call should include the lock timeout
      for (const call of calls) {
        expect(call.args.join(" ")).toContain("SET lock_timeout = '5000ms'");
      }
    });

    it("skips auto lock timeout when script already sets lock_timeout", async () => {
      const plan = `%syntax-version=1.0.0
%project=myproject

create_schema 2025-01-01T00:00:00Z Test User <test@example.com> # Schema
`;
      writePlan(testDir, plan);
      writeDeployScript(testDir, "create_schema", "SET lock_timeout = '10s';\nCREATE SCHEMA myapp;");

      const calls: Array<{ args: string[] }> = [];
      const mockSpawn: SpawnFn = (_cmd, args, _opts) => {
        calls.push({ args: [...args] });
        const child = Object.assign(new EventEmitter(), {
          stdout: new EventEmitter(),
          stderr: new EventEmitter(),
        });
        queueMicrotask(() => child.emit("close", 0));
        return child as ReturnType<typeof import("child_process").spawn>;
      };

      const deps = await createDeps();
      deps.psqlRunner = new PsqlRunner("psql", mockSpawn);

      const options = defaultOptions(testDir);
      options.lockTimeout = 5000;

      await executeDeploy(options, deps);

      // Should NOT contain the auto-set lock timeout
      for (const call of calls) {
        expect(call.args.join(" ")).not.toContain("SET lock_timeout = '5000ms'");
      }
    });
  });

  // -----------------------------------------------------------------------
  // executeDeploy — tags
  // -----------------------------------------------------------------------

  describe("executeDeploy() — tags", () => {
    it("records tags after deploying the tagged change", async () => {
      writePlan(testDir, TAGGED_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");
      writeDeployScript(testDir, "add_posts", "CREATE TABLE posts (id int);");

      const deps = await createDeps();
      const options = defaultOptions(testDir);
      const result = await executeDeploy(options, deps);

      expect(result.deployed).toBe(3);

      const pgClient = getPgClient();
      const tagInserts = pgClient.queries.filter((q) =>
        q.text.includes("INSERT INTO sqitch.tags"),
      );
      expect(tagInserts.length).toBe(1);
      expect(tagInserts[0]!.values![1]).toBe("@v1.0");
    });
  });

  // -----------------------------------------------------------------------
  // executeDeploy — psql variables
  // -----------------------------------------------------------------------

  describe("executeDeploy() — psql variables", () => {
    it("passes --set variables to psql", async () => {
      const plan = `%syntax-version=1.0.0
%project=myproject

create_schema 2025-01-01T00:00:00Z Test User <test@example.com> # Schema
`;
      writePlan(testDir, plan);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA :schema_name;");

      const calls: Array<{ args: string[] }> = [];
      const mockSpawn: SpawnFn = (_cmd, args, _opts) => {
        calls.push({ args: [...args] });
        const child = Object.assign(new EventEmitter(), {
          stdout: new EventEmitter(),
          stderr: new EventEmitter(),
        });
        queueMicrotask(() => child.emit("close", 0));
        return child as ReturnType<typeof import("child_process").spawn>;
      };

      const deps = await createDeps();
      deps.psqlRunner = new PsqlRunner("psql", mockSpawn);

      const options = defaultOptions(testDir);
      options.variables = { schema_name: "myapp" };

      await executeDeploy(options, deps);

      // Verify the variable was passed
      expect(calls.length).toBe(1);
      const psqlArgs = calls[0]!.args;
      const varIdx = psqlArgs.indexOf("-v");
      expect(varIdx).toBeGreaterThan(-1);
      // The variable flag is followed by key=value
      const nextArgs = psqlArgs.slice(varIdx);
      expect(nextArgs).toContain("schema_name=myapp");
    });
  });

  // -----------------------------------------------------------------------
  // executeDeploy — advisory lock ordering
  // -----------------------------------------------------------------------

  describe("executeDeploy() — ordering invariants", () => {
    it("acquires deploy lock before creating registry, releases after all changes", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps();
      const options = defaultOptions(testDir);
      await executeDeploy(options, deps);

      const pgClient = getPgClient();

      // Find the deploy advisory lock (pg_try_advisory_lock with two args)
      const deployLockIdx = pgClient.queries.findIndex((q) =>
        q.text.includes("pg_try_advisory_lock") && q.values?.length === 2,
      );
      // Find the registry DDL
      const registryIdx = pgClient.queries.findIndex((q) =>
        q.text.includes("CREATE SCHEMA IF NOT EXISTS sqitch"),
      );
      // Find the first change INSERT
      const firstChangeIdx = pgClient.queries.findIndex((q) =>
        q.text.includes("INSERT INTO sqitch.changes"),
      );
      // Find the deploy advisory unlock (two-arg form)
      const deployUnlockIdx = pgClient.queries.findIndex((q) =>
        q.text.includes("pg_advisory_unlock") && q.values?.length === 2,
      );

      // Deploy lock -> registry -> changes -> deploy unlock
      expect(deployLockIdx).toBeGreaterThanOrEqual(0);
      expect(registryIdx).toBeGreaterThanOrEqual(0);
      expect(firstChangeIdx).toBeGreaterThanOrEqual(0);
      expect(deployUnlockIdx).toBeGreaterThanOrEqual(0);

      expect(deployLockIdx).toBeLessThan(registryIdx);
      expect(registryIdx).toBeLessThan(firstChangeIdx);
      expect(firstChangeIdx).toBeLessThan(deployUnlockIdx);
    });
  });

  // -----------------------------------------------------------------------
  // parseDeployOptions
  // -----------------------------------------------------------------------

  describe("parseDeployOptions()", () => {
    it("parses --to flag", () => {
      const args = {
        command: "deploy",
        rest: ["--to", "add_users"],
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
      };

      const options = parseDeployOptions(args);
      expect(options.to).toBe("add_users");
    });

    it("parses --mode change flag", () => {
      const args = {
        command: "deploy",
        rest: ["--mode", "change"],
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
      };

      const options = parseDeployOptions(args);
      expect(options.mode).toBe("change");
    });

    it("parses --dry-run flag", () => {
      const args = {
        command: "deploy",
        rest: ["--dry-run"],
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
      };

      const options = parseDeployOptions(args);
      expect(options.dryRun).toBe(true);
    });

    it("parses --verify and --no-verify flags", () => {
      const args1 = {
        command: "deploy",
        rest: ["--verify"],
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
      };

      const options1 = parseDeployOptions(args1);
      expect(options1.verify).toBe(true);

      const args2 = {
        command: "deploy",
        rest: ["--no-verify"],
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
      };

      const options2 = parseDeployOptions(args2);
      expect(options2.verify).toBe(false);
    });

    it("parses --set key=value", () => {
      const args = {
        command: "deploy",
        rest: ["--set", "schema=public", "--set", "table=users"],
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
      };

      const options = parseDeployOptions(args);
      expect(options.variables).toEqual({ schema: "public", table: "users" });
    });
  });

  // -----------------------------------------------------------------------
  // executeDeploy — registry recordFail
  // -----------------------------------------------------------------------

  describe("Registry.recordFail()", () => {
    it("inserts a fail event into sqitch.events", async () => {
      const db = new DatabaseClient("postgresql://localhost/testdb");
      await db.connect();
      const pgClient = getPgClient();
      const registry = new Registry(db);

      await registry.recordFail({
        change_id: "abc123",
        script_hash: "hash",
        change: "failed_change",
        project: "myproject",
        note: "This failed",
        committer_name: "Test",
        committer_email: "test@x.com",
        planned_at: new Date("2025-01-01"),
        planner_name: "Test",
        planner_email: "test@x.com",
        requires: [],
        conflicts: [],
        tags: [],
        dependencies: [],
      });

      const failEvent = pgClient.queries.find((q) =>
        q.text.includes("INSERT INTO sqitch.events") && q.values?.[0] === "fail",
      );
      expect(failEvent).toBeDefined();
      expect(failEvent!.values![0]).toBe("fail");
      expect(failEvent!.values![2]).toBe("failed_change");
    });
  });

  // -----------------------------------------------------------------------
  // runDeploy — exit code pattern (no process.exit)
  // -----------------------------------------------------------------------

  describe("runDeploy() — exit codes", () => {
    it("returns EXIT_DEPLOY_FAILED when no DB URI is specified", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");

      const args = {
        command: "deploy",
        rest: [],
        help: false,
        version: false,
        format: "text" as const,
        quiet: false,
        verbose: false,
        dbUri: undefined as string | undefined,
        planFile: undefined,
        topDir: testDir,
        registry: undefined,
        target: undefined,
      };

      const exitCode = await runDeploy(args);
      expect(exitCode).toBe(EXIT_DEPLOY_FAILED);
    });

    it("returns exit code instead of calling process.exit()", async () => {
      // Verify the return type is a number (not void), confirming
      // runDeploy no longer calls process.exit() directly.
      writePlan(testDir, SIMPLE_PLAN);

      const args = {
        command: "deploy",
        rest: [],
        help: false,
        version: false,
        format: "text" as const,
        quiet: false,
        verbose: false,
        dbUri: undefined as string | undefined,
        planFile: undefined,
        topDir: testDir,
        registry: undefined,
        target: undefined,
      };

      const exitCode = await runDeploy(args);
      expect(typeof exitCode).toBe("number");
      expect(exitCode).toBe(EXIT_DEPLOY_FAILED);
    });
  });

  // -----------------------------------------------------------------------
  // Cleanup runs even on failure (advisory lock release)
  // -----------------------------------------------------------------------

  describe("executeDeploy() — cleanup on failure", () => {
    it("releases advisory lock even when deploy script throws", async () => {
      writePlan(testDir, SIMPLE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps({ failOnScript: "create_schema" });
      const options = defaultOptions(testDir);
      const result = await executeDeploy(options, deps);

      // Deploy should have failed
      expect(result.error).toBeDefined();
      expect(result.failedChange).toBe("create_schema");

      // But the advisory lock MUST still be released (finally block ran)
      const pgClient = getPgClient();
      const unlockQuery = pgClient.queries.find((q) =>
        q.text.includes("pg_advisory_unlock") && q.values?.length === 2,
      );
      expect(unlockQuery).toBeDefined();
    });
  });

  // -----------------------------------------------------------------------
  // parseDeployOptions — argument validation
  // -----------------------------------------------------------------------

  describe("parseDeployOptions() — argument validation", () => {
    function makeArgs(rest: string[]) {
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
      };
    }

    it("throws when --to is missing its value", () => {
      expect(() => parseDeployOptions(makeArgs(["--to"]))).toThrow("--to requires a change name");
    });

    it("throws when --to is followed by another flag", () => {
      expect(() => parseDeployOptions(makeArgs(["--to", "--dry-run"]))).toThrow("--to requires a change name");
    });

    it("throws when --mode is missing its value", () => {
      expect(() => parseDeployOptions(makeArgs(["--mode"]))).toThrow("--mode requires a value");
    });

    it("throws when --mode is an unknown value", () => {
      expect(() => parseDeployOptions(makeArgs(["--mode", "banana"]))).toThrow("Unknown mode: banana");
    });

    it("throws when --mode all is used (not yet implemented)", () => {
      expect(() => parseDeployOptions(makeArgs(["--mode", "all"]))).toThrow("--mode all is not yet implemented");
    });

    it("throws when --mode tag is used (not yet implemented)", () => {
      expect(() => parseDeployOptions(makeArgs(["--mode", "tag"]))).toThrow("--mode tag is not yet implemented");
    });

    it("throws when --set is missing its value", () => {
      expect(() => parseDeployOptions(makeArgs(["--set"]))).toThrow("--set requires a key=value argument");
    });

    it("throws when --db-client is missing its value", () => {
      expect(() => parseDeployOptions(makeArgs(["--db-client"]))).toThrow("--db-client requires a path");
    });

    it("throws when --lock-timeout is missing its value", () => {
      expect(() => parseDeployOptions(makeArgs(["--lock-timeout"]))).toThrow("--lock-timeout requires a value");
    });
  });
});
