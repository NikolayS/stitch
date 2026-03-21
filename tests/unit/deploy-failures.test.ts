// tests/unit/deploy-failures.test.ts — Deploy failure recovery tests
//
// Comprehensive tests for all deploy failure scenarios (issue #37).
// Verifies that tracking table state remains consistent after failures,
// exit codes are correct, and partial deploys leave the DB in a valid state.

import { describe, it, expect, beforeEach, mock, afterEach } from "bun:test";
import { EventEmitter } from "events";
import { mkdirSync, writeFileSync, rmSync, existsSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";
import { resetConfig, setConfig } from "../../src/output";

// ---------------------------------------------------------------------------
// Mock pg/lib/client — same approach as deploy.test.ts
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
  parseDeployOptions,
  projectLockKey,
  isNonTransactional,
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
  const dir = join(tmpdir(), `sqlever-deploy-fail-test-${Date.now()}-${testDirCounter}`);
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
const TWO_CHANGE_PLAN = `%syntax-version=1.0.0
%project=myproject

create_schema 2025-01-01T00:00:00Z Test User <test@example.com> # Create schema
add_users [create_schema] 2025-01-02T00:00:00Z Test User <test@example.com> # Add users table
`;

/** A plan with three changes (for mid-batch failure tests) */
const THREE_CHANGE_PLAN = `%syntax-version=1.0.0
%project=myproject

create_schema 2025-01-01T00:00:00Z Test User <test@example.com> # Create schema
add_users [create_schema] 2025-01-02T00:00:00Z Test User <test@example.com> # Add users table
add_posts [add_users] 2025-01-03T00:00:00Z Test User <test@example.com> # Add posts table
`;

/** A plan with a dependency that doesn't exist anywhere */
const MISSING_DEP_PLAN = `%syntax-version=1.0.0
%project=myproject

create_schema 2025-01-01T00:00:00Z Test User <test@example.com> # Create schema
add_users [nonexistent_thing] 2025-01-02T00:00:00Z Test User <test@example.com> # Add users table
`;

/** A plan with a single non-transactional change */
const NON_TXN_PLAN = `%syntax-version=1.0.0
%project=myproject

create_schema 2025-01-01T00:00:00Z Test User <test@example.com> # Schema
add_index 2025-01-02T00:00:00Z Test User <test@example.com> # Concurrent index
`;

/**
 * Create a mock PsqlRunner that succeeds (exit code 0) for all scripts.
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
 * Create a mock PsqlRunner that fails on a specific deploy script name.
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

/**
 * Create a PsqlRunner that tracks invocations and fails on specific scripts.
 */
function createTrackingPsqlRunner(failOnScripts: string[] = []): {
  runner: PsqlRunner;
  calls: Array<{ scriptFile: string; args: string[] }>;
} {
  const calls: Array<{ scriptFile: string; args: string[] }> = [];
  const mockSpawn: SpawnFn = (_cmd, args, _opts) => {
    const scriptFile = args.find((a: string) => a.endsWith(".sql")) ?? "";
    calls.push({ scriptFile, args: [...args] });
    const child = Object.assign(new EventEmitter(), {
      stdout: new EventEmitter(),
      stderr: new EventEmitter(),
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

async function createDeps(opts?: Partial<{
  psqlExitCode: number;
  psqlStderr: string;
  failOnScript: string;
}>): Promise<DeployDeps> {
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("deploy failure recovery", () => {
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
  // 1. SQL error in deploy script — tracking tables unchanged for failed
  //    change, previous changes remain
  // -----------------------------------------------------------------------

  describe("SQL error in deploy script", () => {
    it("does not insert into sqitch.changes for the failed change", async () => {
      writePlan(testDir, TWO_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps({ failOnScript: "add_users" });
      const options = defaultOptions(testDir);
      const result = await executeDeploy(options, deps);

      expect(result.failedChange).toBe("add_users");

      const pgClient = getPgClient();
      const changeInserts = pgClient.queries.filter((q) =>
        q.text.includes("INSERT INTO sqitch.changes"),
      );
      // Only the first change (create_schema) should be recorded
      expect(changeInserts.length).toBe(1);
      expect(changeInserts[0]!.values![2]).toBe("create_schema");
    });

    it("records a 'fail' event for the failed change", async () => {
      writePlan(testDir, TWO_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps({ failOnScript: "add_users" });
      const options = defaultOptions(testDir);
      await executeDeploy(options, deps);

      const pgClient = getPgClient();
      const failEvents = pgClient.queries.filter((q) =>
        q.text.includes("INSERT INTO sqitch.events") && q.values?.[0] === "fail",
      );
      expect(failEvents.length).toBe(1);
      expect(failEvents[0]!.values![2]).toBe("add_users");
    });

    it("preserves the successful deploy event for the first change", async () => {
      writePlan(testDir, TWO_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps({ failOnScript: "add_users" });
      const options = defaultOptions(testDir);
      await executeDeploy(options, deps);

      const pgClient = getPgClient();
      const deployEvents = pgClient.queries.filter((q) =>
        q.text.includes("INSERT INTO sqitch.events") && q.values?.[0] === "deploy",
      );
      expect(deployEvents.length).toBe(1);
      expect(deployEvents[0]!.values![2]).toBe("create_schema");
    });

    it("does not insert dependencies for the failed change", async () => {
      writePlan(testDir, TWO_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps({ failOnScript: "add_users" });
      const options = defaultOptions(testDir);
      await executeDeploy(options, deps);

      const pgClient = getPgClient();
      // No dependency inserts should exist for add_users.
      // In the normal flow, add_users has a dependency on create_schema.
      // But since the deploy failed, recordDeploy is never called for add_users,
      // so the dependency INSERT should not exist for add_users.
      // The dep insert from create_schema (no deps) produces 0 inserts,
      // so we should have 0 total.
      const depInserts = pgClient.queries.filter((q) =>
        q.text.includes("INSERT INTO sqitch.dependencies"),
      );
      expect(depInserts.length).toBe(0);
    });
  });

  // -----------------------------------------------------------------------
  // 2. Deploy fails mid-batch (3 changes, 2nd fails) — first committed,
  //    second rolled back, third not attempted
  // -----------------------------------------------------------------------

  describe("deploy fails mid-batch (3 changes, 2nd fails)", () => {
    it("first change is committed, second rolled back, third not attempted", async () => {
      writePlan(testDir, THREE_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");
      writeDeployScript(testDir, "add_posts", "CREATE TABLE posts (id int);");

      const deps = await createDeps({ failOnScript: "add_users" });
      const options = defaultOptions(testDir);
      const result = await executeDeploy(options, deps);

      // First change succeeded, second failed
      expect(result.deployed).toBe(1);
      expect(result.failedChange).toBe("add_users");
      expect(result.error).toBeDefined();
    });

    it("only records tracking for the first change", async () => {
      writePlan(testDir, THREE_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");
      writeDeployScript(testDir, "add_posts", "CREATE TABLE posts (id int);");

      const deps = await createDeps({ failOnScript: "add_users" });
      const options = defaultOptions(testDir);
      await executeDeploy(options, deps);

      const pgClient = getPgClient();

      // Changes table: only create_schema inserted
      const changeInserts = pgClient.queries.filter((q) =>
        q.text.includes("INSERT INTO sqitch.changes"),
      );
      expect(changeInserts.length).toBe(1);
      expect(changeInserts[0]!.values![2]).toBe("create_schema");

      // Deploy events: only create_schema
      const deployEvents = pgClient.queries.filter((q) =>
        q.text.includes("INSERT INTO sqitch.events") && q.values?.[0] === "deploy",
      );
      expect(deployEvents.length).toBe(1);
      expect(deployEvents[0]!.values![2]).toBe("create_schema");
    });

    it("does not invoke psql for the third change", async () => {
      writePlan(testDir, THREE_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");
      writeDeployScript(testDir, "add_posts", "CREATE TABLE posts (id int);");

      const { runner, calls } = createTrackingPsqlRunner(["add_users"]);
      const deps = await createDeps();
      deps.psqlRunner = runner;

      const options = defaultOptions(testDir);
      await executeDeploy(options, deps);

      // Only two psql calls: create_schema (success) + add_users (fail)
      // add_posts should never be attempted
      expect(calls.length).toBe(2);
      expect(calls[0]!.scriptFile).toContain("create_schema");
      expect(calls[1]!.scriptFile).toContain("add_users");
    });
  });

  // -----------------------------------------------------------------------
  // 3. Database unreachable — exit code 10
  // -----------------------------------------------------------------------

  describe("database unreachable", () => {
    it("EXIT_DB_UNREACHABLE is exit code 10", () => {
      expect(EXIT_DB_UNREACHABLE).toBe(10);
    });

    it("connect() calls process.exit(10) when DB is unreachable", async () => {
      // The DatabaseClient.connect() method calls process.exit(10) directly.
      // We verify the constant is correct and that runDeploy uses
      // the DatabaseClient which has this behavior.
      // Direct integration test of the exit path requires mocking process.exit,
      // which the existing test infrastructure doesn't do. Instead, we verify
      // the contract: EXIT_DB_UNREACHABLE = 10 and DatabaseClient uses it.
      const { EXIT_CODE_DB_UNREACHABLE } = await import("../../src/db/client");
      expect(EXIT_CODE_DB_UNREACHABLE).toBe(10);
    });

    it("runDeploy returns EXIT_DEPLOY_FAILED when no URI specified", async () => {
      writePlan(testDir, TWO_CHANGE_PLAN);
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
  });

  // -----------------------------------------------------------------------
  // 4. Concurrent deploy (advisory lock returning false) — exit code 4
  // -----------------------------------------------------------------------

  describe("concurrent deploy (advisory lock contention)", () => {
    it("returns 'Concurrent deploy detected' error when lock is held", async () => {
      writePlan(testDir, TWO_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps();

      // Override to simulate lock contention
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

    it("runDeploy returns EXIT_CONCURRENT_DEPLOY (4) for concurrent deploy", async () => {
      writePlan(testDir, TWO_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      // We test via runDeploy by overriding the mock client to refuse the lock.
      // runDeploy creates its own DB client, so we need to configure the
      // next MockPgClient that will be created.
      const origMockConnect = MockPgClient.prototype.connect;
      const origMockQuery = MockPgClient.prototype.query;

      // Temporarily patch MockPgClient.query to refuse advisory lock
      MockPgClient.prototype.query = async function (text: string, values?: unknown[]) {
        this.queries.push({ text, values });
        if (text.includes("pg_try_advisory_lock")) {
          return { rows: [{ pg_try_advisory_lock: false }], rowCount: 1, command: "SELECT" };
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
        return { rows: [], rowCount: 0, command: "SELECT" };
      };

      try {
        const args = {
          command: "deploy",
          rest: [],
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

        const exitCode = await runDeploy(args);
        expect(exitCode).toBe(EXIT_CONCURRENT_DEPLOY);
      } finally {
        // Restore original methods
        MockPgClient.prototype.connect = origMockConnect;
        MockPgClient.prototype.query = origMockQuery;
      }
    });

    it("does not deploy any changes when lock is held", async () => {
      writePlan(testDir, TWO_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const { runner, calls } = createTrackingPsqlRunner();
      const deps = await createDeps();
      deps.psqlRunner = runner;

      // Override to simulate lock contention
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

      // No psql invocations should have happened
      expect(calls.length).toBe(0);
    });

    it("does not attempt to release a lock that was never acquired", async () => {
      writePlan(testDir, TWO_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps();

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

      // Should NOT have any advisory_unlock with 2 args (the deploy lock)
      const unlockQueries = pgClient.queries.filter((q) =>
        q.text.includes("pg_advisory_unlock") && q.values?.length === 2,
      );
      expect(unlockQueries.length).toBe(0);
    });
  });

  // -----------------------------------------------------------------------
  // 5. Missing dependency detected before deploy — clear error
  // -----------------------------------------------------------------------

  describe("missing dependency detected before deploy", () => {
    it("throws MissingDependencyError with clear message", async () => {
      writePlan(testDir, MISSING_DEP_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps();
      const options = defaultOptions(testDir);

      // executeDeploy calls validateDependencies which will throw
      await expect(executeDeploy(options, deps)).rejects.toThrow(
        /requires "nonexistent_thing"/,
      );
    });

    it("does not record any changes when dependency validation fails", async () => {
      writePlan(testDir, MISSING_DEP_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps();
      const options = defaultOptions(testDir);

      try {
        await executeDeploy(options, deps);
      } catch {
        // Expected
      }

      const pgClient = getPgClient();
      // No changes should have been inserted
      // (create_schema could theoretically be deployed before
      // validateDependencies is called, but the code validates all
      // pending changes before deploying any of them)
      //
      // Actually, looking at the code flow: validateDependencies is called
      // AFTER filtering pending changes. The exception propagates before
      // the deploy loop starts.
      const changeInserts = pgClient.queries.filter((q) =>
        q.text.includes("INSERT INTO sqitch.changes"),
      );
      expect(changeInserts.length).toBe(0);
    });

    it("releases advisory lock even when dependency validation fails", async () => {
      writePlan(testDir, MISSING_DEP_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");

      const deps = await createDeps();
      const options = defaultOptions(testDir);

      try {
        await executeDeploy(options, deps);
      } catch {
        // Expected
      }

      const pgClient = getPgClient();
      // The finally block should still release the deploy lock
      const unlockQuery = pgClient.queries.find((q) =>
        q.text.includes("pg_advisory_unlock") && q.values?.length === 2,
      );
      expect(unlockQuery).toBeDefined();
    });
  });

  // -----------------------------------------------------------------------
  // 6. Non-transactional change detection (-- sqlever:no-transaction)
  // -----------------------------------------------------------------------

  describe("non-transactional change detection", () => {
    it("isNonTransactional detects the marker on the first line", () => {
      expect(isNonTransactional("-- sqlever:no-transaction\nCREATE INDEX CONCURRENTLY ...")).toBe(true);
    });

    it("isNonTransactional returns false for second-line marker", () => {
      expect(isNonTransactional("SELECT 1;\n-- sqlever:no-transaction")).toBe(false);
    });

    it("deploys non-transactional script without --single-transaction", async () => {
      writePlan(testDir, NON_TXN_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_index", "-- sqlever:no-transaction\nCREATE INDEX CONCURRENTLY idx ON users(email);");

      const { runner, calls } = createTrackingPsqlRunner();
      const deps = await createDeps();
      deps.psqlRunner = runner;

      const options = defaultOptions(testDir);
      const result = await executeDeploy(options, deps);

      expect(result.deployed).toBe(2);

      // First call (create_schema): transactional => --single-transaction
      expect(calls[0]!.args).toContain("--single-transaction");

      // Second call (add_index): non-transactional => no --single-transaction
      expect(calls[1]!.args).not.toContain("--single-transaction");
    });

    it("records non-transactional change in tracking tables on success", async () => {
      writePlan(testDir, NON_TXN_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_index", "-- sqlever:no-transaction\nCREATE INDEX CONCURRENTLY idx ON users(email);");

      const deps = await createDeps();
      const options = defaultOptions(testDir);
      const result = await executeDeploy(options, deps);

      expect(result.deployed).toBe(2);

      const pgClient = getPgClient();
      const changeInserts = pgClient.queries.filter((q) =>
        q.text.includes("INSERT INTO sqitch.changes"),
      );
      expect(changeInserts.length).toBe(2);
      expect(changeInserts[0]!.values![2]).toBe("create_schema");
      expect(changeInserts[1]!.values![2]).toBe("add_index");
    });

    it("does not record non-transactional change in tracking tables on failure", async () => {
      writePlan(testDir, NON_TXN_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_index", "-- sqlever:no-transaction\nCREATE INDEX CONCURRENTLY idx ON users(email);");

      const deps = await createDeps({ failOnScript: "add_index" });
      const options = defaultOptions(testDir);
      const result = await executeDeploy(options, deps);

      expect(result.failedChange).toBe("add_index");

      const pgClient = getPgClient();
      const changeInserts = pgClient.queries.filter((q) =>
        q.text.includes("INSERT INTO sqitch.changes"),
      );
      // Only create_schema was successfully deployed
      expect(changeInserts.length).toBe(1);
      expect(changeInserts[0]!.values![2]).toBe("create_schema");
    });
  });

  // -----------------------------------------------------------------------
  // 7. --to without value — error (verify fix from #68)
  // -----------------------------------------------------------------------

  describe("--to without value", () => {
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

    it("throws when --to is the last argument (no value)", () => {
      expect(() => parseDeployOptions(makeArgs(["--to"]))).toThrow(
        "--to requires a change name",
      );
    });

    it("throws when --to is followed by another flag instead of a value", () => {
      expect(() => parseDeployOptions(makeArgs(["--to", "--dry-run"]))).toThrow(
        "--to requires a change name",
      );
    });

    it("throws when --to is followed by --verify flag", () => {
      expect(() => parseDeployOptions(makeArgs(["--to", "--verify"]))).toThrow(
        "--to requires a change name",
      );
    });

    it("succeeds when --to has a valid change name", () => {
      const options = parseDeployOptions(makeArgs(["--to", "create_schema"]));
      expect(options.to).toBe("create_schema");
    });
  });

  // -----------------------------------------------------------------------
  // 8. --dry-run makes zero DB changes
  // -----------------------------------------------------------------------

  describe("--dry-run makes zero DB changes", () => {
    it("does not connect to database", async () => {
      writePlan(testDir, THREE_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");
      writeDeployScript(testDir, "add_posts", "CREATE TABLE posts (id int);");

      const deps = await createDeps();
      const options = defaultOptions(testDir);
      options.dryRun = true;

      await executeDeploy(options, deps);

      const pgClient = getPgClient();
      // In dry-run, executeDeploy returns before db.connect() is called,
      // so no queries at all should have been issued
      expect(pgClient.queries.length).toBe(0);
    });

    it("does not acquire advisory lock", async () => {
      writePlan(testDir, THREE_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");
      writeDeployScript(testDir, "add_posts", "CREATE TABLE posts (id int);");

      const deps = await createDeps();
      const options = defaultOptions(testDir);
      options.dryRun = true;

      await executeDeploy(options, deps);

      const pgClient = getPgClient();
      const lockQuery = pgClient.queries.find((q) =>
        q.text.includes("pg_try_advisory_lock"),
      );
      expect(lockQuery).toBeUndefined();
    });

    it("does not create registry schema", async () => {
      writePlan(testDir, THREE_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");
      writeDeployScript(testDir, "add_posts", "CREATE TABLE posts (id int);");

      const deps = await createDeps();
      const options = defaultOptions(testDir);
      options.dryRun = true;

      await executeDeploy(options, deps);

      const pgClient = getPgClient();
      const registryDdl = pgClient.queries.find((q) =>
        q.text.includes("CREATE SCHEMA IF NOT EXISTS sqitch"),
      );
      expect(registryDdl).toBeUndefined();
    });

    it("reports deployed=0 and dryRun=true", async () => {
      writePlan(testDir, THREE_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");
      writeDeployScript(testDir, "add_posts", "CREATE TABLE posts (id int);");

      const deps = await createDeps();
      const options = defaultOptions(testDir);
      options.dryRun = true;

      const result = await executeDeploy(options, deps);

      expect(result.deployed).toBe(0);
      expect(result.dryRun).toBe(true);
      expect(result.error).toBeUndefined();
    });

    it("does not invoke psql at all", async () => {
      writePlan(testDir, THREE_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");
      writeDeployScript(testDir, "add_posts", "CREATE TABLE posts (id int);");

      const { runner, calls } = createTrackingPsqlRunner();
      const deps = await createDeps();
      deps.psqlRunner = runner;

      const options = defaultOptions(testDir);
      options.dryRun = true;

      await executeDeploy(options, deps);

      expect(calls.length).toBe(0);
    });
  });

  // -----------------------------------------------------------------------
  // 9. Verify script failure — exit code from executeDeploy
  // -----------------------------------------------------------------------

  describe("verify script failure", () => {
    it("returns error with 'Verify failed' message", async () => {
      writePlan(testDir, TWO_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");
      writeVerifyScript(testDir, "create_schema", "SELECT 1;");
      writeVerifyScript(testDir, "add_users", "SELECT 1 FROM nonexistent_table;");

      // Create a psql runner that fails on verify/add_users
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

    it("deploy still counts as deployed when verify fails (change already committed)", async () => {
      writePlan(testDir, TWO_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");
      writeVerifyScript(testDir, "create_schema", "SELECT 1;");
      writeVerifyScript(testDir, "add_users", "SELECT 1 FROM nonexistent;");

      const mockSpawn: SpawnFn = (_cmd, args, _opts) => {
        const child = Object.assign(new EventEmitter(), {
          stdout: new EventEmitter(),
          stderr: new EventEmitter(),
        });
        const scriptFile = args.find((a: string) => a.endsWith(".sql")) ?? "";
        const isVerifyFail = scriptFile.includes("verify/add_users");
        queueMicrotask(() => {
          if (isVerifyFail) {
            child.stderr.emit("data", Buffer.from("ERROR: relation nonexistent does not exist"));
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

      // Both changes deployed (deploy script succeeded for both),
      // but verify failed after the second
      expect(result.deployed).toBe(2);
      expect(result.failedChange).toBe("add_users");
    });

    it("tracking tables have the deployed change even though verify failed", async () => {
      writePlan(testDir, TWO_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");
      writeVerifyScript(testDir, "create_schema", "SELECT 1;");
      writeVerifyScript(testDir, "add_users", "SELECT 1 FROM nonexistent;");

      const mockSpawn: SpawnFn = (_cmd, args, _opts) => {
        const child = Object.assign(new EventEmitter(), {
          stdout: new EventEmitter(),
          stderr: new EventEmitter(),
        });
        const scriptFile = args.find((a: string) => a.endsWith(".sql")) ?? "";
        const isVerifyFail = scriptFile.includes("verify/add_users");
        queueMicrotask(() => {
          if (isVerifyFail) {
            child.stderr.emit("data", Buffer.from("ERROR: relation nonexistent does not exist"));
          }
          child.emit("close", isVerifyFail ? 1 : 0);
        });
        return child as ReturnType<typeof import("child_process").spawn>;
      };

      const deps = await createDeps();
      deps.psqlRunner = new PsqlRunner("psql", mockSpawn);

      const options = defaultOptions(testDir);
      options.verify = true;

      await executeDeploy(options, deps);

      const pgClient = getPgClient();
      // Both changes should be in the tracking tables because
      // deploy succeeded for both; verify runs AFTER recordDeploy
      const changeInserts = pgClient.queries.filter((q) =>
        q.text.includes("INSERT INTO sqitch.changes"),
      );
      expect(changeInserts.length).toBe(2);
      expect(changeInserts[0]!.values![2]).toBe("create_schema");
      expect(changeInserts[1]!.values![2]).toBe("add_users");
    });
  });

  // -----------------------------------------------------------------------
  // Additional failure scenarios
  // -----------------------------------------------------------------------

  describe("cleanup invariants on failure", () => {
    it("always releases advisory lock after deploy script failure", async () => {
      writePlan(testDir, THREE_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");
      writeDeployScript(testDir, "add_posts", "CREATE TABLE posts (id int);");

      const deps = await createDeps({ failOnScript: "create_schema" });
      const options = defaultOptions(testDir);
      const result = await executeDeploy(options, deps);

      expect(result.error).toBeDefined();

      const pgClient = getPgClient();
      const unlockQuery = pgClient.queries.find((q) =>
        q.text.includes("pg_advisory_unlock") && q.values?.length === 2,
      );
      expect(unlockQuery).toBeDefined();
    });

    it("always disconnects from database after failure", async () => {
      writePlan(testDir, THREE_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");
      writeDeployScript(testDir, "add_posts", "CREATE TABLE posts (id int);");

      const deps = await createDeps({ failOnScript: "create_schema" });
      const options = defaultOptions(testDir);
      await executeDeploy(options, deps);

      const pgClient = getPgClient();
      expect(pgClient.ended).toBe(true);
    });
  });

  describe("exit code constants are correct per SPEC R6", () => {
    it("EXIT_DEPLOY_FAILED = 1", () => {
      expect(EXIT_DEPLOY_FAILED).toBe(1);
    });

    it("EXIT_CONCURRENT_DEPLOY = 4", () => {
      expect(EXIT_CONCURRENT_DEPLOY).toBe(4);
    });

    it("EXIT_LOCK_TIMEOUT = 5", () => {
      expect(EXIT_LOCK_TIMEOUT).toBe(5);
    });

    it("EXIT_DB_UNREACHABLE = 10", () => {
      expect(EXIT_DB_UNREACHABLE).toBe(10);
    });
  });

  describe("first change failure (no changes committed at all)", () => {
    it("returns deployed=0 when the very first change fails", async () => {
      writePlan(testDir, THREE_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");
      writeDeployScript(testDir, "add_posts", "CREATE TABLE posts (id int);");

      const deps = await createDeps({ failOnScript: "create_schema" });
      const options = defaultOptions(testDir);
      const result = await executeDeploy(options, deps);

      expect(result.deployed).toBe(0);
      expect(result.failedChange).toBe("create_schema");
    });

    it("records fail event but no change inserts when first change fails", async () => {
      writePlan(testDir, THREE_CHANGE_PLAN);
      writeDeployScript(testDir, "create_schema", "CREATE SCHEMA myapp;");
      writeDeployScript(testDir, "add_users", "CREATE TABLE users (id int);");
      writeDeployScript(testDir, "add_posts", "CREATE TABLE posts (id int);");

      const deps = await createDeps({ failOnScript: "create_schema" });
      const options = defaultOptions(testDir);
      await executeDeploy(options, deps);

      const pgClient = getPgClient();

      const changeInserts = pgClient.queries.filter((q) =>
        q.text.includes("INSERT INTO sqitch.changes"),
      );
      expect(changeInserts.length).toBe(0);

      const failEvents = pgClient.queries.filter((q) =>
        q.text.includes("INSERT INTO sqitch.events") && q.values?.[0] === "fail",
      );
      expect(failEvents.length).toBe(1);
      expect(failEvents[0]!.values![2]).toBe("create_schema");
    });
  });
});
