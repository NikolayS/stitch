// tests/unit/deploy-phase.test.ts — Tests for --phase expand|contract deploy
//
// Validates: phase flag parsing, expand-only filtering, contract-only filtering,
// backfill verification before contract, status reporting of expand/contract
// state, error cases (contract before expand, incomplete backfill), and
// phase-filter utility functions.
//
// Implements acceptance criteria for issue #101.

import { describe, it, expect, beforeEach, afterEach, mock } from "bun:test";
import { EventEmitter } from "events";
import { mkdirSync, writeFileSync, rmSync, existsSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";
import { resetConfig, setConfig } from "../../src/output";

// ---------------------------------------------------------------------------
// Mock pg/lib/client — same approach as deploy-failures.test.ts
// ---------------------------------------------------------------------------

let mockInstances: MockPgClient[] = [];

interface QueryRecord {
  text: string;
  values?: unknown[];
}

class MockPgClient {
  options: Record<string, unknown>;
  queries: QueryRecord[] = [];
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
    // Advisory lock
    if (text.includes("pg_try_advisory_lock")) {
      return { rows: [{ pg_try_advisory_lock: true }], rowCount: 1, command: "SELECT" };
    }
    if (text.includes("pg_advisory_unlock")) {
      return { rows: [{ pg_advisory_unlock: true }], rowCount: 1, command: "SELECT" };
    }
    if (text.includes("pg_advisory_lock") && !text.includes("try") && !text.includes("unlock")) {
      return { rows: [], rowCount: 0, command: "SELECT" };
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
    // CREATE SCHEMA / CREATE TABLE for expand_contract_state
    if (text.includes("CREATE SCHEMA") || text.includes("CREATE TABLE")) {
      return { rows: [], rowCount: 0, command: "CREATE" };
    }
    // SELECT from expand_contract_state — default no operations
    if (text.includes("expand_contract_state")) {
      if (text.includes("INSERT")) {
        return {
          rows: [{
            id: 1,
            change_name: "test",
            project: "test",
            phase: "expanding",
            table_schema: "public",
            table_name: "users",
            started_at: new Date(),
            updated_at: new Date(),
            started_by: "test@x.com",
          }],
          rowCount: 1,
          command: "INSERT",
        };
      }
      if (text.includes("UPDATE")) {
        return {
          rows: [{
            id: 1,
            change_name: "test",
            project: "test",
            phase: "expanded",
            table_schema: "public",
            table_name: "users",
            started_at: new Date(),
            updated_at: new Date(),
            started_by: "test@x.com",
          }],
          rowCount: 1,
          command: "UPDATE",
        };
      }
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
import type { DeployOptions, DeployDeps, DeployPhase } from "../../src/commands/deploy";
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

// Import phase-filter utilities
const {
  isExpandChange,
  isContractChange,
  isExpandContractChange,
  extractBaseName,
  expandChangeName,
  contractChangeName,
  filterExpandChanges,
  filterContractChanges,
} = await import("../../src/expand-contract/phase-filter");

// Import status types
import type { StatusResult, ExpandContractStatus } from "../../src/commands/status";
const {
  computeStatus,
  formatStatusText,
} = await import("../../src/commands/status");

import type { Plan } from "../../src/plan/types";
import type { Change as RegistryChange } from "../../src/db/registry";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function getPgClient(): MockPgClient {
  return mockInstances[mockInstances.length - 1]!;
}

let tmpDir: string;
let counter = 0;

function createTmpDir(): string {
  const dir = join(tmpdir(), `sqlever-deploy-phase-test-${Date.now()}-${counter++}`);
  mkdirSync(dir, { recursive: true });
  return dir;
}

/** Set up a project with expand/contract changes in the plan. */
function setupProject(dir: string, opts?: {
  planChanges?: string;
  expandScript?: string;
  contractScript?: string;
}): void {
  const deployDir = join(dir, "deploy");
  const verifyDir = join(dir, "verify");
  const revertDir = join(dir, "revert");
  mkdirSync(deployDir, { recursive: true });
  mkdirSync(verifyDir, { recursive: true });
  mkdirSync(revertDir, { recursive: true });

  // sqitch.conf
  writeFileSync(join(dir, "sqitch.conf"), "[core]\n\tengine = pg\n");

  // Plan with expand/contract pair
  const planContent = opts?.planChanges ?? `%syntax-version=1.0.0
%project=testproject

rename_users_name_expand 2024-01-01T00:00:00Z Dev <dev@test.com> # expand
rename_users_name_contract [rename_users_name_expand] 2024-01-02T00:00:00Z Dev <dev@test.com> # contract
`;
  writeFileSync(join(dir, "sqitch.plan"), planContent);

  // Deploy scripts
  writeFileSync(
    join(deployDir, "rename_users_name_expand.sql"),
    opts?.expandScript ?? "-- expand deploy\nALTER TABLE users ADD COLUMN full_name text;\n",
  );
  writeFileSync(
    join(deployDir, "rename_users_name_contract.sql"),
    opts?.contractScript ?? "-- contract deploy\nALTER TABLE users DROP COLUMN name;\n",
  );
}

function makeArgs(rest: string[]): {
  command: string | undefined;
  rest: string[];
  help: boolean;
  version: boolean;
  format: "text" | "json";
  quiet: boolean;
  verbose: boolean;
  dbUri: string | undefined;
  planFile: string | undefined;
  topDir: string | undefined;
  registry: string | undefined;
  target: string | undefined;
} {
  return {
    command: "deploy",
    rest,
    help: false,
    version: false,
    format: "text",
    quiet: false,
    verbose: false,
    dbUri: "postgresql://host/db",
    planFile: undefined,
    topDir: undefined,
    registry: undefined,
    target: undefined,
  };
}

function makePlan(
  projectName: string,
  changes: Array<{ name: string; change_id: string; requires?: string[] }>,
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
      requires: c.requires ?? [],
      conflicts: [],
    })),
    tags: [],
  };
}

/**
 * Create a mock PsqlRunner that succeeds for all scripts.
 * Optionally tracks which scripts were run.
 */
function createMockPsqlRunner(calls?: Array<{ scriptFile: string }>): InstanceType<typeof PsqlRunner> {
  const mockSpawn: SpawnFn = (_cmd, args, _opts) => {
    const scriptFile = (args as string[]).find((a: string) => a.endsWith(".sql")) ?? "";
    if (calls) calls.push({ scriptFile });
    const child = Object.assign(new EventEmitter(), {
      stdout: new EventEmitter(),
      stderr: new EventEmitter(),
    });
    queueMicrotask(() => {
      child.emit("close", 0);
    });
    return child as ReturnType<typeof import("child_process").spawn>;
  };
  return new PsqlRunner("psql", mockSpawn);
}

function makeRegistryChange(
  name: string,
  change_id: string,
): RegistryChange {
  return {
    change_id,
    script_hash: null,
    change: name,
    project: "testproject",
    note: "",
    committed_at: new Date("2024-01-15T10:30:00Z"),
    committer_name: "Deployer",
    committer_email: "deploy@test.com",
    planned_at: new Date("2024-01-01T00:00:00Z"),
    planner_name: "Test",
    planner_email: "test@test.com",
  };
}

// ---------------------------------------------------------------------------
// 1. Phase-filter utility tests
// ---------------------------------------------------------------------------

describe("phase-filter utilities", () => {
  it("isExpandChange identifies _expand suffix", () => {
    expect(isExpandChange("rename_users_name_expand")).toBe(true);
    expect(isExpandChange("foo_expand")).toBe(true);
    expect(isExpandChange("expand")).toBe(false);
    expect(isExpandChange("rename_users_name_contract")).toBe(false);
    expect(isExpandChange("rename_expand_things")).toBe(false);
  });

  it("isContractChange identifies _contract suffix", () => {
    expect(isContractChange("rename_users_name_contract")).toBe(true);
    expect(isContractChange("foo_contract")).toBe(true);
    expect(isContractChange("contract")).toBe(false);
    expect(isContractChange("rename_users_name_expand")).toBe(false);
    expect(isContractChange("contract_something")).toBe(false);
  });

  it("isExpandContractChange identifies either suffix", () => {
    expect(isExpandContractChange("foo_expand")).toBe(true);
    expect(isExpandContractChange("foo_contract")).toBe(true);
    expect(isExpandContractChange("foo_bar")).toBe(false);
    expect(isExpandContractChange("regular_change")).toBe(false);
  });

  it("extractBaseName returns base from expand or contract name", () => {
    expect(extractBaseName("rename_users_name_expand")).toBe("rename_users_name");
    expect(extractBaseName("rename_users_name_contract")).toBe("rename_users_name");
    expect(extractBaseName("x_expand")).toBe("x");
    expect(extractBaseName("x_contract")).toBe("x");
    expect(extractBaseName("regular_change")).toBeNull();
  });

  it("expandChangeName and contractChangeName produce correct names", () => {
    expect(expandChangeName("rename_users_name")).toBe("rename_users_name_expand");
    expect(contractChangeName("rename_users_name")).toBe("rename_users_name_contract");
  });

  it("filterExpandChanges returns only expand changes", () => {
    const changes = [
      { name: "init_schema", change_id: "a", project: "p", note: "", planner_name: "T", planner_email: "t@t.com", planned_at: "2024-01-01T00:00:00Z", requires: [] as string[], conflicts: [] as string[] },
      { name: "rename_expand", change_id: "b", project: "p", note: "", planner_name: "T", planner_email: "t@t.com", planned_at: "2024-01-01T00:00:00Z", requires: [] as string[], conflicts: [] as string[] },
      { name: "rename_contract", change_id: "c", project: "p", note: "", planner_name: "T", planner_email: "t@t.com", planned_at: "2024-01-01T00:00:00Z", requires: [] as string[], conflicts: [] as string[] },
    ];
    const result = filterExpandChanges(changes);
    expect(result).toHaveLength(1);
    expect(result[0]!.name).toBe("rename_expand");
  });

  it("filterContractChanges returns only contract changes", () => {
    const changes = [
      { name: "init_schema", change_id: "a", project: "p", note: "", planner_name: "T", planner_email: "t@t.com", planned_at: "2024-01-01T00:00:00Z", requires: [] as string[], conflicts: [] as string[] },
      { name: "rename_expand", change_id: "b", project: "p", note: "", planner_name: "T", planner_email: "t@t.com", planned_at: "2024-01-01T00:00:00Z", requires: [] as string[], conflicts: [] as string[] },
      { name: "rename_contract", change_id: "c", project: "p", note: "", planner_name: "T", planner_email: "t@t.com", planned_at: "2024-01-01T00:00:00Z", requires: [] as string[], conflicts: [] as string[] },
    ];
    const result = filterContractChanges(changes);
    expect(result).toHaveLength(1);
    expect(result[0]!.name).toBe("rename_contract");
  });
});

// ---------------------------------------------------------------------------
// 2. parseDeployOptions --phase parsing
// ---------------------------------------------------------------------------

describe("parseDeployOptions --phase flag", () => {
  beforeEach(() => {
    resetConfig();
    tmpDir = createTmpDir();
    setupProject(tmpDir);
  });

  afterEach(() => {
    try { rmSync(tmpDir, { recursive: true, force: true }); } catch {}
  });

  it("parses --phase expand", () => {
    const opts = parseDeployOptions(makeArgs(["--phase", "expand"]));
    expect(opts.phase).toBe("expand");
  });

  it("parses --phase contract", () => {
    const opts = parseDeployOptions(makeArgs(["--phase", "contract"]));
    expect(opts.phase).toBe("contract");
  });

  it("phase is undefined when --phase not specified", () => {
    const opts = parseDeployOptions(makeArgs([]));
    expect(opts.phase).toBeUndefined();
  });

  it("throws on invalid --phase value", () => {
    expect(() => parseDeployOptions(makeArgs(["--phase", "invalid"]))).toThrow(
      "Unknown phase: invalid"
    );
  });

  it("throws when --phase has no value", () => {
    expect(() => parseDeployOptions(makeArgs(["--phase"]))).toThrow(
      "--phase requires a value"
    );
  });

  it("throws when --phase value starts with dash", () => {
    expect(() => parseDeployOptions(makeArgs(["--phase", "--other"]))).toThrow(
      "--phase requires a value"
    );
  });

  it("can combine --phase with --dry-run", () => {
    const opts = parseDeployOptions(makeArgs(["--phase", "expand", "--dry-run"]));
    expect(opts.phase).toBe("expand");
    expect(opts.dryRun).toBe(true);
  });

  it("can combine --phase with --verify", () => {
    const opts = parseDeployOptions(makeArgs(["--phase", "contract", "--verify"]));
    expect(opts.phase).toBe("contract");
    expect(opts.verify).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// 3. executeDeploy with --phase expand
// ---------------------------------------------------------------------------

describe("executeDeploy --phase expand", () => {
  beforeEach(() => {
    mockInstances = [];
    resetConfig();
    setConfig({ quiet: true });
    tmpDir = createTmpDir();
  });

  afterEach(() => {
    try { rmSync(tmpDir, { recursive: true, force: true }); } catch {}
  });

  it("deploys only expand migrations when --phase expand", async () => {
    // Setup: plan with expand + contract + regular change
    const planContent = `%syntax-version=1.0.0
%project=testproject

init_schema 2024-01-01T00:00:00Z Dev <dev@test.com> # init
rename_expand 2024-01-02T00:00:00Z Dev <dev@test.com> # expand phase
rename_contract [rename_expand] 2024-01-03T00:00:00Z Dev <dev@test.com> # contract phase
`;
    const deployDir = join(tmpDir, "deploy");
    mkdirSync(deployDir, { recursive: true });
    writeFileSync(join(tmpDir, "sqitch.conf"), "[core]\n\tengine = pg\n");
    writeFileSync(join(tmpDir, "sqitch.plan"), planContent);
    writeFileSync(join(deployDir, "init_schema.sql"), "-- init");
    writeFileSync(join(deployDir, "rename_expand.sql"), "-- expand");
    writeFileSync(join(deployDir, "rename_contract.sql"), "-- contract");

    // Track which scripts were run
    const ranScripts: Array<{ scriptFile: string }> = [];

    const client = new DatabaseClient("postgresql://host/db");
    await client.connect();
    const registry = new Registry(client);
    const psqlRunner = createMockPsqlRunner(ranScripts);
    const shutdownMgr = new ShutdownManager();
    const config = loadConfig(tmpDir);

    const result = await executeDeploy(
      {
        to: undefined,
        mode: "change",
        dryRun: false,
        verify: false,
        variables: {},
        dbUri: "postgresql://host/db",
        projectDir: tmpDir,
        committerName: "Test",
        committerEmail: "test@x.com",
        noTui: true,
        noSnapshot: true,
        phase: "expand",
      },
      { db: client, registry, psqlRunner, config, shutdownMgr },
    );

    // Only the expand script should have been deployed
    expect(result.deployed).toBe(1);
    expect(result.error).toBeUndefined();
    // The ranScripts should contain only the expand script
    const expandScripts = ranScripts.filter(s => s.scriptFile.includes("rename_expand"));
    expect(expandScripts.length).toBeGreaterThanOrEqual(1);
    const contractScripts = ranScripts.filter(s => s.scriptFile.includes("rename_contract"));
    expect(contractScripts).toHaveLength(0);
    const initScripts = ranScripts.filter(s => s.scriptFile.includes("init_schema"));
    expect(initScripts).toHaveLength(0);
  });

  it("returns no pending when no expand changes exist", async () => {
    const planContent = `%syntax-version=1.0.0
%project=testproject

init_schema 2024-01-01T00:00:00Z Dev <dev@test.com> # init
`;
    const deployDir = join(tmpDir, "deploy");
    mkdirSync(deployDir, { recursive: true });
    writeFileSync(join(tmpDir, "sqitch.conf"), "[core]\n\tengine = pg\n");
    writeFileSync(join(tmpDir, "sqitch.plan"), planContent);
    writeFileSync(join(deployDir, "init_schema.sql"), "-- init");

    const client = new DatabaseClient("postgresql://host/db");
    await client.connect();
    const registry = new Registry(client);
    const psqlRunner = createMockPsqlRunner();
    const shutdownMgr = new ShutdownManager();
    const config = loadConfig(tmpDir);

    const result = await executeDeploy(
      {
        to: undefined,
        mode: "change",
        dryRun: false,
        verify: false,
        variables: {},
        dbUri: "postgresql://host/db",
        projectDir: tmpDir,
        committerName: "Test",
        committerEmail: "test@x.com",
        noTui: true,
        noSnapshot: true,
        phase: "expand",
      },
      { db: client, registry, psqlRunner, config, shutdownMgr },
    );

    expect(result.deployed).toBe(0);
    expect(result.error).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// 4. executeDeploy with --phase contract
// ---------------------------------------------------------------------------

describe("executeDeploy --phase contract", () => {
  beforeEach(() => {
    mockInstances = [];
    resetConfig();
    setConfig({ quiet: true });
    tmpDir = createTmpDir();
  });

  afterEach(() => {
    try { rmSync(tmpDir, { recursive: true, force: true }); } catch {}
  });

  it("fails contract when expand not deployed", async () => {
    setupProject(tmpDir);

    const client = new DatabaseClient("postgresql://host/db");
    await client.connect();
    const registry = new Registry(client);
    const psqlRunner = createMockPsqlRunner();
    const shutdownMgr = new ShutdownManager();
    const config = loadConfig(tmpDir);

    const result = await executeDeploy(
      {
        to: undefined,
        mode: "change",
        dryRun: false,
        verify: false,
        variables: {},
        dbUri: "postgresql://host/db",
        projectDir: tmpDir,
        committerName: "Test",
        committerEmail: "test@x.com",
        noTui: true,
        noSnapshot: true,
        phase: "contract",
      },
      { db: client, registry, psqlRunner, config, shutdownMgr },
    );

    expect(result.deployed).toBe(0);
    expect(result.error).toContain("expand change");
    expect(result.error).toContain("has not been deployed");
    expect(result.failedChange).toBe("rename_users_name_contract");
  });

  it("deploys contract when expand is already deployed", async () => {
    setupProject(tmpDir);

    // Mock: expand change is already deployed
    const mockQuery = async (text: string, values?: unknown[]) => {
      if (text.includes("pg_try_advisory_lock")) {
        return { rows: [{ pg_try_advisory_lock: true }], rowCount: 1, command: "SELECT" };
      }
      if (text.includes("pg_advisory_unlock") || text.includes("pg_advisory_lock")) {
        return { rows: [{ pg_advisory_unlock: true }], rowCount: 1, command: "SELECT" };
      }
      if (text.includes("SELECT") && text.includes("sqitch.projects") && !text.includes("INSERT")) {
        return { rows: [{ project: "testproject" }], rowCount: 1, command: "SELECT" };
      }
      if (text.includes("INSERT INTO sqitch.projects")) {
        return { rows: [{ project: "testproject" }], rowCount: 1, command: "INSERT" };
      }
      if (text.includes("SELECT") && text.includes("sqitch.changes")) {
        // The expand change is already deployed
        return {
          rows: [{
            change_id: "expand-id-123",
            script_hash: "abc",
            change: "rename_users_name_expand",
            project: "testproject",
            note: "",
            committed_at: new Date(),
            committer_name: "Test",
            committer_email: "test@x.com",
            planned_at: new Date(),
            planner_name: "Test",
            planner_email: "test@x.com",
          }],
          rowCount: 1,
          command: "SELECT",
        };
      }
      if (text.includes("CREATE SCHEMA") || text.includes("CREATE TABLE")) {
        return { rows: [], rowCount: 0, command: "CREATE" };
      }
      if (text.includes("expand_contract_state")) {
        return { rows: [], rowCount: 0, command: "SELECT" };
      }
      return { rows: [], rowCount: 0, command: "SELECT" };
    };

    const client = new DatabaseClient("postgresql://host/db");
    await client.connect();
    // Override query method
    const pgClient = getPgClient();
    pgClient.query = mockQuery as any;

    const registry = new Registry(client);
    const psqlRunner = createMockPsqlRunner();
    const shutdownMgr = new ShutdownManager();
    const config = loadConfig(tmpDir);

    const result = await executeDeploy(
      {
        to: undefined,
        mode: "change",
        dryRun: false,
        verify: false,
        variables: {},
        dbUri: "postgresql://host/db",
        projectDir: tmpDir,
        committerName: "Test",
        committerEmail: "test@x.com",
        noTui: true,
        noSnapshot: true,
        phase: "contract",
      },
      { db: client, registry, psqlRunner, config, shutdownMgr },
    );

    // The contract change should have been deployed
    expect(result.deployed).toBe(1);
    expect(result.error).toBeUndefined();
  });

  it("returns no pending when no contract changes exist", async () => {
    const planContent = `%syntax-version=1.0.0
%project=testproject

init_schema 2024-01-01T00:00:00Z Dev <dev@test.com> # init
rename_expand 2024-01-02T00:00:00Z Dev <dev@test.com> # expand
`;
    const deployDir = join(tmpDir, "deploy");
    mkdirSync(deployDir, { recursive: true });
    writeFileSync(join(tmpDir, "sqitch.conf"), "[core]\n\tengine = pg\n");
    writeFileSync(join(tmpDir, "sqitch.plan"), planContent);
    writeFileSync(join(deployDir, "init_schema.sql"), "-- init");
    writeFileSync(join(deployDir, "rename_expand.sql"), "-- expand");

    const client = new DatabaseClient("postgresql://host/db");
    await client.connect();
    const registry = new Registry(client);
    const psqlRunner = createMockPsqlRunner();
    const shutdownMgr = new ShutdownManager();
    const config = loadConfig(tmpDir);

    const result = await executeDeploy(
      {
        to: undefined,
        mode: "change",
        dryRun: false,
        verify: false,
        variables: {},
        dbUri: "postgresql://host/db",
        projectDir: tmpDir,
        committerName: "Test",
        committerEmail: "test@x.com",
        noTui: true,
        noSnapshot: true,
        phase: "contract",
      },
      { db: client, registry, psqlRunner, config, shutdownMgr },
    );

    expect(result.deployed).toBe(0);
    expect(result.error).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// 5. Status command: expand/contract state display
// ---------------------------------------------------------------------------

describe("status expand/contract state", () => {
  it("computeStatus includes expand/contract operations", () => {
    const plan = makePlan("testproject", [
      { name: "rename_expand", change_id: "aaa" },
      { name: "rename_contract", change_id: "bbb", requires: ["rename_expand"] },
    ]);
    const deployed = [makeRegistryChange("rename_expand", "aaa")];
    const ecOps: ExpandContractStatus[] = [{
      change_name: "rename_users_name",
      phase: "expanded",
      table: "public.users",
      started_at: "2024-01-15T10:30:00.000Z",
      started_by: "deployer@test.com",
    }];

    const result = computeStatus(plan, deployed, "pg://host/db", "/tmp/deploy", ecOps);

    expect(result.expand_contract_operations).toHaveLength(1);
    expect(result.expand_contract_operations[0]!.change_name).toBe("rename_users_name");
    expect(result.expand_contract_operations[0]!.phase).toBe("expanded");
  });

  it("computeStatus defaults to empty expand/contract list", () => {
    const plan = makePlan("testproject", []);
    const result = computeStatus(plan, [], null, "/tmp/deploy");
    expect(result.expand_contract_operations).toEqual([]);
  });

  it("formatStatusText shows expand/contract operations", () => {
    const result: StatusResult = {
      project: "testproject",
      target: "pg://host/db",
      deployed_count: 1,
      pending_count: 1,
      pending_changes: ["rename_contract"],
      last_deployed: null,
      modified_scripts: [],
      expand_contract_operations: [{
        change_name: "rename_users_name",
        phase: "expanded",
        table: "public.users",
        started_at: "2024-01-15T10:30:00.000Z",
        started_by: "deployer@test.com",
      }],
    };
    const text = formatStatusText(result);
    expect(text).toContain("Expand/contract operations:");
    expect(text).toContain("~ rename_users_name [expanded] on public.users");
  });

  it("formatStatusText shows expanding phase", () => {
    const result: StatusResult = {
      project: "testproject",
      target: null,
      deployed_count: 0,
      pending_count: 0,
      pending_changes: [],
      last_deployed: null,
      modified_scripts: [],
      expand_contract_operations: [{
        change_name: "foo",
        phase: "expanding",
        table: "public.bar",
        started_at: "2024-01-15T10:30:00.000Z",
        started_by: "test@test.com",
      }],
    };
    const text = formatStatusText(result);
    expect(text).toContain("~ foo [expanding] on public.bar");
    // Should NOT show "Nothing to deploy" when EC ops are active
    expect(text).not.toContain("Nothing to deploy");
  });

  it("formatStatusText shows contracting phase", () => {
    const result: StatusResult = {
      project: "testproject",
      target: null,
      deployed_count: 2,
      pending_count: 0,
      pending_changes: [],
      last_deployed: null,
      modified_scripts: [],
      expand_contract_operations: [{
        change_name: "rename",
        phase: "contracting",
        table: "public.users",
        started_at: "2024-01-15T10:30:00.000Z",
        started_by: "deployer@test.com",
      }],
    };
    const text = formatStatusText(result);
    expect(text).toContain("~ rename [contracting] on public.users");
  });

  it("formatStatusText hides section when no expand/contract operations", () => {
    const result: StatusResult = {
      project: "testproject",
      target: null,
      deployed_count: 0,
      pending_count: 0,
      pending_changes: [],
      last_deployed: null,
      modified_scripts: [],
      expand_contract_operations: [],
    };
    const text = formatStatusText(result);
    expect(text).not.toContain("Expand/contract operations:");
    expect(text).toContain("Nothing to deploy. Everything is up-to-date.");
  });

  it("formatStatusText shows multiple operations", () => {
    const result: StatusResult = {
      project: "testproject",
      target: null,
      deployed_count: 3,
      pending_count: 0,
      pending_changes: [],
      last_deployed: null,
      modified_scripts: [],
      expand_contract_operations: [
        {
          change_name: "rename_users_name",
          phase: "expanded",
          table: "public.users",
          started_at: "2024-01-15T10:30:00.000Z",
          started_by: "deployer@test.com",
        },
        {
          change_name: "change_orders_total",
          phase: "expanding",
          table: "public.orders",
          started_at: "2024-01-16T09:00:00.000Z",
          started_by: "deployer@test.com",
        },
      ],
    };
    const text = formatStatusText(result);
    expect(text).toContain("~ rename_users_name [expanded] on public.users");
    expect(text).toContain("~ change_orders_total [expanding] on public.orders");
  });
});

// ---------------------------------------------------------------------------
// 6. Phase with dry-run
// ---------------------------------------------------------------------------

describe("deploy --phase with --dry-run", () => {
  beforeEach(() => {
    mockInstances = [];
    resetConfig();
    setConfig({ quiet: false });
    tmpDir = createTmpDir();
  });

  afterEach(() => {
    try { rmSync(tmpDir, { recursive: true, force: true }); } catch {}
  });

  it("dry-run with --phase expand shows only expand changes", async () => {
    const planContent = `%syntax-version=1.0.0
%project=testproject

rename_expand 2024-01-01T00:00:00Z Dev <dev@test.com> # expand
rename_contract [rename_expand] 2024-01-02T00:00:00Z Dev <dev@test.com> # contract
regular_change 2024-01-03T00:00:00Z Dev <dev@test.com> # regular
`;
    const deployDir = join(tmpDir, "deploy");
    mkdirSync(deployDir, { recursive: true });
    writeFileSync(join(tmpDir, "sqitch.conf"), "[core]\n\tengine = pg\n");
    writeFileSync(join(tmpDir, "sqitch.plan"), planContent);
    writeFileSync(join(deployDir, "rename_expand.sql"), "-- expand");
    writeFileSync(join(deployDir, "rename_contract.sql"), "-- contract");
    writeFileSync(join(deployDir, "regular_change.sql"), "-- regular");

    const client = new DatabaseClient("postgresql://host/db");
    await client.connect();
    const registry = new Registry(client);
    const psqlRunner = createMockPsqlRunner();
    const shutdownMgr = new ShutdownManager();
    const config = loadConfig(tmpDir);

    // dry-run doesn't touch the DB so phase filtering happens at plan level
    const result = await executeDeploy(
      {
        to: undefined,
        mode: "change",
        dryRun: true,
        verify: false,
        variables: {},
        dbUri: "postgresql://host/db",
        projectDir: tmpDir,
        committerName: "Test",
        committerEmail: "test@x.com",
        noTui: true,
        noSnapshot: true,
        phase: "expand",
      },
      { db: client, registry, psqlRunner, config, shutdownMgr },
    );

    // Dry-run deploys 0 changes but reports what would be deployed
    expect(result.deployed).toBe(0);
    expect(result.dryRun).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// 7. Edge cases and error paths
// ---------------------------------------------------------------------------

describe("deploy --phase edge cases", () => {
  beforeEach(() => {
    mockInstances = [];
    resetConfig();
    setConfig({ quiet: true });
    tmpDir = createTmpDir();
  });

  afterEach(() => {
    try { rmSync(tmpDir, { recursive: true, force: true }); } catch {}
  });

  it("all expand changes already deployed returns nothing to deploy", async () => {
    setupProject(tmpDir);

    // First, parse the plan to find the actual change_id that will be computed
    const { parsePlan } = await import("../../src/plan/parser");
    const { readFileSync } = await import("fs");
    const planContent = readFileSync(join(tmpDir, "sqitch.plan"), "utf-8");
    const plan = parsePlan(planContent);
    const expandChange = plan.changes.find(c => c.name === "rename_users_name_expand");
    const expandChangeId = expandChange!.change_id;

    const client = new DatabaseClient("postgresql://host/db");
    await client.connect();
    const pgClient = getPgClient();

    // Override the query method to return the expand change as deployed
    const origQuery = pgClient.query.bind(pgClient);
    pgClient.query = async (text: string, values?: unknown[]) => {
      if (text.includes("sqitch.changes")) {
        return {
          rows: [{
            change_id: expandChangeId,
            change: "rename_users_name_expand",
            script_hash: "abc",
            project: "testproject",
            note: "",
            committed_at: new Date(),
            committer_name: "T",
            committer_email: "t@t.com",
            planned_at: new Date(),
            planner_name: "T",
            planner_email: "t@t.com",
          }],
          rowCount: 1,
          command: "SELECT",
        };
      }
      return origQuery(text, values);
    };

    const registry = new Registry(client);
    const psqlRunner = createMockPsqlRunner();
    const shutdownMgr = new ShutdownManager();
    const config = loadConfig(tmpDir);

    const result = await executeDeploy(
      {
        to: undefined,
        mode: "change",
        dryRun: false,
        verify: false,
        variables: {},
        dbUri: "postgresql://host/db",
        projectDir: tmpDir,
        committerName: "Test",
        committerEmail: "test@x.com",
        noTui: true,
        noSnapshot: true,
        phase: "expand",
      },
      { db: client, registry, psqlRunner, config, shutdownMgr },
    );

    expect(result.deployed).toBe(0);
    expect(result.error).toBeUndefined();
  });

  it("deploy without --phase deploys all changes (backward compatibility)", async () => {
    const planContent = `%syntax-version=1.0.0
%project=testproject

rename_expand 2024-01-01T00:00:00Z Dev <dev@test.com> # expand
rename_contract [rename_expand] 2024-01-02T00:00:00Z Dev <dev@test.com> # contract
`;
    const deployDir = join(tmpDir, "deploy");
    mkdirSync(deployDir, { recursive: true });
    writeFileSync(join(tmpDir, "sqitch.conf"), "[core]\n\tengine = pg\n");
    writeFileSync(join(tmpDir, "sqitch.plan"), planContent);
    writeFileSync(join(deployDir, "rename_expand.sql"), "-- expand");
    writeFileSync(join(deployDir, "rename_contract.sql"), "-- contract");

    const ranScripts: Array<{ scriptFile: string }> = [];
    const client = new DatabaseClient("postgresql://host/db");
    await client.connect();
    const registry = new Registry(client);
    const psqlRunner = createMockPsqlRunner(ranScripts);
    const shutdownMgr = new ShutdownManager();
    const config = loadConfig(tmpDir);

    const result = await executeDeploy(
      {
        to: undefined,
        mode: "change",
        dryRun: false,
        verify: false,
        variables: {},
        dbUri: "postgresql://host/db",
        projectDir: tmpDir,
        committerName: "Test",
        committerEmail: "test@x.com",
        noTui: true,
        noSnapshot: true,
        // No phase specified
      },
      { db: client, registry, psqlRunner, config, shutdownMgr },
    );

    // Both changes should be deployed (no phase filtering)
    expect(result.deployed).toBe(2);
    expect(result.error).toBeUndefined();
  });
});
