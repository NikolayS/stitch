// tests/unit/expand-contract-deep.test.ts — Deep end-to-end expand/contract tests
//
// Covers: full lifecycle, trigger correctness, recursion guard,
// phase tracker state machine, and edge cases.
//
// Implements acceptance criteria for issue #127.

import { describe, expect, it, beforeEach, afterEach, mock } from "bun:test";
import {
  mkdtempSync,
  writeFileSync,
  readFileSync,
  existsSync,
  mkdirSync,
  rmSync,
} from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { resetConfig } from "../../src/output";

// ---------------------------------------------------------------------------
// Imports from generator (file-based tests — no mocking needed)
// ---------------------------------------------------------------------------

import {
  deriveChangeNames,
  syncTriggerName,
  syncTriggerFunctionName,
  expandDeployTemplate,
  expandRevertTemplate,
  expandVerifyTemplate,
  contractDeployTemplate,
  contractRevertTemplate,
  contractVerifyTemplate,
  parseExpandArgs,
  validateExpandOptions,
  inferOperation,
  generateExpandContract,
  type ExpandContractConfig,
} from "../../src/expand-contract/generator";

import {
  forwardSyncExpression,
  reverseSyncExpression,
  generateTriggerFunctionBody,
  generateCreateFunction,
  generateCreateTrigger,
  generateCreateSQL,
  generateDropTrigger,
  generateDropFunction,
  generateDropSQL,
  generateSyncTrigger,
  configToTriggerOptions,
  validateTriggerOptions,
  generateSyncTriggerSafe,
  type SyncTriggerOptions,
} from "../../src/expand-contract/triggers";

import {
  isExpandChange,
  isContractChange,
  isExpandContractChange,
  extractBaseName,
  expandChangeName,
  contractChangeName,
  filterExpandChanges,
  filterContractChanges,
} from "../../src/expand-contract/phase-filter";

// ---------------------------------------------------------------------------
// Mock pg/lib/client — for tracker tests
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

  queryResults: Record<
    string,
    | { rows: unknown[]; rowCount: number; command: string }
    | ((text: string, values?: unknown[]) => { rows: unknown[]; rowCount: number; command: string })
  > = {};

  queryErrors: Record<string, Error> = {};

  constructor(options: Record<string, unknown>) {
    this.options = options;
    mockInstances.push(this);
  }

  async connect() {
    this.connected = true;
  }

  async query(text: string, values?: unknown[]) {
    this.queries.push({ text, values });

    if (this.queryErrors[text]) {
      throw this.queryErrors[text];
    }

    const handler = this.queryResults[text];
    if (handler) {
      if (typeof handler === "function") {
        return handler(text, values);
      }
      return handler;
    }

    for (const [key, val] of Object.entries(this.queryResults)) {
      if (text.startsWith(key)) {
        if (typeof val === "function") {
          return val(text, values);
        }
        return val;
      }
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

// Import after mocking
const { DatabaseClient } = await import("../../src/db/client");
const {
  ExpandContractTracker,
  EXPAND_CONTRACT_DDL,
  EC_LOCK_NAMESPACE,
  VALID_TRANSITIONS,
} = await import("../../src/expand-contract/tracker");

import type {
  Phase,
  ExpandContractState,
  BackfillCheckInput,
} from "../../src/expand-contract/tracker";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let tmpDir: string;

function createTmpDir(): string {
  return mkdtempSync(join(tmpdir(), "sqlever-ec-deep-"));
}

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

const TEST_ENV: Record<string, string | undefined> = {
  SQLEVER_USER_NAME: "Test User",
  SQLEVER_USER_EMAIL: "test@example.com",
};

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

function renameConfig(overrides?: Partial<ExpandContractConfig>): ExpandContractConfig {
  return {
    name: "rename_users_name",
    operation: "rename_col",
    table: "public.users",
    oldColumn: "name",
    newColumn: "full_name",
    oldType: "text",
    note: "Rename name to full_name",
    requires: [],
    conflicts: [],
    ...overrides,
  };
}

function renameOpts(overrides?: Partial<SyncTriggerOptions>): SyncTriggerOptions {
  return {
    table: "public.users",
    oldColumn: "name",
    newColumn: "full_name",
    oldType: "text",
    newType: "text",
    ...overrides,
  };
}

function typeChangeOpts(overrides?: Partial<SyncTriggerOptions>): SyncTriggerOptions {
  return {
    table: "public.users",
    oldColumn: "age_text",
    newColumn: "age",
    oldType: "text",
    newType: "integer",
    castForward: "NEW.age_text::integer",
    castReverse: "NEW.age::text",
    ...overrides,
  };
}

async function createConnectedClient(): Promise<InstanceType<typeof DatabaseClient>> {
  const client = new DatabaseClient("postgresql://host/db");
  await client.connect();
  return client;
}

function getPgClient(): MockPgClient {
  return mockInstances[mockInstances.length - 1]!;
}

function queryTexts(pgClient: MockPgClient): string[] {
  return pgClient.queries.map((q) => q.text);
}

function mockStateRow(overrides: Partial<ExpandContractState> = {}): ExpandContractState {
  return {
    id: 1,
    change_name: "rename_users_name",
    project: "myproject",
    phase: "expanding" as Phase,
    table_schema: "public",
    table_name: "users",
    started_at: new Date("2025-06-01T10:00:00Z"),
    updated_at: new Date("2025-06-01T10:00:00Z"),
    started_by: "deployer@example.com",
    ...overrides,
  };
}

// ============================================================================
// 1. Full lifecycle (9 tests)
// ============================================================================

describe("Full lifecycle", () => {
  beforeEach(() => {
    tmpDir = createTmpDir();
  });

  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it("add --expand generates 6 files (deploy/revert/verify x expand/contract)", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    const result = await generateExpandContract({
      name: "rename_users_name",
      note: "Rename name to full_name",
      requires: [],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "public.users",
      oldColumn: "name",
      newColumn: "full_name",
      oldType: "text",
    }, cfg, TEST_ENV);

    expect(result.files.length).toBe(6);

    const expectedFiles = [
      join(tmpDir, "deploy", "rename_users_name_expand.sql"),
      join(tmpDir, "revert", "rename_users_name_expand.sql"),
      join(tmpDir, "verify", "rename_users_name_expand.sql"),
      join(tmpDir, "deploy", "rename_users_name_contract.sql"),
      join(tmpDir, "revert", "rename_users_name_contract.sql"),
      join(tmpDir, "verify", "rename_users_name_contract.sql"),
    ];

    for (const f of expectedFiles) {
      expect(existsSync(f)).toBe(true);
    }
  });

  it("plan has 2 entries with dependency (contract depends on expand)", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    await generateExpandContract({
      name: "rename_users_name",
      note: "test",
      requires: [],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "public.users",
      oldColumn: "name",
      newColumn: "full_name",
    }, cfg, TEST_ENV);

    const plan = readFileSync(join(tmpDir, "sqitch.plan"), "utf-8");
    const changeLines = plan.split("\n").filter(l => !l.startsWith("%") && l.trim() !== "");
    expect(changeLines.length).toBe(2);
    expect(changeLines[0]).toMatch(/^rename_users_name_expand\s/);
    expect(changeLines[1]).toMatch(/^rename_users_name_contract\s/);
    // Contract must depend on expand
    expect(changeLines[1]).toContain("[rename_users_name_expand]");
  });

  it("--phase expand filters correctly (only _expand changes pass)", () => {
    const changes = [
      { name: "init_schema", change_id: "a", project: "p", note: "", planner_name: "T", planner_email: "t@t", planned_at: "2024-01-01T00:00:00Z", requires: [] as string[], conflicts: [] as string[] },
      { name: "rename_users_name_expand", change_id: "b", project: "p", note: "", planner_name: "T", planner_email: "t@t", planned_at: "2024-01-02T00:00:00Z", requires: [] as string[], conflicts: [] as string[] },
      { name: "rename_users_name_contract", change_id: "c", project: "p", note: "", planner_name: "T", planner_email: "t@t", planned_at: "2024-01-03T00:00:00Z", requires: ["rename_users_name_expand"] as string[], conflicts: [] as string[] },
    ];
    const expandOnly = filterExpandChanges(changes);
    expect(expandOnly).toHaveLength(1);
    expect(expandOnly[0]!.name).toBe("rename_users_name_expand");
  });

  it("--phase contract filters correctly (only _contract changes pass)", () => {
    const changes = [
      { name: "init_schema", change_id: "a", project: "p", note: "", planner_name: "T", planner_email: "t@t", planned_at: "2024-01-01T00:00:00Z", requires: [] as string[], conflicts: [] as string[] },
      { name: "rename_users_name_expand", change_id: "b", project: "p", note: "", planner_name: "T", planner_email: "t@t", planned_at: "2024-01-02T00:00:00Z", requires: [] as string[], conflicts: [] as string[] },
      { name: "rename_users_name_contract", change_id: "c", project: "p", note: "", planner_name: "T", planner_email: "t@t", planned_at: "2024-01-03T00:00:00Z", requires: ["rename_users_name_expand"] as string[], conflicts: [] as string[] },
    ];
    const contractOnly = filterContractChanges(changes);
    expect(contractOnly).toHaveLength(1);
    expect(contractOnly[0]!.name).toBe("rename_users_name_contract");
  });

  it("contract blocked before expand (dependency enforced in plan)", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    const result = await generateExpandContract({
      name: "rename_users_name",
      note: "",
      requires: [],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "public.users",
      oldColumn: "name",
      newColumn: "full_name",
    }, cfg, TEST_ENV);

    // Contract always requires expand
    expect(result.contractChange.requires).toContain("rename_users_name_expand");
    // The contract's parent is the expand change_id
    expect(result.contractChange.parent).toBe(result.expandChange.change_id);
  });

  it("expand deploy creates column + trigger", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    await generateExpandContract({
      name: "rename_users_name",
      note: "",
      requires: [],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "public.users",
      oldColumn: "name",
      newColumn: "full_name",
      oldType: "text",
    }, cfg, TEST_ENV);

    const deploySql = readFileSync(join(tmpDir, "deploy", "rename_users_name_expand.sql"), "utf-8");
    // Column creation
    expect(deploySql).toContain("ALTER TABLE public.users ADD COLUMN full_name text");
    // Trigger function creation
    expect(deploySql).toContain("CREATE OR REPLACE FUNCTION sqlever_sync_fn_users_name_full_name()");
    // Trigger creation
    expect(deploySql).toContain("CREATE TRIGGER sqlever_sync_users_name_full_name");
    expect(deploySql).toContain("BEFORE INSERT OR UPDATE ON public.users");
  });

  it("contract deploy drops old column + trigger", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    await generateExpandContract({
      name: "rename_users_name",
      note: "",
      requires: [],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "public.users",
      oldColumn: "name",
      newColumn: "full_name",
    }, cfg, TEST_ENV);

    const contractSql = readFileSync(join(tmpDir, "deploy", "rename_users_name_contract.sql"), "utf-8");
    // Drops trigger
    expect(contractSql).toContain("DROP TRIGGER IF EXISTS sqlever_sync_users_name_full_name");
    // Drops function
    expect(contractSql).toContain("DROP FUNCTION IF EXISTS sqlever_sync_fn_users_name_full_name()");
    // Drops old column
    expect(contractSql).toContain("ALTER TABLE public.users DROP COLUMN name");
  });

  it("expand and contract changes have distinct change_ids", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    const result = await generateExpandContract({
      name: "rename_users_name",
      note: "",
      requires: [],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "public.users",
      oldColumn: "name",
      newColumn: "full_name",
    }, cfg, TEST_ENV);

    expect(result.expandChange.change_id).toBeTruthy();
    expect(result.contractChange.change_id).toBeTruthy();
    expect(result.expandChange.change_id).not.toBe(result.contractChange.change_id);
  });

  it("expand change note has [expand] prefix, contract has [contract]", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    const result = await generateExpandContract({
      name: "rename_users_name",
      note: "Rename column",
      requires: [],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "public.users",
      oldColumn: "name",
      newColumn: "full_name",
    }, cfg, TEST_ENV);

    expect(result.expandChange.note).toMatch(/^\[expand\]/);
    expect(result.contractChange.note).toMatch(/^\[contract\]/);
  });
});

// ============================================================================
// 2. Trigger correctness (6 tests)
// ============================================================================

describe("Trigger correctness", () => {
  it("INSERT forward sync: old column set populates new column", () => {
    const body = generateTriggerFunctionBody(renameOpts());
    // On INSERT when new column IS NULL, sync from old
    expect(body).toContain("IF NEW.full_name IS NULL THEN");
    expect(body).toContain("NEW.full_name := NEW.name");
  });

  it("INSERT reverse sync: new column set populates old column", () => {
    const body = generateTriggerFunctionBody(renameOpts());
    // On INSERT when old column IS NULL and new IS NOT NULL, sync from new
    expect(body).toContain("IF NEW.name IS NULL AND NEW.full_name IS NOT NULL THEN");
    expect(body).toContain("NEW.name := NEW.full_name");
  });

  it("UPDATE forward sync: change in old column propagates to new", () => {
    const body = generateTriggerFunctionBody(renameOpts());
    expect(body).toContain("IF NEW.name IS DISTINCT FROM OLD.name THEN");
    expect(body).toContain("NEW.full_name :=");
  });

  it("UPDATE reverse sync: change in new column propagates to old", () => {
    const body = generateTriggerFunctionBody(renameOpts());
    expect(body).toContain("ELSIF NEW.full_name IS DISTINCT FROM OLD.full_name THEN");
    expect(body).toContain("NEW.name :=");
  });

  it("NULL handling: NULL stays NULL (no coalesce without default)", () => {
    const body = generateTriggerFunctionBody(renameOpts());
    // Without defaults, direct column copy preserves NULL
    // The sync expression is "NEW.name" / "NEW.full_name" (no COALESCE wrapper)
    const fwd = forwardSyncExpression(renameOpts());
    const rev = reverseSyncExpression(renameOpts());
    expect(fwd).toBe("NEW.name");
    expect(rev).toBe("NEW.full_name");
    // No COALESCE in the body when no defaults are configured
    expect(body).not.toContain("COALESCE");
  });

  it("type conversion with cast expression", () => {
    const body = generateTriggerFunctionBody(typeChangeOpts());
    // Forward cast: text -> integer
    expect(body).toContain("NEW.age := (NEW.age_text::integer)");
    // Reverse cast: integer -> text
    expect(body).toContain("NEW.age_text := (NEW.age::text)");
    // Verify the expression wrappers
    const fwd = forwardSyncExpression(typeChangeOpts());
    const rev = reverseSyncExpression(typeChangeOpts());
    expect(fwd).toBe("(NEW.age_text::integer)");
    expect(rev).toBe("(NEW.age::text)");
  });
});

// ============================================================================
// 3. Recursion guard (4 tests)
// ============================================================================

describe("Recursion guard", () => {
  it("pg_trigger_depth() in generated SQL prevents infinite loops", () => {
    const body = generateTriggerFunctionBody(renameOpts());
    expect(body).toContain("pg_trigger_depth() < 2");
    // Also present in the full expand deploy template
    const deploySql = expandDeployTemplate(renameConfig());
    expect(deploySql).toContain("pg_trigger_depth() < 2");
  });

  it("sqlever_sync_ prefix on all generated trigger names", () => {
    const trigName = syncTriggerName("public.users", "name", "full_name");
    expect(trigName).toMatch(/^sqlever_sync_/);

    const result = generateSyncTrigger(renameOpts());
    expect(result.triggerName).toMatch(/^sqlever_sync_/);
    // Function name also follows convention
    expect(result.functionName).toMatch(/^sqlever_sync_fn_/);
  });

  it("no infinite loop on bidirectional: depth guard ensures single cascade", () => {
    const body = generateTriggerFunctionBody(renameOpts());
    // The guard allows depth 0 (user DML) and depth 1 (first cascade),
    // but blocks depth >= 2 (recursive cascade).
    expect(body).toContain("pg_trigger_depth() < 2");
    // Verify the comment explains the mechanism
    expect(body).toContain("Recursion guard");
    expect(body).toContain("depth >= 2");
  });

  it("multiple sync pairs on different tables are independent", () => {
    const t1 = generateSyncTrigger(renameOpts({ table: "public.users" }));
    const t2 = generateSyncTrigger(renameOpts({
      table: "public.orders",
      oldColumn: "status",
      newColumn: "order_status",
    }));

    // Distinct trigger names ensure independent firing
    expect(t1.triggerName).not.toBe(t2.triggerName);
    expect(t1.functionName).not.toBe(t2.functionName);

    // Both use the same recursion guard mechanism
    expect(t1.createSQL).toContain("pg_trigger_depth() < 2");
    expect(t2.createSQL).toContain("pg_trigger_depth() < 2");

    // Trigger on users does not reference orders and vice versa
    expect(t1.createSQL).toContain("ON public.users");
    expect(t1.createSQL).not.toContain("ON public.orders");
    expect(t2.createSQL).toContain("ON public.orders");
    expect(t2.createSQL).not.toContain("ON public.users");
  });
});

// ============================================================================
// 4. Phase tracker (6 tests)
// ============================================================================

describe("Phase tracker", () => {
  beforeEach(() => {
    mockInstances = [];
    resetConfig();
  });

  it("all valid transitions tested: expanding->expanded->contracting->completed", async () => {
    const db = await createConnectedClient();
    const tracker = new ExpandContractTracker(db);

    // Every valid forward transition should not throw
    expect(() => tracker.validateTransition("expanding", "expanded")).not.toThrow();
    expect(() => tracker.validateTransition("expanded", "contracting")).not.toThrow();
    expect(() => tracker.validateTransition("contracting", "completed")).not.toThrow();

    // Verify the VALID_TRANSITIONS map is exhaustive
    expect(Object.keys(VALID_TRANSITIONS)).toHaveLength(4);
    expect(VALID_TRANSITIONS.expanding).toEqual(["expanded"]);
    expect(VALID_TRANSITIONS.expanded).toEqual(["contracting"]);
    expect(VALID_TRANSITIONS.contracting).toEqual(["completed"]);
    expect(VALID_TRANSITIONS.completed).toEqual([]);
  });

  it("all invalid transitions rejected (backward, skip, cycle)", async () => {
    const db = await createConnectedClient();
    const tracker = new ExpandContractTracker(db);

    // Backward transitions
    expect(() => tracker.validateTransition("expanded", "expanding")).toThrow(/Invalid phase transition/);
    expect(() => tracker.validateTransition("contracting", "expanded")).toThrow(/Invalid phase transition/);
    expect(() => tracker.validateTransition("completed", "contracting")).toThrow(/Invalid phase transition/);

    // Skip transitions
    expect(() => tracker.validateTransition("expanding", "contracting")).toThrow(/Invalid phase transition/);
    expect(() => tracker.validateTransition("expanding", "completed")).toThrow(/Invalid phase transition/);
    expect(() => tracker.validateTransition("expanded", "completed")).toThrow(/Invalid phase transition/);

    // Cycle transitions
    expect(() => tracker.validateTransition("completed", "expanding")).toThrow(/Invalid phase transition/);
    expect(() => tracker.validateTransition("completed", "expanded")).toThrow(/Invalid phase transition/);

    // Self-transitions
    expect(() => tracker.validateTransition("expanding", "expanding")).toThrow(/Invalid phase transition/);
    expect(() => tracker.validateTransition("expanded", "expanded")).toThrow(/Invalid phase transition/);
  });

  it("advisory lock protects transitions from concurrent access", async () => {
    const db = await createConnectedClient();
    const tracker = new ExpandContractTracker(db);
    const pgClient = getPgClient();

    // Lock NOT acquired (another process holds it)
    pgClient.queryResults["SELECT pg_try_advisory_lock($1, $2)"] = {
      rows: [{ pg_try_advisory_lock: false }],
      rowCount: 1,
      command: "SELECT",
    };

    await expect(tracker.transitionPhase(1, "expanded")).rejects.toThrow(
      /another process is currently performing a phase transition/,
    );

    // Verify the lock was attempted with correct namespace
    const lockQuery = pgClient.queries.find((q) =>
      q.text === "SELECT pg_try_advisory_lock($1, $2)",
    );
    expect(lockQuery?.values).toEqual([EC_LOCK_NAMESPACE, 1]);
  });

  it("backfill verification: incomplete -> error prevents contracting", async () => {
    const db = await createConnectedClient();
    const tracker = new ExpandContractTracker(db);
    const pgClient = getPgClient();

    pgClient.queryResults["SELECT pg_try_advisory_lock($1, $2)"] = {
      rows: [{ pg_try_advisory_lock: true }],
      rowCount: 1,
      command: "SELECT",
    };

    pgClient.queryResults["SELECT id, change_name, project, phase"] = {
      rows: [mockStateRow({ id: 1, phase: "expanded" as Phase })],
      rowCount: 1,
      command: "SELECT",
    };

    // Backfill incomplete: 80 of 100 rows done
    pgClient.queryResults["SELECT COUNT(*)::int AS cnt FROM"] = (text: string) => {
      if (text.includes("IS NOT NULL")) {
        return { rows: [{ cnt: 80 }], rowCount: 1, command: "SELECT" };
      }
      return { rows: [{ cnt: 100 }], rowCount: 1, command: "SELECT" };
    };

    const backfillCheck: BackfillCheckInput = {
      table_schema: "public",
      table_name: "users",
      new_column: "full_name",
    };

    await expect(
      tracker.transitionPhase(1, "contracting", backfillCheck),
    ).rejects.toThrow(/backfill is not complete.*80\/100/);

    // Lock should still be released
    const texts = queryTexts(pgClient);
    expect(texts).toContain("SELECT pg_advisory_unlock($1, $2)");
  });

  it("backfill verification: complete -> proceed to contracting", async () => {
    const db = await createConnectedClient();
    const tracker = new ExpandContractTracker(db);
    const pgClient = getPgClient();

    pgClient.queryResults["SELECT pg_try_advisory_lock($1, $2)"] = {
      rows: [{ pg_try_advisory_lock: true }],
      rowCount: 1,
      command: "SELECT",
    };

    pgClient.queryResults["SELECT id, change_name, project, phase"] = {
      rows: [mockStateRow({ id: 1, phase: "expanded" as Phase })],
      rowCount: 1,
      command: "SELECT",
    };

    // Backfill complete: 100 of 100 rows done
    pgClient.queryResults["SELECT COUNT(*)::int AS cnt FROM"] = {
      rows: [{ cnt: 100 }],
      rowCount: 1,
      command: "SELECT",
    };

    pgClient.queryResults["UPDATE sqlever.expand_contract_state"] = {
      rows: [mockStateRow({ id: 1, phase: "contracting" as Phase })],
      rowCount: 1,
      command: "UPDATE",
    };

    const backfillCheck: BackfillCheckInput = {
      table_schema: "public",
      table_name: "users",
      new_column: "full_name",
    };

    const result = await tracker.transitionPhase(1, "contracting", backfillCheck);
    expect(result.phase).toBe("contracting");
  });

  it("concurrent transition detection: lock contention with correct error message", async () => {
    const db = await createConnectedClient();
    const tracker = new ExpandContractTracker(db);
    const pgClient = getPgClient();

    // First attempt: lock acquired
    pgClient.queryResults["SELECT pg_try_advisory_lock($1, $2)"] = {
      rows: [{ pg_try_advisory_lock: true }],
      rowCount: 1,
      command: "SELECT",
    };

    pgClient.queryResults["SELECT id, change_name, project, phase"] = {
      rows: [mockStateRow({ id: 1, phase: "expanding" as Phase })],
      rowCount: 1,
      command: "SELECT",
    };

    pgClient.queryResults["UPDATE sqlever.expand_contract_state"] = {
      rows: [mockStateRow({ id: 1, phase: "expanded" as Phase })],
      rowCount: 1,
      command: "UPDATE",
    };

    // Should succeed
    const result = await tracker.transitionPhase(1, "expanded");
    expect(result.phase).toBe("expanded");

    // Now simulate contention: lock NOT acquired
    pgClient.queryResults["SELECT pg_try_advisory_lock($1, $2)"] = {
      rows: [{ pg_try_advisory_lock: false }],
      rowCount: 1,
      command: "SELECT",
    };

    // Second attempt should fail with lock contention error
    await expect(tracker.transitionPhase(1, "expanded")).rejects.toThrow(
      /another process/,
    );
  });
});

// ============================================================================
// 5. Edge cases (5 tests)
// ============================================================================

describe("Edge cases", () => {
  beforeEach(() => {
    tmpDir = createTmpDir();
    mockInstances = [];
    resetConfig();
  });

  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it("schema-qualified tables: trigger name strips schema, SQL uses full name", () => {
    const opts = renameOpts({ table: "myschema.accounts" });
    const result = generateSyncTrigger(opts);

    // Trigger name strips schema
    expect(result.triggerName).toBe("sqlever_sync_accounts_name_full_name");
    expect(result.triggerName).not.toContain("myschema");

    // SQL uses the full schema-qualified table
    expect(result.createSQL).toContain("ON myschema.accounts");
    expect(result.dropSQL).toContain("ON myschema.accounts");

    // Expand deploy template also handles schema-qualified tables
    const deploySql = expandDeployTemplate(renameConfig({ table: "myschema.accounts" }));
    expect(deploySql).toContain("ALTER TABLE myschema.accounts ADD COLUMN");
    expect(deploySql).toContain("BEFORE INSERT OR UPDATE ON myschema.accounts");
  });

  it("partitioned tables: trigger on parent (PG 14+ inherits to partitions)", () => {
    const opts = renameOpts({ table: "sales.orders_partitioned" });
    const result = generateSyncTrigger(opts);

    // Trigger installed on the parent table
    expect(result.createSQL).toContain("ON sales.orders_partitioned");
    // Comment in source confirms inheritance per SPEC 5.4 point 3
    const trigSQL = generateCreateTrigger(opts);
    expect(trigSQL).toContain("ON sales.orders_partitioned");

    // Drop also targets parent
    expect(result.dropSQL).toContain("ON sales.orders_partitioned");

    // Trigger name uses the table name without schema
    expect(result.triggerName).toBe("sqlever_sync_orders_partitioned_name_full_name");
  });

  it("rename vs change_type operations inferred correctly", () => {
    // Rename: same type or no type specified
    const renameOp = inferOperation({
      name: "x", note: "", requires: [], conflicts: [], noVerify: false,
      expand: true, table: "t", oldColumn: "a", newColumn: "b",
    });
    expect(renameOp).toBe("rename_col");

    const renameOp2 = inferOperation({
      name: "x", note: "", requires: [], conflicts: [], noVerify: false,
      expand: true, table: "t", oldColumn: "a", newColumn: "b",
      oldType: "text", newType: "text",
    });
    expect(renameOp2).toBe("rename_col");

    // Change type: different types specified
    const changeTypeOp = inferOperation({
      name: "x", note: "", requires: [], conflicts: [], noVerify: false,
      expand: true, table: "t", oldColumn: "a", newColumn: "b",
      oldType: "varchar", newType: "text",
    });
    expect(changeTypeOp).toBe("change_type");
  });

  it("--cast-forward/--cast-reverse parsed and used in generated SQL", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    await generateExpandContract({
      name: "change_users_age",
      note: "Convert age from text to integer",
      requires: [],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "public.users",
      oldColumn: "age",
      newColumn: "age_int",
      oldType: "text",
      newType: "integer",
      castForward: "NEW.age::integer",
      castReverse: "NEW.age_int::text",
    }, cfg, TEST_ENV);

    const expandSql = readFileSync(join(tmpDir, "deploy", "change_users_age_expand.sql"), "utf-8");
    expect(expandSql).toContain("ADD COLUMN age_int integer");
    expect(expandSql).toContain("(NEW.age::integer)");
    expect(expandSql).toContain("(NEW.age_int::text)");

    // Parse verifies the args are extracted correctly
    const opts = parseExpandArgs([
      "change_users_age", "--expand",
      "--table", "public.users",
      "--old-column", "age",
      "--new-column", "age_int",
      "--cast-forward", "NEW.age::integer",
      "--cast-reverse", "NEW.age_int::text",
    ]);
    expect(opts.castForward).toBe("NEW.age::integer");
    expect(opts.castReverse).toBe("NEW.age_int::text");
  });

  it("contract revert reinstalls old column + trigger", () => {
    const revertSql = contractRevertTemplate(renameConfig());

    // Re-adds old column with correct type
    expect(revertSql).toContain("ALTER TABLE public.users ADD COLUMN name text");

    // Re-creates the sync trigger function
    expect(revertSql).toContain("CREATE OR REPLACE FUNCTION sqlever_sync_fn_users_name_full_name()");
    // Includes recursion guard in re-created function
    expect(revertSql).toContain("pg_trigger_depth() < 2");

    // Re-installs the trigger
    expect(revertSql).toContain("CREATE TRIGGER sqlever_sync_users_name_full_name");
    expect(revertSql).toContain("BEFORE INSERT OR UPDATE ON public.users");

    // Backfills old column from new
    expect(revertSql).toContain("UPDATE public.users SET name = full_name");
    expect(revertSql).toContain("WHERE full_name IS NOT NULL");
  });
});

// ============================================================================
// Additional cross-cutting tests to reach >= 30
// ============================================================================

describe("Cross-cutting: template content correctness", () => {
  it("expand deploy wraps in transaction (BEGIN/COMMIT)", () => {
    const sql = expandDeployTemplate(renameConfig());
    expect(sql).toContain("BEGIN;");
    expect(sql).toContain("COMMIT;");
  });

  it("expand revert drops in correct order: trigger, function, column", () => {
    const sql = expandRevertTemplate(renameConfig());
    const trigIdx = sql.indexOf("DROP TRIGGER");
    const fnIdx = sql.indexOf("DROP FUNCTION");
    const colIdx = sql.indexOf("DROP COLUMN");
    expect(trigIdx).toBeLessThan(fnIdx);
    expect(fnIdx).toBeLessThan(colIdx);
  });

  it("expand verify uses ROLLBACK (not COMMIT) to avoid side effects", () => {
    const sql = expandVerifyTemplate(renameConfig());
    expect(sql).toContain("ROLLBACK;");
    expect(sql).not.toContain("COMMIT;");
  });

  it("contract deploy verifies backfill before dropping column", () => {
    const sql = contractDeployTemplate(renameConfig());
    const backfillIdx = sql.indexOf("Backfill incomplete");
    const dropIdx = sql.indexOf("DROP COLUMN");
    // Backfill check must come before drop
    expect(backfillIdx).toBeLessThan(dropIdx);
  });

  it("contract verify confirms old column is gone and new column exists", () => {
    const sql = contractVerifyTemplate(renameConfig());
    expect(sql).toContain("column_name = 'name'");
    expect(sql).toContain("Old column name still exists");
    expect(sql).toContain("SELECT full_name FROM public.users WHERE false");
  });
});

describe("Cross-cutting: phase filter edge cases", () => {
  it("extractBaseName returns null for non-expand/contract names", () => {
    expect(extractBaseName("regular_change")).toBeNull();
    expect(extractBaseName("expand_things")).toBeNull();
    expect(extractBaseName("contract_first")).toBeNull();
  });

  it("extractBaseName extracts correctly for both suffixes", () => {
    expect(extractBaseName("foo_expand")).toBe("foo");
    expect(extractBaseName("foo_contract")).toBe("foo");
    expect(extractBaseName("multi_word_name_expand")).toBe("multi_word_name");
    expect(extractBaseName("multi_word_name_contract")).toBe("multi_word_name");
  });

  it("isExpandContractChange identifies both sides of a pair", () => {
    expect(isExpandContractChange("rename_expand")).toBe(true);
    expect(isExpandContractChange("rename_contract")).toBe(true);
    expect(isExpandContractChange("regular")).toBe(false);
  });

  it("expandChangeName and contractChangeName are inverse of extractBaseName", () => {
    const base = "rename_users_name";
    expect(extractBaseName(expandChangeName(base))).toBe(base);
    expect(extractBaseName(contractChangeName(base))).toBe(base);
  });
});

describe("Cross-cutting: tracker DDL and schema", () => {
  beforeEach(() => {
    mockInstances = [];
    resetConfig();
  });

  it("DDL creates sqlever schema and table with all required columns", () => {
    expect(EXPAND_CONTRACT_DDL).toContain("CREATE SCHEMA IF NOT EXISTS sqlever");
    expect(EXPAND_CONTRACT_DDL).toContain("CREATE TABLE IF NOT EXISTS sqlever.expand_contract_state");
    for (const col of ["id", "change_name", "project", "phase", "table_schema", "table_name", "started_at", "updated_at", "started_by"]) {
      expect(EXPAND_CONTRACT_DDL).toContain(col);
    }
    expect(EXPAND_CONTRACT_DDL).toContain("UNIQUE (project, change_name)");
  });

  it("ensureSchema acquires and releases advisory lock even on failure", async () => {
    const db = await createConnectedClient();
    const tracker = new ExpandContractTracker(db);
    const pgClient = getPgClient();

    pgClient.queryErrors[EXPAND_CONTRACT_DDL] = new Error("DDL failed");

    await expect(tracker.ensureSchema()).rejects.toThrow("DDL failed");

    const texts = queryTexts(pgClient);
    expect(texts).toContain("SELECT pg_advisory_lock($1)");
    expect(texts).toContain("SELECT pg_advisory_unlock($1)");
  });
});
