// tests/integration/customer-zero.test.ts — Customer-zero plan validation
//
// Validates two things:
//   1. The customer-zero plan file (255 changes from PostgresAI Console)
//      parses successfully with correct structure.
//   2. A representative 5-migration test project deploys against real PG
//      and the tracking state (sqitch.changes, sqitch.events, sqitch.dependencies)
//      is correct after deployment.
//
// The 5 test migrations exercise different patterns:
//   1. Simple CREATE TABLE
//   2. CREATE FUNCTION (multi-statement)
//   3. Migration with a dependency (FK)
//   4. Migration that would use \i (inline SQL instead)
//   5. Migration with a note containing special characters
//
// Prerequisites:
//   - PostgreSQL reachable at localhost:5417 (docker compose up)
//   - Password: test, user: postgres
//
// See: https://github.com/NikolayS/sqlever/issues/75

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { readFileSync } from "node:fs";
import { resolve, join } from "node:path";
import { cp } from "node:fs/promises";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";

import {
  setupTestDb,
  teardownTestDb,
  queryDb,
  pgUri,
  runSqlever,
} from "./helpers";
import { parsePlan } from "../../src/plan/parser";
import type { Plan, Change } from "../../src/plan/types";

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

const FIXTURES_DIR = resolve(
  new URL("../fixtures", import.meta.url).pathname,
);
const CUSTOMER_ZERO_PLAN = join(FIXTURES_DIR, "customer-zero.plan");
const TEST_PROJECT_DIR = join(FIXTURES_DIR, "customer-zero-project");

// ---------------------------------------------------------------------------
// Section 1: Parse the customer-zero plan (255 changes)
// ---------------------------------------------------------------------------

describe("customer-zero plan parsing", () => {
  let plan: Plan;

  test("parses the full 255-change plan without errors", () => {
    const content = readFileSync(CUSTOMER_ZERO_PLAN, "utf-8");
    plan = parsePlan(content);

    expect(plan.changes).toHaveLength(255);
    expect(plan.tags).toHaveLength(0);
  });

  test("extracts correct project metadata", () => {
    const content = readFileSync(CUSTOMER_ZERO_PLAN, "utf-8");
    plan = parsePlan(content);

    expect(plan.project.name).toBe("postgres_ai");
    expect(plan.project.uri).toBe(
      "https://gitlab.com/postgres-ai/platform/",
    );
    expect(plan.pragmas.get("syntax-version")).toBe("1.0.0");
  });

  test("first change has correct fields", () => {
    const content = readFileSync(CUSTOMER_ZERO_PLAN, "utf-8");
    plan = parsePlan(content);

    const first = plan.changes[0]!;
    expect(first.name).toBe("20190726_init_api");
    expect(first.note).toBe("Init Rest API");
    expect(first.planned_at).toBe("2019-07-26T13:24:32Z");
    expect(first.planner_name).toBe("Dmitry,Udalov,,");
    expect(first.planner_email).toBe("dmius@dev");
    expect(first.requires).toEqual([]);
    expect(first.conflicts).toEqual([]);
    // First change has no parent
    expect(first.parent).toBeUndefined();
  });

  test("last change has correct fields", () => {
    const content = readFileSync(CUSTOMER_ZERO_PLAN, "utf-8");
    plan = parsePlan(content);

    const last = plan.changes[254]!;
    expect(last.name).toBe(
      "20260311_fix_telemetry_usage_billing_cycle_anchor_cast",
    );
    expect(last.note).toBe(
      "Fix billing_cycle_anchor: cast epoch to int for Stripe API compatibility",
    );
    // Last change has a parent (the 254th change)
    expect(last.parent).toBeDefined();
    expect(last.parent).toBe(plan.changes[253]!.change_id);
  });

  test("all 255 changes have non-empty change_id", () => {
    const content = readFileSync(CUSTOMER_ZERO_PLAN, "utf-8");
    plan = parsePlan(content);

    for (const change of plan.changes) {
      expect(change.change_id).toBeTruthy();
      // SHA-1 hex = 40 chars
      expect(change.change_id).toHaveLength(40);
    }
  });

  test("all change_ids are unique", () => {
    const content = readFileSync(CUSTOMER_ZERO_PLAN, "utf-8");
    plan = parsePlan(content);

    const ids = plan.changes.map((c) => c.change_id);
    const unique = new Set(ids);
    expect(unique.size).toBe(255);
  });

  test("parent chain is correctly linked", () => {
    const content = readFileSync(CUSTOMER_ZERO_PLAN, "utf-8");
    plan = parsePlan(content);

    // First change: no parent
    expect(plan.changes[0]!.parent).toBeUndefined();

    // Every subsequent change's parent is the previous change's ID
    for (let i = 1; i < plan.changes.length; i++) {
      expect(plan.changes[i]!.parent).toBe(
        plan.changes[i - 1]!.change_id,
      );
    }
  });

  test("handles planner names with commas", () => {
    const content = readFileSync(CUSTOMER_ZERO_PLAN, "utf-8");
    plan = parsePlan(content);

    // Several early changes have "Dmitry,Udalov,," as planner_name
    const first = plan.changes[0]!;
    expect(first.planner_name).toBe("Dmitry,Udalov,,");
  });

  test("handles notes with escaped characters", () => {
    const content = readFileSync(CUSTOMER_ZERO_PLAN, "utf-8");
    plan = parsePlan(content);

    // Change 44 (0-indexed 39) has a note with literal \n in it
    const billingImprovements = plan.changes.find(
      (c) => c.name === "20200907_billing_impovements",
    );
    expect(billingImprovements).toBeDefined();
    // The note contains a literal \n (two chars: backslash + n), not a newline
    expect(billingImprovements!.note).toContain("\\n");
  });

  test("handles notes with hash-without-space separator", () => {
    const content = readFileSync(CUSTOMER_ZERO_PLAN, "utf-8");
    plan = parsePlan(content);

    // Line like: 20191114_dblab ... <dmius@postgres.ai># DB Lab tables...
    // The parser should handle # preceded by > without space
    const dblab = plan.changes.find((c) => c.name === "20191114_dblab");
    expect(dblab).toBeDefined();
    expect(dblab!.note).toBe("DB Lab tables and API calls");
  });

  test("handles empty planner names", () => {
    const content = readFileSync(CUSTOMER_ZERO_PLAN, "utf-8");
    plan = parsePlan(content);

    // Some changes have empty planner names like:
    // "20251013_toggle_ai_models ... <sqitch@01157dfe3b0b>"
    const toggleAi = plan.changes.find(
      (c) => c.name === "20251013_toggle_ai_models",
    );
    expect(toggleAi).toBeDefined();
    // Planner name may be empty or whitespace-only
    expect(toggleAi!.planner_email).toBe("sqitch@01157dfe3b0b");
  });
});

// ---------------------------------------------------------------------------
// Section 2: Parse the 5-migration test project plan
// ---------------------------------------------------------------------------

describe("test project plan parsing", () => {
  let plan: Plan;

  test("parses the 5-change test plan", () => {
    const content = readFileSync(
      join(TEST_PROJECT_DIR, "sqitch.plan"),
      "utf-8",
    );
    plan = parsePlan(content);

    expect(plan.changes).toHaveLength(5);
    expect(plan.project.name).toBe("customer_zero_test");
    expect(plan.project.uri).toBe(
      "https://example.com/customer-zero-test",
    );
  });

  test("dependencies are parsed correctly", () => {
    const content = readFileSync(
      join(TEST_PROJECT_DIR, "sqitch.plan"),
      "utf-8",
    );
    plan = parsePlan(content);

    // create_users: no deps
    expect(plan.changes[0]!.requires).toEqual([]);

    // create_audit_func: requires create_users
    expect(plan.changes[1]!.requires).toEqual(["create_users"]);

    // add_user_profiles: requires create_users
    expect(plan.changes[2]!.requires).toEqual(["create_users"]);

    // seed_initial_data: requires create_users AND add_user_profiles
    expect(plan.changes[3]!.requires).toEqual([
      "create_users",
      "add_user_profiles",
    ]);

    // add_user_notes: requires create_users
    expect(plan.changes[4]!.requires).toEqual(["create_users"]);
  });

  test("note with special characters is preserved", () => {
    const content = readFileSync(
      join(TEST_PROJECT_DIR, "sqitch.plan"),
      "utf-8",
    );
    plan = parsePlan(content);

    const notes = plan.changes[4]!;
    expect(notes.note).toContain('"quotes"');
    expect(notes.note).toContain("\\n\\t");
    expect(notes.note).toContain("& ampersands");
    expect(notes.note).toContain("<angle>");
  });
});

// ---------------------------------------------------------------------------
// Section 3: Deploy test project against real PG and verify tracking
// ---------------------------------------------------------------------------

describe("customer-zero deploy against real PG", () => {
  let dbName: string;
  let tmpDir: string;

  beforeEach(async () => {
    dbName = await setupTestDb();
    // Copy fixture project to a temp dir so tests are isolated
    tmpDir = await mkdtemp(join(tmpdir(), "cz-deploy-"));
    await cp(TEST_PROJECT_DIR, tmpDir, { recursive: true });
  });

  afterEach(async () => {
    await teardownTestDb(dbName);
    await rm(tmpDir, { recursive: true, force: true });
  });

  test("deploys all 5 changes successfully", async () => {
    const dbUri = pgUri(dbName);

    const result = await runSqlever(
      ["deploy", "--db-uri", dbUri, "--top-dir", tmpDir],
      { cwd: tmpDir },
    );

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain("Deploying change: create_users");
    expect(result.stdout).toContain("Deploying change: create_audit_func");
    expect(result.stdout).toContain("Deploying change: add_user_profiles");
    expect(result.stdout).toContain("Deploying change: seed_initial_data");
    expect(result.stdout).toContain("Deploying change: add_user_notes");
    expect(result.stdout).toContain("Deployed 5 change(s) successfully");
  }, 30_000);

  test("tracking tables have correct state after deploy", async () => {
    const dbUri = pgUri(dbName);

    // Deploy first
    const deployResult = await runSqlever(
      ["deploy", "--db-uri", dbUri, "--top-dir", tmpDir],
      { cwd: tmpDir },
    );
    expect(deployResult.exitCode).toBe(0);

    // Verify sqitch.projects
    const projects = await queryDb<{ project: string; uri: string }>(
      dbName,
      "SELECT project, uri FROM sqitch.projects",
    );
    expect(projects).toHaveLength(1);
    expect(projects[0]!.project).toBe("customer_zero_test");
    expect(projects[0]!.uri).toBe(
      "https://example.com/customer-zero-test",
    );

    // Verify sqitch.changes — 5 rows, one per deployed change
    const changes = await queryDb<{
      change_id: string;
      change: string;
      project: string;
      note: string;
      script_hash: string;
    }>(
      dbName,
      "SELECT change_id, change, project, note, script_hash FROM sqitch.changes ORDER BY committed_at",
    );
    expect(changes).toHaveLength(5);

    const changeNames = changes.map((c) => c.change);
    expect(changeNames).toEqual([
      "create_users",
      "create_audit_func",
      "add_user_profiles",
      "seed_initial_data",
      "add_user_notes",
    ]);

    // Every change should have a non-null script_hash (SHA-1 of deploy script)
    for (const c of changes) {
      expect(c.script_hash).toBeTruthy();
      expect(c.script_hash).toHaveLength(40);
    }

    // Verify the special-characters note made it through
    const notesChange = changes.find((c) => c.change === "add_user_notes");
    expect(notesChange!.note).toContain('"quotes"');
    expect(notesChange!.note).toContain("<angle>");

    // Verify sqitch.events — 5 deploy events
    const events = await queryDb<{
      event: string;
      change: string;
      change_id: string;
      requires: string[];
      conflicts: string[];
    }>(
      dbName,
      "SELECT event, change, change_id, requires, conflicts FROM sqitch.events WHERE project = 'customer_zero_test' ORDER BY committed_at",
    );
    expect(events).toHaveLength(5);
    for (const e of events) {
      expect(e.event).toBe("deploy");
    }

    // Verify that events reference the same change_ids as changes
    const changeIds = new Set(changes.map((c) => c.change_id));
    for (const e of events) {
      expect(changeIds.has(e.change_id)).toBe(true);
    }

    // Verify sqitch.dependencies are tracked
    const deps = await queryDb<{
      change_id: string;
      type: string;
      dependency: string;
    }>(
      dbName,
      "SELECT change_id, type, dependency FROM sqitch.dependencies ORDER BY change_id, dependency",
    );

    // create_audit_func requires create_users (1 dep)
    // add_user_profiles requires create_users (1 dep)
    // seed_initial_data requires create_users, add_user_profiles (2 deps)
    // add_user_notes requires create_users (1 dep)
    // Total: 5 dependency rows
    expect(deps).toHaveLength(5);

    const requireDeps = deps.filter((d) => d.type === "require");
    expect(requireDeps).toHaveLength(5);

    // seed_initial_data should have 2 dependencies
    const seedChangeId = changes.find(
      (c) => c.change === "seed_initial_data",
    )!.change_id;
    const seedDeps = deps.filter((d) => d.change_id === seedChangeId);
    expect(seedDeps).toHaveLength(2);
    const seedDepNames = seedDeps.map((d) => d.dependency).sort();
    expect(seedDepNames).toEqual(["add_user_profiles", "create_users"]);
  }, 30_000);

  test("application tables exist after deploy", async () => {
    const dbUri = pgUri(dbName);

    await runSqlever(
      ["deploy", "--db-uri", dbUri, "--top-dir", tmpDir],
      { cwd: tmpDir },
    );

    // Verify application tables were actually created
    const tables = await queryDb<{ tablename: string }>(
      dbName,
      `SELECT tablename FROM pg_tables
       WHERE schemaname = 'public'
       ORDER BY tablename`,
    );
    const tableNames = tables.map((t) => t.tablename);
    expect(tableNames).toContain("users");
    expect(tableNames).toContain("audit_log");
    expect(tableNames).toContain("user_profiles");
    expect(tableNames).toContain("user_notes");

    // Verify seed data was inserted
    const users = await queryDb<{ username: string }>(
      dbName,
      "SELECT username FROM public.users ORDER BY username",
    );
    expect(users).toEqual([
      { username: "admin" },
      { username: "demo_user" },
    ]);

    // Verify the audit trigger function exists
    const funcs = await queryDb<{ proname: string }>(
      dbName,
      "SELECT proname FROM pg_proc WHERE proname = 'audit_trigger_func'",
    );
    expect(funcs).toHaveLength(1);

    // Verify the index on user_notes was created
    const indexes = await queryDb<{ indexname: string }>(
      dbName,
      "SELECT indexname FROM pg_indexes WHERE tablename = 'user_notes' AND indexname = 'idx_user_notes_user_id'",
    );
    expect(indexes).toHaveLength(1);
  }, 30_000);

  test("second deploy is a no-op (nothing to deploy)", async () => {
    const dbUri = pgUri(dbName);

    // First deploy
    const first = await runSqlever(
      ["deploy", "--db-uri", dbUri, "--top-dir", tmpDir],
      { cwd: tmpDir },
    );
    expect(first.exitCode).toBe(0);

    // Second deploy — should be idempotent
    const second = await runSqlever(
      ["deploy", "--db-uri", dbUri, "--top-dir", tmpDir],
      { cwd: tmpDir },
    );
    expect(second.exitCode).toBe(0);
    expect(second.stdout).toContain("Nothing to deploy");

    // Still 5 changes in registry
    const changes = await queryDb(
      dbName,
      "SELECT change FROM sqitch.changes WHERE project = 'customer_zero_test'",
    );
    expect(changes).toHaveLength(5);

    // Still 5 deploy events (no duplicate events)
    const events = await queryDb(
      dbName,
      "SELECT event FROM sqitch.events WHERE project = 'customer_zero_test'",
    );
    expect(events).toHaveLength(5);
  }, 30_000);

  test("status reports correct state after deploy", async () => {
    const dbUri = pgUri(dbName);

    // Deploy
    await runSqlever(
      ["deploy", "--db-uri", dbUri, "--top-dir", tmpDir],
      { cwd: tmpDir },
    );

    // Status in JSON format
    const status = await runSqlever(
      ["status", "--db-uri", dbUri, "--top-dir", tmpDir, "--format", "json"],
      { cwd: tmpDir },
    );
    expect(status.exitCode).toBe(0);

    const parsed = JSON.parse(status.stdout);
    expect(parsed.project).toBe("customer_zero_test");
    expect(parsed.deployed_count).toBe(5);
    expect(parsed.pending_count).toBe(0);
    expect(parsed.pending_changes).toEqual([]);
    expect(parsed.last_deployed).toBeDefined();
    expect(parsed.last_deployed.change).toBe("add_user_notes");
  }, 30_000);
});
