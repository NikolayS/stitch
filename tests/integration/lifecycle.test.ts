// tests/integration/lifecycle.test.ts — end-to-end lifecycle test
//
// Exercises the full init → add → deploy → verify → status → revert cycle
// against a real PostgreSQL database.
//
// Prerequisites:
//   - PostgreSQL reachable at localhost:5417 (docker compose up)
//   - Password: test, user: postgres

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  setupTestDb,
  teardownTestDb,
  runSqlever,
  queryDb,
  pgUri,
} from "./helpers";

// ---------------------------------------------------------------------------
// Helpers local to this test file
// ---------------------------------------------------------------------------

async function makeTempDir(): Promise<string> {
  return mkdtemp(join(tmpdir(), "sqlever-integ-"));
}

// ---------------------------------------------------------------------------
// Sanity checks for the test helpers themselves
// ---------------------------------------------------------------------------

describe("integration helpers", () => {
  let dbName: string;

  beforeEach(async () => {
    dbName = await setupTestDb();
  });

  afterEach(async () => {
    await teardownTestDb(dbName);
  });

  test("setupTestDb creates a database that accepts connections", async () => {
    const rows = await queryDb(dbName, "SELECT 1 AS ok");
    expect(rows).toEqual([{ ok: 1 }]);
  });

  test("queryDb can create and query a table", async () => {
    await queryDb(dbName, "CREATE TABLE t (id int)");
    await queryDb(dbName, "INSERT INTO t VALUES (42)");
    const rows = await queryDb<{ id: number }>(dbName, "SELECT id FROM t");
    expect(rows).toEqual([{ id: 42 }]);
  });
});

// ---------------------------------------------------------------------------
// Init + Add (these commands are already implemented)
// ---------------------------------------------------------------------------

describe("integration: init + add", () => {
  let tmpDir: string;

  beforeEach(async () => {
    tmpDir = await makeTempDir();
  });

  afterEach(async () => {
    await rm(tmpDir, { recursive: true, force: true });
  });

  test("init creates project structure", async () => {
    const result = await runSqlever(
      ["init", "testproject", "--top-dir", tmpDir],
    );
    expect(result.exitCode).toBe(0);

    // Verify key files exist
    const { existsSync } = await import("node:fs");
    expect(existsSync(join(tmpDir, "sqitch.conf"))).toBe(true);
    expect(existsSync(join(tmpDir, "sqitch.plan"))).toBe(true);
    expect(existsSync(join(tmpDir, "deploy"))).toBe(true);
    expect(existsSync(join(tmpDir, "revert"))).toBe(true);
    expect(existsSync(join(tmpDir, "verify"))).toBe(true);
  });

  test("add creates migration files and updates plan", async () => {
    // Init first
    await runSqlever(["init", "testproject", "--top-dir", tmpDir]);

    // Add a change
    const result = await runSqlever(
      ["add", "create_users", "-n", "add users table", "--top-dir", tmpDir],
    );
    expect(result.exitCode).toBe(0);

    // Verify migration files were created
    const { existsSync, readFileSync } = await import("node:fs");
    expect(existsSync(join(tmpDir, "deploy", "create_users.sql"))).toBe(true);
    expect(existsSync(join(tmpDir, "revert", "create_users.sql"))).toBe(true);
    expect(existsSync(join(tmpDir, "verify", "create_users.sql"))).toBe(true);

    // Verify the plan was updated
    const plan = readFileSync(join(tmpDir, "sqitch.plan"), "utf-8");
    expect(plan).toContain("create_users");
    expect(plan).toContain("add users table");
  });
});

// ---------------------------------------------------------------------------
// Full lifecycle: init → add → deploy → verify → status → revert
// ---------------------------------------------------------------------------

describe("integration: full lifecycle", () => {
  let tmpDir: string;
  let dbName: string;

  beforeEach(async () => {
    tmpDir = await makeTempDir();
    dbName = await setupTestDb();
  });

  afterEach(async () => {
    await teardownTestDb(dbName);
    await rm(tmpDir, { recursive: true, force: true });
  });

  test("init → add → deploy → verify → status → revert lifecycle", async () => {
    const dbUri = pgUri(dbName);

    // Step 1: init
    const initResult = await runSqlever([
      "init", "testproject", "--top-dir", tmpDir,
    ]);
    expect(initResult.exitCode).toBe(0);

    // Step 2: add create_users with real SQL scripts
    const addResult = await runSqlever([
      "add", "create_users", "-n", "users table", "--top-dir", tmpDir,
    ]);
    expect(addResult.exitCode).toBe(0);

    // Write a real deploy script (CREATE TABLE)
    await writeFile(
      join(tmpDir, "deploy", "create_users.sql"),
      [
        "-- Deploy create_users",
        "BEGIN;",
        "",
        "CREATE TABLE public.users (",
        "    id    SERIAL PRIMARY KEY,",
        "    name  TEXT NOT NULL,",
        "    email TEXT NOT NULL UNIQUE",
        ");",
        "",
        "COMMIT;",
      ].join("\n"),
    );

    // Write a real revert script (DROP TABLE)
    await writeFile(
      join(tmpDir, "revert", "create_users.sql"),
      [
        "-- Revert create_users",
        "BEGIN;",
        "",
        "DROP TABLE IF EXISTS public.users;",
        "",
        "COMMIT;",
      ].join("\n"),
    );

    // Write a real verify script (SELECT)
    await writeFile(
      join(tmpDir, "verify", "create_users.sql"),
      [
        "-- Verify create_users",
        "SELECT id, name, email FROM public.users WHERE FALSE;",
      ].join("\n"),
    );

    // Step 3: deploy
    const deployResult = await runSqlever([
      "deploy", "--db-uri", dbUri, "--top-dir", tmpDir,
    ]);
    expect(deployResult.exitCode).toBe(0);

    // Step 4: verify database state after deploy
    // 4a. users table exists
    const tables = await queryDb<{ tablename: string }>(
      dbName,
      "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = 'users'",
    );
    expect(tables).toHaveLength(1);
    expect(tables[0]!.tablename).toBe("users");

    // 4b. sqitch.changes has exactly 1 entry
    const changes = await queryDb<{ change: string; project: string }>(
      dbName,
      "SELECT change, project FROM sqitch.changes WHERE project = 'testproject'",
    );
    expect(changes).toHaveLength(1);
    expect(changes[0]!.change).toBe("create_users");

    // 4c. sqitch.events has exactly 1 deploy event
    const deployEvents = await queryDb<{ event: string; change: string }>(
      dbName,
      "SELECT event, change FROM sqitch.events WHERE project = 'testproject' AND event = 'deploy'",
    );
    expect(deployEvents).toHaveLength(1);
    expect(deployEvents[0]!.change).toBe("create_users");

    // Step 5: verify passes
    const verifyResult = await runSqlever([
      "verify", "--db-uri", dbUri, "--top-dir", tmpDir,
    ]);
    expect(verifyResult.exitCode).toBe(0);

    // Step 6: status shows 0 pending (change is deployed)
    const statusResult = await runSqlever([
      "status", "--db-uri", dbUri, "--top-dir", tmpDir,
    ]);
    expect(statusResult.exitCode).toBe(0);
    expect(statusResult.stdout).toContain("create_users");

    // Step 7: revert
    const revertResult = await runSqlever([
      "revert", "-y", "--db-uri", dbUri, "--top-dir", tmpDir,
    ]);
    expect(revertResult.exitCode).toBe(0);

    // Step 8: verify database state after revert
    // 8a. users table gone
    const tablesAfter = await queryDb<{ tablename: string }>(
      dbName,
      "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = 'users'",
    );
    expect(tablesAfter).toHaveLength(0);

    // 8b. sqitch.changes is empty for this project
    const changesAfter = await queryDb<{ change: string }>(
      dbName,
      "SELECT change FROM sqitch.changes WHERE project = 'testproject'",
    );
    expect(changesAfter).toHaveLength(0);

    // 8c. sqitch.events has both deploy and revert events
    const allEvents = await queryDb<{ event: string; change: string }>(
      dbName,
      "SELECT event, change FROM sqitch.events WHERE project = 'testproject' ORDER BY committed_at",
    );
    expect(allEvents).toHaveLength(2);
    expect(allEvents[0]!.event).toBe("deploy");
    expect(allEvents[0]!.change).toBe("create_users");
    expect(allEvents[1]!.event).toBe("revert");
    expect(allEvents[1]!.change).toBe("create_users");
  }, 30_000);
});
