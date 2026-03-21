// tests/compat/oracle.test.ts — Sqitch oracle test
//
// Side-by-side comparison: deploy the same project with both Sqitch (via
// Docker sqitch/sqitch:latest) and sqlever, then compare every row in the
// sqitch.changes, sqitch.events, and sqitch.tags tracking tables.
//
// Any divergence is a bug in sqlever.
//
// Prerequisites:
//   - Docker available and able to pull sqitch/sqitch:latest
//   - PostgreSQL at localhost:5417 (docker compose up)
//
// The test project has 10 changes with:
//   - Linear dependencies (each change depends on the previous)
//   - 2 tags (@v1.0 after change 5, @v2.0 after change 9)
//   - 1 reworked change (add_users reworked after @v1.0)
//
// Timestamp tolerance: 5 seconds (committed_at will differ between runs)

import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execSync } from "node:child_process";
import Client from "pg/lib/client";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const PG_HOST = process.env.TEST_PG_HOST ?? "localhost";
const PG_PORT = Number(process.env.TEST_PG_PORT ?? "5417");
const PG_USER = process.env.TEST_PG_USER ?? "postgres";
const PG_PASS = process.env.TEST_PG_PASS ?? "test";

function pgUri(database: string): string {
  return `postgresql://${PG_USER}:${PG_PASS}@${PG_HOST}:${PG_PORT}/${database}`;
}

function dbPgUri(database: string): string {
  return `db:pg://${PG_USER}:${PG_PASS}@${PG_HOST}:${PG_PORT}/${database}`;
}

const ADMIN_URI = pgUri("postgres");
const TIMESTAMP_TOLERANCE_MS = 5_000;

// Stable planner identity — used by both Sqitch and sqlever
const PLANNER_NAME = "Test Planner";
const PLANNER_EMAIL = "planner@test.example";

// ---------------------------------------------------------------------------
// DB helpers
// ---------------------------------------------------------------------------

async function createDb(name: string): Promise<void> {
  const client = new Client({ connectionString: ADMIN_URI });
  await client.connect();
  try {
    await client.query(`DROP DATABASE IF EXISTS ${name}`);
    await client.query(`CREATE DATABASE ${name}`);
  } finally {
    await client.end();
  }
}

async function dropDb(name: string): Promise<void> {
  const client = new Client({ connectionString: ADMIN_URI });
  await client.connect();
  try {
    await client.query(
      `SELECT pg_terminate_backend(pid)
       FROM pg_stat_activity
       WHERE datname = $1 AND pid <> pg_backend_pid()`,
      [name],
    );
    await client.query(`DROP DATABASE IF EXISTS ${name}`);
  } finally {
    await client.end();
  }
}

async function queryDb<T = Record<string, unknown>>(
  dbName: string,
  sql: string,
): Promise<T[]> {
  const client = new Client({ connectionString: pgUri(dbName) });
  await client.connect();
  try {
    const result = await client.query(sql);
    return result.rows as T[];
  } finally {
    await client.end();
  }
}

// ---------------------------------------------------------------------------
// Docker / shell helpers
// ---------------------------------------------------------------------------

function dockerAvailable(): boolean {
  try {
    execSync("docker info", { stdio: "ignore", timeout: 10_000 });
    return true;
  } catch {
    return false;
  }
}

async function pgAvailable(): Promise<boolean> {
  const { createConnection } = await import("node:net");
  return new Promise<boolean>((resolve) => {
    const socket = createConnection(
      { host: PG_HOST, port: PG_PORT, timeout: 2_000 },
      () => {
        socket.destroy();
        resolve(true);
      },
    );
    socket.on("error", () => resolve(false));
    socket.on("timeout", () => {
      socket.destroy();
      resolve(false);
    });
  });
}

const hasDocker = dockerAvailable();
const hasPg = await pgAvailable();

/** Run Sqitch via Docker against the host-network PG. */
function runSqitch(projectDir: string, args: string[]): string {
  // Mount the project dir into the container; use host networking so
  // Sqitch can reach PG on localhost:5417.
  // Note: The sqitch/sqitch:latest image has ENTRYPOINT ["/bin/sqitch"],
  // so we only pass sqitch sub-command args (e.g. "deploy", "status").
  // We use shell quoting to handle spaces in env values (e.g. PLANNER_NAME).
  const cmd = [
    "docker", "run", "--rm",
    "--network", "host",
    "-v", `${projectDir}:/repo`,
    "-w", "/repo",
    "-e", `SQITCH_FULLNAME=${PLANNER_NAME}`,
    "-e", `SQITCH_EMAIL=${PLANNER_EMAIL}`,
    "sqitch/sqitch:latest",
    ...args,
  ].map(a => `'${a}'`).join(" ");

  return execSync(cmd, {
    encoding: "utf-8",
    timeout: 60_000,
    env: { ...process.env, DOCKER_CLI_HINTS: "false" },
  });
}

/** Run sqlever CLI via bun. */
function runSqlever(args: string[], cwd?: string): string {
  const projectRoot = join(import.meta.dir, "..", "..");
  const cliEntry = join(projectRoot, "src", "cli.ts");
  const cmd = ["bun", "run", cliEntry, ...args].join(" ");

  return execSync(cmd, {
    encoding: "utf-8",
    timeout: 60_000,
    cwd: cwd ?? projectRoot,
    env: {
      ...process.env,
      SQLEVER_USER_NAME: PLANNER_NAME,
      SQLEVER_USER_EMAIL: PLANNER_EMAIL,
      SQITCH_FULLNAME: PLANNER_NAME,
      SQITCH_EMAIL: PLANNER_EMAIL,
    },
  });
}

// ---------------------------------------------------------------------------
// Test project scaffolding
// ---------------------------------------------------------------------------

/**
 * The 10-change project:
 *
 *   1. create_schema       — CREATE SCHEMA app
 *   2. add_users           — CREATE TABLE app.users
 *   3. add_posts           — CREATE TABLE app.posts (FK to users)
 *   4. add_comments        — CREATE TABLE app.comments (FK to posts)
 *   5. add_user_index      — CREATE INDEX on users(email)
 *      @v1.0
 *   6. add_tags            — CREATE TABLE app.tags
 *   7. add_post_tags       — CREATE TABLE app.post_tags (junction)
 *   8. add_users [add_users@v1.0]  — REWORK: add bio column
 *   9. add_categories      — CREATE TABLE app.categories
 *      @v2.0
 *  10. add_settings        — CREATE TABLE app.settings
 *
 * Total: 10 changes, 2 tags, 1 rework.
 */

interface ChangeSpec {
  name: string;
  note: string;
  requires: string[];
  deploy: string;
  revert: string;
  verify: string;
}

const CHANGES: ChangeSpec[] = [
  {
    name: "create_schema",
    note: "add app schema",
    requires: [],
    deploy: `-- Deploy create_schema
BEGIN;
CREATE SCHEMA IF NOT EXISTS app;
COMMIT;
`,
    revert: `-- Revert create_schema
BEGIN;
DROP SCHEMA IF EXISTS app CASCADE;
COMMIT;
`,
    verify: `-- Verify create_schema
SELECT 1/COUNT(*) FROM information_schema.schemata WHERE schema_name = 'app';
`,
  },
  {
    name: "add_users",
    note: "add users table",
    requires: ["create_schema"],
    deploy: `-- Deploy add_users
-- requires: create_schema
BEGIN;
CREATE TABLE app.users (
    id    SERIAL PRIMARY KEY,
    name  TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE
);
COMMIT;
`,
    revert: `-- Revert add_users
BEGIN;
DROP TABLE IF EXISTS app.users CASCADE;
COMMIT;
`,
    verify: `-- Verify add_users
SELECT id, name, email FROM app.users WHERE FALSE;
`,
  },
  {
    name: "add_posts",
    note: "add posts table",
    requires: ["add_users"],
    deploy: `-- Deploy add_posts
-- requires: add_users
BEGIN;
CREATE TABLE app.posts (
    id        SERIAL PRIMARY KEY,
    user_id   INTEGER NOT NULL REFERENCES app.users(id),
    title     TEXT NOT NULL,
    body      TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMIT;
`,
    revert: `-- Revert add_posts
BEGIN;
DROP TABLE IF EXISTS app.posts CASCADE;
COMMIT;
`,
    verify: `-- Verify add_posts
SELECT id, user_id, title, body, created_at FROM app.posts WHERE FALSE;
`,
  },
  {
    name: "add_comments",
    note: "add comments table",
    requires: ["add_posts"],
    deploy: `-- Deploy add_comments
-- requires: add_posts
BEGIN;
CREATE TABLE app.comments (
    id        SERIAL PRIMARY KEY,
    post_id   INTEGER NOT NULL REFERENCES app.posts(id),
    user_id   INTEGER NOT NULL REFERENCES app.users(id),
    body      TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMIT;
`,
    revert: `-- Revert add_comments
BEGIN;
DROP TABLE IF EXISTS app.comments CASCADE;
COMMIT;
`,
    verify: `-- Verify add_comments
SELECT id, post_id, user_id, body, created_at FROM app.comments WHERE FALSE;
`,
  },
  {
    name: "add_user_index",
    note: "index users by email",
    requires: ["add_users"],
    deploy: `-- Deploy add_user_index
-- requires: add_users
BEGIN;
CREATE INDEX idx_users_email ON app.users (email);
COMMIT;
`,
    revert: `-- Revert add_user_index
BEGIN;
DROP INDEX IF EXISTS app.idx_users_email;
COMMIT;
`,
    verify: `-- Verify add_user_index
SELECT 1/COUNT(*) FROM pg_indexes WHERE schemaname = 'app' AND indexname = 'idx_users_email';
`,
  },
];

// Tag @v1.0 is placed after change 5 (add_user_index)

const CHANGES_AFTER_TAG1: ChangeSpec[] = [
  {
    name: "add_tags",
    note: "add tags table",
    requires: ["create_schema"],
    deploy: `-- Deploy add_tags
-- requires: create_schema
BEGIN;
CREATE TABLE app.tags (
    id   SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);
COMMIT;
`,
    revert: `-- Revert add_tags
BEGIN;
DROP TABLE IF EXISTS app.tags CASCADE;
COMMIT;
`,
    verify: `-- Verify add_tags
SELECT id, name FROM app.tags WHERE FALSE;
`,
  },
  {
    name: "add_post_tags",
    note: "add post_tags junction table",
    requires: ["add_posts", "add_tags"],
    deploy: `-- Deploy add_post_tags
-- requires: add_posts, add_tags
BEGIN;
CREATE TABLE app.post_tags (
    post_id INTEGER NOT NULL REFERENCES app.posts(id),
    tag_id  INTEGER NOT NULL REFERENCES app.tags(id),
    PRIMARY KEY (post_id, tag_id)
);
COMMIT;
`,
    revert: `-- Revert add_post_tags
BEGIN;
DROP TABLE IF EXISTS app.post_tags CASCADE;
COMMIT;
`,
    verify: `-- Verify add_post_tags
SELECT post_id, tag_id FROM app.post_tags WHERE FALSE;
`,
  },
];

// Reworked add_users (change 8): adds bio column
const REWORKED_ADD_USERS: ChangeSpec = {
  name: "add_users",
  note: "add bio column to users",
  requires: ["add_users@v1.0"],
  deploy: `-- Deploy add_users
-- requires: add_users@v1.0
BEGIN;
ALTER TABLE app.users ADD COLUMN IF NOT EXISTS bio TEXT DEFAULT '';
COMMIT;
`,
  revert: `-- Revert add_users
BEGIN;
ALTER TABLE app.users DROP COLUMN IF EXISTS bio;
COMMIT;
`,
  verify: `-- Verify add_users
SELECT id, name, email, bio FROM app.users WHERE FALSE;
`,
};

const CHANGES_AFTER_REWORK: ChangeSpec[] = [
  {
    name: "add_categories",
    note: "add categories table",
    requires: ["create_schema"],
    deploy: `-- Deploy add_categories
-- requires: create_schema
BEGIN;
CREATE TABLE app.categories (
    id   SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);
COMMIT;
`,
    revert: `-- Revert add_categories
BEGIN;
DROP TABLE IF EXISTS app.categories CASCADE;
COMMIT;
`,
    verify: `-- Verify add_categories
SELECT id, name FROM app.categories WHERE FALSE;
`,
  },
];

// Tag @v2.0 is placed after change 9 (add_categories)

const LAST_CHANGE: ChangeSpec = {
  name: "add_settings",
  note: "add settings table",
  requires: ["create_schema"],
  deploy: `-- Deploy add_settings
-- requires: create_schema
BEGIN;
CREATE TABLE app.settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL DEFAULT ''
);
COMMIT;
`,
  revert: `-- Revert add_settings
BEGIN;
DROP TABLE IF EXISTS app.settings CASCADE;
COMMIT;
`,
  verify: `-- Verify add_settings
SELECT key, value FROM app.settings WHERE FALSE;
`,
};

/**
 * Build the entire test project on disk.
 *
 * Uses sqlever CLI to init, add changes, tag, and rework — so the
 * sqitch.plan file and all script files are created identically to
 * how a real user would do it.
 *
 * After building, writes real SQL content into deploy/revert/verify
 * scripts (overwriting the templates).
 */
async function buildProject(dir: string): Promise<void> {
  // 1. Init
  runSqlever(["init", "oracle_test", "--top-dir", dir]);

  // Also create sqitch.conf for Sqitch (it needs engine configuration)
  await writeFile(
    join(dir, "sqitch.conf"),
    [
      "[core]",
      "  engine = pg",
      "[engine \"pg\"]",
      `  target = db:pg://${PG_USER}:${PG_PASS}@${PG_HOST}:${PG_PORT}/placeholder`,
      "",
    ].join("\n"),
  );

  // 2. Add changes 1-5
  for (const change of CHANGES) {
    const args = ["add", change.name, "-n", change.note, "--top-dir", dir];
    for (const req of change.requires) {
      args.push("-r", req);
    }
    runSqlever(args);
    await writeFile(join(dir, "deploy", `${change.name}.sql`), change.deploy);
    await writeFile(join(dir, "revert", `${change.name}.sql`), change.revert);
    await writeFile(join(dir, "verify", `${change.name}.sql`), change.verify);
  }

  // 3. Tag @v1.0
  runSqlever(["tag", "v1.0", "-n", "release v1.0", "--top-dir", dir]);

  // 4. Add changes 6-7
  for (const change of CHANGES_AFTER_TAG1) {
    const args = ["add", change.name, "-n", change.note, "--top-dir", dir];
    for (const req of change.requires) {
      args.push("-r", req);
    }
    runSqlever(args);
    await writeFile(join(dir, "deploy", `${change.name}.sql`), change.deploy);
    await writeFile(join(dir, "revert", `${change.name}.sql`), change.revert);
    await writeFile(join(dir, "verify", `${change.name}.sql`), change.verify);
  }

  // 5. Rework add_users (change 8)
  runSqlever(["rework", "add_users", "-n", REWORKED_ADD_USERS.note, "--top-dir", dir]);
  // Overwrite the fresh deploy/revert/verify with real SQL
  await writeFile(join(dir, "deploy", "add_users.sql"), REWORKED_ADD_USERS.deploy);
  await writeFile(join(dir, "revert", "add_users.sql"), REWORKED_ADD_USERS.revert);
  await writeFile(join(dir, "verify", "add_users.sql"), REWORKED_ADD_USERS.verify);

  // 6. Add change 9
  for (const change of CHANGES_AFTER_REWORK) {
    const args = ["add", change.name, "-n", change.note, "--top-dir", dir];
    for (const req of change.requires) {
      args.push("-r", req);
    }
    runSqlever(args);
    await writeFile(join(dir, "deploy", `${change.name}.sql`), change.deploy);
    await writeFile(join(dir, "revert", `${change.name}.sql`), change.revert);
    await writeFile(join(dir, "verify", `${change.name}.sql`), change.verify);
  }

  // 7. Tag @v2.0
  runSqlever(["tag", "v2.0", "-n", "release v2.0", "--top-dir", dir]);

  // 8. Add change 10
  {
    const change = LAST_CHANGE;
    const args = ["add", change.name, "-n", change.note, "--top-dir", dir];
    for (const req of change.requires) {
      args.push("-r", req);
    }
    runSqlever(args);
    await writeFile(join(dir, "deploy", `${change.name}.sql`), change.deploy);
    await writeFile(join(dir, "revert", `${change.name}.sql`), change.revert);
    await writeFile(join(dir, "verify", `${change.name}.sql`), change.verify);
  }
}

// ---------------------------------------------------------------------------
// Table row types for comparison
// ---------------------------------------------------------------------------

interface ChangeRow {
  change_id: string;
  change: string;
  project: string;
  note: string;
  script_hash: string | null;
  committed_at: Date;
  committer_name: string;
  committer_email: string;
  planner_name: string;
  planner_email: string;
  planned_at: Date;
}

interface EventRow {
  event: string;
  change_id: string;
  change: string;
  project: string;
  note: string;
  requires: string[];
  conflicts: string[];
  tags: string[];
  committed_at: Date;
  committer_name: string;
  committer_email: string;
  planner_name: string;
  planner_email: string;
  planned_at: Date;
}

interface TagRow {
  tag_id: string;
  tag: string;
  change_id: string;
  project: string;
  note: string;
  committed_at: Date;
  committer_name: string;
  committer_email: string;
  planner_name: string;
  planner_email: string;
  planned_at: Date;
}

// ---------------------------------------------------------------------------
// Comparison helpers
// ---------------------------------------------------------------------------

/**
 * Assert two timestamps are within TIMESTAMP_TOLERANCE_MS of each other.
 */
function assertTimestampClose(
  sqitchTs: Date | string,
  sqleverTs: Date | string,
): void {
  const a = new Date(sqitchTs).getTime();
  const b = new Date(sqleverTs).getTime();
  const diff = Math.abs(a - b);
  expect(diff).toBeLessThanOrEqual(TIMESTAMP_TOLERANCE_MS);
}

// ---------------------------------------------------------------------------
// Test suite
// ---------------------------------------------------------------------------

describe.skipIf(!hasDocker || !hasPg)("compat: sqitch oracle", () => {
  let projectDir: string;
  const sqitchDb = "oracle_sqitch";
  const sqleverDb = "oracle_sqlever";

  beforeAll(async () => {
    // Pull Sqitch image (may take a while the first time)
    execSync("docker pull sqitch/sqitch:latest", {
      stdio: "ignore",
      timeout: 120_000,
    });

    // Create temp project directory
    projectDir = await mkdtemp(join(tmpdir(), "sqlever-oracle-"));

    // Create both databases
    await createDb(sqitchDb);
    await createDb(sqleverDb);

    // Build the test project
    await buildProject(projectDir);

    // --- Deploy with Sqitch ---
    const sqitchUri = dbPgUri(sqitchDb);
    runSqitch(projectDir, ["deploy", sqitchUri]);

    // --- Deploy with sqlever ---
    const sqleverUri = pgUri(sqleverDb);
    runSqlever([
      "deploy",
      "--db-uri", sqleverUri,
      "--top-dir", projectDir,
    ]);
  }, 180_000); // generous timeout for Docker pulls

  afterAll(async () => {
    if (projectDir) {
      await rm(projectDir, { recursive: true, force: true });
    }
    try { await dropDb(sqitchDb); } catch { /* best effort */ }
    try { await dropDb(sqleverDb); } catch { /* best effort */ }
  });

  // -------------------------------------------------------------------------
  // sqitch.changes comparison
  // -------------------------------------------------------------------------

  test("changes: row count matches", async () => {

    const sqitchChanges = await queryDb<ChangeRow>(
      sqitchDb,
      "SELECT * FROM sqitch.changes ORDER BY committed_at, change",
    );
    const sqleverChanges = await queryDb<ChangeRow>(
      sqleverDb,
      "SELECT * FROM sqitch.changes ORDER BY committed_at, change",
    );
    expect(sqleverChanges.length).toBe(sqitchChanges.length);
    // We expect 10 changes (the reworked add_users replaces the original
    // in sqitch.changes — actually no, Sqitch keeps both; the rework is
    // a separate change_id but same name).
    expect(sqitchChanges.length).toBeGreaterThanOrEqual(10);
  });

  test("changes: change_id values match", async () => {

    const sqitchIds = await queryDb<{ change_id: string }>(
      sqitchDb,
      "SELECT change_id FROM sqitch.changes ORDER BY committed_at, change",
    );
    const sqleverIds = await queryDb<{ change_id: string }>(
      sqleverDb,
      "SELECT change_id FROM sqitch.changes ORDER BY committed_at, change",
    );
    expect(sqleverIds.map((r) => r.change_id)).toEqual(
      sqitchIds.map((r) => r.change_id),
    );
  });

  test("changes: change names match", async () => {

    const sqitch = await queryDb<{ change: string }>(
      sqitchDb,
      "SELECT change FROM sqitch.changes ORDER BY committed_at, change",
    );
    const sqlever = await queryDb<{ change: string }>(
      sqleverDb,
      "SELECT change FROM sqitch.changes ORDER BY committed_at, change",
    );
    expect(sqlever.map((r) => r.change)).toEqual(
      sqitch.map((r) => r.change),
    );
  });

  test("changes: project values match", async () => {

    const sqitch = await queryDb<{ project: string }>(
      sqitchDb,
      "SELECT project FROM sqitch.changes ORDER BY committed_at, change",
    );
    const sqlever = await queryDb<{ project: string }>(
      sqleverDb,
      "SELECT project FROM sqitch.changes ORDER BY committed_at, change",
    );
    expect(sqlever.map((r) => r.project)).toEqual(
      sqitch.map((r) => r.project),
    );
  });

  test("changes: note values match", async () => {

    const sqitch = await queryDb<{ note: string }>(
      sqitchDb,
      "SELECT note FROM sqitch.changes ORDER BY committed_at, change_id",
    );
    const sqlever = await queryDb<{ note: string }>(
      sqleverDb,
      "SELECT note FROM sqitch.changes ORDER BY committed_at, change_id",
    );
    expect(sqlever.map((r) => r.note)).toEqual(
      sqitch.map((r) => r.note),
    );
  });

  test("changes: script_hash values match", async () => {

    const sqitch = await queryDb<{ change_id: string; script_hash: string | null }>(
      sqitchDb,
      "SELECT change_id, script_hash FROM sqitch.changes ORDER BY committed_at, change_id",
    );
    const sqlever = await queryDb<{ change_id: string; script_hash: string | null }>(
      sqleverDb,
      "SELECT change_id, script_hash FROM sqitch.changes ORDER BY committed_at, change_id",
    );
    expect(sqlever.map((r) => r.script_hash)).toEqual(
      sqitch.map((r) => r.script_hash),
    );
  });

  test("changes: planner_name and planner_email match", async () => {

    const sqitch = await queryDb<{ planner_name: string; planner_email: string }>(
      sqitchDb,
      "SELECT planner_name, planner_email FROM sqitch.changes ORDER BY committed_at, change_id",
    );
    const sqlever = await queryDb<{ planner_name: string; planner_email: string }>(
      sqleverDb,
      "SELECT planner_name, planner_email FROM sqitch.changes ORDER BY committed_at, change_id",
    );
    for (let i = 0; i < sqitch.length; i++) {
      expect(sqlever[i]!.planner_name).toBe(sqitch[i]!.planner_name);
      expect(sqlever[i]!.planner_email).toBe(sqitch[i]!.planner_email);
    }
  });

  test("changes: planned_at timestamps match", async () => {

    const sqitch = await queryDb<{ change_id: string; planned_at: Date }>(
      sqitchDb,
      "SELECT change_id, planned_at FROM sqitch.changes ORDER BY committed_at, change_id",
    );
    const sqlever = await queryDb<{ change_id: string; planned_at: Date }>(
      sqleverDb,
      "SELECT change_id, planned_at FROM sqitch.changes ORDER BY committed_at, change_id",
    );
    for (let i = 0; i < sqitch.length; i++) {
      // planned_at comes from the plan file, so it should match exactly
      // (both tools read the same plan). Use tolerance anyway.
      assertTimestampClose(
        sqitch[i]!.planned_at,
        sqlever[i]!.planned_at,
      );
    }
  });

  test("changes: committer_name and committer_email match", async () => {

    const sqitch = await queryDb<{ committer_name: string; committer_email: string }>(
      sqitchDb,
      "SELECT committer_name, committer_email FROM sqitch.changes ORDER BY committed_at, change_id",
    );
    const sqlever = await queryDb<{ committer_name: string; committer_email: string }>(
      sqleverDb,
      "SELECT committer_name, committer_email FROM sqitch.changes ORDER BY committed_at, change_id",
    );
    for (let i = 0; i < sqitch.length; i++) {
      expect(sqlever[i]!.committer_name).toBe(sqitch[i]!.committer_name);
      expect(sqlever[i]!.committer_email).toBe(sqitch[i]!.committer_email);
    }
  });

  // -------------------------------------------------------------------------
  // sqitch.events comparison
  // -------------------------------------------------------------------------

  test("events: row count matches", async () => {

    const sqitch = await queryDb<EventRow>(
      sqitchDb,
      "SELECT * FROM sqitch.events ORDER BY committed_at, change_id",
    );
    const sqlever = await queryDb<EventRow>(
      sqleverDb,
      "SELECT * FROM sqitch.events ORDER BY committed_at, change_id",
    );
    expect(sqlever.length).toBe(sqitch.length);
  });

  test("events: event types match", async () => {

    const sqitch = await queryDb<{ event: string }>(
      sqitchDb,
      "SELECT event FROM sqitch.events ORDER BY committed_at, change_id",
    );
    const sqlever = await queryDb<{ event: string }>(
      sqleverDb,
      "SELECT event FROM sqitch.events ORDER BY committed_at, change_id",
    );
    expect(sqlever.map((r) => r.event)).toEqual(
      sqitch.map((r) => r.event),
    );
  });

  test("events: change_id values match", async () => {

    const sqitch = await queryDb<{ change_id: string }>(
      sqitchDb,
      "SELECT change_id FROM sqitch.events ORDER BY committed_at, change_id",
    );
    const sqlever = await queryDb<{ change_id: string }>(
      sqleverDb,
      "SELECT change_id FROM sqitch.events ORDER BY committed_at, change_id",
    );
    expect(sqlever.map((r) => r.change_id)).toEqual(
      sqitch.map((r) => r.change_id),
    );
  });

  test("events: change names match", async () => {

    const sqitch = await queryDb<{ change: string }>(
      sqitchDb,
      "SELECT change FROM sqitch.events ORDER BY committed_at, change_id",
    );
    const sqlever = await queryDb<{ change: string }>(
      sqleverDb,
      "SELECT change FROM sqitch.events ORDER BY committed_at, change_id",
    );
    expect(sqlever.map((r) => r.change)).toEqual(
      sqitch.map((r) => r.change),
    );
  });

  test("events: tags arrays match", async () => {

    const sqitch = await queryDb<{ change_id: string; tags: string[] }>(
      sqitchDb,
      "SELECT change_id, tags FROM sqitch.events ORDER BY committed_at, change_id",
    );
    const sqlever = await queryDb<{ change_id: string; tags: string[] }>(
      sqleverDb,
      "SELECT change_id, tags FROM sqitch.events ORDER BY committed_at, change_id",
    );
    for (let i = 0; i < sqitch.length; i++) {
      expect(sqlever[i]!.tags).toEqual(sqitch[i]!.tags);
    }
  });

  test("events: requires arrays match", async () => {

    const sqitch = await queryDb<{ change_id: string; requires: string[] }>(
      sqitchDb,
      "SELECT change_id, requires FROM sqitch.events ORDER BY committed_at, change_id",
    );
    const sqlever = await queryDb<{ change_id: string; requires: string[] }>(
      sqleverDb,
      "SELECT change_id, requires FROM sqitch.events ORDER BY committed_at, change_id",
    );
    for (let i = 0; i < sqitch.length; i++) {
      expect(sqlever[i]!.requires).toEqual(sqitch[i]!.requires);
    }
  });

  test("events: conflicts arrays match", async () => {

    const sqitch = await queryDb<{ change_id: string; conflicts: string[] }>(
      sqitchDb,
      "SELECT change_id, conflicts FROM sqitch.events ORDER BY committed_at, change_id",
    );
    const sqlever = await queryDb<{ change_id: string; conflicts: string[] }>(
      sqleverDb,
      "SELECT change_id, conflicts FROM sqitch.events ORDER BY committed_at, change_id",
    );
    for (let i = 0; i < sqitch.length; i++) {
      expect(sqlever[i]!.conflicts).toEqual(sqitch[i]!.conflicts);
    }
  });

  // -------------------------------------------------------------------------
  // sqitch.tags comparison
  // -------------------------------------------------------------------------

  test("tags: row count matches", async () => {

    const sqitch = await queryDb<TagRow>(
      sqitchDb,
      "SELECT * FROM sqitch.tags ORDER BY committed_at, tag",
    );
    const sqlever = await queryDb<TagRow>(
      sqleverDb,
      "SELECT * FROM sqitch.tags ORDER BY committed_at, tag",
    );
    expect(sqlever.length).toBe(sqitch.length);
    expect(sqitch.length).toBe(2); // @v1.0 and @v2.0
  });

  test("tags: tag_id values match", async () => {

    const sqitch = await queryDb<{ tag_id: string }>(
      sqitchDb,
      "SELECT tag_id FROM sqitch.tags ORDER BY committed_at, tag",
    );
    const sqlever = await queryDb<{ tag_id: string }>(
      sqleverDb,
      "SELECT tag_id FROM sqitch.tags ORDER BY committed_at, tag",
    );
    expect(sqlever.map((r) => r.tag_id)).toEqual(
      sqitch.map((r) => r.tag_id),
    );
  });

  test("tags: tag names match", async () => {

    const sqitch = await queryDb<{ tag: string }>(
      sqitchDb,
      "SELECT tag FROM sqitch.tags ORDER BY committed_at, tag",
    );
    const sqlever = await queryDb<{ tag: string }>(
      sqleverDb,
      "SELECT tag FROM sqitch.tags ORDER BY committed_at, tag",
    );
    expect(sqlever.map((r) => r.tag)).toEqual(
      sqitch.map((r) => r.tag),
    );
  });

  test("tags: change_id references match", async () => {

    const sqitch = await queryDb<{ tag: string; change_id: string }>(
      sqitchDb,
      "SELECT tag, change_id FROM sqitch.tags ORDER BY committed_at, tag",
    );
    const sqlever = await queryDb<{ tag: string; change_id: string }>(
      sqleverDb,
      "SELECT tag, change_id FROM sqitch.tags ORDER BY committed_at, tag",
    );
    expect(sqlever.map((r) => r.change_id)).toEqual(
      sqitch.map((r) => r.change_id),
    );
  });

  test("tags: project values match", async () => {

    const sqitch = await queryDb<{ project: string }>(
      sqitchDb,
      "SELECT project FROM sqitch.tags ORDER BY committed_at, tag",
    );
    const sqlever = await queryDb<{ project: string }>(
      sqleverDb,
      "SELECT project FROM sqitch.tags ORDER BY committed_at, tag",
    );
    expect(sqlever.map((r) => r.project)).toEqual(
      sqitch.map((r) => r.project),
    );
  });

  test("tags: planner_name and planner_email match", async () => {

    const sqitch = await queryDb<{ planner_name: string; planner_email: string }>(
      sqitchDb,
      "SELECT planner_name, planner_email FROM sqitch.tags ORDER BY committed_at, tag",
    );
    const sqlever = await queryDb<{ planner_name: string; planner_email: string }>(
      sqleverDb,
      "SELECT planner_name, planner_email FROM sqitch.tags ORDER BY committed_at, tag",
    );
    for (let i = 0; i < sqitch.length; i++) {
      expect(sqlever[i]!.planner_name).toBe(sqitch[i]!.planner_name);
      expect(sqlever[i]!.planner_email).toBe(sqitch[i]!.planner_email);
    }
  });

  // -------------------------------------------------------------------------
  // Cross-table integrity checks
  // -------------------------------------------------------------------------

  test("every change has a deploy event", async () => {

    const sqleverChanges = await queryDb<{ change_id: string }>(
      sqleverDb,
      "SELECT change_id FROM sqitch.changes",
    );
    const sqleverEvents = await queryDb<{ change_id: string; event: string }>(
      sqleverDb,
      "SELECT change_id, event FROM sqitch.events WHERE event = 'deploy'",
    );
    const eventIds = new Set(sqleverEvents.map((e) => e.change_id));
    for (const change of sqleverChanges) {
      expect(eventIds.has(change.change_id)).toBe(true);
    }
  });

  test("application tables were created correctly by both tools", async () => {

    // Verify both databases have the same application tables
    const tablesQuery = `
      SELECT tablename FROM pg_tables
      WHERE schemaname = 'app'
      ORDER BY tablename
    `;
    const sqitchTables = await queryDb<{ tablename: string }>(sqitchDb, tablesQuery);
    const sqleverTables = await queryDb<{ tablename: string }>(sqleverDb, tablesQuery);

    expect(sqleverTables.map((t) => t.tablename)).toEqual(
      sqitchTables.map((t) => t.tablename),
    );

    // Should have at least these tables
    const tableNames = sqleverTables.map((t) => t.tablename);
    expect(tableNames).toContain("users");
    expect(tableNames).toContain("posts");
    expect(tableNames).toContain("comments");
    expect(tableNames).toContain("tags");
    expect(tableNames).toContain("post_tags");
    expect(tableNames).toContain("categories");
    expect(tableNames).toContain("settings");
  });
});
