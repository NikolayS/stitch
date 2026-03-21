// tests/compat/handoff.test.ts — Mid-deploy handoff tests
//
// Proves that Sqitch and sqlever can share the same tracking tables:
//
//   Test 1 (Sqitch -> sqlever): Deploy changes 1-5 with Sqitch (Docker),
//     then deploy changes 6-10 with sqlever. Verify all 10 changes tracked.
//
//   Test 2 (sqlever -> Sqitch): Deploy all 10 changes with sqlever,
//     then run `sqitch status` and `sqitch log` via Docker to verify Sqitch
//     can read sqlever's tracking state.
//
// This proves the "alias sqitch=sqlever" adoption path (issue #77).
//
// Prerequisites:
//   - Docker available (for sqitch/sqitch:latest and postgres:17)
//   - PostgreSQL reachable at localhost:5417 (docker compose up)

import { describe, test, expect, beforeEach, afterEach, beforeAll } from "bun:test";
import { mkdtemp, rm, writeFile, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execSync } from "node:child_process";

import {
  setupTestDb,
  teardownTestDb,
  runSqlever,
  queryDb,
  pgUri,
  hasPg,
} from "../integration/helpers";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const SQITCH_IMAGE = "sqitch/sqitch:latest";
const PG_HOST_FROM_DOCKER = "host.docker.internal";
const PG_PORT = 5417;
const PG_USER = "postgres";
const PG_PASS = "test";
const PROJECT_NAME = "handoff_test";
const NUM_CHANGES = 10;

/** Build a db:pg:// URI that Sqitch (inside Docker) can reach. */
function sqitchDbUri(dbName: string): string {
  return `db:pg://${PG_USER}:${PG_PASS}@${PG_HOST_FROM_DOCKER}:${PG_PORT}/${dbName}`;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function makeTempDir(): Promise<string> {
  return mkdtemp(join(tmpdir(), "sqlever-handoff-"));
}

/**
 * Generate a project with N changes using sqlever init + add.
 * Each change creates a simple table: CREATE TABLE public.tN (id int).
 */
async function createProject(
  tmpDir: string,
  n: number,
): Promise<void> {
  // Init project
  const initResult = await runSqlever([
    "init", PROJECT_NAME, "--top-dir", tmpDir,
  ]);
  if (initResult.exitCode !== 0) {
    throw new Error(`init failed: ${initResult.stderr}`);
  }

  // Add N changes
  for (let i = 1; i <= n; i++) {
    const changeName = `change_${String(i).padStart(3, "0")}`;
    const addResult = await runSqlever([
      "add", changeName, "-n", `create table t${i}`, "--top-dir", tmpDir,
    ]);
    if (addResult.exitCode !== 0) {
      throw new Error(`add ${changeName} failed: ${addResult.stderr}`);
    }

    // Write real deploy script
    await writeFile(
      join(tmpDir, "deploy", `${changeName}.sql`),
      `-- Deploy ${changeName}\nCREATE TABLE public.t${i} (id int);\n`,
    );

    // Write real revert script
    await writeFile(
      join(tmpDir, "revert", `${changeName}.sql`),
      `-- Revert ${changeName}\nDROP TABLE IF EXISTS public.t${i};\n`,
    );

    // Write verify script
    await writeFile(
      join(tmpDir, "verify", `${changeName}.sql`),
      `-- Verify ${changeName}\nSELECT id FROM public.t${i} LIMIT 0;\n`,
    );
  }
}

/**
 * Run a Sqitch command via Docker. The project directory is bind-mounted.
 *
 * Uses --add-host to make host.docker.internal resolve to the Docker
 * host on Linux (macOS has this built in).
 */
function runSqitchDocker(
  args: string[],
  projectDir: string,
  options: { env?: Record<string, string> } = {},
): { stdout: string; stderr: string; exitCode: number } {
  const envFlags: string[] = [];
  if (options.env) {
    for (const [k, v] of Object.entries(options.env)) {
      envFlags.push("-e", `${k}=${v}`);
    }
  }

  const cmd = [
    "docker", "run", "--rm",
    "--add-host", "host.docker.internal:host-gateway",
    "-v", `${projectDir}:/repo`,
    "-w", "/repo",
    ...envFlags,
    SQITCH_IMAGE,
    ...args,
  ].map(a => `'${a}'`).join(" ");

  try {
    const stdout = execSync(cmd, {
      encoding: "utf-8",
      timeout: 120_000,
      env: { ...process.env, ...options.env },
    });
    return { stdout, stderr: "", exitCode: 0 };
  } catch (err: unknown) {
    const e = err as { stdout?: string; stderr?: string; status?: number };
    return {
      stdout: e.stdout ?? "",
      stderr: e.stderr ?? "",
      exitCode: e.status ?? 1,
    };
  }
}

/**
 * Check if Docker is available and the sqitch image can be pulled.
 */
function dockerAvailable(): boolean {
  try {
    execSync("docker info", { stdio: "ignore", timeout: 10_000 });
    return true;
  } catch {
    return false;
  }
}

/**
 * Ensure the sqitch Docker image is available (pull if needed).
 */
function ensureSqitchImage(): void {
  try {
    execSync(`docker image inspect ${SQITCH_IMAGE}`, {
      stdio: "ignore",
      timeout: 10_000,
    });
  } catch {
    // Image not present; pull it
    execSync(`docker pull ${SQITCH_IMAGE}`, {
      stdio: "inherit",
      timeout: 300_000,
    });
  }
}

// ---------------------------------------------------------------------------
// Test suite
// ---------------------------------------------------------------------------

const hasDocker = dockerAvailable();

describe.skipIf(!hasDocker || !hasPg)("compat: mid-deploy handoff", () => {
  let tmpDir: string;
  let dbName: string;

  beforeAll(() => {
    ensureSqitchImage();
  });

  beforeEach(async () => {
    tmpDir = await makeTempDir();
    dbName = await setupTestDb();
  });

  afterEach(async () => {
    await teardownTestDb(dbName);
    await rm(tmpDir, { recursive: true, force: true });
  });

  // -------------------------------------------------------------------------
  // Test 1: Sqitch deploys 1-5, sqlever deploys 6-10
  // -------------------------------------------------------------------------

  test("Sqitch deploys changes 1-5, sqlever deploys changes 6-10", async () => {
    // 1. Create project with 10 changes
    await createProject(tmpDir, NUM_CHANGES);

    const dockerDbUri = sqitchDbUri(dbName);
    const localDbUri = pgUri(dbName);

    // We need to update sqitch.conf to set the engine target for Sqitch.
    // Sqitch Docker needs a db:pg:// URI via its config.
    const confPath = join(tmpDir, "sqitch.conf");
    const existingConf = await readFile(confPath, "utf-8");
    const updatedConf = existingConf + `
[engine "pg"]
\ttarget = ${dockerDbUri}
`;
    await writeFile(confPath, updatedConf);

    // 2. Deploy changes 1-5 with Sqitch (Docker)
    const sqitchDeployResult = runSqitchDocker(
      ["deploy", "--to", "change_005", dockerDbUri],
      tmpDir,
      {
        env: {
          SQITCH_FULLNAME: "Sqitch Deployer",
          SQITCH_EMAIL: "sqitch@test.local",
        },
      },
    );

    expect(sqitchDeployResult.exitCode).toBe(0);

    // Verify 5 changes deployed
    const changesAfterSqitch = await queryDb<{ change: string }>(
      dbName,
      "SELECT change FROM sqitch.changes ORDER BY committed_at ASC",
    );
    expect(changesAfterSqitch).toHaveLength(5);
    for (let i = 1; i <= 5; i++) {
      expect(changesAfterSqitch[i - 1]!.change).toBe(
        `change_${String(i).padStart(3, "0")}`,
      );
    }

    // Verify 5 tables created
    const tablesAfterSqitch = await queryDb<{ tablename: string }>(
      dbName,
      `SELECT tablename FROM pg_tables
       WHERE schemaname = 'public' AND tablename LIKE 't%'
       ORDER BY tablename`,
    );
    expect(tablesAfterSqitch).toHaveLength(5);

    // 3. Deploy remaining changes 6-10 with sqlever
    const sqleverResult = await runSqlever(
      ["deploy", "--db-uri", localDbUri, "--top-dir", tmpDir],
    );

    expect(sqleverResult.exitCode).toBe(0);

    // 4. Verify all 10 changes are in sqitch.changes
    const allChanges = await queryDb<{ change: string; project: string }>(
      dbName,
      "SELECT change, project FROM sqitch.changes ORDER BY committed_at ASC",
    );
    expect(allChanges).toHaveLength(10);
    for (let i = 1; i <= 10; i++) {
      const expected = `change_${String(i).padStart(3, "0")}`;
      expect(allChanges[i - 1]!.change).toBe(expected);
      expect(allChanges[i - 1]!.project).toBe(PROJECT_NAME);
    }

    // 5. Verify all 10 events are deploy events
    const allEvents = await queryDb<{ event: string; change: string }>(
      dbName,
      `SELECT event, change FROM sqitch.events
       WHERE project = $1
       ORDER BY committed_at ASC`,
      [PROJECT_NAME],
    );
    expect(allEvents).toHaveLength(10);
    for (let i = 0; i < 10; i++) {
      expect(allEvents[i]!.event).toBe("deploy");
      expect(allEvents[i]!.change).toBe(
        `change_${String(i + 1).padStart(3, "0")}`,
      );
    }

    // 6. Verify all 10 tables exist
    const allTables = await queryDb<{ tablename: string }>(
      dbName,
      `SELECT tablename FROM pg_tables
       WHERE schemaname = 'public' AND tablename LIKE 't%'
       ORDER BY tablename`,
    );
    expect(allTables).toHaveLength(10);
  }, 120_000);

  // -------------------------------------------------------------------------
  // Test 2: sqlever deploys all 10, Sqitch reads the state
  // -------------------------------------------------------------------------

  test("sqlever deploys all 10, Sqitch status and log read correctly", async () => {
    // 1. Create project with 10 changes
    await createProject(tmpDir, NUM_CHANGES);

    const dockerDbUri = sqitchDbUri(dbName);
    const localDbUri = pgUri(dbName);

    // Update sqitch.conf with engine target for Sqitch
    const confPath = join(tmpDir, "sqitch.conf");
    const existingConf = await readFile(confPath, "utf-8");
    const updatedConf = existingConf + `
[engine "pg"]
\ttarget = ${dockerDbUri}
`;
    await writeFile(confPath, updatedConf);

    // 2. Deploy all 10 changes with sqlever
    const deployResult = await runSqlever(
      ["deploy", "--db-uri", localDbUri, "--top-dir", tmpDir],
    );
    expect(deployResult.exitCode).toBe(0);

    // Verify all 10 in tracking tables
    const allChanges = await queryDb<{ change: string }>(
      dbName,
      "SELECT change FROM sqitch.changes ORDER BY committed_at ASC",
    );
    expect(allChanges).toHaveLength(10);

    // 3. Run `sqitch status` via Docker -- verify it reads sqlever's state
    const statusResult = runSqitchDocker(
      ["status", dockerDbUri],
      tmpDir,
      {
        env: {
          SQITCH_FULLNAME: "Sqitch Checker",
          SQITCH_EMAIL: "sqitch@test.local",
        },
      },
    );

    // Sqitch status should succeed (exit 0) and mention the last change
    expect(statusResult.exitCode).toBe(0);
    expect(statusResult.stdout).toContain("change_010");
    // Should report nothing to deploy (all up to date)
    expect(statusResult.stdout).toContain("Nothing to deploy");

    // 4. Run `sqitch log` via Docker -- verify it shows all 10 changes
    const logResult = runSqitchDocker(
      ["log", dockerDbUri],
      tmpDir,
      {
        env: {
          SQITCH_FULLNAME: "Sqitch Checker",
          SQITCH_EMAIL: "sqitch@test.local",
        },
      },
    );

    expect(logResult.exitCode).toBe(0);

    // The log output should contain all 10 change names
    for (let i = 1; i <= 10; i++) {
      const changeName = `change_${String(i).padStart(3, "0")}`;
      expect(logResult.stdout).toContain(changeName);
    }
  }, 120_000);
});
