import { describe, it, expect, beforeEach, mock } from "bun:test";
import { resetConfig } from "../../src/output";

// ---------------------------------------------------------------------------
// Mock pg/lib/client — same approach as client.test.ts
// ---------------------------------------------------------------------------

let mockInstances: MockPgClient[] = [];

class MockPgClient {
  options: Record<string, unknown>;
  queries: Array<{ text: string; values?: unknown[] }> = [];
  connected = false;
  ended = false;
  queryResults: Record<string, { rows: unknown[]; rowCount: number; command: string }> = {};

  constructor(options: Record<string, unknown>) {
    this.options = options;
    mockInstances.push(this);
  }

  async connect() {
    this.connected = true;
  }

  async query(text: string, values?: unknown[]) {
    this.queries.push({ text, values });
    return (
      this.queryResults[text] ?? {
        rows: [],
        rowCount: 0,
        command: "SELECT",
      }
    );
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
  Registry,
  REGISTRY_DDL,
  REGISTRY_LOCK_KEY,
} = await import("../../src/db/registry");

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

// Sample data for tests
const sampleProject = {
  project: "myproject",
  uri: "https://example.com/myproject",
  creator_name: "Test User",
  creator_email: "test@example.com",
};

const sampleDeployInput = {
  change_id: "abc123def456",
  script_hash: "sha1hashvalue",
  change: "add_users_table",
  project: "myproject",
  note: "Add users table",
  committer_name: "Test User",
  committer_email: "test@example.com",
  planned_at: new Date("2025-01-15T10:00:00Z"),
  planner_name: "Plan User",
  planner_email: "plan@example.com",
  requires: ["create_schema"],
  conflicts: [],
  tags: ["@v1.0"],
  dependencies: [
    {
      type: "require",
      dependency: "create_schema",
      dependency_id: "def789abc012",
    },
  ],
};

const sampleTagInput = {
  tag_id: "tag123abc",
  tag: "@v1.0",
  project: "myproject",
  change_id: "abc123def456",
  note: "Release v1.0",
  committer_name: "Test User",
  committer_email: "test@example.com",
  planned_at: new Date("2025-01-15T10:00:00Z"),
  planner_name: "Plan User",
  planner_email: "plan@example.com",
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Registry", () => {
  beforeEach(() => {
    mockInstances = [];
    resetConfig();
  });

  // -----------------------------------------------------------------------
  // REGISTRY_DDL
  // -----------------------------------------------------------------------

  describe("REGISTRY_DDL", () => {
    it("contains CREATE SCHEMA IF NOT EXISTS sqitch", () => {
      expect(REGISTRY_DDL).toContain("CREATE SCHEMA IF NOT EXISTS sqitch");
    });

    it("creates all 6 tables with IF NOT EXISTS", () => {
      const tables = [
        "sqitch.projects",
        "sqitch.releases",
        "sqitch.changes",
        "sqitch.tags",
        "sqitch.dependencies",
        "sqitch.events",
      ];
      for (const table of tables) {
        expect(REGISTRY_DDL).toContain(
          `CREATE TABLE IF NOT EXISTS ${table}`,
        );
      }
    });

    it("uses clock_timestamp() for all default timestamps", () => {
      // Count DEFAULT clock_timestamp() occurrences — one per table that has
      // a timestamptz column with a default (projects, releases, changes, tags, events = 5)
      const matches = REGISTRY_DDL.match(/DEFAULT clock_timestamp\(\)/g);
      expect(matches).not.toBeNull();
      expect(matches!.length).toBe(5);

      // Must NOT contain NOW()
      expect(REGISTRY_DDL).not.toContain("NOW()");
      expect(REGISTRY_DDL).not.toContain("now()");
    });

    // -- sqitch.projects --

    it("defines sqitch.projects with correct PK and UNIQUE", () => {
      expect(REGISTRY_DDL).toContain("project         TEXT        PRIMARY KEY");
      expect(REGISTRY_DDL).toContain("uri             TEXT        NULL UNIQUE");
    });

    // -- sqitch.releases --

    it("defines sqitch.releases with REAL PRIMARY KEY", () => {
      expect(REGISTRY_DDL).toContain("version         REAL        PRIMARY KEY");
    });

    // -- sqitch.changes --

    it("defines sqitch.changes with FK to projects ON UPDATE CASCADE", () => {
      expect(REGISTRY_DDL).toContain(
        "project         TEXT        NOT NULL REFERENCES sqitch.projects(project) ON UPDATE CASCADE",
      );
    });

    it("defines sqitch.changes with UNIQUE (project, script_hash)", () => {
      expect(REGISTRY_DDL).toContain("UNIQUE (project, script_hash)");
    });

    // -- sqitch.tags --

    it("defines sqitch.tags with FK to projects and changes ON UPDATE CASCADE", () => {
      expect(REGISTRY_DDL).toContain(
        "project         TEXT        NOT NULL REFERENCES sqitch.projects(project) ON UPDATE CASCADE",
      );
      expect(REGISTRY_DDL).toContain(
        "change_id       TEXT        NOT NULL REFERENCES sqitch.changes(change_id) ON UPDATE CASCADE",
      );
    });

    it("defines sqitch.tags with UNIQUE (project, tag)", () => {
      expect(REGISTRY_DDL).toContain("UNIQUE (project, tag)");
    });

    // -- sqitch.dependencies --

    it("defines sqitch.dependencies with composite PK and cascading FKs", () => {
      expect(REGISTRY_DDL).toContain(
        "change_id    TEXT NOT NULL REFERENCES sqitch.changes(change_id) ON UPDATE CASCADE ON DELETE CASCADE",
      );
      expect(REGISTRY_DDL).toContain(
        "dependency_id TEXT NULL REFERENCES sqitch.changes(change_id) ON UPDATE CASCADE",
      );
      expect(REGISTRY_DDL).toContain("PRIMARY KEY (change_id, dependency)");
    });

    // -- sqitch.events --

    it("defines sqitch.events with CHECK constraint on event column", () => {
      expect(REGISTRY_DDL).toContain(
        "CHECK (event IN ('deploy', 'revert', 'fail', 'merge'))",
      );
    });

    it("defines sqitch.events with composite PK (change_id, committed_at)", () => {
      expect(REGISTRY_DDL).toContain("PRIMARY KEY (change_id, committed_at)");
    });

    it("defines sqitch.events with TEXT[] array columns", () => {
      expect(REGISTRY_DDL).toContain("requires        TEXT[]      NOT NULL DEFAULT '{}'");
      expect(REGISTRY_DDL).toContain("conflicts       TEXT[]      NOT NULL DEFAULT '{}'");
      expect(REGISTRY_DDL).toContain("tags            TEXT[]      NOT NULL DEFAULT '{}'");
    });
  });

  // -----------------------------------------------------------------------
  // REGISTRY_LOCK_KEY
  // -----------------------------------------------------------------------

  describe("REGISTRY_LOCK_KEY", () => {
    it("is a positive integer", () => {
      expect(typeof REGISTRY_LOCK_KEY).toBe("number");
      expect(REGISTRY_LOCK_KEY).toBeGreaterThan(0);
    });
  });

  // -----------------------------------------------------------------------
  // createRegistry()
  // -----------------------------------------------------------------------

  describe("createRegistry()", () => {
    it("acquires advisory lock, runs DDL, releases lock", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      await registry.createRegistry();

      const texts = queryTexts(pgClient);

      // Advisory lock acquired
      const lockQuery = pgClient.queries.find((q) =>
        q.text.includes("pg_advisory_lock"),
      );
      expect(lockQuery).toBeDefined();
      expect(lockQuery!.values).toEqual([REGISTRY_LOCK_KEY]);

      // DDL executed
      expect(texts).toContain(REGISTRY_DDL);

      // Advisory lock released
      const unlockQuery = pgClient.queries.find((q) =>
        q.text.includes("pg_advisory_unlock"),
      );
      expect(unlockQuery).toBeDefined();
      expect(unlockQuery!.values).toEqual([REGISTRY_LOCK_KEY]);

      // Lock acquired before DDL, DDL before unlock
      const lockIdx = texts.findIndex((t) => t.includes("pg_advisory_lock("));
      const ddlIdx = texts.indexOf(REGISTRY_DDL);
      const unlockIdx = texts.findIndex((t) => t.includes("pg_advisory_unlock"));
      expect(lockIdx).toBeLessThan(ddlIdx);
      expect(ddlIdx).toBeLessThan(unlockIdx);
    });

    it("releases advisory lock even on DDL error", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();

      // Make the DDL fail
      pgClient.queryResults = {};
      const origQuery = pgClient.query.bind(pgClient);
      let callCount = 0;
      pgClient.query = async (text: string, values?: unknown[]) => {
        callCount++;
        if (text === REGISTRY_DDL) {
          // Still record it so we can see it was attempted
          pgClient.queries.push({ text, values });
          throw new Error("simulated DDL failure");
        }
        return origQuery(text, values);
      };

      const registry = new Registry(client);

      await expect(registry.createRegistry()).rejects.toThrow(
        "simulated DDL failure",
      );

      // Unlock should still have been called
      const unlockQuery = pgClient.queries.find((q) =>
        q.text.includes("pg_advisory_unlock"),
      );
      expect(unlockQuery).toBeDefined();
    });
  });

  // -----------------------------------------------------------------------
  // getProject()
  // -----------------------------------------------------------------------

  describe("getProject()", () => {
    it("returns existing project when found", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      const projectRow = {
        project: "myproject",
        uri: "https://example.com/myproject",
        created_at: new Date("2025-01-01"),
        creator_name: "Test User",
        creator_email: "test@example.com",
      };

      // Set up the SELECT to return a result
      for (const key of Object.keys(pgClient.queryResults)) {
        delete pgClient.queryResults[key];
      }
      pgClient.query = async (text: string, values?: unknown[]) => {
        pgClient.queries.push({ text, values });
        if (text.includes("SELECT") && text.includes("sqitch.projects")) {
          return { rows: [projectRow], rowCount: 1, command: "SELECT" };
        }
        return { rows: [], rowCount: 0, command: "SELECT" };
      };

      const result = await registry.getProject(sampleProject);

      expect(result).toEqual(projectRow);

      // Should have queried with parameterized project name
      const selectQuery = pgClient.queries.find(
        (q) => q.text.includes("SELECT") && q.text.includes("sqitch.projects"),
      );
      expect(selectQuery).toBeDefined();
      expect(selectQuery!.values).toEqual(["myproject"]);
    });

    it("creates project when not found", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      const insertedRow = {
        project: "myproject",
        uri: "https://example.com/myproject",
        created_at: new Date("2025-01-01"),
        creator_name: "Test User",
        creator_email: "test@example.com",
      };

      pgClient.query = async (text: string, values?: unknown[]) => {
        pgClient.queries.push({ text, values });
        if (text.includes("SELECT") && text.includes("sqitch.projects") && !text.includes("INSERT")) {
          return { rows: [], rowCount: 0, command: "SELECT" };
        }
        if (text.includes("INSERT INTO sqitch.projects")) {
          return { rows: [insertedRow], rowCount: 1, command: "INSERT" };
        }
        return { rows: [], rowCount: 0, command: "SELECT" };
      };

      const result = await registry.getProject(sampleProject);

      expect(result).toEqual(insertedRow);

      // Should have attempted SELECT first, then INSERT
      const insertQuery = pgClient.queries.find((q) =>
        q.text.includes("INSERT INTO sqitch.projects"),
      );
      expect(insertQuery).toBeDefined();
      expect(insertQuery!.values).toEqual([
        "myproject",
        "https://example.com/myproject",
        "Test User",
        "test@example.com",
      ]);
    });

    it("uses parameterized queries (no SQL injection)", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      const maliciousInput = {
        project: "'; DROP TABLE sqitch.projects; --",
        uri: null,
        creator_name: "Attacker",
        creator_email: "evil@evil.com",
      };

      pgClient.query = async (text: string, values?: unknown[]) => {
        pgClient.queries.push({ text, values });
        if (text.includes("SELECT") && text.includes("sqitch.projects") && !text.includes("INSERT")) {
          return { rows: [], rowCount: 0, command: "SELECT" };
        }
        if (text.includes("INSERT")) {
          return {
            rows: [{ ...maliciousInput, created_at: new Date() }],
            rowCount: 1,
            command: "INSERT",
          };
        }
        return { rows: [], rowCount: 0, command: "SELECT" };
      };

      await registry.getProject(maliciousInput);

      // The SQL text must NOT contain the malicious input — it should
      // only appear in the parameterized values
      const insertQuery = pgClient.queries.find((q) =>
        q.text.includes("INSERT INTO sqitch.projects"),
      );
      expect(insertQuery).toBeDefined();
      expect(insertQuery!.text).not.toContain("DROP TABLE");
      expect(insertQuery!.values).toContain(maliciousInput.project);
    });
  });

  // -----------------------------------------------------------------------
  // getDeployedChanges()
  // -----------------------------------------------------------------------

  describe("getDeployedChanges()", () => {
    it("queries changes ordered by committed_at ASC", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      await registry.getDeployedChanges("myproject");

      const selectQuery = pgClient.queries.find(
        (q) => q.text.includes("sqitch.changes") && q.text.includes("SELECT"),
      );
      expect(selectQuery).toBeDefined();
      expect(selectQuery!.text).toContain("ORDER BY committed_at ASC");
      expect(selectQuery!.values).toEqual(["myproject"]);
    });

    it("returns rows from query result", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      const rows = [
        {
          change_id: "abc",
          script_hash: "hash1",
          change: "first",
          project: "myproject",
          note: "",
          committed_at: new Date("2025-01-01"),
          committer_name: "User",
          committer_email: "user@example.com",
          planned_at: new Date("2025-01-01"),
          planner_name: "User",
          planner_email: "user@example.com",
        },
      ];

      pgClient.query = async (text: string, values?: unknown[]) => {
        pgClient.queries.push({ text, values });
        if (text.includes("sqitch.changes") && text.includes("SELECT")) {
          return { rows, rowCount: 1, command: "SELECT" };
        }
        return { rows: [], rowCount: 0, command: "SELECT" };
      };

      const result = await registry.getDeployedChanges("myproject");
      expect(result).toEqual(rows);
    });
  });

  // -----------------------------------------------------------------------
  // getLastDeployedChange()
  // -----------------------------------------------------------------------

  describe("getLastDeployedChange()", () => {
    it("queries with ORDER BY committed_at DESC LIMIT 1", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      await registry.getLastDeployedChange("myproject");

      const selectQuery = pgClient.queries.find(
        (q) => q.text.includes("sqitch.changes") && q.text.includes("LIMIT 1"),
      );
      expect(selectQuery).toBeDefined();
      expect(selectQuery!.text).toContain("ORDER BY committed_at DESC");
      expect(selectQuery!.values).toEqual(["myproject"]);
    });

    it("returns null when no changes deployed", async () => {
      const client = await createConnectedClient();
      const registry = new Registry(client);

      const result = await registry.getLastDeployedChange("myproject");
      expect(result).toBeNull();
    });

    it("returns the change when found", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      const changeRow = {
        change_id: "abc",
        script_hash: "hash1",
        change: "latest",
        project: "myproject",
        note: "The latest change",
        committed_at: new Date("2025-06-01"),
        committer_name: "User",
        committer_email: "user@example.com",
        planned_at: new Date("2025-06-01"),
        planner_name: "User",
        planner_email: "user@example.com",
      };

      pgClient.query = async (text: string, values?: unknown[]) => {
        pgClient.queries.push({ text, values });
        if (text.includes("sqitch.changes") && text.includes("LIMIT 1")) {
          return { rows: [changeRow], rowCount: 1, command: "SELECT" };
        }
        return { rows: [], rowCount: 0, command: "SELECT" };
      };

      const result = await registry.getLastDeployedChange("myproject");
      expect(result).toEqual(changeRow);
    });
  });

  // -----------------------------------------------------------------------
  // getDeployedTags()
  // -----------------------------------------------------------------------

  describe("getDeployedTags()", () => {
    it("queries tags ordered by committed_at ASC", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      await registry.getDeployedTags("myproject");

      const selectQuery = pgClient.queries.find(
        (q) => q.text.includes("sqitch.tags") && q.text.includes("SELECT"),
      );
      expect(selectQuery).toBeDefined();
      expect(selectQuery!.text).toContain("ORDER BY committed_at ASC");
      expect(selectQuery!.values).toEqual(["myproject"]);
    });
  });

  // -----------------------------------------------------------------------
  // recordTag()
  // -----------------------------------------------------------------------

  describe("recordTag()", () => {
    it("inserts into sqitch.tags with all 10 parameterized values", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      await registry.recordTag(sampleTagInput);

      const insertQuery = pgClient.queries.find((q) =>
        q.text.includes("INSERT INTO sqitch.tags"),
      );
      expect(insertQuery).toBeDefined();
      expect(insertQuery!.values).toEqual([
        sampleTagInput.tag_id,
        sampleTagInput.tag,
        sampleTagInput.project,
        sampleTagInput.change_id,
        sampleTagInput.note,
        sampleTagInput.committer_name,
        sampleTagInput.committer_email,
        sampleTagInput.planned_at,
        sampleTagInput.planner_name,
        sampleTagInput.planner_email,
      ]);
    });

    it("does not embed values in SQL text", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      await registry.recordTag(sampleTagInput);

      const insertQuery = pgClient.queries.find((q) =>
        q.text.includes("INSERT INTO sqitch.tags"),
      );
      expect(insertQuery).toBeDefined();
      // SQL should use $1..$10 placeholders, not literal values
      expect(insertQuery!.text).toContain("$1");
      expect(insertQuery!.text).toContain("$10");
      expect(insertQuery!.text).not.toContain(sampleTagInput.tag_id);
    });
  });

  // -----------------------------------------------------------------------
  // recordDeploy()
  // -----------------------------------------------------------------------

  describe("recordDeploy()", () => {
    it("inserts into changes, events, and dependencies", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      await registry.recordDeploy(sampleDeployInput);

      // 1. Change inserted
      const changeInsert = pgClient.queries.find((q) =>
        q.text.includes("INSERT INTO sqitch.changes"),
      );
      expect(changeInsert).toBeDefined();
      expect(changeInsert!.values).toEqual([
        sampleDeployInput.change_id,
        sampleDeployInput.script_hash,
        sampleDeployInput.change,
        sampleDeployInput.project,
        sampleDeployInput.note,
        sampleDeployInput.committer_name,
        sampleDeployInput.committer_email,
        sampleDeployInput.planned_at,
        sampleDeployInput.planner_name,
        sampleDeployInput.planner_email,
      ]);

      // 2. Deploy event inserted
      const eventInsert = pgClient.queries.find((q) =>
        q.text.includes("INSERT INTO sqitch.events"),
      );
      expect(eventInsert).toBeDefined();
      expect(eventInsert!.values![0]).toBe("deploy");
      expect(eventInsert!.values).toEqual([
        "deploy",
        sampleDeployInput.change_id,
        sampleDeployInput.change,
        sampleDeployInput.project,
        sampleDeployInput.note,
        sampleDeployInput.requires,
        sampleDeployInput.conflicts,
        sampleDeployInput.tags,
        sampleDeployInput.committer_name,
        sampleDeployInput.committer_email,
        sampleDeployInput.planned_at,
        sampleDeployInput.planner_name,
        sampleDeployInput.planner_email,
      ]);

      // 3. Dependency inserted
      const depInsert = pgClient.queries.find((q) =>
        q.text.includes("INSERT INTO sqitch.dependencies"),
      );
      expect(depInsert).toBeDefined();
      expect(depInsert!.values).toEqual([
        sampleDeployInput.change_id,
        "require",
        "create_schema",
        "def789abc012",
      ]);
    });

    it("inserts change before event (ordering)", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      await registry.recordDeploy(sampleDeployInput);

      const texts = queryTexts(pgClient);
      const changeIdx = texts.findIndex((t) =>
        t.includes("INSERT INTO sqitch.changes"),
      );
      const eventIdx = texts.findIndex((t) =>
        t.includes("INSERT INTO sqitch.events"),
      );
      const depIdx = texts.findIndex((t) =>
        t.includes("INSERT INTO sqitch.dependencies"),
      );

      expect(changeIdx).toBeLessThan(eventIdx);
      expect(eventIdx).toBeLessThan(depIdx);
    });

    it("handles multiple dependencies", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      const input = {
        ...sampleDeployInput,
        dependencies: [
          { type: "require", dependency: "dep_a", dependency_id: "id_a" },
          { type: "require", dependency: "dep_b", dependency_id: "id_b" },
          { type: "conflict", dependency: "dep_c", dependency_id: null },
        ],
      };

      await registry.recordDeploy(input);

      const depInserts = pgClient.queries.filter((q) =>
        q.text.includes("INSERT INTO sqitch.dependencies"),
      );
      expect(depInserts.length).toBe(3);
      expect(depInserts[0]!.values).toEqual([input.change_id, "require", "dep_a", "id_a"]);
      expect(depInserts[1]!.values).toEqual([input.change_id, "require", "dep_b", "id_b"]);
      expect(depInserts[2]!.values).toEqual([input.change_id, "conflict", "dep_c", null]);
    });

    it("handles zero dependencies", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      const input = { ...sampleDeployInput, dependencies: [] };
      await registry.recordDeploy(input);

      const depInserts = pgClient.queries.filter((q) =>
        q.text.includes("INSERT INTO sqitch.dependencies"),
      );
      expect(depInserts.length).toBe(0);
    });

    it("uses parameterized queries throughout (no SQL injection)", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      const evilInput = {
        ...sampleDeployInput,
        change: "'; DROP TABLE sqitch.changes; --",
        note: "Robert'); DROP TABLE students;--",
      };

      await registry.recordDeploy(evilInput);

      // The SQL text of all INSERT queries must not contain the injected values
      const inserts = pgClient.queries.filter((q) => q.text.includes("INSERT"));
      for (const q of inserts) {
        expect(q.text).not.toContain("DROP TABLE");
        // Values should be in the params array, not the SQL text
        expect(q.values).toBeDefined();
        expect(q.values!.length).toBeGreaterThan(0);
      }
    });
  });

  // -----------------------------------------------------------------------
  // recordRevert()
  // -----------------------------------------------------------------------

  describe("recordRevert()", () => {
    it("inserts revert event, deletes dependencies and change", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      await registry.recordRevert(sampleDeployInput);

      // 1. Revert event inserted
      const eventInsert = pgClient.queries.find((q) =>
        q.text.includes("INSERT INTO sqitch.events"),
      );
      expect(eventInsert).toBeDefined();
      expect(eventInsert!.values![0]).toBe("revert");

      // 2. Dependencies deleted
      const depDelete = pgClient.queries.find((q) =>
        q.text.includes("DELETE FROM sqitch.dependencies"),
      );
      expect(depDelete).toBeDefined();
      expect(depDelete!.values).toEqual([sampleDeployInput.change_id]);

      // 3. Change deleted
      const changeDelete = pgClient.queries.find((q) =>
        q.text.includes("DELETE FROM sqitch.changes"),
      );
      expect(changeDelete).toBeDefined();
      expect(changeDelete!.values).toEqual([sampleDeployInput.change_id]);
    });

    it("inserts event before deleting change (correct ordering)", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      await registry.recordRevert(sampleDeployInput);

      const texts = queryTexts(pgClient);
      const eventIdx = texts.findIndex((t) =>
        t.includes("INSERT INTO sqitch.events"),
      );
      const depDeleteIdx = texts.findIndex((t) =>
        t.includes("DELETE FROM sqitch.dependencies"),
      );
      const changeDeleteIdx = texts.findIndex((t) =>
        t.includes("DELETE FROM sqitch.changes"),
      );

      expect(eventIdx).toBeLessThan(depDeleteIdx);
      expect(depDeleteIdx).toBeLessThan(changeDeleteIdx);
    });

    it("uses parameterized event values for revert", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      await registry.recordRevert(sampleDeployInput);

      const eventInsert = pgClient.queries.find((q) =>
        q.text.includes("INSERT INTO sqitch.events"),
      );
      expect(eventInsert).toBeDefined();
      expect(eventInsert!.values).toEqual([
        "revert",
        sampleDeployInput.change_id,
        sampleDeployInput.change,
        sampleDeployInput.project,
        sampleDeployInput.note,
        sampleDeployInput.requires,
        sampleDeployInput.conflicts,
        sampleDeployInput.tags,
        sampleDeployInput.committer_name,
        sampleDeployInput.committer_email,
        sampleDeployInput.planned_at,
        sampleDeployInput.planner_name,
        sampleDeployInput.planner_email,
      ]);
    });
  });

  // -----------------------------------------------------------------------
  // getPendingChanges()
  // -----------------------------------------------------------------------

  describe("getPendingChanges()", () => {
    it("returns change IDs not in deployed set", () => {
      const client = new DatabaseClient("postgresql://host/db");
      const registry = new Registry(client);

      const plan = ["a", "b", "c", "d", "e"];
      const deployed = new Set(["a", "c"]);

      const pending = registry.getPendingChanges(plan, deployed);
      expect(pending).toEqual(["b", "d", "e"]);
    });

    it("returns empty array when all deployed", () => {
      const client = new DatabaseClient("postgresql://host/db");
      const registry = new Registry(client);

      const plan = ["a", "b"];
      const deployed = new Set(["a", "b"]);

      const pending = registry.getPendingChanges(plan, deployed);
      expect(pending).toEqual([]);
    });

    it("returns all when none deployed", () => {
      const client = new DatabaseClient("postgresql://host/db");
      const registry = new Registry(client);

      const plan = ["a", "b", "c"];
      const deployed = new Set<string>();

      const pending = registry.getPendingChanges(plan, deployed);
      expect(pending).toEqual(["a", "b", "c"]);
    });

    it("preserves plan order", () => {
      const client = new DatabaseClient("postgresql://host/db");
      const registry = new Registry(client);

      const plan = ["z", "m", "a", "q"];
      const deployed = new Set(["m"]);

      const pending = registry.getPendingChanges(plan, deployed);
      expect(pending).toEqual(["z", "a", "q"]);
    });
  });
});
