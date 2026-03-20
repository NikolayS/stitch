import { describe, it, expect, beforeEach, spyOn, mock } from "bun:test";
import { resetConfig, setConfig } from "../../src/output";

// ---------------------------------------------------------------------------
// Mock pg/lib/client — we never want a real database connection in unit tests.
//
// We mock at the module level before importing DatabaseClient. Bun's mock()
// intercepts require("pg/lib/client") so DatabaseClient gets our fake.
// ---------------------------------------------------------------------------

// Captured instances for assertions
let mockInstances: MockPgClient[] = [];

class MockPgClient {
  options: Record<string, unknown>;
  queries: Array<{ text: string; values?: unknown[] }> = [];
  connected = false;
  ended = false;

  // Controllable behavior
  connectShouldFail = false;
  connectError = new Error("connection refused");
  queryResults: Record<string, { rows: unknown[]; rowCount: number; command: string }> = {};
  queryShouldFail: Record<string, Error> = {};

  constructor(options: Record<string, unknown>) {
    this.options = options;
    mockInstances.push(this);
  }

  async connect() {
    if (this.connectShouldFail) {
      throw this.connectError;
    }
    this.connected = true;
  }

  async query(text: string, values?: unknown[]) {
    this.queries.push({ text, values });

    if (this.queryShouldFail[text]) {
      throw this.queryShouldFail[text];
    }

    return (
      this.queryResults[text] ?? {
        rows: [],
        rowCount: 0,
        command: "SET",
      }
    );
  }

  async end() {
    this.ended = true;
    this.connected = false;
  }
}

// Register the mock before importing DatabaseClient
mock.module("pg/lib/client", () => ({
  default: MockPgClient,
  __esModule: true,
}));

// Now import — it will pick up our mock
const { DatabaseClient, EXIT_CODE_DB_UNREACHABLE } = await import(
  "../../src/db/client"
);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function captureStderr() {
  let output = "";
  const spy = spyOn(process.stderr, "write").mockImplementation(
    (chunk: string | Uint8Array) => {
      output += String(chunk);
      return true;
    },
  );
  return {
    get output() {
      return output;
    },
    restore() {
      spy.mockRestore();
    },
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("DatabaseClient", () => {
  beforeEach(() => {
    mockInstances = [];
    resetConfig();
  });

  // -----------------------------------------------------------------------
  // Construction
  // -----------------------------------------------------------------------

  describe("constructor", () => {
    it("accepts postgresql:// URI", () => {
      new DatabaseClient("postgresql://user:pass@host:5432/db");
      expect(mockInstances.length).toBe(1);
      expect(mockInstances[0]!.options.host).toBe("host");
      expect(mockInstances[0]!.options.port).toBe(5432);
      expect(mockInstances[0]!.options.database).toBe("db");
    });

    it("accepts db:pg:// URI", () => {
      new DatabaseClient("db:pg://user:pass@host:5433/mydb");
      expect(mockInstances.length).toBe(1);
      expect(mockInstances[0]!.options.host).toBe("host");
      expect(mockInstances[0]!.options.port).toBe(5433);
      expect(mockInstances[0]!.options.database).toBe("mydb");
    });

    it("throws on invalid URI scheme", () => {
      expect(() => new DatabaseClient("mysql://host/db")).toThrow(
        "Unsupported URI scheme",
      );
    });
  });

  // -----------------------------------------------------------------------
  // connect()
  // -----------------------------------------------------------------------

  describe("connect()", () => {
    it("connects and applies session settings", async () => {
      const client = new DatabaseClient("postgresql://user:pass@host/db", {
        command: "deploy",
        project: "myproject",
      });
      await client.connect();

      const pgClient = mockInstances[0]!;
      expect(pgClient.connected).toBe(true);
      expect(client.isConnected).toBe(true);

      // Session settings should have been applied
      const queryTexts = pgClient.queries.map((q) => q.text);
      expect(queryTexts).toContain("SET statement_timeout = 0");
      expect(queryTexts).toContain("SET lock_timeout = 5000");
      expect(queryTexts).toContain(
        "SET idle_in_transaction_session_timeout = 600000",
      );
      // application_name uses parameterized set_config() to avoid SQL injection
      const appNameQuery = pgClient.queries.find(
        (q) => q.text === "SELECT set_config('application_name', $1, false)",
      );
      expect(appNameQuery).toBeDefined();
      expect(appNameQuery!.values).toEqual(["sqlever/deploy/myproject"]);
    });

    it("applies custom session settings", async () => {
      const client = new DatabaseClient("postgresql://host/db", {
        command: "verify",
        project: "proj",
        lockTimeout: 10000,
        idleInTransactionSessionTimeout: 300000,
        statementTimeout: 60000,
      });
      await client.connect();

      const pgClient = mockInstances[0]!;
      const queryTexts = pgClient.queries.map((q) => q.text);
      expect(queryTexts).toContain("SET statement_timeout = 60000");
      expect(queryTexts).toContain("SET lock_timeout = 10000");
      expect(queryTexts).toContain(
        "SET idle_in_transaction_session_timeout = 300000",
      );
    });

    it("safely handles single quotes in application_name", async () => {
      const client = new DatabaseClient("postgresql://host/db", {
        command: "deploy",
        project: "O'Reilly's project",
      });
      await client.connect();

      const pgClient = mockInstances[0]!;
      // Should use parameterized set_config, not string interpolation
      const appNameQuery = pgClient.queries.find(
        (q) => q.text === "SELECT set_config('application_name', $1, false)",
      );
      expect(appNameQuery).toBeDefined();
      expect(appNameQuery!.values).toEqual([
        "sqlever/deploy/O'Reilly's project",
      ]);
      // Must NOT contain any SET application_name = '...' query
      const unsafeSetQuery = pgClient.queries.find(
        (q) => q.text.startsWith("SET application_name"),
      );
      expect(unsafeSetQuery).toBeUndefined();
    });

    it("exits with code 10 on connection failure", async () => {
      const exitSpy = spyOn(process, "exit").mockImplementation(
        (_code?: number) => {
          throw new Error(`process.exit(${_code})`);
        },
      );
      const stderr = captureStderr();

      try {
        const client = new DatabaseClient("postgresql://user:pass@host/db");
        const pgClient = mockInstances[0]!;
        pgClient.connectShouldFail = true;
        pgClient.connectError = new Error("ECONNREFUSED");

        await expect(client.connect()).rejects.toThrow("process.exit(10)");
        expect(exitSpy).toHaveBeenCalledWith(EXIT_CODE_DB_UNREACHABLE);
      } finally {
        exitSpy.mockRestore();
        stderr.restore();
      }
    });

    it("does not log password on connection failure", async () => {
      const exitSpy = spyOn(process, "exit").mockImplementation(
        (_code?: number) => {
          throw new Error(`process.exit(${_code})`);
        },
      );
      setConfig({ verbose: true });
      const stderr = captureStderr();

      try {
        const client = new DatabaseClient(
          "postgresql://admin:supersecret@host/db",
        );
        const pgClient = mockInstances[0]!;
        pgClient.connectShouldFail = true;

        await expect(client.connect()).rejects.toThrow("process.exit");
        expect(stderr.output).not.toContain("supersecret");
        expect(stderr.output).toContain("***");
      } finally {
        exitSpy.mockRestore();
        stderr.restore();
      }
    });
  });

  // -----------------------------------------------------------------------
  // disconnect()
  // -----------------------------------------------------------------------

  describe("disconnect()", () => {
    it("disconnects when connected", async () => {
      const client = new DatabaseClient("postgresql://host/db");
      await client.connect();
      expect(client.isConnected).toBe(true);

      await client.disconnect();
      expect(client.isConnected).toBe(false);
      expect(mockInstances[0]!.ended).toBe(true);
    });

    it("is a no-op when not connected", async () => {
      const client = new DatabaseClient("postgresql://host/db");
      await client.disconnect(); // Should not throw
      expect(client.isConnected).toBe(false);
    });
  });

  // -----------------------------------------------------------------------
  // query()
  // -----------------------------------------------------------------------

  describe("query()", () => {
    it("executes a parameterized query", async () => {
      const client = new DatabaseClient("postgresql://host/db");
      await client.connect();

      const pgClient = mockInstances[0]!;
      pgClient.queryResults["SELECT $1::int AS n"] = {
        rows: [{ n: 42 }],
        rowCount: 1,
        command: "SELECT",
      };

      const result = await client.query("SELECT $1::int AS n", [42]);
      expect(result.rows).toEqual([{ n: 42 }]);
      expect(result.rowCount).toBe(1);
      expect(result.command).toBe("SELECT");
    });

    it("throws when not connected", async () => {
      const client = new DatabaseClient("postgresql://host/db");
      await expect(client.query("SELECT 1")).rejects.toThrow(
        "not connected",
      );
    });
  });

  // -----------------------------------------------------------------------
  // transaction()
  // -----------------------------------------------------------------------

  describe("transaction()", () => {
    it("commits on success", async () => {
      const client = new DatabaseClient("postgresql://host/db");
      await client.connect();

      const pgClient = mockInstances[0]!;
      pgClient.queryResults["SELECT 1"] = {
        rows: [{ "?column?": 1 }],
        rowCount: 1,
        command: "SELECT",
      };

      const result = await client.transaction(async (c) => {
        return c.query("SELECT 1");
      });

      const queryTexts = pgClient.queries.map((q) => q.text);
      expect(queryTexts).toContain("BEGIN");
      expect(queryTexts).toContain("COMMIT");
      expect(queryTexts).not.toContain("ROLLBACK");
      expect(result.rows).toEqual([{ "?column?": 1 }]);
    });

    it("rolls back on error", async () => {
      const client = new DatabaseClient("postgresql://host/db");
      await client.connect();

      const pgClient = mockInstances[0]!;
      pgClient.queryShouldFail["INSERT INTO bad_table VALUES (1)"] =
        new Error("relation does not exist");

      await expect(
        client.transaction(async (c) => {
          await c.query("INSERT INTO bad_table VALUES (1)");
        }),
      ).rejects.toThrow("relation does not exist");

      const queryTexts = pgClient.queries.map((q) => q.text);
      expect(queryTexts).toContain("BEGIN");
      expect(queryTexts).toContain("ROLLBACK");
      expect(queryTexts).not.toContain("COMMIT");
    });

    it("throws when not connected", async () => {
      const client = new DatabaseClient("postgresql://host/db");
      await expect(
        client.transaction(async () => {}),
      ).rejects.toThrow("not connected");
    });
  });

  // -----------------------------------------------------------------------
  // EXIT_CODE_DB_UNREACHABLE
  // -----------------------------------------------------------------------

  describe("EXIT_CODE_DB_UNREACHABLE", () => {
    it("equals 10 per SPEC R6", () => {
      expect(EXIT_CODE_DB_UNREACHABLE).toBe(10);
    });
  });
});
