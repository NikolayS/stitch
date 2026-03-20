import { describe, it, expect, beforeEach, spyOn, mock } from "bun:test";
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
const { Registry } = await import("../../src/db/registry");
const { parseLogArgs, formatEventsText, formatEventsJson } = await import(
  "../../src/commands/log"
);
const { parseArgs } = await import("../../src/cli");

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

function captureWrites() {
  let stdout = "";
  let stderr = "";

  const stdoutSpy = spyOn(process.stdout, "write").mockImplementation(
    (chunk: string | Uint8Array) => {
      stdout += String(chunk);
      return true;
    },
  );

  const stderrSpy = spyOn(process.stderr, "write").mockImplementation(
    (chunk: string | Uint8Array) => {
      stderr += String(chunk);
      return true;
    },
  );

  return {
    get stdout() {
      return stdout;
    },
    get stderr() {
      return stderr;
    },
    restore() {
      stdoutSpy.mockRestore();
      stderrSpy.mockRestore();
    },
  };
}

// Sample event data
function makeSampleEvent(overrides: Partial<Record<string, unknown>> = {}): Record<string, unknown> {
  return {
    event: "deploy",
    change_id: "abc123",
    change: "add_users_table",
    project: "myproject",
    note: "Add users table",
    requires: [],
    conflicts: [],
    tags: [],
    committed_at: new Date("2025-06-15T10:30:00Z"),
    committer_name: "Test User",
    committer_email: "test@example.com",
    planned_at: new Date("2025-06-15T09:00:00Z"),
    planner_name: "Plan User",
    planner_email: "plan@example.com",
    ...overrides,
  };
}

const stubParsedArgs = {
  command: "log",
  rest: [] as string[],
  help: false,
  version: false,
  format: "text" as const,
  quiet: false,
  verbose: false,
  dbUri: undefined,
  planFile: undefined,
  topDir: undefined,
  registry: undefined,
  target: undefined,
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("log command", () => {
  beforeEach(() => {
    mockInstances = [];
    resetConfig();
  });

  // -----------------------------------------------------------------------
  // parseLogArgs
  // -----------------------------------------------------------------------

  describe("parseLogArgs()", () => {
    it("returns empty options when no flags given", () => {
      const opts = parseLogArgs([], stubParsedArgs);
      expect(opts).toEqual({});
    });

    it("parses --event deploy", () => {
      const opts = parseLogArgs(["--event", "deploy"], stubParsedArgs);
      expect(opts.event).toBe("deploy");
    });

    it("parses --event revert", () => {
      const opts = parseLogArgs(["--event", "revert"], stubParsedArgs);
      expect(opts.event).toBe("revert");
    });

    it("parses --event fail", () => {
      const opts = parseLogArgs(["--event", "fail"], stubParsedArgs);
      expect(opts.event).toBe("fail");
    });

    it("throws on invalid --event value", () => {
      expect(() =>
        parseLogArgs(["--event", "invalid"], stubParsedArgs),
      ).toThrow("Invalid --event value 'invalid'");
    });

    it("parses --limit N", () => {
      const opts = parseLogArgs(["--limit", "10"], stubParsedArgs);
      expect(opts.limit).toBe(10);
    });

    it("throws on non-integer --limit", () => {
      expect(() =>
        parseLogArgs(["--limit", "abc"], stubParsedArgs),
      ).toThrow("Invalid --limit value 'abc'");
    });

    it("throws on negative --limit", () => {
      expect(() =>
        parseLogArgs(["--limit", "-5"], stubParsedArgs),
      ).toThrow("Invalid --limit value '-5'");
    });

    it("parses --offset N", () => {
      const opts = parseLogArgs(["--offset", "5"], stubParsedArgs);
      expect(opts.offset).toBe(5);
    });

    it("throws on invalid --offset value", () => {
      expect(() =>
        parseLogArgs(["--offset", "xyz"], stubParsedArgs),
      ).toThrow("Invalid --offset value 'xyz'");
    });

    it("parses --reverse flag", () => {
      const opts = parseLogArgs(["--reverse"], stubParsedArgs);
      expect(opts.reverse).toBe(true);
    });

    it("parses --format json", () => {
      const opts = parseLogArgs(["--format", "json"], stubParsedArgs);
      expect(opts.format).toBe("json");
    });

    it("throws on invalid --format value", () => {
      expect(() =>
        parseLogArgs(["--format", "csv"], stubParsedArgs),
      ).toThrow("Invalid --format value 'csv'");
    });

    it("parses all flags together", () => {
      const opts = parseLogArgs(
        ["--event", "deploy", "--limit", "20", "--offset", "10", "--reverse", "--format", "json"],
        stubParsedArgs,
      );
      expect(opts.event).toBe("deploy");
      expect(opts.limit).toBe(20);
      expect(opts.offset).toBe(10);
      expect(opts.reverse).toBe(true);
      expect(opts.format).toBe("json");
    });
  });

  // -----------------------------------------------------------------------
  // Registry.getEvents()
  // -----------------------------------------------------------------------

  describe("Registry.getEvents()", () => {
    it("queries sqitch.events with project filter", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      await registry.getEvents("myproject");

      const selectQuery = pgClient.queries.find(
        (q) => q.text.includes("sqitch.events") && q.text.includes("SELECT"),
      );
      expect(selectQuery).toBeDefined();
      expect(selectQuery!.text).toContain("project = $1");
      expect(selectQuery!.values).toEqual(["myproject"]);
    });

    it("orders by committed_at DESC by default (newest-first)", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      await registry.getEvents("myproject");

      const selectQuery = pgClient.queries.find(
        (q) => q.text.includes("sqitch.events"),
      );
      expect(selectQuery!.text).toContain("ORDER BY committed_at DESC");
    });

    it("orders by committed_at ASC when reverse=true", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      await registry.getEvents("myproject", { reverse: true });

      const selectQuery = pgClient.queries.find(
        (q) => q.text.includes("sqitch.events"),
      );
      expect(selectQuery!.text).toContain("ORDER BY committed_at ASC");
    });

    it("adds event filter when event option is provided", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      await registry.getEvents("myproject", { event: "deploy" });

      const selectQuery = pgClient.queries.find(
        (q) => q.text.includes("sqitch.events"),
      );
      expect(selectQuery!.text).toContain("event = $2");
      expect(selectQuery!.values).toEqual(["myproject", "deploy"]);
    });

    it("adds LIMIT clause when limit option is provided", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      await registry.getEvents("myproject", { limit: 10 });

      const selectQuery = pgClient.queries.find(
        (q) => q.text.includes("sqitch.events"),
      );
      expect(selectQuery!.text).toContain("LIMIT $2");
      expect(selectQuery!.values).toEqual(["myproject", 10]);
    });

    it("adds OFFSET clause when offset option is provided", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      await registry.getEvents("myproject", { offset: 5 });

      const selectQuery = pgClient.queries.find(
        (q) => q.text.includes("sqitch.events"),
      );
      expect(selectQuery!.text).toContain("OFFSET $2");
      expect(selectQuery!.values).toEqual(["myproject", 5]);
    });

    it("combines event, limit, and offset with correct param indices", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      await registry.getEvents("myproject", {
        event: "revert",
        limit: 25,
        offset: 50,
      });

      const selectQuery = pgClient.queries.find(
        (q) => q.text.includes("sqitch.events"),
      );
      expect(selectQuery!.text).toContain("event = $2");
      expect(selectQuery!.text).toContain("LIMIT $3");
      expect(selectQuery!.text).toContain("OFFSET $4");
      expect(selectQuery!.values).toEqual(["myproject", "revert", 25, 50]);
    });

    it("returns rows from query result", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      const eventRows = [makeSampleEvent(), makeSampleEvent({ change: "second_change", event: "revert" })];

      pgClient.query = async (text: string, values?: unknown[]) => {
        pgClient.queries.push({ text, values });
        if (text.includes("sqitch.events") && text.includes("SELECT")) {
          return { rows: eventRows, rowCount: eventRows.length, command: "SELECT" };
        }
        return { rows: [], rowCount: 0, command: "SELECT" };
      };

      const result = await registry.getEvents("myproject");
      expect(result).toEqual(eventRows);
      expect(result.length).toBe(2);
    });

    it("returns empty array when no events match", async () => {
      const client = await createConnectedClient();
      const registry = new Registry(client);

      const result = await registry.getEvents("myproject", { event: "fail" });
      expect(result).toEqual([]);
    });

    it("uses parameterized queries — no SQL injection", async () => {
      const client = await createConnectedClient();
      const pgClient = getPgClient();
      const registry = new Registry(client);

      await registry.getEvents("'; DROP TABLE sqitch.events; --", {
        event: "deploy",
      });

      const selectQuery = pgClient.queries.find(
        (q) => q.text.includes("sqitch.events"),
      );
      expect(selectQuery!.text).not.toContain("DROP TABLE");
      expect(selectQuery!.values).toContain("'; DROP TABLE sqitch.events; --");
    });
  });

  // -----------------------------------------------------------------------
  // formatEventsText()
  // -----------------------------------------------------------------------

  describe("formatEventsText()", () => {
    it("prints 'No events found.' when events array is empty", () => {
      const cap = captureWrites();
      try {
        formatEventsText([]);
        expect(cap.stdout).toContain("No events found.");
      } finally {
        cap.restore();
      }
    });

    it("prints a table with headers for non-empty events", () => {
      const cap = captureWrites();
      try {
        const events = [makeSampleEvent()] as any[];
        formatEventsText(events);
        expect(cap.stdout).toContain("event");
        expect(cap.stdout).toContain("change");
        expect(cap.stdout).toContain("committed_at");
        expect(cap.stdout).toContain("committer");
        expect(cap.stdout).toContain("add_users_table");
        expect(cap.stdout).toContain("deploy");
        expect(cap.stdout).toContain("Test User");
      } finally {
        cap.restore();
      }
    });

    it("formats dates as ISO without milliseconds", () => {
      const cap = captureWrites();
      try {
        const events = [makeSampleEvent()] as any[];
        formatEventsText(events);
        // The date should be formatted without .000Z
        expect(cap.stdout).toContain("2025-06-15 10:30:00Z");
        expect(cap.stdout).not.toContain(".000Z");
      } finally {
        cap.restore();
      }
    });
  });

  // -----------------------------------------------------------------------
  // formatEventsJson()
  // -----------------------------------------------------------------------

  describe("formatEventsJson()", () => {
    it("outputs valid JSON to stdout", () => {
      const cap = captureWrites();
      try {
        const events = [makeSampleEvent()] as any[];
        formatEventsJson(events);
        const parsed = JSON.parse(cap.stdout);
        expect(Array.isArray(parsed)).toBe(true);
        expect(parsed.length).toBe(1);
        expect(parsed[0].event).toBe("deploy");
        expect(parsed[0].change).toBe("add_users_table");
      } finally {
        cap.restore();
      }
    });

    it("outputs empty array for no events", () => {
      const cap = captureWrites();
      try {
        formatEventsJson([]);
        const parsed = JSON.parse(cap.stdout);
        expect(parsed).toEqual([]);
      } finally {
        cap.restore();
      }
    });
  });

  // -----------------------------------------------------------------------
  // CLI integration — parseArgs for log command
  // -----------------------------------------------------------------------

  describe("CLI integration", () => {
    it("parseArgs recognizes 'log' as a command", () => {
      const args = parseArgs(["log"]);
      expect(args.command).toBe("log");
    });

    it("parseArgs passes remaining flags to rest", () => {
      const args = parseArgs(["log", "--event", "deploy", "--limit", "5"]);
      expect(args.command).toBe("log");
      expect(args.rest).toEqual(["--event", "deploy", "--limit", "5"]);
    });

    it("parseArgs handles global flags alongside log command", () => {
      const args = parseArgs(["--quiet", "log", "--reverse"]);
      expect(args.command).toBe("log");
      expect(args.quiet).toBe(true);
      expect(args.rest).toEqual(["--reverse"]);
    });

    it("parseArgs handles --db-uri before log command", () => {
      const args = parseArgs(["--db-uri", "postgresql://host/db", "log"]);
      expect(args.command).toBe("log");
      expect(args.dbUri).toBe("postgresql://host/db");
    });
  });
});
