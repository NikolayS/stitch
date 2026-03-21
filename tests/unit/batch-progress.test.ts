import { describe, it, expect, beforeEach, mock } from "bun:test";

// ---------------------------------------------------------------------------
// Mock pg/lib/client — identical pattern to batch-queue.test.ts
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

const { DatabaseClient } = await import("../../src/db/client");
const {
  calculateProgress,
  parseIntervalToSeconds,
  computeDeadTupleRatio,
  checkHeartbeatStaleness,
  ProgressMonitor,
  DEFAULT_REPLICATION_LAG_THRESHOLD_SECONDS,
  DEFAULT_MAX_DEAD_TUPLE_RATIO,
  DEFAULT_HEARTBEAT_STALENESS_MS,
} = await import("../../src/batch/progress");

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function makeClient(): Promise<InstanceType<typeof DatabaseClient>> {
  const client = new DatabaseClient("postgresql://user@localhost/testdb");
  await client.connect();
  return client;
}

function findQuery(pgClient: MockPgClient, pattern: string | RegExp) {
  return pgClient.queries.find((q) =>
    typeof pattern === "string"
      ? q.text.includes(pattern)
      : pattern.test(q.text),
  );
}

/**
 * Set up a MockPgClient so that queries matching a pattern return specific rows.
 */
function setupQueryResponses(
  pgClient: MockPgClient,
  responses: Array<{
    pattern: string | RegExp;
    rows: unknown[];
    rowCount?: number;
    command?: string;
  }>,
) {
  const origQuery = pgClient.query.bind(pgClient);
  pgClient.query = async (text: string, values?: unknown[]) => {
    for (const r of responses) {
      const match =
        typeof r.pattern === "string"
          ? text.includes(r.pattern)
          : r.pattern.test(text);
      if (match) {
        pgClient.queries.push({ text, values });
        return {
          rows: r.rows,
          rowCount: r.rowCount ?? r.rows.length,
          command: r.command ?? "SELECT",
        };
      }
    }
    return origQuery(text, values);
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("batch/progress", () => {
  beforeEach(() => {
    mockInstances = [];
  });

  // -----------------------------------------------------------------------
  // Constants
  // -----------------------------------------------------------------------

  describe("constants", () => {
    it("DEFAULT_REPLICATION_LAG_THRESHOLD_SECONDS is 10", () => {
      expect(DEFAULT_REPLICATION_LAG_THRESHOLD_SECONDS).toBe(10);
    });

    it("DEFAULT_MAX_DEAD_TUPLE_RATIO is 0.10", () => {
      expect(DEFAULT_MAX_DEAD_TUPLE_RATIO).toBe(0.10);
    });

    it("DEFAULT_HEARTBEAT_STALENESS_MS is 5 minutes", () => {
      expect(DEFAULT_HEARTBEAT_STALENESS_MS).toBe(300_000);
    });
  });

  // -----------------------------------------------------------------------
  // calculateProgress()
  // -----------------------------------------------------------------------

  describe("calculateProgress()", () => {
    it("computes percentage correctly for partial completion", () => {
      const p = calculateProgress(250, 1000, 5000);
      expect(p.percentage).toBe(25);
      expect(p.rowsDone).toBe(250);
      expect(p.rowsTotal).toBe(1000);
    });

    it("computes throughput (rows per second)", () => {
      // 500 rows in 2000ms = 250 rows/sec
      const p = calculateProgress(500, 1000, 2000);
      expect(p.rowsPerSecond).toBe(250);
    });

    it("computes ETA from observed throughput", () => {
      // 500 done in 2000ms => 250 rows/sec => 500 remaining / 250 = 2s = 2000ms
      const p = calculateProgress(500, 1000, 2000);
      expect(p.etaMs).toBe(2000);
    });

    it("returns null ETA when no rows processed yet", () => {
      const p = calculateProgress(0, 1000, 0);
      expect(p.etaMs).toBeNull();
    });

    it("returns 100% when rowsTotal is 0", () => {
      const p = calculateProgress(0, 0, 1000);
      expect(p.percentage).toBe(100);
    });

    it("clamps rowsDone to rowsTotal when it exceeds total", () => {
      const p = calculateProgress(1500, 1000, 5000);
      expect(p.rowsDone).toBe(1000);
      expect(p.percentage).toBe(100);
    });

    it("returns ETA of 0 when all rows are done", () => {
      const p = calculateProgress(1000, 1000, 5000);
      expect(p.etaMs).toBe(0);
    });
  });

  // -----------------------------------------------------------------------
  // parseIntervalToSeconds()
  // -----------------------------------------------------------------------

  describe("parseIntervalToSeconds()", () => {
    it("parses HH:MM:SS format", () => {
      expect(parseIntervalToSeconds("00:00:05")).toBe(5);
    });

    it("parses HH:MM:SS.fractional format", () => {
      const result = parseIntervalToSeconds("00:00:05.123");
      expect(result).toBeCloseTo(5.123, 2);
    });

    it("parses hours and minutes", () => {
      expect(parseIntervalToSeconds("01:30:00")).toBe(5400);
    });

    it("parses days prefix", () => {
      expect(parseIntervalToSeconds("1 day 00:00:00")).toBe(86400);
    });

    it("returns 0 for null", () => {
      expect(parseIntervalToSeconds(null)).toBe(0);
    });

    it("returns 0 for empty string", () => {
      expect(parseIntervalToSeconds("")).toBe(0);
    });

    it("parses bare number as seconds", () => {
      expect(parseIntervalToSeconds("12.5")).toBe(12.5);
    });
  });

  // -----------------------------------------------------------------------
  // computeDeadTupleRatio()
  // -----------------------------------------------------------------------

  describe("computeDeadTupleRatio()", () => {
    it("computes correct ratio", () => {
      // 100 dead / (900 live + 100 dead) = 0.1
      expect(computeDeadTupleRatio(100, 900)).toBeCloseTo(0.1, 5);
    });

    it("returns 0 when both are 0 (empty table)", () => {
      expect(computeDeadTupleRatio(0, 0)).toBe(0);
    });

    it("returns 1 when all tuples are dead", () => {
      expect(computeDeadTupleRatio(1000, 0)).toBe(1);
    });

    it("returns 0 when no dead tuples", () => {
      expect(computeDeadTupleRatio(0, 1000)).toBe(0);
    });

    it("handles large numbers correctly", () => {
      // 1M dead / 10M total = 0.1
      const ratio = computeDeadTupleRatio(1_000_000, 9_000_000);
      expect(ratio).toBeCloseTo(0.1, 5);
    });
  });

  // -----------------------------------------------------------------------
  // checkHeartbeatStaleness()
  // -----------------------------------------------------------------------

  describe("checkHeartbeatStaleness()", () => {
    it("detects stale heartbeat when age exceeds threshold", () => {
      const now = new Date("2025-01-01T00:10:00Z");
      const heartbeat = new Date("2025-01-01T00:00:00Z"); // 10 min ago
      const result = checkHeartbeatStaleness(heartbeat, 300_000, now);
      expect(result.isStale).toBe(true);
      expect(result.ageMs).toBe(600_000); // 10 minutes
    });

    it("detects fresh heartbeat within threshold", () => {
      const now = new Date("2025-01-01T00:02:00Z");
      const heartbeat = new Date("2025-01-01T00:00:00Z"); // 2 min ago
      const result = checkHeartbeatStaleness(heartbeat, 300_000, now);
      expect(result.isStale).toBe(false);
      expect(result.ageMs).toBe(120_000);
    });

    it("treats null heartbeat as always stale", () => {
      const result = checkHeartbeatStaleness(null, 300_000);
      expect(result.isStale).toBe(true);
      expect(result.ageMs).toBe(Infinity);
    });

    it("uses configurable threshold", () => {
      const now = new Date("2025-01-01T00:01:30Z");
      const heartbeat = new Date("2025-01-01T00:00:00Z"); // 90s ago
      // 60s threshold — 90s is stale
      expect(checkHeartbeatStaleness(heartbeat, 60_000, now).isStale).toBe(
        true,
      );
      // 120s threshold — 90s is fresh
      expect(checkHeartbeatStaleness(heartbeat, 120_000, now).isStale).toBe(
        false,
      );
    });
  });

  // -----------------------------------------------------------------------
  // ProgressMonitor — replication lag
  // -----------------------------------------------------------------------

  describe("ProgressMonitor.getReplicationLag()", () => {
    it("returns lag info from pg_stat_replication", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        {
          pattern: "pg_stat_replication",
          rows: [{ replay_lag: "00:00:15.000", application_name: "replica1" }],
        },
      ]);

      const monitor = new ProgressMonitor(client);
      const lag = await monitor.getReplicationLag();

      expect(lag.replayLagSeconds).toBe(15);
      expect(lag.exceedsThreshold).toBe(true);
      expect(lag.thresholdSeconds).toBe(10);
      expect(lag.replicaName).toBe("replica1");
    });

    it("returns zero lag when no replicas exist", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        { pattern: "pg_stat_replication", rows: [] },
      ]);

      const monitor = new ProgressMonitor(client);
      const lag = await monitor.getReplicationLag();

      expect(lag.replayLagSeconds).toBe(0);
      expect(lag.exceedsThreshold).toBe(false);
      expect(lag.replicaName).toBeNull();
    });

    it("uses configurable threshold", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        {
          pattern: "pg_stat_replication",
          rows: [{ replay_lag: "00:00:08.000", application_name: "replica1" }],
        },
      ]);

      // 8s lag with 5s threshold => exceeds
      const monitor = new ProgressMonitor(client, {
        replicationLagThresholdSeconds: 5,
      });
      const lag = await monitor.getReplicationLag();

      expect(lag.exceedsThreshold).toBe(true);
      expect(lag.thresholdSeconds).toBe(5);
    });
  });

  // -----------------------------------------------------------------------
  // ProgressMonitor — VACUUM pressure
  // -----------------------------------------------------------------------

  describe("ProgressMonitor.getVacuumPressure()", () => {
    it("computes dead tuple ratio from pg_stat_user_tables", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        {
          pattern: "pg_stat_user_tables",
          rows: [{ n_dead_tup: "150", n_live_tup: "850" }],
        },
      ]);

      const monitor = new ProgressMonitor(client);
      const pressure = await monitor.getVacuumPressure("public", "users");

      expect(pressure.deadTuples).toBe(150);
      expect(pressure.liveTuples).toBe(850);
      expect(pressure.deadTupleRatio).toBe(0.15);
      expect(pressure.exceedsThreshold).toBe(true);
      expect(pressure.tableName).toBe("public.users");
    });

    it("returns no pressure when table not found in stats", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        { pattern: "pg_stat_user_tables", rows: [] },
      ]);

      const monitor = new ProgressMonitor(client);
      const pressure = await monitor.getVacuumPressure(
        "public",
        "nonexistent",
      );

      expect(pressure.deadTupleRatio).toBe(0);
      expect(pressure.exceedsThreshold).toBe(false);
    });

    it("uses configurable threshold", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        {
          pattern: "pg_stat_user_tables",
          rows: [{ n_dead_tup: "50", n_live_tup: "950" }],
        },
      ]);

      // 5% ratio with 3% threshold => exceeds
      const monitor = new ProgressMonitor(client, {
        maxDeadTupleRatio: 0.03,
      });
      const pressure = await monitor.getVacuumPressure("public", "users");

      expect(pressure.exceedsThreshold).toBe(true);
      expect(pressure.thresholdRatio).toBe(0.03);
    });

    it("queries with correct schema and table parameters", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        {
          pattern: "pg_stat_user_tables",
          rows: [{ n_dead_tup: "0", n_live_tup: "100" }],
        },
      ]);

      const monitor = new ProgressMonitor(client);
      await monitor.getVacuumPressure("myschema", "orders");

      const q = findQuery(pgClient, "pg_stat_user_tables");
      expect(q!.values).toEqual(["myschema", "orders"]);
    });
  });

  // -----------------------------------------------------------------------
  // ProgressMonitor — heartbeat staleness
  // -----------------------------------------------------------------------

  describe("ProgressMonitor.checkJobHeartbeat()", () => {
    it("queries batch_jobs for heartbeat_at", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      const recentHeartbeat = new Date();

      setupQueryResponses(pgClient, [
        {
          pattern: "batch_jobs",
          rows: [{ heartbeat_at: recentHeartbeat }],
        },
      ]);

      const monitor = new ProgressMonitor(client);
      const status = await monitor.checkJobHeartbeat(42, 0);

      expect(status.jobId).toBe(42);
      expect(status.lastHeartbeat).toBe(recentHeartbeat);
      expect(status.isStale).toBe(false);
    });

    it("marks missing job as stale", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        { pattern: "batch_jobs", rows: [] },
      ]);

      const monitor = new ProgressMonitor(client);
      const status = await monitor.checkJobHeartbeat(999, 0);

      expect(status.isStale).toBe(true);
      expect(status.ageMs).toBe(Infinity);
    });
  });

  // -----------------------------------------------------------------------
  // ProgressMonitor — pg_stat_activity
  // -----------------------------------------------------------------------

  describe("ProgressMonitor.getBatchQueries()", () => {
    it("returns batch queries from pg_stat_activity", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        {
          pattern: "pg_stat_activity",
          rows: [
            {
              pid: 1234,
              query: "UPDATE users SET tier = 'gold'",
              duration_ms: "5000",
              wait_event_type: null,
              wait_event: null,
              state: "active",
              application_name: "sqlever/batch/backfill",
            },
          ],
        },
      ]);

      const monitor = new ProgressMonitor(client);
      const queries = await monitor.getBatchQueries();

      expect(queries).toHaveLength(1);
      expect(queries[0]!.pid).toBe(1234);
      expect(queries[0]!.query).toBe("UPDATE users SET tier = 'gold'");
      expect(queries[0]!.durationMs).toBe(5000);
      expect(queries[0]!.state).toBe("active");
      expect(queries[0]!.applicationName).toBe("sqlever/batch/backfill");
    });

    it("returns empty array when no batch queries are running", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        { pattern: "pg_stat_activity", rows: [] },
      ]);

      const monitor = new ProgressMonitor(client);
      const queries = await monitor.getBatchQueries();

      expect(queries).toHaveLength(0);
    });

    it("filters by sqlever application_name", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        { pattern: "pg_stat_activity", rows: [] },
      ]);

      const monitor = new ProgressMonitor(client);
      await monitor.getBatchQueries();

      const q = findQuery(pgClient, "pg_stat_activity");
      expect(q!.text).toContain("application_name LIKE 'sqlever/%'");
    });
  });

  // -----------------------------------------------------------------------
  // ProgressMonitor — shouldPause (combined check)
  // -----------------------------------------------------------------------

  describe("ProgressMonitor.shouldPause()", () => {
    it("returns null when no concerns", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        {
          pattern: "pg_stat_replication",
          rows: [{ replay_lag: "00:00:02.000", application_name: "r1" }],
        },
        {
          pattern: "pg_stat_user_tables",
          rows: [{ n_dead_tup: "10", n_live_tup: "990" }],
        },
      ]);

      const monitor = new ProgressMonitor(client);
      const reason = await monitor.shouldPause("public", "users");

      expect(reason).toBeNull();
    });

    it("returns replication lag reason when lag exceeds threshold", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        {
          pattern: "pg_stat_replication",
          rows: [{ replay_lag: "00:00:15.000", application_name: "replica1" }],
        },
        {
          pattern: "pg_stat_user_tables",
          rows: [{ n_dead_tup: "10", n_live_tup: "990" }],
        },
      ]);

      const monitor = new ProgressMonitor(client);
      const reason = await monitor.shouldPause("public", "users");

      expect(reason).toContain("Replication lag");
      expect(reason).toContain("15.0s");
      expect(reason).toContain("replica1");
    });

    it("returns VACUUM pressure reason when ratio exceeds threshold", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        {
          pattern: "pg_stat_replication",
          rows: [{ replay_lag: "00:00:02.000", application_name: "r1" }],
        },
        {
          pattern: "pg_stat_user_tables",
          rows: [{ n_dead_tup: "200", n_live_tup: "800" }],
        },
      ]);

      const monitor = new ProgressMonitor(client);
      const reason = await monitor.shouldPause("public", "users");

      expect(reason).toContain("Dead tuple ratio");
      expect(reason).toContain("20.0%");
    });

    it("prioritizes replication lag over VACUUM pressure", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      // Both exceed thresholds
      setupQueryResponses(pgClient, [
        {
          pattern: "pg_stat_replication",
          rows: [{ replay_lag: "00:00:20.000", application_name: "r1" }],
        },
        {
          pattern: "pg_stat_user_tables",
          rows: [{ n_dead_tup: "500", n_live_tup: "500" }],
        },
      ]);

      const monitor = new ProgressMonitor(client);
      const reason = await monitor.shouldPause("public", "users");

      // Replication lag is checked first
      expect(reason).toContain("Replication lag");
    });
  });

  // -----------------------------------------------------------------------
  // ProgressMonitor — findStaleWorkers
  // -----------------------------------------------------------------------

  describe("ProgressMonitor.findStaleWorkers()", () => {
    it("returns stale workers from batch_jobs", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      const staleDate = new Date(Date.now() - 600_000); // 10 min ago

      setupQueryResponses(pgClient, [
        {
          pattern: "batch_jobs",
          rows: [{ id: 1, heartbeat_at: staleDate }],
        },
      ]);

      const monitor = new ProgressMonitor(client);
      const stale = await monitor.findStaleWorkers();

      expect(stale).toHaveLength(1);
      expect(stale[0]!.jobId).toBe(1);
      expect(stale[0]!.isStale).toBe(true);
    });

    it("passes staleness threshold to the query", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        { pattern: "batch_jobs", rows: [] },
      ]);

      const monitor = new ProgressMonitor(client, {
        heartbeatStalenessMs: 60_000,
      });
      await monitor.findStaleWorkers();

      const q = findQuery(pgClient, "batch_jobs");
      expect(q!.values).toContain(60_000);
    });
  });
});
