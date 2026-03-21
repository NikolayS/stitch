import { describe, it, expect, beforeEach, mock } from "bun:test";

// ---------------------------------------------------------------------------
// Mock pg/lib/client — identical pattern to client.test.ts
// ---------------------------------------------------------------------------

let mockInstances: MockPgClient[] = [];

class MockPgClient {
  options: Record<string, unknown>;
  queries: Array<{ text: string; values?: unknown[] }> = [];
  connected = false;
  ended = false;
  queryResults: Record<string, { rows: unknown[]; rowCount: number; command: string }> = {};
  queryShouldFail: Record<string, Error> = {};

  constructor(options: Record<string, unknown>) {
    this.options = options;
    mockInstances.push(this);
  }

  async connect() {
    this.connected = true;
  }

  async query(text: string, values?: unknown[]) {
    this.queries.push({ text, values });
    if (this.queryShouldFail[text]) {
      throw this.queryShouldFail[text];
    }
    return (
      this.queryResults[text] ?? { rows: [], rowCount: 0, command: "SELECT" }
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

const { DatabaseClient } = await import("../../src/db/client");
const {
  BatchQueue,
  generateDDL,
  splitStatements,
  isValidTransition,
  VALID_TRANSITIONS,
  PARTITION_COUNT,
  DEFAULT_HEARTBEAT_STALENESS_MS,
  DEFAULT_BATCH_SIZE,
  DEFAULT_SLEEP_MS,
  DEFAULT_MAX_RETRIES,
} = await import("../../src/batch/queue");

import type {
  BatchJob,
  JobStatus,
  PartitionId,
} from "../../src/batch/queue";

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

function allQueriesMatching(pgClient: MockPgClient, pattern: string | RegExp) {
  return pgClient.queries.filter((q) =>
    typeof pattern === "string"
      ? q.text.includes(pattern)
      : pattern.test(q.text),
  );
}

/** Create a mock job row for test returns. */
function mockJob(overrides: Partial<BatchJob> = {}): BatchJob {
  return {
    id: 1,
    name: "backfill_tiers",
    status: "pending",
    partition_id: 0 as PartitionId,
    table_name: "users",
    batch_size: DEFAULT_BATCH_SIZE,
    sleep_ms: DEFAULT_SLEEP_MS,
    last_pk: null,
    attempt: 0,
    max_retries: DEFAULT_MAX_RETRIES,
    error_message: null,
    heartbeat_at: null,
    created_at: new Date("2025-01-01"),
    updated_at: new Date("2025-01-01"),
    ...overrides,
  };
}

/**
 * Set up a MockPgClient so that queries matching a pattern return specific
 * rows. Uses a simple approach: intercepts the query method.
 */
function setupQueryResponses(
  pgClient: MockPgClient,
  responses: Array<{ pattern: string | RegExp; rows: unknown[]; rowCount?: number; command?: string }>,
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

describe("batch/queue", () => {
  beforeEach(() => {
    mockInstances = [];
  });

  // -----------------------------------------------------------------------
  // Constants
  // -----------------------------------------------------------------------

  describe("constants", () => {
    it("PARTITION_COUNT is 3 (PGQ-style 3-partition)", () => {
      expect(PARTITION_COUNT).toBe(3);
    });

    it("DEFAULT_HEARTBEAT_STALENESS_MS is 5 minutes", () => {
      expect(DEFAULT_HEARTBEAT_STALENESS_MS).toBe(300_000);
    });

    it("DEFAULT_MAX_RETRIES is 3", () => {
      expect(DEFAULT_MAX_RETRIES).toBe(3);
    });

    it("DEFAULT_BATCH_SIZE is 1000", () => {
      expect(DEFAULT_BATCH_SIZE).toBe(1000);
    });
  });

  // -----------------------------------------------------------------------
  // DDL generation
  // -----------------------------------------------------------------------

  describe("generateDDL()", () => {
    it("creates parent table partitioned by LIST on partition_id", () => {
      const ddl = generateDDL();
      expect(ddl).toContain("PARTITION BY LIST (partition_id)");
    });

    it("creates exactly 3 child partitions (p0, p1, p2)", () => {
      const ddl = generateDDL();
      expect(ddl).toContain("batch_jobs_p0");
      expect(ddl).toContain("FOR VALUES IN (0)");
      expect(ddl).toContain("batch_jobs_p1");
      expect(ddl).toContain("FOR VALUES IN (1)");
      expect(ddl).toContain("batch_jobs_p2");
      expect(ddl).toContain("FOR VALUES IN (2)");
    });

    it("includes heartbeat_at column", () => {
      const ddl = generateDDL();
      expect(ddl).toContain("heartbeat_at");
      expect(ddl).toContain("timestamptz");
    });

    it("includes status CHECK constraint with all lifecycle states", () => {
      const ddl = generateDDL();
      expect(ddl).toContain("'pending'");
      expect(ddl).toContain("'running'");
      expect(ddl).toContain("'done'");
      expect(ddl).toContain("'failed'");
      expect(ddl).toContain("'dead'");
    });

    it("creates partial indexes for pending status on each partition", () => {
      const ddl = generateDDL();
      expect(ddl).toContain("batch_jobs_pending_p0");
      expect(ddl).toContain("batch_jobs_pending_p1");
      expect(ddl).toContain("batch_jobs_pending_p2");
      expect(ddl).toContain("WHERE status = 'pending'");
    });

    it("creates running indexes for heartbeat staleness detection", () => {
      const ddl = generateDDL();
      expect(ddl).toContain("batch_jobs_running_p0");
      expect(ddl).toContain("batch_jobs_running_p1");
      expect(ddl).toContain("batch_jobs_running_p2");
      expect(ddl).toContain("WHERE status = 'running'");
    });

    it("creates batch_queue_meta table with active_partition", () => {
      const ddl = generateDDL();
      expect(ddl).toContain("batch_queue_meta");
      expect(ddl).toContain("active_partition");
    });

    it("uses custom schema when provided", () => {
      const ddl = generateDDL("custom_schema");
      expect(ddl).toContain('"custom_schema"');
      expect(ddl).not.toContain('"sqlever"');
    });

    it("includes max_retries and attempt columns", () => {
      const ddl = generateDDL();
      expect(ddl).toContain("max_retries");
      expect(ddl).toContain("attempt");
    });
  });

  // -----------------------------------------------------------------------
  // splitStatements()
  // -----------------------------------------------------------------------

  describe("splitStatements()", () => {
    it("splits DDL into executable statements", () => {
      const ddl = generateDDL();
      const stmts = splitStatements(ddl);
      // Should have: CREATE SCHEMA, CREATE TABLE parent, 3 partitions,
      // 6 indexes, CREATE meta table, INSERT meta
      expect(stmts.length).toBeGreaterThanOrEqual(10);
    });

    it("strips trailing semicolons", () => {
      const stmts = splitStatements("SELECT 1;\nSELECT 2;");
      for (const s of stmts) {
        expect(s.endsWith(";")).toBe(false);
      }
    });

    it("skips pure comment lines", () => {
      const stmts = splitStatements("-- comment\nSELECT 1;");
      expect(stmts.length).toBe(1);
      expect(stmts[0]).toContain("SELECT 1");
    });
  });

  // -----------------------------------------------------------------------
  // isValidTransition()
  // -----------------------------------------------------------------------

  describe("isValidTransition()", () => {
    it("allows pending -> running", () => {
      expect(isValidTransition("pending", "running")).toBe(true);
    });

    it("allows running -> done", () => {
      expect(isValidTransition("running", "done")).toBe(true);
    });

    it("allows running -> failed", () => {
      expect(isValidTransition("running", "failed")).toBe(true);
    });

    it("allows failed -> running (retry)", () => {
      expect(isValidTransition("failed", "running")).toBe(true);
    });

    it("allows failed -> dead (max retries exceeded)", () => {
      expect(isValidTransition("failed", "dead")).toBe(true);
    });

    it("allows dead -> running (manual retry)", () => {
      expect(isValidTransition("dead", "running")).toBe(true);
    });

    it("disallows done -> any transition", () => {
      expect(isValidTransition("done", "pending")).toBe(false);
      expect(isValidTransition("done", "running")).toBe(false);
      expect(isValidTransition("done", "failed")).toBe(false);
    });

    it("disallows pending -> done (must go through running)", () => {
      expect(isValidTransition("pending", "done")).toBe(false);
    });

    it("disallows pending -> failed", () => {
      expect(isValidTransition("pending", "failed")).toBe(false);
    });
  });

  // -----------------------------------------------------------------------
  // VALID_TRANSITIONS map
  // -----------------------------------------------------------------------

  describe("VALID_TRANSITIONS", () => {
    it("covers all 5 status values", () => {
      const statuses: JobStatus[] = ["pending", "running", "done", "failed", "dead"];
      for (const s of statuses) {
        expect(VALID_TRANSITIONS[s]).toBeDefined();
      }
    });
  });

  // -----------------------------------------------------------------------
  // BatchQueue — getActivePartition
  // -----------------------------------------------------------------------

  describe("BatchQueue.getActivePartition()", () => {
    it("returns the active partition from metadata", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        {
          pattern: "batch_queue_meta",
          rows: [{ value: "1" }],
        },
      ]);

      const queue = new BatchQueue(client);
      const active = await queue.getActivePartition();
      expect(active).toBe(1);
    });

    it("throws if metadata is not initialized", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [] },
      ]);

      const queue = new BatchQueue(client);
      await expect(queue.getActivePartition()).rejects.toThrow(
        "batch_queue_meta not initialized",
      );
    });
  });

  // -----------------------------------------------------------------------
  // BatchQueue — createJob
  // -----------------------------------------------------------------------

  describe("BatchQueue.createJob()", () => {
    it("inserts into the active partition with correct defaults", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      const createdJob = mockJob({ id: 42, partition_id: 0 as PartitionId });

      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "0" }] },
        { pattern: "INSERT INTO", rows: [createdJob], command: "INSERT" },
      ]);

      const queue = new BatchQueue(client);
      const job = await queue.createJob({
        name: "backfill_tiers",
        tableName: "users",
      });

      expect(job.id).toBe(42);
      expect(job.name).toBe("backfill_tiers");

      // Verify the INSERT query included partition_id
      const insertQ = findQuery(pgClient, "INSERT INTO");
      expect(insertQ).toBeDefined();
      expect(insertQ!.values).toContain(0); // active partition
    });

    it("uses custom batch options when provided", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      const createdJob = mockJob({
        batch_size: 500,
        sleep_ms: 200,
        max_retries: 5,
      });

      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "0" }] },
        { pattern: "INSERT INTO", rows: [createdJob], command: "INSERT" },
      ]);

      const queue = new BatchQueue(client);
      await queue.createJob({
        name: "backfill",
        tableName: "orders",
        batchSize: 500,
        sleepMs: 200,
        maxRetries: 5,
      });

      const insertQ = findQuery(pgClient, "INSERT INTO");
      expect(insertQ!.values).toContain(500);
      expect(insertQ!.values).toContain(200);
      expect(insertQ!.values).toContain(5);
    });
  });

  // -----------------------------------------------------------------------
  // BatchQueue — dequeueJob (SKIP LOCKED)
  // -----------------------------------------------------------------------

  describe("BatchQueue.dequeueJob()", () => {
    it("uses FOR UPDATE SKIP LOCKED in the dequeue query", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      const runningJob = mockJob({ status: "running", attempt: 1 });

      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "0" }] },
        { pattern: "FOR UPDATE SKIP LOCKED", rows: [runningJob] },
      ]);

      const queue = new BatchQueue(client);
      const job = await queue.dequeueJob();

      expect(job).not.toBeNull();
      expect(job!.status).toBe("running");

      // Verify SKIP LOCKED was used
      const dequeueQ = findQuery(pgClient, "FOR UPDATE SKIP LOCKED");
      expect(dequeueQ).toBeDefined();
    });

    it("returns null when no pending jobs available", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "0" }] },
        { pattern: "FOR UPDATE SKIP LOCKED", rows: [] },
      ]);

      const queue = new BatchQueue(client);
      const job = await queue.dequeueJob();

      expect(job).toBeNull();
    });

    it("sets heartbeat_at and increments attempt on dequeue", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "0" }] },
        { pattern: "FOR UPDATE SKIP LOCKED", rows: [mockJob()] },
      ]);

      const queue = new BatchQueue(client);
      await queue.dequeueJob();

      const dequeueQ = findQuery(pgClient, "FOR UPDATE SKIP LOCKED");
      expect(dequeueQ!.text).toContain("heartbeat_at = now()");
      expect(dequeueQ!.text).toContain("attempt = attempt + 1");
    });

    it("transitions job from pending to running", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "0" }] },
        { pattern: "FOR UPDATE SKIP LOCKED", rows: [mockJob()] },
      ]);

      const queue = new BatchQueue(client);
      await queue.dequeueJob();

      const dequeueQ = findQuery(pgClient, "FOR UPDATE SKIP LOCKED");
      expect(dequeueQ!.text).toContain("status = 'running'");
    });
  });

  // -----------------------------------------------------------------------
  // BatchQueue — rotatePartitions (DD9: TRUNCATE vs DELETE)
  // -----------------------------------------------------------------------

  describe("BatchQueue.rotatePartitions()", () => {
    it("TRUNCATEs the drain partition (not DELETE — zero bloat)", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      // Active = 0, processing = 1, drain = 2
      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "0" }] },
        { pattern: "TRUNCATE", rows: [], command: "TRUNCATE" },
        { pattern: "UPDATE", rows: [], rowCount: 1, command: "UPDATE" },
      ]);

      const queue = new BatchQueue(client);
      const newActive = await queue.rotatePartitions();

      // The drain partition (2) should have been truncated
      const truncQ = findQuery(pgClient, "TRUNCATE");
      expect(truncQ).toBeDefined();
      expect(truncQ!.text).toContain("batch_jobs_p2");

      // New active should be the old drain (2)
      expect(newActive).toBe(2);
    });

    it("updates the active_partition metadata after rotation", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "1" }] },
        { pattern: "TRUNCATE", rows: [], command: "TRUNCATE" },
        { pattern: "UPDATE", rows: [], rowCount: 1, command: "UPDATE" },
      ]);

      const queue = new BatchQueue(client);
      await queue.rotatePartitions();

      // Active was 1, drain = (1+2)%3 = 0, new active = 0
      const updateQ = allQueriesMatching(pgClient, "UPDATE");
      const metaUpdate = updateQ.find((q) =>
        q.text.includes("batch_queue_meta"),
      );
      expect(metaUpdate).toBeDefined();
      expect(metaUpdate!.values).toContain("0"); // new active partition
    });

    it("rotates correctly through full cycle (0 -> 2 -> 1 -> 0)", async () => {
      // This tests the rotation math: drain = (active + 2) % 3
      // active=0 -> drain=2, new_active=2
      // active=2 -> drain=1, new_active=1
      // active=1 -> drain=0, new_active=0

      const activeSequence: [PartitionId, PartitionId, PartitionId] = [0, 2, 1];
      const drainPartitions: [PartitionId, PartitionId, PartitionId] = [2, 1, 0];
      const newActives: [PartitionId, PartitionId, PartitionId] = [2, 1, 0];

      for (let i = 0; i < 3; i++) {
        const client = await makeClient();
        const pgClient = mockInstances[mockInstances.length - 1]!;

        setupQueryResponses(pgClient, [
          {
            pattern: "batch_queue_meta",
            rows: [{ value: String(activeSequence[i]!) }],
          },
          { pattern: "TRUNCATE", rows: [], command: "TRUNCATE" },
          { pattern: "UPDATE", rows: [], rowCount: 1, command: "UPDATE" },
        ]);

        const queue = new BatchQueue(client);
        const newActive = await queue.rotatePartitions();

        expect(newActive).toBe(newActives[i]!);

        const truncQ = findQuery(pgClient, "TRUNCATE");
        expect(truncQ!.text).toContain(`batch_jobs_p${drainPartitions[i]!}`);
      }
    });
  });

  // -----------------------------------------------------------------------
  // BatchQueue — completeJob
  // -----------------------------------------------------------------------

  describe("BatchQueue.completeJob()", () => {
    it("transitions running -> done", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      const runningJob = mockJob({ status: "running" });
      const doneJob = mockJob({ status: "done" });

      setupQueryResponses(pgClient, [
        { pattern: "SELECT *", rows: [runningJob] },
        { pattern: "UPDATE", rows: [doneJob], command: "UPDATE" },
      ]);

      const queue = new BatchQueue(client);
      const result = await queue.completeJob(1, 0 as PartitionId);
      expect(result.status).toBe("done");
    });

    it("stores last_pk when provided", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      const runningJob = mockJob({ status: "running" });
      const doneJob = mockJob({ status: "done", last_pk: "12345" });

      setupQueryResponses(pgClient, [
        { pattern: "SELECT *", rows: [runningJob] },
        { pattern: "UPDATE", rows: [doneJob], command: "UPDATE" },
      ]);

      const queue = new BatchQueue(client);
      await queue.completeJob(1, 0 as PartitionId, "12345");

      const updateQ = allQueriesMatching(pgClient, "UPDATE");
      const jobUpdate = updateQ.find((q) => q.text.includes("last_pk"));
      expect(jobUpdate).toBeDefined();
      expect(jobUpdate!.values).toContain("12345");
    });
  });

  // -----------------------------------------------------------------------
  // BatchQueue — failJob
  // -----------------------------------------------------------------------

  describe("BatchQueue.failJob()", () => {
    it("transitions running -> failed when retries remain", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      const runningJob = mockJob({ status: "running", attempt: 1, max_retries: 3 });
      const failedJob = mockJob({ status: "failed", error_message: "timeout" });

      setupQueryResponses(pgClient, [
        { pattern: "SELECT *", rows: [runningJob] },
        { pattern: "UPDATE", rows: [failedJob], command: "UPDATE" },
      ]);

      const queue = new BatchQueue(client);
      const result = await queue.failJob(1, 0 as PartitionId, "timeout");
      expect(result.status).toBe("failed");
    });

    it("transitions running -> dead when max retries exceeded", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      // attempt >= max_retries -> dead
      const runningJob = mockJob({ status: "running", attempt: 3, max_retries: 3 });
      const deadJob = mockJob({ status: "dead", error_message: "timeout" });

      setupQueryResponses(pgClient, [
        { pattern: "SELECT *", rows: [runningJob] },
        { pattern: "UPDATE", rows: [deadJob], command: "UPDATE" },
      ]);

      const queue = new BatchQueue(client);
      const result = await queue.failJob(1, 0 as PartitionId, "timeout");
      expect(result.status).toBe("dead");
    });

    it("rejects if job is not in running status", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      const pendingJob = mockJob({ status: "pending" });

      setupQueryResponses(pgClient, [
        { pattern: "SELECT *", rows: [pendingJob] },
      ]);

      const queue = new BatchQueue(client);
      await expect(
        queue.failJob(1, 0 as PartitionId, "error"),
      ).rejects.toThrow("Cannot fail job 1: status is 'pending'");
    });

    it("stores error message", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      const runningJob = mockJob({ status: "running", attempt: 1 });
      const failedJob = mockJob({ status: "failed", error_message: "OOM killed" });

      setupQueryResponses(pgClient, [
        { pattern: "SELECT *", rows: [runningJob] },
        { pattern: "UPDATE", rows: [failedJob], command: "UPDATE" },
      ]);

      const queue = new BatchQueue(client);
      await queue.failJob(1, 0 as PartitionId, "OOM killed");

      const updateQ = allQueriesMatching(pgClient, "UPDATE");
      const jobUpdate = updateQ.find((q) => q.text.includes("error_message"));
      expect(jobUpdate).toBeDefined();
      expect(jobUpdate!.values).toContain("OOM killed");
    });
  });

  // -----------------------------------------------------------------------
  // BatchQueue — retryJob
  // -----------------------------------------------------------------------

  describe("BatchQueue.retryJob()", () => {
    it("transitions failed -> running", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      const failedJob = mockJob({ status: "failed" });
      const runningJob = mockJob({ status: "running", attempt: 2 });

      setupQueryResponses(pgClient, [
        { pattern: "SELECT *", rows: [failedJob] },
        { pattern: "UPDATE", rows: [runningJob], command: "UPDATE" },
      ]);

      const queue = new BatchQueue(client);
      const result = await queue.retryJob(1, 0 as PartitionId);
      expect(result.status).toBe("running");
    });

    it("transitions dead -> running (manual retry)", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      const deadJob = mockJob({ status: "dead" });
      const runningJob = mockJob({ status: "running", attempt: 4 });

      setupQueryResponses(pgClient, [
        { pattern: "SELECT *", rows: [deadJob] },
        { pattern: "UPDATE", rows: [runningJob], command: "UPDATE" },
      ]);

      const queue = new BatchQueue(client);
      const result = await queue.retryJob(1, 0 as PartitionId);
      expect(result.status).toBe("running");
    });

    it("clears error_message on retry", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      const failedJob = mockJob({ status: "failed", error_message: "old error" });
      const runningJob = mockJob({ status: "running", error_message: null });

      setupQueryResponses(pgClient, [
        { pattern: "SELECT *", rows: [failedJob] },
        { pattern: "UPDATE", rows: [runningJob], command: "UPDATE" },
      ]);

      const queue = new BatchQueue(client);
      await queue.retryJob(1, 0 as PartitionId);

      const updateQ = findQuery(pgClient, "UPDATE");
      expect(updateQ!.text).toContain("error_message = NULL");
    });

    it("rejects retry of a pending job", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      const pendingJob = mockJob({ status: "pending" });

      setupQueryResponses(pgClient, [
        { pattern: "SELECT *", rows: [pendingJob] },
      ]);

      const queue = new BatchQueue(client);
      await expect(
        queue.retryJob(1, 0 as PartitionId),
      ).rejects.toThrow("Cannot retry job 1: status is 'pending'");
    });

    it("rejects retry of a done job", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      const doneJob = mockJob({ status: "done" });

      setupQueryResponses(pgClient, [
        { pattern: "SELECT *", rows: [doneJob] },
      ]);

      const queue = new BatchQueue(client);
      await expect(
        queue.retryJob(1, 0 as PartitionId),
      ).rejects.toThrow("Cannot retry job 1: status is 'done'");
    });
  });

  // -----------------------------------------------------------------------
  // BatchQueue — updateHeartbeat
  // -----------------------------------------------------------------------

  describe("BatchQueue.updateHeartbeat()", () => {
    it("updates heartbeat_at for a running job", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        {
          pattern: "heartbeat_at = now()",
          rows: [],
          rowCount: 1,
          command: "UPDATE",
        },
      ]);

      const queue = new BatchQueue(client);
      await queue.updateHeartbeat(1, 0 as PartitionId);

      const hbQ = findQuery(pgClient, "heartbeat_at = now()");
      expect(hbQ).toBeDefined();
      expect(hbQ!.text).toContain("status = 'running'");
    });

    it("throws when job not found or not running", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        {
          pattern: "heartbeat_at = now()",
          rows: [],
          rowCount: 0,
          command: "UPDATE",
        },
      ]);

      const queue = new BatchQueue(client);
      await expect(
        queue.updateHeartbeat(999, 0 as PartitionId),
      ).rejects.toThrow("Cannot update heartbeat for job 999");
    });
  });

  // -----------------------------------------------------------------------
  // BatchQueue — detectStaleJobs (heartbeat staleness)
  // -----------------------------------------------------------------------

  describe("BatchQueue.detectStaleJobs()", () => {
    it("marks stale running jobs as failed", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      const staleJob = mockJob({
        status: "failed",
        error_message: "Worker heartbeat stale",
      });

      setupQueryResponses(pgClient, [
        {
          pattern: "heartbeat_at <",
          rows: [staleJob],
          command: "UPDATE",
        },
      ]);

      const queue = new BatchQueue(client);
      const staleJobs = await queue.detectStaleJobs();

      expect(staleJobs).toHaveLength(1);

      const detectQ = findQuery(pgClient, "heartbeat_at <");
      expect(detectQ).toBeDefined();
      expect(detectQ!.text).toContain("status = 'running'");
      expect(detectQ!.values).toContain(DEFAULT_HEARTBEAT_STALENESS_MS);
    });

    it("uses custom staleness threshold", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        { pattern: "heartbeat_at <", rows: [], command: "UPDATE" },
      ]);

      const queue = new BatchQueue(client, {
        heartbeatStalenessMs: 60_000,
      });
      await queue.detectStaleJobs();

      const detectQ = findQuery(pgClient, "heartbeat_at <");
      expect(detectQ!.values).toContain(60_000);
    });

    it("returns empty array when no stale jobs", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        { pattern: "heartbeat_at <", rows: [], command: "UPDATE" },
      ]);

      const queue = new BatchQueue(client);
      const staleJobs = await queue.detectStaleJobs();

      expect(staleJobs).toHaveLength(0);
    });

    it("marks jobs as dead when attempt >= max_retries", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        { pattern: "heartbeat_at <", rows: [], command: "UPDATE" },
      ]);

      const queue = new BatchQueue(client);
      await queue.detectStaleJobs();

      // Verify the SQL contains the CASE for dead vs failed
      const detectQ = findQuery(pgClient, "heartbeat_at <");
      expect(detectQ!.text).toContain("WHEN attempt >= max_retries THEN 'dead'");
      expect(detectQ!.text).toContain("ELSE 'failed'");
    });
  });

  // -----------------------------------------------------------------------
  // BatchQueue — listJobs / countByStatus
  // -----------------------------------------------------------------------

  describe("BatchQueue.listJobs()", () => {
    it("lists all jobs without filters", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      const jobs = [mockJob({ id: 1 }), mockJob({ id: 2 })];

      setupQueryResponses(pgClient, [
        { pattern: "SELECT *", rows: jobs },
      ]);

      const queue = new BatchQueue(client);
      const result = await queue.listJobs();
      expect(result).toHaveLength(2);
    });

    it("filters by status", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        { pattern: "status = $", rows: [mockJob({ status: "pending" })] },
      ]);

      const queue = new BatchQueue(client);
      await queue.listJobs({ status: "pending" });

      const listQ = findQuery(pgClient, "status = $");
      expect(listQ!.values).toContain("pending");
    });

    it("filters by partition", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        { pattern: "partition_id = $", rows: [] },
      ]);

      const queue = new BatchQueue(client);
      await queue.listJobs({ partitionId: 1 as PartitionId });

      const listQ = findQuery(pgClient, "partition_id = $");
      expect(listQ!.values).toContain(1);
    });
  });

  // -----------------------------------------------------------------------
  // BatchQueue — custom schema
  // -----------------------------------------------------------------------

  describe("BatchQueue with custom schema", () => {
    it("uses custom schema in all queries", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        { pattern: "my_schema", rows: [{ value: "0" }] },
      ]);

      const queue = new BatchQueue(client, { schema: "my_schema" });
      await queue.getActivePartition();

      const q = findQuery(pgClient, "my_schema");
      expect(q).toBeDefined();
    });
  });

  // -----------------------------------------------------------------------
  // BatchQueue — ensureSchema
  // -----------------------------------------------------------------------

  describe("BatchQueue.ensureSchema()", () => {
    it("executes all DDL statements", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      const queue = new BatchQueue(client);
      await queue.ensureSchema();

      // Should have executed multiple CREATE statements.
      // Filter out session-setup queries (SET commands from connect())
      const ddlQueries = pgClient.queries.filter(
        (q) =>
          q.text.includes("CREATE") ||
          q.text.includes("INSERT INTO") ||
          q.text.includes("PARTITION"),
      );
      expect(ddlQueries.length).toBeGreaterThanOrEqual(5);
    });
  });

  // -----------------------------------------------------------------------
  // BatchQueue — getJob
  // -----------------------------------------------------------------------

  describe("BatchQueue.getJob()", () => {
    it("returns job when found", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      const job = mockJob({ id: 42 });

      setupQueryResponses(pgClient, [
        { pattern: "SELECT *", rows: [job] },
      ]);

      const queue = new BatchQueue(client);
      const result = await queue.getJob(42, 0 as PartitionId);
      expect(result).not.toBeNull();
      expect(result!.id).toBe(42);
    });

    it("returns null when job not found", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      setupQueryResponses(pgClient, [
        { pattern: "SELECT *", rows: [] },
      ]);

      const queue = new BatchQueue(client);
      const result = await queue.getJob(999, 0 as PartitionId);
      expect(result).toBeNull();
    });
  });

  // -----------------------------------------------------------------------
  // BatchQueue — invalid transitions
  // -----------------------------------------------------------------------

  describe("BatchQueue transition validation", () => {
    it("rejects completing a pending job (must go through running first)", async () => {
      const client = await makeClient();
      const pgClient = mockInstances[0]!;

      const pendingJob = mockJob({ status: "pending" });

      setupQueryResponses(pgClient, [
        { pattern: "SELECT *", rows: [pendingJob] },
      ]);

      const queue = new BatchQueue(client);
      await expect(
        queue.completeJob(1, 0 as PartitionId),
      ).rejects.toThrow("Invalid status transition: pending -> done");
    });
  });

  // -----------------------------------------------------------------------
  // BatchQueue — partitionTable helper
  // -----------------------------------------------------------------------

  describe("BatchQueue.partitionTable()", () => {
    it("returns qualified partition table name", async () => {
      const client = await makeClient();
      const queue = new BatchQueue(client);

      expect(queue.partitionTable(0 as PartitionId)).toBe(
        '"sqlever"."batch_jobs_p0"',
      );
      expect(queue.partitionTable(1 as PartitionId)).toBe(
        '"sqlever"."batch_jobs_p1"',
      );
      expect(queue.partitionTable(2 as PartitionId)).toBe(
        '"sqlever"."batch_jobs_p2"',
      );
    });
  });
});
