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

const {
  BatchWorker,
  DEFAULT_LOCK_TIMEOUT_MS,
  DEFAULT_STATEMENT_TIMEOUT_MS,
  DEFAULT_SEARCH_PATH,
} = await import("../../src/batch/worker");

const {
  calculateProgress,
  parseIntervalToSeconds,
  computeDeadTupleRatio,
  checkHeartbeatStaleness,
  ProgressMonitor,
  DEFAULT_REPLICATION_LAG_THRESHOLD_SECONDS,
  DEFAULT_MAX_DEAD_TUPLE_RATIO,
} = await import("../../src/batch/progress");

const {
  parseBatchAddArgs,
  parseBatchNameArgs,
  parseSleepValue,
  formatJobText,
  formatJobListText,
  formatJobJson,
  BATCH_SUBCOMMANDS,
} = await import("../../src/commands/batch");

import type {
  BatchJob,
  JobStatus,
  PartitionId,
} from "../../src/batch/queue";

import type {
  DmlExecutor,
  SignalCheckFn,
  WorkerResult,
} from "../../src/batch/worker";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function makeClient(): Promise<InstanceType<typeof DatabaseClient>> {
  const client = new DatabaseClient("postgresql://user@localhost/testdb");
  await client.connect();
  return client;
}

function latestPgClient(): MockPgClient {
  return mockInstances[mockInstances.length - 1]!;
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

/** No-op sleep for tests. */
const instantSleep = async (_ms: number) => {};

/** DML executor that returns a fixed number of rows affected per call. */
function fixedDml(
  schedule: Array<{ rowsAffected: number; lastPk: string | null }>,
): DmlExecutor {
  let callIndex = 0;
  return async (_db, _job, _lastPk) => {
    const result = schedule[callIndex] ?? { rowsAffected: 0, lastPk: null };
    callIndex++;
    return result;
  };
}

/**
 * Set up standard query responses for a worker run.
 */
function setupWorkerQueries(
  pgClient: MockPgClient,
  job: BatchJob,
  extraResponses: Array<{
    pattern: string | RegExp;
    rows: unknown[];
    rowCount?: number;
    command?: string;
  }> = [],
) {
  setupQueryResponses(pgClient, [
    { pattern: "batch_queue_meta", rows: [{ value: String(job.partition_id) }] },
    { pattern: "FOR UPDATE SKIP LOCKED", rows: [job] },
    { pattern: "BEGIN", rows: [], command: "BEGIN" },
    { pattern: "COMMIT", rows: [], command: "COMMIT" },
    { pattern: "ROLLBACK", rows: [], command: "ROLLBACK" },
    { pattern: "SET lock_timeout", rows: [] },
    { pattern: "SET statement_timeout", rows: [] },
    { pattern: "SET search_path", rows: [] },
    {
      pattern: "heartbeat_at = now()",
      rows: [],
      rowCount: 1,
      command: "UPDATE",
    },
    {
      pattern: /UPDATE.*last_pk = \$1/,
      rows: [],
      rowCount: 1,
      command: "UPDATE",
    },
    { pattern: "SELECT *", rows: [{ ...job, status: "running" }] },
    {
      pattern: /UPDATE.*status = \$3/,
      rows: [{ ...job, status: "done" }],
      rowCount: 1,
      command: "UPDATE",
    },
    ...extraResponses,
  ]);
}

// ===========================================================================
// TEST SUITE: Batch Deep Tests (issue #128)
// ===========================================================================

describe("batch-deep", () => {
  beforeEach(() => {
    mockInstances = [];
  });

  // =========================================================================
  // 1. Queue operations (9 tests)
  // =========================================================================

  describe("Queue operations", () => {
    it("add job — inserts into active partition with pending status", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();

      const created = mockJob({ id: 7, status: "pending", partition_id: 0 as PartitionId });

      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "0" }] },
        { pattern: "INSERT INTO", rows: [created], command: "INSERT" },
      ]);

      const queue = new BatchQueue(client);
      const job = await queue.createJob({ name: "deep_add", tableName: "orders" });

      expect(job.id).toBe(7);
      expect(job.status).toBe("pending");

      const insertQ = findQuery(pgClient, "INSERT INTO");
      expect(insertQ).toBeDefined();
      expect(insertQ!.text).toContain("'pending'");
      expect(insertQ!.values).toContain(0); // active partition
    });

    it("dequeue — transitions status from pending to running", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();

      const runningJob = mockJob({ status: "running", attempt: 1 });

      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "0" }] },
        { pattern: "FOR UPDATE SKIP LOCKED", rows: [runningJob] },
      ]);

      const queue = new BatchQueue(client);
      const job = await queue.dequeueJob();

      expect(job).not.toBeNull();
      expect(job!.status).toBe("running");

      const dequeueQ = findQuery(pgClient, "FOR UPDATE SKIP LOCKED");
      expect(dequeueQ!.text).toContain("status = 'running'");
      expect(dequeueQ!.text).toContain("attempt = attempt + 1");
    });

    it("complete — transitions status from running to done", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();

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

    it("fail — transitions status from running to failed", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();

      const runningJob = mockJob({ status: "running", attempt: 1, max_retries: 3 });
      const failedJob = mockJob({ status: "failed", error_message: "lock timeout" });

      setupQueryResponses(pgClient, [
        { pattern: "SELECT *", rows: [runningJob] },
        { pattern: "UPDATE", rows: [failedJob], command: "UPDATE" },
      ]);

      const queue = new BatchQueue(client);
      const result = await queue.failJob(1, 0 as PartitionId, "lock timeout");
      expect(result.status).toBe("failed");
    });

    it("retry failed — increments attempt and transitions back to running", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();

      const failedJob = mockJob({ status: "failed", attempt: 1, error_message: "lock timeout" });
      const retriedJob = mockJob({ status: "running", attempt: 2, error_message: null });

      setupQueryResponses(pgClient, [
        { pattern: "SELECT *", rows: [failedJob] },
        { pattern: "UPDATE", rows: [retriedJob], command: "UPDATE" },
      ]);

      const queue = new BatchQueue(client);
      const result = await queue.retryJob(1, 0 as PartitionId);
      expect(result.status).toBe("running");
      expect(result.attempt).toBe(2);

      const updateQ = findQuery(pgClient, "UPDATE");
      expect(updateQ!.text).toContain("attempt = attempt + 1");
    });

    it("max retries exceeded — transitions to dead instead of failed", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();

      // attempt >= max_retries => dead
      const runningJob = mockJob({ status: "running", attempt: 3, max_retries: 3 });
      const deadJob = mockJob({ status: "dead", error_message: "connection reset" });

      setupQueryResponses(pgClient, [
        { pattern: "SELECT *", rows: [runningJob] },
        { pattern: "UPDATE", rows: [deadJob], command: "UPDATE" },
      ]);

      const queue = new BatchQueue(client);
      const result = await queue.failJob(1, 0 as PartitionId, "connection reset");
      expect(result.status).toBe("dead");
    });

    it("retry dead (manual) — dead job transitions back to running", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();

      const deadJob = mockJob({ status: "dead", attempt: 3, error_message: "old error" });
      const retriedJob = mockJob({ status: "running", attempt: 4, error_message: null });

      setupQueryResponses(pgClient, [
        { pattern: "SELECT *", rows: [deadJob] },
        { pattern: "UPDATE", rows: [retriedJob], command: "UPDATE" },
      ]);

      const queue = new BatchQueue(client);
      const result = await queue.retryJob(1, 0 as PartitionId);
      expect(result.status).toBe("running");
      expect(result.error_message).toBeNull();
    });

    it("all valid transitions exercised — every allowed transition returns true", () => {
      const allStatuses: JobStatus[] = [
        "pending", "running", "paused", "done", "failed", "dead", "cancelled",
      ];

      for (const from of allStatuses) {
        const allowed = VALID_TRANSITIONS[from];
        for (const to of allowed) {
          expect(isValidTransition(from, to)).toBe(true);
        }
      }
    });

    it("all invalid transitions rejected — disallowed pairs return false", () => {
      const allStatuses: JobStatus[] = [
        "pending", "running", "paused", "done", "failed", "dead", "cancelled",
      ];

      const invalid: [JobStatus, JobStatus][] = [
        ["pending", "done"],
        ["pending", "failed"],
        ["pending", "dead"],
        ["pending", "paused"],
        ["running", "pending"],
        ["done", "running"],
        ["done", "pending"],
        ["done", "failed"],
        ["done", "dead"],
        ["done", "paused"],
        ["done", "cancelled"],
        ["failed", "done"],
        ["failed", "pending"],
        ["failed", "paused"],
        ["failed", "cancelled"],
        ["dead", "done"],
        ["dead", "pending"],
        ["dead", "failed"],
        ["dead", "paused"],
        ["dead", "cancelled"],
        ["cancelled", "running"],
        ["cancelled", "pending"],
        ["cancelled", "done"],
        ["cancelled", "failed"],
        ["cancelled", "dead"],
        ["cancelled", "paused"],
      ];

      for (const [from, to] of invalid) {
        expect(isValidTransition(from, to)).toBe(false);
      }
    });
  });

  // =========================================================================
  // 2. 3-partition (5 tests)
  // =========================================================================

  describe("3-partition rotation", () => {
    it("DDL creates exactly 3 partitions — p0, p1, p2", () => {
      const ddl = generateDDL();
      const stmts = splitStatements(ddl);

      // Count the PARTITION OF statements
      const partitionStmts = stmts.filter((s) => s.includes("PARTITION OF"));
      expect(partitionStmts).toHaveLength(3);

      expect(ddl).toContain("batch_jobs_p0");
      expect(ddl).toContain("FOR VALUES IN (0)");
      expect(ddl).toContain("batch_jobs_p1");
      expect(ddl).toContain("FOR VALUES IN (1)");
      expect(ddl).toContain("batch_jobs_p2");
      expect(ddl).toContain("FOR VALUES IN (2)");
    });

    it("rotation cycle: 0 -> 2 -> 1 -> 0 (full cycle returns to start)", async () => {
      // active=0 => drain=(0+2)%3=2 => newActive=2
      // active=2 => drain=(2+2)%3=1 => newActive=1
      // active=1 => drain=(1+2)%3=0 => newActive=0
      const expectedCycle: [string, PartitionId][] = [
        ["0", 2 as PartitionId],
        ["2", 1 as PartitionId],
        ["1", 0 as PartitionId],
      ];

      for (const [currentActive, expectedNew] of expectedCycle) {
        const client = await makeClient();
        const pgClient = latestPgClient();

        setupQueryResponses(pgClient, [
          { pattern: "batch_queue_meta", rows: [{ value: currentActive }] },
          { pattern: "TRUNCATE", rows: [], command: "TRUNCATE" },
          { pattern: "UPDATE", rows: [], rowCount: 1, command: "UPDATE" },
        ]);

        const queue = new BatchQueue(client);
        const newActive = await queue.rotatePartitions();
        expect(newActive).toBe(expectedNew);
      }
    });

    it("TRUNCATE issued on drain partition during rotation", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();

      // active=0 => drain = (0+2)%3 = 2
      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "0" }] },
        { pattern: "TRUNCATE", rows: [], command: "TRUNCATE" },
        { pattern: "UPDATE", rows: [], rowCount: 1, command: "UPDATE" },
      ]);

      const queue = new BatchQueue(client);
      await queue.rotatePartitions();

      const truncQ = findQuery(pgClient, "TRUNCATE");
      expect(truncQ).toBeDefined();
      expect(truncQ!.text).toContain("batch_jobs_p2");
      // Verify it is TRUNCATE and not DELETE (no dead tuples)
      expect(truncQ!.text).not.toContain("DELETE");
    });

    it("partition metadata tracking — active_partition updated after rotation", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();

      // active=1, drain=(1+2)%3=0, newActive=0
      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "1" }] },
        { pattern: "TRUNCATE", rows: [], command: "TRUNCATE" },
        { pattern: "UPDATE", rows: [], rowCount: 1, command: "UPDATE" },
      ]);

      const queue = new BatchQueue(client);
      const newActive = await queue.rotatePartitions();
      expect(newActive).toBe(0);

      const metaUpdate = allQueriesMatching(pgClient, "UPDATE").find((q) =>
        q.text.includes("batch_queue_meta"),
      );
      expect(metaUpdate).toBeDefined();
      expect(metaUpdate!.values).toContain("0"); // new active partition stored
    });

    it("jobs land in active partition — createJob uses current active", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();

      // Active partition is 2
      const created = mockJob({ partition_id: 2 as PartitionId });
      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "2" }] },
        { pattern: "INSERT INTO", rows: [created], command: "INSERT" },
      ]);

      const queue = new BatchQueue(client);
      const job = await queue.createJob({ name: "p2_job", tableName: "items" });

      const insertQ = findQuery(pgClient, "INSERT INTO");
      expect(insertQ!.values).toContain(2); // partition_id = 2
      expect(job.partition_id).toBe(2);
    });
  });

  // =========================================================================
  // 3. SKIP LOCKED (4 tests)
  // =========================================================================

  describe("SKIP LOCKED concurrency", () => {
    it("two workers get different jobs — second dequeue skips locked row", async () => {
      // Worker 1 dequeues job id=1
      const client1 = await makeClient();
      const pg1 = latestPgClient();

      const job1 = mockJob({ id: 1, status: "running", attempt: 1 });
      setupQueryResponses(pg1, [
        { pattern: "batch_queue_meta", rows: [{ value: "0" }] },
        { pattern: "FOR UPDATE SKIP LOCKED", rows: [job1] },
      ]);

      const queue1 = new BatchQueue(client1);
      const dequeued1 = await queue1.dequeueJob();
      expect(dequeued1).not.toBeNull();
      expect(dequeued1!.id).toBe(1);

      // Worker 2 dequeues job id=2 (id=1 is locked)
      const client2 = await makeClient();
      const pg2 = latestPgClient();

      const job2 = mockJob({ id: 2, status: "running", attempt: 1 });
      setupQueryResponses(pg2, [
        { pattern: "batch_queue_meta", rows: [{ value: "0" }] },
        { pattern: "FOR UPDATE SKIP LOCKED", rows: [job2] },
      ]);

      const queue2 = new BatchQueue(client2);
      const dequeued2 = await queue2.dequeueJob();
      expect(dequeued2).not.toBeNull();
      expect(dequeued2!.id).toBe(2);

      // Confirm both used SKIP LOCKED
      expect(findQuery(pg1, "FOR UPDATE SKIP LOCKED")).toBeDefined();
      expect(findQuery(pg2, "FOR UPDATE SKIP LOCKED")).toBeDefined();
    });

    it("locked job skipped — dequeue query contains SKIP LOCKED clause", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();

      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "0" }] },
        { pattern: "FOR UPDATE SKIP LOCKED", rows: [mockJob({ status: "running" })] },
      ]);

      const queue = new BatchQueue(client);
      await queue.dequeueJob();

      const dequeueQ = findQuery(pgClient, "FOR UPDATE SKIP LOCKED");
      expect(dequeueQ).toBeDefined();
      // Confirm FOR UPDATE SKIP LOCKED is in the subquery, not just anywhere
      expect(dequeueQ!.text).toContain("FOR UPDATE SKIP LOCKED");
      // Confirm it uses ORDER BY id LIMIT 1 for deterministic selection
      expect(dequeueQ!.text).toContain("ORDER BY id");
      expect(dequeueQ!.text).toContain("LIMIT 1");
    });

    it("no contention on concurrent dequeue — each uses independent CTE/subquery", async () => {
      // Verify that the dequeue is an atomic UPDATE-with-subquery, not
      // a separate SELECT + UPDATE pair (which would create a TOCTOU race).
      const client = await makeClient();
      const pgClient = latestPgClient();

      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "0" }] },
        { pattern: "FOR UPDATE SKIP LOCKED", rows: [mockJob({ status: "running" })] },
      ]);

      const queue = new BatchQueue(client);
      await queue.dequeueJob();

      const dequeueQ = findQuery(pgClient, "FOR UPDATE SKIP LOCKED");
      // The UPDATE and SELECT must be in a single statement (atomic)
      expect(dequeueQ!.text).toContain("UPDATE");
      expect(dequeueQ!.text).toContain("WHERE id = (");
      expect(dequeueQ!.text).toContain("SELECT id FROM");
    });

    it("dequeue returns null when all jobs are locked or empty", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();

      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "0" }] },
        { pattern: "FOR UPDATE SKIP LOCKED", rows: [] },
      ]);

      const queue = new BatchQueue(client);
      const job = await queue.dequeueJob();
      expect(job).toBeNull();
    });
  });

  // =========================================================================
  // 4. Worker lifecycle (7 tests)
  // =========================================================================

  describe("Worker lifecycle", () => {
    it("execute-commit-sleep loop — processes batches in a loop until 0 rows", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob({ status: "running", attempt: 1 });
      setupWorkerQueries(pgClient, job);

      const sleepCalls: number[] = [];
      const trackingSleep = async (ms: number) => { sleepCalls.push(ms); };

      const dml = fixedDml([
        { rowsAffected: 50, lastPk: "50" },
        { rowsAffected: 50, lastPk: "100" },
        { rowsAffected: 0, lastPk: null },
      ]);

      const worker = new BatchWorker(client, dml, {}, undefined, trackingSleep);
      const result = await worker.run();

      expect(result.status).toBe("completed");
      expect(result.batchesProcessed).toBe(3);
      expect(result.totalRowsAffected).toBe(100);

      // Each batch wrapped in BEGIN/COMMIT
      const begins = allQueriesMatching(pgClient, "BEGIN");
      const commits = allQueriesMatching(pgClient, "COMMIT");
      expect(begins.length).toBeGreaterThanOrEqual(3);
      expect(commits.length).toBeGreaterThanOrEqual(3);

      // Sleep called between batches (not after final 0-row batch)
      expect(sleepCalls).toHaveLength(2);
    });

    it("heartbeat updated each batch — heartbeat_at = now() issued per batch", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob({ status: "running", attempt: 1 });
      setupWorkerQueries(pgClient, job);

      const dml = fixedDml([
        { rowsAffected: 100, lastPk: "100" },
        { rowsAffected: 100, lastPk: "200" },
        { rowsAffected: 100, lastPk: "300" },
        { rowsAffected: 0, lastPk: null },
      ]);

      const worker = new BatchWorker(client, dml, {}, undefined, instantSleep);
      await worker.run();

      const heartbeats = allQueriesMatching(pgClient, "heartbeat_at = now()");
      // At least 4 heartbeat updates (one per batch)
      expect(heartbeats.length).toBeGreaterThanOrEqual(4);
    });

    it("SET statements per batch txn — lock_timeout, statement_timeout, search_path each batch", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob({ status: "running", attempt: 1 });
      setupWorkerQueries(pgClient, job);

      const dml = fixedDml([
        { rowsAffected: 50, lastPk: "50" },
        { rowsAffected: 0, lastPk: null },
      ]);

      const worker = new BatchWorker(
        client,
        dml,
        { lockTimeoutMs: 7000, statementTimeoutMs: 15000, searchPath: "app" },
        undefined,
        instantSleep,
      );
      await worker.run();

      // 2 batches => at least 2 of each SET
      const lockSets = allQueriesMatching(pgClient, "SET lock_timeout");
      const stmtSets = allQueriesMatching(pgClient, "SET statement_timeout");
      const pathSets = allQueriesMatching(pgClient, "SET search_path");

      expect(lockSets.length).toBeGreaterThanOrEqual(2);
      expect(stmtSets.length).toBeGreaterThanOrEqual(2);
      expect(pathSets.length).toBeGreaterThanOrEqual(2);

      expect(lockSets.some((q) => q.text.includes("7000"))).toBe(true);
      expect(stmtSets.some((q) => q.text.includes("15000"))).toBe(true);
      expect(pathSets.some((q) => q.text.includes("app"))).toBe(true);
    });

    it("pause stops loop — signalCheck returning pause halts execution", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob({ status: "running", attempt: 1, max_retries: 3 });

      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "0" }] },
        { pattern: "FOR UPDATE SKIP LOCKED", rows: [job] },
        { pattern: "BEGIN", rows: [], command: "BEGIN" },
        { pattern: "COMMIT", rows: [], command: "COMMIT" },
        { pattern: "SET lock_timeout", rows: [] },
        { pattern: "SET statement_timeout", rows: [] },
        { pattern: "SET search_path", rows: [] },
        { pattern: "heartbeat_at = now()", rows: [], rowCount: 1, command: "UPDATE" },
        { pattern: /last_pk = \$1/, rows: [], rowCount: 1, command: "UPDATE" },
        { pattern: "SELECT *", rows: [{ ...job, status: "running" }] },
        { pattern: /status = \$3/, rows: [{ ...job, status: "failed" }], rowCount: 1, command: "UPDATE" },
      ]);

      let checkCount = 0;
      const signalCheck: SignalCheckFn = () => {
        checkCount++;
        return checkCount >= 2 ? "pause" : "continue";
      };

      const dml = fixedDml([
        { rowsAffected: 100, lastPk: "100" },
        { rowsAffected: 100, lastPk: "200" },
      ]);

      const worker = new BatchWorker(client, dml, {}, signalCheck, instantSleep);
      const result = await worker.run();

      expect(result.status).toBe("paused");
      expect(result.batchesProcessed).toBe(1); // only 1 batch before pause
    });

    it("resume continues — paused job can be resumed via queue", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();

      const pausedJob = mockJob({ status: "paused" });
      const resumedJob = mockJob({ status: "running" });

      setupQueryResponses(pgClient, [
        { pattern: /status = 'paused'/, rows: [resumedJob], rowCount: 1, command: "UPDATE" },
      ]);

      const queue = new BatchQueue(client);
      const result = await queue.resumeJob(1, 0 as PartitionId);
      expect(result.status).toBe("running");
    });

    it("cancel stops permanently — signalCheck returning cancel stops worker", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob({ status: "running", attempt: 1, max_retries: 3 });

      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "0" }] },
        { pattern: "FOR UPDATE SKIP LOCKED", rows: [job] },
        { pattern: "SELECT *", rows: [{ ...job, status: "running" }] },
        { pattern: /status = \$3/, rows: [{ ...job, status: "failed" }], rowCount: 1, command: "UPDATE" },
      ]);

      const signalCheck: SignalCheckFn = () => "cancel";
      const dml = fixedDml([]);

      const worker = new BatchWorker(client, dml, {}, signalCheck, instantSleep);
      const result = await worker.run();

      expect(result.status).toBe("cancelled");
      expect(result.batchesProcessed).toBe(0);
      expect(result.totalRowsAffected).toBe(0);
    });

    it("DML error marks job failed with error_message", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();
      const job = mockJob({ status: "running", attempt: 1, max_retries: 3 });

      setupQueryResponses(pgClient, [
        { pattern: "batch_queue_meta", rows: [{ value: "0" }] },
        { pattern: "FOR UPDATE SKIP LOCKED", rows: [job] },
        { pattern: "BEGIN", rows: [], command: "BEGIN" },
        { pattern: "ROLLBACK", rows: [], command: "ROLLBACK" },
        { pattern: "SET lock_timeout", rows: [] },
        { pattern: "SET statement_timeout", rows: [] },
        { pattern: "SET search_path", rows: [] },
        { pattern: "SELECT *", rows: [{ ...job, status: "running" }] },
        { pattern: /status = \$3/, rows: [{ ...job, status: "failed" }], rowCount: 1, command: "UPDATE" },
      ]);

      let callCount = 0;
      const dml: DmlExecutor = async () => {
        callCount++;
        throw new Error("deadlock detected");
      };

      const worker = new BatchWorker(client, dml, {}, undefined, instantSleep);
      const result = await worker.run();

      expect(result.status).toBe("failed");
      expect(result.error).toBe("deadlock detected");
      expect(result.jobId).toBe(1);
    });
  });

  // =========================================================================
  // 5. Monitoring (8 tests)
  // =========================================================================

  describe("Monitoring", () => {
    it("replication lag query — queries pg_stat_replication", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();

      setupQueryResponses(pgClient, [
        {
          pattern: "pg_stat_replication",
          rows: [{ replay_lag: "00:00:03.500", application_name: "standby1" }],
        },
      ]);

      const monitor = new ProgressMonitor(client);
      const lag = await monitor.getReplicationLag();

      expect(lag.replayLagSeconds).toBeCloseTo(3.5, 1);
      expect(lag.replicaName).toBe("standby1");

      const q = findQuery(pgClient, "pg_stat_replication");
      expect(q).toBeDefined();
      expect(q!.text).toContain("replay_lag");
    });

    it("lag > threshold — shouldPause returns true", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();

      setupQueryResponses(pgClient, [
        {
          pattern: "pg_stat_replication",
          rows: [{ replay_lag: "00:00:15.000", application_name: "r1" }],
        },
      ]);

      const monitor = new ProgressMonitor(client, {
        replicationLagThresholdSeconds: 10,
      });
      const shouldPause = await monitor.shouldPauseForReplicationLag();
      expect(shouldPause).toBe(true);
    });

    it("lag < threshold — shouldPause returns false", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();

      setupQueryResponses(pgClient, [
        {
          pattern: "pg_stat_replication",
          rows: [{ replay_lag: "00:00:05.000", application_name: "r1" }],
        },
      ]);

      const monitor = new ProgressMonitor(client, {
        replicationLagThresholdSeconds: 10,
      });
      const shouldPause = await monitor.shouldPauseForReplicationLag();
      expect(shouldPause).toBe(false);
    });

    it("dead tuple ratio computation — correct formula n_dead/(n_dead+n_live)", () => {
      // 200 dead, 800 live => ratio = 200/1000 = 0.2
      const ratio = computeDeadTupleRatio(200, 800);
      expect(ratio).toBeCloseTo(0.2, 5);

      // Edge: all dead
      expect(computeDeadTupleRatio(500, 0)).toBe(1.0);

      // Edge: none dead
      expect(computeDeadTupleRatio(0, 500)).toBe(0);

      // Edge: both zero
      expect(computeDeadTupleRatio(0, 0)).toBe(0);
    });

    it("ratio > 10% triggers pause — VACUUM pressure exceeds threshold", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();

      setupQueryResponses(pgClient, [
        {
          pattern: "pg_stat_user_tables",
          rows: [{ n_dead_tup: "120", n_live_tup: "880" }],
        },
      ]);

      const monitor = new ProgressMonitor(client); // default 10% threshold
      const shouldPause = await monitor.shouldPauseForVacuumPressure(
        "public",
        "large_table",
      );
      expect(shouldPause).toBe(true);
    });

    it("stale worker detection — old heartbeat detected as stale", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();

      const staleDate = new Date(Date.now() - 600_000); // 10 min ago

      setupQueryResponses(pgClient, [
        {
          pattern: "batch_jobs",
          rows: [{ id: 42, heartbeat_at: staleDate }],
        },
      ]);

      const monitor = new ProgressMonitor(client); // default 5 min threshold
      const staleWorkers = await monitor.findStaleWorkers();

      expect(staleWorkers).toHaveLength(1);
      expect(staleWorkers[0]!.jobId).toBe(42);
      expect(staleWorkers[0]!.isStale).toBe(true);
      expect(staleWorkers[0]!.ageMs).toBeGreaterThan(DEFAULT_HEARTBEAT_STALENESS_MS);
    });

    it("pg_stat_activity batch query filter — filters by sqlever application_name", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();

      setupQueryResponses(pgClient, [
        {
          pattern: "pg_stat_activity",
          rows: [
            {
              pid: 5678,
              query: "UPDATE orders SET processed = true WHERE id > 500 LIMIT 1000",
              duration_ms: "3200",
              wait_event_type: "Lock",
              wait_event: "relation",
              state: "active",
              application_name: "sqlever/batch/process_orders",
            },
          ],
        },
      ]);

      const monitor = new ProgressMonitor(client);
      const queries = await monitor.getBatchQueries();

      expect(queries).toHaveLength(1);
      expect(queries[0]!.pid).toBe(5678);
      expect(queries[0]!.waitEventType).toBe("Lock");
      expect(queries[0]!.applicationName).toBe("sqlever/batch/process_orders");

      const q = findQuery(pgClient, "pg_stat_activity");
      expect(q!.text).toContain("application_name LIKE 'sqlever/%'");
      expect(q!.text).toContain("pg_backend_pid()");
    });

    it("combined shouldPause checks both lag + VACUUM — replication lag takes priority", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();

      // Both lag and vacuum pressure exceed thresholds
      setupQueryResponses(pgClient, [
        {
          pattern: "pg_stat_replication",
          rows: [{ replay_lag: "00:00:20.000", application_name: "replica_a" }],
        },
        {
          pattern: "pg_stat_user_tables",
          rows: [{ n_dead_tup: "500", n_live_tup: "500" }],
        },
      ]);

      const monitor = new ProgressMonitor(client);
      const reason = await monitor.shouldPause("public", "users");

      // Replication lag is checked first (higher priority)
      expect(reason).not.toBeNull();
      expect(reason).toContain("Replication lag");
      expect(reason).toContain("20.0s");
      expect(reason).toContain("replica_a");

      // Now test: lag OK but vacuum bad
      const client2 = await makeClient();
      const pg2 = latestPgClient();

      setupQueryResponses(pg2, [
        {
          pattern: "pg_stat_replication",
          rows: [{ replay_lag: "00:00:02.000", application_name: "r1" }],
        },
        {
          pattern: "pg_stat_user_tables",
          rows: [{ n_dead_tup: "300", n_live_tup: "700" }],
        },
      ]);

      const monitor2 = new ProgressMonitor(client2);
      const reason2 = await monitor2.shouldPause("public", "users");

      expect(reason2).not.toBeNull();
      expect(reason2).toContain("Dead tuple ratio");
      expect(reason2).toContain("30.0%");
    });
  });

  // =========================================================================
  // 6. Progress (4 tests)
  // =========================================================================

  describe("Progress tracking", () => {
    it("row counting — done/total computed correctly", () => {
      const p = calculateProgress(750, 3000, 10000);
      expect(p.rowsDone).toBe(750);
      expect(p.rowsTotal).toBe(3000);
    });

    it("ETA from throughput — estimated time remaining calculated", () => {
      // 1000 done in 4000ms = 250 rows/sec
      // 2000 remaining / 250 = 8 sec = 8000ms
      const p = calculateProgress(1000, 3000, 4000);
      expect(p.rowsPerSecond).toBe(250);
      expect(p.etaMs).toBe(8000);
    });

    it("percentage calculation — correct rounding", () => {
      // 333 / 1000 = 33.3%
      const p = calculateProgress(333, 1000, 5000);
      expect(p.percentage).toBe(33.3);

      // 1 / 3 = 33.33%
      const p2 = calculateProgress(1, 3, 1000);
      expect(p2.percentage).toBe(33.33);

      // Full completion
      const p3 = calculateProgress(500, 500, 2000);
      expect(p3.percentage).toBe(100);
    });

    it("last_pk tracked for resume — worker passes lastPk through batches", async () => {
      const client = await makeClient();
      const pgClient = latestPgClient();

      // Job was previously interrupted at PK "999"
      const job = mockJob({ status: "running", attempt: 1, last_pk: "999" });
      setupWorkerQueries(pgClient, job);

      const receivedPks: Array<string | null> = [];
      let callIndex = 0;
      const dml: DmlExecutor = async (_db, _job, lastPk) => {
        receivedPks.push(lastPk);
        callIndex++;
        if (callIndex === 1) {
          return { rowsAffected: 50, lastPk: "1049" };
        }
        return { rowsAffected: 0, lastPk: null };
      };

      const worker = new BatchWorker(client, dml, {}, undefined, instantSleep);
      const result = await worker.run();

      // First call should get "999" (resume point from job)
      expect(receivedPks[0]).toBe("999");
      // Second call should get "1049" (updated by first batch)
      expect(receivedPks[1]).toBe("1049");
      expect(result.lastPk).toBe("1049");

      // Verify last_pk was persisted
      const pkUpdates = allQueriesMatching(pgClient, /last_pk = \$1/);
      expect(pkUpdates.length).toBeGreaterThanOrEqual(1);
    });
  });

  // =========================================================================
  // 7. CLI commands (3 tests)
  // =========================================================================

  describe("CLI commands", () => {
    it("batch add parses all flags — name, --table, --batch-size, --sleep, --max-retries", () => {
      const args = parseBatchAddArgs([
        "migrate_users",
        "--table", "users",
        "--batch-size", "2000",
        "--sleep", "500ms",
        "--max-retries", "7",
      ]);

      expect(args.name).toBe("migrate_users");
      expect(args.table).toBe("users");
      expect(args.batchSize).toBe(2000);
      expect(args.sleepMs).toBe(500);
      expect(args.maxRetries).toBe(7);

      // Short flag -t also works
      const args2 = parseBatchAddArgs(["job2", "-t", "items"]);
      expect(args2.table).toBe("items");

      // --sleep with seconds
      const args3 = parseBatchAddArgs(["job3", "--table", "t", "--sleep", "3s"]);
      expect(args3.sleepMs).toBe(3000);

      // Defaults applied
      const args4 = parseBatchAddArgs(["job4", "--table", "t"]);
      expect(args4.batchSize).toBe(1000);
      expect(args4.sleepMs).toBe(100);
      expect(args4.maxRetries).toBe(3);
    });

    it("batch list/status format text+json — formatters produce correct output", () => {
      const job = mockJob({
        name: "process_orders",
        status: "running",
        table_name: "orders",
        batch_size: 500,
        sleep_ms: 200,
        attempt: 2,
        max_retries: 5,
        last_pk: "42000",
        error_message: null,
        heartbeat_at: new Date("2025-06-15T12:00:00Z"),
      });

      // Text format
      const text = formatJobText(job);
      expect(text).toContain("Name:        process_orders");
      expect(text).toContain("Status:      running");
      expect(text).toContain("Table:       orders");
      expect(text).toContain("Batch size:  500");
      expect(text).toContain("Sleep:       200ms");
      expect(text).toContain("Attempt:     2/5");
      expect(text).toContain("Last PK:     42000");
      expect(text).toContain("Heartbeat:");

      // JSON format
      const json = formatJobJson(job);
      expect(json.name).toBe("process_orders");
      expect(json.status).toBe("running");
      expect(json.table).toBe("orders");
      expect(json.batch_size).toBe(500);
      expect(json.sleep_ms).toBe(200);
      expect(json.attempt).toBe(2);
      expect(json.max_retries).toBe(5);
      expect(json.last_pk).toBe("42000");
      expect(json.heartbeat_at).toBe("2025-06-15T12:00:00.000Z");

      // List format
      const jobs = [job, mockJob({ id: 2, name: "backfill_v2", table_name: "items" })];
      const listText = formatJobListText(jobs);
      expect(listText).toContain("NAME");
      expect(listText).toContain("STATUS");
      expect(listText).toContain("TABLE");
      expect(listText).toContain("process_orders");
      expect(listText).toContain("backfill_v2");

      // Empty list
      expect(formatJobListText([])).toBe("No batch jobs found.");
    });

    it("batch pause/resume/cancel state transitions — valid transitions checked", () => {
      // pause: running -> paused
      expect(isValidTransition("running", "paused")).toBe(true);

      // resume: paused -> running
      expect(isValidTransition("paused", "running")).toBe(true);

      // cancel from pending
      expect(isValidTransition("pending", "cancelled")).toBe(true);
      // cancel from running
      expect(isValidTransition("running", "cancelled")).toBe(true);
      // cancel from paused
      expect(isValidTransition("paused", "cancelled")).toBe(true);

      // cancelled is terminal
      expect(isValidTransition("cancelled", "running")).toBe(false);
      expect(isValidTransition("cancelled", "pending")).toBe(false);

      // done is terminal
      expect(isValidTransition("done", "running")).toBe(false);
      expect(isValidTransition("done", "paused")).toBe(false);

      // Invalid: cannot pause a pending job
      expect(isValidTransition("pending", "paused")).toBe(false);
      // Invalid: cannot pause a failed job
      expect(isValidTransition("failed", "paused")).toBe(false);
    });
  });
});
