// src/batch/queue.ts — PGQ-style 3-partition rotating queue for batched DML
//
// Implements the batch job queue described in SPEC Section 5.5 and DD9:
//
// - **3-partition rotation** solves bloat: completed batches cleaned via
//   TRUNCATE (instant, zero dead tuples, no VACUUM pressure) rather than
//   DELETE. Three partitions rotate: active (receiving work), processing
//   (being consumed), drain (being truncated).
//
// - **SKIP LOCKED** solves worker concurrency: multiple batch workers
//   dequeue from the active partition without blocking each other via
//   SELECT ... FOR UPDATE SKIP LOCKED.
//
// Job lifecycle:
//   pending -> running -> done
//                  |
//                failed -> (retry) -> done
//                                  -> dead (max retries exceeded)
//                                       |
//                                   (manual retry) -> running
//
// Heartbeat: heartbeat_at column updated at start of each batch. Stale
// workers (exceeding staleness threshold) detected and marked failed.

import type { DatabaseClient } from "../db/client";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Job status values matching the lifecycle in SPEC Section 5.5. */
export type JobStatus = "pending" | "running" | "done" | "failed" | "dead";

/** Which partition a job lives in. */
export type PartitionId = 0 | 1 | 2;

/** Row shape for sqlever.batch_jobs. */
export interface BatchJob {
  id: number;
  name: string;
  status: JobStatus;
  partition_id: PartitionId;
  table_name: string;
  batch_size: number;
  sleep_ms: number;
  last_pk: string | null;
  attempt: number;
  max_retries: number;
  error_message: string | null;
  heartbeat_at: Date | null;
  created_at: Date;
  updated_at: Date;
}

/** Options for creating a new batch job. */
export interface CreateJobOptions {
  name: string;
  tableName: string;
  batchSize?: number;
  sleepMs?: number;
  maxRetries?: number;
}

/** Options for the BatchQueue itself. */
export interface BatchQueueOptions {
  /** Schema for the queue tables. Default: "sqlever". */
  schema?: string;
  /** Heartbeat staleness threshold in ms. Default: 300000 (5 min). */
  heartbeatStalenessMs?: number;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Default heartbeat staleness: 5 minutes (SPEC Section 5.5). */
export const DEFAULT_HEARTBEAT_STALENESS_MS = 300_000;

/** Default batch size. */
export const DEFAULT_BATCH_SIZE = 1000;

/** Default sleep between batches in ms. */
export const DEFAULT_SLEEP_MS = 100;

/** Default max retries before a job goes to "dead". */
export const DEFAULT_MAX_RETRIES = 3;

/** Number of partitions in the rotating queue (PGQ-style). */
export const PARTITION_COUNT = 3;

/** Valid status transitions. Maps current status -> allowed next statuses. */
export const VALID_TRANSITIONS: Record<JobStatus, readonly JobStatus[]> = {
  pending: ["running"],
  running: ["done", "failed", "dead"],
  failed: ["running", "dead"],
  dead: ["running"],
  done: [],
} as const;

// ---------------------------------------------------------------------------
// DDL
// ---------------------------------------------------------------------------

/**
 * Generate the DDL to create the sqlever.batch_jobs table with
 * 3 partitions (PGQ-style rotating queue per DD9).
 *
 * The parent table is partitioned by LIST on partition_id.
 * Three child partitions hold values 0, 1, 2.
 */
export function generateDDL(schema = "sqlever"): string {
  const s = quoteIdent(schema);

  return `-- sqlever batch queue: PGQ-style 3-partition rotating table (DD9)
CREATE SCHEMA IF NOT EXISTS ${s};

CREATE TABLE IF NOT EXISTS ${s}.batch_jobs (
  id            bigint GENERATED ALWAYS AS IDENTITY,
  name          text        NOT NULL,
  status        text        NOT NULL DEFAULT 'pending',
  partition_id  smallint    NOT NULL,
  table_name    text        NOT NULL,
  batch_size    integer     NOT NULL DEFAULT ${DEFAULT_BATCH_SIZE},
  sleep_ms      integer     NOT NULL DEFAULT ${DEFAULT_SLEEP_MS},
  last_pk       text,
  attempt       integer     NOT NULL DEFAULT 0,
  max_retries   integer     NOT NULL DEFAULT ${DEFAULT_MAX_RETRIES},
  error_message text,
  heartbeat_at  timestamptz,
  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id, partition_id),
  CONSTRAINT batch_jobs_status_check
    CHECK (status IN ('pending', 'running', 'done', 'failed', 'dead')),
  CONSTRAINT batch_jobs_partition_id_check
    CHECK (partition_id IN (0, 1, 2))
) PARTITION BY LIST (partition_id);

CREATE TABLE IF NOT EXISTS ${s}.batch_jobs_p0
  PARTITION OF ${s}.batch_jobs FOR VALUES IN (0);
CREATE TABLE IF NOT EXISTS ${s}.batch_jobs_p1
  PARTITION OF ${s}.batch_jobs FOR VALUES IN (1);
CREATE TABLE IF NOT EXISTS ${s}.batch_jobs_p2
  PARTITION OF ${s}.batch_jobs FOR VALUES IN (2);

-- Partial indexes for efficient dequeue (DD9: SKIP LOCKED on active partition)
CREATE INDEX IF NOT EXISTS batch_jobs_pending_p0
  ON ${s}.batch_jobs_p0 (id) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS batch_jobs_pending_p1
  ON ${s}.batch_jobs_p1 (id) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS batch_jobs_pending_p2
  ON ${s}.batch_jobs_p2 (id) WHERE status = 'pending';

-- Index for heartbeat staleness detection
CREATE INDEX IF NOT EXISTS batch_jobs_running_p0
  ON ${s}.batch_jobs_p0 (heartbeat_at) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS batch_jobs_running_p1
  ON ${s}.batch_jobs_p1 (heartbeat_at) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS batch_jobs_running_p2
  ON ${s}.batch_jobs_p2 (heartbeat_at) WHERE status = 'running';

-- Metadata table tracking which partition is in which role
CREATE TABLE IF NOT EXISTS ${s}.batch_queue_meta (
  key   text PRIMARY KEY,
  value text NOT NULL
);

-- Initialize partition roles: active=0, processing=1, drain=2
INSERT INTO ${s}.batch_queue_meta (key, value)
VALUES ('active_partition', '0')
ON CONFLICT (key) DO NOTHING;
`;
}

// ---------------------------------------------------------------------------
// BatchQueue class
// ---------------------------------------------------------------------------

/**
 * PGQ-style 3-partition rotating queue for batched DML jobs.
 *
 * Partition roles rotate:
 * - **active**: receives new jobs and is the dequeue source
 * - **processing**: jobs are being worked on (was previously active)
 * - **drain**: completed jobs awaiting TRUNCATE (was previously processing)
 *
 * Rotation: drain -> TRUNCATE -> drain becomes active -> old active
 * becomes processing -> old processing becomes drain.
 */
export class BatchQueue {
  private db: DatabaseClient;
  private schema: string;
  private heartbeatStalenessMs: number;

  constructor(db: DatabaseClient, options: BatchQueueOptions = {}) {
    this.db = db;
    this.schema = options.schema ?? "sqlever";
    this.heartbeatStalenessMs =
      options.heartbeatStalenessMs ?? DEFAULT_HEARTBEAT_STALENESS_MS;
  }

  // -----------------------------------------------------------------------
  // Schema setup
  // -----------------------------------------------------------------------

  /** Create the queue tables if they don't exist. */
  async ensureSchema(): Promise<void> {
    const ddl = generateDDL(this.schema);
    // Execute each statement separately (pg client doesn't support multi-statement)
    const statements = splitStatements(ddl);
    for (const stmt of statements) {
      await this.db.query(stmt);
    }
  }

  // -----------------------------------------------------------------------
  // Partition management (DD9: 3-partition rotation)
  // -----------------------------------------------------------------------

  /** Get the current active partition ID. */
  async getActivePartition(): Promise<PartitionId> {
    const result = await this.db.query<{ value: string }>(
      `SELECT value FROM ${this.qualifiedName("batch_queue_meta")}
       WHERE key = 'active_partition'`,
    );
    if (result.rows.length === 0) {
      throw new Error("batch_queue_meta not initialized");
    }
    return Number(result.rows[0]!.value) as PartitionId;
  }

  /**
   * Rotate partitions (DD9).
   *
   * The rotation cycle:
   * 1. Identify current roles: active, processing = (active+1)%3, drain = (active+2)%3
   * 2. TRUNCATE the drain partition (instant cleanup, zero bloat)
   * 3. Advance: old drain becomes new active, old active becomes processing,
   *    old processing becomes drain
   *
   * Returns the new active partition ID.
   */
  async rotatePartitions(): Promise<PartitionId> {
    const current = await this.getActivePartition();
    const drain = ((current + 2) % PARTITION_COUNT) as PartitionId;
    const newActive = drain;

    // TRUNCATE the drain partition — this is the key bloat-avoidance mechanism.
    // TRUNCATE is instant regardless of row count; no dead tuples, no VACUUM.
    await this.db.query(
      `TRUNCATE ${this.qualifiedName(`batch_jobs_p${drain}`)}`,
    );

    // Update the active partition pointer
    await this.db.query(
      `UPDATE ${this.qualifiedName("batch_queue_meta")}
       SET value = $1
       WHERE key = 'active_partition'`,
      [String(newActive)],
    );

    return newActive;
  }

  /**
   * Get the partition table name for a given partition ID.
   */
  partitionTable(partitionId: PartitionId): string {
    return this.qualifiedName(`batch_jobs_p${partitionId}`);
  }

  // -----------------------------------------------------------------------
  // Job creation
  // -----------------------------------------------------------------------

  /**
   * Enqueue a new batch job into the active partition.
   */
  async createJob(options: CreateJobOptions): Promise<BatchJob> {
    const activePartition = await this.getActivePartition();

    const result = await this.db.query<BatchJob>(
      `INSERT INTO ${this.qualifiedName("batch_jobs")}
         (name, table_name, batch_size, sleep_ms, max_retries, partition_id, status)
       VALUES ($1, $2, $3, $4, $5, $6, 'pending')
       RETURNING *`,
      [
        options.name,
        options.tableName,
        options.batchSize ?? DEFAULT_BATCH_SIZE,
        options.sleepMs ?? DEFAULT_SLEEP_MS,
        options.maxRetries ?? DEFAULT_MAX_RETRIES,
        activePartition,
      ],
    );

    return result.rows[0]!;
  }

  // -----------------------------------------------------------------------
  // Dequeue (DD9: SELECT FOR UPDATE SKIP LOCKED)
  // -----------------------------------------------------------------------

  /**
   * Dequeue the next pending job from the active partition.
   *
   * Uses SELECT ... FOR UPDATE SKIP LOCKED so multiple workers can
   * dequeue concurrently without blocking each other (DD9).
   *
   * The job is atomically transitioned from pending -> running and
   * its heartbeat is set.
   *
   * Returns null if no pending jobs are available.
   */
  async dequeueJob(): Promise<BatchJob | null> {
    const activePartition = await this.getActivePartition();

    const result = await this.db.query<BatchJob>(
      `UPDATE ${this.qualifiedName("batch_jobs")}
       SET status = 'running',
           attempt = attempt + 1,
           heartbeat_at = now(),
           updated_at = now()
       WHERE id = (
         SELECT id FROM ${this.qualifiedName("batch_jobs")}
         WHERE partition_id = $1 AND status = 'pending'
         ORDER BY id
         LIMIT 1
         FOR UPDATE SKIP LOCKED
       ) AND partition_id = $1
       RETURNING *`,
      [activePartition],
    );

    return result.rows[0] ?? null;
  }

  // -----------------------------------------------------------------------
  // Job lifecycle transitions
  // -----------------------------------------------------------------------

  /**
   * Mark a job as done.
   */
  async completeJob(
    jobId: number,
    partitionId: PartitionId,
    lastPk?: string,
  ): Promise<BatchJob> {
    return this.transitionJob(jobId, partitionId, "done", {
      lastPk,
    });
  }

  /**
   * Mark a job as failed. If max retries exceeded, marks as dead instead.
   */
  async failJob(
    jobId: number,
    partitionId: PartitionId,
    errorMessage: string,
  ): Promise<BatchJob> {
    // Check current attempt count vs max_retries
    const current = await this.getJob(jobId, partitionId);
    if (!current) {
      throw new Error(`Job ${jobId} not found in partition ${partitionId}`);
    }

    if (current.status !== "running") {
      throw new Error(
        `Cannot fail job ${jobId}: status is '${current.status}', expected 'running'`,
      );
    }

    if (current.attempt >= current.max_retries) {
      // Max retries exceeded -> dead
      return this.transitionJob(jobId, partitionId, "dead", {
        errorMessage,
      });
    }

    return this.transitionJob(jobId, partitionId, "failed", {
      errorMessage,
    });
  }

  /**
   * Retry a failed or dead job. Transitions back to running.
   */
  async retryJob(jobId: number, partitionId: PartitionId): Promise<BatchJob> {
    const current = await this.getJob(jobId, partitionId);
    if (!current) {
      throw new Error(`Job ${jobId} not found in partition ${partitionId}`);
    }

    if (current.status !== "failed" && current.status !== "dead") {
      throw new Error(
        `Cannot retry job ${jobId}: status is '${current.status}', expected 'failed' or 'dead'`,
      );
    }

    const result = await this.db.query<BatchJob>(
      `UPDATE ${this.qualifiedName("batch_jobs")}
       SET status = 'running',
           attempt = attempt + 1,
           heartbeat_at = now(),
           error_message = NULL,
           updated_at = now()
       WHERE id = $1 AND partition_id = $2
       RETURNING *`,
      [jobId, partitionId],
    );

    return result.rows[0]!;
  }

  /**
   * Update heartbeat for a running job.
   */
  async updateHeartbeat(
    jobId: number,
    partitionId: PartitionId,
  ): Promise<void> {
    const result = await this.db.query(
      `UPDATE ${this.qualifiedName("batch_jobs")}
       SET heartbeat_at = now(), updated_at = now()
       WHERE id = $1 AND partition_id = $2 AND status = 'running'`,
      [jobId, partitionId],
    );
    if (result.rowCount === 0) {
      throw new Error(
        `Cannot update heartbeat for job ${jobId}: not found or not running`,
      );
    }
  }

  // -----------------------------------------------------------------------
  // Heartbeat staleness detection (SPEC Section 5.5)
  // -----------------------------------------------------------------------

  /**
   * Detect running jobs with stale heartbeats and mark them as failed.
   *
   * A job is stale if its heartbeat_at is older than the staleness
   * threshold (default: 5 minutes). This handles the case where a
   * batch worker process dies silently (OOM kill, network partition).
   *
   * Returns the list of jobs that were marked as failed.
   */
  async detectStaleJobs(): Promise<BatchJob[]> {
    const result = await this.db.query<BatchJob>(
      `UPDATE ${this.qualifiedName("batch_jobs")}
       SET status = CASE
             WHEN attempt >= max_retries THEN 'dead'
             ELSE 'failed'
           END,
           error_message = 'Worker heartbeat stale (exceeded ' ||
             $1::text || 'ms threshold)',
           updated_at = now()
       WHERE status = 'running'
         AND heartbeat_at < now() - ($1 || ' milliseconds')::interval
       RETURNING *`,
      [this.heartbeatStalenessMs],
    );

    return result.rows;
  }

  // -----------------------------------------------------------------------
  // Query helpers
  // -----------------------------------------------------------------------

  /**
   * Get a job by ID and partition.
   */
  async getJob(
    jobId: number,
    partitionId: PartitionId,
  ): Promise<BatchJob | null> {
    const result = await this.db.query<BatchJob>(
      `SELECT * FROM ${this.qualifiedName("batch_jobs")}
       WHERE id = $1 AND partition_id = $2`,
      [jobId, partitionId],
    );
    return result.rows[0] ?? null;
  }

  /**
   * List all jobs, optionally filtered by status and/or partition.
   */
  async listJobs(filters?: {
    status?: JobStatus;
    partitionId?: PartitionId;
  }): Promise<BatchJob[]> {
    const conditions: string[] = [];
    const params: unknown[] = [];

    if (filters?.status) {
      params.push(filters.status);
      conditions.push(`status = $${params.length}`);
    }
    if (filters?.partitionId !== undefined) {
      params.push(filters.partitionId);
      conditions.push(`partition_id = $${params.length}`);
    }

    const where =
      conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

    const result = await this.db.query<BatchJob>(
      `SELECT * FROM ${this.qualifiedName("batch_jobs")} ${where} ORDER BY id`,
      params,
    );

    return result.rows;
  }

  /**
   * Count jobs by status in a partition.
   */
  async countByStatus(
    partitionId: PartitionId,
  ): Promise<Record<JobStatus, number>> {
    const result = await this.db.query<{ status: JobStatus; count: string }>(
      `SELECT status, count(*)::text as count
       FROM ${this.qualifiedName("batch_jobs")}
       WHERE partition_id = $1
       GROUP BY status`,
      [partitionId],
    );

    const counts: Record<JobStatus, number> = {
      pending: 0,
      running: 0,
      done: 0,
      failed: 0,
      dead: 0,
    };

    for (const row of result.rows) {
      counts[row.status] = parseInt(row.count, 10);
    }

    return counts;
  }

  // -----------------------------------------------------------------------
  // Internal helpers
  // -----------------------------------------------------------------------

  /**
   * Transition a job to a new status with validation.
   */
  private async transitionJob(
    jobId: number,
    partitionId: PartitionId,
    newStatus: JobStatus,
    options?: { errorMessage?: string; lastPk?: string },
  ): Promise<BatchJob> {
    const current = await this.getJob(jobId, partitionId);
    if (!current) {
      throw new Error(`Job ${jobId} not found in partition ${partitionId}`);
    }

    if (!isValidTransition(current.status, newStatus)) {
      throw new Error(
        `Invalid status transition: ${current.status} -> ${newStatus}`,
      );
    }

    const setClauses = [`status = $3`, `updated_at = now()`];
    const params: unknown[] = [jobId, partitionId, newStatus];

    if (options?.errorMessage !== undefined) {
      params.push(options.errorMessage);
      setClauses.push(`error_message = $${params.length}`);
    }

    if (options?.lastPk !== undefined) {
      params.push(options.lastPk);
      setClauses.push(`last_pk = $${params.length}`);
    }

    const result = await this.db.query<BatchJob>(
      `UPDATE ${this.qualifiedName("batch_jobs")}
       SET ${setClauses.join(", ")}
       WHERE id = $1 AND partition_id = $2
       RETURNING *`,
      params,
    );

    return result.rows[0]!;
  }

  /** Build a schema-qualified table name. */
  private qualifiedName(table: string): string {
    return `${quoteIdent(this.schema)}.${quoteIdent(table)}`;
  }
}

// ---------------------------------------------------------------------------
// Utility functions
// ---------------------------------------------------------------------------

/**
 * Check if a status transition is valid per the job lifecycle.
 */
export function isValidTransition(
  from: JobStatus,
  to: JobStatus,
): boolean {
  return VALID_TRANSITIONS[from].includes(to);
}

/**
 * Simple SQL identifier quoting. Double-quotes the identifier and
 * escapes any embedded double quotes.
 */
function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

/**
 * Split a DDL string into individual statements.
 * Handles comments and semicolons correctly.
 */
export function splitStatements(ddl: string): string[] {
  const statements: string[] = [];
  let current = "";

  for (const line of ddl.split("\n")) {
    const trimmed = line.trim();

    // Skip pure comment lines and blank lines at statement boundaries
    if (trimmed.startsWith("--") && current.trim() === "") {
      continue;
    }

    current += line + "\n";

    if (trimmed.endsWith(";")) {
      const stmt = current.trim();
      if (stmt && !stmt.startsWith("--")) {
        // Remove trailing semicolons for pg client compatibility
        statements.push(stmt.replace(/;\s*$/, ""));
      }
      current = "";
    }
  }

  // Handle any trailing statement without semicolon
  const remaining = current.trim();
  if (remaining && !remaining.startsWith("--")) {
    statements.push(remaining.replace(/;\s*$/, ""));
  }

  return statements;
}
