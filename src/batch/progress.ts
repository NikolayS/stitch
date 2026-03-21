// src/batch/progress.ts — Batch job progress monitoring (SPEC Section 5.5)
//
// Monitors batch job health across five dimensions:
//
// 1. **Row counting**: rows done vs total, percentage, ETA based on
//    observed throughput.
//
// 2. **Replication lag**: queries pg_stat_replication.replay_lag and
//    pauses the batch when lag exceeds a configurable threshold (default
//    10s). Most production databases have replicas; unthrottled batched
//    writes cause replica lag incidents.
//
// 3. **VACUUM pressure**: queries pg_stat_user_tables.n_dead_tup and
//    computes the dead tuple ratio n_dead_tup/(n_live_tup+n_dead_tup).
//    Pauses when the ratio exceeds a configurable threshold (default 10%).
//    Many small transactions create dead tuples; autovacuum may not keep
//    up on hot tables.
//
// 4. **Heartbeat staleness**: detects dead workers by comparing
//    heartbeat_at to a configurable threshold (default 5 min). Handles
//    OOM kill, network partition, etc.
//
// 5. **pg_stat_activity**: shows the current batch query, duration, and
//    wait event for operator visibility.

import type { DatabaseClient, QueryResult } from "../db/client";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Progress snapshot for a batch job. */
export interface BatchProgress {
  /** Rows processed so far. */
  rowsDone: number;
  /** Total rows to process (estimated). */
  rowsTotal: number;
  /** Completion percentage (0-100). */
  percentage: number;
  /** Estimated time remaining in milliseconds, or null if not enough data. */
  etaMs: number | null;
  /** Rows processed per second (throughput). */
  rowsPerSecond: number;
}

/** Replication lag information from pg_stat_replication. */
export interface ReplicationLagInfo {
  /** Replay lag as an interval string from Postgres (e.g. "00:00:05.123"). */
  replayLag: string | null;
  /** Replay lag converted to seconds. */
  replayLagSeconds: number;
  /** Whether lag exceeds the configured threshold. */
  exceedsThreshold: boolean;
  /** The configured threshold in seconds. */
  thresholdSeconds: number;
  /** Name of the replica (application_name from pg_stat_replication). */
  replicaName: string | null;
}

/** VACUUM pressure information from pg_stat_user_tables. */
export interface VacuumPressureInfo {
  /** Number of dead tuples. */
  deadTuples: number;
  /** Number of live tuples. */
  liveTuples: number;
  /** Dead tuple ratio: n_dead_tup / (n_live_tup + n_dead_tup). */
  deadTupleRatio: number;
  /** Whether the ratio exceeds the configured threshold. */
  exceedsThreshold: boolean;
  /** The configured threshold ratio (0-1). */
  thresholdRatio: number;
  /** Schema-qualified table name. */
  tableName: string;
}

/** Heartbeat staleness detection result. */
export interface HeartbeatStatus {
  /** Job ID. */
  jobId: number;
  /** Last heartbeat timestamp. */
  lastHeartbeat: Date | null;
  /** Age of heartbeat in milliseconds. */
  ageMs: number;
  /** Whether the heartbeat is stale (exceeds threshold). */
  isStale: boolean;
  /** The configured staleness threshold in milliseconds. */
  thresholdMs: number;
}

/** Current batch query from pg_stat_activity. */
export interface BatchQueryInfo {
  /** Process ID (pid) from pg_stat_activity. */
  pid: number;
  /** The currently executing query text. */
  query: string;
  /** How long the query has been running, in milliseconds. */
  durationMs: number;
  /** Current wait event type (or null if not waiting). */
  waitEventType: string | null;
  /** Current wait event (or null if not waiting). */
  waitEvent: string | null;
  /** State of the backend (active, idle, etc.). */
  state: string;
  /** Application name (should match sqlever/batch/...). */
  applicationName: string;
}

/** Configuration for the progress monitor. */
export interface ProgressMonitorConfig {
  /** Replication lag threshold in seconds. Default: 10. */
  replicationLagThresholdSeconds?: number;
  /** Dead tuple ratio threshold (0-1). Default: 0.10 (10%). */
  maxDeadTupleRatio?: number;
  /** Heartbeat staleness threshold in milliseconds. Default: 300000 (5 min). */
  heartbeatStalenessMs?: number;
  /** Schema for sqlever tables. Default: "sqlever". */
  schema?: string;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Default replication lag threshold: 10 seconds (SPEC Section 5.5). */
export const DEFAULT_REPLICATION_LAG_THRESHOLD_SECONDS = 10;

/** Default max dead tuple ratio: 10% (SPEC Section 5.5). */
export const DEFAULT_MAX_DEAD_TUPLE_RATIO = 0.10;

/** Default heartbeat staleness threshold: 5 minutes (SPEC Section 5.5). */
export const DEFAULT_HEARTBEAT_STALENESS_MS = 300_000;

// ---------------------------------------------------------------------------
// Pure functions (no DB dependency — easily testable)
// ---------------------------------------------------------------------------

/**
 * Calculate batch progress: percentage, ETA, throughput.
 *
 * ETA is computed from observed throughput (rowsDone / elapsedMs). Returns
 * null when rowsDone is 0 (not enough data to extrapolate).
 */
export function calculateProgress(
  rowsDone: number,
  rowsTotal: number,
  elapsedMs: number,
): BatchProgress {
  if (rowsTotal <= 0) {
    return {
      rowsDone,
      rowsTotal: 0,
      percentage: 100,
      etaMs: null,
      rowsPerSecond: 0,
    };
  }

  const clamped = Math.min(rowsDone, rowsTotal);
  const percentage = (clamped / rowsTotal) * 100;

  let rowsPerSecond = 0;
  let etaMs: number | null = null;

  if (elapsedMs > 0 && rowsDone > 0) {
    rowsPerSecond = (rowsDone / elapsedMs) * 1000;
    const rowsRemaining = rowsTotal - clamped;
    etaMs = rowsRemaining > 0 ? (rowsRemaining / rowsPerSecond) * 1000 : 0;
  }

  return {
    rowsDone: clamped,
    rowsTotal,
    percentage: Math.round(percentage * 100) / 100,
    etaMs: etaMs !== null ? Math.round(etaMs) : null,
    rowsPerSecond: Math.round(rowsPerSecond * 100) / 100,
  };
}

/**
 * Parse a PostgreSQL interval string (from replay_lag) to seconds.
 *
 * Handles formats:
 *   - "HH:MM:SS" or "HH:MM:SS.fff"
 *   - null or empty -> 0
 */
export function parseIntervalToSeconds(interval: string | null): number {
  if (!interval || interval.trim() === "") {
    return 0;
  }

  const trimmed = interval.trim();

  // Match HH:MM:SS or HH:MM:SS.fractional
  const match = trimmed.match(
    /^(?:(\d+)\s+days?\s+)?(\d{1,2}):(\d{2}):(\d{2})(?:\.(\d+))?$/,
  );
  if (match) {
    const days = match[1] ? parseInt(match[1], 10) : 0;
    const hours = parseInt(match[2]!, 10);
    const minutes = parseInt(match[3]!, 10);
    const seconds = parseInt(match[4]!, 10);
    const fraction = match[5] ? parseFloat(`0.${match[5]}`) : 0;
    return days * 86400 + hours * 3600 + minutes * 60 + seconds + fraction;
  }

  // Attempt to parse as a bare number (seconds)
  const num = parseFloat(trimmed);
  if (!isNaN(num)) {
    return num;
  }

  return 0;
}

/**
 * Compute the dead tuple ratio.
 *
 * Formula: n_dead_tup / (n_live_tup + n_dead_tup)
 *
 * Returns 0 when both are 0 (empty table — no pressure).
 */
export function computeDeadTupleRatio(
  deadTuples: number,
  liveTuples: number,
): number {
  const total = liveTuples + deadTuples;
  if (total <= 0) return 0;
  return deadTuples / total;
}

/**
 * Determine if a heartbeat is stale.
 *
 * @param heartbeatAt - Last heartbeat timestamp (null = never heartbeated)
 * @param thresholdMs - Staleness threshold in milliseconds
 * @param now - Current time (injectable for testing)
 * @returns Object with ageMs and isStale
 */
export function checkHeartbeatStaleness(
  heartbeatAt: Date | null,
  thresholdMs: number,
  now: Date = new Date(),
): { ageMs: number; isStale: boolean } {
  if (heartbeatAt === null) {
    // Never heartbeated — always stale
    return { ageMs: Infinity, isStale: true };
  }

  const ageMs = now.getTime() - heartbeatAt.getTime();
  return {
    ageMs,
    isStale: ageMs > thresholdMs,
  };
}

// ---------------------------------------------------------------------------
// ProgressMonitor class — queries Postgres for live metrics
// ---------------------------------------------------------------------------

/**
 * Monitors batch job progress and Postgres health metrics.
 *
 * Queries pg_stat_replication, pg_stat_user_tables, and pg_stat_activity
 * to provide real-time visibility into batch job impact.
 */
export class ProgressMonitor {
  private db: DatabaseClient;
  private config: Required<ProgressMonitorConfig>;

  constructor(db: DatabaseClient, config: ProgressMonitorConfig = {}) {
    this.db = db;
    this.config = {
      replicationLagThresholdSeconds:
        config.replicationLagThresholdSeconds ??
        DEFAULT_REPLICATION_LAG_THRESHOLD_SECONDS,
      maxDeadTupleRatio:
        config.maxDeadTupleRatio ?? DEFAULT_MAX_DEAD_TUPLE_RATIO,
      heartbeatStalenessMs:
        config.heartbeatStalenessMs ?? DEFAULT_HEARTBEAT_STALENESS_MS,
      schema: config.schema ?? "sqlever",
    };
  }

  // -----------------------------------------------------------------------
  // Replication lag monitoring (SPEC Section 5.5)
  // -----------------------------------------------------------------------

  /**
   * Query pg_stat_replication for the maximum replay lag across all
   * replicas. Returns lag info for the most-lagged replica.
   *
   * When no replicas are connected, returns zero lag (no pause needed).
   */
  async getReplicationLag(): Promise<ReplicationLagInfo> {
    const result: QueryResult<{
      replay_lag: string | null;
      application_name: string | null;
    }> = await this.db.query(
      `SELECT replay_lag::text, application_name
       FROM pg_stat_replication
       ORDER BY replay_lag DESC NULLS LAST
       LIMIT 1`,
    );

    if (result.rows.length === 0) {
      // No replicas — no lag concern
      return {
        replayLag: null,
        replayLagSeconds: 0,
        exceedsThreshold: false,
        thresholdSeconds: this.config.replicationLagThresholdSeconds,
        replicaName: null,
      };
    }

    const row = result.rows[0]!;
    const lagSeconds = parseIntervalToSeconds(row.replay_lag);

    return {
      replayLag: row.replay_lag,
      replayLagSeconds: lagSeconds,
      exceedsThreshold: lagSeconds > this.config.replicationLagThresholdSeconds,
      thresholdSeconds: this.config.replicationLagThresholdSeconds,
      replicaName: row.application_name ?? null,
    };
  }

  /**
   * Check if replication lag requires pausing the batch.
   *
   * Returns true if any replica's replay_lag exceeds the threshold.
   */
  async shouldPauseForReplicationLag(): Promise<boolean> {
    const lag = await this.getReplicationLag();
    return lag.exceedsThreshold;
  }

  // -----------------------------------------------------------------------
  // VACUUM pressure monitoring (SPEC Section 5.5)
  // -----------------------------------------------------------------------

  /**
   * Query pg_stat_user_tables for dead tuple information on a specific
   * table. Computes the dead tuple ratio and checks against threshold.
   *
   * @param schemaName - Table schema (e.g., "public")
   * @param tableName - Table name (e.g., "users")
   */
  async getVacuumPressure(
    schemaName: string,
    tableName: string,
  ): Promise<VacuumPressureInfo> {
    const result: QueryResult<{
      n_dead_tup: string;
      n_live_tup: string;
    }> = await this.db.query(
      `SELECT n_dead_tup::text, n_live_tup::text
       FROM pg_stat_user_tables
       WHERE schemaname = $1 AND relname = $2`,
      [schemaName, tableName],
    );

    if (result.rows.length === 0) {
      // Table not found in stats — assume no pressure
      return {
        deadTuples: 0,
        liveTuples: 0,
        deadTupleRatio: 0,
        exceedsThreshold: false,
        thresholdRatio: this.config.maxDeadTupleRatio,
        tableName: `${schemaName}.${tableName}`,
      };
    }

    const row = result.rows[0]!;
    const deadTuples = parseInt(row.n_dead_tup, 10);
    const liveTuples = parseInt(row.n_live_tup, 10);
    const ratio = computeDeadTupleRatio(deadTuples, liveTuples);

    return {
      deadTuples,
      liveTuples,
      deadTupleRatio: Math.round(ratio * 10000) / 10000,
      exceedsThreshold: ratio > this.config.maxDeadTupleRatio,
      thresholdRatio: this.config.maxDeadTupleRatio,
      tableName: `${schemaName}.${tableName}`,
    };
  }

  /**
   * Check if VACUUM pressure requires pausing the batch.
   *
   * Returns true if the dead tuple ratio exceeds the configured threshold.
   */
  async shouldPauseForVacuumPressure(
    schemaName: string,
    tableName: string,
  ): Promise<boolean> {
    const pressure = await this.getVacuumPressure(schemaName, tableName);
    return pressure.exceedsThreshold;
  }

  // -----------------------------------------------------------------------
  // Heartbeat staleness detection (SPEC Section 5.5)
  // -----------------------------------------------------------------------

  /**
   * Check heartbeat staleness for a specific job.
   *
   * Queries the batch_jobs table for the job's heartbeat_at and
   * determines if the worker is likely dead.
   */
  async checkJobHeartbeat(
    jobId: number,
    partitionId: number,
  ): Promise<HeartbeatStatus> {
    const s = quoteIdent(this.config.schema);

    const result: QueryResult<{ heartbeat_at: Date | null }> =
      await this.db.query(
        `SELECT heartbeat_at
         FROM ${s}."batch_jobs"
         WHERE id = $1 AND partition_id = $2 AND status = 'running'`,
        [jobId, partitionId],
      );

    if (result.rows.length === 0) {
      return {
        jobId,
        lastHeartbeat: null,
        ageMs: Infinity,
        isStale: true,
        thresholdMs: this.config.heartbeatStalenessMs,
      };
    }

    const heartbeatAt = result.rows[0]!.heartbeat_at;
    const { ageMs, isStale } = checkHeartbeatStaleness(
      heartbeatAt,
      this.config.heartbeatStalenessMs,
    );

    return {
      jobId,
      lastHeartbeat: heartbeatAt,
      ageMs,
      isStale,
      thresholdMs: this.config.heartbeatStalenessMs,
    };
  }

  /**
   * Find all running jobs with stale heartbeats.
   *
   * Returns heartbeat status for each stale job. Does NOT mark them
   * as failed — that responsibility belongs to BatchQueue.detectStaleJobs().
   */
  async findStaleWorkers(): Promise<HeartbeatStatus[]> {
    const s = quoteIdent(this.config.schema);

    const result: QueryResult<{
      id: number;
      heartbeat_at: Date | null;
    }> = await this.db.query(
      `SELECT id, heartbeat_at
       FROM ${s}."batch_jobs"
       WHERE status = 'running'
         AND (heartbeat_at IS NULL
              OR heartbeat_at < now() - ($1 || ' milliseconds')::interval)`,
      [this.config.heartbeatStalenessMs],
    );

    return result.rows.map((row) => {
      const { ageMs, isStale } = checkHeartbeatStaleness(
        row.heartbeat_at,
        this.config.heartbeatStalenessMs,
      );

      return {
        jobId: row.id,
        lastHeartbeat: row.heartbeat_at,
        ageMs,
        isStale,
        thresholdMs: this.config.heartbeatStalenessMs,
      };
    });
  }

  // -----------------------------------------------------------------------
  // pg_stat_activity monitoring (SPEC Section 5.5)
  // -----------------------------------------------------------------------

  /**
   * Query pg_stat_activity for the current batch query.
   *
   * Filters by application_name matching 'sqlever/batch%' to find
   * batch worker connections.
   */
  async getBatchQueries(): Promise<BatchQueryInfo[]> {
    const result: QueryResult<{
      pid: number;
      query: string;
      duration_ms: string;
      wait_event_type: string | null;
      wait_event: string | null;
      state: string;
      application_name: string;
    }> = await this.db.query(
      `SELECT pid,
              query,
              EXTRACT(EPOCH FROM (now() - query_start))::bigint * 1000 AS duration_ms,
              wait_event_type,
              wait_event,
              state,
              application_name
       FROM pg_stat_activity
       WHERE application_name LIKE 'sqlever/%'
         AND pid <> pg_backend_pid()
       ORDER BY query_start ASC NULLS LAST`,
    );

    return result.rows.map((row) => ({
      pid: row.pid,
      query: row.query,
      durationMs: parseInt(row.duration_ms, 10) || 0,
      waitEventType: row.wait_event_type,
      waitEvent: row.wait_event,
      state: row.state,
      applicationName: row.application_name,
    }));
  }

  // -----------------------------------------------------------------------
  // Combined health check
  // -----------------------------------------------------------------------

  /**
   * Check whether the batch should pause due to any health concern.
   *
   * Returns a reason string if the batch should pause, or null if
   * it is safe to continue.
   */
  async shouldPause(
    schemaName: string,
    tableName: string,
  ): Promise<string | null> {
    const lag = await this.getReplicationLag();
    if (lag.exceedsThreshold) {
      return (
        `Replication lag ${lag.replayLagSeconds.toFixed(1)}s exceeds ` +
        `threshold ${lag.thresholdSeconds}s` +
        (lag.replicaName ? ` (replica: ${lag.replicaName})` : "")
      );
    }

    const pressure = await this.getVacuumPressure(schemaName, tableName);
    if (pressure.exceedsThreshold) {
      return (
        `Dead tuple ratio ${(pressure.deadTupleRatio * 100).toFixed(1)}% ` +
        `exceeds threshold ${(pressure.thresholdRatio * 100).toFixed(1)}% ` +
        `on ${pressure.tableName}`
      );
    }

    return null;
  }
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

/**
 * Simple SQL identifier quoting. Double-quotes the identifier and
 * escapes any embedded double quotes.
 */
function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}
