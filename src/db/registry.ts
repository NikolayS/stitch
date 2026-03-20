// src/db/registry.ts — Sqitch tracking table operations
//
// All operations on the sqitch.* registry tables:
// schema creation, deployed changes, tags, events, dependencies, projects.
//
// DDL matches SPEC R3 exactly. All timestamps use clock_timestamp().
// All queries use parameterized statements — no SQL injection.

import type { DatabaseClient } from "./client";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface Project {
  project: string;
  uri: string | null;
  created_at: Date;
  creator_name: string;
  creator_email: string;
}

export interface Change {
  change_id: string;
  script_hash: string | null;
  change: string;
  project: string;
  note: string;
  committed_at: Date;
  committer_name: string;
  committer_email: string;
  planned_at: Date;
  planner_name: string;
  planner_email: string;
}

export interface Tag {
  tag_id: string;
  tag: string;
  project: string;
  change_id: string;
  note: string;
  committed_at: Date;
  committer_name: string;
  committer_email: string;
  planned_at: Date;
  planner_name: string;
  planner_email: string;
}

export interface Dependency {
  change_id: string;
  type: string;
  dependency: string;
  dependency_id: string | null;
}

export interface Event {
  event: "deploy" | "revert" | "fail" | "merge";
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
  planned_at: Date;
  planner_name: string;
  planner_email: string;
}

export interface Release {
  version: number;
  installed_at: Date;
  installer_name: string;
  installer_email: string;
}

// ---------------------------------------------------------------------------
// Input types for recording operations
// ---------------------------------------------------------------------------

export interface RecordDeployInput {
  change_id: string;
  script_hash: string | null;
  change: string;
  project: string;
  note: string;
  committer_name: string;
  committer_email: string;
  planned_at: Date;
  planner_name: string;
  planner_email: string;
  requires: string[];
  conflicts: string[];
  tags: string[];
  dependencies: Array<{
    type: string;
    dependency: string;
    dependency_id: string | null;
  }>;
}

export interface RecordTagInput {
  tag_id: string;
  tag: string;
  project: string;
  change_id: string;
  note: string;
  committer_name: string;
  committer_email: string;
  planned_at: Date;
  planner_name: string;
  planner_email: string;
}

export interface GetProjectInput {
  project: string;
  uri: string | null;
  creator_name: string;
  creator_email: string;
}

// ---------------------------------------------------------------------------
// DDL — SPEC R3 tracking schema (exact match)
// ---------------------------------------------------------------------------

/**
 * Advisory lock key used to serialize concurrent schema creation.
 * This is a 64-bit integer chosen to avoid collision with application locks.
 * The value is the CRC32 of "sqitch_registry" cast to bigint.
 */
export const REGISTRY_LOCK_KEY = 0x7371_6974; // "sqit" in ASCII

/**
 * Complete DDL for the sqitch tracking schema, matching SPEC R3 exactly.
 * Uses CREATE SCHEMA/TABLE IF NOT EXISTS for idempotent first-deploy.
 */
export const REGISTRY_DDL = `
CREATE SCHEMA IF NOT EXISTS sqitch;

CREATE TABLE IF NOT EXISTS sqitch.projects (
    project         TEXT        PRIMARY KEY,
    uri             TEXT        NULL UNIQUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    creator_name    TEXT        NOT NULL,
    creator_email   TEXT        NOT NULL
);

CREATE TABLE IF NOT EXISTS sqitch.releases (
    version         REAL        PRIMARY KEY,
    installed_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    installer_name  TEXT        NOT NULL,
    installer_email TEXT        NOT NULL
);

CREATE TABLE IF NOT EXISTS sqitch.changes (
    change_id       TEXT        PRIMARY KEY,
    script_hash     TEXT,
    change          TEXT        NOT NULL,
    project         TEXT        NOT NULL REFERENCES sqitch.projects(project) ON UPDATE CASCADE,
    note            TEXT        NOT NULL DEFAULT '',
    committed_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    committer_name  TEXT        NOT NULL,
    committer_email TEXT        NOT NULL,
    planned_at      TIMESTAMPTZ NOT NULL,
    planner_name    TEXT        NOT NULL,
    planner_email   TEXT        NOT NULL,
    UNIQUE (project, script_hash)
);

CREATE TABLE IF NOT EXISTS sqitch.tags (
    tag_id          TEXT        PRIMARY KEY,
    tag             TEXT        NOT NULL,
    project         TEXT        NOT NULL REFERENCES sqitch.projects(project) ON UPDATE CASCADE,
    change_id       TEXT        NOT NULL REFERENCES sqitch.changes(change_id) ON UPDATE CASCADE,
    note            TEXT        NOT NULL DEFAULT '',
    committed_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    committer_name  TEXT        NOT NULL,
    committer_email TEXT        NOT NULL,
    planned_at      TIMESTAMPTZ NOT NULL,
    planner_name    TEXT        NOT NULL,
    planner_email   TEXT        NOT NULL,
    UNIQUE (project, tag)
);

CREATE TABLE IF NOT EXISTS sqitch.dependencies (
    change_id    TEXT NOT NULL REFERENCES sqitch.changes(change_id) ON UPDATE CASCADE ON DELETE CASCADE,
    type         TEXT NOT NULL,
    dependency   TEXT NOT NULL,
    dependency_id TEXT NULL REFERENCES sqitch.changes(change_id) ON UPDATE CASCADE,
    PRIMARY KEY (change_id, dependency)
);

CREATE TABLE IF NOT EXISTS sqitch.events (
    event           TEXT        NOT NULL CHECK (event IN ('deploy', 'revert', 'fail', 'merge')),
    change_id       TEXT        NOT NULL,
    change          TEXT        NOT NULL,
    project         TEXT        NOT NULL REFERENCES sqitch.projects(project) ON UPDATE CASCADE,
    note            TEXT        NOT NULL DEFAULT '',
    requires        TEXT[]      NOT NULL DEFAULT '{}',
    conflicts       TEXT[]      NOT NULL DEFAULT '{}',
    tags            TEXT[]      NOT NULL DEFAULT '{}',
    committed_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    committer_name  TEXT        NOT NULL,
    committer_email TEXT        NOT NULL,
    planned_at      TIMESTAMPTZ NOT NULL,
    planner_name    TEXT        NOT NULL,
    planner_email   TEXT        NOT NULL,
    PRIMARY KEY (change_id, committed_at)
);
`.trim();

// ---------------------------------------------------------------------------
// Registry class
// ---------------------------------------------------------------------------

/**
 * Registry provides all operations on the sqitch.* tracking tables.
 *
 * It wraps a DatabaseClient and issues parameterized queries for
 * reading/writing changes, events, tags, dependencies, and projects.
 */
export class Registry {
  constructor(private readonly db: DatabaseClient) {}

  // -----------------------------------------------------------------------
  // Schema creation
  // -----------------------------------------------------------------------

  /**
   * Create the sqitch schema and all 6 tracking tables.
   *
   * Uses an advisory lock to handle concurrent first-deploys from
   * multiple CI runners. The lock is session-level and released when
   * the function completes (via pg_advisory_unlock).
   *
   * All DDL uses IF NOT EXISTS, so this is fully idempotent.
   */
  async createRegistry(): Promise<void> {
    // Acquire advisory lock to serialize concurrent schema creation
    await this.db.query("SELECT pg_advisory_lock($1)", [REGISTRY_LOCK_KEY]);

    try {
      await this.db.query(REGISTRY_DDL);
    } finally {
      // Always release the advisory lock, even on error
      await this.db.query("SELECT pg_advisory_unlock($1)", [REGISTRY_LOCK_KEY]);
    }
  }

  // -----------------------------------------------------------------------
  // Projects
  // -----------------------------------------------------------------------

  /**
   * Get a project by name. If it doesn't exist, create it.
   *
   * Returns the existing or newly-created project record.
   */
  async getProject(input: GetProjectInput): Promise<Project> {
    // Try to read first
    const existing = await this.db.query<Project>(
      "SELECT project, uri, created_at, creator_name, creator_email FROM sqitch.projects WHERE project = $1",
      [input.project],
    );

    if (existing.rows.length > 0) {
      return existing.rows[0]!;
    }

    // Insert new project
    const inserted = await this.db.query<Project>(
      `INSERT INTO sqitch.projects (project, uri, creator_name, creator_email)
       VALUES ($1, $2, $3, $4)
       RETURNING project, uri, created_at, creator_name, creator_email`,
      [input.project, input.uri, input.creator_name, input.creator_email],
    );

    return inserted.rows[0]!;
  }

  // -----------------------------------------------------------------------
  // Changes
  // -----------------------------------------------------------------------

  /**
   * Get all deployed changes for a project, ordered by committed_at.
   */
  async getDeployedChanges(project: string): Promise<Change[]> {
    const result = await this.db.query<Change>(
      `SELECT change_id, script_hash, change, project, note,
              committed_at, committer_name, committer_email,
              planned_at, planner_name, planner_email
       FROM sqitch.changes
       WHERE project = $1
       ORDER BY committed_at ASC`,
      [project],
    );
    return result.rows;
  }

  /**
   * Get the last deployed change for a project (most recent committed_at).
   * Returns null if no changes have been deployed.
   */
  async getLastDeployedChange(project: string): Promise<Change | null> {
    const result = await this.db.query<Change>(
      `SELECT change_id, script_hash, change, project, note,
              committed_at, committer_name, committer_email,
              planned_at, planner_name, planner_email
       FROM sqitch.changes
       WHERE project = $1
       ORDER BY committed_at DESC
       LIMIT 1`,
      [project],
    );
    return result.rows[0] ?? null;
  }

  // -----------------------------------------------------------------------
  // Tags
  // -----------------------------------------------------------------------

  /**
   * Get all deployed tags for a project, ordered by committed_at.
   */
  async getDeployedTags(project: string): Promise<Tag[]> {
    const result = await this.db.query<Tag>(
      `SELECT tag_id, tag, project, change_id, note,
              committed_at, committer_name, committer_email,
              planned_at, planner_name, planner_email
       FROM sqitch.tags
       WHERE project = $1
       ORDER BY committed_at ASC`,
      [project],
    );
    return result.rows;
  }

  /**
   * Record a tag in the tracking tables.
   */
  async recordTag(input: RecordTagInput): Promise<void> {
    await this.db.query(
      `INSERT INTO sqitch.tags (tag_id, tag, project, change_id, note,
                                committer_name, committer_email,
                                planned_at, planner_name, planner_email)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
      [
        input.tag_id,
        input.tag,
        input.project,
        input.change_id,
        input.note,
        input.committer_name,
        input.committer_email,
        input.planned_at,
        input.planner_name,
        input.planner_email,
      ],
    );
  }

  // -----------------------------------------------------------------------
  // Deploy / Revert
  // -----------------------------------------------------------------------

  /**
   * Record a successful deploy: insert into changes + events + dependencies.
   *
   * This should be called within a transaction to ensure atomicity.
   */
  async recordDeploy(input: RecordDeployInput): Promise<void> {
    // 1. Insert the change
    await this.db.query(
      `INSERT INTO sqitch.changes (change_id, script_hash, change, project, note,
                                   committer_name, committer_email,
                                   planned_at, planner_name, planner_email)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
      [
        input.change_id,
        input.script_hash,
        input.change,
        input.project,
        input.note,
        input.committer_name,
        input.committer_email,
        input.planned_at,
        input.planner_name,
        input.planner_email,
      ],
    );

    // 2. Insert the deploy event
    await this.db.query(
      `INSERT INTO sqitch.events (event, change_id, change, project, note,
                                  requires, conflicts, tags,
                                  committer_name, committer_email,
                                  planned_at, planner_name, planner_email)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
      [
        "deploy",
        input.change_id,
        input.change,
        input.project,
        input.note,
        input.requires,
        input.conflicts,
        input.tags,
        input.committer_name,
        input.committer_email,
        input.planned_at,
        input.planner_name,
        input.planner_email,
      ],
    );

    // 3. Insert dependencies
    for (const dep of input.dependencies) {
      await this.db.query(
        `INSERT INTO sqitch.dependencies (change_id, type, dependency, dependency_id)
         VALUES ($1, $2, $3, $4)`,
        [input.change_id, dep.type, dep.dependency, dep.dependency_id],
      );
    }
  }

  /**
   * Record a revert: delete from changes (cascades to dependencies),
   * insert a revert event.
   *
   * This should be called within a transaction to ensure atomicity.
   */
  async recordRevert(input: RecordDeployInput): Promise<void> {
    // 1. Insert the revert event (before deleting the change, since events
    //    don't FK to changes — the PK is (change_id, committed_at))
    await this.db.query(
      `INSERT INTO sqitch.events (event, change_id, change, project, note,
                                  requires, conflicts, tags,
                                  committer_name, committer_email,
                                  planned_at, planner_name, planner_email)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
      [
        "revert",
        input.change_id,
        input.change,
        input.project,
        input.note,
        input.requires,
        input.conflicts,
        input.tags,
        input.committer_name,
        input.committer_email,
        input.planned_at,
        input.planner_name,
        input.planner_email,
      ],
    );

    // 2. Delete dependencies (explicit, though ON DELETE CASCADE handles it)
    await this.db.query(
      "DELETE FROM sqitch.dependencies WHERE change_id = $1",
      [input.change_id],
    );

    // 3. Delete the change
    await this.db.query("DELETE FROM sqitch.changes WHERE change_id = $1", [
      input.change_id,
    ]);
  }

  /**
   * Record a 'fail' event: insert into events only (no changes/deps deleted).
   *
   * Used when a revert script raises an exception. The change remains
   * deployed, but the failure is logged in the event table for auditing.
   */
  async recordFailEvent(input: RecordDeployInput): Promise<void> {
    await this.db.query(
      `INSERT INTO sqitch.events (event, change_id, change, project, note,
                                  requires, conflicts, tags,
                                  committer_name, committer_email,
                                  planned_at, planner_name, planner_email)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
      [
        "fail",
        input.change_id,
        input.change,
        input.project,
        input.note,
        input.requires,
        input.conflicts,
        input.tags,
        input.committer_name,
        input.committer_email,
        input.planned_at,
        input.planner_name,
        input.planner_email,
      ],
    );
  }

  /**
   * Record a deploy failure event.
   *
   * Inserts a 'fail' event into sqitch.events. Does NOT insert into
   * sqitch.changes (the change was never successfully deployed).
   */
  async recordFail(input: RecordDeployInput): Promise<void> {
    await this.db.query(
      `INSERT INTO sqitch.events (event, change_id, change, project, note,
                                  requires, conflicts, tags,
                                  committer_name, committer_email,
                                  planned_at, planner_name, planner_email)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
      [
        "fail",
        input.change_id,
        input.change,
        input.project,
        input.note,
        input.requires,
        input.conflicts,
        input.tags,
        input.committer_name,
        input.committer_email,
        input.planned_at,
        input.planner_name,
        input.planner_email,
      ],
    );
  }

  // -----------------------------------------------------------------------
  // Events
  // -----------------------------------------------------------------------

  /**
   * Query events from sqitch.events with optional filters.
   *
   * @param project  - Project name to filter by
   * @param options  - Optional filters: event type, limit, offset, ordering
   * @returns Matching event rows
   */
  async getEvents(
    project: string,
    options: {
      event?: "deploy" | "revert" | "fail" | "merge";
      limit?: number;
      offset?: number;
      reverse?: boolean;
    } = {},
  ): Promise<Event[]> {
    const conditions: string[] = ["project = $1"];
    const params: unknown[] = [project];
    let paramIdx = 2;

    if (options.event) {
      conditions.push(`event = $${paramIdx}`);
      params.push(options.event);
      paramIdx++;
    }

    const direction = options.reverse ? "ASC" : "DESC";

    let sql = `SELECT event, change_id, change, project, note,
              requires, conflicts, tags,
              committed_at, committer_name, committer_email,
              planned_at, planner_name, planner_email
       FROM sqitch.events
       WHERE ${conditions.join(" AND ")}
       ORDER BY committed_at ${direction}`;

    if (options.limit !== undefined) {
      sql += ` LIMIT $${paramIdx}`;
      params.push(options.limit);
      paramIdx++;
    }

    if (options.offset !== undefined) {
      sql += ` OFFSET $${paramIdx}`;
      params.push(options.offset);
      paramIdx++;
    }

    const result = await this.db.query<Event>(sql, params);
    return result.rows;
  }

  // -----------------------------------------------------------------------
  // Pending changes
  // -----------------------------------------------------------------------

  /**
   * Compute which changes from the plan still need to be deployed.
   *
   * Compares the plan's change IDs against the deployed set and returns
   * the plan entries whose change_id is not in the deployed list.
   *
   * @param planChangeIds - Ordered list of change IDs from the plan
   * @param deployedChangeIds - Set of change IDs already deployed
   * @returns The change IDs from planChangeIds not in deployedChangeIds, preserving order
   */
  getPendingChanges(
    planChangeIds: string[],
    deployedChangeIds: Set<string>,
  ): string[] {
    return planChangeIds.filter((id) => !deployedChangeIds.has(id));
  }
}
