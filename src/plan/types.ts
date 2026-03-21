// src/plan/types.ts — Core plan types and ID computation for sqlever
//
// Defines Change, Tag, Dependency, Project, Plan, and PlanEntry types.
// Implements Sqitch-compatible SHA-1 ID computation (SPEC R2).
//
// CRITICAL: The ID algorithms MUST match Sqitch byte-for-byte.
// change_id is the primary key in sqitch.changes — any divergence
// breaks mid-deploy handoff from Sqitch to sqlever.

import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** A project definition from plan pragmas (%project, %uri). */
export interface Project {
  /** Project name from %project pragma. */
  name: string;
  /** Optional project URI from %uri pragma. */
  uri?: string;
}

/** Dependency type: require or conflict. */
export type DependencyType = "require" | "conflict";

/** A single dependency reference (require or conflict). */
export interface Dependency {
  /** Whether this is a require or conflict dependency. */
  type: DependencyType;
  /** Dependency name (change name, possibly with @tag suffix). */
  name: string;
  /** Cross-project reference, if using project:change syntax. */
  project?: string;
}

/** A change entry in the plan. */
export interface Change {
  /** SHA-1 change ID computed per SPEC R2 algorithm. */
  change_id: string;
  /** Change name (e.g., "add_users"). */
  name: string;
  /** Project this change belongs to. */
  project: string;
  /** Free-text note (from # comment on change line). */
  note: string;
  /** Name of the person who planned this change. */
  planner_name: string;
  /** Email of the person who planned this change. */
  planner_email: string;
  /** ISO 8601 timestamp when this change was planned. */
  planned_at: string;
  /** Required dependencies (preserves declaration order). */
  requires: string[];
  /** Conflict dependencies (preserves declaration order). */
  conflicts: string[];
  /** Parent change ID, if this is not the first change in the plan. */
  parent?: string;
}

/** A tag entry in the plan. */
export interface Tag {
  /** SHA-1 tag ID computed per SPEC R2 algorithm. */
  tag_id: string;
  /** Tag name without @ prefix (e.g., "v1.0"). */
  name: string;
  /** Project this tag belongs to. */
  project: string;
  /** Change ID that this tag is attached to. */
  change_id: string;
  /** Free-text note. */
  note: string;
  /** Name of the person who planned this tag. */
  planner_name: string;
  /** Email of the person who planned this tag. */
  planner_email: string;
  /** ISO 8601 timestamp when this tag was planned. */
  planned_at: string;
}

/** Discriminated union of plan entries. */
export type PlanEntry =
  | { type: "change"; value: Change }
  | { type: "tag"; value: Tag };

/** A parsed plan file. */
export interface Plan {
  /** Project definition from pragmas. */
  project: Project;
  /** Raw pragmas from the plan file (key -> value). */
  pragmas: Map<string, string>;
  /** Ordered list of changes. */
  changes: Change[];
  /** Ordered list of tags. */
  tags: Tag[];
}

// ---------------------------------------------------------------------------
// Input types for ID computation
// ---------------------------------------------------------------------------

/** Input for computing a change ID. Excludes change_id itself. */
export interface ChangeIdInput {
  project: string;
  uri?: string;
  change: string;
  parent?: string;
  planner_name: string;
  planner_email: string;
  planned_at: string;
  requires: string[];
  conflicts: string[];
  note: string;
}

/** Input for computing a tag ID. Excludes tag_id itself. */
export interface TagIdInput {
  project: string;
  uri?: string;
  tag: string;
  change_id: string;
  planner_name: string;
  planner_email: string;
  planned_at: string;
  note: string;
}

// ---------------------------------------------------------------------------
// ID computation — MUST match Sqitch byte-for-byte
// ---------------------------------------------------------------------------

/**
 * Build the content string for a change ID.
 *
 * Format (each line terminated by \n):
 *   project <project_name>
 *   uri <uri>                    (conditional: only if URI present)
 *   change <change_name>
 *   parent <parent_change_id>    (conditional: only if parent present)
 *   planner <name> <<email>>
 *   date <date_iso8601>
 *   requires                     (conditional: only if requires exist)
 *     + dep1                     (indented "  + " prefix)
 *     + dep2
 *   conflicts                    (conditional: only if conflicts exist)
 *     - dep1                     (indented "  - " prefix)
 *
 *   <note_text>                  (blank line + note, only if note non-empty)
 */
export function buildChangeContent(input: ChangeIdInput): string {
  const lines: string[] = [];

  lines.push(`project ${input.project}`);

  if (input.uri != null && input.uri !== "") {
    lines.push(`uri ${input.uri}`);
  }

  lines.push(`change ${input.change}`);

  if (input.parent != null && input.parent !== "") {
    lines.push(`parent ${input.parent}`);
  }

  lines.push(`planner ${input.planner_name} <${input.planner_email}>`);
  lines.push(`date ${input.planned_at}`);

  if (input.requires.length > 0) {
    lines.push("requires");
    for (const dep of input.requires) {
      lines.push(`  + ${dep}`);
    }
  }

  if (input.conflicts.length > 0) {
    lines.push("conflicts");
    for (const dep of input.conflicts) {
      lines.push(`  - ${dep}`);
    }
  }

  if (input.note !== "") {
    lines.push("");
    lines.push(input.note);
  }

  // Lines joined by \n — NO trailing newline (matches Sqitch's Perl
  // `join "\n", (...)` which only separates, never terminates).
  return lines.join("\n");
}

/**
 * Compute a Sqitch-compatible change ID.
 *
 * SHA-1 envelope: `change <content_length>\0<content>`
 * where content_length is the byte length of the UTF-8 encoded content.
 */
export function computeChangeId(input: ChangeIdInput): string {
  const content = buildChangeContent(input);
  const contentBytes = Buffer.from(content, "utf-8");
  const header = `change ${contentBytes.length}\0`;
  const headerBytes = Buffer.from(header, "utf-8");

  const hash = createHash("sha1");
  hash.update(headerBytes);
  hash.update(contentBytes);

  return hash.digest("hex");
}

/**
 * Build the content string for a tag ID.
 *
 * Format (each line terminated by \n):
 *   project <project_name>
 *   uri <uri>                    (conditional)
 *   tag @<tag_name>              (NOTE: @ prefix on tag name!)
 *   change <change_id>
 *   planner <name> <<email>>
 *   date <date_iso8601>
 *
 *   <note_text>                  (blank line + note, only if note non-empty)
 */
export function buildTagContent(input: TagIdInput): string {
  const lines: string[] = [];

  lines.push(`project ${input.project}`);

  if (input.uri != null && input.uri !== "") {
    lines.push(`uri ${input.uri}`);
  }

  lines.push(`tag @${input.tag}`);
  lines.push(`change ${input.change_id}`);
  lines.push(`planner ${input.planner_name} <${input.planner_email}>`);
  lines.push(`date ${input.planned_at}`);

  if (input.note !== "") {
    lines.push("");
    lines.push(input.note);
  }

  // Lines joined by \n — NO trailing newline (matches Sqitch's Perl
  // `join "\n", (...)` which only separates, never terminates).
  return lines.join("\n");
}

/**
 * Compute a Sqitch-compatible tag ID.
 *
 * SHA-1 envelope: `tag <content_length>\0<content>`
 * where content_length is the byte length of the UTF-8 encoded content.
 */
export function computeTagId(input: TagIdInput): string {
  const content = buildTagContent(input);
  const contentBytes = Buffer.from(content, "utf-8");
  const header = `tag ${contentBytes.length}\0`;
  const headerBytes = Buffer.from(header, "utf-8");

  const hash = createHash("sha1");
  hash.update(headerBytes);
  hash.update(contentBytes);

  return hash.digest("hex");
}

/**
 * Compute script_hash: plain SHA-1 of raw file bytes.
 *
 * NO "blob <size>\0" prefix — Sqitch reads raw binary and feeds
 * directly to SHA-1 (App::Sqitch::Plan::Change->_deploy_hash).
 */
export function computeScriptHash(filePath: string): string {
  const raw = readFileSync(filePath);
  const hash = createHash("sha1");
  hash.update(raw);
  return hash.digest("hex");
}

/**
 * Compute script_hash from a Buffer of raw bytes.
 * Useful when you already have the content in memory.
 */
export function computeScriptHashFromBytes(content: Buffer): string {
  const hash = createHash("sha1");
  hash.update(content);
  return hash.digest("hex");
}
