// src/plan/writer.ts — sqitch.plan writer (append-only for add)
//
// Serializes Plan, Change, and Tag objects to sqitch.plan format.
// Append operations preserve existing file content exactly (DD5).
//
// Format reference (R2):
//   %syntax-version=1.0.0
//   %project=myproject
//   %uri=https://example.com/
//
//   change_name [dep1 dep2 !conflict1] 2024-01-15T10:30:00Z Planner Name <email> # note
//   @tag_name 2024-01-15T10:31:00Z Planner Name <email> # tag note

import { readFile, appendFile } from "node:fs/promises";
import type { Plan, Change, Tag } from "./types";

// ---------------------------------------------------------------------------
// Serialization — individual entries
// ---------------------------------------------------------------------------

/**
 * Serialize a single Change to its plan file line.
 *
 * Format: `<name> [<deps>] <timestamp> <planner_name> <<email>> # <note>`
 * - Dependencies in `[]` only if non-empty
 * - Requires are plain names, conflicts prefixed with `!`
 * - Note section (`# <note>`) omitted if note is empty
 */
export function serializeChange(change: Change): string {
  const parts: string[] = [change.name];

  // Dependencies block — only present if there are requires or conflicts
  const deps: string[] = [
    ...change.requires,
    ...change.conflicts.map((c) => `!${c}`),
  ];
  if (deps.length > 0) {
    parts.push(`[${deps.join(" ")}]`);
  }

  parts.push(change.planned_at);
  parts.push(`${change.planner_name} <${change.planner_email}>`);

  if (change.note !== "") {
    parts.push(`# ${change.note}`);
  }

  return parts.join(" ");
}

/**
 * Serialize a single Tag to its plan file line.
 *
 * Format: `@<name> <timestamp> <planner_name> <<email>> # <note>`
 * - Tag name is prefixed with `@`
 * - Note section (`# <note>`) omitted if note is empty
 */
export function serializeTag(tag: Tag): string {
  const parts: string[] = [`@${tag.name}`];

  parts.push(tag.planned_at);
  parts.push(`${tag.planner_name} <${tag.planner_email}>`);

  if (tag.note !== "") {
    parts.push(`# ${tag.note}`);
  }

  return parts.join(" ");
}

// ---------------------------------------------------------------------------
// Serialization — full plan
// ---------------------------------------------------------------------------

/**
 * Serialize a full Plan to sqitch.plan file content.
 *
 * Output order:
 *   1. Pragmas (syntax-version first if present, then project, then uri,
 *      then any remaining pragmas in insertion order)
 *   2. Blank line separator
 *   3. Entries (changes interleaved with tags in their declared order)
 *
 * The Plan type stores changes and tags separately. To produce correct
 * interleaved output, we rebuild entry order: each tag's change_id links
 * it to the change it follows.
 */
export function serializePlan(plan: Plan): string {
  const lines: string[] = [];

  // --- Pragmas ---
  // Emit in canonical order: syntax-version, project, uri, then rest.
  const emittedKeys = new Set<string>();

  const syntaxVersion = plan.pragmas.get("syntax-version");
  if (syntaxVersion != null) {
    lines.push(`%syntax-version=${syntaxVersion}`);
    emittedKeys.add("syntax-version");
  }

  const projectPragma = plan.pragmas.get("project");
  if (projectPragma != null) {
    lines.push(`%project=${projectPragma}`);
    emittedKeys.add("project");
  }

  const uriPragma = plan.pragmas.get("uri");
  if (uriPragma != null) {
    lines.push(`%uri=${uriPragma}`);
    emittedKeys.add("uri");
  }

  // Any remaining pragmas (rare but possible)
  for (const [key, value] of plan.pragmas) {
    if (!emittedKeys.has(key)) {
      lines.push(`%${key}=${value}`);
    }
  }

  // Blank line after pragmas (only if we emitted pragmas)
  if (lines.length > 0) {
    lines.push("");
  }

  // --- Entries ---
  // Build a map from change_id to tags that follow that change
  const tagsByChangeId = new Map<string, Tag[]>();
  for (const tag of plan.tags) {
    const existing = tagsByChangeId.get(tag.change_id);
    if (existing != null) {
      existing.push(tag);
    } else {
      tagsByChangeId.set(tag.change_id, [tag]);
    }
  }

  // Emit changes in order, with their tags immediately after
  for (const change of plan.changes) {
    lines.push(serializeChange(change));
    const changeTags = tagsByChangeId.get(change.change_id);
    if (changeTags != null) {
      for (const tag of changeTags) {
        lines.push(serializeTag(tag));
      }
    }
  }

  // Trailing newline — sqitch.plan files end with a newline
  return lines.join("\n") + "\n";
}

// ---------------------------------------------------------------------------
// Append operations — DD5: never rewrite existing entries
// ---------------------------------------------------------------------------

/**
 * Append a change entry to an existing plan file.
 *
 * Reads the file, ensures it ends with a newline, then appends the
 * serialized change line. Existing content is preserved byte-for-byte.
 */
export async function appendChange(
  planPath: string,
  change: Change,
): Promise<void> {
  const existing = await readFile(planPath, "utf-8");
  const suffix = existing.length > 0 && !existing.endsWith("\n") ? "\n" : "";
  await appendFile(planPath, suffix + serializeChange(change) + "\n", "utf-8");
}

/**
 * Append a tag entry to an existing plan file.
 *
 * Reads the file, ensures it ends with a newline, then appends the
 * serialized tag line. Existing content is preserved byte-for-byte.
 */
export async function appendTag(
  planPath: string,
  tag: Tag,
): Promise<void> {
  const existing = await readFile(planPath, "utf-8");
  const suffix = existing.length > 0 && !existing.endsWith("\n") ? "\n" : "";
  await appendFile(planPath, suffix + serializeTag(tag) + "\n", "utf-8");
}
