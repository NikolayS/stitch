// src/plan/parser.ts — Full sqitch.plan format parser
//
// Parses plan files into the Plan structure defined in types.ts.
// Computes change IDs and tag IDs using the Sqitch-compatible
// SHA-1 algorithm, and links each change to its parent.
//
// SPEC R2: Plan file format compatibility

import {
  computeChangeId,
  computeTagId,
} from "./types";

import type {
  Plan,
  Change,
  Tag,
  Project,
  Dependency,
  ChangeIdInput,
  TagIdInput,
} from "./types";

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

export class PlanParseError extends Error {
  constructor(
    message: string,
    public readonly line: number,
    public readonly content: string,
  ) {
    super(`Plan parse error at line ${line}: ${message}\n  ${content}`);
    this.name = "PlanParseError";
  }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/**
 * Parse a pragma line: `%key=value`
 */
function parsePragma(line: string): [string, string] {
  // Strip leading %
  const rest = line.slice(1);
  const eqIdx = rest.indexOf("=");
  if (eqIdx === -1) {
    return [rest.trim(), ""];
  }
  return [rest.slice(0, eqIdx).trim(), rest.slice(eqIdx + 1).trim()];
}

/**
 * Parse dependency list from `[dep1 dep2 !conflict1]` bracket syntax.
 *
 * Returns an array of Dependency objects.
 * - `!name` is a conflict
 * - `project:change` is a cross-project dependency
 * - plain `name` is a require
 */
export function parseDependencies(bracketContent: string): Dependency[] {
  const trimmed = bracketContent.trim();
  if (trimmed === "") return [];

  const deps: Dependency[] = [];
  const tokens = trimmed.split(/\s+/);

  for (const token of tokens) {
    if (token === "") continue;

    let type: "require" | "conflict" = "require";
    let raw = token;

    if (raw.startsWith("!")) {
      type = "conflict";
      raw = raw.slice(1);
    }

    // Cross-project: project:change
    const colonIdx = raw.indexOf(":");
    if (colonIdx !== -1) {
      deps.push({
        type,
        name: raw.slice(colonIdx + 1),
        project: raw.slice(0, colonIdx),
      });
    } else {
      deps.push({ type, name: raw });
    }
  }

  return deps;
}

/**
 * Parse a change or tag entry line.
 *
 * Change format:
 *   change_name [deps] YYYY-MM-DDTHH:MM:SSZ planner_name <planner_email> # note
 *
 * Tag format:
 *   @tag_name YYYY-MM-DDTHH:MM:SSZ planner_name <planner_email> # note
 *
 * The timestamp regex is used to split name/deps from the rest.
 * Planner name is everything between timestamp and `<email>`.
 * Note is everything after `# ` or `#` (hash followed by text).
 */

// ISO 8601 timestamp: 2024-01-15T10:30:00Z
const TIMESTAMP_RE = /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z/;

interface ParsedEntry {
  name: string;
  dependencies: Dependency[];
  timestamp: string;
  plannerName: string;
  plannerEmail: string;
  note: string;
  isTag: boolean;
}

function parseEntry(line: string, lineNum: number): ParsedEntry {
  const isTag = line.startsWith("@");

  // Find timestamp
  const tsMatch = line.match(TIMESTAMP_RE);
  if (!tsMatch || tsMatch.index === undefined) {
    throw new PlanParseError("Missing timestamp", lineNum, line);
  }

  const beforeTs = line.slice(0, tsMatch.index).trim();
  const timestamp = tsMatch[0];
  const afterTs = line.slice(tsMatch.index + timestamp.length);

  // Parse name and dependencies from beforeTs
  let name: string;
  let dependencies: Dependency[] = [];

  if (isTag) {
    // Tag: @tag_name — strip leading @
    name = beforeTs.slice(1).trim();
  } else {
    // Change: name [deps]
    const bracketStart = beforeTs.indexOf("[");
    if (bracketStart !== -1) {
      name = beforeTs.slice(0, bracketStart).trim();
      const bracketEnd = beforeTs.indexOf("]", bracketStart);
      if (bracketEnd === -1) {
        throw new PlanParseError("Unclosed dependency bracket", lineNum, line);
      }
      const depContent = beforeTs.slice(bracketStart + 1, bracketEnd);
      dependencies = parseDependencies(depContent);
    } else {
      name = beforeTs.trim();
    }
  }

  if (name === "") {
    throw new PlanParseError("Empty entry name", lineNum, line);
  }

  // Parse planner and note from afterTs
  // Format: " planner_name <planner_email> # note"
  // The email is enclosed in <>, note comes after #

  // Find the email: last <...> pair
  const emailStartIdx = afterTs.indexOf("<");
  const emailEndIdx = afterTs.indexOf(">", emailStartIdx);

  if (emailStartIdx === -1 || emailEndIdx === -1) {
    throw new PlanParseError("Missing planner email (<email>)", lineNum, line);
  }

  const plannerEmail = afterTs.slice(emailStartIdx + 1, emailEndIdx);
  const plannerName = afterTs.slice(0, emailStartIdx).trim();

  // Note is after the email closing >
  const afterEmail = afterTs.slice(emailEndIdx + 1);

  let note = "";
  // Look for # (hash). The note text is everything after it.
  // Sqitch uses "# " (hash space) but customer-zero sometimes has "#" without space.
  const hashIdx = afterEmail.indexOf("#");
  if (hashIdx !== -1) {
    note = afterEmail.slice(hashIdx + 1).trimStart();
    // Also trim trailing whitespace
    note = note.trimEnd();
  }

  return {
    name,
    dependencies,
    timestamp,
    plannerName,
    plannerEmail,
    note,
    isTag,
  };
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Parse a sqitch.plan file into a Plan structure.
 *
 * Computes change IDs and tag IDs using the Sqitch-compatible algorithm,
 * and links each change to its parent (preceding change's ID).
 *
 * @param content - Raw plan file content as a string
 * @returns Parsed Plan with computed IDs and parent links
 * @throws PlanParseError on malformed input
 */
export function parsePlan(content: string): Plan {
  const lines = content.split("\n");
  const pragmas = new Map<string, string>();
  const changes: Change[] = [];
  const tags: Tag[] = [];

  // First pass: extract pragmas
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i]!.trimEnd();
    if (line.startsWith("%")) {
      const [key, value] = parsePragma(line);
      pragmas.set(key, value);
    }
  }

  // Build project from pragmas
  const projectName = pragmas.get("project");
  if (!projectName) {
    throw new PlanParseError(
      "Missing required %project pragma",
      0,
      content.slice(0, 100),
    );
  }

  const projectUri = pragmas.get("uri");
  const project: Project = {
    name: projectName,
    ...(projectUri ? { uri: projectUri } : {}),
  };

  // Second pass: parse changes and tags
  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i]!;
    const line = raw.trimEnd();

    // Skip empty lines, comments, and pragmas
    if (line === "" || line.startsWith("#") || line.startsWith("%")) {
      continue;
    }

    const lineNum = i + 1; // 1-based

    if (line.startsWith("@")) {
      // Tag entry
      const entry = parseEntry(line, lineNum);

      // Tags attach to the most recent change
      if (changes.length === 0) {
        throw new PlanParseError(
          "Tag before any change",
          lineNum,
          line,
        );
      }

      const lastChange = changes[changes.length - 1]!;

      const tagInput: TagIdInput = {
        project: projectName,
        ...(projectUri ? { uri: projectUri } : {}),
        tag: entry.name,
        change_id: lastChange.change_id,
        planner_name: entry.plannerName,
        planner_email: entry.plannerEmail,
        planned_at: entry.timestamp,
        note: entry.note,
      };

      const tagId = computeTagId(tagInput);

      tags.push({
        tag_id: tagId,
        name: entry.name,
        project: projectName,
        change_id: lastChange.change_id,
        note: entry.note,
        planner_name: entry.plannerName,
        planner_email: entry.plannerEmail,
        planned_at: entry.timestamp,
      });
    } else {
      // Change entry
      const entry = parseEntry(line, lineNum);

      // Split dependencies into requires and conflicts
      const requires: string[] = [];
      const conflicts: string[] = [];
      for (const dep of entry.dependencies) {
        if (dep.type === "require") {
          // For cross-project deps, preserve the project:change format
          const depStr = dep.project
            ? `${dep.project}:${dep.name}`
            : dep.name;
          requires.push(depStr);
        } else {
          const depStr = dep.project
            ? `${dep.project}:${dep.name}`
            : dep.name;
          conflicts.push(depStr);
        }
      }

      // Parent is the preceding change's ID (null for the first change)
      const parent =
        changes.length > 0
          ? changes[changes.length - 1]!.change_id
          : undefined;

      const changeInput: ChangeIdInput = {
        project: projectName,
        ...(projectUri ? { uri: projectUri } : {}),
        change: entry.name,
        ...(parent ? { parent } : {}),
        planner_name: entry.plannerName,
        planner_email: entry.plannerEmail,
        planned_at: entry.timestamp,
        requires,
        conflicts,
        note: entry.note,
      };

      const changeId = computeChangeId(changeInput);

      changes.push({
        change_id: changeId,
        name: entry.name,
        project: projectName,
        note: entry.note,
        planner_name: entry.plannerName,
        planner_email: entry.plannerEmail,
        planned_at: entry.timestamp,
        requires,
        conflicts,
        ...(parent ? { parent } : {}),
      });
    }
  }

  return {
    project,
    pragmas,
    changes,
    tags,
  };
}
