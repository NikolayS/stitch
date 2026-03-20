// src/config/sqlever-toml.ts — TOML parser for sqlever.toml
//
// Parses a subset of TOML sufficient for sqlever configuration:
//   [analysis]
//   skip = ["SA001", "SA002"]
//   error_on_warn = false
//   max_affected_rows = 100000
//   pg_version = "16"
//
//   [analysis.rules.SA003]
//   max_affected_rows = 50000
//
//   [analysis.overrides."deploy/backfill_tiers.sql"]
//   skip = ["SA010"]
//
//   [deploy]
//   lock_retries = 3
//   lock_timeout = "5s"
//   idle_in_transaction_session_timeout = "10min"
//   search_path = "public"
//
//   [batch]
//   max_dead_tuple_ratio = 0.10
//
// This is NOT a full TOML parser — it handles the subset used by sqlever:
//   - String values (quoted), integer/float values, boolean values
//   - Arrays of strings: ["a", "b"]
//   - Sections and nested sections via dotted keys
//   - Comments (#)

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface SqleverAnalysisRuleConfig {
  /** Override max_affected_rows for this rule. */
  max_affected_rows?: number;
  /** Severity override: "error" | "warn" | "info" | "off". */
  severity?: string;
  /** Any additional rule-specific key-value pairs. */
  [key: string]: string | number | boolean | string[] | undefined;
}

export interface SqleverAnalysisOverride {
  /** Rules to skip for this file. */
  skip?: string[];
  /** Rule-level overrides for this file. */
  [key: string]: string | number | boolean | string[] | undefined;
}

export interface SqleverAnalysisConfig {
  /** Rules to skip globally. */
  skip?: string[];
  /** Treat warnings as errors. */
  error_on_warn?: boolean;
  /** Max affected rows threshold for batch-related rules. */
  max_affected_rows?: number;
  /** Target PG version for version-aware rules. */
  pg_version?: string;
  /** Per-rule configuration. */
  rules?: Record<string, SqleverAnalysisRuleConfig>;
  /** Per-file overrides. */
  overrides?: Record<string, SqleverAnalysisOverride>;
}

export interface SqleverDeployConfig {
  lock_retries?: number;
  lock_timeout?: string;
  idle_in_transaction_session_timeout?: string;
  search_path?: string;
  verify?: boolean;
  mode?: string;
  [key: string]: string | number | boolean | undefined;
}

export interface SqleverBatchConfig {
  max_dead_tuple_ratio?: number;
  [key: string]: string | number | boolean | undefined;
}

export interface SqleverToml {
  analysis?: SqleverAnalysisConfig;
  deploy?: SqleverDeployConfig;
  batch?: SqleverBatchConfig;
  /** Catch-all for sections we don't explicitly type. */
  [key: string]:
    | SqleverAnalysisConfig
    | SqleverDeployConfig
    | SqleverBatchConfig
    | Record<string, unknown>
    | undefined;
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

type TomlValue = string | number | boolean | string[] | TomlTable;
type TomlTable = { [key: string]: TomlValue };

/**
 * Parse a sqlever.toml string into a SqleverToml structure.
 */
export function parseSqleverToml(text: string): SqleverToml {
  const lines = text.split(/\r?\n/);
  const root: TomlTable = {};

  let currentTable = root;
  let currentPath: string[] = [];

  for (const rawLine of lines) {
    const line = rawLine.trim();

    // Skip blank lines and comments
    if (line === "" || line.startsWith("#")) {
      continue;
    }

    // Table header: [section] or [section.subsection] or [section."quoted.key"]
    const tableMatch = line.match(/^\[([^\]]+)\]\s*(?:#.*)?$/);
    if (tableMatch) {
      currentPath = parseTablePath(tableMatch[1]!.trim());
      currentTable = ensureTable(root, currentPath);
      continue;
    }

    // Key-value pair
    const kvMatch = line.match(/^([a-zA-Z_][a-zA-Z0-9_.-]*)\s*=\s*(.*)/);
    if (kvMatch) {
      const key = kvMatch[1]!.trim();
      const rawVal = stripComment(kvMatch[2]!).trim();
      const value = parseValue(rawVal);
      currentTable[key] = value;
      continue;
    }

    // Unrecognized — skip
  }

  return root as SqleverToml;
}

/**
 * Parse a TOML table path like `analysis.rules.SA003` or
 * `analysis.overrides."deploy/backfill.sql"` into path segments.
 */
function parseTablePath(raw: string): string[] {
  const parts: string[] = [];
  let i = 0;

  while (i < raw.length) {
    // Skip leading dots and whitespace
    while (i < raw.length && (raw[i] === "." || raw[i] === " ")) i++;
    if (i >= raw.length) break;

    if (raw[i] === '"') {
      // Quoted key
      i++; // skip opening quote
      let key = "";
      while (i < raw.length && raw[i] !== '"') {
        if (raw[i] === "\\" && i + 1 < raw.length) {
          key += raw[i + 1];
          i += 2;
        } else {
          key += raw[i];
          i++;
        }
      }
      i++; // skip closing quote
      parts.push(key);
    } else {
      // Unquoted key
      let key = "";
      while (i < raw.length && raw[i] !== "." && raw[i] !== " ") {
        key += raw[i];
        i++;
      }
      if (key) parts.push(key);
    }
  }

  return parts;
}

/**
 * Ensure a nested table exists at the given path, creating intermediate
 * tables as needed. Returns the deepest table.
 */
function ensureTable(root: TomlTable, path: string[]): TomlTable {
  let current = root;
  for (const segment of path) {
    if (!(segment in current) || typeof current[segment] !== "object" || Array.isArray(current[segment])) {
      current[segment] = {} as TomlTable;
    }
    current = current[segment] as TomlTable;
  }
  return current;
}

/**
 * Strip inline comments from a value string.
 * Respects quoted strings.
 */
function stripComment(raw: string): string {
  let inString = false;
  let inArray = false;
  for (let i = 0; i < raw.length; i++) {
    const ch = raw[i];
    if (ch === '"' && (i === 0 || raw[i - 1] !== "\\")) {
      inString = !inString;
    } else if (!inString && ch === "[") {
      inArray = true;
    } else if (!inString && ch === "]") {
      inArray = false;
    } else if (!inString && !inArray && ch === "#") {
      return raw.slice(0, i).trimEnd();
    }
  }
  return raw;
}

/**
 * Parse a TOML value: string, integer, float, boolean, or array of strings.
 */
function parseValue(raw: string): TomlValue {
  // Boolean
  if (raw === "true") return true;
  if (raw === "false") return false;

  // String (double-quoted)
  if (raw.startsWith('"')) {
    return parseQuotedString(raw);
  }

  // Array
  if (raw.startsWith("[")) {
    return parseArray(raw);
  }

  // Number (integer or float)
  // Allow underscores in numbers (TOML spec)
  const numStr = raw.replace(/_/g, "");
  if (/^[+-]?\d+\.\d+$/.test(numStr)) {
    return parseFloat(numStr);
  }
  if (/^[+-]?\d+$/.test(numStr)) {
    return parseInt(numStr, 10);
  }

  // Fallback: treat as bare string (not technically valid TOML, but robust)
  return raw;
}

/**
 * Parse a double-quoted TOML string.
 */
function parseQuotedString(raw: string): string {
  // Find the matching closing quote
  let i = 1; // skip opening quote
  let result = "";
  while (i < raw.length) {
    if (raw[i] === "\\") {
      if (i + 1 < raw.length) {
        const next = raw[i + 1];
        switch (next) {
          case "n":
            result += "\n";
            break;
          case "t":
            result += "\t";
            break;
          case "\\":
            result += "\\";
            break;
          case '"':
            result += '"';
            break;
          default:
            result += "\\" + next;
        }
        i += 2;
      } else {
        result += "\\";
        i++;
      }
    } else if (raw[i] === '"') {
      break;
    } else {
      result += raw[i];
      i++;
    }
  }
  return result;
}

/**
 * Parse a TOML array of strings: `["a", "b", "c"]`
 * Returns a string array. Only supports string elements.
 */
function parseArray(raw: string): string[] {
  const result: string[] = [];

  // Find content between [ and ]
  const inner = raw.slice(1, raw.lastIndexOf("]")).trim();
  if (inner === "") return result;

  // Parse comma-separated values
  let i = 0;
  while (i < inner.length) {
    // Skip whitespace and commas
    while (i < inner.length && (inner[i] === " " || inner[i] === "," || inner[i] === "\t")) {
      i++;
    }
    if (i >= inner.length) break;

    if (inner[i] === '"') {
      // Quoted string element
      i++; // skip opening quote
      let val = "";
      while (i < inner.length && inner[i] !== '"') {
        if (inner[i] === "\\" && i + 1 < inner.length) {
          val += inner[i + 1];
          i += 2;
        } else {
          val += inner[i];
          i++;
        }
      }
      i++; // skip closing quote
      result.push(val);
    } else {
      // Unquoted value (number, boolean — treat as string in array context)
      let val = "";
      while (i < inner.length && inner[i] !== "," && inner[i] !== "]") {
        val += inner[i];
        i++;
      }
      val = val.trim();
      if (val) result.push(val);
    }
  }

  return result;
}

// ---------------------------------------------------------------------------
// Read helpers
// ---------------------------------------------------------------------------

/**
 * Get the analysis config from a parsed sqlever.toml.
 */
export function getAnalysisConfig(toml: SqleverToml): SqleverAnalysisConfig {
  const analysis = toml.analysis;
  if (!analysis) return {};

  const config: SqleverAnalysisConfig = {};

  if (Array.isArray(analysis.skip)) {
    config.skip = analysis.skip as string[];
  }
  if (typeof analysis.error_on_warn === "boolean") {
    config.error_on_warn = analysis.error_on_warn;
  }
  if (typeof analysis.max_affected_rows === "number") {
    config.max_affected_rows = analysis.max_affected_rows;
  }
  if (typeof analysis.pg_version === "string") {
    config.pg_version = analysis.pg_version;
  }

  // Per-rule config: analysis.rules.SA003
  if (analysis.rules && typeof analysis.rules === "object") {
    config.rules = analysis.rules as Record<string, SqleverAnalysisRuleConfig>;
  }

  // Per-file overrides: analysis.overrides."file.sql"
  if (analysis.overrides && typeof analysis.overrides === "object") {
    config.overrides = analysis.overrides as Record<string, SqleverAnalysisOverride>;
  }

  return config;
}

// ---------------------------------------------------------------------------
// Serializer
// ---------------------------------------------------------------------------

/**
 * Serialize a SqleverToml back to TOML text.
 * Produces clean, canonical output.
 */
export function serializeSqleverToml(toml: SqleverToml): string {
  const lines: string[] = [];
  serializeTable(lines, toml as unknown as TomlTable, []);
  return lines.join("\n") + "\n";
}

function serializeTable(
  lines: string[],
  table: TomlTable,
  path: string[],
): void {
  // First pass: emit scalar key-value pairs at this level
  const childTables: [string, TomlTable][] = [];

  for (const [key, value] of Object.entries(table)) {
    if (value === undefined) continue;

    if (typeof value === "object" && !Array.isArray(value)) {
      childTables.push([key, value as TomlTable]);
    } else {
      lines.push(`${key} = ${serializeValue(value)}`);
    }
  }

  // Second pass: emit nested tables
  for (const [key, child] of childTables) {
    const childPath = [...path, key];
    if (lines.length > 0) lines.push("");
    lines.push(`[${serializeTablePath(childPath)}]`);
    serializeTable(lines, child, childPath);
  }
}

function serializeTablePath(path: string[]): string {
  return path
    .map((p) => {
      // Quote path segments that contain dots, spaces, or slashes
      if (/[.\s/]/.test(p)) {
        return `"${p}"`;
      }
      return p;
    })
    .join(".");
}

function serializeValue(value: TomlValue): string {
  if (typeof value === "string") {
    return `"${value.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
  }
  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }
  if (typeof value === "number") {
    return String(value);
  }
  if (Array.isArray(value)) {
    const elements = value.map((v) => `"${String(v)}"`);
    return `[${elements.join(", ")}]`;
  }
  return String(value);
}
