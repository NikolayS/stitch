// src/config/sqitch-conf.ts — Git-style INI parser for sqitch.conf
//
// Sqitch uses Config::GitLike, which is a Git-style config format:
//   - Sections:       [core]
//   - Subsections:    [engine "pg"], [target "production"]
//   - Key-value:      key = value  OR  key value
//   - Comments:       # and ; (only at line start or after whitespace)
//   - Booleans:       true/yes/on/1 are truthy; false/no/off/0 are falsy
//   - Multi-valued:   same key can appear multiple times (last wins for
//                     scalar reads, all collected for multi-value reads)
//   - Bare keys:      a key with no value is treated as boolean true
//
// Data model:
//   SqitchConf stores entries as a flat map keyed by "section.subsection.key"
//   (lowercase). For example:
//     [engine "pg"]
//       target = db:pg:mydb
//   becomes key = "engine.pg.target", value = "db:pg:mydb"

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** A single config entry preserving original order for write support. */
export interface ConfEntry {
  /** Full key: "section.subsection.key" or "section.key" (lowercased). */
  key: string;
  /** Raw string value, or `true` for bare boolean keys. */
  value: string | true;
}

/**
 * Parsed representation of a sqitch.conf file.
 * Preserves insertion order and supports multi-valued keys.
 */
export interface SqitchConf {
  /** Ordered list of all entries (for round-trip fidelity). */
  entries: ConfEntry[];
  /** Raw source text (for diffing / write-back). */
  rawLines: string[];
}

// ---------------------------------------------------------------------------
// Boolean coercion
// ---------------------------------------------------------------------------

const TRUTHY = new Set(["true", "yes", "on", "1"]);
const FALSY = new Set(["false", "no", "off", "0"]);

/**
 * Coerce a config value to boolean using Git-style rules.
 * Returns `undefined` if the value is not a recognized boolean string.
 */
export function toBool(v: string | true | undefined): boolean | undefined {
  if (v === undefined) return undefined;
  if (v === true) return true; // bare key
  const lower = v.toLowerCase().trim();
  if (TRUTHY.has(lower)) return true;
  if (FALSY.has(lower)) return false;
  return undefined;
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

/**
 * Parse a sqitch.conf (Git-style INI) string into a SqitchConf structure.
 *
 * Tolerant of:
 *   - Windows (\r\n) and Unix (\n) line endings
 *   - Leading/trailing whitespace in keys and values
 *   - Empty subsections (e.g. `[engine "pg"]` with no keys)
 *   - Quoted subsection names with spaces
 *   - Inline comments after values (# or ; preceded by whitespace)
 */
export function parseSqitchConf(text: string): SqitchConf {
  const rawLines = text.split(/\r?\n/);
  const entries: ConfEntry[] = [];

  let currentSection = "";
  let currentSubsection: string | undefined;

  for (const rawLine of rawLines) {
    const line = rawLine.trim();

    // Skip blank lines and comment-only lines
    if (line === "" || line.startsWith("#") || line.startsWith(";")) {
      continue;
    }

    // Section header: [section] or [section "subsection"]
    const sectionMatch = line.match(
      /^\[([a-zA-Z][a-zA-Z0-9._-]*)(?:\s+"([^"]*)")?\]\s*(?:[#;].*)?$/,
    );
    if (sectionMatch) {
      currentSection = sectionMatch[1]!.toLowerCase();
      currentSubsection = sectionMatch[2]; // undefined if no subsection
      continue;
    }

    // Key-value line
    // Formats:
    //   key = value
    //   key=value
    //   key value
    //   key        (bare boolean — treated as `true`)
    if (!currentSection) {
      // Key outside of any section — skip (invalid but don't crash)
      continue;
    }

    // Try key = value (with optional inline comment)
    const kvMatch = line.match(/^([a-zA-Z][a-zA-Z0-9_-]*)\s*=\s*(.*)/);
    if (kvMatch) {
      const key = kvMatch[1]!.toLowerCase();
      const rawVal = stripInlineComment(kvMatch[2]!).trim();
      const fullKey = buildFullKey(currentSection, currentSubsection, key);
      entries.push({ key: fullKey, value: rawVal === "" ? true : unquote(rawVal) });
      continue;
    }

    // Try key value (space-separated, no =)
    const spaceMatch = line.match(/^([a-zA-Z][a-zA-Z0-9_-]*)\s+(.+)/);
    if (spaceMatch) {
      const key = spaceMatch[1]!.toLowerCase();
      const rawVal = stripInlineComment(spaceMatch[2]!).trim();
      const fullKey = buildFullKey(currentSection, currentSubsection, key);
      entries.push({ key: fullKey, value: unquote(rawVal) });
      continue;
    }

    // Bare key (boolean true)
    const bareMatch = line.match(/^([a-zA-Z][a-zA-Z0-9_-]*)\s*(?:[#;].*)?$/);
    if (bareMatch) {
      const key = bareMatch[1]!.toLowerCase();
      const fullKey = buildFullKey(currentSection, currentSubsection, key);
      entries.push({ key: fullKey, value: true });
      continue;
    }

    // Unrecognized line — skip silently (robust parsing)
  }

  return { entries, rawLines };
}

/**
 * Build a dot-separated full key from section, optional subsection, and key name.
 *
 * Examples:
 *   ("core", undefined, "engine")   => "core.engine"
 *   ("engine", "pg", "target")      => "engine.pg.target"
 *   ("target", "prod", "uri")       => "target.prod.uri"
 */
function buildFullKey(
  section: string,
  subsection: string | undefined,
  key: string,
): string {
  if (subsection !== undefined) {
    return `${section}.${subsection}.${key}`;
  }
  return `${section}.${key}`;
}

/**
 * Strip inline comments: `value  # comment` => `value`
 * Only strips if the # or ; is preceded by whitespace (Git config rule).
 * Respects quoted strings — does not strip inside quotes.
 */
function stripInlineComment(raw: string): string {
  let inQuote = false;
  for (let i = 0; i < raw.length; i++) {
    const ch = raw[i]!;
    if (ch === '"' && (i === 0 || raw[i - 1] !== "\\")) {
      inQuote = !inQuote;
      continue;
    }
    if (!inQuote && (ch === "#" || ch === ";")) {
      // Only strip if preceded by whitespace (or at start)
      if (i === 0 || /\s/.test(raw[i - 1]!)) {
        return raw.slice(0, i);
      }
    }
  }
  return raw;
}

/**
 * Remove surrounding double quotes from a value, if present.
 * Also handles escaped characters within quotes.
 */
function unquote(v: string): string {
  if (v.startsWith('"') && v.endsWith('"') && v.length >= 2) {
    return v
      .slice(1, -1)
      .replace(/\\n/g, "\n")
      .replace(/\\t/g, "\t")
      .replace(/\\\\/g, "\\")
      .replace(/\\"/g, '"');
  }
  return v;
}

// ---------------------------------------------------------------------------
// Read helpers
// ---------------------------------------------------------------------------

/**
 * Get the last value for a key (scalar read — last-write-wins).
 * Key is case-insensitive for section and key name; subsection is
 * case-sensitive (matching Git behavior).
 */
export function confGet(conf: SqitchConf, key: string): string | true | undefined {
  // Find last matching entry
  let result: string | true | undefined;
  const lower = key.toLowerCase();
  for (const entry of conf.entries) {
    if (entry.key.toLowerCase() === lower) {
      result = entry.value;
    }
  }
  return result;
}

/**
 * Get the last value as a string (returns undefined for bare booleans
 * unless you want "true" — use confGetBool for those).
 */
export function confGetString(conf: SqitchConf, key: string): string | undefined {
  const v = confGet(conf, key);
  if (v === true) return "true";
  return v;
}

/**
 * Get a config value as boolean.
 */
export function confGetBool(conf: SqitchConf, key: string): boolean | undefined {
  return toBool(confGet(conf, key));
}

/**
 * Get all values for a multi-valued key (in order of appearance).
 */
export function confGetAll(conf: SqitchConf, key: string): Array<string | true> {
  const lower = key.toLowerCase();
  return conf.entries
    .filter((e) => e.key.toLowerCase() === lower)
    .map((e) => e.value);
}

/**
 * List all subsection names for a given section.
 * e.g., listSubsections(conf, "target") => ["localtest", "production"]
 */
export function confListSubsections(conf: SqitchConf, section: string): string[] {
  const prefix = section.toLowerCase() + ".";
  const subs = new Set<string>();
  for (const entry of conf.entries) {
    const lower = entry.key.toLowerCase();
    if (lower.startsWith(prefix)) {
      // key is "section.sub.key" or "section.key"
      const rest = entry.key.slice(prefix.length);
      const dotIndex = rest.indexOf(".");
      if (dotIndex > 0) {
        // Has subsection — extract the subsection name (preserve original case from entry)
        subs.add(rest.slice(0, dotIndex));
      }
    }
  }
  return [...subs];
}

/**
 * Get all key-value pairs within a section (and optionally subsection).
 * Returns a Record of key => last value.
 */
export function confGetSection(
  conf: SqitchConf,
  section: string,
  subsection?: string,
): Record<string, string | true> {
  const prefix =
    subsection !== undefined
      ? `${section}.${subsection}.`.toLowerCase()
      : `${section}.`.toLowerCase();
  const result: Record<string, string | true> = {};
  for (const entry of conf.entries) {
    const lower = entry.key.toLowerCase();
    if (lower.startsWith(prefix)) {
      const rest = entry.key.slice(prefix.length);
      // For section-only queries, skip entries that have subsections
      if (subsection === undefined && rest.includes(".")) continue;
      result[rest] = entry.value;
    }
  }
  return result;
}

// ---------------------------------------------------------------------------
// Write helpers
// ---------------------------------------------------------------------------

/**
 * Set a key-value pair. If the key already exists, updates the last
 * occurrence in entries. Otherwise appends a new entry.
 */
export function confSet(conf: SqitchConf, key: string, value: string | true): void {
  const lower = key.toLowerCase();

  // Find last matching entry index
  let lastIndex = -1;
  for (let i = conf.entries.length - 1; i >= 0; i--) {
    if (conf.entries[i]!.key.toLowerCase() === lower) {
      lastIndex = i;
      break;
    }
  }

  if (lastIndex >= 0) {
    conf.entries[lastIndex] = { key: lower, value };
  } else {
    conf.entries.push({ key: lower, value });
  }
}

/**
 * Remove all entries matching a key.
 */
export function confUnset(conf: SqitchConf, key: string): void {
  const lower = key.toLowerCase();
  conf.entries = conf.entries.filter((e) => e.key.toLowerCase() !== lower);
}

/**
 * Serialize a SqitchConf back to INI text.
 *
 * This does NOT attempt to preserve original formatting (comments,
 * blank lines, indentation). It produces a clean, canonical output.
 * For operations that need original formatting, use rawLines.
 */
export function serializeSqitchConf(conf: SqitchConf): string {
  const lines: string[] = [];
  let prevSection = "";
  let prevSubsection: string | undefined;

  // Group entries by section.subsection
  for (const entry of conf.entries) {
    const parts = entry.key.split(".");
    let section: string;
    let subsection: string | undefined;
    let keyName: string;

    if (parts.length === 3) {
      section = parts[0]!;
      subsection = parts[1]!;
      keyName = parts[2]!;
    } else if (parts.length === 2) {
      section = parts[0]!;
      subsection = undefined;
      keyName = parts[1]!;
    } else {
      // Malformed key — skip
      continue;
    }

    // Emit section header if changed
    if (section !== prevSection || subsection !== prevSubsection) {
      if (lines.length > 0) lines.push(""); // blank line between sections
      if (subsection !== undefined) {
        lines.push(`[${section} "${subsection}"]`);
      } else {
        lines.push(`[${section}]`);
      }
      prevSection = section;
      prevSubsection = subsection;
    }

    // Emit key-value
    if (entry.value === true) {
      lines.push(`\t${keyName}`);
    } else {
      lines.push(`\t${keyName} = ${entry.value}`);
    }
  }

  return lines.join("\n") + "\n";
}
