// src/includes/snapshot.ts — git-aware \i / \ir resolution
//
// Resolves psql \i and \ir include directives from the git commit where
// the migration was added to the plan — not HEAD. This ensures that
// deploying historical migrations on a fresh database uses the exact
// file versions that existed when the migration was written.
//
// SPEC Section 5.2: Snapshot includes (v1.2)
//
// \i  — path relative to the working directory (repo root)
// \ir — path relative to the script's own directory
//
// When the git history is unavailable (no repo, file never committed),
// falls back to HEAD (current working tree).

import { execSync, type ExecSyncOptions } from "node:child_process";
import { dirname, join, resolve, normalize } from "node:path";
import { readFileSync, existsSync } from "node:fs";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Result of resolving all includes for a deploy script. */
export interface ResolvedScript {
  /** The fully assembled SQL content with all includes inlined. */
  content: string;
  /** Ordered list of included file paths (for diagnostics). */
  includedFiles: string[];
}

/** Options for include resolution. */
export interface ResolveOptions {
  /** Git commit hash to resolve files from. */
  commitHash?: string;
  /** Repository root directory (absolute path). */
  repoRoot: string;
  /** Maximum nesting depth for includes (prevents infinite recursion). */
  maxDepth?: number;
}

/**
 * A parsed include directive from a SQL script.
 */
export interface IncludeDirective {
  /** The raw matched line text. */
  raw: string;
  /** Whether this is \i (absolute/repo-relative) or \ir (script-relative). */
  type: "i" | "ir";
  /** The path argument from the directive. */
  path: string;
  /** Line number (0-based) where this directive was found. */
  lineIndex: number;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Default maximum include nesting depth. */
const DEFAULT_MAX_DEPTH = 64;

// ---------------------------------------------------------------------------
// Include directive parsing
// ---------------------------------------------------------------------------

/**
 * Regex to match psql \i and \ir directives.
 *
 * Matches lines like:
 *   \i path/to/file.sql
 *   \ir ../shared/functions.sql
 *   \i 'path with spaces/file.sql'
 *   \include relative_to_script.sql
 *   \include_relative ../shared/funcs.sql
 *
 * Does NOT match lines inside SQL comments or strings.
 * The pattern requires the directive at the start of a line (after optional
 * whitespace). This matches psql's own behavior — metacommands must start
 * at the beginning of a line.
 */
const INCLUDE_RE =
  /^[ \t]*\\(i|ir|include|include_relative)\s+(.+)$/;

/**
 * Parse a single line for an include directive.
 *
 * Returns the directive if found, or undefined if the line is not an include.
 */
export function parseIncludeDirective(
  line: string,
  lineIndex: number,
): IncludeDirective | undefined {
  const match = line.match(INCLUDE_RE);
  if (!match) return undefined;

  const rawType = match[1]!;
  const type: "i" | "ir" =
    rawType === "ir" || rawType === "include_relative" ? "ir" : "i";

  // Strip surrounding quotes (single or double) from the path
  let path = match[2]!.trim();

  // Remove trailing semicolons (psql does not require them but scripts
  // occasionally include them — we strip for robustness)
  path = path.replace(/;+$/, "").trim();

  if (
    (path.startsWith("'") && path.endsWith("'")) ||
    (path.startsWith('"') && path.endsWith('"'))
  ) {
    path = path.slice(1, -1);
  }

  return { raw: line, type, path, lineIndex };
}

/**
 * Scan a SQL script for all include directives.
 *
 * Lines inside block comments (/* ... *​/) are skipped.
 * Lines that begin with -- (SQL line comments) are also skipped.
 */
export function findIncludes(content: string): IncludeDirective[] {
  const lines = content.split("\n");
  const directives: IncludeDirective[] = [];
  let inBlockComment = false;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i]!;

    // Track block comments
    if (inBlockComment) {
      if (line.includes("*/")) {
        inBlockComment = false;
      }
      continue;
    }

    if (line.trimStart().startsWith("/*")) {
      // Check if the block comment closes on the same line
      if (!line.includes("*/")) {
        inBlockComment = true;
      }
      continue;
    }

    // Skip SQL line comments
    if (line.trimStart().startsWith("--")) {
      continue;
    }

    const directive = parseIncludeDirective(line, i);
    if (directive) {
      directives.push(directive);
    }
  }

  return directives;
}

// ---------------------------------------------------------------------------
// Git helpers
// ---------------------------------------------------------------------------

/**
 * Execute a git command synchronously and return stdout.
 * Returns undefined on any error (not a git repo, file doesn't exist, etc.).
 */
function gitExec(
  args: string[],
  cwd: string,
): string | undefined {
  try {
    const opts: ExecSyncOptions = {
      cwd,
      stdio: ["ignore", "pipe", "ignore"],
      timeout: 10_000,
      maxBuffer: 10 * 1024 * 1024, // 10 MB — generous for large SQL files
    };
    const result = execSync(`git ${args.join(" ")}`, opts);
    return result.toString();
  } catch {
    return undefined;
  }
}

/**
 * Check if a directory is inside a git repository.
 */
export function isGitRepo(dir: string): boolean {
  return gitExec(["rev-parse", "--is-inside-work-tree"], dir) !== undefined;
}

/**
 * Get the root directory of the git repository.
 */
export function getGitRoot(dir: string): string | undefined {
  const root = gitExec(["rev-parse", "--show-toplevel"], dir);
  return root?.trim();
}

/**
 * Retrieve the content of a file at a specific git commit.
 *
 * @param commitHash - The git commit hash
 * @param filePath   - Path relative to the repo root
 * @param repoRoot   - Absolute path to the repository root
 * @returns The file content as a string, or undefined if not found
 */
export function getFileAtCommit(
  commitHash: string,
  filePath: string,
  repoRoot: string,
): string | undefined {
  // Normalize the path to use forward slashes (git always uses POSIX paths)
  const gitPath = filePath.split("\\").join("/");
  // Use -- to separate the path from the commit to avoid ambiguity
  const result = gitExec(["show", `${commitHash}:${gitPath}`], repoRoot);
  return result;
}

/**
 * Find the nearest git commit to a given timestamp.
 *
 * The planned_at timestamp from the plan file maps to the nearest commit.
 * We find the latest commit at or before that timestamp.
 *
 * @param timestamp - ISO 8601 timestamp string
 * @param repoRoot  - Absolute path to the repository root
 * @returns The commit hash, or undefined if no commits found
 */
export function findCommitByTimestamp(
  timestamp: string,
  repoRoot: string,
): string | undefined {
  // --before accepts ISO 8601; -1 limits to one result; %H = full hash
  const result = gitExec(
    ["log", "--format=%H", "-1", `--before=${timestamp}`],
    repoRoot,
  );
  return result?.trim() || undefined;
}

/**
 * Get the current HEAD commit hash.
 */
export function getHeadCommit(repoRoot: string): string | undefined {
  const result = gitExec(["rev-parse", "HEAD"], repoRoot);
  return result?.trim() || undefined;
}

// ---------------------------------------------------------------------------
// Path resolution
// ---------------------------------------------------------------------------

/**
 * Resolve an include path based on the directive type.
 *
 * \i  paths are relative to the repo root (working directory)
 * \ir paths are relative to the including script's directory
 *
 * Returns the path relative to the repo root (for git show).
 */
export function resolveIncludePath(
  directive: IncludeDirective,
  scriptPathFromRoot: string,
  _repoRoot: string,
): string {
  if (directive.type === "i") {
    // \i: relative to repo root
    return normalize(directive.path);
  } else {
    // \ir: relative to the directory containing the including script
    const scriptDir = dirname(scriptPathFromRoot);
    return normalize(join(scriptDir, directive.path));
  }
}

// ---------------------------------------------------------------------------
// Core resolution
// ---------------------------------------------------------------------------

/**
 * Resolve all \i/\ir includes in a deploy script, substituting each include
 * directive with the file content from the specified git commit.
 *
 * Handles nested includes recursively up to maxDepth.
 *
 * @param scriptPathFromRoot - Path to the script relative to the repo root
 * @param options            - Resolution options (commit hash, repo root, max depth)
 * @returns The assembled script content and list of included files
 */
export function resolveIncludes(
  scriptPathFromRoot: string,
  options: ResolveOptions,
): ResolvedScript {
  const { repoRoot, maxDepth = DEFAULT_MAX_DEPTH } = options;
  const commitHash = options.commitHash;
  const includedFiles: string[] = [];
  const visiting = new Set<string>();

  function resolve_(
    currentPathFromRoot: string,
    depth: number,
  ): string {
    if (depth > maxDepth) {
      throw new Error(
        `Include nesting depth exceeded maximum of ${maxDepth}. ` +
        `Possible circular include at: ${currentPathFromRoot}`,
      );
    }

    // Detect circular includes
    const normalizedPath = normalize(currentPathFromRoot);
    if (visiting.has(normalizedPath)) {
      throw new Error(
        `Circular include detected: ${normalizedPath} includes itself ` +
        `(directly or transitively)`,
      );
    }
    visiting.add(normalizedPath);

    // Get file content
    const content = getFileContent(
      currentPathFromRoot,
      commitHash,
      repoRoot,
    );

    if (content === undefined) {
      throw new Error(
        `Included file not found: ${currentPathFromRoot}` +
        (commitHash ? ` (at commit ${commitHash.slice(0, 8)})` : ""),
      );
    }

    // Parse include directives
    const directives = findIncludes(content);

    if (directives.length === 0) {
      visiting.delete(normalizedPath);
      return content;
    }

    // Replace each include directive with the resolved content
    const lines = content.split("\n");
    const result: string[] = [];

    let directiveIdx = 0;
    for (let i = 0; i < lines.length; i++) {
      if (
        directiveIdx < directives.length &&
        directives[directiveIdx]!.lineIndex === i
      ) {
        const directive = directives[directiveIdx]!;
        const includePath = resolveIncludePath(
          directive,
          currentPathFromRoot,
          repoRoot,
        );

        includedFiles.push(includePath);

        // Recursively resolve the included file
        const includedContent = resolve_(includePath, depth + 1);

        // Add a comment indicating the include for debugging
        result.push(`-- [snapshot] begin include: ${directive.path}`);
        result.push(includedContent);
        result.push(`-- [snapshot] end include: ${directive.path}`);

        directiveIdx++;
      } else {
        result.push(lines[i]!);
      }
    }

    visiting.delete(normalizedPath);
    return result.join("\n");
  }

  const content = resolve_(scriptPathFromRoot, 0);

  return { content, includedFiles };
}

// ---------------------------------------------------------------------------
// File content retrieval with fallback
// ---------------------------------------------------------------------------

/**
 * Get file content, trying git first, then falling back to the working tree.
 *
 * Strategy:
 * 1. If commitHash is provided, try `git show <commit>:<path>`
 * 2. If that fails, try `git show HEAD:<path>`
 * 3. If that fails, read from the working tree
 *
 * This ensures we always get the historically correct version when possible,
 * but never fail just because git is unavailable.
 */
export function getFileContent(
  pathFromRoot: string,
  commitHash: string | undefined,
  repoRoot: string,
): string | undefined {
  // 1. Try the specific commit
  if (commitHash) {
    const content = getFileAtCommit(commitHash, pathFromRoot, repoRoot);
    if (content !== undefined) return content;
  }

  // 2. Fallback to HEAD
  const headContent = getFileAtCommit("HEAD", pathFromRoot, repoRoot);
  if (headContent !== undefined) return headContent;

  // 3. Fallback to working tree
  const absolutePath = join(repoRoot, pathFromRoot);
  if (existsSync(absolutePath)) {
    return readFileSync(absolutePath, "utf-8");
  }

  return undefined;
}

// ---------------------------------------------------------------------------
// High-level API for deploy integration
// ---------------------------------------------------------------------------

/**
 * Resolve includes for a deploy script, returning the assembled content.
 *
 * This is the main entry point for the deploy flow. It:
 * 1. Determines the git commit from the change's planned_at timestamp
 * 2. Resolves all includes to their historical versions
 * 3. Returns the assembled content ready for execution
 *
 * @param deployScriptPath  - Absolute path to the deploy script
 * @param plannedAt         - ISO 8601 timestamp when the change was planned
 * @param repoRoot          - Absolute path to the repository root
 * @param commitHash        - Optional explicit commit hash (overrides timestamp lookup)
 * @param noSnapshot        - If true, skip snapshot resolution and use HEAD
 * @returns The resolved script, or undefined if the script has no includes
 */
export function resolveDeployIncludes(
  deployScriptPath: string,
  plannedAt: string,
  repoRoot: string,
  commitHash?: string,
  noSnapshot?: boolean,
): ResolvedScript | undefined {
  const absoluteRoot = resolve(repoRoot);

  // Read the script content to check for includes first
  const scriptContent = readFileSync(deployScriptPath, "utf-8");
  const directives = findIncludes(scriptContent);

  if (directives.length === 0) {
    return undefined; // No includes — execute the original script
  }

  // Determine the commit hash
  let effectiveCommit: string | undefined;

  if (noSnapshot) {
    // --no-snapshot: always use HEAD
    effectiveCommit = getHeadCommit(absoluteRoot);
  } else if (commitHash) {
    effectiveCommit = commitHash;
  } else if (isGitRepo(absoluteRoot)) {
    // Find the commit from the planned_at timestamp
    effectiveCommit = findCommitByTimestamp(plannedAt, absoluteRoot);
    if (!effectiveCommit) {
      // Fallback to HEAD if no commit found before that date
      effectiveCommit = getHeadCommit(absoluteRoot);
    }
  }
  // If not a git repo, effectiveCommit stays undefined — fallback to working tree

  // Compute the script path relative to the repo root
  const absoluteScript = resolve(deployScriptPath);
  const scriptPathFromRoot = absoluteScript
    .slice(absoluteRoot.length)
    .replace(/^[/\\]+/, "");

  return resolveIncludes(scriptPathFromRoot, {
    commitHash: effectiveCommit,
    repoRoot: absoluteRoot,
  });
}
