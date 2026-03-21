// tests/unit/snapshot-includes.test.ts — Tests for snapshot include resolution
//
// Tests use real temporary git repositories to verify end-to-end behavior
// of \i/\ir resolution from historical commits.

import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { mkdtempSync, writeFileSync, mkdirSync, rmSync } from "node:fs";
import { execSync } from "node:child_process";
import { join } from "node:path";
import { tmpdir } from "node:os";

import {
  parseIncludeDirective,
  findIncludes,
  resolveIncludePath,
  resolveIncludes,
  getFileAtCommit,
  getFileContent,
  isGitRepo,
  getGitRoot,
  findCommitByTimestamp,
  getHeadCommit,
  resolveDeployIncludes,
  type IncludeDirective,
} from "../../src/includes/snapshot";

// ---------------------------------------------------------------------------
// Helpers — temporary git repos
// ---------------------------------------------------------------------------

/** Create a temporary directory for a test git repo. */
function makeTempDir(): string {
  return mkdtempSync(join(tmpdir(), "sqlever-test-"));
}

/**
 * Initialize a git repo with an initial commit.
 * Returns the repo root path.
 */
function initGitRepo(): string {
  const dir = makeTempDir();
  execSync("git init", { cwd: dir, stdio: "ignore" });
  execSync('git config user.email "test@sqlever.dev"', {
    cwd: dir,
    stdio: "ignore",
  });
  execSync('git config user.name "Test User"', {
    cwd: dir,
    stdio: "ignore",
  });
  // Create an initial commit
  writeFileSync(join(dir, ".gitkeep"), "");
  execSync("git add .gitkeep", { cwd: dir, stdio: "ignore" });
  execSync('git commit -m "initial"', { cwd: dir, stdio: "ignore" });
  return dir;
}

/**
 * Create a file, add, and commit it. Returns the commit hash.
 */
function commitFile(
  repoRoot: string,
  filePath: string,
  content: string,
  message?: string,
): string {
  const absolutePath = join(repoRoot, filePath);
  const dir = absolutePath.substring(0, absolutePath.lastIndexOf("/"));
  mkdirSync(dir, { recursive: true });
  writeFileSync(absolutePath, content);
  execSync(`git add "${filePath}"`, { cwd: repoRoot, stdio: "ignore" });
  execSync(`git commit -m "${message ?? `add ${filePath}`}"`, {
    cwd: repoRoot,
    stdio: "ignore",
  });
  const hash = execSync("git rev-parse HEAD", { cwd: repoRoot })
    .toString()
    .trim();
  return hash;
}

/** Clean up a temporary directory. */
function cleanupDir(dir: string): void {
  try {
    rmSync(dir, { recursive: true, force: true });
  } catch {
    // Best effort
  }
}

// ---------------------------------------------------------------------------
// Tests: parseIncludeDirective
// ---------------------------------------------------------------------------

describe("parseIncludeDirective", () => {
  it("parses \\i directive", () => {
    const result = parseIncludeDirective("\\i shared/functions.sql", 0);
    expect(result).toBeDefined();
    expect(result!.type).toBe("i");
    expect(result!.path).toBe("shared/functions.sql");
    expect(result!.lineIndex).toBe(0);
  });

  it("parses \\ir directive", () => {
    const result = parseIncludeDirective("\\ir ../shared/funcs.sql", 5);
    expect(result).toBeDefined();
    expect(result!.type).toBe("ir");
    expect(result!.path).toBe("../shared/funcs.sql");
    expect(result!.lineIndex).toBe(5);
  });

  it("parses \\include as \\i", () => {
    const result = parseIncludeDirective("\\include shared/views.sql", 0);
    expect(result).toBeDefined();
    expect(result!.type).toBe("i");
    expect(result!.path).toBe("shared/views.sql");
  });

  it("parses \\include_relative as \\ir", () => {
    const result = parseIncludeDirective(
      "\\include_relative ../common.sql",
      0,
    );
    expect(result).toBeDefined();
    expect(result!.type).toBe("ir");
    expect(result!.path).toBe("../common.sql");
  });

  it("handles quoted paths with single quotes", () => {
    const result = parseIncludeDirective("\\i 'path with spaces/file.sql'", 0);
    expect(result).toBeDefined();
    expect(result!.path).toBe("path with spaces/file.sql");
  });

  it("handles quoted paths with double quotes", () => {
    const result = parseIncludeDirective(
      '\\i "path with spaces/file.sql"',
      0,
    );
    expect(result).toBeDefined();
    expect(result!.path).toBe("path with spaces/file.sql");
  });

  it("strips trailing semicolons from path", () => {
    const result = parseIncludeDirective("\\i shared/functions.sql;", 0);
    expect(result).toBeDefined();
    expect(result!.path).toBe("shared/functions.sql");
  });

  it("returns undefined for non-include lines", () => {
    expect(parseIncludeDirective("SELECT 1;", 0)).toBeUndefined();
    expect(
      parseIncludeDirective("CREATE TABLE users (id int);", 0),
    ).toBeUndefined();
    expect(parseIncludeDirective("", 0)).toBeUndefined();
  });

  it("returns undefined for lines not starting with backslash", () => {
    expect(
      parseIncludeDirective("some text \\i file.sql", 0),
    ).toBeUndefined();
  });

  it("allows leading whitespace before directive", () => {
    const result = parseIncludeDirective("  \\i shared/file.sql", 0);
    expect(result).toBeDefined();
    expect(result!.type).toBe("i");
    expect(result!.path).toBe("shared/file.sql");
  });
});

// ---------------------------------------------------------------------------
// Tests: findIncludes
// ---------------------------------------------------------------------------

describe("findIncludes", () => {
  it("finds multiple include directives", () => {
    const content = [
      "-- Migration script",
      "\\i shared/types.sql",
      "CREATE TABLE users (id int);",
      "\\ir ../common/functions.sql",
      "SELECT 1;",
    ].join("\n");

    const directives = findIncludes(content);
    expect(directives).toHaveLength(2);
    expect(directives[0]!.type).toBe("i");
    expect(directives[0]!.path).toBe("shared/types.sql");
    expect(directives[1]!.type).toBe("ir");
    expect(directives[1]!.path).toBe("../common/functions.sql");
  });

  it("skips includes in SQL line comments", () => {
    const content = [
      "-- \\i shared/disabled.sql",
      "\\i shared/active.sql",
    ].join("\n");

    const directives = findIncludes(content);
    expect(directives).toHaveLength(1);
    expect(directives[0]!.path).toBe("shared/active.sql");
  });

  it("skips includes in block comments", () => {
    const content = [
      "/* block comment start",
      "\\i shared/disabled.sql",
      "*/",
      "\\i shared/active.sql",
    ].join("\n");

    const directives = findIncludes(content);
    expect(directives).toHaveLength(1);
    expect(directives[0]!.path).toBe("shared/active.sql");
  });

  it("handles single-line block comments", () => {
    const content = [
      "/* \\i shared/disabled.sql */",
      "\\i shared/active.sql",
    ].join("\n");

    const directives = findIncludes(content);
    expect(directives).toHaveLength(1);
    expect(directives[0]!.path).toBe("shared/active.sql");
  });

  it("returns empty array for scripts with no includes", () => {
    const content = "CREATE TABLE users (id int);\nSELECT 1;\n";
    expect(findIncludes(content)).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// Tests: resolveIncludePath
// ---------------------------------------------------------------------------

describe("resolveIncludePath", () => {
  it("resolves \\i path relative to repo root", () => {
    const directive: IncludeDirective = {
      raw: "\\i shared/functions.sql",
      type: "i",
      path: "shared/functions.sql",
      lineIndex: 0,
    };

    const result = resolveIncludePath(
      directive,
      "deploy/001-init.sql",
      "/repo",
    );
    expect(result).toBe("shared/functions.sql");
  });

  it("resolves \\ir path relative to script directory", () => {
    const directive: IncludeDirective = {
      raw: "\\ir ../shared/functions.sql",
      type: "ir",
      path: "../shared/functions.sql",
      lineIndex: 0,
    };

    const result = resolveIncludePath(
      directive,
      "deploy/001-init.sql",
      "/repo",
    );
    expect(result).toBe("shared/functions.sql");
  });

  it("resolves \\ir path in same directory", () => {
    const directive: IncludeDirective = {
      raw: "\\ir helpers.sql",
      type: "ir",
      path: "helpers.sql",
      lineIndex: 0,
    };

    const result = resolveIncludePath(
      directive,
      "deploy/001-init.sql",
      "/repo",
    );
    expect(result).toBe("deploy/helpers.sql");
  });
});

// ---------------------------------------------------------------------------
// Tests: git helpers (with real repos)
// ---------------------------------------------------------------------------

describe("git helpers", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("isGitRepo returns true for a git repo", () => {
    expect(isGitRepo(repoRoot)).toBe(true);
  });

  it("isGitRepo returns false for a non-repo directory", () => {
    const nonRepo = makeTempDir();
    try {
      expect(isGitRepo(nonRepo)).toBe(false);
    } finally {
      cleanupDir(nonRepo);
    }
  });

  it("getGitRoot returns the repo root", () => {
    const subDir = join(repoRoot, "sub", "dir");
    mkdirSync(subDir, { recursive: true });
    const root = getGitRoot(subDir);
    // Resolve both to handle symlinks (e.g., /private/tmp vs /tmp on macOS)
    expect(root).toBeDefined();
    const { realpathSync } = require("node:fs");
    expect(realpathSync(root!)).toBe(realpathSync(repoRoot));
  });

  it("getFileAtCommit retrieves file content at a specific commit", () => {
    const hash1 = commitFile(repoRoot, "file.sql", "-- version 1\n");
    commitFile(repoRoot, "file.sql", "-- version 2\n");

    // Get the old version
    const content = getFileAtCommit(hash1, "file.sql", repoRoot);
    expect(content).toBe("-- version 1\n");

    // Get the current version
    const content2 = getFileAtCommit("HEAD", "file.sql", repoRoot);
    expect(content2).toBe("-- version 2\n");
  });

  it("getFileAtCommit returns undefined for non-existent file", () => {
    const content = getFileAtCommit("HEAD", "nonexistent.sql", repoRoot);
    expect(content).toBeUndefined();
  });

  it("getHeadCommit returns the current HEAD hash", () => {
    const head = getHeadCommit(repoRoot);
    expect(head).toBeDefined();
    expect(head!.length).toBe(40); // Full SHA-1
  });

  it("findCommitByTimestamp finds the right commit", () => {
    commitFile(repoRoot, "a.sql", "-- a\n");
    // Use a far-future timestamp to find the latest commit
    const commit = findCommitByTimestamp("2099-12-31T23:59:59Z", repoRoot);
    expect(commit).toBeDefined();
    expect(commit!.length).toBe(40);
  });
});

// ---------------------------------------------------------------------------
// Tests: getFileContent (fallback chain)
// ---------------------------------------------------------------------------

describe("getFileContent", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("returns file at specific commit when available", () => {
    const hash1 = commitFile(repoRoot, "func.sql", "-- v1\n");
    commitFile(repoRoot, "func.sql", "-- v2\n");

    const content = getFileContent("func.sql", hash1, repoRoot);
    expect(content).toBe("-- v1\n");
  });

  it("falls back to HEAD when commit hash is invalid", () => {
    commitFile(repoRoot, "func.sql", "-- current\n");

    const content = getFileContent(
      "func.sql",
      "0000000000000000000000000000000000000000",
      repoRoot,
    );
    expect(content).toBe("-- current\n");
  });

  it("falls back to working tree when file is not in git", () => {
    // Write a file but don't commit it
    writeFileSync(join(repoRoot, "untracked.sql"), "-- untracked\n");

    const content = getFileContent("untracked.sql", undefined, repoRoot);
    expect(content).toBe("-- untracked\n");
  });

  it("returns undefined when file does not exist anywhere", () => {
    const content = getFileContent("nonexistent.sql", undefined, repoRoot);
    expect(content).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Tests: resolveIncludes (core resolution with real git)
// ---------------------------------------------------------------------------

describe("resolveIncludes", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("resolves a single \\i include from a specific commit", () => {
    // Create shared file v1 and commit
    const hash1 = commitFile(
      repoRoot,
      "shared/functions.sql",
      "CREATE FUNCTION greet_v1() RETURNS void AS $$ $$ LANGUAGE sql;\n",
    );

    // Update shared file v2
    commitFile(
      repoRoot,
      "shared/functions.sql",
      "CREATE FUNCTION greet_v2() RETURNS void AS $$ $$ LANGUAGE sql;\n",
    );

    // Create deploy script that includes the shared file
    commitFile(
      repoRoot,
      "deploy/001-init.sql",
      "-- migration 001\n\\i shared/functions.sql\nSELECT 1;\n",
    );

    const result = resolveIncludes("deploy/001-init.sql", {
      commitHash: hash1,
      repoRoot,
    });

    expect(result.content).toContain("greet_v1");
    expect(result.content).not.toContain("greet_v2");
    expect(result.includedFiles).toEqual(["shared/functions.sql"]);
  });

  it("resolves \\ir include relative to script directory", () => {
    commitFile(
      repoRoot,
      "deploy/helpers.sql",
      "CREATE FUNCTION helper() RETURNS void AS $$ $$ LANGUAGE sql;\n",
    );
    commitFile(
      repoRoot,
      "deploy/001-init.sql",
      "-- migration\n\\ir helpers.sql\nSELECT 1;\n",
    );

    const result = resolveIncludes("deploy/001-init.sql", {
      repoRoot,
    });

    expect(result.content).toContain("helper()");
    expect(result.includedFiles).toEqual(["deploy/helpers.sql"]);
  });

  it("resolves nested includes", () => {
    commitFile(repoRoot, "shared/types.sql", "CREATE TYPE mood AS ENUM ('happy', 'sad');\n");
    commitFile(
      repoRoot,
      "shared/functions.sql",
      "\\i shared/types.sql\nCREATE FUNCTION get_mood() RETURNS mood AS $$ SELECT 'happy'::mood $$ LANGUAGE sql;\n",
    );
    commitFile(
      repoRoot,
      "deploy/001-init.sql",
      "-- migration\n\\i shared/functions.sql\nSELECT 1;\n",
    );

    const result = resolveIncludes("deploy/001-init.sql", {
      repoRoot,
    });

    expect(result.content).toContain("CREATE TYPE mood");
    expect(result.content).toContain("get_mood");
    expect(result.includedFiles).toContain("shared/functions.sql");
    expect(result.includedFiles).toContain("shared/types.sql");
  });

  it("detects circular includes", () => {
    commitFile(repoRoot, "a.sql", "\\i b.sql\n");
    commitFile(repoRoot, "b.sql", "\\i a.sql\n");

    expect(() =>
      resolveIncludes("a.sql", { repoRoot }),
    ).toThrow(/[Cc]ircular include/);
  });

  it("throws on missing include file", () => {
    commitFile(
      repoRoot,
      "deploy/001.sql",
      "\\i nonexistent/missing.sql\nSELECT 1;\n",
    );

    expect(() =>
      resolveIncludes("deploy/001.sql", { repoRoot }),
    ).toThrow(/not found.*nonexistent\/missing\.sql/i);
  });

  it("enforces max depth", () => {
    // Create a chain that exceeds depth 3
    commitFile(repoRoot, "d4.sql", "SELECT 'deep';\n");
    commitFile(repoRoot, "d3.sql", "\\i d4.sql\n");
    commitFile(repoRoot, "d2.sql", "\\i d3.sql\n");
    commitFile(repoRoot, "d1.sql", "\\i d2.sql\n");
    commitFile(repoRoot, "d0.sql", "\\i d1.sql\n");

    expect(() =>
      resolveIncludes("d0.sql", { repoRoot, maxDepth: 3 }),
    ).toThrow(/depth exceeded/i);
  });

  it("handles scripts with no includes (pass-through)", () => {
    commitFile(
      repoRoot,
      "deploy/simple.sql",
      "CREATE TABLE users (id int);\n",
    );

    const result = resolveIncludes("deploy/simple.sql", { repoRoot });
    expect(result.content).toBe("CREATE TABLE users (id int);\n");
    expect(result.includedFiles).toHaveLength(0);
  });

  it("preserves non-include content around includes", () => {
    commitFile(repoRoot, "shared/funcs.sql", "-- functions\n");
    commitFile(
      repoRoot,
      "deploy/001.sql",
      "BEGIN;\n\\i shared/funcs.sql\nCOMMIT;\n",
    );

    const result = resolveIncludes("deploy/001.sql", { repoRoot });

    expect(result.content).toContain("BEGIN;");
    expect(result.content).toContain("COMMIT;");
    expect(result.content).toContain("-- functions");
  });

  it("adds diagnostic comments around included content", () => {
    commitFile(repoRoot, "shared/funcs.sql", "SELECT 1;\n");
    commitFile(
      repoRoot,
      "deploy/001.sql",
      "\\i shared/funcs.sql\n",
    );

    const result = resolveIncludes("deploy/001.sql", { repoRoot });

    expect(result.content).toContain(
      "-- [snapshot] begin include: shared/funcs.sql",
    );
    expect(result.content).toContain(
      "-- [snapshot] end include: shared/funcs.sql",
    );
  });

  it("resolves multiple includes in one script", () => {
    commitFile(repoRoot, "shared/types.sql", "-- types\n");
    commitFile(repoRoot, "shared/funcs.sql", "-- functions\n");
    commitFile(repoRoot, "shared/views.sql", "-- views\n");
    commitFile(
      repoRoot,
      "deploy/big.sql",
      [
        "-- migration with many includes",
        "\\i shared/types.sql",
        "\\i shared/funcs.sql",
        "\\i shared/views.sql",
        "SELECT 1;",
      ].join("\n"),
    );

    const result = resolveIncludes("deploy/big.sql", { repoRoot });

    expect(result.content).toContain("-- types");
    expect(result.content).toContain("-- functions");
    expect(result.content).toContain("-- views");
    expect(result.includedFiles).toHaveLength(3);
  });

  it("resolves historical version of nested include", () => {
    // v1: types has one definition
    const hash1 = commitFile(
      repoRoot,
      "shared/types.sql",
      "CREATE TYPE status_v1 AS ENUM ('active');\n",
    );
    commitFile(
      repoRoot,
      "shared/funcs.sql",
      "\\i shared/types.sql\n-- funcs v1\n",
    );
    commitFile(
      repoRoot,
      "deploy/001.sql",
      "\\i shared/funcs.sql\n",
    );

    // v2: types updated
    commitFile(
      repoRoot,
      "shared/types.sql",
      "CREATE TYPE status_v2 AS ENUM ('active', 'archived');\n",
    );

    // Resolve at v1 commit
    const result = resolveIncludes("deploy/001.sql", {
      commitHash: hash1,
      repoRoot,
    });

    expect(result.content).toContain("status_v1");
    expect(result.content).not.toContain("status_v2");
  });
});

// ---------------------------------------------------------------------------
// Tests: resolveDeployIncludes (high-level API)
// ---------------------------------------------------------------------------

describe("resolveDeployIncludes", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("returns undefined for scripts with no includes", () => {
    commitFile(
      repoRoot,
      "deploy/simple.sql",
      "CREATE TABLE users (id int);\n",
    );

    const result = resolveDeployIncludes(
      join(repoRoot, "deploy/simple.sql"),
      "2025-01-01T00:00:00Z",
      repoRoot,
    );

    expect(result).toBeUndefined();
  });

  it("resolves includes using explicit commit hash", () => {
    const hash1 = commitFile(
      repoRoot,
      "shared/funcs.sql",
      "-- v1 functions\n",
    );
    commitFile(
      repoRoot,
      "shared/funcs.sql",
      "-- v2 functions\n",
    );
    commitFile(
      repoRoot,
      "deploy/001.sql",
      "\\i shared/funcs.sql\nSELECT 1;\n",
    );

    const result = resolveDeployIncludes(
      join(repoRoot, "deploy/001.sql"),
      "2025-01-01T00:00:00Z",
      repoRoot,
      hash1,
    );

    expect(result).toBeDefined();
    expect(result!.content).toContain("v1 functions");
    expect(result!.content).not.toContain("v2 functions");
  });

  it("uses HEAD when --no-snapshot is set", () => {
    commitFile(
      repoRoot,
      "shared/funcs.sql",
      "-- old functions\n",
    );
    commitFile(
      repoRoot,
      "shared/funcs.sql",
      "-- current functions\n",
    );
    commitFile(
      repoRoot,
      "deploy/001.sql",
      "\\i shared/funcs.sql\n",
    );

    const result = resolveDeployIncludes(
      join(repoRoot, "deploy/001.sql"),
      "2025-01-01T00:00:00Z",
      repoRoot,
      undefined,
      true, // noSnapshot
    );

    expect(result).toBeDefined();
    expect(result!.content).toContain("current functions");
  });

  it("falls back to working tree when not a git repo", () => {
    const nonRepo = makeTempDir();
    try {
      mkdirSync(join(nonRepo, "shared"), { recursive: true });
      mkdirSync(join(nonRepo, "deploy"), { recursive: true });
      writeFileSync(
        join(nonRepo, "shared/funcs.sql"),
        "-- working tree version\n",
      );
      writeFileSync(
        join(nonRepo, "deploy/001.sql"),
        "\\i shared/funcs.sql\n",
      );

      const result = resolveDeployIncludes(
        join(nonRepo, "deploy/001.sql"),
        "2025-01-01T00:00:00Z",
        nonRepo,
      );

      expect(result).toBeDefined();
      expect(result!.content).toContain("working tree version");
    } finally {
      cleanupDir(nonRepo);
    }
  });

  it("resolves includes using planned_at timestamp", () => {
    commitFile(
      repoRoot,
      "shared/funcs.sql",
      "-- committed version\n",
    );
    commitFile(
      repoRoot,
      "deploy/001.sql",
      "\\i shared/funcs.sql\n",
    );

    // Use a far-future timestamp to get the latest commit
    const result = resolveDeployIncludes(
      join(repoRoot, "deploy/001.sql"),
      "2099-12-31T23:59:59Z",
      repoRoot,
    );

    expect(result).toBeDefined();
    expect(result!.content).toContain("committed version");
  });
});

// ---------------------------------------------------------------------------
// Tests: edge cases and advanced scenarios
// ---------------------------------------------------------------------------

describe("edge cases", () => {
  let repoRoot: string;

  beforeEach(() => {
    repoRoot = initGitRepo();
  });

  afterEach(() => {
    cleanupDir(repoRoot);
  });

  it("handles deeply nested directory includes", () => {
    commitFile(
      repoRoot,
      "lib/deep/nested/helper.sql",
      "-- deeply nested helper\n",
    );
    commitFile(
      repoRoot,
      "deploy/001.sql",
      "\\i lib/deep/nested/helper.sql\n",
    );

    const result = resolveIncludes("deploy/001.sql", { repoRoot });

    expect(result.content).toContain("deeply nested helper");
  });

  it("handles \\ir with nested relative paths", () => {
    commitFile(
      repoRoot,
      "shared/common/base.sql",
      "-- base definitions\n",
    );
    commitFile(
      repoRoot,
      "shared/functions.sql",
      "\\ir common/base.sql\n-- functions using base\n",
    );
    commitFile(
      repoRoot,
      "deploy/001.sql",
      "\\i shared/functions.sql\n",
    );

    const result = resolveIncludes("deploy/001.sql", { repoRoot });

    expect(result.content).toContain("base definitions");
    expect(result.content).toContain("functions using base");
    expect(result.includedFiles).toContain("shared/functions.sql");
    expect(result.includedFiles).toContain("shared/common/base.sql");
  });

  it("handles a script including the same file via different paths", () => {
    commitFile(repoRoot, "shared/types.sql", "-- types\n");
    commitFile(
      repoRoot,
      "deploy/001.sql",
      [
        "\\i shared/types.sql",
        "\\i shared/types.sql",
      ].join("\n"),
    );

    // This should succeed (same file included twice is not circular)
    const result = resolveIncludes("deploy/001.sql", { repoRoot });

    // The content should appear twice
    const occurrences = result.content.split("-- types").length - 1;
    expect(occurrences).toBe(2);
  });

  it("handles empty included files", () => {
    commitFile(repoRoot, "shared/empty.sql", "");
    commitFile(
      repoRoot,
      "deploy/001.sql",
      "\\i shared/empty.sql\nSELECT 1;\n",
    );

    const result = resolveIncludes("deploy/001.sql", { repoRoot });

    expect(result.content).toContain("SELECT 1;");
    expect(result.includedFiles).toEqual(["shared/empty.sql"]);
  });

  it("handles files with mixed line endings", () => {
    commitFile(
      repoRoot,
      "shared/crlf.sql",
      "-- line 1\r\n-- line 2\r\n",
    );
    commitFile(
      repoRoot,
      "deploy/001.sql",
      "\\i shared/crlf.sql\nSELECT 1;\n",
    );

    const result = resolveIncludes("deploy/001.sql", { repoRoot });
    expect(result.content).toContain("-- line 1");
    expect(result.content).toContain("-- line 2");
  });

  it("self-include is detected as circular", () => {
    commitFile(repoRoot, "self.sql", "\\i self.sql\n");

    expect(() =>
      resolveIncludes("self.sql", { repoRoot }),
    ).toThrow(/[Cc]ircular include/);
  });

  it("diamond dependency (A->B, A->C, B->D, C->D) resolves without error", () => {
    commitFile(repoRoot, "d.sql", "-- D\n");
    commitFile(repoRoot, "c.sql", "\\i d.sql\n-- C\n");
    commitFile(repoRoot, "b.sql", "\\i d.sql\n-- B\n");
    commitFile(
      repoRoot,
      "a.sql",
      "\\i b.sql\n\\i c.sql\n-- A\n",
    );

    // Diamond deps should resolve fine (D included twice, not circular)
    const result = resolveIncludes("a.sql", { repoRoot });

    expect(result.content).toContain("-- A");
    expect(result.content).toContain("-- B");
    expect(result.content).toContain("-- C");
    // D appears twice (once via B, once via C)
    const dCount = result.content.split("-- D").length - 1;
    expect(dCount).toBe(2);
  });
});
