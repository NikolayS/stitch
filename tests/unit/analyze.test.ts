/**
 * Tests for src/commands/analyze.ts — sqlever analyze command.
 *
 * Covers argument parsing, single-file analysis, directory analysis,
 * sqitch.plan-based discovery, format outputs, --strict, --force-rule,
 * exit codes, error handling, and CLI wiring.
 */

import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { loadModule } from "libpg-query";
import {
  parseAnalyzeArgs,
  runAnalyze,
} from "../../src/commands/analyze";
import { join } from "node:path";
import {
  mkdirSync,
  writeFileSync,
  rmSync,
  existsSync,
} from "node:fs";

// Ensure WASM module is loaded before tests
beforeAll(async () => {
  await loadModule();
});

const TMP_DIR = join(import.meta.dir, "..", ".tmp-analyze-tests");

// Create temp directory structure for tests
beforeAll(() => {
  if (existsSync(TMP_DIR)) {
    rmSync(TMP_DIR, { recursive: true });
  }
  mkdirSync(TMP_DIR, { recursive: true });
  mkdirSync(join(TMP_DIR, "deploy"), { recursive: true });
  mkdirSync(join(TMP_DIR, "empty-dir"), { recursive: true });

  // A clean SQL file (no findings expected from most rules)
  writeFileSync(
    join(TMP_DIR, "deploy", "clean.sql"),
    "CREATE TABLE t (id serial PRIMARY KEY);\n",
  );

  // A SQL file that triggers SA004 (CREATE INDEX without CONCURRENTLY)
  writeFileSync(
    join(TMP_DIR, "deploy", "index_issue.sql"),
    "CREATE INDEX idx_t_id ON t (id);\n",
  );

  // A file with a parse error
  writeFileSync(
    join(TMP_DIR, "deploy", "broken.sql"),
    "CREATE TABL oops;\n",
  );

  // A SQL file that triggers SA010 (UPDATE/DELETE without WHERE)
  writeFileSync(
    join(TMP_DIR, "deploy", "no_where.sql"),
    "UPDATE t SET x = 1;\n",
  );

  // A sqitch.plan file
  writeFileSync(
    join(TMP_DIR, "sqitch.plan"),
    `%project=test
%uri=https://example.com

clean 2024-01-15T10:30:00Z dev <dev@example.com> # clean migration
index_issue 2024-01-15T10:31:00Z dev <dev@example.com> # index issue
no_where 2024-01-15T10:32:00Z dev <dev@example.com> # no where
`,
  );

  // Non-sql file (should be ignored by directory scan)
  writeFileSync(join(TMP_DIR, "deploy", "readme.txt"), "not sql");
});

afterAll(() => {
  if (existsSync(TMP_DIR)) {
    rmSync(TMP_DIR, { recursive: true });
  }
});

// Suppress stdout during test runs
function silenceStdout(): () => void {
  const original = process.stdout.write.bind(process.stdout);
  process.stdout.write = (() => true) as typeof process.stdout.write;
  return () => {
    process.stdout.write = original;
  };
}

// Capture stdout during test runs
function captureStdout(): { getOutput: () => string; restore: () => void } {
  let output = "";
  const original = process.stdout.write.bind(process.stdout);
  process.stdout.write = ((chunk: unknown) => {
    output += typeof chunk === "string" ? chunk : String(chunk);
    return true;
  }) as typeof process.stdout.write;
  return {
    getOutput: () => output,
    restore: () => {
      process.stdout.write = original;
    },
  };
}

// ---------------------------------------------------------------------------
// parseAnalyzeArgs
// ---------------------------------------------------------------------------

describe("parseAnalyzeArgs", () => {
  test("parses empty args with defaults", () => {
    const opts = parseAnalyzeArgs([]);
    expect(opts.targets).toEqual([]);
    expect(opts.format).toBe("text");
    expect(opts.strict).toBe(false);
    expect(opts.all).toBe(false);
    expect(opts.changed).toBe(false);
    expect(opts.forceRules).toEqual([]);
  });

  test("parses positional file targets", () => {
    const opts = parseAnalyzeArgs(["file1.sql", "dir/", "file2.sql"]);
    expect(opts.targets).toEqual(["file1.sql", "dir/", "file2.sql"]);
  });

  test("parses --format json", () => {
    const opts = parseAnalyzeArgs(["--format", "json"]);
    expect(opts.format).toBe("json");
  });

  test("parses --format github-annotations", () => {
    const opts = parseAnalyzeArgs(["--format", "github-annotations"]);
    expect(opts.format).toBe("github-annotations");
  });

  test("parses --format gitlab-codequality", () => {
    const opts = parseAnalyzeArgs(["--format", "gitlab-codequality"]);
    expect(opts.format).toBe("gitlab-codequality");
  });

  test("throws on invalid --format value", () => {
    expect(() => parseAnalyzeArgs(["--format", "xml"])).toThrow(
      "Invalid --format",
    );
  });

  test("parses --strict flag", () => {
    const opts = parseAnalyzeArgs(["--strict"]);
    expect(opts.strict).toBe(true);
  });

  test("parses --all flag", () => {
    const opts = parseAnalyzeArgs(["--all"]);
    expect(opts.all).toBe(true);
  });

  test("parses --changed flag", () => {
    const opts = parseAnalyzeArgs(["--changed"]);
    expect(opts.changed).toBe(true);
  });

  test("parses single --force-rule", () => {
    const opts = parseAnalyzeArgs(["--force-rule", "SA003"]);
    expect(opts.forceRules).toEqual(["SA003"]);
  });

  test("parses multiple --force-rule flags", () => {
    const opts = parseAnalyzeArgs([
      "--force-rule",
      "SA003",
      "--force-rule",
      "SA004",
    ]);
    expect(opts.forceRules).toEqual(["SA003", "SA004"]);
  });

  test("throws when --force-rule has no argument", () => {
    expect(() => parseAnalyzeArgs(["--force-rule"])).toThrow(
      "--force-rule requires a rule ID",
    );
  });

  test("parses combined flags and targets", () => {
    const opts = parseAnalyzeArgs([
      "file.sql",
      "--strict",
      "--format",
      "json",
      "--force-rule",
      "SA001",
    ]);
    expect(opts.targets).toEqual(["file.sql"]);
    expect(opts.strict).toBe(true);
    expect(opts.format).toBe("json");
    expect(opts.forceRules).toEqual(["SA001"]);
  });
});

// ---------------------------------------------------------------------------
// runAnalyze — single file
// ---------------------------------------------------------------------------

describe("runAnalyze — single file", () => {
  test("analyzes a clean SQL file with exit code 0", async () => {
    const restore = silenceStdout();
    try {
      const result = await runAnalyze({
        targets: [join(TMP_DIR, "deploy", "clean.sql")],
        format: "text",
        strict: false,
        all: false,
        changed: false,
        forceRules: [],
      });
      expect(result.exitCode).toBe(0);
      expect(result.filesAnalyzed).toBe(1);
    } finally {
      restore();
    }
  });

  test("detects SA010 warnings for UPDATE without WHERE", async () => {
    const restore = silenceStdout();
    try {
      const result = await runAnalyze({
        targets: [join(TMP_DIR, "deploy", "no_where.sql")],
        format: "text",
        strict: false,
        all: false,
        changed: false,
        forceRules: [],
      });
      const sa010 = result.findings.filter((f) => f.ruleId === "SA010");
      expect(sa010.length).toBeGreaterThan(0);
      // SA010 is severity "warn", not "error", so exit code 0 without --strict
      expect(result.exitCode).toBe(0);
    } finally {
      restore();
    }
  });

  test("returns parse error finding for invalid SQL", async () => {
    const restore = silenceStdout();
    try {
      const result = await runAnalyze({
        targets: [join(TMP_DIR, "deploy", "broken.sql")],
        format: "text",
        strict: false,
        all: false,
        changed: false,
        forceRules: [],
      });
      const parseErrors = result.findings.filter(
        (f) => f.ruleId === "parse-error",
      );
      expect(parseErrors.length).toBe(1);
      expect(result.exitCode).toBe(2);
    } finally {
      restore();
    }
  });
});

// ---------------------------------------------------------------------------
// runAnalyze — directory
// ---------------------------------------------------------------------------

describe("runAnalyze — directory", () => {
  test("analyzes all .sql files in a directory", async () => {
    const restore = silenceStdout();
    try {
      const result = await runAnalyze({
        targets: [join(TMP_DIR, "deploy")],
        format: "text",
        strict: false,
        all: false,
        changed: false,
        forceRules: [],
      });
      // Should analyze clean.sql, index_issue.sql, broken.sql, no_where.sql (4 files)
      expect(result.filesAnalyzed).toBe(4);
    } finally {
      restore();
    }
  });

  test("returns exit code 0 for empty directory", async () => {
    const restore = silenceStdout();
    try {
      const result = await runAnalyze({
        targets: [join(TMP_DIR, "empty-dir")],
        format: "text",
        strict: false,
        all: false,
        changed: false,
        forceRules: [],
      });
      expect(result.filesAnalyzed).toBe(0);
      expect(result.exitCode).toBe(0);
    } finally {
      restore();
    }
  });
});

// ---------------------------------------------------------------------------
// runAnalyze — sqitch.plan
// ---------------------------------------------------------------------------

describe("runAnalyze — sqitch.plan", () => {
  test("analyzes changes from sqitch.plan when no targets given", async () => {
    const restore = silenceStdout();
    try {
      const result = await runAnalyze({
        targets: [],
        format: "text",
        strict: false,
        all: false,
        changed: false,
        forceRules: [],
        topDir: TMP_DIR,
        planFile: join(TMP_DIR, "sqitch.plan"),
      });
      // Plan has 3 changes: clean, index_issue, no_where
      expect(result.filesAnalyzed).toBe(3);
    } finally {
      restore();
    }
  });

  test("analyzes all changes with --all flag", async () => {
    const restore = silenceStdout();
    try {
      const result = await runAnalyze({
        targets: [],
        format: "text",
        strict: false,
        all: true,
        changed: false,
        forceRules: [],
        topDir: TMP_DIR,
        planFile: join(TMP_DIR, "sqitch.plan"),
      });
      expect(result.filesAnalyzed).toBe(3);
    } finally {
      restore();
    }
  });

  test("returns 0 files when no sqitch.plan exists and no targets", async () => {
    const restore = silenceStdout();
    try {
      const result = await runAnalyze({
        targets: [],
        format: "text",
        strict: false,
        all: false,
        changed: false,
        forceRules: [],
        topDir: join(TMP_DIR, "empty-dir"),
      });
      expect(result.filesAnalyzed).toBe(0);
      expect(result.exitCode).toBe(0);
    } finally {
      restore();
    }
  });
});

// ---------------------------------------------------------------------------
// --strict
// ---------------------------------------------------------------------------

describe("--strict", () => {
  test("treats warnings as errors for exit code", async () => {
    const restore = silenceStdout();
    try {
      // index_issue.sql triggers SA004 (warn) — CREATE INDEX without CONCURRENTLY
      const result = await runAnalyze({
        targets: [join(TMP_DIR, "deploy", "index_issue.sql")],
        format: "text",
        strict: true,
        all: false,
        changed: false,
        forceRules: [],
      });
      const hasWarningsOrErrors = result.findings.some(
        (f) => f.severity === "warn" || f.severity === "error",
      );
      if (hasWarningsOrErrors) {
        expect(result.exitCode).toBe(2);
      }
    } finally {
      restore();
    }
  });

  test("exit code 0 when clean file with --strict", async () => {
    const restore = silenceStdout();
    try {
      const result = await runAnalyze({
        targets: [join(TMP_DIR, "deploy", "clean.sql")],
        format: "text",
        strict: true,
        all: false,
        changed: false,
        forceRules: [],
      });
      expect(result.exitCode).toBe(0);
    } finally {
      restore();
    }
  });
});

// ---------------------------------------------------------------------------
// --force-rule
// ---------------------------------------------------------------------------

describe("--force-rule", () => {
  test("bypasses specified rule", async () => {
    const restore = silenceStdout();
    try {
      const result = await runAnalyze({
        targets: [join(TMP_DIR, "deploy", "no_where.sql")],
        format: "text",
        strict: false,
        all: false,
        changed: false,
        forceRules: ["SA010"],
      });
      const sa010 = result.findings.filter((f) => f.ruleId === "SA010");
      expect(sa010.length).toBe(0);
    } finally {
      restore();
    }
  });

  test("bypasses multiple rules", async () => {
    const restore = silenceStdout();
    try {
      const result = await runAnalyze({
        targets: [join(TMP_DIR, "deploy", "no_where.sql")],
        format: "text",
        strict: false,
        all: false,
        changed: false,
        forceRules: ["SA010", "SA004"],
      });
      const sa010 = result.findings.filter((f) => f.ruleId === "SA010");
      const sa004 = result.findings.filter((f) => f.ruleId === "SA004");
      expect(sa010.length).toBe(0);
      expect(sa004.length).toBe(0);
    } finally {
      restore();
    }
  });
});

// ---------------------------------------------------------------------------
// --format
// ---------------------------------------------------------------------------

describe("--format", () => {
  test("json format outputs valid JSON with metadata", async () => {
    const cap = captureStdout();
    try {
      await runAnalyze({
        targets: [join(TMP_DIR, "deploy", "clean.sql")],
        format: "json",
        strict: false,
        all: false,
        changed: false,
        forceRules: [],
      });
      const parsed = JSON.parse(cap.getOutput());
      expect(parsed.version).toBe(1);
      expect(parsed.metadata.files_analyzed).toBe(1);
      expect(Array.isArray(parsed.findings)).toBe(true);
      expect(parsed.summary).toBeDefined();
    } finally {
      cap.restore();
    }
  });

  test("github-annotations format outputs ::annotation lines", async () => {
    const cap = captureStdout();
    try {
      await runAnalyze({
        targets: [join(TMP_DIR, "deploy", "no_where.sql")],
        format: "github-annotations",
        strict: false,
        all: false,
        changed: false,
        forceRules: [],
      });
      const output = cap.getOutput();
      expect(output).toContain("::");
      expect(output).toContain("SA010");
    } finally {
      cap.restore();
    }
  });

  test("gitlab-codequality format outputs valid JSON array", async () => {
    const cap = captureStdout();
    try {
      await runAnalyze({
        targets: [join(TMP_DIR, "deploy", "no_where.sql")],
        format: "gitlab-codequality",
        strict: false,
        all: false,
        changed: false,
        forceRules: [],
      });
      const parsed = JSON.parse(cap.getOutput());
      expect(Array.isArray(parsed)).toBe(true);
      expect(parsed.length).toBeGreaterThan(0);
      expect(parsed[0].check_name).toBeDefined();
      expect(parsed[0].fingerprint).toBeDefined();
    } finally {
      cap.restore();
    }
  });

  test("text format includes human-readable output", async () => {
    const cap = captureStdout();
    try {
      await runAnalyze({
        targets: [join(TMP_DIR, "deploy", "clean.sql")],
        format: "text",
        strict: false,
        all: false,
        changed: false,
        forceRules: [],
      });
      expect(cap.getOutput()).toContain("No issues found.");
    } finally {
      cap.restore();
    }
  });
});

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

describe("error handling", () => {
  test("throws on nonexistent file path", async () => {
    const restore = silenceStdout();
    try {
      await expect(
        runAnalyze({
          targets: [join(TMP_DIR, "nonexistent.sql")],
          format: "text",
          strict: false,
          all: false,
          changed: false,
          forceRules: [],
        }),
      ).rejects.toThrow("Path not found");
    } finally {
      restore();
    }
  });

  test("throws on nonexistent plan file via --plan-file", async () => {
    const restore = silenceStdout();
    try {
      await expect(
        runAnalyze({
          targets: [],
          format: "text",
          strict: false,
          all: false,
          changed: false,
          forceRules: [],
          planFile: join(TMP_DIR, "nonexistent.plan"),
        }),
      ).rejects.toThrow("Plan file not found");
    } finally {
      restore();
    }
  });
});

// ---------------------------------------------------------------------------
// Exit codes
// ---------------------------------------------------------------------------

describe("exit codes", () => {
  test("exit code 0 when no findings", async () => {
    const restore = silenceStdout();
    try {
      const result = await runAnalyze({
        targets: [join(TMP_DIR, "deploy", "clean.sql")],
        format: "text",
        strict: false,
        all: false,
        changed: false,
        forceRules: [],
      });
      expect(result.exitCode).toBe(0);
    } finally {
      restore();
    }
  });

  test("exit code 2 when error-level findings exist", async () => {
    const restore = silenceStdout();
    try {
      // broken.sql produces a parse-error finding with severity "error"
      const result = await runAnalyze({
        targets: [join(TMP_DIR, "deploy", "broken.sql")],
        format: "text",
        strict: false,
        all: false,
        changed: false,
        forceRules: [],
      });
      expect(result.exitCode).toBe(2);
    } finally {
      restore();
    }
  });

  test("exit code 0 when no files to analyze", async () => {
    const restore = silenceStdout();
    try {
      const result = await runAnalyze({
        targets: [],
        format: "text",
        strict: false,
        all: false,
        changed: false,
        forceRules: [],
        topDir: join(TMP_DIR, "empty-dir"),
      });
      expect(result.exitCode).toBe(0);
      expect(result.filesAnalyzed).toBe(0);
    } finally {
      restore();
    }
  });
});

// ---------------------------------------------------------------------------
// CLI wiring (subprocess)
// ---------------------------------------------------------------------------

describe("CLI wiring", () => {
  const CLI_PATH = join(import.meta.dir, "..", "..", "src", "cli.ts");

  test("sqlever analyze file.sql runs successfully", async () => {
    const cleanFile = join(TMP_DIR, "deploy", "clean.sql");
    const proc = Bun.spawn(["bun", "run", CLI_PATH, "analyze", cleanFile], {
      stdout: "pipe",
      stderr: "pipe",
    });
    const exitCode = await proc.exited;
    expect(exitCode).toBe(0);
  });

  test("sqlever analyze exits 2 for files with parse errors", async () => {
    const badFile = join(TMP_DIR, "deploy", "broken.sql");
    const proc = Bun.spawn(["bun", "run", CLI_PATH, "analyze", badFile], {
      stdout: "pipe",
      stderr: "pipe",
    });
    const exitCode = await proc.exited;
    expect(exitCode).toBe(2);
  });

  test("sqlever analyze --format json outputs valid JSON", async () => {
    const cleanFile = join(TMP_DIR, "deploy", "clean.sql");
    const proc = Bun.spawn(
      ["bun", "run", CLI_PATH, "analyze", "--format", "json", cleanFile],
      {
        stdout: "pipe",
        stderr: "pipe",
      },
    );
    const stdout = await new Response(proc.stdout).text();
    await proc.exited;
    const parsed = JSON.parse(stdout);
    expect(parsed.version).toBe(1);
  });
});
