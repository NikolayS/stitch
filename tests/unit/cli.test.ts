import { describe, test, expect } from "bun:test";
import { parseArgs } from "../../src/cli";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const CWD = import.meta.dir + "/../..";

/** Spawn the CLI with the given arguments and capture output. */
async function run(
  ...args: string[]
): Promise<{ stdout: string; stderr: string; exitCode: number }> {
  const proc = Bun.spawn(["bun", "run", "src/cli.ts", ...args], {
    cwd: CWD,
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  return { stdout, stderr, exitCode };
}

// ---------------------------------------------------------------------------
// All R1 commands that should exist as stubs
// ---------------------------------------------------------------------------

const ALL_COMMANDS: string[] = [
  "init",
  "add",
  "deploy",
  "revert",
  "verify",
  "status",
  "log",
  "tag",
  "rework",
  "rebase",
  "bundle",
  "checkout",
  "show",
  "plan",
  "upgrade",
  "engine",
  "target",
  "config",
  "analyze",
  "explain",
  "review",
  "batch",
  "diff",
];

// ---------------------------------------------------------------------------
// Tests: parseArgs (unit, no subprocess)
// ---------------------------------------------------------------------------

describe("parseArgs", () => {
  test("empty argv yields no command", () => {
    const result = parseArgs([]);
    expect(result.command).toBeUndefined();
    expect(result.help).toBe(false);
    expect(result.version).toBe(false);
  });

  test("single command is parsed", () => {
    const result = parseArgs(["deploy"]);
    expect(result.command).toBe("deploy");
    expect(result.rest).toEqual([]);
  });

  test("command with positional args", () => {
    const result = parseArgs(["add", "my_change", "-n", "some note"]);
    expect(result.command).toBe("add");
    expect(result.rest).toEqual(["my_change", "-n", "some note"]);
  });

  test("--help flag", () => {
    expect(parseArgs(["--help"]).help).toBe(true);
    expect(parseArgs(["-h"]).help).toBe(true);
  });

  test("--version flag", () => {
    expect(parseArgs(["--version"]).version).toBe(true);
    expect(parseArgs(["-V"]).version).toBe(true);
  });

  test("--quiet flag", () => {
    expect(parseArgs(["--quiet"]).quiet).toBe(true);
    expect(parseArgs(["-q"]).quiet).toBe(true);
  });

  test("--verbose flag", () => {
    expect(parseArgs(["--verbose"]).verbose).toBe(true);
    expect(parseArgs(["-v"]).verbose).toBe(true);
  });

  test("--format json", () => {
    const result = parseArgs(["--format", "json", "status"]);
    expect(result.format).toBe("json");
    expect(result.command).toBe("status");
  });

  test("--format text", () => {
    const result = parseArgs(["--format", "text"]);
    expect(result.format).toBe("text");
  });

  test("--db-uri", () => {
    const result = parseArgs(["--db-uri", "postgresql://localhost/mydb"]);
    expect(result.dbUri).toBe("postgresql://localhost/mydb");
  });

  test("--plan-file", () => {
    const result = parseArgs(["--plan-file", "my.plan"]);
    expect(result.planFile).toBe("my.plan");
  });

  test("--top-dir", () => {
    const result = parseArgs(["--top-dir", "/some/dir"]);
    expect(result.topDir).toBe("/some/dir");
  });

  test("--registry", () => {
    const result = parseArgs(["--registry", "_sqitch"]);
    expect(result.registry).toBe("_sqitch");
  });

  test("--target", () => {
    const result = parseArgs(["--target", "production"]);
    expect(result.target).toBe("production");
  });

  test("flags before command", () => {
    const result = parseArgs(["--quiet", "--format", "json", "deploy"]);
    expect(result.quiet).toBe(true);
    expect(result.format).toBe("json");
    expect(result.command).toBe("deploy");
  });

  test("flags after command go into rest", () => {
    const result = parseArgs(["deploy", "--to", "my_change"]);
    expect(result.command).toBe("deploy");
    expect(result.rest).toEqual(["--to", "my_change"]);
  });

  test("--help with command", () => {
    const result = parseArgs(["deploy", "--help"]);
    // --help is extracted as a top-level flag; command is "deploy"
    // The "rest" array should not contain --help since it was consumed
    // Actually, --help after a command is still consumed as a flag
    // because we parse all --help/-h anywhere in argv
    expect(result.help).toBe(true);
    expect(result.command).toBe("deploy");
  });

  test("combined flags", () => {
    const result = parseArgs([
      "--quiet",
      "--verbose",
      "--format",
      "json",
      "--db-uri",
      "pg://localhost/db",
      "deploy",
    ]);
    expect(result.quiet).toBe(true);
    expect(result.verbose).toBe(true);
    expect(result.format).toBe("json");
    expect(result.dbUri).toBe("pg://localhost/db");
    expect(result.command).toBe("deploy");
  });
});

// ---------------------------------------------------------------------------
// Tests: --help / -h (subprocess)
// ---------------------------------------------------------------------------

describe("sqlever --help", () => {
  test("--help prints usage information and exits 0", async () => {
    const { stdout, exitCode } = await run("--help");
    expect(exitCode).toBe(0);
    expect(stdout).toContain("sqlever");
    expect(stdout).toContain("Usage:");
    expect(stdout).toContain("Commands:");
    // Verify all commands appear in help
    for (const cmd of ALL_COMMANDS) {
      expect(stdout).toContain(cmd);
    }
  });

  test("-h prints usage information and exits 0", async () => {
    const { stdout, exitCode } = await run("-h");
    expect(exitCode).toBe(0);
    expect(stdout).toContain("Usage:");
  });

  test("no arguments prints help and exits 0", async () => {
    const { stdout, exitCode } = await run();
    expect(exitCode).toBe(0);
    expect(stdout).toContain("Usage:");
  });

  test("--help with command shows command-specific help", async () => {
    const { stdout, exitCode } = await run("--help", "deploy");
    expect(exitCode).toBe(0);
    expect(stdout).toContain("sqlever deploy");
    expect(stdout).toContain("No detailed help available yet");
  });

  test("command --help shows command-specific help", async () => {
    const { stdout, exitCode } = await run("deploy", "--help");
    expect(exitCode).toBe(0);
    expect(stdout).toContain("sqlever deploy");
  });

  test("help command with no subcommand shows top-level help", async () => {
    const { stdout, exitCode } = await run("help");
    expect(exitCode).toBe(0);
    expect(stdout).toContain("Usage:");
    expect(stdout).toContain("Commands:");
  });

  test("help command with subcommand shows command help", async () => {
    const { stdout, exitCode } = await run("help", "analyze");
    expect(exitCode).toBe(0);
    expect(stdout).toContain("sqlever analyze");
    expect(stdout).toContain("No detailed help available yet");
  });

  test("help lists global options", async () => {
    const { stdout } = await run("--help");
    expect(stdout).toContain("--help");
    expect(stdout).toContain("--version");
    expect(stdout).toContain("--format");
    expect(stdout).toContain("--quiet");
    expect(stdout).toContain("--verbose");
    expect(stdout).toContain("--db-uri");
    expect(stdout).toContain("--plan-file");
    expect(stdout).toContain("--top-dir");
    expect(stdout).toContain("--registry");
    expect(stdout).toContain("--target");
  });
});

// ---------------------------------------------------------------------------
// Tests: --version / -V (subprocess)
// ---------------------------------------------------------------------------

describe("sqlever --version", () => {
  test("--version prints version from package.json and exits 0", async () => {
    const { stdout, exitCode } = await run("--version");
    expect(exitCode).toBe(0);
    expect(stdout.trim()).toBe("0.1.0");
  });

  test("-V prints version", async () => {
    const { stdout, exitCode } = await run("-V");
    expect(exitCode).toBe(0);
    expect(stdout.trim()).toBe("0.1.0");
  });

  test("--version takes precedence over --help", async () => {
    const { stdout, exitCode } = await run("--version", "--help");
    expect(exitCode).toBe(0);
    expect(stdout.trim()).toBe("0.1.0");
  });

  test("--version takes precedence over commands", async () => {
    const { stdout, exitCode } = await run("--version", "deploy");
    expect(exitCode).toBe(0);
    expect(stdout.trim()).toBe("0.1.0");
  });
});

// ---------------------------------------------------------------------------
// Tests: unknown command (subprocess)
// ---------------------------------------------------------------------------

describe("unknown commands", () => {
  test("unknown command exits with code 1", async () => {
    const { stderr, exitCode } = await run("nonexistent");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("unknown command");
    expect(stderr).toContain("nonexistent");
  });

  test("help for unknown command exits with code 1", async () => {
    const { stderr, exitCode } = await run("help", "nonexistent");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("unknown command");
  });

  test("--help for unknown command exits with code 1", async () => {
    const { stderr, exitCode } = await run("nonexistent", "--help");
    // nonexistent is parsed as the command, --help is a flag
    // --help with an unknown command should error
    expect(exitCode).toBe(1);
    expect(stderr).toContain("unknown command");
  });
});

// ---------------------------------------------------------------------------
// Tests: command stubs (subprocess) — every R1 command
// ---------------------------------------------------------------------------

describe("command stubs", () => {
  // "help" is handled specially (not a stub), "init", "add", and "log" are implemented — exclude them
  const STUB_COMMANDS = ALL_COMMANDS.filter((c) => c !== "help" && c !== "init" && c !== "add" && c !== "log");

  for (const cmd of STUB_COMMANDS) {
    test(`'${cmd}' prints not-yet-implemented and exits 1`, async () => {
      const { stderr, exitCode } = await run(cmd);
      expect(exitCode).toBe(1);
      expect(stderr).toContain(`sqlever ${cmd}: not yet implemented`);
    });
  }
});

// ---------------------------------------------------------------------------
// Tests: output module integration (subprocess)
// ---------------------------------------------------------------------------

describe("output module integration", () => {
  test("--quiet flag is accepted without error", async () => {
    // --quiet with a stub command should still exit 1 (stub) but not
    // crash due to flag parsing
    const { stderr, exitCode } = await run("--quiet", "status");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("not yet implemented");
  });

  test("--verbose flag is accepted without error", async () => {
    const { stderr, exitCode } = await run("--verbose", "status");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("not yet implemented");
  });

  test("--format json is accepted without error", async () => {
    const { stderr, exitCode } = await run("--format", "json", "status");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("not yet implemented");
  });

  test("--format invalid value exits 1 with error", async () => {
    const { stderr, exitCode } = await run("--format", "xml", "status");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("invalid --format");
  });
});

// ---------------------------------------------------------------------------
// Tests: global option flags (subprocess)
// ---------------------------------------------------------------------------

describe("global option flags", () => {
  test("--db-uri is accepted", async () => {
    const { stderr, exitCode } = await run(
      "--db-uri",
      "postgresql://localhost/mydb",
      "status",
    );
    expect(exitCode).toBe(1);
    expect(stderr).toContain("not yet implemented");
  });

  test("--plan-file is accepted", async () => {
    const { exitCode } = await run("--plan-file", "my.plan", "status");
    expect(exitCode).toBe(1);
  });

  test("--top-dir is accepted", async () => {
    const { exitCode } = await run("--top-dir", "/some/path", "status");
    expect(exitCode).toBe(1);
  });

  test("--registry is accepted", async () => {
    const { exitCode } = await run("--registry", "_sqitch", "status");
    expect(exitCode).toBe(1);
  });

  test("--target is accepted", async () => {
    const { exitCode } = await run("--target", "production", "status");
    expect(exitCode).toBe(1);
  });
});
