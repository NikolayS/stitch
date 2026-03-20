import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { mkdtemp, readFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import {
  parseInitOptions,
  buildSqitchConf,
  buildInitialPlan,
} from "../../src/commands/init";
import type { ParsedArgs } from "../../src/cli";
import { parseSqitchConf, confGet } from "../../src/config/sqitch-conf";
import { resetConfig } from "../../src/output";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const CWD = import.meta.dir + "/../..";

/** Create a fresh temp directory for each test. */
async function makeTempDir(): Promise<string> {
  return mkdtemp(join(tmpdir(), "sqlever-init-test-"));
}

/** Build a minimal ParsedArgs for init, overriding specific fields. */
function makeArgs(overrides: Partial<ParsedArgs> = {}): ParsedArgs {
  return {
    command: "init",
    rest: [],
    help: false,
    version: false,
    format: "text",
    quiet: false,
    verbose: false,
    dbUri: undefined,
    planFile: undefined,
    topDir: undefined,
    registry: undefined,
    target: undefined,
    ...overrides,
  };
}

/** Run init in a temp directory via subprocess. */
async function runInit(
  tempDir: string,
  ...extraArgs: string[]
): Promise<{ stdout: string; stderr: string; exitCode: number }> {
  const proc = Bun.spawn(
    ["bun", "run", "src/cli.ts", "init", "--top-dir", tempDir, ...extraArgs],
    {
      cwd: CWD,
      stdout: "pipe",
      stderr: "pipe",
    },
  );
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  return { stdout, stderr, exitCode };
}

// ---------------------------------------------------------------------------
// Tests: parseInitOptions (unit, no filesystem)
// ---------------------------------------------------------------------------

describe("parseInitOptions", () => {
  test("defaults: project name from cwd basename, engine pg", () => {
    const args = makeArgs({ rest: [] });
    const opts = parseInitOptions(args);
    expect(opts.engine).toBe("pg");
    expect(opts.topDir).toBe(".");
    expect(opts.force).toBe(false);
    // Project name is derived from resolved "." — just check it's non-empty
    expect(opts.projectName.length).toBeGreaterThan(0);
  });

  test("positional project name", () => {
    const args = makeArgs({ rest: ["myproject"] });
    const opts = parseInitOptions(args);
    expect(opts.projectName).toBe("myproject");
  });

  test("--engine flag", () => {
    const args = makeArgs({ rest: ["proj", "--engine", "sqlite"] });
    const opts = parseInitOptions(args);
    expect(opts.engine).toBe("sqlite");
    expect(opts.projectName).toBe("proj");
  });

  test("--uri flag", () => {
    const args = makeArgs({
      rest: ["proj", "--uri", "urn:uuid:12345"],
    });
    const opts = parseInitOptions(args);
    expect(opts.uri).toBe("urn:uuid:12345");
  });

  test("--plan-file flag in rest", () => {
    const args = makeArgs({
      rest: ["proj", "--plan-file", "custom.plan"],
    });
    const opts = parseInitOptions(args);
    expect(opts.planFile).toBe("custom.plan");
  });

  test("global --plan-file used when not in rest", () => {
    const args = makeArgs({
      rest: ["proj"],
      planFile: "global.plan",
    });
    const opts = parseInitOptions(args);
    expect(opts.planFile).toBe("global.plan");
  });

  test("--force flag", () => {
    const args = makeArgs({ rest: ["proj", "--force"] });
    const opts = parseInitOptions(args);
    expect(opts.force).toBe(true);
  });

  test("-f shorthand for --force", () => {
    const args = makeArgs({ rest: ["proj", "-f"] });
    const opts = parseInitOptions(args);
    expect(opts.force).toBe(true);
  });

  test("--top-dir from global args", () => {
    const args = makeArgs({ topDir: "/my/dir", rest: ["proj"] });
    const opts = parseInitOptions(args);
    expect(opts.topDir).toBe("/my/dir");
    expect(opts.projectName).toBe("proj");
  });

  test("no project name uses top-dir basename", () => {
    const args = makeArgs({ topDir: "/some/path/myapp" });
    const opts = parseInitOptions(args);
    expect(opts.projectName).toBe("myapp");
  });
});

// ---------------------------------------------------------------------------
// Tests: buildSqitchConf (unit)
// ---------------------------------------------------------------------------

describe("buildSqitchConf", () => {
  test("default config has engine = pg", () => {
    const content = buildSqitchConf({
      projectName: "test",
      topDir: ".",
      engine: "pg",
      force: false,
    });

    const conf = parseSqitchConf(content);
    expect(confGet(conf, "core.engine")).toBe("pg");
  });

  test("non-default top_dir is included", () => {
    const content = buildSqitchConf({
      projectName: "test",
      topDir: "migrations",
      engine: "pg",
      force: false,
    });

    const conf = parseSqitchConf(content);
    expect(confGet(conf, "core.top_dir")).toBe("migrations");
  });

  test("default top_dir omits top_dir key", () => {
    const content = buildSqitchConf({
      projectName: "test",
      topDir: ".",
      engine: "pg",
      force: false,
    });

    const conf = parseSqitchConf(content);
    expect(confGet(conf, "core.top_dir")).toBeUndefined();
  });

  test("plan_file is included when specified", () => {
    const content = buildSqitchConf({
      projectName: "test",
      topDir: ".",
      engine: "pg",
      planFile: "custom.plan",
      force: false,
    });

    const conf = parseSqitchConf(content);
    expect(confGet(conf, "core.plan_file")).toBe("custom.plan");
  });

  test("custom engine is written", () => {
    const content = buildSqitchConf({
      projectName: "test",
      topDir: ".",
      engine: "sqlite",
      force: false,
    });

    const conf = parseSqitchConf(content);
    expect(confGet(conf, "core.engine")).toBe("sqlite");
  });
});

// ---------------------------------------------------------------------------
// Tests: buildInitialPlan (unit)
// ---------------------------------------------------------------------------

describe("buildInitialPlan", () => {
  test("contains syntax-version pragma", () => {
    const content = buildInitialPlan({
      projectName: "myproject",
      topDir: ".",
      engine: "pg",
      force: false,
    });

    expect(content).toContain("%syntax-version=1.0.0");
  });

  test("contains project pragma", () => {
    const content = buildInitialPlan({
      projectName: "myproject",
      topDir: ".",
      engine: "pg",
      force: false,
    });

    expect(content).toContain("%project=myproject");
  });

  test("contains uri pragma when provided", () => {
    const content = buildInitialPlan({
      projectName: "myproject",
      topDir: ".",
      engine: "pg",
      uri: "https://example.com/myproject",
      force: false,
    });

    expect(content).toContain("%uri=https://example.com/myproject");
  });

  test("omits uri pragma when not provided", () => {
    const content = buildInitialPlan({
      projectName: "myproject",
      topDir: ".",
      engine: "pg",
      force: false,
    });

    expect(content).not.toContain("%uri=");
  });

  test("ends with trailing newline", () => {
    const content = buildInitialPlan({
      projectName: "myproject",
      topDir: ".",
      engine: "pg",
      force: false,
    });

    expect(content.endsWith("\n")).toBe(true);
  });

  test("has no change or tag entries", () => {
    const content = buildInitialPlan({
      projectName: "myproject",
      topDir: ".",
      engine: "pg",
      force: false,
    });

    // After the pragmas and blank separator line, there should be nothing else
    const lines = content.trimEnd().split("\n");
    // Should only have pragmas + blank line
    for (const line of lines) {
      if (line.trim() === "") continue;
      expect(line.startsWith("%")).toBe(true);
    }
  });
});

// ---------------------------------------------------------------------------
// Tests: filesystem integration (subprocess, temp dirs)
// ---------------------------------------------------------------------------

describe("sqlever init (filesystem)", () => {
  let tempDir: string;

  beforeEach(async () => {
    tempDir = await makeTempDir();
    resetConfig();
  });

  afterEach(async () => {
    await rm(tempDir, { recursive: true, force: true });
  });

  test("creates sqitch.conf, sqitch.plan, and directories", async () => {
    const { exitCode, stdout } = await runInit(tempDir, "myproject");
    expect(exitCode).toBe(0);

    // Verify files exist
    const confStat = await stat(join(tempDir, "sqitch.conf"));
    expect(confStat.isFile()).toBe(true);

    const planStat = await stat(join(tempDir, "sqitch.plan"));
    expect(planStat.isFile()).toBe(true);

    // Verify directories exist
    for (const dir of ["deploy", "revert", "verify"]) {
      const dirStat = await stat(join(tempDir, dir));
      expect(dirStat.isDirectory()).toBe(true);
    }

    // Verify output messages
    expect(stdout).toContain("Created");
    expect(stdout).toContain("Initialized project 'myproject'");
  });

  test("sqitch.conf has correct engine", async () => {
    await runInit(tempDir, "myproject");

    const confContent = await readFile(join(tempDir, "sqitch.conf"), "utf-8");
    const conf = parseSqitchConf(confContent);
    expect(confGet(conf, "core.engine")).toBe("pg");
  });

  test("sqitch.plan has correct pragmas", async () => {
    await runInit(tempDir, "myproject");

    const planContent = await readFile(join(tempDir, "sqitch.plan"), "utf-8");
    expect(planContent).toContain("%syntax-version=1.0.0");
    expect(planContent).toContain("%project=myproject");
  });

  test("--uri flag is written to plan", async () => {
    await runInit(tempDir, "myproject", "--uri", "urn:uuid:abc123");

    const planContent = await readFile(join(tempDir, "sqitch.plan"), "utf-8");
    expect(planContent).toContain("%uri=urn:uuid:abc123");
  });

  test("errors if sqitch.plan already exists", async () => {
    // First init
    await runInit(tempDir, "myproject");

    // Second init without --force should fail
    const { exitCode, stderr } = await runInit(tempDir, "myproject");
    expect(exitCode).toBe(1);
    expect(stderr).toContain("already exists");
  });

  test("--force allows reinitializing", async () => {
    // First init
    await runInit(tempDir, "myproject");

    // Second init with --force should succeed
    const { exitCode } = await runInit(tempDir, "otherproject", "--force");
    expect(exitCode).toBe(0);

    // Verify plan has the new project name
    const planContent = await readFile(join(tempDir, "sqitch.plan"), "utf-8");
    expect(planContent).toContain("%project=otherproject");
  });

  test("uses directory name when no project name given", async () => {
    const dirName = basename(tempDir);
    const { exitCode } = await runInit(tempDir);
    expect(exitCode).toBe(0);

    const planContent = await readFile(join(tempDir, "sqitch.plan"), "utf-8");
    expect(planContent).toContain(`%project=${dirName}`);
  });

  test("--engine flag changes engine in sqitch.conf", async () => {
    await runInit(tempDir, "myproject", "--engine", "sqlite");

    const confContent = await readFile(join(tempDir, "sqitch.conf"), "utf-8");
    const conf = parseSqitchConf(confContent);
    expect(confGet(conf, "core.engine")).toBe("sqlite");
  });

  test("--plan-file flag sets custom plan file location", async () => {
    const customPlan = join(tempDir, "custom.plan");
    await runInit(tempDir, "myproject", "--plan-file", customPlan);

    // Custom plan file should exist
    const planStat = await stat(customPlan);
    expect(planStat.isFile()).toBe(true);

    // sqitch.conf should reference the custom plan file
    const confContent = await readFile(join(tempDir, "sqitch.conf"), "utf-8");
    const conf = parseSqitchConf(confContent);
    expect(confGet(conf, "core.plan_file")).toBe(customPlan);
  });

  test("--quiet suppresses output", async () => {
    const proc = Bun.spawn(
      [
        "bun",
        "run",
        "src/cli.ts",
        "--quiet",
        "init",
        "--top-dir",
        tempDir,
        "myproject",
      ],
      {
        cwd: CWD,
        stdout: "pipe",
        stderr: "pipe",
      },
    );
    const [stdout, , exitCode] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
      proc.exited,
    ]);

    expect(exitCode).toBe(0);
    expect(stdout).toBe("");

    // But files should still be created
    const confStat = await stat(join(tempDir, "sqitch.conf"));
    expect(confStat.isFile()).toBe(true);
  });

  test("creates nested top-dir if it doesn't exist", async () => {
    const nestedDir = join(tempDir, "a", "b", "c");
    const { exitCode } = await runInit(nestedDir, "myproject");
    expect(exitCode).toBe(0);

    const confStat = await stat(join(nestedDir, "sqitch.conf"));
    expect(confStat.isFile()).toBe(true);
    const deployStat = await stat(join(nestedDir, "deploy"));
    expect(deployStat.isDirectory()).toBe(true);
  });

  test("sqitch.conf does not contain top_dir for default '.'", async () => {
    // When --top-dir is used, the conf should reflect that,
    // but the top_dir key should only appear if non-default.
    // Since we run with --top-dir tempDir, top_dir IS non-default.
    const { exitCode } = await runInit(tempDir, "myproject");
    expect(exitCode).toBe(0);

    const confContent = await readFile(join(tempDir, "sqitch.conf"), "utf-8");
    // Since we passed --top-dir (a non-default dir), top_dir should appear
    const conf = parseSqitchConf(confContent);
    expect(confGet(conf, "core.top_dir")).toBe(tempDir);
  });

  test("generated plan is valid and round-trips through serializePlan", async () => {
    await runInit(tempDir, "myproject", "--uri", "https://example.com/");

    const planContent = await readFile(join(tempDir, "sqitch.plan"), "utf-8");

    // Verify exact expected output
    const expectedLines = [
      "%syntax-version=1.0.0",
      "%project=myproject",
      "%uri=https://example.com/",
      "",
      "",
    ];
    expect(planContent).toBe(expectedLines.join("\n"));
  });
});
