import { describe, test, expect } from "bun:test";

describe("sqlever CLI", () => {
  test("--help prints usage information", async () => {
    const proc = Bun.spawn(["bun", "run", "src/cli.ts", "--help"], {
      cwd: import.meta.dir + "/../..",
      stdout: "pipe",
      stderr: "pipe",
    });
    const stdout = await new Response(proc.stdout).text();
    const exitCode = await proc.exited;

    expect(exitCode).toBe(0);
    expect(stdout).toContain("sqlever");
    expect(stdout).toContain("Usage:");
    expect(stdout).toContain("Commands:");
    expect(stdout).toContain("deploy");
  });

  test("-h prints usage information", async () => {
    const proc = Bun.spawn(["bun", "run", "src/cli.ts", "-h"], {
      cwd: import.meta.dir + "/../..",
      stdout: "pipe",
      stderr: "pipe",
    });
    const stdout = await new Response(proc.stdout).text();
    const exitCode = await proc.exited;

    expect(exitCode).toBe(0);
    expect(stdout).toContain("Usage:");
  });

  test("no arguments prints help", async () => {
    const proc = Bun.spawn(["bun", "run", "src/cli.ts"], {
      cwd: import.meta.dir + "/../..",
      stdout: "pipe",
      stderr: "pipe",
    });
    const stdout = await new Response(proc.stdout).text();
    const exitCode = await proc.exited;

    expect(exitCode).toBe(0);
    expect(stdout).toContain("Usage:");
  });

  test("--version prints version from package.json", async () => {
    const proc = Bun.spawn(["bun", "run", "src/cli.ts", "--version"], {
      cwd: import.meta.dir + "/../..",
      stdout: "pipe",
      stderr: "pipe",
    });
    const stdout = await new Response(proc.stdout).text();
    const exitCode = await proc.exited;

    expect(exitCode).toBe(0);
    expect(stdout.trim()).toBe("0.1.0");
  });

  test("-V prints version", async () => {
    const proc = Bun.spawn(["bun", "run", "src/cli.ts", "-V"], {
      cwd: import.meta.dir + "/../..",
      stdout: "pipe",
      stderr: "pipe",
    });
    const stdout = await new Response(proc.stdout).text();
    const exitCode = await proc.exited;

    expect(exitCode).toBe(0);
    expect(stdout.trim()).toBe("0.1.0");
  });

  test("unknown command exits with code 1", async () => {
    const proc = Bun.spawn(["bun", "run", "src/cli.ts", "nonexistent"], {
      cwd: import.meta.dir + "/../..",
      stdout: "pipe",
      stderr: "pipe",
    });
    const stderr = await new Response(proc.stderr).text();
    const exitCode = await proc.exited;

    expect(exitCode).toBe(1);
    expect(stderr).toContain("unknown command");
  });
});
