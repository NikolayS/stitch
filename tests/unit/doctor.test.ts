// tests/unit/doctor.test.ts — Tests for the sqlever doctor command
//
// Validates runDoctorChecks() with various project configurations.

import { describe, expect, it, beforeEach, afterEach } from "bun:test";
import { mkdtempSync, writeFileSync, mkdirSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { runDoctorChecks, type DoctorReport } from "../../src/commands/doctor";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function createTempProject(): string {
  const dir = mkdtempSync(join(tmpdir(), "sqlever-doctor-test-"));
  return dir;
}

function writePlan(dir: string, content: string): void {
  writeFileSync(join(dir, "sqitch.plan"), content);
}

function writeConf(dir: string, content: string): void {
  writeFileSync(join(dir, "sqitch.conf"), content);
}

function writeScript(dir: string, subdir: string, name: string, content: string): void {
  const scriptDir = join(dir, subdir);
  mkdirSync(scriptDir, { recursive: true });
  writeFileSync(join(scriptDir, `${name}.sql`), content);
}

function minimalPlan(lines: string[] = []): string {
  return [
    "%syntax-version=1.0.0",
    "%project=testproject",
    "",
    ...lines,
  ].join("\n");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("doctor — plan file parsing", () => {
  let tempDir: string;

  beforeEach(() => {
    tempDir = createTempProject();
  });

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  it("reports error when plan file is missing", () => {
    const report = runDoctorChecks({ topDir: tempDir });
    const planCheck = report.checks.find((c) => c.check === "plan-file");
    expect(planCheck).toBeDefined();
    expect(planCheck!.severity).toBe("error");
    expect(planCheck!.message).toContain("not found");
    expect(report.summary.error).toBeGreaterThan(0);
  });

  it("reports ok when plan file parses successfully", () => {
    writePlan(tempDir, minimalPlan([
      "first_change 2024-01-15T10:30:00Z A <a@b.com> # first",
    ]));
    const report = runDoctorChecks({ topDir: tempDir });
    const planCheck = report.checks.find((c) => c.check === "plan-file");
    expect(planCheck).toBeDefined();
    expect(planCheck!.severity).toBe("ok");
    expect(planCheck!.message).toContain("1 change(s)");
  });

  it("reports error when plan file has parse errors", () => {
    writePlan(tempDir, "not a valid plan");
    const report = runDoctorChecks({ topDir: tempDir });
    const planCheck = report.checks.find((c) => c.check === "plan-file");
    expect(planCheck).toBeDefined();
    expect(planCheck!.severity).toBe("error");
    expect(planCheck!.message).toContain("parse error");
  });
});

describe("doctor — syntax version check", () => {
  let tempDir: string;

  beforeEach(() => {
    tempDir = createTempProject();
  });

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  it("reports ok for syntax-version 1.0.0", () => {
    writePlan(tempDir, minimalPlan());
    const report = runDoctorChecks({ topDir: tempDir });
    const check = report.checks.find((c) => c.check === "syntax-version");
    expect(check).toBeDefined();
    expect(check!.severity).toBe("ok");
  });

  it("reports warn when syntax-version is missing", () => {
    writePlan(tempDir, "%project=testproject\n");
    const report = runDoctorChecks({ topDir: tempDir });
    const check = report.checks.find((c) => c.check === "syntax-version");
    expect(check).toBeDefined();
    expect(check!.severity).toBe("warn");
    expect(check!.message).toContain("No %syntax-version");
  });
});

describe("doctor — change ID chain", () => {
  let tempDir: string;

  beforeEach(() => {
    tempDir = createTempProject();
  });

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  it("reports ok for a valid chain", () => {
    writePlan(tempDir, minimalPlan([
      "first 2024-01-15T10:30:00Z A <a@b.com> # 1",
      "second 2024-01-15T10:31:00Z A <a@b.com> # 2",
      "third 2024-01-15T10:32:00Z A <a@b.com> # 3",
    ]));
    const report = runDoctorChecks({ topDir: tempDir });
    const check = report.checks.find((c) => c.check === "change-id-chain");
    expect(check).toBeDefined();
    expect(check!.severity).toBe("ok");
    expect(check!.message).toContain("3 change(s)");
  });

  it("reports ok for empty plan (no changes)", () => {
    writePlan(tempDir, minimalPlan());
    const report = runDoctorChecks({ topDir: tempDir });
    const check = report.checks.find((c) => c.check === "change-id-chain");
    expect(check).toBeDefined();
    expect(check!.severity).toBe("ok");
  });
});

describe("doctor — script files", () => {
  let tempDir: string;

  beforeEach(() => {
    tempDir = createTempProject();
  });

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  it("reports error when deploy scripts are missing", () => {
    writePlan(tempDir, minimalPlan([
      "my_change 2024-01-15T10:30:00Z A <a@b.com> # change",
    ]));
    // Only create revert script, not deploy
    writeScript(tempDir, "revert", "my_change", "-- revert");
    const report = runDoctorChecks({ topDir: tempDir });
    const check = report.checks.find((c) => c.check === "script-files");
    expect(check).toBeDefined();
    expect(check!.severity).toBe("error");
    expect(check!.details).toBeDefined();
    expect(check!.details!.some((d) => d.includes("deploy"))).toBe(true);
  });

  it("reports warn when only verify scripts are missing", () => {
    writePlan(tempDir, minimalPlan([
      "my_change 2024-01-15T10:30:00Z A <a@b.com> # change",
    ]));
    writeScript(tempDir, "deploy", "my_change", "-- deploy");
    writeScript(tempDir, "revert", "my_change", "-- revert");
    const report = runDoctorChecks({ topDir: tempDir });
    const check = report.checks.find((c) => c.check === "script-files");
    expect(check).toBeDefined();
    expect(check!.severity).toBe("warn");
  });

  it("reports ok when all scripts exist", () => {
    writePlan(tempDir, minimalPlan([
      "my_change 2024-01-15T10:30:00Z A <a@b.com> # change",
    ]));
    writeScript(tempDir, "deploy", "my_change", "-- deploy");
    writeScript(tempDir, "revert", "my_change", "-- revert");
    writeScript(tempDir, "verify", "my_change", "-- verify");
    const report = runDoctorChecks({ topDir: tempDir });
    const check = report.checks.find((c) => c.check === "script-files");
    expect(check).toBeDefined();
    expect(check!.severity).toBe("ok");
  });
});

describe("doctor — psql metacommand detection", () => {
  let tempDir: string;

  beforeEach(() => {
    tempDir = createTempProject();
  });

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  it("reports warn when metacommands found in deploy scripts", () => {
    writePlan(tempDir, minimalPlan([
      "my_change 2024-01-15T10:30:00Z A <a@b.com> # change",
    ]));
    writeScript(tempDir, "deploy", "my_change", "\\set ON_ERROR_STOP on\nCREATE TABLE t();\n");
    writeScript(tempDir, "revert", "my_change", "DROP TABLE t;\n");
    const report = runDoctorChecks({ topDir: tempDir });
    const check = report.checks.find((c) => c.check === "psql-metacommands");
    expect(check).toBeDefined();
    expect(check!.severity).toBe("warn");
    expect(check!.details).toBeDefined();
    expect(check!.details!.some((d) => d.includes("deploy/my_change.sql"))).toBe(true);
  });

  it("reports ok when no metacommands found", () => {
    writePlan(tempDir, minimalPlan([
      "my_change 2024-01-15T10:30:00Z A <a@b.com> # change",
    ]));
    writeScript(tempDir, "deploy", "my_change", "CREATE TABLE t (id int);\n");
    writeScript(tempDir, "revert", "my_change", "DROP TABLE t;\n");
    const report = runDoctorChecks({ topDir: tempDir });
    const check = report.checks.find((c) => c.check === "psql-metacommands");
    expect(check).toBeDefined();
    expect(check!.severity).toBe("ok");
  });
});

describe("doctor — summary", () => {
  let tempDir: string;

  beforeEach(() => {
    tempDir = createTempProject();
  });

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true });
  });

  it("produces correct summary counts", () => {
    writePlan(tempDir, minimalPlan([
      "my_change 2024-01-15T10:30:00Z A <a@b.com> # change",
    ]));
    writeScript(tempDir, "deploy", "my_change", "CREATE TABLE t (id int);\n");
    writeScript(tempDir, "revert", "my_change", "DROP TABLE t;\n");
    writeScript(tempDir, "verify", "my_change", "SELECT 1;\n");

    const report = runDoctorChecks({ topDir: tempDir });
    expect(report.summary.ok + report.summary.warn + report.summary.error).toBe(
      report.checks.length,
    );
  });

  it("all checks pass for a well-formed project", () => {
    writePlan(tempDir, minimalPlan([
      "my_change 2024-01-15T10:30:00Z A <a@b.com> # change",
    ]));
    writeScript(tempDir, "deploy", "my_change", "CREATE TABLE t (id int);\n");
    writeScript(tempDir, "revert", "my_change", "DROP TABLE t;\n");
    writeScript(tempDir, "verify", "my_change", "SELECT 1;\n");

    const report = runDoctorChecks({ topDir: tempDir });
    expect(report.summary.error).toBe(0);
    expect(report.summary.warn).toBe(0);
    expect(report.summary.ok).toBe(report.checks.length);
  });
});
