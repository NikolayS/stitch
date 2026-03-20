import { describe, it, expect, beforeEach, mock } from "bun:test";
import { resetConfig } from "../../src/output";

// ---------------------------------------------------------------------------
// Mock pg/lib/client — same approach as registry.test.ts / revert.test.ts
// ---------------------------------------------------------------------------

let mockInstances: MockPgClient[] = [];

class MockPgClient {
  options: Record<string, unknown>;
  queries: Array<{ text: string; values?: unknown[] }> = [];
  connected = false;
  ended = false;

  constructor(options: Record<string, unknown>) {
    this.options = options;
    mockInstances.push(this);
  }

  async connect() {
    this.connected = true;
  }

  async query(text: string, values?: unknown[]) {
    this.queries.push({ text, values });
    return { rows: [], rowCount: 0, command: "SELECT" };
  }

  async end() {
    this.ended = true;
    this.connected = false;
  }
}

mock.module("pg/lib/client", () => ({
  default: MockPgClient,
  __esModule: true,
}));

// Import after mocking
const {
  parseVerifyOptions,
  filterChangesForRange,
  getVerifyScriptPath,
  runVerifyScript,
  formatVerifyResult,
  resolveTargetUri,
  EXIT_CODE_VERIFY_FAILED,
} = await import("../../src/commands/verify");
const { parseArgs } = await import("../../src/cli");

// We also need PsqlRunner for mocking
const { PsqlRunner } = await import("../../src/psql");

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Build a minimal RegistryChange for testing. */
function makeDeployedChange(
  name: string,
  changeId: string,
  overrides: Record<string, unknown> = {},
) {
  return {
    change_id: changeId,
    script_hash: `hash_${changeId}`,
    change: name,
    project: "testproject",
    note: `Note for ${name}`,
    committed_at: new Date("2025-01-15T10:00:00Z"),
    committer_name: "Test User",
    committer_email: "test@example.com",
    planned_at: new Date("2025-01-15T10:00:00Z"),
    planner_name: "Plan User",
    planner_email: "plan@example.com",
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("verify command", () => {
  beforeEach(() => {
    mockInstances = [];
    resetConfig();
  });

  // -----------------------------------------------------------------------
  // EXIT_CODE_VERIFY_FAILED constant
  // -----------------------------------------------------------------------

  describe("EXIT_CODE_VERIFY_FAILED", () => {
    it("equals 3 per SPEC R6", () => {
      expect(EXIT_CODE_VERIFY_FAILED).toBe(3);
    });
  });

  // -----------------------------------------------------------------------
  // parseVerifyOptions
  // -----------------------------------------------------------------------

  describe("parseVerifyOptions()", () => {
    it("parses --from flag", () => {
      const args = parseArgs(["verify", "--from", "add_users"]);
      const opts = parseVerifyOptions(args);
      expect(opts.fromChange).toBe("add_users");
    });

    it("parses --to flag", () => {
      const args = parseArgs(["verify", "--to", "add_roles"]);
      const opts = parseVerifyOptions(args);
      expect(opts.toChange).toBe("add_roles");
    });

    it("parses both --from and --to flags", () => {
      const args = parseArgs([
        "verify",
        "--from",
        "create_schema",
        "--to",
        "add_users",
      ]);
      const opts = parseVerifyOptions(args);
      expect(opts.fromChange).toBe("create_schema");
      expect(opts.toChange).toBe("add_users");
    });

    it("parses positional target", () => {
      const args = parseArgs(["verify", "production"]);
      const opts = parseVerifyOptions(args);
      expect(opts.target).toBe("production");
    });

    it("defaults fromChange and toChange to undefined", () => {
      const args = parseArgs(["verify"]);
      const opts = parseVerifyOptions(args);
      expect(opts.fromChange).toBeUndefined();
      expect(opts.toChange).toBeUndefined();
    });

    it("inherits --db-uri from global args", () => {
      const args = parseArgs(["--db-uri", "postgresql://host/db", "verify"]);
      const opts = parseVerifyOptions(args);
      expect(opts.dbUri).toBe("postgresql://host/db");
    });

    it("inherits --plan-file from global args", () => {
      const args = parseArgs(["--plan-file", "custom.plan", "verify"]);
      const opts = parseVerifyOptions(args);
      expect(opts.planFile).toBe("custom.plan");
    });

    it("defaults topDir to '.'", () => {
      const args = parseArgs(["verify"]);
      const opts = parseVerifyOptions(args);
      expect(opts.topDir).toBe(".");
    });

    it("inherits --top-dir from global args", () => {
      const args = parseArgs(["--top-dir", "/my/project", "verify"]);
      const opts = parseVerifyOptions(args);
      expect(opts.topDir).toBe("/my/project");
    });
  });

  // -----------------------------------------------------------------------
  // filterChangesForRange
  // -----------------------------------------------------------------------

  describe("filterChangesForRange()", () => {
    it("returns all changes when no from/to specified", () => {
      const deployed = [
        makeDeployedChange("a", "id_a"),
        makeDeployedChange("b", "id_b"),
        makeDeployedChange("c", "id_c"),
      ];

      const result = filterChangesForRange(deployed);
      expect(result.map((c) => c.change)).toEqual(["a", "b", "c"]);
    });

    it("returns empty array when no changes deployed", () => {
      const result = filterChangesForRange([]);
      expect(result).toEqual([]);
    });

    it("filters from --from (inclusive) to end", () => {
      const deployed = [
        makeDeployedChange("a", "id_a"),
        makeDeployedChange("b", "id_b"),
        makeDeployedChange("c", "id_c"),
        makeDeployedChange("d", "id_d"),
      ];

      const result = filterChangesForRange(deployed, "b");
      expect(result.map((c) => c.change)).toEqual(["b", "c", "d"]);
    });

    it("filters from start to --to (inclusive)", () => {
      const deployed = [
        makeDeployedChange("a", "id_a"),
        makeDeployedChange("b", "id_b"),
        makeDeployedChange("c", "id_c"),
        makeDeployedChange("d", "id_d"),
      ];

      const result = filterChangesForRange(deployed, undefined, "b");
      expect(result.map((c) => c.change)).toEqual(["a", "b"]);
    });

    it("filters with both --from and --to (inclusive)", () => {
      const deployed = [
        makeDeployedChange("a", "id_a"),
        makeDeployedChange("b", "id_b"),
        makeDeployedChange("c", "id_c"),
        makeDeployedChange("d", "id_d"),
        makeDeployedChange("e", "id_e"),
      ];

      const result = filterChangesForRange(deployed, "b", "d");
      expect(result.map((c) => c.change)).toEqual(["b", "c", "d"]);
    });

    it("returns single change when --from equals --to", () => {
      const deployed = [
        makeDeployedChange("a", "id_a"),
        makeDeployedChange("b", "id_b"),
        makeDeployedChange("c", "id_c"),
      ];

      const result = filterChangesForRange(deployed, "b", "b");
      expect(result.map((c) => c.change)).toEqual(["b"]);
    });

    it("returns empty array when --from is after --to", () => {
      const deployed = [
        makeDeployedChange("a", "id_a"),
        makeDeployedChange("b", "id_b"),
        makeDeployedChange("c", "id_c"),
      ];

      const result = filterChangesForRange(deployed, "c", "a");
      expect(result).toEqual([]);
    });

    it("throws when --from change is not deployed", () => {
      const deployed = [
        makeDeployedChange("a", "id_a"),
        makeDeployedChange("b", "id_b"),
      ];

      expect(() => filterChangesForRange(deployed, "nonexistent")).toThrow(
        "Change 'nonexistent' is not deployed. Cannot use as --from target.",
      );
    });

    it("throws when --to change is not deployed", () => {
      const deployed = [
        makeDeployedChange("a", "id_a"),
        makeDeployedChange("b", "id_b"),
      ];

      expect(() =>
        filterChangesForRange(deployed, undefined, "nonexistent"),
      ).toThrow(
        "Change 'nonexistent' is not deployed. Cannot use as --to target.",
      );
    });

    it("returns first change only when --from and --to are both first", () => {
      const deployed = [
        makeDeployedChange("a", "id_a"),
        makeDeployedChange("b", "id_b"),
      ];

      const result = filterChangesForRange(deployed, "a", "a");
      expect(result.map((c) => c.change)).toEqual(["a"]);
    });

    it("returns last change only when --from and --to are both last", () => {
      const deployed = [
        makeDeployedChange("a", "id_a"),
        makeDeployedChange("b", "id_b"),
      ];

      const result = filterChangesForRange(deployed, "b", "b");
      expect(result.map((c) => c.change)).toEqual(["b"]);
    });
  });

  // -----------------------------------------------------------------------
  // getVerifyScriptPath
  // -----------------------------------------------------------------------

  describe("getVerifyScriptPath()", () => {
    it("constructs path from verify dir and change name", () => {
      const result = getVerifyScriptPath("/project/verify", "add_users");
      expect(result).toBe("/project/verify/add_users.sql");
    });

    it("handles nested change names", () => {
      const result = getVerifyScriptPath("/project/verify", "schema/add_users");
      expect(result).toBe("/project/verify/schema/add_users.sql");
    });
  });

  // -----------------------------------------------------------------------
  // runVerifyScript
  // -----------------------------------------------------------------------

  describe("runVerifyScript()", () => {
    it("returns skipped=true when script does not exist", async () => {
      const runner = new PsqlRunner();

      const result = await runVerifyScript(
        runner,
        "/nonexistent/path/to/verify.sql",
        "postgresql://host/db",
        "/project",
        "add_users",
        "id_1",
      );

      expect(result.pass).toBe(true);
      expect(result.skipped).toBe(true);
      expect(result.name).toBe("add_users");
      expect(result.change_id).toBe("id_1");
    });

    it("returns pass=true when psql exits 0", async () => {
      // Create a mock PsqlRunner that always succeeds
      const mockRunner = {
        run: async () => ({
          exitCode: 0,
          stdout: "VERIFY SUCCESSFUL",
          stderr: "",
        }),
      } as unknown as InstanceType<typeof PsqlRunner>;

      // Use a path that exists — the test file itself
      const thisFile = import.meta.path;

      const result = await runVerifyScript(
        mockRunner,
        thisFile,
        "postgresql://host/db",
        "/project",
        "add_users",
        "id_1",
      );

      expect(result.pass).toBe(true);
      expect(result.skipped).toBe(false);
      expect(result.error).toBeUndefined();
    });

    it("returns pass=false with error when psql exits non-zero", async () => {
      const mockRunner = {
        run: async () => ({
          exitCode: 1,
          stdout: "",
          stderr: "",
          error: { message: "relation \"users\" does not exist", severity: "ERROR" },
        }),
      } as unknown as InstanceType<typeof PsqlRunner>;

      const thisFile = import.meta.path;

      const result = await runVerifyScript(
        mockRunner,
        thisFile,
        "postgresql://host/db",
        "/project",
        "add_users",
        "id_1",
      );

      expect(result.pass).toBe(false);
      expect(result.skipped).toBe(false);
      expect(result.error).toContain("relation");
    });

    it("returns pass=false when psql run throws (spawn failure)", async () => {
      const mockRunner = {
        run: async () => {
          throw new Error("psql: command not found");
        },
      } as unknown as InstanceType<typeof PsqlRunner>;

      const thisFile = import.meta.path;

      const result = await runVerifyScript(
        mockRunner,
        thisFile,
        "postgresql://host/db",
        "/project",
        "add_users",
        "id_1",
      );

      expect(result.pass).toBe(false);
      expect(result.skipped).toBe(false);
      expect(result.error).toContain("psql: command not found");
    });

    it("uses stderr as fallback error when no parsed error", async () => {
      const mockRunner = {
        run: async () => ({
          exitCode: 1,
          stdout: "",
          stderr: "psql:verify/foo.sql:3: ERROR:  something went wrong",
          error: undefined,
        }),
      } as unknown as InstanceType<typeof PsqlRunner>;

      const thisFile = import.meta.path;

      const result = await runVerifyScript(
        mockRunner,
        thisFile,
        "postgresql://host/db",
        "/project",
        "foo",
        "id_foo",
      );

      expect(result.pass).toBe(false);
      expect(result.error).toContain("something went wrong");
    });
  });

  // -----------------------------------------------------------------------
  // formatVerifyResult
  // -----------------------------------------------------------------------

  describe("formatVerifyResult()", () => {
    it("formats all-pass results", () => {
      const result = formatVerifyResult({
        changes: [
          { name: "a", change_id: "id_a", pass: true, skipped: false },
          { name: "b", change_id: "id_b", pass: true, skipped: false },
        ],
        total: 2,
        passed: 2,
        failed: 0,
        skipped: 0,
      });

      expect(result).toContain("a .. ok");
      expect(result).toContain("b .. ok");
      expect(result).toContain("2 passed");
      expect(result).toContain("2 total");
    });

    it("formats results with failures", () => {
      const result = formatVerifyResult({
        changes: [
          { name: "a", change_id: "id_a", pass: true, skipped: false },
          {
            name: "b",
            change_id: "id_b",
            pass: false,
            skipped: false,
            error: "table missing",
          },
        ],
        total: 2,
        passed: 1,
        failed: 1,
        skipped: 0,
      });

      expect(result).toContain("a .. ok");
      expect(result).toContain("b .. FAIL: table missing");
      expect(result).toContain("1 passed");
      expect(result).toContain("1 failed");
    });

    it("formats results with skipped changes", () => {
      const result = formatVerifyResult({
        changes: [
          { name: "a", change_id: "id_a", pass: true, skipped: false },
          { name: "b", change_id: "id_b", pass: true, skipped: true },
        ],
        total: 2,
        passed: 1,
        failed: 0,
        skipped: 1,
      });

      expect(result).toContain("a .. ok");
      expect(result).toContain("b .. skipped (no verify script)");
      expect(result).toContain("1 passed");
      expect(result).toContain("1 skipped");
    });

    it("formats mixed pass/fail/skip results", () => {
      const result = formatVerifyResult({
        changes: [
          { name: "a", change_id: "id_a", pass: true, skipped: false },
          { name: "b", change_id: "id_b", pass: true, skipped: true },
          {
            name: "c",
            change_id: "id_c",
            pass: false,
            skipped: false,
            error: "constraint violation",
          },
        ],
        total: 3,
        passed: 1,
        failed: 1,
        skipped: 1,
      });

      expect(result).toContain("a .. ok");
      expect(result).toContain("b .. skipped");
      expect(result).toContain("c .. FAIL: constraint violation");
      expect(result).toContain("3 total");
    });

    it("includes summary line with all counts", () => {
      const result = formatVerifyResult({
        changes: [],
        total: 0,
        passed: 0,
        failed: 0,
        skipped: 0,
      });

      expect(result).toContain("Verify summary:");
      expect(result).toContain("0 passed");
    });
  });

  // -----------------------------------------------------------------------
  // resolveTargetUri
  // -----------------------------------------------------------------------

  describe("resolveTargetUri()", () => {
    it("returns --db-uri when provided", () => {
      const uri = resolveTargetUri(
        { dbUri: "postgresql://host/db", topDir: "." },
        { targets: {}, engines: {} } as never,
      );
      expect(uri).toBe("postgresql://host/db");
    });

    it("looks up named target from config", () => {
      const uri = resolveTargetUri(
        { target: "prod", topDir: "." },
        {
          targets: { prod: { name: "prod", uri: "postgresql://prod/db" } },
          engines: {},
        } as never,
      );
      expect(uri).toBe("postgresql://prod/db");
    });

    it("falls back to engine target string", () => {
      const uri = resolveTargetUri(
        { topDir: "." },
        {
          targets: {},
          engines: { pg: { name: "pg", target: "db:pg://local/mydb" } },
        } as never,
      );
      expect(uri).toBe("db:pg://local/mydb");
    });

    it("returns undefined when no target configured", () => {
      const uri = resolveTargetUri(
        { topDir: "." },
        { targets: {}, engines: {} } as never,
      );
      expect(uri).toBeUndefined();
    });
  });

  // -----------------------------------------------------------------------
  // CLI integration via parseArgs
  // -----------------------------------------------------------------------

  describe("CLI routing", () => {
    it("parseArgs recognizes 'verify' command", () => {
      const args = parseArgs(["verify"]);
      expect(args.command).toBe("verify");
    });

    it("parseArgs passes --from and --to through to rest", () => {
      const args = parseArgs(["verify", "--from", "a", "--to", "b"]);
      expect(args.command).toBe("verify");
      expect(args.rest).toContain("--from");
      expect(args.rest).toContain("a");
      expect(args.rest).toContain("--to");
      expect(args.rest).toContain("b");
    });

    it("parseArgs handles global flags before verify", () => {
      const args = parseArgs([
        "--verbose",
        "--db-uri",
        "postgresql://h/d",
        "verify",
        "--from",
        "x",
      ]);
      expect(args.command).toBe("verify");
      expect(args.verbose).toBe(true);
      expect(args.dbUri).toBe("postgresql://h/d");
      expect(args.rest).toContain("--from");
    });
  });

  // -----------------------------------------------------------------------
  // Integration: customer-zero scenario (6 missing verify scripts)
  // -----------------------------------------------------------------------

  describe("customer-zero: missing verify scripts", () => {
    it("reports 6 skipped for 6 missing verify scripts among 10 changes", async () => {
      // Simulate: 10 changes, 6 have no verify script (nonexistent paths)
      const mockRunner = {
        run: async () => ({
          exitCode: 0,
          stdout: "OK",
          stderr: "",
        }),
      } as unknown as InstanceType<typeof PsqlRunner>;

      const results = [];
      for (let i = 0; i < 10; i++) {
        const hasVerify = i < 4; // Only first 4 have verify scripts
        const scriptPath = hasVerify
          ? import.meta.path // existing file
          : `/nonexistent/verify/change_${i}.sql`;

        const result = await runVerifyScript(
          mockRunner,
          scriptPath,
          "postgresql://host/db",
          "/project",
          `change_${i}`,
          `id_${i}`,
        );
        results.push(result);
      }

      const skipped = results.filter((r) => r.skipped);
      const passed = results.filter((r) => r.pass && !r.skipped);

      expect(skipped.length).toBe(6);
      expect(passed.length).toBe(4);
      expect(results.every((r) => r.pass)).toBe(true); // all pass (skipped counts as pass)
    });
  });

  // -----------------------------------------------------------------------
  // Edge cases
  // -----------------------------------------------------------------------

  describe("edge cases", () => {
    it("filterChangesForRange preserves full change objects", () => {
      const deployed = [
        makeDeployedChange("a", "id_a", { note: "Custom note" }),
      ];

      const result = filterChangesForRange(deployed);
      expect(result[0]!.note).toBe("Custom note");
      expect(result[0]!.change_id).toBe("id_a");
    });

    it("filterChangesForRange works with single change and --from matching it", () => {
      const deployed = [makeDeployedChange("only", "id_only")];

      const result = filterChangesForRange(deployed, "only");
      expect(result.map((c) => c.change)).toEqual(["only"]);
    });

    it("filterChangesForRange works with single change and --to matching it", () => {
      const deployed = [makeDeployedChange("only", "id_only")];

      const result = filterChangesForRange(deployed, undefined, "only");
      expect(result.map((c) => c.change)).toEqual(["only"]);
    });
  });
});
