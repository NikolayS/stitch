import { describe, it, expect } from "bun:test";
import { EventEmitter } from "events";
import {
  PsqlRunner,
  buildPsqlCommand,
  parsePsqlStderr,
  extractPassword,
  type SpawnFn,
  type PsqlRunOptions,
} from "../../src/psql";

// ---------------------------------------------------------------------------
// Helpers — mock subprocess
// ---------------------------------------------------------------------------

interface MockChildProcess extends EventEmitter {
  stdout: EventEmitter;
  stderr: EventEmitter;
}

/**
 * Create a mock spawn function that records calls and allows
 * controlling stdout/stderr/exit of the child process.
 */
function createMockSpawn(
  exitCode = 0,
  stdoutData = "",
  stderrData = "",
) {
  const calls: Array<{
    command: string;
    args: string[];
    options: Record<string, unknown>;
  }> = [];

  const spawnFn: SpawnFn = (command, args, options) => {
    calls.push({ command, args, options: options as Record<string, unknown> });

    const child: MockChildProcess = Object.assign(new EventEmitter(), {
      stdout: new EventEmitter(),
      stderr: new EventEmitter(),
    });

    // Emit data and close asynchronously to simulate real subprocess
    queueMicrotask(() => {
      if (stdoutData) child.stdout.emit("data", Buffer.from(stdoutData));
      if (stderrData) child.stderr.emit("data", Buffer.from(stderrData));
      child.emit("close", exitCode);
    });

    return child as ReturnType<typeof import("child_process").spawn>;
  };

  return { spawnFn, calls };
}

/**
 * Create a mock spawn function that emits an error (e.g., binary not found).
 */
function createErrorSpawn(error: Error) {
  const spawnFn: SpawnFn = () => {
    const child: MockChildProcess = Object.assign(new EventEmitter(), {
      stdout: new EventEmitter(),
      stderr: new EventEmitter(),
    });

    queueMicrotask(() => {
      child.emit("error", error);
    });

    return child as ReturnType<typeof import("child_process").spawn>;
  };

  return { spawnFn };
}

// ---------------------------------------------------------------------------
// Default options helper
// ---------------------------------------------------------------------------

const defaultUri = "postgresql://user@localhost:5432/testdb";

function opts(overrides: Partial<PsqlRunOptions> = {}): PsqlRunOptions {
  return { uri: defaultUri, ...overrides };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("psql module", () => {
  // -----------------------------------------------------------------------
  // buildPsqlCommand — argument construction
  // -----------------------------------------------------------------------

  describe("buildPsqlCommand()", () => {
    it("builds minimal command with required args", () => {
      const cmd = buildPsqlCommand("deploy/001.sql", opts(), "psql");

      expect(cmd.bin).toBe("psql");
      expect(cmd.args).toContain("--no-psqlrc");
      expect(cmd.args).toContain("-f");
      expect(cmd.args).toContain("deploy/001.sql");
      expect(cmd.env["PSQLRC"]).toBe("/dev/null");
    });

    it("sets ON_ERROR_STOP=1", () => {
      const cmd = buildPsqlCommand("test.sql", opts(), "psql");

      // Should have -v ON_ERROR_STOP=1
      const vIdx = cmd.args.indexOf("-v");
      expect(vIdx).toBeGreaterThanOrEqual(0);
      expect(cmd.args[vIdx + 1]).toBe("ON_ERROR_STOP=1");
    });

    it("passes --single-transaction when requested", () => {
      const cmd = buildPsqlCommand(
        "test.sql",
        opts({ singleTransaction: true }),
        "psql",
      );

      expect(cmd.args).toContain("--single-transaction");
    });

    it("omits --single-transaction when not requested", () => {
      const cmd = buildPsqlCommand(
        "test.sql",
        opts({ singleTransaction: false }),
        "psql",
      );

      expect(cmd.args).not.toContain("--single-transaction");
    });

    it("passes user variables via -v key=value", () => {
      const cmd = buildPsqlCommand(
        "test.sql",
        opts({ variables: { schema: "public", version: "42" } }),
        "psql",
      );

      // Find all -v arguments
      const varArgs: string[] = [];
      for (let i = 0; i < cmd.args.length; i++) {
        if (cmd.args[i] === "-v" && i + 1 < cmd.args.length) {
          varArgs.push(cmd.args[i + 1]!);
        }
      }

      expect(varArgs).toContain("ON_ERROR_STOP=1");
      expect(varArgs).toContain("schema=public");
      expect(varArgs).toContain("version=42");
    });

    it("passes URI via --dbname", () => {
      const cmd = buildPsqlCommand("test.sql", opts(), "psql");

      const dbIdx = cmd.args.indexOf("--dbname");
      expect(dbIdx).toBeGreaterThanOrEqual(0);
      expect(cmd.args[dbIdx + 1]).toBe(defaultUri);
    });

    it("uses custom dbClient when provided", () => {
      const cmd = buildPsqlCommand(
        "test.sql",
        opts({ dbClient: "/opt/pg16/bin/psql" }),
        "psql",
      );

      expect(cmd.bin).toBe("/opt/pg16/bin/psql");
    });

    it("uses constructor default when dbClient not provided", () => {
      const cmd = buildPsqlCommand("test.sql", opts(), "/usr/bin/psql");

      expect(cmd.bin).toBe("/usr/bin/psql");
    });

    it("passes working directory through", () => {
      const cmd = buildPsqlCommand(
        "test.sql",
        opts({ workingDir: "/home/project" }),
        "psql",
      );

      expect(cmd.cwd).toBe("/home/project");
    });

    it("sets cwd to undefined when no workingDir specified", () => {
      const cmd = buildPsqlCommand("test.sql", opts(), "psql");

      expect(cmd.cwd).toBeUndefined();
    });

    it("places -f scriptPath as the last arguments", () => {
      const cmd = buildPsqlCommand("deploy/001-init.sql", opts(), "psql");

      const lastTwo = cmd.args.slice(-2);
      expect(lastTwo).toEqual(["-f", "deploy/001-init.sql"]);
    });
  });

  // -----------------------------------------------------------------------
  // URI password handling — security
  // -----------------------------------------------------------------------

  describe("extractPassword()", () => {
    it("extracts password from postgresql:// URI", () => {
      const result = extractPassword("postgresql://user:secret@host/db");
      expect(result.password).toBe("secret");
      expect(result.cleanUri).toBe("postgresql://user@host/db");
    });

    it("extracts password from postgres:// URI", () => {
      const result = extractPassword("postgres://admin:p4ssw0rd@db.example.com:5432/mydb");
      expect(result.password).toBe("p4ssw0rd");
      expect(result.cleanUri).toBe("postgres://admin@db.example.com:5432/mydb");
    });

    it("handles password with special characters", () => {
      const result = extractPassword("postgresql://user:p@ss:word!@host/db");
      expect(result.password).toBe("p@ss:word!");
      expect(result.cleanUri).toBe("postgresql://user@host/db");
    });

    it("returns undefined password when URI has no password", () => {
      const result = extractPassword("postgresql://user@host/db");
      expect(result.password).toBeUndefined();
      expect(result.cleanUri).toBe("postgresql://user@host/db");
    });

    it("returns undefined password for host-only URI", () => {
      const result = extractPassword("postgresql://host/db");
      expect(result.password).toBeUndefined();
      expect(result.cleanUri).toBe("postgresql://host/db");
    });
  });

  describe("password never appears in args", () => {
    it("strips password from URI and sets PGPASSWORD env var", () => {
      const cmd = buildPsqlCommand(
        "test.sql",
        opts({ uri: "postgresql://admin:s3cret@host:5432/db" }),
        "psql",
      );

      // Password must NOT appear anywhere in args
      const argsJoined = cmd.args.join(" ");
      expect(argsJoined).not.toContain("s3cret");

      // Password must be in PGPASSWORD env var
      expect(cmd.env["PGPASSWORD"]).toBe("s3cret");

      // The URI passed via --dbname must not contain the password
      const dbIdx = cmd.args.indexOf("--dbname");
      expect(cmd.args[dbIdx + 1]).toBe("postgresql://admin@host:5432/db");
    });

    it("does not set PGPASSWORD when URI has no password", () => {
      const cmd = buildPsqlCommand(
        "test.sql",
        opts({ uri: "postgresql://user@host/db" }),
        "psql",
      );

      expect(cmd.env["PGPASSWORD"]).toBeUndefined();
    });
  });

  // -----------------------------------------------------------------------
  // parsePsqlStderr — error extraction
  // -----------------------------------------------------------------------

  describe("parsePsqlStderr()", () => {
    it("returns undefined for empty stderr", () => {
      expect(parsePsqlStderr("")).toBeUndefined();
      expect(parsePsqlStderr("   ")).toBeUndefined();
    });

    it("parses standard psql error with file location", () => {
      const stderr =
        'psql:deploy/001-init.sql:42: ERROR:  relation "users" does not exist\n' +
        "LINE 1: SELECT * FROM users;\n" +
        "                       ^\n";

      const err = parsePsqlStderr(stderr);
      expect(err).toBeDefined();
      expect(err!.severity).toBe("ERROR");
      expect(err!.message).toBe('relation "users" does not exist');
      expect(err!.location).toBe("deploy/001-init.sql:42");
    });

    it("parses error without file location", () => {
      const stderr = 'ERROR:  syntax error at or near "SELEC"\n';

      const err = parsePsqlStderr(stderr);
      expect(err).toBeDefined();
      expect(err!.severity).toBe("ERROR");
      expect(err!.message).toBe('syntax error at or near "SELEC"');
      expect(err!.location).toBeUndefined();
    });

    it("parses FATAL error", () => {
      const stderr =
        'FATAL:  database "nonexistent" does not exist\n';

      const err = parsePsqlStderr(stderr);
      expect(err).toBeDefined();
      expect(err!.severity).toBe("FATAL");
      expect(err!.message).toBe('database "nonexistent" does not exist');
    });

    it("extracts DETAIL line", () => {
      const stderr =
        'psql:test.sql:5: ERROR:  insert or update on table "orders" violates foreign key constraint "orders_user_id_fkey"\n' +
        "DETAIL:  Key (user_id)=(999) is not present in table \"users\".\n";

      const err = parsePsqlStderr(stderr);
      expect(err).toBeDefined();
      expect(err!.detail).toBe(
        'Key (user_id)=(999) is not present in table "users".',
      );
    });

    it("extracts HINT line", () => {
      const stderr =
        "ERROR:  column \"foo\" does not exist\n" +
        'HINT:  Perhaps you meant to reference the column "bar.foo".\n';

      const err = parsePsqlStderr(stderr);
      expect(err).toBeDefined();
      expect(err!.hint).toBe(
        'Perhaps you meant to reference the column "bar.foo".',
      );
    });

    it("extracts CONTEXT line", () => {
      const stderr =
        "ERROR:  null value in column \"id\" violates not-null constraint\n" +
        "CONTEXT:  SQL function \"insert_foo\" statement 1\n";

      const err = parsePsqlStderr(stderr);
      expect(err).toBeDefined();
      expect(err!.context).toBe(
        'SQL function "insert_foo" statement 1',
      );
    });

    it("extracts STATEMENT line", () => {
      const stderr =
        "ERROR:  division by zero\n" +
        "STATEMENT:  SELECT 1/0;\n";

      const err = parsePsqlStderr(stderr);
      expect(err).toBeDefined();
      expect(err!.statement).toBe("SELECT 1/0;");
    });

    it("returns undefined for non-error stderr output", () => {
      // psql may emit notices or warnings to stderr
      const stderr = "NOTICE:  table \"foo\" does not exist, skipping\n";

      const err = parsePsqlStderr(stderr);
      expect(err).toBeUndefined();
    });

    it("handles multi-field error output", () => {
      const stderr =
        'psql:deploy/002.sql:10: ERROR:  duplicate key value violates unique constraint "users_pkey"\n' +
        "DETAIL:  Key (id)=(1) already exists.\n" +
        "HINT:  Use ON CONFLICT to handle duplicates.\n" +
        "CONTEXT:  SQL statement in PL/pgSQL function\n" +
        "STATEMENT:  INSERT INTO users (id) VALUES (1);\n";

      const err = parsePsqlStderr(stderr);
      expect(err).toBeDefined();
      expect(err!.severity).toBe("ERROR");
      expect(err!.message).toBe(
        'duplicate key value violates unique constraint "users_pkey"',
      );
      expect(err!.location).toBe("deploy/002.sql:10");
      expect(err!.detail).toBe("Key (id)=(1) already exists.");
      expect(err!.hint).toBe("Use ON CONFLICT to handle duplicates.");
      expect(err!.context).toBe("SQL statement in PL/pgSQL function");
      expect(err!.statement).toBe("INSERT INTO users (id) VALUES (1);");
    });
  });

  // -----------------------------------------------------------------------
  // PsqlRunner — integration with mock subprocess
  // -----------------------------------------------------------------------

  describe("PsqlRunner", () => {
    it("runs psql and returns stdout/stderr on success", async () => {
      const { spawnFn, calls } = createMockSpawn(0, "SELECT 1\n", "");
      const runner = new PsqlRunner("psql", spawnFn);

      const result = await runner.run("test.sql", opts());

      expect(result.exitCode).toBe(0);
      expect(result.stdout).toBe("SELECT 1\n");
      expect(result.stderr).toBe("");
      expect(result.error).toBeUndefined();

      // Verify spawn was called with correct binary
      expect(calls).toHaveLength(1);
      expect(calls[0]!.command).toBe("psql");
    });

    it("returns parsed error on non-zero exit", async () => {
      const stderrOutput =
        'psql:deploy/001.sql:5: ERROR:  relation "foo" does not exist\n';
      const { spawnFn } = createMockSpawn(2, "", stderrOutput);
      const runner = new PsqlRunner("psql", spawnFn);

      const result = await runner.run("deploy/001.sql", opts());

      expect(result.exitCode).toBe(2);
      expect(result.error).toBeDefined();
      expect(result.error!.severity).toBe("ERROR");
      expect(result.error!.message).toBe('relation "foo" does not exist');
    });

    it("does not parse errors when exit code is 0", async () => {
      // Even if stderr has content, no error parsing on success
      const { spawnFn } = createMockSpawn(
        0,
        "",
        "NOTICE:  table created\n",
      );
      const runner = new PsqlRunner("psql", spawnFn);

      const result = await runner.run("test.sql", opts());

      expect(result.exitCode).toBe(0);
      expect(result.error).toBeUndefined();
      expect(result.stderr).toBe("NOTICE:  table created\n");
    });

    it("rejects when spawn fails (e.g., binary not found)", async () => {
      const spawnError = new Error("spawn psql ENOENT");
      const { spawnFn } = createErrorSpawn(spawnError);
      const runner = new PsqlRunner("psql", spawnFn);

      await expect(runner.run("test.sql", opts())).rejects.toThrow(
        "spawn psql ENOENT",
      );
    });

    it("uses custom psql path from constructor", async () => {
      const { spawnFn, calls } = createMockSpawn(0);
      const runner = new PsqlRunner("/opt/pg16/bin/psql", spawnFn);

      await runner.run("test.sql", opts());

      expect(calls[0]!.command).toBe("/opt/pg16/bin/psql");
    });

    it("uses dbClient from options over constructor default", async () => {
      const { spawnFn, calls } = createMockSpawn(0);
      const runner = new PsqlRunner("/default/psql", spawnFn);

      await runner.run("test.sql", opts({ dbClient: "/override/psql" }));

      expect(calls[0]!.command).toBe("/override/psql");
    });

    it("passes PSQLRC=/dev/null in environment", async () => {
      const { spawnFn, calls } = createMockSpawn(0);
      const runner = new PsqlRunner("psql", spawnFn);

      await runner.run("test.sql", opts());

      const env = calls[0]!.options["env"] as Record<string, string>;
      expect(env["PSQLRC"]).toBe("/dev/null");
    });

    it("passes PGPASSWORD in environment when URI has password", async () => {
      const { spawnFn, calls } = createMockSpawn(0);
      const runner = new PsqlRunner("psql", spawnFn);

      await runner.run(
        "test.sql",
        opts({ uri: "postgresql://user:topsecret@host/db" }),
      );

      const env = calls[0]!.options["env"] as Record<string, string>;
      expect(env["PGPASSWORD"]).toBe("topsecret");

      // Password must not be in args
      const args = calls[0]!.args;
      expect(args.join(" ")).not.toContain("topsecret");
    });

    it("sets working directory when provided", async () => {
      const { spawnFn, calls } = createMockSpawn(0);
      const runner = new PsqlRunner("psql", spawnFn);

      await runner.run("test.sql", opts({ workingDir: "/project/root" }));

      expect(calls[0]!.options["cwd"]).toBe("/project/root");
    });

    it("passes --single-transaction when requested", async () => {
      const { spawnFn, calls } = createMockSpawn(0);
      const runner = new PsqlRunner("psql", spawnFn);

      await runner.run("test.sql", opts({ singleTransaction: true }));

      expect(calls[0]!.args).toContain("--single-transaction");
    });

    it("passes variables as -v key=value", async () => {
      const { spawnFn, calls } = createMockSpawn(0);
      const runner = new PsqlRunner("psql", spawnFn);

      await runner.run(
        "test.sql",
        opts({ variables: { schema: "myschema", debug: "on" } }),
      );

      const args = calls[0]!.args;
      // Find all -v args after ON_ERROR_STOP
      const varArgs: string[] = [];
      for (let i = 0; i < args.length; i++) {
        if (args[i] === "-v" && i + 1 < args.length) {
          varArgs.push(args[i + 1]!);
        }
      }

      expect(varArgs).toContain("schema=myschema");
      expect(varArgs).toContain("debug=on");
    });

    it("handles null exit code as exit code 1", async () => {
      // When the process is killed, exit code can be null
      const { spawnFn } = createMockSpawn(null as unknown as number);
      const runner = new PsqlRunner("psql", spawnFn);

      const result = await runner.run("test.sql", opts());

      expect(result.exitCode).toBe(1);
    });

    it("includes --no-psqlrc in args", async () => {
      const { spawnFn, calls } = createMockSpawn(0);
      const runner = new PsqlRunner("psql", spawnFn);

      await runner.run("test.sql", opts());

      expect(calls[0]!.args).toContain("--no-psqlrc");
    });

    it("uses stdin=ignore to prevent interactive prompts", async () => {
      const { spawnFn, calls } = createMockSpawn(0);
      const runner = new PsqlRunner("psql", spawnFn);

      await runner.run("test.sql", opts());

      const stdio = calls[0]!.options["stdio"] as string[];
      expect(stdio[0]).toBe("ignore");
    });
  });
});
