// tests/unit/expand-contract.test.ts — Tests for expand/contract generator
//
// Validates: pair generation, plan linkage, deploy/revert/verify script
// correctness, naming conventions, edge cases (partitioned tables, type
// conversions), argument parsing, and validation.

import { describe, expect, it, beforeEach, afterEach } from "bun:test";
import {
  mkdtempSync,
  writeFileSync,
  readFileSync,
  existsSync,
  mkdirSync,
} from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { rmSync } from "node:fs";

import {
  deriveChangeNames,
  syncTriggerName,
  syncTriggerFunctionName,
  expandDeployTemplate,
  expandRevertTemplate,
  expandVerifyTemplate,
  contractDeployTemplate,
  contractRevertTemplate,
  contractVerifyTemplate,
  parseExpandArgs,
  validateExpandOptions,
  inferOperation,
  generateExpandContract,
  type ExpandContractConfig,
} from "../../src/expand-contract/generator";
import { readPlanInfo } from "../../src/commands/add";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let tmpDir: string;

function createTmpDir(): string {
  return mkdtempSync(join(tmpdir(), "sqlever-ec-test-"));
}

/** Create a minimal project directory with sqitch.plan. */
function setupProject(
  dir: string,
  planContent?: string,
): { planPath: string; deployDir: string; revertDir: string; verifyDir: string } {
  const planPath = join(dir, "sqitch.plan");
  const deployDir = join(dir, "deploy");
  const revertDir = join(dir, "revert");
  const verifyDir = join(dir, "verify");

  writeFileSync(
    planPath,
    planContent ??
      "%syntax-version=1.0.0\n" +
      "%project=myproject\n" +
      "\n",
    "utf-8",
  );

  return { planPath, deployDir, revertDir, verifyDir };
}

/** Mock environment with planner identity. */
const TEST_ENV: Record<string, string | undefined> = {
  SQLEVER_USER_NAME: "Test User",
  SQLEVER_USER_EMAIL: "test@example.com",
};

/** Create a minimal MergedConfig for testing. */
function testConfig(topDir: string) {
  return {
    core: {
      engine: undefined,
      top_dir: topDir,
      deploy_dir: "deploy",
      revert_dir: "revert",
      verify_dir: "verify",
      plan_file: "sqitch.plan",
    },
    deploy: {
      verify: true,
      mode: "change" as const,
      lock_retries: 0,
      lock_timeout: "5s",
      idle_in_transaction_session_timeout: "10min",
      search_path: undefined,
    },
    engines: {},
    targets: {},
    analysis: {},
    sqitchConf: { entries: [], rawLines: [], sections: new Set<string>() },
    sqleverToml: null,
  };
}

/** Build a default ExpandContractConfig for rename_col. */
function renameConfig(overrides?: Partial<ExpandContractConfig>): ExpandContractConfig {
  return {
    name: "rename_users_name",
    operation: "rename_col",
    table: "public.users",
    oldColumn: "name",
    newColumn: "full_name",
    oldType: "text",
    note: "Rename name to full_name",
    requires: [],
    conflicts: [],
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Naming conventions
// ---------------------------------------------------------------------------

describe("deriveChangeNames", () => {
  it("appends _expand and _contract suffixes", () => {
    const { expandName, contractName } = deriveChangeNames("rename_users_name");
    expect(expandName).toBe("rename_users_name_expand");
    expect(contractName).toBe("rename_users_name_contract");
  });

  it("handles hyphens in base name", () => {
    const { expandName, contractName } = deriveChangeNames("rename-col");
    expect(expandName).toBe("rename-col_expand");
    expect(contractName).toBe("rename-col_contract");
  });
});

describe("syncTriggerName", () => {
  it("generates trigger name with sqlever_sync_ prefix", () => {
    const name = syncTriggerName("public.users", "name", "full_name");
    expect(name).toBe("sqlever_sync_users_name_full_name");
    expect(name.startsWith("sqlever_sync_")).toBe(true);
  });

  it("strips schema from table name", () => {
    const name = syncTriggerName("myschema.accounts", "email", "contact_email");
    expect(name).toBe("sqlever_sync_accounts_email_contact_email");
  });

  it("handles unqualified table name", () => {
    const name = syncTriggerName("users", "name", "full_name");
    expect(name).toBe("sqlever_sync_users_name_full_name");
  });
});

describe("syncTriggerFunctionName", () => {
  it("generates function name with sqlever_sync_fn_ prefix", () => {
    const name = syncTriggerFunctionName("public.users", "name", "full_name");
    expect(name).toBe("sqlever_sync_fn_users_name_full_name");
  });
});

// ---------------------------------------------------------------------------
// Expand SQL Templates
// ---------------------------------------------------------------------------

describe("expandDeployTemplate", () => {
  it("contains ALTER TABLE ADD COLUMN", () => {
    const sql = expandDeployTemplate(renameConfig());
    expect(sql).toContain("ALTER TABLE public.users ADD COLUMN full_name text");
  });

  it("contains sync trigger function with recursion guard", () => {
    const sql = expandDeployTemplate(renameConfig());
    expect(sql).toContain("pg_trigger_depth() < 2");
    expect(sql).toContain("CREATE OR REPLACE FUNCTION sqlever_sync_fn_users_name_full_name()");
  });

  it("contains CREATE TRIGGER with BEFORE INSERT OR UPDATE", () => {
    const sql = expandDeployTemplate(renameConfig());
    expect(sql).toContain("CREATE TRIGGER sqlever_sync_users_name_full_name");
    expect(sql).toContain("BEFORE INSERT OR UPDATE ON public.users");
  });

  it("wraps in transaction (BEGIN/COMMIT)", () => {
    const sql = expandDeployTemplate(renameConfig());
    expect(sql).toContain("BEGIN;");
    expect(sql).toContain("COMMIT;");
  });

  it("uses custom cast expressions when provided", () => {
    const sql = expandDeployTemplate(renameConfig({
      newType: "integer",
      castForward: "NEW.name::integer",
      castReverse: "NEW.full_name::text",
    }));
    expect(sql).toContain("ADD COLUMN full_name integer");
    expect(sql).toContain("(NEW.name::integer)");
    expect(sql).toContain("(NEW.full_name::text)");
  });

  it("includes table and operation in header comment", () => {
    const sql = expandDeployTemplate(renameConfig());
    expect(sql).toContain("-- Table: public.users");
    expect(sql).toContain("-- Operation: rename_col (name -> full_name)");
  });
});

describe("expandRevertTemplate", () => {
  it("drops trigger, function, and column", () => {
    const sql = expandRevertTemplate(renameConfig());
    expect(sql).toContain("DROP TRIGGER IF EXISTS sqlever_sync_users_name_full_name ON public.users");
    expect(sql).toContain("DROP FUNCTION IF EXISTS sqlever_sync_fn_users_name_full_name()");
    expect(sql).toContain("ALTER TABLE public.users DROP COLUMN IF EXISTS full_name");
  });

  it("wraps in transaction", () => {
    const sql = expandRevertTemplate(renameConfig());
    expect(sql).toContain("BEGIN;");
    expect(sql).toContain("COMMIT;");
  });
});

describe("expandVerifyTemplate", () => {
  it("verifies new column and trigger existence", () => {
    const sql = expandVerifyTemplate(renameConfig());
    expect(sql).toContain("SELECT full_name FROM public.users WHERE false");
    expect(sql).toContain("tgname = 'sqlever_sync_users_name_full_name'");
  });

  it("uses ROLLBACK (not COMMIT) for verify", () => {
    const sql = expandVerifyTemplate(renameConfig());
    expect(sql).toContain("ROLLBACK;");
    expect(sql).not.toContain("COMMIT;");
  });
});

// ---------------------------------------------------------------------------
// Contract SQL Templates
// ---------------------------------------------------------------------------

describe("contractDeployTemplate", () => {
  it("verifies backfill completeness", () => {
    const sql = contractDeployTemplate(renameConfig());
    expect(sql).toContain("Backfill incomplete");
    expect(sql).toContain("WHERE name IS NOT NULL");
    expect(sql).toContain("AND full_name IS NULL");
  });

  it("drops trigger, function, and old column", () => {
    const sql = contractDeployTemplate(renameConfig());
    expect(sql).toContain("DROP TRIGGER IF EXISTS sqlever_sync_users_name_full_name");
    expect(sql).toContain("DROP FUNCTION IF EXISTS sqlever_sync_fn_users_name_full_name()");
    expect(sql).toContain("ALTER TABLE public.users DROP COLUMN name");
  });

  it("wraps in transaction", () => {
    const sql = contractDeployTemplate(renameConfig());
    expect(sql).toContain("BEGIN;");
    expect(sql).toContain("COMMIT;");
  });
});

describe("contractRevertTemplate", () => {
  it("re-adds old column", () => {
    const sql = contractRevertTemplate(renameConfig());
    expect(sql).toContain("ALTER TABLE public.users ADD COLUMN name text");
  });

  it("re-creates sync trigger", () => {
    const sql = contractRevertTemplate(renameConfig());
    expect(sql).toContain("CREATE OR REPLACE FUNCTION sqlever_sync_fn_users_name_full_name()");
    expect(sql).toContain("CREATE TRIGGER sqlever_sync_users_name_full_name");
  });

  it("backfills old column from new", () => {
    const sql = contractRevertTemplate(renameConfig());
    expect(sql).toContain("UPDATE public.users SET name = full_name");
  });
});

describe("contractVerifyTemplate", () => {
  it("verifies old column is gone", () => {
    const sql = contractVerifyTemplate(renameConfig());
    expect(sql).toContain("column_name = 'name'");
    expect(sql).toContain("Old column name still exists");
  });

  it("verifies new column still exists", () => {
    const sql = contractVerifyTemplate(renameConfig());
    expect(sql).toContain("SELECT full_name FROM public.users WHERE false");
  });
});

// ---------------------------------------------------------------------------
// parseExpandArgs
// ---------------------------------------------------------------------------

describe("parseExpandArgs", () => {
  it("parses all expand-specific flags", () => {
    const opts = parseExpandArgs([
      "rename_users_name", "--expand",
      "--table", "public.users",
      "--old-column", "name",
      "--new-column", "full_name",
      "--old-type", "varchar(255)",
      "--new-type", "text",
      "-n", "Rename column",
    ]);
    expect(opts.name).toBe("rename_users_name");
    expect(opts.expand).toBe(true);
    expect(opts.table).toBe("public.users");
    expect(opts.oldColumn).toBe("name");
    expect(opts.newColumn).toBe("full_name");
    expect(opts.oldType).toBe("varchar(255)");
    expect(opts.newType).toBe("text");
    expect(opts.note).toBe("Rename column");
  });

  it("parses cast expressions", () => {
    const opts = parseExpandArgs([
      "change_type", "--expand",
      "--table", "t",
      "--old-column", "a",
      "--new-column", "b",
      "--cast-forward", "NEW.a::integer",
      "--cast-reverse", "NEW.b::text",
    ]);
    expect(opts.castForward).toBe("NEW.a::integer");
    expect(opts.castReverse).toBe("NEW.b::text");
  });

  it("parses requires and conflicts", () => {
    const opts = parseExpandArgs([
      "rename_col", "--expand",
      "--table", "t",
      "--old-column", "a",
      "--new-column", "b",
      "-r", "dep1",
      "-r", "dep2",
      "-c", "conflict1",
    ]);
    expect(opts.requires).toEqual(["dep1", "dep2"]);
    expect(opts.conflicts).toEqual(["conflict1"]);
  });

  it("defaults expand to false when flag not present", () => {
    const opts = parseExpandArgs(["some_name"]);
    expect(opts.expand).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// validateExpandOptions
// ---------------------------------------------------------------------------

describe("validateExpandOptions", () => {
  it("returns null for valid options", () => {
    const err = validateExpandOptions({
      name: "rename_col",
      note: "",
      requires: [],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "public.users",
      oldColumn: "name",
      newColumn: "full_name",
    });
    expect(err).toBeNull();
  });

  it("rejects missing name", () => {
    const err = validateExpandOptions({
      name: "",
      note: "",
      requires: [],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "t",
      oldColumn: "a",
      newColumn: "b",
    });
    expect(err).toContain("change name is required");
  });

  it("rejects invalid name", () => {
    const err = validateExpandOptions({
      name: "123-bad",
      note: "",
      requires: [],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "t",
      oldColumn: "a",
      newColumn: "b",
    });
    expect(err).toContain("invalid change name");
  });

  it("rejects missing table", () => {
    const err = validateExpandOptions({
      name: "rename_col",
      note: "",
      requires: [],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "",
      oldColumn: "a",
      newColumn: "b",
    });
    expect(err).toContain("--table is required");
  });

  it("rejects missing old-column", () => {
    const err = validateExpandOptions({
      name: "rename_col",
      note: "",
      requires: [],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "t",
      oldColumn: "",
      newColumn: "b",
    });
    expect(err).toContain("--old-column is required");
  });

  it("rejects missing new-column", () => {
    const err = validateExpandOptions({
      name: "rename_col",
      note: "",
      requires: [],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "t",
      oldColumn: "a",
      newColumn: "",
    });
    expect(err).toContain("--new-column is required");
  });

  it("rejects same old and new column", () => {
    const err = validateExpandOptions({
      name: "rename_col",
      note: "",
      requires: [],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "t",
      oldColumn: "a",
      newColumn: "a",
    });
    expect(err).toContain("old column and new column must be different");
  });
});

// ---------------------------------------------------------------------------
// inferOperation
// ---------------------------------------------------------------------------

describe("inferOperation", () => {
  it("infers rename_col when types are same or absent", () => {
    const op = inferOperation({
      name: "x", note: "", requires: [], conflicts: [], noVerify: false,
      expand: true, table: "t", oldColumn: "a", newColumn: "b",
    });
    expect(op).toBe("rename_col");
  });

  it("infers change_type when old and new types differ", () => {
    const op = inferOperation({
      name: "x", note: "", requires: [], conflicts: [], noVerify: false,
      expand: true, table: "t", oldColumn: "a", newColumn: "b",
      oldType: "varchar", newType: "text",
    });
    expect(op).toBe("change_type");
  });

  it("infers rename_col when types are the same", () => {
    const op = inferOperation({
      name: "x", note: "", requires: [], conflicts: [], noVerify: false,
      expand: true, table: "t", oldColumn: "a", newColumn: "b",
      oldType: "text", newType: "text",
    });
    expect(op).toBe("rename_col");
  });
});

// ---------------------------------------------------------------------------
// generateExpandContract — full integration
// ---------------------------------------------------------------------------

describe("generateExpandContract", () => {
  beforeEach(() => {
    tmpDir = createTmpDir();
  });

  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it("creates all 6 SQL files (deploy/revert/verify for expand and contract)", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    const result = await generateExpandContract({
      name: "rename_users_name",
      note: "Rename name to full_name",
      requires: [],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "public.users",
      oldColumn: "name",
      newColumn: "full_name",
      oldType: "text",
    }, cfg, TEST_ENV);

    expect(result.files.length).toBe(6);
    expect(existsSync(join(tmpDir, "deploy", "rename_users_name_expand.sql"))).toBe(true);
    expect(existsSync(join(tmpDir, "revert", "rename_users_name_expand.sql"))).toBe(true);
    expect(existsSync(join(tmpDir, "verify", "rename_users_name_expand.sql"))).toBe(true);
    expect(existsSync(join(tmpDir, "deploy", "rename_users_name_contract.sql"))).toBe(true);
    expect(existsSync(join(tmpDir, "revert", "rename_users_name_contract.sql"))).toBe(true);
    expect(existsSync(join(tmpDir, "verify", "rename_users_name_contract.sql"))).toBe(true);
  });

  it("skips verify files when noVerify is true", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    const result = await generateExpandContract({
      name: "rename_users_name",
      note: "",
      requires: [],
      conflicts: [],
      noVerify: true,
      expand: true,
      table: "public.users",
      oldColumn: "name",
      newColumn: "full_name",
    }, cfg, TEST_ENV);

    expect(result.files.length).toBe(4);
    expect(existsSync(join(tmpDir, "verify", "rename_users_name_expand.sql"))).toBe(false);
    expect(existsSync(join(tmpDir, "verify", "rename_users_name_contract.sql"))).toBe(false);
  });

  it("appends both changes to the plan file", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    await generateExpandContract({
      name: "rename_users_name",
      note: "test",
      requires: [],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "public.users",
      oldColumn: "name",
      newColumn: "full_name",
    }, cfg, TEST_ENV);

    const plan = readFileSync(join(tmpDir, "sqitch.plan"), "utf-8");
    const lines = plan.split("\n").filter(l => !l.startsWith("%") && l.trim() !== "");
    expect(lines.length).toBe(2);
    expect(lines[0]).toMatch(/^rename_users_name_expand\s/);
    expect(lines[1]).toMatch(/^rename_users_name_contract\s/);
  });

  it("contract change depends on expand change in plan", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    await generateExpandContract({
      name: "rename_users_name",
      note: "",
      requires: [],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "public.users",
      oldColumn: "name",
      newColumn: "full_name",
    }, cfg, TEST_ENV);

    const plan = readFileSync(join(tmpDir, "sqitch.plan"), "utf-8");
    const contractLine = plan.split("\n").find(l => l.startsWith("rename_users_name_contract"));
    expect(contractLine).toBeTruthy();
    expect(contractLine).toContain("[rename_users_name_expand]");
  });

  it("expand change inherits user-specified requires", async () => {
    setupProject(tmpDir,
      "%syntax-version=1.0.0\n" +
      "%project=myproject\n" +
      "\n" +
      "create_schema 2024-01-15T10:30:00Z Test User <test@example.com> # schema\n",
    );
    const cfg = testConfig(tmpDir);

    await generateExpandContract({
      name: "rename_users_name",
      note: "",
      requires: ["create_schema"],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "public.users",
      oldColumn: "name",
      newColumn: "full_name",
    }, cfg, TEST_ENV);

    const plan = readFileSync(join(tmpDir, "sqitch.plan"), "utf-8");
    const expandLine = plan.split("\n").find(l => l.startsWith("rename_users_name_expand"));
    expect(expandLine).toBeTruthy();
    expect(expandLine).toContain("[create_schema]");
  });

  it("errors on duplicate expand name", async () => {
    setupProject(tmpDir,
      "%syntax-version=1.0.0\n" +
      "%project=myproject\n" +
      "\n" +
      "rename_users_name_expand 2024-01-15T10:30:00Z Test User <test@example.com> # existing\n",
    );
    const cfg = testConfig(tmpDir);

    await expect(
      generateExpandContract({
        name: "rename_users_name",
        note: "",
        requires: [],
        conflicts: [],
        noVerify: false,
        expand: true,
        table: "public.users",
        oldColumn: "name",
        newColumn: "full_name",
      }, cfg, TEST_ENV),
    ).rejects.toThrow("already exists");
  });

  it("errors on duplicate contract name", async () => {
    setupProject(tmpDir,
      "%syntax-version=1.0.0\n" +
      "%project=myproject\n" +
      "\n" +
      "rename_users_name_contract 2024-01-15T10:30:00Z Test User <test@example.com> # existing\n",
    );
    const cfg = testConfig(tmpDir);

    await expect(
      generateExpandContract({
        name: "rename_users_name",
        note: "",
        requires: [],
        conflicts: [],
        noVerify: false,
        expand: true,
        table: "public.users",
        oldColumn: "name",
        newColumn: "full_name",
      }, cfg, TEST_ENV),
    ).rejects.toThrow("already exists");
  });

  it("errors when plan file is missing", async () => {
    // No plan file setup
    const cfg = testConfig(tmpDir);

    await expect(
      generateExpandContract({
        name: "rename_users_name",
        note: "",
        requires: [],
        conflicts: [],
        noVerify: false,
        expand: true,
        table: "public.users",
        oldColumn: "name",
        newColumn: "full_name",
      }, cfg, TEST_ENV),
    ).rejects.toThrow("plan file not found");
  });

  it("errors when deploy file already exists", async () => {
    setupProject(tmpDir);
    mkdirSync(join(tmpDir, "deploy"), { recursive: true });
    writeFileSync(join(tmpDir, "deploy", "rename_users_name_expand.sql"), "existing", "utf-8");
    const cfg = testConfig(tmpDir);

    await expect(
      generateExpandContract({
        name: "rename_users_name",
        note: "",
        requires: [],
        conflicts: [],
        noVerify: false,
        expand: true,
        table: "public.users",
        oldColumn: "name",
        newColumn: "full_name",
      }, cfg, TEST_ENV),
    ).rejects.toThrow("file already exists");
  });

  it("creates correct deploy script content for expand", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    await generateExpandContract({
      name: "rename_users_name",
      note: "",
      requires: [],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "public.users",
      oldColumn: "name",
      newColumn: "full_name",
      oldType: "text",
    }, cfg, TEST_ENV);

    const deploySql = readFileSync(join(tmpDir, "deploy", "rename_users_name_expand.sql"), "utf-8");
    expect(deploySql).toContain("ALTER TABLE public.users ADD COLUMN full_name text");
    expect(deploySql).toContain("pg_trigger_depth()");
    expect(deploySql).toContain("CREATE TRIGGER sqlever_sync_users_name_full_name");
  });

  it("creates correct deploy script content for contract", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    await generateExpandContract({
      name: "rename_users_name",
      note: "",
      requires: [],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "public.users",
      oldColumn: "name",
      newColumn: "full_name",
    }, cfg, TEST_ENV);

    const deploySql = readFileSync(join(tmpDir, "deploy", "rename_users_name_contract.sql"), "utf-8");
    expect(deploySql).toContain("Backfill incomplete");
    expect(deploySql).toContain("DROP TRIGGER IF EXISTS sqlever_sync_users_name_full_name");
    expect(deploySql).toContain("ALTER TABLE public.users DROP COLUMN name");
  });

  it("handles type change operation with cast expressions", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    await generateExpandContract({
      name: "change_users_age",
      note: "Convert age from text to integer",
      requires: [],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "public.users",
      oldColumn: "age",
      newColumn: "age_int",
      oldType: "text",
      newType: "integer",
      castForward: "NEW.age::integer",
      castReverse: "NEW.age_int::text",
    }, cfg, TEST_ENV);

    const expandSql = readFileSync(join(tmpDir, "deploy", "change_users_age_expand.sql"), "utf-8");
    expect(expandSql).toContain("ADD COLUMN age_int integer");
    expect(expandSql).toContain("(NEW.age::integer)");
    expect(expandSql).toContain("(NEW.age_int::text)");
  });

  it("adds [expand] and [contract] prefixes to notes", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    const result = await generateExpandContract({
      name: "rename_users_name",
      note: "Rename name column",
      requires: [],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "public.users",
      oldColumn: "name",
      newColumn: "full_name",
    }, cfg, TEST_ENV);

    expect(result.expandChange.note).toContain("[expand]");
    expect(result.contractChange.note).toContain("[contract]");
  });

  it("generates auto-notes when no note is provided", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    const result = await generateExpandContract({
      name: "rename_users_name",
      note: "",
      requires: [],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "public.users",
      oldColumn: "name",
      newColumn: "full_name",
    }, cfg, TEST_ENV);

    expect(result.expandChange.note).toContain("[expand]");
    expect(result.expandChange.note).toContain("rename_col");
    expect(result.expandChange.note).toContain("public.users.name");
  });

  it("sets correct parent chain (expand -> contract)", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    const result = await generateExpandContract({
      name: "rename_users_name",
      note: "",
      requires: [],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "public.users",
      oldColumn: "name",
      newColumn: "full_name",
    }, cfg, TEST_ENV);

    // Contract's parent should be expand's change_id
    expect(result.contractChange.parent).toBe(result.expandChange.change_id);
  });

  it("handles schema-qualified table in partitioned table scenario", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    await generateExpandContract({
      name: "rename_orders_status",
      note: "Partitioned table rename",
      requires: [],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "sales.orders",
      oldColumn: "status",
      newColumn: "order_status",
      oldType: "varchar(50)",
    }, cfg, TEST_ENV);

    const expandSql = readFileSync(join(tmpDir, "deploy", "rename_orders_status_expand.sql"), "utf-8");
    // Trigger is installed on parent table (PG 14+ inherits to partitions per SPEC 5.4)
    expect(expandSql).toContain("ON sales.orders");
    expect(expandSql).toContain("ADD COLUMN order_status varchar(50)");

    // Trigger name strips schema
    expect(expandSql).toContain("sqlever_sync_orders_status_order_status");
  });

  it("plan file has correct readback after generation", async () => {
    setupProject(tmpDir);
    const cfg = testConfig(tmpDir);

    await generateExpandContract({
      name: "rename_users_name",
      note: "",
      requires: [],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "public.users",
      oldColumn: "name",
      newColumn: "full_name",
    }, cfg, TEST_ENV);

    const planInfo = readPlanInfo(join(tmpDir, "sqitch.plan"));
    expect(planInfo.existingNames.has("rename_users_name_expand")).toBe(true);
    expect(planInfo.existingNames.has("rename_users_name_contract")).toBe(true);
    expect(planInfo.lastChangeId).toBeTruthy();
  });

  it("creates directories if they do not exist", async () => {
    // Only create plan file, not the dirs
    const planPath = join(tmpDir, "sqitch.plan");
    writeFileSync(planPath, "%syntax-version=1.0.0\n%project=myproject\n\n", "utf-8");
    const cfg = testConfig(tmpDir);

    await generateExpandContract({
      name: "rename_users_name",
      note: "",
      requires: [],
      conflicts: [],
      noVerify: false,
      expand: true,
      table: "public.users",
      oldColumn: "name",
      newColumn: "full_name",
    }, cfg, TEST_ENV);

    expect(existsSync(join(tmpDir, "deploy"))).toBe(true);
    expect(existsSync(join(tmpDir, "revert"))).toBe(true);
    expect(existsSync(join(tmpDir, "verify"))).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// CLI integration (subprocess)
// ---------------------------------------------------------------------------

describe("sqlever add --expand (subprocess)", () => {
  const CWD = import.meta.dir + "/../..";

  beforeEach(() => {
    tmpDir = createTmpDir();
  });

  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true });
  });

  async function runCli(
    ...args: string[]
  ): Promise<{ stdout: string; stderr: string; exitCode: number }> {
    const proc = Bun.spawn(["bun", "run", join(CWD, "src/cli.ts"), ...args], {
      cwd: tmpDir,
      stdout: "pipe",
      stderr: "pipe",
      env: {
        ...process.env,
        SQLEVER_USER_NAME: "CLI Test",
        SQLEVER_USER_EMAIL: "cli@test.com",
      },
    });
    const [stdout, stderr, exitCode] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
      proc.exited,
    ]);
    return { stdout, stderr, exitCode };
  }

  it("creates expand/contract pair via CLI", async () => {
    setupProject(tmpDir);

    const { stdout, exitCode } = await runCli(
      "add", "rename_users_name", "--expand",
      "--table", "public.users",
      "--old-column", "name",
      "--new-column", "full_name",
      "--old-type", "text",
      "-n", "Rename column",
    );
    expect(exitCode).toBe(0);
    expect(stdout).toContain("expand/contract pair");
    expect(existsSync(join(tmpDir, "deploy", "rename_users_name_expand.sql"))).toBe(true);
    expect(existsSync(join(tmpDir, "deploy", "rename_users_name_contract.sql"))).toBe(true);
  });

  it("exits 1 when --table is missing", async () => {
    setupProject(tmpDir);

    const { exitCode, stderr } = await runCli(
      "add", "rename_col", "--expand",
      "--old-column", "a",
      "--new-column", "b",
    );
    expect(exitCode).toBe(1);
    expect(stderr).toContain("--table is required");
  });
});
