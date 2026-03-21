// tests/unit/sync-triggers.test.ts — Tests for bidirectional sync trigger generation
//
// Validates: bidirectional sync, recursion guard, type conversions,
// NULL handling, defaults, partitioned tables, lock behavior,
// CREATE/DROP SQL correctness, validation, and edge cases.
//
// Covers SPEC Section 5.4 trigger edge cases 1-6.

import { describe, expect, it } from "bun:test";

import {
  forwardSyncExpression,
  reverseSyncExpression,
  generateTriggerFunctionBody,
  generateCreateFunction,
  generateCreateTrigger,
  generateCreateSQL,
  generateDropTrigger,
  generateDropFunction,
  generateDropSQL,
  generateSyncTrigger,
  configToTriggerOptions,
  validateTriggerOptions,
  generateSyncTriggerSafe,
  type SyncTriggerOptions,
} from "../../src/expand-contract/triggers";

import type { ExpandContractConfig } from "../../src/expand-contract/generator";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Standard rename scenario: public.users.name -> full_name. */
function renameOpts(overrides?: Partial<SyncTriggerOptions>): SyncTriggerOptions {
  return {
    table: "public.users",
    oldColumn: "name",
    newColumn: "full_name",
    oldType: "text",
    newType: "text",
    ...overrides,
  };
}

/** Type-change scenario: text -> integer with casts. */
function typeChangeOpts(overrides?: Partial<SyncTriggerOptions>): SyncTriggerOptions {
  return {
    table: "public.users",
    oldColumn: "age_text",
    newColumn: "age",
    oldType: "text",
    newType: "integer",
    castForward: "NEW.age_text::integer",
    castReverse: "NEW.age::text",
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// 1. Bidirectional sync — forward expression (old -> new)
// ---------------------------------------------------------------------------

describe("forwardSyncExpression", () => {
  it("uses direct column reference when no cast is provided", () => {
    const expr = forwardSyncExpression(renameOpts());
    expect(expr).toBe("NEW.name");
  });

  it("wraps castForward expression in parentheses", () => {
    const expr = forwardSyncExpression(typeChangeOpts());
    expect(expr).toBe("(NEW.age_text::integer)");
  });

  it("applies COALESCE with newDefault when provided", () => {
    const expr = forwardSyncExpression(renameOpts({ newDefault: "'unknown'" }));
    expect(expr).toBe("COALESCE(NEW.name, 'unknown')");
  });

  it("applies COALESCE with newDefault and cast together", () => {
    const expr = forwardSyncExpression(typeChangeOpts({ newDefault: "0" }));
    expect(expr).toBe("COALESCE((NEW.age_text::integer), 0)");
  });
});

// ---------------------------------------------------------------------------
// 2. Bidirectional sync — reverse expression (new -> old)
// ---------------------------------------------------------------------------

describe("reverseSyncExpression", () => {
  it("uses direct column reference when no cast is provided", () => {
    const expr = reverseSyncExpression(renameOpts());
    expect(expr).toBe("NEW.full_name");
  });

  it("wraps castReverse expression in parentheses", () => {
    const expr = reverseSyncExpression(typeChangeOpts());
    expect(expr).toBe("(NEW.age::text)");
  });

  it("applies COALESCE with oldDefault when provided", () => {
    const expr = reverseSyncExpression(renameOpts({ oldDefault: "'N/A'" }));
    expect(expr).toBe("COALESCE(NEW.full_name, 'N/A')");
  });

  it("applies COALESCE with oldDefault and cast together", () => {
    const expr = reverseSyncExpression(typeChangeOpts({ oldDefault: "'0'" }));
    expect(expr).toBe("COALESCE((NEW.age::text), '0')");
  });
});

// ---------------------------------------------------------------------------
// 3. Recursion guard (SPEC 5.4, point 1)
// ---------------------------------------------------------------------------

describe("recursion guard", () => {
  it("uses pg_trigger_depth() < 2 in the function body", () => {
    const body = generateTriggerFunctionBody(renameOpts());
    expect(body).toContain("pg_trigger_depth() < 2");
  });

  it("documents the recursion guard rationale in comments", () => {
    const body = generateTriggerFunctionBody(renameOpts());
    expect(body).toContain("Recursion guard");
    expect(body).toContain("depth >= 2");
  });

  it("does NOT use SET LOCAL approach (SPEC 5.4 explicitly rejects it)", () => {
    const body = generateTriggerFunctionBody(renameOpts());
    expect(body).not.toContain("SET LOCAL");
    expect(body).not.toContain("sqlever.syncing");
  });
});

// ---------------------------------------------------------------------------
// 4. Trigger function body — bidirectional sync logic
// ---------------------------------------------------------------------------

describe("generateTriggerFunctionBody", () => {
  it("handles INSERT with forward sync (old -> new)", () => {
    const body = generateTriggerFunctionBody(renameOpts());
    // On INSERT: if new column is NULL, sync from old
    expect(body).toContain("IF NEW.full_name IS NULL THEN");
    expect(body).toContain("NEW.full_name := NEW.name");
  });

  it("handles INSERT with reverse sync (new -> old)", () => {
    const body = generateTriggerFunctionBody(renameOpts());
    // On INSERT: if old column is NULL and new is set, sync from new
    expect(body).toContain("IF NEW.name IS NULL AND NEW.full_name IS NOT NULL THEN");
    expect(body).toContain("NEW.name := NEW.full_name");
  });

  it("handles UPDATE with IS DISTINCT FROM for old column", () => {
    const body = generateTriggerFunctionBody(renameOpts());
    expect(body).toContain("IF NEW.name IS DISTINCT FROM OLD.name THEN");
    expect(body).toContain("NEW.full_name :=");
  });

  it("handles UPDATE with IS DISTINCT FROM for new column", () => {
    const body = generateTriggerFunctionBody(renameOpts());
    expect(body).toContain("ELSIF NEW.full_name IS DISTINCT FROM OLD.full_name THEN");
    expect(body).toContain("NEW.name :=");
  });

  it("returns NEW at the end", () => {
    const body = generateTriggerFunctionBody(renameOpts());
    expect(body).toContain("RETURN NEW;");
  });

  it("includes type cast expressions in the sync assignments", () => {
    const body = generateTriggerFunctionBody(typeChangeOpts());
    expect(body).toContain("NEW.age := (NEW.age_text::integer)");
    expect(body).toContain("NEW.age_text := (NEW.age::text)");
  });
});

// ---------------------------------------------------------------------------
// 5. Type conversions (castForward / castReverse)
// ---------------------------------------------------------------------------

describe("type conversions", () => {
  it("generates correct forward cast in CREATE FUNCTION", () => {
    const sql = generateCreateFunction(typeChangeOpts());
    expect(sql).toContain("(NEW.age_text::integer)");
  });

  it("generates correct reverse cast in CREATE FUNCTION", () => {
    const sql = generateCreateFunction(typeChangeOpts());
    expect(sql).toContain("(NEW.age::text)");
  });

  it("handles complex cast expressions (e.g., trim + cast)", () => {
    const opts = renameOpts({
      castForward: "TRIM(NEW.name)::varchar(100)",
      castReverse: "NEW.full_name::text",
    });
    const body = generateTriggerFunctionBody(opts);
    expect(body).toContain("(TRIM(NEW.name)::varchar(100))");
    expect(body).toContain("(NEW.full_name::text)");
  });

  it("works without any casts (identity sync)", () => {
    const opts = renameOpts({ castForward: undefined, castReverse: undefined });
    const body = generateTriggerFunctionBody(opts);
    // Direct column reference without cast
    expect(body).toContain("NEW.full_name := NEW.name");
    expect(body).toContain("NEW.name := NEW.full_name");
  });
});

// ---------------------------------------------------------------------------
// 6. NULL semantics
// ---------------------------------------------------------------------------

describe("NULL handling", () => {
  it("uses IS NULL check on INSERT for forward sync", () => {
    const body = generateTriggerFunctionBody(renameOpts());
    expect(body).toContain("IF NEW.full_name IS NULL THEN");
  });

  it("uses IS NOT NULL check on INSERT for reverse sync", () => {
    const body = generateTriggerFunctionBody(renameOpts());
    expect(body).toContain("NEW.full_name IS NOT NULL");
  });

  it("uses IS DISTINCT FROM on UPDATE (handles NULL correctly)", () => {
    const body = generateTriggerFunctionBody(renameOpts());
    // IS DISTINCT FROM handles NULL <> NULL correctly (unlike != or <>)
    expect(body).toContain("IS DISTINCT FROM OLD.name");
    expect(body).toContain("IS DISTINCT FROM OLD.full_name");
  });

  it("applies COALESCE with defaults when provided", () => {
    const opts = renameOpts({
      newDefault: "'default_value'",
      oldDefault: "''",
    });
    const body = generateTriggerFunctionBody(opts);
    expect(body).toContain("COALESCE(NEW.name, 'default_value')");
    expect(body).toContain("COALESCE(NEW.full_name, '')");
  });
});

// ---------------------------------------------------------------------------
// 7. CREATE TRIGGER SQL
// ---------------------------------------------------------------------------

describe("generateCreateTrigger", () => {
  it("uses sqlever_sync_ prefix for trigger name", () => {
    const sql = generateCreateTrigger(renameOpts());
    expect(sql).toContain("CREATE TRIGGER sqlever_sync_users_name_full_name");
  });

  it("creates a BEFORE INSERT OR UPDATE trigger", () => {
    const sql = generateCreateTrigger(renameOpts());
    expect(sql).toContain("BEFORE INSERT OR UPDATE ON public.users");
  });

  it("uses FOR EACH ROW", () => {
    const sql = generateCreateTrigger(renameOpts());
    expect(sql).toContain("FOR EACH ROW");
  });

  it("references the correct function name", () => {
    const sql = generateCreateTrigger(renameOpts());
    expect(sql).toContain("EXECUTE FUNCTION sqlever_sync_fn_users_name_full_name()");
  });
});

// ---------------------------------------------------------------------------
// 8. CREATE FUNCTION SQL
// ---------------------------------------------------------------------------

describe("generateCreateFunction", () => {
  it("uses CREATE OR REPLACE FUNCTION", () => {
    const sql = generateCreateFunction(renameOpts());
    expect(sql).toContain("CREATE OR REPLACE FUNCTION sqlever_sync_fn_users_name_full_name()");
  });

  it("returns trigger type", () => {
    const sql = generateCreateFunction(renameOpts());
    expect(sql).toContain("RETURNS trigger");
  });

  it("uses plpgsql language", () => {
    const sql = generateCreateFunction(renameOpts());
    expect(sql).toContain("LANGUAGE plpgsql");
  });
});

// ---------------------------------------------------------------------------
// 9. DROP TRIGGER / DROP FUNCTION SQL
// ---------------------------------------------------------------------------

describe("generateDropTrigger", () => {
  it("generates DROP TRIGGER IF EXISTS on the correct table", () => {
    const sql = generateDropTrigger(renameOpts());
    expect(sql).toBe("DROP TRIGGER IF EXISTS sqlever_sync_users_name_full_name ON public.users;");
  });
});

describe("generateDropFunction", () => {
  it("generates DROP FUNCTION IF EXISTS", () => {
    const sql = generateDropFunction(renameOpts());
    expect(sql).toBe("DROP FUNCTION IF EXISTS sqlever_sync_fn_users_name_full_name();");
  });
});

describe("generateDropSQL", () => {
  it("drops trigger before function (correct order)", () => {
    const sql = generateDropSQL(renameOpts());
    const trigIdx = sql.indexOf("DROP TRIGGER");
    const fnIdx = sql.indexOf("DROP FUNCTION");
    expect(trigIdx).toBeLessThan(fnIdx);
  });

  it("contains both DROP statements", () => {
    const sql = generateDropSQL(renameOpts());
    expect(sql).toContain("DROP TRIGGER IF EXISTS");
    expect(sql).toContain("DROP FUNCTION IF EXISTS");
  });
});

// ---------------------------------------------------------------------------
// 10. Combined generateCreateSQL
// ---------------------------------------------------------------------------

describe("generateCreateSQL", () => {
  it("contains both function and trigger creation", () => {
    const sql = generateCreateSQL(renameOpts());
    expect(sql).toContain("CREATE OR REPLACE FUNCTION");
    expect(sql).toContain("CREATE TRIGGER");
  });

  it("function appears before trigger", () => {
    const sql = generateCreateSQL(renameOpts());
    const fnIdx = sql.indexOf("CREATE OR REPLACE FUNCTION");
    const trigIdx = sql.indexOf("CREATE TRIGGER");
    expect(fnIdx).toBeLessThan(trigIdx);
  });
});

// ---------------------------------------------------------------------------
// 11. Partitioned tables (SPEC 5.4, point 3)
// ---------------------------------------------------------------------------

describe("partitioned tables", () => {
  it("installs trigger on the parent table (PG 14+ inherits to partitions)", () => {
    const opts = renameOpts({ table: "sales.orders_partitioned" });
    const sql = generateCreateTrigger(opts);
    // Trigger is on the parent — PG 14+ automatically inherits to partitions
    expect(sql).toContain("ON sales.orders_partitioned");
  });

  it("strips schema from trigger name for partitioned tables", () => {
    const opts = renameOpts({ table: "sales.orders_partitioned" });
    const sql = generateCreateTrigger(opts);
    expect(sql).toContain("sqlever_sync_orders_partitioned_name_full_name");
    expect(sql).not.toContain("sqlever_sync_sales.");
  });

  it("generates DROP for the parent table", () => {
    const opts = renameOpts({ table: "analytics.events" });
    const sql = generateDropTrigger(opts);
    expect(sql).toContain("ON analytics.events");
  });
});

// ---------------------------------------------------------------------------
// 12. Lock behavior (SPEC 5.4, point 5)
// ---------------------------------------------------------------------------

describe("lock_timeout for trigger installation", () => {
  it("wraps CREATE SQL with SET lock_timeout when lockTimeout is provided", () => {
    const opts = renameOpts({ lockTimeout: "5s" });
    const sql = generateCreateSQL(opts);
    expect(sql).toContain("SET lock_timeout = '5s'");
    expect(sql).toContain("RESET lock_timeout");
  });

  it("includes documentation comment about AccessExclusiveLock", () => {
    const opts = renameOpts({ lockTimeout: "1s" });
    const sql = generateCreateSQL(opts);
    expect(sql).toContain("AccessExclusiveLock");
  });

  it("does NOT include lock_timeout when not specified", () => {
    const sql = generateCreateSQL(renameOpts());
    expect(sql).not.toContain("lock_timeout");
    expect(sql).not.toContain("RESET lock_timeout");
  });

  it("SET lock_timeout appears before CREATE TRIGGER statement", () => {
    const opts = renameOpts({ lockTimeout: "3s" });
    const sql = generateCreateSQL(opts);
    const lockIdx = sql.indexOf("SET lock_timeout");
    // Find the actual CREATE TRIGGER statement (not the comment)
    const trigIdx = sql.indexOf("\nCREATE TRIGGER");
    expect(lockIdx).toBeLessThan(trigIdx);
  });

  it("RESET lock_timeout appears after CREATE TRIGGER statement", () => {
    const opts = renameOpts({ lockTimeout: "3s" });
    const sql = generateCreateSQL(opts);
    const trigIdx = sql.indexOf("\nCREATE TRIGGER");
    const resetIdx = sql.indexOf("RESET lock_timeout");
    expect(trigIdx).toBeLessThan(resetIdx);
  });
});

// ---------------------------------------------------------------------------
// 13. Combined generateSyncTrigger
// ---------------------------------------------------------------------------

describe("generateSyncTrigger", () => {
  it("returns createSQL and dropSQL", () => {
    const result = generateSyncTrigger(renameOpts());
    expect(result.createSQL).toContain("CREATE OR REPLACE FUNCTION");
    expect(result.createSQL).toContain("CREATE TRIGGER");
    expect(result.dropSQL).toContain("DROP TRIGGER");
    expect(result.dropSQL).toContain("DROP FUNCTION");
  });

  it("returns correct trigger and function names", () => {
    const result = generateSyncTrigger(renameOpts());
    expect(result.triggerName).toBe("sqlever_sync_users_name_full_name");
    expect(result.functionName).toBe("sqlever_sync_fn_users_name_full_name");
  });

  it("trigger name starts with sqlever_sync_ prefix (required by recursion guard)", () => {
    const result = generateSyncTrigger(renameOpts());
    expect(result.triggerName.startsWith("sqlever_sync_")).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// 14. configToTriggerOptions — bridge from generator.ts
// ---------------------------------------------------------------------------

describe("configToTriggerOptions", () => {
  it("converts ExpandContractConfig to SyncTriggerOptions", () => {
    const config: ExpandContractConfig = {
      name: "rename_users_name",
      operation: "rename_col",
      table: "public.users",
      oldColumn: "name",
      newColumn: "full_name",
      oldType: "text",
      newType: "text",
      note: "test",
      requires: [],
      conflicts: [],
    };

    const opts = configToTriggerOptions(config);
    expect(opts.table).toBe("public.users");
    expect(opts.oldColumn).toBe("name");
    expect(opts.newColumn).toBe("full_name");
    expect(opts.oldType).toBe("text");
    expect(opts.newType).toBe("text");
  });

  it("passes through cast expressions", () => {
    const config: ExpandContractConfig = {
      name: "change_type",
      operation: "change_type",
      table: "public.t",
      oldColumn: "a",
      newColumn: "b",
      castForward: "NEW.a::int",
      castReverse: "NEW.b::text",
      note: "",
      requires: [],
      conflicts: [],
    };

    const opts = configToTriggerOptions(config);
    expect(opts.castForward).toBe("NEW.a::int");
    expect(opts.castReverse).toBe("NEW.b::text");
  });
});

// ---------------------------------------------------------------------------
// 15. Validation
// ---------------------------------------------------------------------------

describe("validateTriggerOptions", () => {
  it("returns null for valid options", () => {
    expect(validateTriggerOptions(renameOpts())).toBeNull();
  });

  it("rejects missing table", () => {
    const err = validateTriggerOptions(renameOpts({ table: "" }));
    expect(err).toContain("table is required");
  });

  it("rejects missing oldColumn", () => {
    const err = validateTriggerOptions(renameOpts({ oldColumn: "" }));
    expect(err).toContain("oldColumn is required");
  });

  it("rejects missing newColumn", () => {
    const err = validateTriggerOptions(renameOpts({ newColumn: "" }));
    expect(err).toContain("newColumn is required");
  });

  it("rejects same old and new column", () => {
    const err = validateTriggerOptions(renameOpts({ oldColumn: "x", newColumn: "x" }));
    expect(err).toContain("must be different");
  });

  it("rejects invalid table name (SQL injection attempt)", () => {
    const err = validateTriggerOptions(renameOpts({ table: "users; DROP TABLE" }));
    expect(err).toContain("invalid table name");
  });

  it("rejects invalid column name", () => {
    const err = validateTriggerOptions(renameOpts({ oldColumn: "col;--" }));
    expect(err).toContain("invalid column name");
  });

  it("accepts schema-qualified table names", () => {
    const err = validateTriggerOptions(renameOpts({ table: "my_schema.my_table" }));
    expect(err).toBeNull();
  });

  it("accepts underscored identifiers", () => {
    const err = validateTriggerOptions(renameOpts({
      table: "public.user_accounts",
      oldColumn: "first_name",
      newColumn: "given_name",
    }));
    expect(err).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// 16. generateSyncTriggerSafe — validated generation
// ---------------------------------------------------------------------------

describe("generateSyncTriggerSafe", () => {
  it("returns TriggerSQL for valid options", () => {
    const result = generateSyncTriggerSafe(renameOpts());
    expect(result.triggerName).toBe("sqlever_sync_users_name_full_name");
    expect(result.createSQL).toContain("CREATE TRIGGER");
  });

  it("throws on invalid options", () => {
    expect(() =>
      generateSyncTriggerSafe(renameOpts({ table: "" })),
    ).toThrow("Invalid sync trigger options");
  });

  it("throws with specific validation message", () => {
    expect(() =>
      generateSyncTriggerSafe(renameOpts({ oldColumn: "x", newColumn: "x" })),
    ).toThrow("must be different");
  });
});

// ---------------------------------------------------------------------------
// 17. Edge case: unqualified table names
// ---------------------------------------------------------------------------

describe("unqualified table names", () => {
  it("generates valid SQL for tables without schema prefix", () => {
    const opts = renameOpts({ table: "users" });
    const result = generateSyncTrigger(opts);
    expect(result.triggerName).toBe("sqlever_sync_users_name_full_name");
    expect(result.createSQL).toContain("ON users");
    expect(result.dropSQL).toContain("ON users");
  });
});

// ---------------------------------------------------------------------------
// 18. Multiple sync triggers on different tables in one transaction
//     (SPEC 5.4, point 1: pg_trigger_depth() allows independent firing)
// ---------------------------------------------------------------------------

describe("multiple independent sync triggers", () => {
  it("generates distinct trigger names for different tables", () => {
    const t1 = generateSyncTrigger(renameOpts({ table: "public.users" }));
    const t2 = generateSyncTrigger(renameOpts({ table: "public.accounts" }));
    expect(t1.triggerName).not.toBe(t2.triggerName);
    expect(t1.functionName).not.toBe(t2.functionName);
  });

  it("generates distinct trigger names for different column pairs on same table", () => {
    const t1 = generateSyncTrigger(renameOpts({
      oldColumn: "name",
      newColumn: "full_name",
    }));
    const t2 = generateSyncTrigger(renameOpts({
      oldColumn: "email",
      newColumn: "contact_email",
    }));
    expect(t1.triggerName).not.toBe(t2.triggerName);
    expect(t1.functionName).not.toBe(t2.functionName);
  });

  it("all trigger names use sqlever_sync_ prefix for recursion guard scoping", () => {
    const triggers = [
      generateSyncTrigger(renameOpts({ table: "public.users" })),
      generateSyncTrigger(renameOpts({ table: "public.orders", oldColumn: "status", newColumn: "order_status" })),
      generateSyncTrigger(typeChangeOpts()),
    ];
    for (const t of triggers) {
      expect(t.triggerName.startsWith("sqlever_sync_")).toBe(true);
    }
  });
});

// ---------------------------------------------------------------------------
// 19. Defaults with type conversion
// ---------------------------------------------------------------------------

describe("defaults with type conversion", () => {
  it("combines cast and default in forward direction", () => {
    const opts: SyncTriggerOptions = {
      table: "public.products",
      oldColumn: "price_text",
      newColumn: "price",
      castForward: "NEW.price_text::numeric",
      newDefault: "0.0",
    };
    const body = generateTriggerFunctionBody(opts);
    expect(body).toContain("COALESCE((NEW.price_text::numeric), 0.0)");
  });

  it("combines cast and default in reverse direction", () => {
    const opts: SyncTriggerOptions = {
      table: "public.products",
      oldColumn: "price_text",
      newColumn: "price",
      castReverse: "NEW.price::text",
      oldDefault: "'0'",
    };
    const body = generateTriggerFunctionBody(opts);
    expect(body).toContain("COALESCE((NEW.price::text), '0')");
  });
});

// ---------------------------------------------------------------------------
// 20. Full round-trip: generateSyncTrigger produces valid SQL structure
// ---------------------------------------------------------------------------

describe("full round-trip SQL structure", () => {
  it("createSQL for rename produces all required clauses", () => {
    const result = generateSyncTrigger(renameOpts());
    const sql = result.createSQL;
    // Function
    expect(sql).toContain("CREATE OR REPLACE FUNCTION");
    expect(sql).toContain("RETURNS trigger");
    expect(sql).toContain("LANGUAGE plpgsql");
    expect(sql).toContain("$$");
    // Recursion guard
    expect(sql).toContain("pg_trigger_depth() < 2");
    // INSERT handling
    expect(sql).toContain("TG_OP = 'INSERT'");
    // UPDATE handling
    expect(sql).toContain("TG_OP = 'UPDATE'");
    // IS DISTINCT FROM for NULL-safe comparison
    expect(sql).toContain("IS DISTINCT FROM");
    // Trigger
    expect(sql).toContain("CREATE TRIGGER sqlever_sync_");
    expect(sql).toContain("BEFORE INSERT OR UPDATE");
    expect(sql).toContain("FOR EACH ROW");
    expect(sql).toContain("EXECUTE FUNCTION");
  });

  it("createSQL for type change includes cast expressions", () => {
    const result = generateSyncTrigger(typeChangeOpts());
    const sql = result.createSQL;
    expect(sql).toContain("NEW.age_text::integer");
    expect(sql).toContain("NEW.age::text");
  });

  it("dropSQL contains correct order and identifiers", () => {
    const result = generateSyncTrigger(renameOpts());
    const sql = result.dropSQL;
    expect(sql).toContain("DROP TRIGGER IF EXISTS sqlever_sync_users_name_full_name ON public.users;");
    expect(sql).toContain("DROP FUNCTION IF EXISTS sqlever_sync_fn_users_name_full_name();");
  });

  it("createSQL with lockTimeout wraps correctly", () => {
    const result = generateSyncTrigger(renameOpts({ lockTimeout: "2s" }));
    const sql = result.createSQL;
    // Order: SET lock_timeout -> CREATE FUNCTION -> CREATE TRIGGER -> RESET
    const parts = [
      sql.indexOf("SET lock_timeout"),
      sql.indexOf("CREATE OR REPLACE FUNCTION"),
      sql.indexOf("\nCREATE TRIGGER"),
      sql.indexOf("RESET lock_timeout"),
    ];
    for (let i = 0; i < parts.length - 1; i++) {
      expect(parts[i]!).toBeLessThan(parts[i + 1]!);
    }
  });
});

// ---------------------------------------------------------------------------
// 21. Logical replication documentation (SPEC 5.4, point 2)
// ---------------------------------------------------------------------------

describe("logical replication awareness", () => {
  it("module-level documentation mentions logical replication limitation", async () => {
    // Read the source file to verify documentation exists
    const fs = await import("node:fs");
    const source = fs.readFileSync(
      new URL("../../src/expand-contract/triggers.ts", import.meta.url).pathname,
      "utf-8",
    );
    expect(source).toContain("logical replication");
    expect(source).toContain("do not fire on logical replication subscribers");
  });
});

// ---------------------------------------------------------------------------
// 22. COPY performance documentation (SPEC 5.4, point 4)
// ---------------------------------------------------------------------------

describe("COPY performance awareness", () => {
  it("module-level documentation mentions COPY performance impact", async () => {
    const fs = await import("node:fs");
    const source = fs.readFileSync(
      new URL("../../src/expand-contract/triggers.ts", import.meta.url).pathname,
      "utf-8",
    );
    expect(source).toContain("COPY");
    expect(source).toContain("bulk load performance");
  });
});
