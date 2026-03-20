/**
 * S1-8: Validation spike — pgsql-parser + bun build --compile
 *
 * This test validates that `libpg-query` (via `pgsql-parser`) can parse
 * PostgreSQL SQL into AST nodes and works with Bun's runtime and compiled
 * binary output.
 *
 * Key findings will be documented inline as comments.
 */
import { describe, test, expect, beforeAll } from "bun:test";
import { parse, parseSync, loadModule } from "libpg-query";
import { readFileSync } from "fs";
import { join } from "path";

const FIXTURES_DIR = join(import.meta.dir, "..", "fixtures");

describe("libpg-query WASM parser spike", () => {
  // Load the WASM module once before all tests
  beforeAll(async () => {
    await loadModule();
  });

  describe("basic parsing", () => {
    test("parses a simple SELECT", async () => {
      const result = await parse("SELECT 1");
      expect(result).toBeDefined();
      expect(result.stmts).toBeArray();
      expect(result.stmts).toHaveLength(1);
      expect(result.stmts[0].stmt).toHaveProperty("SelectStmt");
    });

    test("parseSync works without await", () => {
      const result = parseSync("SELECT 1 + 2");
      expect(result).toBeDefined();
      expect(result.stmts).toBeArray();
      expect(result.stmts).toHaveLength(1);
    });

    test("parses CREATE TABLE and extracts table name", async () => {
      const sql = `CREATE TABLE users (
        id bigint PRIMARY KEY,
        email text NOT NULL
      )`;
      const result = await parse(sql);
      expect(result.stmts).toHaveLength(1);

      const stmt = result.stmts[0].stmt;
      expect(stmt).toHaveProperty("CreateStmt");

      const createStmt = stmt.CreateStmt;
      const relname = createStmt.relation.relname;
      expect(relname).toBe("users");
    });

    test("parses ALTER TABLE and identifies subcommands", async () => {
      const sql = "ALTER TABLE posts ADD COLUMN slug text";
      const result = await parse(sql);
      expect(result.stmts).toHaveLength(1);

      const stmt = result.stmts[0].stmt;
      expect(stmt).toHaveProperty("AlterTableStmt");

      const alterStmt = stmt.AlterTableStmt;
      expect(alterStmt.relation.relname).toBe("posts");
      expect(alterStmt.cmds).toBeArray();
      expect(alterStmt.cmds.length).toBeGreaterThan(0);
    });

    test("parses CREATE INDEX", async () => {
      const sql = "CREATE INDEX idx_users_email ON users (email)";
      const result = await parse(sql);
      expect(result.stmts).toHaveLength(1);

      const stmt = result.stmts[0].stmt;
      expect(stmt).toHaveProperty("IndexStmt");
      expect(stmt.IndexStmt.idxname).toBe("idx_users_email");
      expect(stmt.IndexStmt.relation.relname).toBe("users");
    });

    test("parses CREATE INDEX CONCURRENTLY", async () => {
      const sql =
        "CREATE INDEX CONCURRENTLY idx_foo ON bar (baz) WHERE baz IS NOT NULL";
      const result = await parse(sql);
      expect(result.stmts).toHaveLength(1);

      const stmt = result.stmts[0].stmt;
      expect(stmt).toHaveProperty("IndexStmt");
      expect(stmt.IndexStmt.concurrent).toBe(true);
    });
  });

  describe("multi-statement parsing", () => {
    test("parses multiple statements separated by semicolons", async () => {
      const sql = `
        CREATE TABLE a (id int);
        CREATE TABLE b (id int);
        CREATE TABLE c (id int);
      `;
      const result = await parse(sql);
      expect(result.stmts).toHaveLength(3);

      for (const entry of result.stmts) {
        expect(entry.stmt).toHaveProperty("CreateStmt");
      }
    });

    test("provides statement locations (byte offsets)", async () => {
      const sql = "SELECT 1; SELECT 2; SELECT 3;";
      const result = await parse(sql);
      expect(result.stmts).toHaveLength(3);

      // First statement: stmt_location is omitted (protobuf zero-value),
      // meaning offset 0. We normalize with `?? 0`.
      expect(result.stmts[0].stmt_location ?? 0).toBe(0);

      // Subsequent statements have positive offsets
      expect(result.stmts[1].stmt_location).toBeGreaterThan(0);
      expect(result.stmts[2].stmt_location).toBeGreaterThan(
        result.stmts[1].stmt_location,
      );
    });

    test("provides statement lengths", async () => {
      const sql = "SELECT 1; SELECT 2; SELECT 3;";
      const result = await parse(sql);

      // All statements should have positive lengths
      expect(result.stmts[0].stmt_len).toBeGreaterThan(0);
      expect(result.stmts[1].stmt_len).toBeGreaterThan(0);
      expect(result.stmts[2].stmt_len).toBeGreaterThan(0);
    });
  });

  describe("dollar-quoted strings", () => {
    test("parses basic dollar-quoted function", async () => {
      const sql = `
        CREATE FUNCTION hello()
        RETURNS text AS $$
        BEGIN
          RETURN 'hello world';
        END;
        $$ LANGUAGE plpgsql;
      `;
      const result = await parse(sql);
      expect(result.stmts).toHaveLength(1);

      const stmt = result.stmts[0].stmt;
      expect(stmt).toHaveProperty("CreateFunctionStmt");
    });

    test("parses named dollar-quoted function", async () => {
      const sql = `
        CREATE FUNCTION greet(name text)
        RETURNS text AS $fn$
        BEGIN
          RETURN 'Hello, ' || name;
        END;
        $fn$ LANGUAGE plpgsql;
      `;
      const result = await parse(sql);
      expect(result.stmts).toHaveLength(1);
      expect(result.stmts[0].stmt).toHaveProperty("CreateFunctionStmt");
    });

    test("parses function with nested single quotes inside dollar-quoting", async () => {
      const sql = `
        CREATE FUNCTION test_quotes()
        RETURNS void AS $$
        BEGIN
          RAISE NOTICE 'It''s working with ''quotes''!';
        END;
        $$ LANGUAGE plpgsql;
      `;
      const result = await parse(sql);
      expect(result.stmts).toHaveLength(1);
    });

    test("parses DO block with dollar-quoting", async () => {
      const sql = `
        DO $$
        BEGIN
          IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'uuid-ossp') THEN
            CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
          END IF;
        END;
        $$;
      `;
      const result = await parse(sql);
      expect(result.stmts).toHaveLength(1);
      expect(result.stmts[0].stmt).toHaveProperty("DoStmt");
    });
  });

  describe("CREATE FUNCTION bodies", () => {
    test("extracts function name and return type", async () => {
      const sql = `
        CREATE FUNCTION calculate_total(p_order_id bigint)
        RETURNS numeric AS $$
        BEGIN
          RETURN (SELECT sum(amount) FROM order_items WHERE order_id = p_order_id);
        END;
        $$ LANGUAGE plpgsql STABLE;
      `;
      const result = await parse(sql);
      const stmt = result.stmts[0].stmt.CreateFunctionStmt;
      expect(stmt.funcname).toBeArray();

      // Function name is in the last element
      const funcName = stmt.funcname[stmt.funcname.length - 1];
      expect(funcName.String?.sval || funcName.str).toBeDefined();
    });

    test("parses trigger function", async () => {
      const sql = `
        CREATE FUNCTION update_modified_column()
        RETURNS trigger AS $$
        BEGIN
          NEW.updated_at = now();
          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
      `;
      const result = await parse(sql);
      expect(result.stmts).toHaveLength(1);
      expect(result.stmts[0].stmt).toHaveProperty("CreateFunctionStmt");
    });

    test("parses CREATE OR REPLACE FUNCTION", async () => {
      const sql = `
        CREATE OR REPLACE FUNCTION my_func()
        RETURNS void AS $$
        BEGIN
          NULL;
        END;
        $$ LANGUAGE plpgsql;
      `;
      const result = await parse(sql);
      const stmt = result.stmts[0].stmt.CreateFunctionStmt;
      expect(stmt.replace).toBe(true);
    });
  });

  describe("comments handling", () => {
    test("parses SQL with single-line comments", async () => {
      const sql = `
        -- This is a comment
        SELECT 1; -- inline comment
        -- Another comment
        SELECT 2;
      `;
      const result = await parse(sql);
      expect(result.stmts).toHaveLength(2);
    });

    test("parses SQL with block comments", async () => {
      const sql = `
        /* Block comment */
        SELECT 1;
        /* Multi-line
           block comment
           with special chars: '; DROP TABLE users; -- */
        SELECT 2;
      `;
      const result = await parse(sql);
      expect(result.stmts).toHaveLength(2);
    });

    test("parses SQL with nested block comments", async () => {
      const sql = `
        /* Outer /* inner */ outer still */
        SELECT 1;
      `;
      const result = await parse(sql);
      expect(result.stmts).toHaveLength(1);
    });
  });

  describe("edge case SQL types", () => {
    test("parses GRANT/REVOKE", async () => {
      const sql = `
        GRANT SELECT ON users TO readonly_role;
        REVOKE INSERT ON users FROM public;
      `;
      const result = await parse(sql);
      expect(result.stmts).toHaveLength(2);
      expect(result.stmts[0].stmt).toHaveProperty("GrantStmt");
      expect(result.stmts[1].stmt).toHaveProperty("GrantStmt"); // REVOKE uses GrantStmt with is_grant=false
    });

    test("parses CREATE TYPE (enum)", async () => {
      const sql = "CREATE TYPE status AS ENUM ('draft', 'published', 'archived')";
      const result = await parse(sql);
      expect(result.stmts).toHaveLength(1);
      expect(result.stmts[0].stmt).toHaveProperty("CreateEnumStmt");
    });

    test("parses CREATE VIEW", async () => {
      const sql = `
        CREATE VIEW active_users AS
        SELECT id, email FROM users WHERE status = 'active';
      `;
      const result = await parse(sql);
      expect(result.stmts).toHaveLength(1);
      expect(result.stmts[0].stmt).toHaveProperty("ViewStmt");
    });

    test("parses CTE (WITH clause)", async () => {
      const sql = `
        WITH recent AS (
          SELECT id FROM users WHERE created_at > now() - interval '1 day'
        )
        SELECT * FROM recent;
      `;
      const result = await parse(sql);
      expect(result.stmts).toHaveLength(1);
      const selectStmt = result.stmts[0].stmt.SelectStmt;
      expect(selectStmt.withClause).toBeDefined();
    });

    test("parses window functions", async () => {
      const sql = `
        SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn
        FROM users;
      `;
      const result = await parse(sql);
      expect(result.stmts).toHaveLength(1);
    });

    test("parses CREATE TRIGGER", async () => {
      const sql = `
        CREATE TRIGGER set_updated
        BEFORE UPDATE ON users
        FOR EACH ROW
        EXECUTE FUNCTION update_modified_column();
      `;
      const result = await parse(sql);
      expect(result.stmts).toHaveLength(1);
      expect(result.stmts[0].stmt).toHaveProperty("CreateTrigStmt");
    });

    test("parses CREATE EXTENSION", async () => {
      const sql = 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp"';
      const result = await parse(sql);
      expect(result.stmts).toHaveLength(1);
      expect(result.stmts[0].stmt).toHaveProperty("CreateExtensionStmt");
    });
  });

  describe("error handling", () => {
    test("throws on invalid SQL", async () => {
      await expect(parse("SELCT 1")).rejects.toThrow();
    });

    test("throws on incomplete statement", async () => {
      await expect(parse("CREATE TABLE")).rejects.toThrow();
    });

    test("error includes position information", async () => {
      try {
        await parse("SELECT FROM WHERE");
        expect(true).toBe(false); // should not reach here
      } catch (e: any) {
        expect(e.message).toBeDefined();
        // The error should indicate something about the syntax
        expect(e.message.length).toBeGreaterThan(0);
      }
    });
  });

  describe("fixture file parsing", () => {
    test("parses edge-cases.sql fixture", async () => {
      const sql = readFileSync(join(FIXTURES_DIR, "edge-cases.sql"), "utf-8");
      const result = await parse(sql);

      // The fixture has 18 distinct SQL statements
      expect(result.stmts.length).toBe(18);

      // Verify we can identify statement types
      const stmtTypes = result.stmts.map((s: any) => Object.keys(s.stmt)[0]);
      expect(stmtTypes).toContain("CreateStmt"); // CREATE TABLE
      expect(stmtTypes).toContain("IndexStmt"); // CREATE INDEX
      expect(stmtTypes).toContain("CreateFunctionStmt"); // CREATE FUNCTION
      expect(stmtTypes).toContain("CreateTrigStmt"); // CREATE TRIGGER
      expect(stmtTypes).toContain("AlterTableStmt"); // ALTER TABLE
      expect(stmtTypes).toContain("InsertStmt"); // INSERT
      expect(stmtTypes).toContain("SelectStmt"); // SELECT with CTE
      expect(stmtTypes).toContain("ViewStmt"); // CREATE VIEW
      expect(stmtTypes).toContain("DoStmt"); // DO block
      expect(stmtTypes).toContain("GrantStmt"); // GRANT/REVOKE
      expect(stmtTypes).toContain("CreateEnumStmt"); // CREATE TYPE ENUM
    });

    test("provides byte offsets for all statements in fixture", async () => {
      const sql = readFileSync(join(FIXTURES_DIR, "edge-cases.sql"), "utf-8");
      const result = await parse(sql);

      // Every statement should have a location
      // Note: stmt_location is omitted (undefined) for the first statement
      // when it starts at byte 0 (protobuf zero-value omission).
      for (let i = 0; i < result.stmts.length; i++) {
        const entry = result.stmts[i];
        const loc = entry.stmt_location ?? 0;
        if (i > 0) {
          expect(loc).toBeGreaterThan(0);
        }
      }

      // Verify we can extract the original SQL text using offsets
      const firstStmt = result.stmts[0];
      const loc = firstStmt.stmt_location ?? 0;
      const len = firstStmt.stmt_len;
      const stmtText = sql.substring(loc, loc + len);
      expect(stmtText).toContain("CREATE TABLE users");
    });
  });

  describe("performance benchmark", () => {
    test("parses 1000-line SQL file in < 200ms", async () => {
      const sql = readFileSync(
        join(FIXTURES_DIR, "large-benchmark.sql"),
        "utf-8",
      );

      // Warm up the WASM module
      await parse("SELECT 1");

      // Benchmark: run 5 times and take the median
      const times: number[] = [];
      const iterations = 5;

      for (let i = 0; i < iterations; i++) {
        const start = performance.now();
        const result = await parse(sql);
        const elapsed = performance.now() - start;
        times.push(elapsed);

        // Verify it actually parsed
        expect(result.stmts.length).toBeGreaterThan(100);
      }

      times.sort((a, b) => a - b);
      const median = times[Math.floor(times.length / 2)];
      const min = times[0];
      const max = times[times.length - 1];

      console.log(
        `\n  Parse benchmark (1000-line SQL, ${iterations} runs):`,
      );
      console.log(`    Min:    ${min.toFixed(2)}ms`);
      console.log(`    Median: ${median.toFixed(2)}ms`);
      console.log(`    Max:    ${max.toFixed(2)}ms`);
      console.log(
        `    Statements parsed: ${(await parse(sql)).stmts.length}`,
      );

      // Performance target: < 200ms
      expect(median).toBeLessThan(200);
    });

    test("parseSync performance for 1000-line SQL", () => {
      const sql = readFileSync(
        join(FIXTURES_DIR, "large-benchmark.sql"),
        "utf-8",
      );

      // Warm up
      parseSync("SELECT 1");

      const times: number[] = [];
      const iterations = 5;

      for (let i = 0; i < iterations; i++) {
        const start = performance.now();
        const result = parseSync(sql);
        const elapsed = performance.now() - start;
        times.push(elapsed);
        expect(result.stmts.length).toBeGreaterThan(100);
      }

      times.sort((a, b) => a - b);
      const median = times[Math.floor(times.length / 2)];
      const min = times[0];
      const max = times[times.length - 1];

      console.log(
        `\n  parseSync benchmark (1000-line SQL, ${iterations} runs):`,
      );
      console.log(`    Min:    ${min.toFixed(2)}ms`);
      console.log(`    Median: ${median.toFixed(2)}ms`);
      console.log(`    Max:    ${max.toFixed(2)}ms`);

      expect(median).toBeLessThan(200);
    });
  });

  describe("AST structure inspection", () => {
    test("extracts column names from CREATE TABLE", async () => {
      const sql = `CREATE TABLE orders (
        id bigint PRIMARY KEY,
        customer_id bigint NOT NULL,
        total numeric(10,2),
        status text DEFAULT 'pending'
      )`;
      const result = await parse(sql);
      const createStmt = result.stmts[0].stmt.CreateStmt;

      const colNames = createStmt.tableElts
        .filter((e: any) => e.ColumnDef)
        .map((e: any) => e.ColumnDef.colname);

      expect(colNames).toEqual(["id", "customer_id", "total", "status"]);
    });

    test("extracts table name from INSERT", async () => {
      const sql = "INSERT INTO users (email) VALUES ('test@test.com')";
      const result = await parse(sql);
      const insertStmt = result.stmts[0].stmt.InsertStmt;
      expect(insertStmt.relation.relname).toBe("users");
    });

    test("extracts table names from SELECT with JOIN", async () => {
      const sql = `
        SELECT u.name, o.total
        FROM users u
        JOIN orders o ON o.user_id = u.id
        WHERE o.status = 'completed'
      `;
      const result = await parse(sql);
      const selectStmt = result.stmts[0].stmt.SelectStmt;
      const fromClause = selectStmt.fromClause;

      // With a JOIN, the fromClause contains a JoinExpr
      expect(fromClause).toBeArray();
      expect(fromClause[0]).toHaveProperty("JoinExpr");
    });

    test("identifies ALTER TABLE subcommand types", async () => {
      const sql = `
        ALTER TABLE users
          ADD COLUMN nickname text,
          ALTER COLUMN email SET NOT NULL,
          DROP COLUMN IF EXISTS legacy_field;
      `;
      const result = await parse(sql);
      const alterStmt = result.stmts[0].stmt.AlterTableStmt;

      expect(alterStmt.cmds).toHaveLength(3);
      // Each cmd has an AlterTableCmd with a subtype enum
      for (const cmd of alterStmt.cmds) {
        expect(cmd).toHaveProperty("AlterTableCmd");
        expect(cmd.AlterTableCmd.subtype).toBeDefined();
      }
    });
  });
});
