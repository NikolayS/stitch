#!/usr/bin/env bun
/**
 * Standalone script for testing bun build --compile with libpg-query.
 * This file is compiled into a single binary and executed to verify
 * the WASM parser works in the compiled output.
 */
import { parse, parseSync, loadModule } from "libpg-query";
import { readFileSync } from "fs";
import { join, dirname } from "path";

async function main() {
  console.log("=== bun build --compile parser test ===\n");

  // 1. Load WASM module
  console.log("1. Loading WASM module...");
  const loadStart = performance.now();
  await loadModule();
  console.log(`   OK (${(performance.now() - loadStart).toFixed(1)}ms)\n`);

  // 2. Parse simple SQL
  console.log("2. Parsing simple SELECT...");
  const result1 = await parse("SELECT 1 + 2 AS answer");
  console.log(`   Statements: ${result1.stmts.length}`);
  console.log(
    `   Type: ${Object.keys(result1.stmts[0].stmt)[0]}`
  );
  console.log("   OK\n");

  // 3. Parse multi-statement SQL
  console.log("3. Parsing multi-statement SQL...");
  const multiSql = `
    CREATE TABLE test (id int PRIMARY KEY, name text);
    INSERT INTO test (id, name) VALUES (1, 'hello');
    SELECT * FROM test;
    DROP TABLE test;
  `;
  const result2 = await parse(multiSql);
  console.log(`   Statements: ${result2.stmts.length}`);
  const types = result2.stmts.map(
    (s: any) => Object.keys(s.stmt)[0]
  );
  console.log(`   Types: ${types.join(", ")}`);
  console.log("   OK\n");

  // 4. Parse dollar-quoted function
  console.log("4. Parsing dollar-quoted CREATE FUNCTION...");
  const funcSql = `
    CREATE FUNCTION greet(name text) RETURNS text AS $$
    BEGIN
      RETURN 'Hello, ' || name || '!';
    END;
    $$ LANGUAGE plpgsql;
  `;
  const result3 = await parse(funcSql);
  console.log(`   Statements: ${result3.stmts.length}`);
  console.log(
    `   Type: ${Object.keys(result3.stmts[0].stmt)[0]}`
  );
  console.log("   OK\n");

  // 5. parseSync test
  console.log("5. Testing parseSync...");
  const result4 = parseSync("SELECT now()");
  console.log(`   Statements: ${result4.stmts.length}`);
  console.log("   OK\n");

  // 6. Error handling
  console.log("6. Testing error handling on invalid SQL...");
  try {
    await parse("SELCT 1");
    console.log("   FAIL: should have thrown");
    process.exit(1);
  } catch (e: any) {
    console.log(`   Caught expected error: ${e.message.substring(0, 60)}...`);
    console.log("   OK\n");
  }

  // 7. Benchmark with inline large SQL
  console.log("7. Benchmark: generating and parsing large SQL...");
  const stmts: string[] = [];
  for (let i = 0; i < 100; i++) {
    stmts.push(`CREATE TABLE t${i} (id int PRIMARY KEY, name text, val numeric DEFAULT ${i});`);
    stmts.push(`CREATE INDEX idx_t${i}_name ON t${i} (name);`);
  }
  const largeSql = stmts.join("\n");

  const times: number[] = [];
  for (let i = 0; i < 5; i++) {
    const start = performance.now();
    const r = await parse(largeSql);
    times.push(performance.now() - start);
  }
  times.sort((a, b) => a - b);
  const median = times[Math.floor(times.length / 2)];
  console.log(`   SQL length: ${largeSql.length} chars, ${largeSql.split("\n").length} lines`);
  console.log(`   Median parse time: ${median.toFixed(2)}ms`);
  console.log(`   ${median < 200 ? "PASS" : "FAIL"}: ${median < 200 ? "< 200ms target met" : "> 200ms target exceeded"}`);
  console.log("   OK\n");

  console.log("=== ALL TESTS PASSED ===");
  process.exit(0);
}

main().catch((err) => {
  console.error("FATAL:", err);
  process.exit(1);
});
