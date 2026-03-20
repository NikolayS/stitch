#!/usr/bin/env bun
// Generates a ~1000-line SQL fixture for benchmarking parser performance.

const lines: string[] = [];

lines.push("-- Auto-generated 1000-line SQL fixture for benchmark testing");
lines.push("");

// Generate 50 tables (each ~8 lines = 400 lines)
for (let i = 1; i <= 50; i++) {
  lines.push(`CREATE TABLE bench_table_${i} (`);
  lines.push(`    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,`);
  lines.push(`    name text NOT NULL,`);
  lines.push(`    value numeric(12, 2) DEFAULT 0,`);
  lines.push(`    status text DEFAULT 'active',`);
  lines.push(`    created_at timestamptz NOT NULL DEFAULT now(),`);
  lines.push(`    updated_at timestamptz`);
  lines.push(`);`);
  lines.push("");
}

// Generate 50 indexes (50 lines)
for (let i = 1; i <= 50; i++) {
  lines.push(`CREATE INDEX idx_bench_table_${i}_name ON bench_table_${i} (name);`);
}
lines.push("");

// Generate 20 functions with dollar-quoting (each ~12 lines = 240 lines)
for (let i = 1; i <= 20; i++) {
  lines.push(`CREATE OR REPLACE FUNCTION bench_func_${i}(p_id bigint)`);
  lines.push(`RETURNS void AS $$`);
  lines.push(`BEGIN`);
  lines.push(`    UPDATE bench_table_${i} SET updated_at = now() WHERE id = p_id;`);
  lines.push(`    IF NOT FOUND THEN`);
  lines.push(`        RAISE NOTICE 'Record % not found in bench_table_${i}', p_id;`);
  lines.push(`    END IF;`);
  lines.push(`END;`);
  lines.push(`$$ LANGUAGE plpgsql;`);
  lines.push("");
}

// Generate 20 views with joins (each ~6 lines = 120 lines)
for (let i = 1; i <= 20; i++) {
  const j = i + 1 <= 50 ? i + 1 : 1;
  lines.push(`CREATE VIEW bench_view_${i} AS`);
  lines.push(`SELECT a.id, a.name, b.value, a.created_at`);
  lines.push(`FROM bench_table_${i} a`);
  lines.push(`JOIN bench_table_${j} b ON b.id = a.id`);
  lines.push(`WHERE a.status = 'active';`);
  lines.push("");
}

// Generate 30 ALTER TABLE statements (60 lines)
for (let i = 1; i <= 30; i++) {
  lines.push(`ALTER TABLE bench_table_${i} ADD COLUMN extra_col_${i} text;`);
  lines.push(`ALTER TABLE bench_table_${i} ADD COLUMN score_${i} integer DEFAULT 0;`);
}
lines.push("");

// Generate INSERT statements (80 lines)
for (let i = 1; i <= 20; i++) {
  lines.push(`INSERT INTO bench_table_${i} (name, value, status) VALUES`);
  lines.push(`    ('item_a', ${i * 10}.50, 'active'),`);
  lines.push(`    ('item_b', ${i * 20}.75, 'inactive'),`);
  lines.push(`    ('item_c', ${i * 30}.00, 'active');`);
}
lines.push("");

// Generate GRANT statements (50 lines)
for (let i = 1; i <= 50; i++) {
  lines.push(`GRANT SELECT ON bench_table_${i} TO readonly_role;`);
}
lines.push("");

// Footer comment
lines.push(`-- End of generated fixture (${lines.length + 1} lines)`);

const content = lines.join("\n") + "\n";
const path = new URL("./large-benchmark.sql", import.meta.url).pathname;
await Bun.write(path, content);
console.log(`Generated ${path} with ${lines.length} lines`);
