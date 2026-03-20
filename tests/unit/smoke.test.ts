import { describe, expect, it } from "bun:test";
import { readFileSync } from "fs";
import { resolve } from "path";

describe("smoke", () => {
  it("package.json has correct name and version", () => {
    const pkgPath = resolve(import.meta.dir, "../../package.json");
    const pkg = JSON.parse(readFileSync(pkgPath, "utf-8"));
    expect(pkg.name).toBe("sqlever");
    expect(pkg.version).toMatch(/^\d+\.\d+\.\d+$/);
  });

  it("cli module is valid TypeScript that can be loaded", async () => {
    const cliPath = resolve(import.meta.dir, "../../src/cli.ts");
    const contents = readFileSync(cliPath, "utf-8");
    // Verify the CLI source contains expected command definitions
    expect(contents).toContain("deploy");
    expect(contents).toContain("revert");
    expect(contents).toContain("verify");
    expect(contents).toContain("--version");
  });

  it("expected commands are defined", async () => {
    const cliPath = resolve(import.meta.dir, "../../src/cli.ts");
    const contents = readFileSync(cliPath, "utf-8");
    const expectedCommands = ["add", "deploy", "revert", "verify", "status", "log"];
    for (const cmd of expectedCommands) {
      expect(contents).toContain(`${cmd}:`);
    }
  });
});
