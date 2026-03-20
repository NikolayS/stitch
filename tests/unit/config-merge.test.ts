import { describe, it, expect } from "bun:test";
import { loadConfig } from "../../src/config/index";

// ---------------------------------------------------------------------------
// loadConfig with fixture files
// ---------------------------------------------------------------------------

describe("loadConfig", () => {
  it("returns sensible defaults when no config files exist", () => {
    // Use a directory that has no config files
    const config = loadConfig("/tmp/nonexistent-project-dir-12345", {}, {});

    expect(config.core.top_dir).toBe(".");
    expect(config.core.deploy_dir).toBe("deploy");
    expect(config.core.revert_dir).toBe("revert");
    expect(config.core.verify_dir).toBe("verify");
    expect(config.core.plan_file).toBe("sqitch.plan");
    expect(config.core.engine).toBeUndefined();
    expect(config.deploy.verify).toBe(true);
    expect(config.deploy.mode).toBe("change");
    expect(config.deploy.lock_retries).toBe(0);
    expect(config.deploy.lock_timeout).toBe("5s");
  });
});

// ---------------------------------------------------------------------------
// Environment variable overrides
// ---------------------------------------------------------------------------

describe("environment variable overrides", () => {
  it("SQITCH_ENGINE overrides core.engine", () => {
    const config = loadConfig("/tmp/nonexistent-12345", {}, {
      SQITCH_ENGINE: "mysql",
    });
    expect(config.core.engine).toBe("mysql");
  });

  it("SQITCH_TOP_DIR overrides core.top_dir", () => {
    const config = loadConfig("/tmp/nonexistent-12345", {}, {
      SQITCH_TOP_DIR: "/custom/dir",
    });
    expect(config.core.top_dir).toBe("/custom/dir");
  });

  it("SQITCH_DEPLOY_DIR overrides core.deploy_dir", () => {
    const config = loadConfig("/tmp/nonexistent-12345", {}, {
      SQITCH_DEPLOY_DIR: "custom_deploy",
    });
    expect(config.core.deploy_dir).toBe("custom_deploy");
  });

  it("SQITCH_REVERT_DIR overrides core.revert_dir", () => {
    const config = loadConfig("/tmp/nonexistent-12345", {}, {
      SQITCH_REVERT_DIR: "custom_revert",
    });
    expect(config.core.revert_dir).toBe("custom_revert");
  });

  it("SQITCH_VERIFY_DIR overrides core.verify_dir", () => {
    const config = loadConfig("/tmp/nonexistent-12345", {}, {
      SQITCH_VERIFY_DIR: "custom_verify",
    });
    expect(config.core.verify_dir).toBe("custom_verify");
  });

  it("SQITCH_PLAN_FILE overrides core.plan_file", () => {
    const config = loadConfig("/tmp/nonexistent-12345", {}, {
      SQITCH_PLAN_FILE: "custom.plan",
    });
    expect(config.core.plan_file).toBe("custom.plan");
  });

  it("SQLEVER_VERIFY overrides deploy.verify", () => {
    const config = loadConfig("/tmp/nonexistent-12345", {}, {
      SQLEVER_VERIFY: "false",
    });
    expect(config.deploy.verify).toBe(false);
  });

  it("SQLEVER_VERIFY=1 sets deploy.verify to true", () => {
    const config = loadConfig("/tmp/nonexistent-12345", {}, {
      SQLEVER_VERIFY: "1",
    });
    expect(config.deploy.verify).toBe(true);
  });

  it("SQLEVER_MODE overrides deploy.mode", () => {
    const config = loadConfig("/tmp/nonexistent-12345", {}, {
      SQLEVER_MODE: "all",
    });
    expect(config.deploy.mode).toBe("all");
  });

  it("SQLEVER_PG_VERSION overrides analysis.pg_version", () => {
    const config = loadConfig("/tmp/nonexistent-12345", {}, {
      SQLEVER_PG_VERSION: "15",
    });
    expect(config.analysis.pg_version).toBe("15");
  });

  it("SQLEVER_ERROR_ON_WARN overrides analysis.error_on_warn", () => {
    const config = loadConfig("/tmp/nonexistent-12345", {}, {
      SQLEVER_ERROR_ON_WARN: "true",
    });
    expect(config.analysis.error_on_warn).toBe(true);
  });

  it("invalid SQLEVER_MODE is ignored", () => {
    const config = loadConfig("/tmp/nonexistent-12345", {}, {
      SQLEVER_MODE: "invalid",
    });
    expect(config.deploy.mode).toBe("change"); // default
  });
});

// ---------------------------------------------------------------------------
// CLI flag overrides
// ---------------------------------------------------------------------------

describe("CLI flag overrides", () => {
  it("flags override all other sources", () => {
    const config = loadConfig(
      "/tmp/nonexistent-12345",
      {
        engine: "sqlite",
        topDir: "/flag/dir",
        verify: false,
        mode: "all",
        lockRetries: 5,
        lockTimeout: "30s",
        strict: true,
        pgVersion: "14",
      },
      {
        SQITCH_ENGINE: "mysql", // should be overridden by flag
      },
    );

    expect(config.core.engine).toBe("sqlite"); // flag wins over env
    expect(config.core.top_dir).toBe("/flag/dir");
    expect(config.deploy.verify).toBe(false);
    expect(config.deploy.mode).toBe("all");
    expect(config.deploy.lock_retries).toBe(5);
    expect(config.deploy.lock_timeout).toBe("30s");
    expect(config.analysis.error_on_warn).toBe(true); // --strict
    expect(config.analysis.pg_version).toBe("14");
  });

  it("partial flags only override specified values", () => {
    const config = loadConfig("/tmp/nonexistent-12345", {
      engine: "pg",
    }, {});

    expect(config.core.engine).toBe("pg");
    expect(config.core.top_dir).toBe("."); // default, not overridden
    expect(config.deploy.verify).toBe(true); // default
  });
});

// ---------------------------------------------------------------------------
// Precedence chain
// ---------------------------------------------------------------------------

describe("precedence chain", () => {
  it("env overrides defaults", () => {
    const config = loadConfig("/tmp/nonexistent-12345", {}, {
      SQITCH_ENGINE: "mysql",
    });
    expect(config.core.engine).toBe("mysql");
  });

  it("flags override env", () => {
    const config = loadConfig(
      "/tmp/nonexistent-12345",
      { engine: "sqlite" },
      { SQITCH_ENGINE: "mysql" },
    );
    expect(config.core.engine).toBe("sqlite");
  });

  it("flags override everything for deploy.verify", () => {
    const config = loadConfig(
      "/tmp/nonexistent-12345",
      { verify: true },
      { SQLEVER_VERIFY: "false" },
    );
    expect(config.deploy.verify).toBe(true); // flag wins
  });
});

// ---------------------------------------------------------------------------
// MergedConfig structure
// ---------------------------------------------------------------------------

describe("MergedConfig structure", () => {
  it("has all expected top-level keys", () => {
    const config = loadConfig("/tmp/nonexistent-12345", {}, {});
    expect(config.core).toBeDefined();
    expect(config.deploy).toBeDefined();
    expect(config.engines).toBeDefined();
    expect(config.targets).toBeDefined();
    expect(config.analysis).toBeDefined();
    expect(config.sqitchConf).toBeDefined();
  });

  it("engines is empty record when no engine sections", () => {
    const config = loadConfig("/tmp/nonexistent-12345", {}, {});
    expect(config.engines).toEqual({});
  });

  it("targets is empty record when no target sections", () => {
    const config = loadConfig("/tmp/nonexistent-12345", {}, {});
    expect(config.targets).toEqual({});
  });

  it("sqleverToml is null when no sqlever.toml exists", () => {
    const config = loadConfig("/tmp/nonexistent-12345", {}, {});
    expect(config.sqleverToml).toBeNull();
  });
});
