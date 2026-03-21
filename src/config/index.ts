// src/config/index.ts — Config loading and merging with precedence
//
// Precedence (lowest to highest):
//   1. system   — /etc/sqlever/sqitch.conf (or $(prefix)/etc/sqitch/sqitch.conf)
//   2. user     — ~/.sqitch/sqitch.conf
//   3. project  — ./sqitch.conf
//   4. sqlever  — ./sqlever.toml (sqlever-specific settings)
//   5. env      — SQLEVER_* and SQITCH_* environment variables
//   6. flags    — CLI flags (--engine, --target, --verify, etc.)
//
// The merged config is the single source of truth for all commands.

import { existsSync, readFileSync } from "fs";
import { join, resolve } from "path";
import { homedir } from "os";
import {
  parseSqitchConf,
  confGetString,
  confGetBool,
  confGetSection,
  confListSubsections,
  type SqitchConf,
  type ConfEntry,
} from "./sqitch-conf";
import {
  parseSqleverToml,
  getAnalysisConfig,
  type SqleverToml,
  type SqleverAnalysisConfig,
  type SqleverDeployConfig,
} from "./sqlever-toml";

// Re-export for convenience
export { parseSqitchConf, confGet, confGetString, confGetBool } from "./sqitch-conf";
export { confGetAll, confListSubsections, confGetSection } from "./sqitch-conf";
export { confSet, confUnset, serializeSqitchConf } from "./sqitch-conf";
export { parseSqleverToml, getAnalysisConfig, serializeSqleverToml } from "./sqlever-toml";
export type { SqitchConf, ConfEntry } from "./sqitch-conf";
export type { SqleverToml, SqleverAnalysisConfig } from "./sqlever-toml";

// ---------------------------------------------------------------------------
// Merged config types
// ---------------------------------------------------------------------------

export interface TargetConfig {
  name: string;
  uri?: string;
}

export interface EngineConfig {
  name: string;
  target?: string;
  client?: string;
}

export interface CoreConfig {
  engine?: string;
  top_dir: string;
  deploy_dir: string;
  revert_dir: string;
  verify_dir: string;
  plan_file: string;
}

export interface DeployConfig {
  verify: boolean;
  mode: "change" | "tag" | "all";
  lock_retries: number;
  lock_timeout: string;
  idle_in_transaction_session_timeout: string;
  search_path?: string;
}

export interface MergedConfig {
  core: CoreConfig;
  deploy: DeployConfig;
  engines: Record<string, EngineConfig>;
  targets: Record<string, TargetConfig>;
  analysis: SqleverAnalysisConfig;
  /** Raw sqitch.conf (project-level, merged). */
  sqitchConf: SqitchConf;
  /** Raw sqlever.toml. */
  sqleverToml: SqleverToml | null;
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

const DEFAULT_CORE: CoreConfig = {
  engine: undefined,
  top_dir: ".",
  deploy_dir: "deploy",
  revert_dir: "revert",
  verify_dir: "verify",
  plan_file: "sqitch.plan",
};

const DEFAULT_DEPLOY: DeployConfig = {
  verify: true,
  mode: "change",
  lock_retries: 0,
  lock_timeout: "5s",
  idle_in_transaction_session_timeout: "10min",
  search_path: undefined,
};

// ---------------------------------------------------------------------------
// CLI flag overrides
// ---------------------------------------------------------------------------

export interface CliFlags {
  engine?: string;
  target?: string;
  topDir?: string;
  deployDir?: string;
  revertDir?: string;
  verifyDir?: string;
  planFile?: string;
  verify?: boolean;
  mode?: string;
  lockRetries?: number;
  lockTimeout?: string;
  strict?: boolean;
  pgVersion?: string;
}

// ---------------------------------------------------------------------------
// Config loading
// ---------------------------------------------------------------------------

/**
 * Load and merge configuration from all sources.
 *
 * @param projectDir — Project root directory (defaults to cwd)
 * @param flags      — CLI flag overrides
 * @param env        — Environment variables (defaults to process.env)
 */
export function loadConfig(
  projectDir?: string,
  flags?: CliFlags,
  env?: Record<string, string | undefined>,
): MergedConfig {
  const cwd = projectDir ?? process.cwd();
  const environment = env ?? process.env;

  // 1. Load sqitch.conf files (system < user < project)
  const merged = mergeConfs(loadConfFiles(cwd));

  // 2. Load sqlever.toml
  const sqleverToml = loadSqleverToml(cwd);

  // 3. Build core config from sqitch.conf
  //
  // Sqitch convention: when top_dir is set, the default paths for plan_file,
  // deploy_dir, revert_dir, and verify_dir are relative to top_dir — not
  // the project root. Only apply this when the individual path is NOT
  // explicitly configured in sqitch.conf.
  const topDir = confGetString(merged, "core.top_dir") ?? DEFAULT_CORE.top_dir;
  const core: CoreConfig = {
    engine: confGetString(merged, "core.engine") ?? DEFAULT_CORE.engine,
    top_dir: topDir,
    deploy_dir: confGetString(merged, "core.deploy_dir") ?? join(topDir, DEFAULT_CORE.deploy_dir),
    revert_dir: confGetString(merged, "core.revert_dir") ?? join(topDir, DEFAULT_CORE.revert_dir),
    verify_dir: confGetString(merged, "core.verify_dir") ?? join(topDir, DEFAULT_CORE.verify_dir),
    plan_file: confGetString(merged, "core.plan_file") ?? join(topDir, DEFAULT_CORE.plan_file),
  };

  // 4. Build deploy config
  const deploy: DeployConfig = { ...DEFAULT_DEPLOY };
  const confVerify = confGetBool(merged, "deploy.verify");
  if (confVerify !== undefined) deploy.verify = confVerify;
  const confMode = confGetString(merged, "deploy.mode");
  if (confMode === "change" || confMode === "tag" || confMode === "all") {
    deploy.mode = confMode;
  }

  // 5. Merge sqlever.toml deploy overrides
  if (sqleverToml) {
    const tomlDeploy = sqleverToml.deploy as SqleverDeployConfig | undefined;
    if (tomlDeploy) {
      if (typeof tomlDeploy.verify === "boolean") deploy.verify = tomlDeploy.verify;
      if (tomlDeploy.mode === "change" || tomlDeploy.mode === "tag" || tomlDeploy.mode === "all") {
        deploy.mode = tomlDeploy.mode;
      }
      if (typeof tomlDeploy.lock_retries === "number") deploy.lock_retries = tomlDeploy.lock_retries;
      if (typeof tomlDeploy.lock_timeout === "string") deploy.lock_timeout = tomlDeploy.lock_timeout;
      if (typeof tomlDeploy.idle_in_transaction_session_timeout === "string") {
        deploy.idle_in_transaction_session_timeout = tomlDeploy.idle_in_transaction_session_timeout;
      }
      if (typeof tomlDeploy.search_path === "string") deploy.search_path = tomlDeploy.search_path;
    }
  }

  // 6. Build engines and targets from sqitch.conf sections
  const engines: Record<string, EngineConfig> = {};
  for (const sub of confListSubsections(merged, "engine")) {
    const section = confGetSection(merged, "engine", sub);
    engines[sub] = {
      name: sub,
      target: typeof section.target === "string" ? section.target : undefined,
      client: typeof section.client === "string" ? section.client : undefined,
    };
  }

  const targets: Record<string, TargetConfig> = {};
  for (const sub of confListSubsections(merged, "target")) {
    const section = confGetSection(merged, "target", sub);
    targets[sub] = {
      name: sub,
      uri: typeof section.uri === "string" ? section.uri : undefined,
    };
  }

  // 7. Analysis config from sqlever.toml
  const analysis = sqleverToml ? getAnalysisConfig(sqleverToml) : {};

  // 8. Apply environment variable overrides
  applyEnvOverrides(core, deploy, analysis, environment);

  // 9. Apply CLI flag overrides (highest precedence)
  if (flags) {
    applyFlagOverrides(core, deploy, analysis, flags);
  }

  return {
    core,
    deploy,
    engines,
    targets,
    analysis,
    sqitchConf: merged,
    sqleverToml,
  };
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/**
 * Load sqitch.conf files in precedence order.
 * Returns array of parsed configs: [system, user, project].
 */
function loadConfFiles(projectDir: string): SqitchConf[] {
  const confs: SqitchConf[] = [];

  // System config
  const systemPaths = [
    "/etc/sqlever/sqitch.conf",
    "/etc/sqitch/sqitch.conf",
  ];
  for (const path of systemPaths) {
    const text = readFileSafe(path);
    if (text !== null) {
      confs.push(parseSqitchConf(text));
      break; // use first found
    }
  }

  // User config
  const home = homedir();
  const userPath = join(home, ".sqitch", "sqitch.conf");
  const userText = readFileSafe(userPath);
  if (userText !== null) {
    confs.push(parseSqitchConf(userText));
  }

  // Project config
  const projectPath = join(resolve(projectDir), "sqitch.conf");
  const projectText = readFileSafe(projectPath);
  if (projectText !== null) {
    confs.push(parseSqitchConf(projectText));
  }

  return confs;
}

/**
 * Load sqlever.toml from the project directory.
 */
function loadSqleverToml(projectDir: string): SqleverToml | null {
  const tomlPath = join(resolve(projectDir), "sqlever.toml");
  const text = readFileSafe(tomlPath);
  if (text === null) return null;
  return parseSqleverToml(text);
}

/**
 * Read a file, returning null if it doesn't exist or can't be read.
 */
function readFileSafe(path: string): string | null {
  try {
    if (!existsSync(path)) return null;
    return readFileSync(path, "utf-8");
  } catch {
    return null;
  }
}

/**
 * Merge multiple SqitchConf objects. Later configs override earlier ones.
 *
 * When a key appears in a later (higher-precedence) config, ALL values for
 * that key from earlier configs are removed and replaced by the values from
 * the later config. This ensures that multi-valued keys are fully overridden
 * rather than partially appended to.
 *
 * For example, if system config has `core.engine = pg` repeated 3 times and
 * the project config has `core.engine = mysql`, the merged result will contain
 * only `core.engine = mysql` — not 3 pg entries plus 1 mysql entry.
 */
export function mergeConfs(confs: SqitchConf[]): SqitchConf {
  if (confs.length === 0) {
    return { entries: [], rawLines: [], sections: new Set() };
  }
  if (confs.length === 1) {
    return confs[0]!;
  }

  // Collect the set of keys defined in each later config so we know which
  // keys from earlier configs to drop.
  const overriddenKeys = new Set<string>();
  for (let i = 1; i < confs.length; i++) {
    for (const entry of confs[i]!.entries) {
      overriddenKeys.add(normalizeKeyForMerge(entry.key));
    }
  }

  // Build merged entries:
  // 1. Start with base config, filtering out keys that are overridden by later configs
  // 2. Then append entries from later configs (also filtering earlier overrides)
  const merged: ConfEntry[] = [];
  const addedFromLater = new Set<string>(); // track which keys we've already added from later confs

  // First, add entries from the base config that are NOT overridden
  for (const entry of confs[0]!.entries) {
    const nk = normalizeKeyForMerge(entry.key);
    if (!overriddenKeys.has(nk)) {
      merged.push(entry);
    }
  }

  // Then, for each subsequent config, add all their entries.
  // If multiple later configs define the same key, only the last one's entries survive.
  // We process in reverse to determine which later config "wins" for each key.
  const winningConf = new Map<string, number>(); // normalized key -> index of winning conf
  for (let i = confs.length - 1; i >= 1; i--) {
    for (const entry of confs[i]!.entries) {
      const nk = normalizeKeyForMerge(entry.key);
      if (!winningConf.has(nk)) {
        winningConf.set(nk, i);
      }
    }
  }

  // Now add entries from later configs, but only from the winning conf for each key
  for (let i = 1; i < confs.length; i++) {
    for (const entry of confs[i]!.entries) {
      const nk = normalizeKeyForMerge(entry.key);
      if (winningConf.get(nk) === i) {
        merged.push(entry);
      }
    }
  }

  // Merge sections sets
  const mergedSections = new Set<string>();
  for (const conf of confs) {
    if (conf.sections) {
      for (const s of conf.sections) {
        mergedSections.add(s);
      }
    }
  }

  return {
    entries: merged,
    rawLines: confs[confs.length - 1]!.rawLines,
    sections: mergedSections,
  };
}

/**
 * Normalize a key for merge comparison. Uses the same rules as normalizeKey
 * in sqitch-conf.ts: lowercase section and key name, preserve subsection case.
 */
function normalizeKeyForMerge(key: string): string {
  const firstDot = key.indexOf(".");
  if (firstDot < 0) return key.toLowerCase();

  const lastDot = key.lastIndexOf(".");
  if (firstDot === lastDot) {
    return key.toLowerCase();
  }

  const section = key.slice(0, firstDot).toLowerCase();
  const subsection = key.slice(firstDot + 1, lastDot);
  const keyName = key.slice(lastDot + 1).toLowerCase();
  return `${section}.${subsection}.${keyName}`;
}

/**
 * Apply SQLEVER_* and SQITCH_* environment variable overrides.
 *
 * Env var mapping:
 *   SQITCH_ENGINE       => core.engine
 *   SQITCH_TOP_DIR      => core.top_dir
 *   SQITCH_DEPLOY_DIR   => core.deploy_dir
 *   SQITCH_REVERT_DIR   => core.revert_dir
 *   SQITCH_VERIFY_DIR   => core.verify_dir
 *   SQITCH_PLAN_FILE    => core.plan_file
 *   SQLEVER_VERIFY      => deploy.verify
 *   SQLEVER_MODE        => deploy.mode
 *   SQLEVER_PG_VERSION  => analysis.pg_version
 *   SQLEVER_ERROR_ON_WARN => analysis.error_on_warn
 */
function applyEnvOverrides(
  core: CoreConfig,
  deploy: DeployConfig,
  analysis: SqleverAnalysisConfig,
  env: Record<string, string | undefined>,
): void {
  if (env.SQITCH_ENGINE) core.engine = env.SQITCH_ENGINE;
  if (env.SQITCH_TOP_DIR) core.top_dir = env.SQITCH_TOP_DIR;
  if (env.SQITCH_DEPLOY_DIR) core.deploy_dir = env.SQITCH_DEPLOY_DIR;
  if (env.SQITCH_REVERT_DIR) core.revert_dir = env.SQITCH_REVERT_DIR;
  if (env.SQITCH_VERIFY_DIR) core.verify_dir = env.SQITCH_VERIFY_DIR;
  if (env.SQITCH_PLAN_FILE) core.plan_file = env.SQITCH_PLAN_FILE;

  if (env.SQLEVER_VERIFY !== undefined) {
    deploy.verify = env.SQLEVER_VERIFY === "true" || env.SQLEVER_VERIFY === "1";
  }
  if (env.SQLEVER_MODE) {
    const m = env.SQLEVER_MODE;
    if (m === "change" || m === "tag" || m === "all") deploy.mode = m;
  }
  if (env.SQLEVER_PG_VERSION) {
    analysis.pg_version = env.SQLEVER_PG_VERSION;
  }
  if (env.SQLEVER_ERROR_ON_WARN !== undefined) {
    analysis.error_on_warn =
      env.SQLEVER_ERROR_ON_WARN === "true" || env.SQLEVER_ERROR_ON_WARN === "1";
  }
}

/**
 * Apply CLI flag overrides (highest precedence).
 */
function applyFlagOverrides(
  core: CoreConfig,
  deploy: DeployConfig,
  analysis: SqleverAnalysisConfig,
  flags: CliFlags,
): void {
  if (flags.engine !== undefined) core.engine = flags.engine;
  if (flags.topDir !== undefined) core.top_dir = flags.topDir;
  if (flags.deployDir !== undefined) core.deploy_dir = flags.deployDir;
  if (flags.revertDir !== undefined) core.revert_dir = flags.revertDir;
  if (flags.verifyDir !== undefined) core.verify_dir = flags.verifyDir;
  if (flags.planFile !== undefined) core.plan_file = flags.planFile;
  if (flags.verify !== undefined) deploy.verify = flags.verify;
  if (flags.mode !== undefined) {
    const m = flags.mode;
    if (m === "change" || m === "tag" || m === "all") deploy.mode = m;
  }
  if (flags.lockRetries !== undefined) deploy.lock_retries = flags.lockRetries;
  if (flags.lockTimeout !== undefined) deploy.lock_timeout = flags.lockTimeout;
  if (flags.strict !== undefined) analysis.error_on_warn = flags.strict;
  if (flags.pgVersion !== undefined) analysis.pg_version = flags.pgVersion;
}
