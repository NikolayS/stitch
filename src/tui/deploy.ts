// src/tui/deploy.ts — Interactive TUI dashboard for deploy progress
//
// When stdout is a TTY, shows a live-updating dashboard with ANSI escape
// codes. Falls back to plain text when piped, --no-tui, or --quiet.
//
// See SPEC.md Section 5.3 — TUI interactive deployment dashboard.
// No external dependencies — pure ANSI escape codes.

// ---------------------------------------------------------------------------
// ANSI escape helpers
// ---------------------------------------------------------------------------

/** Erase the current line and move cursor to column 0. */
const ERASE_LINE = "\x1B[2K\r";

/** Move cursor up N lines. */
function cursorUp(n: number): string {
  return n > 0 ? `\x1B[${n}A` : "";
}

/** ANSI color helpers (foreground). */
const ANSI = {
  green: (s: string) => `\x1B[32m${s}\x1B[0m`,
  yellow: (s: string) => `\x1B[33m${s}\x1B[0m`,
  red: (s: string) => `\x1B[31m${s}\x1B[0m`,
  dim: (s: string) => `\x1B[2m${s}\x1B[0m`,
  bold: (s: string) => `\x1B[1m${s}\x1B[0m`,
};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Status of a single change during deploy. */
export type ChangeStatus = "pending" | "running" | "done" | "failed";

/** Internal record for a tracked change. */
interface ChangeRecord {
  name: string;
  status: ChangeStatus;
  durationMs?: number;
}

/** Summary passed to finish(). */
export interface DeploySummary {
  totalDeployed: number;
  totalFailed: number;
  totalSkipped: number;
  elapsedMs: number;
}

// ---------------------------------------------------------------------------
// DeployProgress class
// ---------------------------------------------------------------------------

/**
 * Live deploy progress dashboard.
 *
 * When `isTTY` is true, redraws the dashboard in-place using ANSI escape
 * codes. When false, emits one line per status change (plain text).
 */
export class DeployProgress {
  private changes: ChangeRecord[] = [];
  private totalChanges = 0;
  private warnings: string[] = [];
  private linesDrawn = 0;
  private isTTY: boolean;
  private writer: (s: string) => void;
  constructor(options?: {
    /** Override TTY detection (useful for testing). */
    isTTY?: boolean;
    /** Override output writer (useful for testing). */
    writer?: (s: string) => void;
  }) {
    this.isTTY = options?.isTTY ?? (process.stdout.isTTY === true);
    this.writer = options?.writer ?? ((s: string) => process.stdout.write(s));
  }

  // -----------------------------------------------------------------------
  // Public API
  // -----------------------------------------------------------------------

  /** Initialize the dashboard with the total number of changes to deploy. */
  start(totalChanges: number): void {
    this.totalChanges = totalChanges;
    this.changes = [];
    this.warnings = [];
    this.linesDrawn = 0;

    if (this.isTTY) {
      this.draw();
    } else {
      this.writer(`Deploying ${totalChanges} change(s)...\n`);
    }
  }

  /**
   * Update the status of a change.
   *
   * If the change hasn't been seen before, it is added to the list.
   * In TTY mode the entire dashboard is redrawn. In plain mode a single
   * line is emitted for each status transition.
   */
  updateChange(name: string, status: ChangeStatus, durationMs?: number): void {
    let record = this.changes.find((c) => c.name === name);
    if (!record) {
      record = { name, status, durationMs };
      this.changes.push(record);
    } else {
      record.status = status;
      record.durationMs = durationMs ?? record.durationMs;
    }

    if (this.isTTY) {
      this.draw();
    } else {
      this.writePlainStatus(record);
    }
  }

  /** Add a warning message (e.g. from analysis). */
  addWarning(message: string): void {
    this.warnings.push(message);
    if (this.isTTY) {
      this.draw();
    }
    // In plain mode, warnings are shown at finish
  }

  /** Finalize the dashboard with a summary. */
  finish(summary: DeploySummary): void {
    if (this.isTTY) {
      this.draw();
      this.writer("\n");
    }
    this.writeSummaryLine(summary);
  }

  // -----------------------------------------------------------------------
  // TTY rendering
  // -----------------------------------------------------------------------

  /** Build the full dashboard as an array of lines. */
  buildLines(): string[] {
    const lines: string[] = [];

    // Change list
    for (const change of this.changes) {
      lines.push(this.formatChangeLine(change));
    }

    // Show remaining pending slots
    const remaining = this.totalChanges - this.changes.length;
    for (let i = 0; i < remaining; i++) {
      lines.push(ANSI.dim("  [ ] (pending)"));
    }

    // Blank separator
    if (this.changes.length > 0 || remaining > 0) {
      lines.push("");
    }

    // Warnings summary
    if (this.warnings.length > 0) {
      const warnCount = this.warnings.length;
      lines.push(`  Analysis: ${ANSI.yellow(`${warnCount} warning(s)`)}`);
      for (const w of this.warnings) {
        lines.push(`    ${ANSI.yellow("!")} ${w}`);
      }
    } else {
      lines.push(ANSI.dim("  Analysis: 0 warnings"));
    }

    // Progress bar
    lines.push("  " + this.buildProgressBar());

    return lines;
  }

  /** Clear previous drawing and redraw all lines. */
  private draw(): void {
    // Move cursor up to overwrite previous output
    if (this.linesDrawn > 0) {
      this.writer(cursorUp(this.linesDrawn));
    }

    const lines = this.buildLines();

    for (const line of lines) {
      this.writer(ERASE_LINE + line + "\n");
    }

    this.linesDrawn = lines.length;
  }

  // -----------------------------------------------------------------------
  // Formatting helpers
  // -----------------------------------------------------------------------

  /** Format a single change line with status icon, name, and duration. */
  formatChangeLine(change: ChangeRecord): string {
    const duration = change.durationMs != null
      ? formatDuration(change.durationMs)
      : "";

    switch (change.status) {
      case "done":
        return `  ${ANSI.green("[✓]")} ${change.name}  ${ANSI.dim(duration)}`;
      case "running":
        return `  ${ANSI.yellow("[→]")} ${change.name}  ${ANSI.yellow("running...")}`;
      case "failed":
        return `  ${ANSI.red("[✗]")} ${change.name}  ${ANSI.red("failed")}`;
      case "pending":
      default:
        return `  ${ANSI.dim("[ ]")} ${ANSI.dim(change.name)}  ${ANSI.dim("pending")}`;
    }
  }

  /** Build the progress bar string: `████████░░░░ 50% (3/6 changes)` */
  buildProgressBar(): string {
    const completed = this.changes.filter(
      (c) => c.status === "done" || c.status === "failed",
    ).length;
    const total = this.totalChanges || 1; // avoid division by zero
    const pct = Math.round((completed / total) * 100);

    const barWidth = 20;
    const filled = Math.round((completed / total) * barWidth);
    const empty = barWidth - filled;

    const bar = "█".repeat(filled) + "░".repeat(empty);

    return `Progress: ${bar} ${pct}% (${completed}/${this.totalChanges} changes)`;
  }

  // -----------------------------------------------------------------------
  // Plain text output
  // -----------------------------------------------------------------------

  /** Write a single status line for non-TTY output. */
  private writePlainStatus(change: ChangeRecord): void {
    const icon = statusIcon(change.status);
    const duration = change.durationMs != null
      ? ` (${formatDuration(change.durationMs)})`
      : "";
    this.writer(`${icon} ${change.name} — ${change.status}${duration}\n`);
  }

  /** Write the final summary line. */
  private writeSummaryLine(summary: DeploySummary): void {
    const elapsed = formatDuration(summary.elapsedMs);
    const parts: string[] = [];
    if (summary.totalDeployed > 0) {
      parts.push(`${summary.totalDeployed} deployed`);
    }
    if (summary.totalFailed > 0) {
      parts.push(`${summary.totalFailed} failed`);
    }
    if (summary.totalSkipped > 0) {
      parts.push(`${summary.totalSkipped} skipped`);
    }
    if (this.warnings.length > 0) {
      parts.push(`${this.warnings.length} warning(s)`);
    }
    const line = parts.length > 0
      ? `Deploy complete: ${parts.join(", ")} in ${elapsed}`
      : `Deploy complete in ${elapsed}`;
    this.writer(line + "\n");
  }
}

// ---------------------------------------------------------------------------
// Shared utilities
// ---------------------------------------------------------------------------

/** Format milliseconds as a human-friendly duration string. */
export function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  const seconds = ms / 1000;
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const minutes = Math.floor(seconds / 60);
  const remainingSecs = Math.round(seconds % 60);
  return `${minutes}m${remainingSecs}s`;
}

/** Return a plain-text status icon for non-TTY output. */
function statusIcon(status: ChangeStatus): string {
  switch (status) {
    case "done":
      return "[+]";
    case "running":
      return "[>]";
    case "failed":
      return "[!]";
    case "pending":
    default:
      return "[ ]";
  }
}

// ---------------------------------------------------------------------------
// TTY detection helper for CLI integration
// ---------------------------------------------------------------------------

/**
 * Determine whether the TUI should be used.
 *
 * Returns true when:
 *  - stdout is a TTY
 *  - --no-tui was NOT passed
 *  - --quiet was NOT set
 */
export function shouldUseTUI(options: {
  noTui?: boolean;
  quiet?: boolean;
  isTTY?: boolean;
}): boolean {
  if (options.quiet) return false;
  if (options.noTui) return false;
  const tty = options.isTTY ?? (process.stdout.isTTY === true);
  return tty;
}
