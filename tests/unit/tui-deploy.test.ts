import { describe, it, expect } from "bun:test";
import {
  DeployProgress,
  formatDuration,
  shouldUseTUI,
} from "../../src/tui/deploy";

// ---------------------------------------------------------------------------
// Helper — capture output from DeployProgress via writer callback
// ---------------------------------------------------------------------------

function createCapture() {
  let output = "";
  return {
    writer: (s: string) => { output += s; },
    get output() { return output; },
    reset() { output = ""; },
  };
}

// ---------------------------------------------------------------------------
// formatDuration()
// ---------------------------------------------------------------------------

describe("formatDuration()", () => {
  it("formats milliseconds under 1 second", () => {
    expect(formatDuration(12)).toBe("12ms");
    expect(formatDuration(0)).toBe("0ms");
    expect(formatDuration(999)).toBe("999ms");
  });

  it("formats seconds under 60", () => {
    expect(formatDuration(1000)).toBe("1.0s");
    expect(formatDuration(1500)).toBe("1.5s");
    expect(formatDuration(59999)).toBe("60.0s");
  });

  it("formats minutes", () => {
    expect(formatDuration(60000)).toBe("1m0s");
    expect(formatDuration(90000)).toBe("1m30s");
    expect(formatDuration(125000)).toBe("2m5s");
  });
});

// ---------------------------------------------------------------------------
// shouldUseTUI()
// ---------------------------------------------------------------------------

describe("shouldUseTUI()", () => {
  it("returns true when TTY and no flags", () => {
    expect(shouldUseTUI({ isTTY: true })).toBe(true);
  });

  it("returns false when not a TTY", () => {
    expect(shouldUseTUI({ isTTY: false })).toBe(false);
  });

  it("returns false when --no-tui is set", () => {
    expect(shouldUseTUI({ isTTY: true, noTui: true })).toBe(false);
  });

  it("returns false when --quiet is set", () => {
    expect(shouldUseTUI({ isTTY: true, quiet: true })).toBe(false);
  });

  it("returns false when both --quiet and --no-tui are set", () => {
    expect(shouldUseTUI({ isTTY: true, quiet: true, noTui: true })).toBe(false);
  });

  it("returns false when TTY is false even with no flags", () => {
    expect(shouldUseTUI({ isTTY: false, noTui: false, quiet: false })).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// DeployProgress — plain text mode (non-TTY)
// ---------------------------------------------------------------------------

describe("DeployProgress (plain text)", () => {
  it("emits start line with total changes", () => {
    const cap = createCapture();
    const dp = new DeployProgress({ isTTY: false, writer: cap.writer });
    dp.start(5);
    expect(cap.output).toBe("Deploying 5 change(s)...\n");
  });

  it("emits status lines for each change update", () => {
    const cap = createCapture();
    const dp = new DeployProgress({ isTTY: false, writer: cap.writer });
    dp.start(2);
    cap.reset();

    dp.updateChange("create_users", "running");
    expect(cap.output).toContain("[>] create_users");
    expect(cap.output).toContain("running");

    cap.reset();
    dp.updateChange("create_users", "done", 42);
    expect(cap.output).toContain("[+] create_users");
    expect(cap.output).toContain("done");
    expect(cap.output).toContain("42ms");
  });

  it("emits failed status with icon", () => {
    const cap = createCapture();
    const dp = new DeployProgress({ isTTY: false, writer: cap.writer });
    dp.start(1);
    cap.reset();

    dp.updateChange("bad_migration", "failed", 100);
    expect(cap.output).toContain("[!] bad_migration");
    expect(cap.output).toContain("failed");
  });

  it("emits pending status with icon", () => {
    const cap = createCapture();
    const dp = new DeployProgress({ isTTY: false, writer: cap.writer });
    dp.start(1);
    cap.reset();

    dp.updateChange("waiting", "pending");
    expect(cap.output).toContain("[ ] waiting");
    expect(cap.output).toContain("pending");
  });

  it("emits summary on finish", () => {
    const cap = createCapture();
    const dp = new DeployProgress({ isTTY: false, writer: cap.writer });
    dp.start(3);
    dp.updateChange("a", "done", 10);
    dp.updateChange("b", "done", 20);
    dp.updateChange("c", "done", 30);
    cap.reset();

    dp.finish({
      totalDeployed: 3,
      totalFailed: 0,
      totalSkipped: 0,
      elapsedMs: 60,
    });
    expect(cap.output).toContain("Deploy complete");
    expect(cap.output).toContain("3 deployed");
    expect(cap.output).toContain("60ms");
  });

  it("includes failed and skipped counts in summary", () => {
    const cap = createCapture();
    const dp = new DeployProgress({ isTTY: false, writer: cap.writer });
    dp.start(3);
    cap.reset();

    dp.finish({
      totalDeployed: 1,
      totalFailed: 1,
      totalSkipped: 1,
      elapsedMs: 500,
    });
    expect(cap.output).toContain("1 deployed");
    expect(cap.output).toContain("1 failed");
    expect(cap.output).toContain("1 skipped");
  });

  it("includes warning count in summary when warnings were added", () => {
    const cap = createCapture();
    const dp = new DeployProgress({ isTTY: false, writer: cap.writer });
    dp.start(1);
    dp.addWarning("SA004: missing CONCURRENT");
    dp.updateChange("idx", "done", 10);
    cap.reset();

    dp.finish({
      totalDeployed: 1,
      totalFailed: 0,
      totalSkipped: 0,
      elapsedMs: 10,
    });
    expect(cap.output).toContain("1 warning(s)");
  });
});

// ---------------------------------------------------------------------------
// DeployProgress — TTY mode
// ---------------------------------------------------------------------------

describe("DeployProgress (TTY mode)", () => {
  it("draws initial state with pending slots", () => {
    const cap = createCapture();
    const dp = new DeployProgress({ isTTY: true, writer: cap.writer });
    dp.start(3);

    // Should contain pending indicators
    expect(cap.output).toContain("(pending)");
    // Should contain progress bar at 0%
    expect(cap.output).toContain("0%");
    expect(cap.output).toContain("0/3 changes");
  });

  it("redraws when a change status updates", () => {
    const cap = createCapture();
    const dp = new DeployProgress({ isTTY: true, writer: cap.writer });
    dp.start(2);
    cap.reset();

    dp.updateChange("create_users", "running");
    // Should contain ANSI escape for cursor movement (redraw)
    expect(cap.output).toContain("\x1B[");
    // Should contain the change name
    expect(cap.output).toContain("create_users");
    // Should contain running indicator arrow
    expect(cap.output).toContain("[→]");
  });

  it("shows done status with green checkmark", () => {
    const cap = createCapture();
    const dp = new DeployProgress({ isTTY: true, writer: cap.writer });
    dp.start(1);
    cap.reset();

    dp.updateChange("create_users", "done", 42);
    expect(cap.output).toContain("[✓]");
    expect(cap.output).toContain("42ms");
    // Progress should show 100%
    expect(cap.output).toContain("100%");
    expect(cap.output).toContain("1/1 changes");
  });

  it("shows failed status with red X", () => {
    const cap = createCapture();
    const dp = new DeployProgress({ isTTY: true, writer: cap.writer });
    dp.start(1);
    cap.reset();

    dp.updateChange("bad_migration", "failed", 100);
    expect(cap.output).toContain("[✗]");
    expect(cap.output).toContain("bad_migration");
  });

  it("displays analysis warnings in TTY", () => {
    const cap = createCapture();
    const dp = new DeployProgress({ isTTY: true, writer: cap.writer });
    dp.start(1);
    cap.reset();

    dp.addWarning("SA004: missing CONCURRENT");
    expect(cap.output).toContain("1 warning(s)");
    expect(cap.output).toContain("SA004: missing CONCURRENT");
  });

  it("updates existing change record rather than adding duplicate", () => {
    const cap = createCapture();
    const dp = new DeployProgress({ isTTY: true, writer: cap.writer });
    dp.start(2);

    dp.updateChange("create_users", "running");
    dp.updateChange("create_users", "done", 50);

    // buildLines should show create_users only once
    const lines = dp.buildLines();
    const userLines = lines.filter((l: string) => l.includes("create_users"));
    expect(userLines.length).toBe(1);
    expect(userLines[0]).toContain("[✓]");
  });

  it("builds correct progress bar at 50%", () => {
    const cap = createCapture();
    const dp = new DeployProgress({ isTTY: true, writer: cap.writer });
    dp.start(4);
    dp.updateChange("a", "done", 10);
    dp.updateChange("b", "done", 10);
    dp.updateChange("c", "running");

    const bar = dp.buildProgressBar();
    // 2 completed out of 4 = 50%
    expect(bar).toContain("50%");
    expect(bar).toContain("2/4 changes");
    // Should have filled and empty blocks
    expect(bar).toContain("█");
    expect(bar).toContain("░");
  });

  it("builds progress bar at 100%", () => {
    const cap = createCapture();
    const dp = new DeployProgress({ isTTY: true, writer: cap.writer });
    dp.start(2);
    dp.updateChange("a", "done", 10);
    dp.updateChange("b", "done", 20);

    const bar = dp.buildProgressBar();
    expect(bar).toContain("100%");
    expect(bar).toContain("2/2 changes");
    // All filled
    expect(bar).not.toContain("░");
  });

  it("counts failed changes as completed in progress bar", () => {
    const cap = createCapture();
    const dp = new DeployProgress({ isTTY: true, writer: cap.writer });
    dp.start(2);
    dp.updateChange("a", "done", 10);
    dp.updateChange("b", "failed", 5);

    const bar = dp.buildProgressBar();
    expect(bar).toContain("100%");
    expect(bar).toContain("2/2 changes");
  });

  it("formats change lines for all statuses", () => {
    const dp = new DeployProgress({ isTTY: true, writer: () => {} });

    const done = dp.formatChangeLine({ name: "test", status: "done", durationMs: 42 });
    expect(done).toContain("[✓]");
    expect(done).toContain("42ms");

    const running = dp.formatChangeLine({ name: "test", status: "running" });
    expect(running).toContain("[→]");
    expect(running).toContain("running...");

    const failed = dp.formatChangeLine({ name: "test", status: "failed" });
    expect(failed).toContain("[✗]");
    expect(failed).toContain("failed");

    const pending = dp.formatChangeLine({ name: "test", status: "pending" });
    expect(pending).toContain("[ ]");
    expect(pending).toContain("pending");
  });

  it("clears previous lines on redraw with cursor-up escape", () => {
    const cap = createCapture();
    const dp = new DeployProgress({ isTTY: true, writer: cap.writer });
    dp.start(1);
    cap.reset();

    // Second draw should move cursor up
    dp.updateChange("a", "running");
    expect(cap.output).toContain("\x1B[");
    // Should contain line erase sequence
    expect(cap.output).toContain("\x1B[2K\r");
  });

  it("emits trailing newline on finish in TTY mode", () => {
    const cap = createCapture();
    const dp = new DeployProgress({ isTTY: true, writer: cap.writer });
    dp.start(1);
    dp.updateChange("a", "done", 10);
    cap.reset();

    dp.finish({
      totalDeployed: 1,
      totalFailed: 0,
      totalSkipped: 0,
      elapsedMs: 10,
    });
    // Should contain summary line
    expect(cap.output).toContain("Deploy complete");
  });
});

// ---------------------------------------------------------------------------
// DeployProgress — edge cases
// ---------------------------------------------------------------------------

describe("DeployProgress edge cases", () => {
  it("handles zero total changes", () => {
    const cap = createCapture();
    const dp = new DeployProgress({ isTTY: false, writer: cap.writer });
    dp.start(0);
    dp.finish({
      totalDeployed: 0,
      totalFailed: 0,
      totalSkipped: 0,
      elapsedMs: 0,
    });
    expect(cap.output).toContain("Deploy complete");
  });

  it("handles duration formatting at boundary", () => {
    // Exactly 1 second
    expect(formatDuration(1000)).toBe("1.0s");
    // Exactly 1 minute
    expect(formatDuration(60000)).toBe("1m0s");
  });
});
