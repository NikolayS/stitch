#!/usr/bin/env bun
// stitch — Sqitch-compatible PostgreSQL migration tool

const [, , cmd, ...args] = process.argv;

const commands: Record<string, () => void> = {
  add: () => console.error("stitch add: not yet implemented"),
  deploy: () => console.error("stitch deploy: not yet implemented"),
  revert: () => console.error("stitch revert: not yet implemented"),
  verify: () => console.error("stitch verify: not yet implemented"),
  status: () => console.error("stitch status: not yet implemented"),
  log: () => console.error("stitch log: not yet implemented"),
};

if (!cmd || cmd === "--help" || cmd === "-h") {
  console.log(`stitch — Sqitch-compatible PostgreSQL migration tool

Usage:
  stitch <command> [options]

Commands:
  add       Add a new migration
  deploy    Deploy migrations
  revert    Revert migrations
  verify    Verify deployed migrations
  status    Show deployment status
  log       Show deployment log

Not yet implemented — contributions welcome.
https://github.com/NikolayS/stitch
`);
  process.exit(0);
}

const handler = commands[cmd];
if (!handler) {
  console.error(`stitch: unknown command '${cmd}'`);
  process.exit(1);
}

handler();
