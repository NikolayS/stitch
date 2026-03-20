#!/usr/bin/env bun
// sqlever — Sqitch-compatible PostgreSQL migration tool

const [, , cmd, ...args] = process.argv;

const commands: Record<string, () => void> = {
  add: () => console.error("sqlever add: not yet implemented"),
  deploy: () => console.error("sqlever deploy: not yet implemented"),
  revert: () => console.error("sqlever revert: not yet implemented"),
  verify: () => console.error("sqlever verify: not yet implemented"),
  status: () => console.error("sqlever status: not yet implemented"),
  log: () => console.error("sqlever log: not yet implemented"),
};

if (!cmd || cmd === "--help" || cmd === "-h") {
  console.log(`sqlever — Sqitch-compatible PostgreSQL migration tool

Usage:
  sqlever <command> [options]

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
  console.error(`sqlever: unknown command '${cmd}'`);
  process.exit(1);
}

handler();
