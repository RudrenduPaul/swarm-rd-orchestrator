#!/usr/bin/env node
"use strict";

// Thin passthrough wrapper: this package does NOT bundle or reimplement
// the CLI, and does NOT auto-install anything via a postinstall script
// (auto-running `pip install` at install time is a supply-chain smell,
// not a convenience worth the risk). It requires the Python package
// already installed (`pip install swarm-rd-orchestrator-cli`) and simply
// execs the real `swarm-rd-cli` binary with your args, inheriting stdio.

const { spawnSync, execFileSync } = require("node:child_process");

function findPythonCli() {
  const finder = process.platform === "win32" ? "where" : "which";
  try {
    const out = execFileSync(finder, ["swarm-rd-cli"], { encoding: "utf8" });
    return out.split("\n")[0].trim();
  } catch {
    return null;
  }
}

const binPath = findPythonCli();

if (!binPath) {
  process.stderr.write(
    "swarm-rd-cli: the Python CLI isn't installed or isn't on PATH.\n" +
      "This npm package is a thin wrapper, not a bundled reimplementation.\n" +
      "Install the Python package first:\n\n" +
      "  pip install swarm-rd-orchestrator-cli\n\n" +
      "Then re-run this command.\n"
  );
  process.exit(1);
}

// args array, never a shell string -- no injection surface regardless of
// what the caller passes as arguments.
const result = spawnSync(binPath, process.argv.slice(2), {
  stdio: "inherit",
});

if (result.error) {
  process.stderr.write(`swarm-rd-cli: failed to run ${binPath}: ${result.error.message}\n`);
  process.exit(1);
}

process.exit(result.status === null ? 1 : result.status);
