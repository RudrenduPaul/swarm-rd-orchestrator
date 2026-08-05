# swarm-rd-orchestrator-cli

**Ray-native context/memory sharing for parallel research agents, with an
agent-native CLI and MCP server.** An append-only, SQLite-WAL-backed event
log wrapped as a Ray actor, so parallel agents can write findings and pull
each other's without a shared mutable store — plus a CLI and MCP server so
both humans and other agents can drive it directly.

This is a Milestone 1 prototype (2026-08-03) — validate the approach on a
real task before building further. See "Locked decisions" below and
`spike.py` for the actual validation harness.

## Install

```bash
pip install swarm-rd-orchestrator-cli
# or
npm install -g swarm-rd-orchestrator-cli
```

Either gives you a `swarm-rd-cli` command on your `PATH`. The npm package is
a thin wrapper around the Python CLI (it execs the real binary, it does not
reimplement it) — install the Python package too if you use the npm one.

## Quickstart

```bash
swarm-rd-cli append task-1 agent-a "found a race condition in the retry loop" --kind result
swarm-rd-cli append task-1 agent-b "confirmed: retry loop isn't holding the lock" --kind result
swarm-rd-cli pull task-1
# [1] (agent-a/result) found a race condition in the retry loop
# [2] (agent-b/result) confirmed: retry loop isn't holding the lock

swarm-rd-cli --json pull task-1     # structured output for scripts/agents
swarm-rd-cli list-tasks             # every task_id with a delta count
swarm-rd-cli mcp                    # run as an MCP server over stdio
```

Every data-returning command supports `--json` for agent/script consumption
— no screen-scraping required. The `mcp` subcommand exposes `append_delta`,
`pull_deltas`, and `list_tasks` as typed MCP tools over stdio, so an agent
can call this programmatically instead of shelling out.

## Command reference

Generated from the CLI's own `--help` output:

```
usage: swarm-rd-cli [-h] [--db DB] [--json] {append,pull,list-tasks,mcp} ...

positional arguments:
  {append,pull,list-tasks,mcp}
    append              append a delta to the event log
    pull                pull deltas for a task
    list-tasks          list every task_id with a delta count
    mcp                 run as an MCP server over stdio

options:
  -h, --help            show this help message and exit
  --db DB               path to the event log (default: swarm-events.db)
  --json                structured JSON output (for agent/script use)
```

```
usage: swarm-rd-cli append [-h] [--kind {note,result,tool_output}]
                            task_id agent_id content

positional arguments:
  task_id
  agent_id
  content

options:
  -h, --help            show this help message and exit
  --kind {note,result,tool_output}
```

```
usage: swarm-rd-cli pull [-h] [--since SINCE] task_id

positional arguments:
  task_id

options:
  -h, --help     show this help message and exit
  --since SINCE  cursor to pull after
```

## How this differs from `swarmmesh`

`swarmmesh` (a sibling project in this author's portfolio, also published as
`swarmmesh-cli` on PyPI/npm) already does shared context/memory across
parallel agents — over HTTP, with SQLite-backed persistence, an MCP server,
and cross-language (Python + Node) support. This project exists as a
deliberately **Ray-native** alternative: actor-model distributed compute
instead of an HTTP server, aimed at the "thousands of parallel agents"
distributed-orchestration case Ray is actually built for. If that
distinction doesn't end up mattering for your use case, `swarmmesh` is the
more complete, more battle-tested option today — check it out first.

## Build from source

```bash
git clone https://github.com/RudrenduPaul/swarm-rd-orchestrator.git
cd swarm-rd-orchestrator
python3 -m venv .venv
.venv/bin/pip install -e ".[dev,mcp]"
```

Requires Python >=3.10 (needed for the `mcp` SDK dependency).

## Run the tests

```bash
.venv/bin/pytest test_event_log.py -v
```

9/9 passing, including the load-bearing test: 3 concurrent Ray actors
appending to one shared event log, reconciled with zero lost or duplicated
deltas.

## Run the actual validation spike

Edit `REAL_TASK_ID` / `REAL_TASK_DESCRIPTION` / the sample agent findings in
`spike.py` to reflect a real research task you're doing, then:

```bash
.venv/bin/python3 spike.py
```

Read the printed rubric at the end. The decision rule: fewer than 3
qualifying architectural failure cases against raw Ray/LangGraph → fall
back to a thin CLI wrapper instead of building this out further.

## Locked decisions (2026-08-03)

- Primitive: Ray (actor model + object store), LangGraph as fallback
- Storage: SQLite, WAL mode
- Delta shape: `{task_id, agent_id, timestamp, content, kind}`
- Malformed delta → `InvalidDeltaError`, never silent
- License: Apache 2.0
- Python: >=3.10 (for the `mcp` SDK)

## FAQ

**Q: What does this actually do?**
A: Gives parallel AI agents (running as Ray actors) a shared, durable place
to write findings and pull each other's, without stepping on each other's
state. It's a transport/persistence layer, not an orchestration framework —
it doesn't schedule agents or decide what they do.

**Q: How is this different from `swarmmesh`?**
A: See "How this differs from swarmmesh" above — same core idea, different
transport (Ray actors vs. HTTP), and swarmmesh is currently the more
complete option.

**Q: Does this need my own API keys?**
A: No. This project has no LLM calls of its own — it's a coordination layer
your own agents (whatever model/framework they use) write to and read from.

**Q: Is this safe to depend on?**
A: This is a pre-validation Milestone 1 spike, not a stable release —
version `0.0.1`. The event log's core guarantees (atomic append, no
partial writes) are tested (see `test_event_log.py`), but the project
hasn't been validated against real multi-agent workloads beyond the spike
script yet.

**Q: How do I use this from an agent, not just a human?**
A: Either shell out to the CLI with `--json` on every command, or run
`swarm-rd-cli mcp` and connect to it as an MCP server — both give
structured, parseable output.

**Q: Is this a library or just a CLI?**
A: Both. `event_log.py`'s `EventLog` and `EventLogActor` classes are
importable directly if you're already in Python and don't need the CLI/MCP
layer.
