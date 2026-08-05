"""CLI for swarm-rd-orchestrator: inspect and drive the SQLite-backed event
log directly (append/pull/list), and run the Ray-actor spike. Structured
JSON output on every data-returning command via --json, so an agent
shelling out to this CLI can parse stdout without screen-scraping.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys

from event_log import EventLog, InvalidDeltaError

DEFAULT_DB = "swarm-events.db"


def _delta_to_dict(d) -> dict:
    return dataclasses.asdict(d)


def cmd_append(args: argparse.Namespace) -> int:
    log = EventLog(args.db)
    try:
        cursor = log.append_delta(
            args.task_id,
            args.agent_id,
            {"content": args.content, "kind": args.kind},
        )
    except InvalidDeltaError as exc:
        if args.json:
            print(json.dumps({"error": str(exc)}))
        else:
            print(f"error: {exc}", file=sys.stderr)
        return 1
    finally:
        log.close()

    if args.json:
        print(json.dumps({"cursor": cursor}))
    else:
        print(f"appended at cursor {cursor}")
    return 0


def cmd_pull(args: argparse.Namespace) -> int:
    log = EventLog(args.db)
    deltas = log.pull_deltas(args.task_id, since_cursor=args.since)
    log.close()

    if args.json:
        print(json.dumps([_delta_to_dict(d) for d in deltas]))
    else:
        for d in deltas:
            print(f"[{d.id}] ({d.agent_id}/{d.kind}) {d.content}")
        if not deltas:
            print("(no deltas)")
    return 0


def cmd_list_tasks(args: argparse.Namespace) -> int:
    log = EventLog(args.db)
    rows = log.list_tasks()
    log.close()

    if args.json:
        print(json.dumps([{"task_id": r[0], "delta_count": r[1]} for r in rows]))
    else:
        for task_id, count in rows:
            print(f"{task_id}: {count} deltas")
        if not rows:
            print("(no tasks)")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="swarm-rd-cli")
    parser.add_argument(
        "--db", default=DEFAULT_DB, help=f"path to the event log (default: {DEFAULT_DB})"
    )
    parser.add_argument(
        "--json", action="store_true", help="structured JSON output (for agent/script use)"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_append = sub.add_parser("append", help="append a delta to the event log")
    p_append.add_argument("task_id")
    p_append.add_argument("agent_id")
    p_append.add_argument("content")
    p_append.add_argument("--kind", default="note", choices=["note", "result", "tool_output"])
    p_append.set_defaults(func=cmd_append)

    p_pull = sub.add_parser("pull", help="pull deltas for a task")
    p_pull.add_argument("task_id")
    p_pull.add_argument("--since", type=int, default=0, help="cursor to pull after")
    p_pull.set_defaults(func=cmd_pull)

    p_list = sub.add_parser("list-tasks", help="list every task_id with a delta count")
    p_list.set_defaults(func=cmd_list_tasks)

    p_mcp = sub.add_parser("mcp", help="run as an MCP server over stdio")
    p_mcp.set_defaults(func=None)  # handled specially in main()

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "mcp":
        from mcp_server import run_mcp_server

        run_mcp_server(args.db)
        return 0

    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
