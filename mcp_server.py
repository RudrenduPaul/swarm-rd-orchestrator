"""MCP server exposing the event log as typed tools over stdio, so an
agent can call append/pull/list-tasks programmatically instead of
shelling out to the CLI and parsing text. Run via `swarm-rd-cli mcp`.
"""

from __future__ import annotations

from mcp.server.mcpserver import MCPServer

from event_log import EventLog, InvalidDeltaError


def build_server(db_path: str) -> MCPServer:
    server = MCPServer(
        name="swarm-rd-orchestrator",
        description=(
            "Append-only, SQLite-WAL-backed event log for cross-agent "
            "context/memory sharing between parallel research agents."
        ),
    )

    @server.tool()
    def append_delta(
        task_id: str, agent_id: str, content: str, kind: str = "note"
    ) -> dict:
        """Append a delta (a finding/result/tool-output) to the event log
        for a task. Raises on missing task_id/agent_id/content."""
        log = EventLog(db_path)
        try:
            cursor = log.append_delta(
                task_id, agent_id, {"content": content, "kind": kind}
            )
        except InvalidDeltaError as exc:
            return {"error": str(exc)}
        finally:
            log.close()
        return {"cursor": cursor}

    @server.tool()
    def pull_deltas(task_id: str, since_cursor: int = 0) -> list[dict]:
        """Pull every delta for a task with id > since_cursor, oldest
        first. A cursor past the latest delta returns an empty list."""
        log = EventLog(db_path)
        deltas = log.pull_deltas(task_id, since_cursor=since_cursor)
        log.close()
        return [
            {
                "id": d.id,
                "task_id": d.task_id,
                "agent_id": d.agent_id,
                "timestamp": d.timestamp,
                "content": d.content,
                "kind": d.kind,
            }
            for d in deltas
        ]

    @server.tool()
    def list_tasks() -> list[dict]:
        """List every task_id currently in the event log, with its delta
        count."""
        log = EventLog(db_path)
        tasks = log.list_tasks()
        log.close()
        return [{"task_id": t, "delta_count": c} for t, c in tasks]

    return server


def run_mcp_server(db_path: str) -> None:
    build_server(db_path).run(transport="stdio")
