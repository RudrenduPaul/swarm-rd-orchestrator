"""Milestone 1: append-only, SQLite-WAL-backed event log for cross-agent
context/memory sharing, wrapped as a Ray actor.

Locked decisions (2026-08-03):
  - Mechanism: append-only event log keyed by task_id. Agents write deltas,
    other agents pull them. No shared mutable store, no in-place conflict
    resolution.
  - Storage: SQLite in WAL mode. WAL gives concurrent readers alongside one
    writer; concurrent writers still serialize (SQLITE_BUSY retries), which
    is a correctness win (no corrupted/lost writes), not a parallelism win.
  - Delta shape: {task_id, agent_id, timestamp, content, kind}.
  - Malformed delta (missing task_id/agent_id/content) raises
    InvalidDeltaError before touching SQLite -- no silent drop, no partial
    write.
"""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from typing import Any

import ray

VALID_KINDS = {"note", "result", "tool_output"}
REQUIRED_FIELDS = ("task_id", "agent_id", "content")


class InvalidDeltaError(ValueError):
    """Raised when a delta is missing a required field."""


@dataclass(frozen=True)
class Delta:
    id: int
    task_id: str
    agent_id: str
    timestamp: float
    content: str
    kind: str


def _validate_delta(task_id: str, agent_id: str, delta: dict[str, Any]) -> None:
    if not task_id:
        raise InvalidDeltaError("task_id is required")
    if not agent_id:
        raise InvalidDeltaError("agent_id is required")
    if not delta.get("content"):
        raise InvalidDeltaError("delta.content is required")
    kind = delta.get("kind", "note")
    if kind not in VALID_KINDS:
        raise InvalidDeltaError(
            f"delta.kind must be one of {sorted(VALID_KINDS)}, got {kind!r}"
        )


class EventLog:
    """Plain (non-Ray) event log. Unit-testable directly; the Ray actor
    below is a thin wrapper so the concurrency-relevant logic isn't hidden
    behind Ray's remote-call machinery during tests.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS deltas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL,
                agent_id TEXT NOT NULL,
                timestamp REAL NOT NULL,
                content TEXT NOT NULL,
                kind TEXT NOT NULL
            )
            """
        )

    def append_delta(self, task_id: str, agent_id: str, delta: dict[str, Any]) -> int:
        """Validate then atomically append. Returns the new row id (cursor
        position). Raises InvalidDeltaError before any write on bad input.
        """
        _validate_delta(task_id, agent_id, delta)
        kind = delta.get("kind", "note")
        ts = delta.get("timestamp", time.time())
        cur = self._conn.execute(
            "INSERT INTO deltas (task_id, agent_id, timestamp, content, kind) "
            "VALUES (?, ?, ?, ?, ?)",
            (task_id, agent_id, ts, delta["content"], kind),
        )
        return cur.lastrowid

    def pull_deltas(self, task_id: str, since_cursor: int = 0) -> list[Delta]:
        """Return every delta for task_id with id > since_cursor, oldest
        first. A cursor past the latest delta just returns an empty list --
        not an error.
        """
        rows = self._conn.execute(
            "SELECT id, task_id, agent_id, timestamp, content, kind "
            "FROM deltas WHERE task_id = ? AND id > ? ORDER BY id ASC",
            (task_id, since_cursor),
        ).fetchall()
        return [Delta(*row) for row in rows]

    def list_tasks(self) -> list[tuple[str, int]]:
        """Every distinct task_id with its delta count."""
        rows = self._conn.execute(
            "SELECT task_id, COUNT(*) FROM deltas GROUP BY task_id"
        ).fetchall()
        return [(row[0], row[1]) for row in rows]

    def close(self) -> None:
        self._conn.close()


@ray.remote
class EventLogActor:
    """Ray actor wrapping EventLog. One actor per event-log file is the
    intended usage -- Ray serializes calls to a single actor instance, so
    this actor itself is the natural point where concurrent agent appends
    get funneled before hitting SQLite.
    """

    def __init__(self, db_path: str):
        self._log = EventLog(db_path)

    def append_delta(self, task_id: str, agent_id: str, delta: dict[str, Any]) -> int:
        return self._log.append_delta(task_id, agent_id, delta)

    def pull_deltas(self, task_id: str, since_cursor: int = 0) -> list[Delta]:
        return self._log.pull_deltas(task_id, since_cursor)
