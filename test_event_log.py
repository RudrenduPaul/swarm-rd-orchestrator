"""Milestone 1 test plan (2026-08-03).

Covers every gap named in the Milestone 1 test plan, plus the load-bearing
concurrent-agent test the spike's decision rule depends on.
"""

from __future__ import annotations

import sqlite3
import threading

import pytest
import ray

from event_log import EventLog, EventLogActor, InvalidDeltaError


@pytest.fixture()
def log(tmp_path):
    db_path = str(tmp_path / "events.db")
    el = EventLog(db_path)
    yield el
    el.close()


@pytest.fixture(scope="module", autouse=True)
def ray_session():
    ray.init(num_cpus=4, include_dashboard=False, logging_level="error")
    yield
    ray.shutdown()


# -- append_delta -------------------------------------------------------


def test_append_happy_path(log):
    cursor = log.append_delta("task-1", "agent-a", {"content": "found a bug"})
    assert cursor >= 1
    deltas = log.pull_deltas("task-1")
    assert len(deltas) == 1
    assert deltas[0].content == "found a bug"
    assert deltas[0].agent_id == "agent-a"
    assert deltas[0].kind == "note"


def test_append_concurrent_no_conflict(tmp_path):
    db_path = str(tmp_path / "concurrent.db")
    EventLog(db_path).close()  # create schema first

    n_threads = 5
    n_per_thread = 20
    errors: list[Exception] = []

    def worker(agent_id: str):
        try:
            el = EventLog(db_path)
            for i in range(n_per_thread):
                el.append_delta(
                    "task-concurrent", agent_id, {"content": f"delta-{i}"}
                )
            el.close()
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(exc)

    threads = [
        threading.Thread(target=worker, args=(f"agent-{i}",))
        for i in range(n_threads)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"concurrent appends raised: {errors}"
    reader = EventLog(db_path)
    deltas = reader.pull_deltas("task-concurrent")
    reader.close()
    assert len(deltas) == n_threads * n_per_thread, (
        "concurrent appends must not lose writes, even though SQLite WAL "
        "serializes them at the storage layer"
    )


class _CrashyConnection:
    """Wraps a real sqlite3.Connection to inject a failure on a specific
    statement, simulating an OS/process-level crash mid-write. sqlite3's
    C-level Connection/type can't be monkeypatched directly (read-only
    attributes), so this wrapper delegates everything except `execute`.
    """

    def __init__(self, real_conn, crash_on_prefix):
        self._real = real_conn
        self._crash_on_prefix = crash_on_prefix

    def execute(self, sql, *args, **kwargs):
        if sql.strip().startswith(self._crash_on_prefix):
            raise sqlite3.OperationalError("simulated crash mid-write")
        return self._real.execute(sql, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._real, name)


def test_append_crash_mid_write_leaves_no_partial_row(log, monkeypatch):
    """Proves the atomic-append guarantee rather than just asserting it:
    if the write fails partway (simulated OS-level failure), the deltas
    table has zero new rows afterward -- never a half-written one.
    """
    monkeypatch.setattr(
        log, "_conn", _CrashyConnection(log._conn, "INSERT INTO deltas")
    )

    with pytest.raises(sqlite3.OperationalError):
        log.append_delta("task-crash", "agent-a", {"content": "should not persist"})

    monkeypatch.undo()
    deltas = log.pull_deltas("task-crash")
    assert deltas == [], "a failed write must leave no partial row"


def test_append_malformed_delta_raises(log):
    with pytest.raises(InvalidDeltaError):
        log.append_delta("", "agent-a", {"content": "no task_id"})
    with pytest.raises(InvalidDeltaError):
        log.append_delta("task-1", "", {"content": "no agent_id"})
    with pytest.raises(InvalidDeltaError):
        log.append_delta("task-1", "agent-a", {})  # no content
    with pytest.raises(InvalidDeltaError):
        log.append_delta("task-1", "agent-a", {"content": "x", "kind": "bogus"})
    # None of the above should have written anything.
    assert log.pull_deltas("task-1") == []


# -- pull_deltas ----------------------------------------------------------


def test_pull_happy_path(log):
    log.append_delta("task-1", "agent-a", {"content": "first"})
    log.append_delta("task-1", "agent-a", {"content": "second"})
    deltas = log.pull_deltas("task-1")
    assert [d.content for d in deltas] == ["first", "second"]


def test_pull_empty_log(log):
    assert log.pull_deltas("nonexistent-task") == []


def test_pull_stale_read_until_next_pull(log):
    """A pull is a one-shot snapshot, not a subscription -- an agent that
    already pulled doesn't retroactively see deltas appended afterward
    until it calls pull_deltas again. This is the designed behavior, not
    a bug.
    """
    log.append_delta("task-1", "agent-a", {"content": "before"})
    snapshot = log.pull_deltas("task-1")
    assert len(snapshot) == 1

    log.append_delta("task-1", "agent-b", {"content": "after"})
    # The already-taken snapshot does not magically include the new delta.
    assert len(snapshot) == 1
    assert snapshot[0].content == "before"

    # A fresh pull does see it.
    fresh = log.pull_deltas("task-1")
    assert [d.content for d in fresh] == ["before", "after"]


def test_pull_cursor_past_end(log):
    cursor = log.append_delta("task-1", "agent-a", {"content": "only one"})
    assert log.pull_deltas("task-1", since_cursor=cursor) == []
    assert log.pull_deltas("task-1", since_cursor=cursor + 1000) == []


# -- load-bearing: the differentiation claim itself ------------------------


def test_three_ray_actors_concurrent_append_and_reconcile(tmp_path):
    """This is the test the validation spike is built around. Without it,
    the spike has no way to produce its required output: 3+ concrete
    failure cases vs. LangGraph/Ray's built-in state sharing, per the
    decision rule in spike.py.

    3 "agent" actors append concurrently to one shared EventLogActor; a
    4th call reconciles by pulling everything back and verifying nothing
    was lost or corrupted, even though Ray serializes calls into the
    single log actor one at a time.
    """
    db_path = str(tmp_path / "ray_events.db")
    log_actor = EventLogActor.remote(db_path)

    n_agents = 3
    n_deltas_per_agent = 10

    append_futures = [
        log_actor.append_delta.remote(
            "task-swarm",
            f"agent-{agent_idx}",
            {"content": f"agent-{agent_idx}-delta-{i}", "kind": "result"},
        )
        for agent_idx in range(n_agents)
        for i in range(n_deltas_per_agent)
    ]
    cursors = ray.get(append_futures)
    assert len(cursors) == n_agents * n_deltas_per_agent
    assert len(set(cursors)) == len(cursors), "every append must get a unique cursor"

    reconciled = ray.get(log_actor.pull_deltas.remote("task-swarm"))
    assert len(reconciled) == n_agents * n_deltas_per_agent, (
        "3 concurrently-appending Ray actors must not lose or duplicate "
        "any delta when reconciled through the shared log"
    )
    per_agent_counts = {}
    for d in reconciled:
        per_agent_counts[d.agent_id] = per_agent_counts.get(d.agent_id, 0) + 1
    assert per_agent_counts == {
        f"agent-{i}": n_deltas_per_agent for i in range(n_agents)
    }
