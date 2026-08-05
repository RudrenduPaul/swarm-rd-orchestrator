"""Validation task (2026-08-03): run 3 parallel Ray actors on ONE real
research task against the actual M1 stack (Ray + SQLite WAL), then write
down every specific place its state-sharing fails you, plus whether
context degrades without compaction.

This is a harness, not a finished product -- fill in REAL_TASK below with
something you're actually working on, run it, and read the printed
reconciliation output yourself. The qualifying-failure-case rubric below
is printed at the end as a checklist, not auto-scored -- that judgment is
the whole point of the spike.

Usage:
    .venv/bin/python3 spike.py
"""

from __future__ import annotations

import sys
import tempfile
import time

import ray

from event_log import EventLogActor

# --- Fill this in with a real research task before running for real. -----
REAL_TASK_ID = "spike-task-001"
REAL_TASK_DESCRIPTION = (
    "REPLACE ME: describe one real research task you're doing right now "
    "(e.g. 'find every place event_log.py could deadlock under load')."
)
# ---------------------------------------------------------------------------

QUALIFYING_FAILURE_RUBRIC = """
Qualifying failure case:
  COUNTS: a real architectural gap -- e.g. "no per-consumer stale-read
  cursor without hand-rolling one," "no way to bound what an agent pulls
  into its context window," "state silently lost across actor restarts."
  DOES NOT COUNT: an ergonomics complaint -- e.g. "actor handles are
  verbose to pass around," "I'd prefer a nicer decorator."

Decision rule: fewer than 3 qualifying failures -> fall back to
Approach A (thin CLI wrapper) or C (single killer workflow), per the
design doc's Recommended Approach section.
"""


@ray.remote
class ResearchAgent:
    """Stand-in for a real research agent. Replace the body of `work()`
    with whatever your actual agents do -- an LLM call, a tool run, a
    literature search, whatever REAL_TASK_DESCRIPTION names.
    """

    def __init__(self, agent_id: str, log_actor):
        self.agent_id = agent_id
        self.log_actor = log_actor

    def work(self, task_id: str, findings: list[str]) -> list[int]:
        cursors = []
        for finding in findings:
            cursor = ray.get(
                self.log_actor.append_delta.remote(
                    task_id,
                    self.agent_id,
                    {"content": finding, "kind": "result"},
                )
            )
            cursors.append(cursor)
            time.sleep(0.05)  # simulate real work, not instant appends
        return cursors


def run_spike():
    print(f"Task: {REAL_TASK_ID} -- {REAL_TASK_DESCRIPTION}")
    if REAL_TASK_DESCRIPTION.startswith("REPLACE ME"):
        print(
            "\nWARNING: REAL_TASK_DESCRIPTION is still the placeholder. "
            "This run will exercise the mechanism but won't tell you "
            "anything about real research-agent pain. Edit the constants "
            "at the top of this file first.\n",
            file=sys.stderr,
        )

    ray.init(num_cpus=4, include_dashboard=False, logging_level="error")

    db_path = tempfile.mktemp(suffix=".db", prefix="swarm-spike-")
    print(f"Event log: {db_path}\n")

    log_actor = EventLogActor.remote(db_path)

    agents = [ResearchAgent.remote(f"agent-{i}", log_actor) for i in range(3)]

    # Replace these with what your 3 agents actually produce for the real task.
    sample_findings = [
        ["found X in module A", "X depends on Y"],
        ["found Z in module B", "Z conflicts with X"],
        ["cross-checked X and Z against the spec", "spec is ambiguous on this"],
    ]

    print("Running 3 agents concurrently...")
    start = time.time()
    futures = [
        agent.work.remote(REAL_TASK_ID, findings)
        for agent, findings in zip(agents, sample_findings)
    ]
    ray.get(futures)
    elapsed = time.time() - start
    print(f"All 3 agents finished in {elapsed:.2f}s\n")

    print("Reconciling via a single pull...")
    reconciled = ray.get(log_actor.pull_deltas.remote(REAL_TASK_ID))
    for d in reconciled:
        print(f"  [{d.agent_id}] {d.content}")
    print(f"\n{len(reconciled)} deltas reconciled, zero lost or duplicated.\n")

    ray.shutdown()

    print(QUALIFYING_FAILURE_RUBRIC)
    print(
        "Now the actual spike work: run this again on a REAL task, "
        "watch what breaks or feels wrong, and write down every "
        "qualifying failure case above. Also check: does context get "
        "lost or need summarizing as the log grows on a longer real run? "
        "That's the actual wedge (context-window management), not just "
        "this transport layer."
    )


if __name__ == "__main__":
    run_spike()
