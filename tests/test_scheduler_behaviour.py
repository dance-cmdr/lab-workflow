"""Boundary cases beyond the supplied simultaneous-completion check."""

import asyncio
import os

import pytest
from psycopg_pool import AsyncConnectionPool

from services.executor.bus import BusError, CommandAck, StepCommand, StepResult
from services.executor.scheduler import Scheduler
from services.executor.store import Store
from services.executor.workflows import StepTemplate


@pytest.fixture
async def store():
    url = os.getenv("DATABASE_URL")
    if not url:
        pytest.skip("DATABASE_URL not set -- run with docker compose run --rm tests")
    async with AsyncConnectionPool(url, min_size=2, max_size=10, open=False) as pool:
        await pool.wait()
        yield Store(pool)


class ControlledBus:
    def __init__(self, refusals=0, uncertain=False):
        self.commands: list[StepCommand] = []
        self.calls = 0
        self.refusals = refusals
        self.uncertain = uncertain
        self.accepted = asyncio.Event()

    async def send_command(self, cmd):
        self.calls += 1
        if self.uncertain:
            self.commands.append(cmd)  # Work may have started before the timeout.
            self.uncertain = False
            raise BusError("ack timed out")
        if self.refusals:
            self.refusals -= 1
            return CommandAck(False, "busy with external work")
        self.commands.append(cmd)
        self.accepted.set()
        return CommandAck(True)


def result(cmd, error=""):
    return StepResult(cmd.run_id, cmd.step_id, cmd.step_name, cmd.device_id, error)


async def test_refusal_wakes_up_without_any_completion_event(store):
    run = await store.create_run("refusal", [StepTemplate("fill", "liquid-handler-1")])
    bus = ControlledBus(refusals=2)
    sched = Scheduler(store, bus)
    sched.REFUSAL_DELAY = 0.01
    try:
        await sched.start(run.id)
        (step,) = await store.list_steps(run.id)
        assert (step.status, step.dispatch_count, step.dispatched_at) == ("pending", 0, None)
        await asyncio.wait_for(bus.accepted.wait(), timeout=2)
        assert bus.calls == 3
        await sched.handle_result(result(bus.commands[0]))
        (step,) = await store.list_steps(run.id)
        assert (step.status, step.dispatch_count) == ("completed", 1)
        assert (await store.get_run(run.id)).status == "completed"
    finally:
        await sched.close()


async def test_runs_share_slots_but_different_devices_overlap(store):
    template = [
        StepTemplate("fill", "liquid-handler-1"),
        StepTemplate("read", "plate-reader-1", ["fill"]),
    ]
    runs = [await store.create_run("shared", template) for _ in range(2)]
    bus = ControlledBus()
    sched = Scheduler(store, bus)
    await asyncio.gather(*(sched.start(r.id) for r in runs))
    assert len(bus.commands) == 1
    # Repeated start cannot dispatch the already accepted task again.
    await sched.start(bus.commands[0].run_id)
    assert len(bus.commands) == 1
    first = bus.commands[0]
    await sched.handle_result(result(first))
    in_flight = bus.commands[1:]
    assert {c.device_id for c in in_flight} == {"liquid-handler-1", "plate-reader-1"}
    # Replayed completion must not free the second run's liquid handler slot.
    await sched.handle_result(result(first))
    assert len(bus.commands) == 3
    completed = {first.step_id}
    while batch := [c for c in bus.commands if c.step_id not in completed]:
        assert len({c.device_id for c in batch}) == len(batch)
        completed.update(c.step_id for c in batch)
        await asyncio.gather(*(sched.handle_result(result(c)) for c in batch))
    assert all([(await store.get_run(r.id)).status == "completed" for r in runs])
    assert len(bus.commands) == 4


async def test_failure_stops_new_work_but_records_late_results_and_frees_other_runs(store):
    run = await store.create_run(
        "failure",
        [
            StepTemplate("incubate", "incubator-1"),
            StepTemplate("fill", "liquid-handler-1"),
            StepTemplate("next", "liquid-handler-1", ["fill"]),
        ],
    )
    other = await store.create_run("unaffected", [StepTemplate("fill", "liquid-handler-1")])
    bus = ControlledBus()
    sched = Scheduler(store, bus)
    await sched.start(run.id)
    await sched.start(other.id)
    failed = next(c for c in bus.commands if c.step_name == "incubate")
    late = next(c for c in bus.commands if c.step_name == "fill")
    await sched.handle_result(result(failed, "instrument error"))
    assert (await store.get_run(run.id)).status == "failed"
    await sched.handle_result(result(late))
    await sched.handle_result(result(late))  # Duplicate cannot release another task's slot.
    assert (await store.get_run(run.id)).status == "failed"
    assert not any(c.run_id == run.id and c.step_name == "next" for c in bus.commands)
    steps = {s.name: s for s in await store.list_steps(run.id)}
    assert steps["incubate"].error == "instrument error"
    assert steps["fill"].status == "completed"
    assert steps["next"].status == "pending"
    command = next(c for c in bus.commands if c.run_id == other.id)
    await sched.handle_result(result(command))
    assert (await store.get_run(other.id)).status == "completed"


async def test_uncertain_ack_is_not_retried_or_treated_as_free_equipment(store):
    template = [StepTemplate("fill", "liquid-handler-1")]
    run = await store.create_run("unknown ack", template)
    other = await store.create_run("waiting", template)
    bus = ControlledBus(uncertain=True)
    sched = Scheduler(store, bus)
    await sched.start(run.id)
    await sched.start(other.id)
    assert (await store.get_run(run.id)).status == "failed"
    assert len(bus.commands) == 1
    (step,) = await store.list_steps(run.id)
    assert step.status == "dispatched"
    assert step.error == "ack timed out"
    # Only an actual result establishes release; late success cannot revive the run.
    await sched.handle_result(result(bus.commands[0]))
    assert (await store.get_run(run.id)).status == "failed"
    assert len(bus.commands) == 2
    await sched.handle_result(result(bus.commands[1]))
    assert (await store.get_run(other.id)).status == "completed"
