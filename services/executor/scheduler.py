import asyncio
import logging
from contextlib import suppress

from .bus import Bus, BusError, StepCommand, StepResult
from .models import (
    RUN_COMPLETED,
    RUN_FAILED,
    RUN_RUNNING,
    STEP_COMPLETED,
    STEP_DISPATCHED,
    STEP_FAILED,
    STEP_PENDING,
    STEP_RUNNING,
)
from .store import Store

log = logging.getLogger(__name__)


class Scheduler:
    """Scan persisted task state, serialising decisions across all local runs.

    One executor process owns the instruments. The lock covers database changes
    and command acknowledgements, never physical execution. It also lets a fast
    result wait until its dispatch has been recorded. Crash recovery and multiple
    executor processes require stronger coordination than this implementation.
    """

    REFUSAL_DELAY = 0.25

    def __init__(self, store: Store, bus: Bus) -> None:
        self.store = store
        self.bus = bus
        self._lock = asyncio.Lock()
        # Dict preserves start order; this is simple priority, not fair scheduling.
        self._runs: dict[str, None] = {}
        self._occupied: dict[str, str] = {}
        self._retry_task: asyncio.Task[None] | None = None

    async def start(self, run_id: str) -> None:
        async with self._lock:
            await self.store.start_run(run_id)
            run = await self.store.get_run(run_id)
            if run.status != RUN_RUNNING:
                return
            self._runs[run_id] = None
            for step in await self.store.list_steps(run_id):
                if step.status in (STEP_DISPATCHED, STEP_RUNNING):
                    self._occupied[step.device_id] = step.id
            await self._schedule()

    async def handle_result(self, result: StepResult) -> None:
        async with self._lock:
            steps = await self.store.list_steps(result.run_id)
            step = next((s for s in steps if s.id == result.step_id), None)
            if (
                step is None
                or step.name != result.step_name
                or step.device_id != result.device_id
                or step.status not in (STEP_DISPATCHED, STEP_RUNNING)
            ):
                log.warning("ignoring duplicate or unmatched result: %s", result)
                return
            await self.store.record_step_finished(
                step.id, STEP_FAILED if result.error else STEP_COMPLETED, result.error
            )
            if self._occupied.get(step.device_id) == step.id:
                del self._occupied[step.device_id]
            run = await self.store.get_run(result.run_id)
            if result.error and run.status == RUN_RUNNING:
                await self.store.finish_run(run.id, RUN_FAILED)
                self._runs.pop(run.id, None)
                log.error("run %s failed at %s: %s", run.id, step.name, result.error)
            # Record late successes, but never revive a run another task failed.
            await self._schedule()

    async def _schedule(self) -> None:
        """Called only with the global lock held; scan each active run once."""
        refused = False
        for run_id in list(self._runs):
            run = await self.store.get_run(run_id)
            if run.status != RUN_RUNNING:
                self._runs.pop(run_id, None)
                continue
            steps = await self.store.list_steps(run_id)
            if all(s.status == STEP_COMPLETED for s in steps):
                await self.store.finish_run(run_id, RUN_COMPLETED)
                self._runs.pop(run_id, None)
                log.info("run %s completed", run_id)
                continue
            completed = {s.name for s in steps if s.status == STEP_COMPLETED}
            for step in steps:
                if (
                    step.status != STEP_PENDING
                    or not completed.issuperset(step.depends_on)
                    or step.device_id in self._occupied
                ):
                    continue
                # Claim before sending; the global lock also protects the slot.
                await self.store.record_step_dispatched(step.id)
                self._occupied[step.device_id] = step.id
                try:
                    ack = await self.bus.send_command(
                        StepCommand(run_id, step.id, step.name, step.device_id)
                    )
                except BusError as exc:
                    # Timeout is not refusal: the driver may have taken the work.
                    # Fail closed; keep its slot and never repeat the command.
                    await self.store.record_dispatch_error(step.id, str(exc))
                    await self.store.finish_run(run_id, RUN_FAILED)
                    self._runs.pop(run_id, None)
                    log.error("command outcome unknown for %s: %s", step.id, exc)
                    break
                if not ack.accepted:
                    await self.store.record_step_refused(step.id)
                    del self._occupied[step.device_id]
                    refused = True
                    log.info("driver refused %s: %s", step.id, ack.reason)
                else:
                    log.info("dispatched %s on %s (run %s)", step.name, step.device_id, run_id)
        if refused and (self._retry_task is None or self._retry_task.done()):
            self._retry_task = asyncio.create_task(self._retry_refusals())

    async def _retry_refusals(self) -> None:
        try:
            await asyncio.sleep(self.REFUSAL_DELAY)
            async with self._lock:
                # Let a further refusal arrange the next wakeup.
                self._retry_task = None
                await self._schedule()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("refusal retry failed")

    async def close(self) -> None:
        if self._retry_task is not None:
            self._retry_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._retry_task
