import asyncio
import logging
from contextlib import suppress
from datetime import UTC, datetime

from .bus import Bus, BusError, StepCommand, StepResult
from .models import (
    RUN_COMPLETED,
    RUN_RUNNING,
    STEP_COMPLETED,
    STEP_DISPATCHED,
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
    RETRY_DELAY = 3.0
    MAX_RETRIES = 1

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
            await self._rescan()

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
            state = await self.store.record_result(
                result,
                max_retries=self.MAX_RETRIES,
                retry_delay=self.RETRY_DELAY,
            )
            if state is None:
                log.warning("ignoring stale attempt result: %s", result)
                return
            if self._occupied.get(step.device_id) == step.id:
                del self._occupied[step.device_id]
            if result.error:
                log.warning(
                    "attempt %s failed for %s: %s (next state: %s)",
                    result.attempt_id,
                    step.name,
                    result.error,
                    state,
                )
            # A late result is recorded even after the run fails; it never revives it.
            await self._rescan()

    async def _schedule(self) -> bool:
        """Called only with the global lock held; scan each active run once."""
        needs_wakeup = False
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
                    step.status == STEP_PENDING
                    and step.retry_after is not None
                    and step.retry_after > datetime.now(UTC)
                ):
                    needs_wakeup = True
                    continue
                if (
                    step.status != STEP_PENDING
                    or not completed.issuperset(step.depends_on)
                    or step.device_id in self._occupied
                ):
                    continue
                # Claim before sending; the global lock also protects the slot.
                attempt_id = await self.store.claim_step(step.id)
                if attempt_id is None:
                    needs_wakeup = True
                    continue
                self._occupied[step.device_id] = step.id
                try:
                    ack = await self.bus.send_command(
                        StepCommand(run_id, step.id, step.name, step.device_id, attempt_id)
                    )
                except BusError as exc:
                    # Timeout is not refusal: the driver may have taken the work.
                    # Fail closed; keep its slot and never repeat the command.
                    await self.store.record_dispatch_error(step.id, attempt_id, str(exc))
                    self._runs.pop(run_id, None)
                    log.error("command outcome unknown for %s: %s", step.id, exc)
                    break
                await self.store.record_ack(step.id, attempt_id, ack.accepted, ack.reason)
                if not ack.accepted:
                    del self._occupied[step.device_id]
                    needs_wakeup = True
                    log.info("driver refused %s: %s", step.id, ack.reason)
                else:
                    log.info("dispatched %s on %s (run %s)", step.name, step.device_id, run_id)
        return needs_wakeup

    async def _rescan(self) -> None:
        if await self._schedule() and (self._retry_task is None or self._retry_task.done()):
            self._retry_task = asyncio.create_task(self._wake_pending())

    async def _wake_pending(self) -> None:
        """A retained timer handles refusal and retry backoff without needing a result."""
        try:
            while True:
                await asyncio.sleep(self.REFUSAL_DELAY)
                async with self._lock:
                    if not await self._schedule():
                        return
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("pending-work wakeup failed")

    async def close(self) -> None:
        if self._retry_task is not None:
            self._retry_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._retry_task
