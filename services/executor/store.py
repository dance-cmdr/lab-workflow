import secrets
from collections.abc import Sequence
from typing import Any

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from .bus import StepResult
from .models import (
    RUN_FAILED,
    RUN_PENDING,
    RUN_RUNNING,
    STEP_COMPLETED,
    STEP_DISPATCHED,
    STEP_FAILED,
    STEP_PENDING,
    STEP_RUNNING,
    STEP_SKIPPED,
    Device,
    Run,
    Step,
)
from .workflows import StepTemplate


class RunNotFound(Exception):
    """No run with that id."""


def new_id(prefix: str) -> str:
    return f"{prefix}-{secrets.token_hex(6)}"


RUN_COLS = "id, workflow_name, status, created_at, started_at, finished_at, failed_step_id"
STEP_COLS = (
    "id, run_id, name, device_id, status, depends_on, "
    "dispatch_count, dispatched_at, finished_at, error, current_attempt_id, retry_after"
)


def _run(row: Sequence[Any]) -> Run:
    return Run(*row)


def _step(row: Sequence[Any]) -> Step:
    return Step(*row)


class Store:
    def __init__(self, pool: AsyncConnectionPool) -> None:
        self._pool = pool

    async def list_devices(self) -> list[Device]:
        async with self._pool.connection() as conn:
            cur = await conn.execute("SELECT id, name, type FROM devices ORDER BY id")
            return [Device(*row) for row in await cur.fetchall()]

    async def list_runs(self) -> list[Run]:
        async with self._pool.connection() as conn:
            cur = await conn.execute(f"SELECT {RUN_COLS} FROM runs ORDER BY created_at DESC")
            return [_run(row) for row in await cur.fetchall()]

    async def get_run(self, run_id: str) -> Run:
        async with self._pool.connection() as conn:
            cur = await conn.execute(f"SELECT {RUN_COLS} FROM runs WHERE id = %s", (run_id,))
            row = await cur.fetchone()
        if row is None:
            raise RunNotFound(run_id)
        return _run(row)

    async def create_run(self, workflow_name: str, template: list[StepTemplate]) -> Run:
        """Materialise a workflow template into a run and its steps."""
        run_id = new_id("run")
        # The pool's context manager commits on exit and rolls back on error, so
        # a run is never left without its steps.
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                f"INSERT INTO runs (id, workflow_name) VALUES (%s, %s) RETURNING {RUN_COLS}",
                (run_id, workflow_name),
            )
            row = await cur.fetchone()
            for st in template:
                await conn.execute(
                    "INSERT INTO steps (id, run_id, name, device_id, depends_on)"
                    " VALUES (%s, %s, %s, %s, %s)",
                    (new_id("step"), run_id, st.name, st.device_id, list(st.depends_on)),
                )
        assert row is not None  # RETURNING on a successful INSERT always yields a row
        return _run(row)

    async def list_steps(self, run_id: str) -> list[Step]:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                f"SELECT {STEP_COLS} FROM steps WHERE run_id = %s ORDER BY name",
                (run_id,),
            )
            return [_step(row) for row in await cur.fetchall()]

    async def start_run(self, run_id: str) -> None:
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE runs SET status = %s, started_at = now(), updated_at = now()"
                " WHERE id = %s AND status = %s",
                (RUN_RUNNING, run_id, RUN_PENDING),
            )

    async def finish_run(self, run_id: str, status: str) -> None:
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE runs SET status = %s, finished_at = now(), updated_at = now()"
                " WHERE id = %s AND status = %s",
                (status, run_id, RUN_RUNNING),
            )

    @staticmethod
    async def _event(
        conn: AsyncConnection[Any],
        attempt_id: str,
        kind: str,
        error: str = "",
        retryable: bool | None = None,
    ) -> None:
        await conn.execute(
            "INSERT INTO attempt_events (attempt_id, kind, error, retryable)"
            " VALUES (%s, %s, %s, %s)",
            (attempt_id, kind, error or None, retryable),
        )

    @staticmethod
    async def _fail_run(conn: AsyncConnection[Any], run_id: str, step_id: str) -> None:
        # Keep the first causal failure; late results cannot change it.
        await conn.execute(
            "UPDATE runs SET status = %s, failed_step_id = %s,"
            " finished_at = now(), updated_at = now() WHERE id = %s AND status = %s",
            (RUN_FAILED, step_id, run_id, RUN_RUNNING),
        )
        await conn.execute(
            "UPDATE steps SET status = %s, retry_after = NULL, finished_at = now(),"
            " updated_at = now(), error = %s WHERE run_id = %s AND status = %s",
            (STEP_SKIPPED, f"Run stopped after failure of {step_id}", run_id, STEP_PENDING),
        )

    async def claim_step(self, step_id: str) -> str | None:
        """Atomically claim current state and append the command offer."""
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT s.id FROM steps s JOIN runs r ON r.id = s.run_id"
                " WHERE s.id = %s AND s.status = %s AND r.status = %s"
                " AND (s.retry_after IS NULL OR s.retry_after <= now()) FOR UPDATE OF s",
                (step_id, STEP_PENDING, RUN_RUNNING),
            )
            if await cur.fetchone() is None:
                return None
            cur = await conn.execute(
                "SELECT COALESCE(MAX(number), 0) + 1 FROM step_attempts WHERE step_id = %s",
                (step_id,),
            )
            row = await cur.fetchone()
            assert row is not None
            attempt_id = new_id("attempt")
            await conn.execute(
                "INSERT INTO step_attempts (id, step_id, number) VALUES (%s, %s, %s)",
                (attempt_id, step_id, row[0]),
            )
            await conn.execute(
                "UPDATE steps SET status = %s, current_attempt_id = %s,"
                " dispatch_count = dispatch_count + 1,"
                " dispatched_at = COALESCE(dispatched_at, now()), retry_after = NULL,"
                " finished_at = NULL, error = NULL, updated_at = now() WHERE id = %s",
                (STEP_DISPATCHED, attempt_id, step_id),
            )
            await self._event(conn, attempt_id, "offered")
            return attempt_id

    async def record_ack(self, step_id: str, attempt_id: str, accepted: bool, reason: str) -> None:
        async with self._pool.connection() as conn:
            await self._event(conn, attempt_id, "accepted" if accepted else "refused", reason)
            if not accepted:
                await conn.execute(
                    "UPDATE steps SET status = %s, current_attempt_id = NULL,"
                    " dispatched_at = CASE WHEN dispatch_count = 1 THEN NULL ELSE dispatched_at END,"
                    " dispatch_count = dispatch_count - 1, updated_at = now()"
                    " WHERE id = %s AND current_attempt_id = %s AND status = %s",
                    (STEP_PENDING, step_id, attempt_id, STEP_DISPATCHED),
                )

    async def record_dispatch_error(self, step_id: str, attempt_id: str, error: str) -> None:
        """An unknown acceptance keeps its slot; fail and skip atomically."""
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "UPDATE steps SET error = %s, updated_at = now()"
                " WHERE id = %s AND current_attempt_id = %s RETURNING run_id",
                (error, step_id, attempt_id),
            )
            row = await cur.fetchone()
            if row is None:
                return
            await self._event(conn, attempt_id, "unknown", error)
            await self._fail_run(conn, row[0], step_id)

    async def record_result(
        self,
        result: StepResult,
        *,
        max_retries: int,
        retry_delay: float,
    ) -> str | None:
        """Append the outcome and transition task/run state in one transaction.

        Return the resulting step state, or None for a stale/duplicate result.
        Unversioned results are allowed only for a task's very first offer, for
        compatibility with the supplied RecordingBus concurrency test.
        """
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT s.current_attempt_id, a.number, r.status FROM steps s"
                " JOIN runs r ON r.id = s.run_id"
                " JOIN step_attempts a ON a.id = s.current_attempt_id"
                " WHERE s.id = %s AND s.run_id = %s AND s.name = %s AND s.device_id = %s"
                " AND s.status IN (%s, %s) FOR UPDATE OF s, r",
                (
                    result.step_id,
                    result.run_id,
                    result.step_name,
                    result.device_id,
                    STEP_DISPATCHED,
                    STEP_RUNNING,
                ),
            )
            row = await cur.fetchone()
            if row is None:
                return None
            attempt_id, number, run_status = row
            if result.attempt_id != attempt_id and not (result.attempt_id is None and number == 1):
                return None
            retry = False
            if result.error and result.retryable and run_status == RUN_RUNNING:
                cur = await conn.execute(
                    "SELECT count(*) FROM attempt_events e JOIN step_attempts a ON a.id=e.attempt_id"
                    " WHERE a.step_id = %s AND e.kind = 'failed'",
                    (result.step_id,),
                )
                failures = await cur.fetchone()
                assert failures is not None
                retry = failures[0] < max_retries
            state = STEP_PENDING if retry else STEP_FAILED if result.error else STEP_COMPLETED
            await self._event(
                conn,
                attempt_id,
                "failed" if result.error else "completed",
                result.error,
                result.retryable if result.error else None,
            )
            await conn.execute(
                "UPDATE steps SET status = %s, error = %s, updated_at = now(),"
                " finished_at = CASE WHEN %s THEN NULL ELSE now() END,"
                " retry_after = CASE WHEN %s THEN now() + %s * interval '1 second' ELSE NULL END,"
                " current_attempt_id = CASE WHEN %s THEN NULL ELSE current_attempt_id END"
                " WHERE id = %s",
                (state, result.error or None, retry, retry, retry_delay, retry, result.step_id),
            )
            if retry:
                await self._event(conn, attempt_id, "retry_scheduled")
            elif result.error and run_status == RUN_RUNNING:
                await self._fail_run(conn, result.run_id, result.step_id)
            return state

    async def list_attempts(self, run_id: str) -> list[dict[str, Any]]:
        # One query gives a consistent snapshot while other handlers append events.
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT a.id, a.step_id, a.number, a.created_at, s.name, s.device_id,"
                " e.id, e.kind, e.error, e.retryable, e.created_at"
                " FROM step_attempts a JOIN steps s ON s.id = a.step_id"
                " LEFT JOIN attempt_events e ON e.attempt_id = a.id"
                " WHERE s.run_id = %s ORDER BY a.created_at, a.id, e.id",
                (run_id,),
            )
            by_id: dict[str, dict[str, Any]] = {}
            for row in await cur.fetchall():
                attempt_id = row[0]
                if attempt_id not in by_id:
                    by_id[attempt_id] = dict(
                        zip(
                            ("id", "step_id", "number", "created_at", "step_name", "device_id"),
                            row[:6],
                        ),
                        events=[],
                    )
                if row[6] is not None:
                    by_id[attempt_id]["events"].append(
                        dict(
                            zip(
                                ("id", "kind", "error", "retryable", "created_at"),
                                row[6:],
                            )
                        )
                    )
            return list(by_id.values())
