# Copyright Amethyst Reese
# Licensed under the MIT license

"""
Regression tests for sharing one connection across several event loops.

Each test drives two independent event loops, each in its own thread, and
uses threading primitives (events/barriers) to control when the connection's
worker thread starts and finishes an operation relative to the closure of
the loop that submitted it.  These tests reproduce the failure where a
closed submitting loop made ``call_soon_threadsafe`` raise ``RuntimeError``
inside the worker thread, killing it and stranding every later operation.
"""

import asyncio
import concurrent.futures
import logging
import sqlite3
import threading
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Awaitable
from unittest import IsolatedAsyncioTestCase

import aiosqlite


class _ExpectedLoopShutdownFilter(logging.Filter):
    """Hide only the asyncio noise caused by closing a loop mid-operation."""

    def filter(self, record: logging.LogRecord) -> bool:
        return not (
            record.name == "asyncio"
            and "Task was destroyed but it is pending" in record.getMessage()
        )


logging.getLogger("asyncio").addFilter(_ExpectedLoopShutdownFilter())


class _Gate:
    """Block the worker thread until the test releases it."""

    def __init__(self) -> None:
        self.entered = threading.Barrier(2)
        self.release = threading.Event()

    def __call__(self) -> str:
        self.entered.wait(5)
        self.release.wait(5)
        return "gate"


class _LoopThread(threading.Thread):
    """Run coroutines on a private event loop that can be closed mid-flight."""

    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.loop = asyncio.new_event_loop()
        self.ready = threading.Event()

    def run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.ready.set()
        self.loop.run_forever()
        asyncio.set_event_loop(None)

    def submit(self, coro: Awaitable[Any]) -> "asyncio.Future[Any]":
        self.ready.wait(5)
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def close(self) -> None:
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.join(5)
        self.loop.close()
        self.assert_joined()

    def assert_joined(self) -> None:
        if self.is_alive():  # pragma: no cover - failure diagnostics only
            raise AssertionError("helper event loop thread did not stop")


def run_on_new_loop(coro: Awaitable[Any], timeout: float = 5) -> Any:
    """Run a coroutine on a throwaway event loop and close that loop.

    The timeout keeps a dead worker thread from hanging the test suite: a
    broken implementation fails fast instead of blocking forever.
    """
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(asyncio.wait_for(coro, timeout))
    finally:
        loop.close()


class MultiLoopTest(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tmpdir = TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        db_path = Path(self.tmpdir.name).resolve() / "multiloop.db"
        self.db = await aiosqlite.connect(db_path)
        await self.db.execute("create table t (id integer primary key, name text)")
        await self.db.executemany(
            "insert into t (id, name) values (?, ?)",
            [(1, "one"), (2, "two"), (3, "three")],
        )
        await self.db.commit()
        self.worker = self.db._thread

    async def asyncTearDown(self) -> None:
        if self.db._connection is not None:
            if self.worker.is_alive():
                # A healthy worker drains the remaining queue and closes the
                # underlying sqlite connection normally.
                await self.db.close()
            else:
                # A broken worker can no longer drain the queue; awaiting
                # close() would hang forever.  Drop the reference so GC can
                # finalize the sqlite connection instead of leaking it.
                self.db.stop()
                connection, self.db._connection = self.db._connection, None
                if connection is not None:
                    try:
                        connection.close()
                    except sqlite3.Error:
                        pass
        self.worker.join(5)
        # The positive worker/connection assertions live in each test so that
        # a regression is reported at the point where it actually happens.

    def assert_worker_survived(self, timeout: float = 5) -> None:
        """Prove the worker still drains the queue from another live loop."""
        probe = _LoopThread()
        probe.start()
        try:
            future = probe.submit(self.db._execute(lambda: "alive"))
            try:
                self.assertEqual("alive", future.result(timeout))
            except concurrent.futures.TimeoutError:
                self.fail(
                    "worker thread died after the submitting event loop "
                    "was closed"
                )
        finally:
            probe.close()

    def test_result_undeliverable_keeps_worker_alive(self) -> None:
        gate = _Gate()
        first = _LoopThread()
        second = _LoopThread()
        first.start()
        second.start()
        try:
            first.submit(self.db._execute(gate))
            gate.entered.wait(5)

            second_future = second.submit(
                self.db.execute_fetchall("select 42")
            )
            threading.Event().wait(0.1)

            first.close()
        finally:
            gate.release.set()

        self.assert_worker_survived()
        self.assertEqual([(42,)], list(second_future.result(5)))
        second.close()
        self.assertEqual(
            [(7,)], run_on_new_loop(self.db.execute_fetchall("select 7"))
        )

    def test_sql_error_undeliverable_keeps_worker_alive(self) -> None:
        gate = _Gate()
        first = _LoopThread()
        first.start()

        def gated_bad_sql() -> None:
            gate()
            self.db._conn.execute("select * from table_that_does_not_exist")

        try:
            first.submit(self.db._execute(gated_bad_sql))
            gate.entered.wait(5)
            first.close()
        finally:
            gate.release.set()

        self.assert_worker_survived()
        with self.assertRaises(sqlite3.OperationalError):
            run_on_new_loop(
                self.db.execute_fetchall("select * from table_that_does_not_exist")
            )
        self.assertEqual([(1,)], run_on_new_loop(self.db.execute_fetchall("select 1")))

    def test_cancelled_future_then_loop_close(self) -> None:
        gate = _Gate()
        first = _LoopThread()
        first.start()
        try:
            first.submit(self.db._execute(gate))
            gate.entered.wait(5)

            cancelled = first.submit(self._cancel_queued_operation())
            self.assertTrue(cancelled.result(5))

            first.close()
        finally:
            gate.release.set()

        self.assert_worker_survived()
        self.assertEqual(
            [(2,)], run_on_new_loop(self.db.execute_fetchall("select 2"))
        )

    async def _cancel_queued_operation(self) -> bool:
        task = asyncio.ensure_future(
            self.db._execute(lambda: "never delivered")
        )
        await asyncio.sleep(0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            return True
        return False

    def test_stop_future_on_closed_loop_still_terminates_worker(self) -> None:
        gate = _Gate()
        first = _LoopThread()
        second = _LoopThread()
        first.start()
        second.start()
        try:
            # Keep the worker busy while stop() is submitted from the loop
            # that is about to die; the stop future itself is undeliverable.
            first.submit(self.db._execute(gate))
            gate.entered.wait(5)

            second.submit(self._await_stop())
            threading.Event().wait(0.1)
            first.close()
        finally:
            gate.release.set()

        self.worker.join(5)
        self.assertFalse(self.worker.is_alive())
        self.assertIsNone(self.db._connection)
        second.close()

    async def _await_stop(self) -> None:
        future = self.db.stop()
        assert future is not None
        try:
            await future
        except asyncio.CancelledError:
            pass

    def test_close_interrupted_by_loop_closure_is_recoverable(self) -> None:
        gate = _Gate()
        first = _LoopThread()
        first.start()
        try:
            first.submit(self.db._execute(gate))
            gate.entered.wait(5)
            first.submit(self.db.close())
            threading.Event().wait(0.1)
            first.close()
        finally:
            gate.release.set()

        # The worker survived the dead loop; a later loop finalizes the
        # connection and terminates the thread.
        self.assert_worker_survived()
        run_on_new_loop(self._finalize_connection())
        self.worker.join(5)
        self.assertFalse(self.worker.is_alive())
        self.assertIsNone(self.db._connection)

    async def _finalize_connection(self) -> None:
        await self.db.close()

    def test_unconsumed_cursor_survives_loop_closure(self) -> None:
        gate = _Gate()
        cursor = run_on_new_loop(self.db.execute("select id from t order by id"))
        first = _LoopThread()
        first.start()
        try:
            first.submit(self._fetch_after_gate(gate, cursor))
            gate.entered.wait(5)
            first.close()
        finally:
            gate.release.set()

        self.assert_worker_survived()
        # The fetch was never queued before the loop died, so the cursor is
        # still fully unconsumed and usable from a fresh loop.
        self.assertEqual(
            [(1,), (2,), (3,)], run_on_new_loop(cursor.fetchall())
        )

    async def _fetch_after_gate(self, gate: _Gate, cursor: aiosqlite.Cursor) -> Any:
        await self.db._execute(gate)
        return await cursor.fetchall()

    def test_queued_operation_still_runs_without_submitter_loop(self) -> None:
        gate = _Gate()
        cursor = run_on_new_loop(self.db.execute("select id from t order by id"))
        first = _LoopThread()
        first.start()
        try:
            first.submit(self.db._execute(gate))
            gate.entered.wait(5)
            first.submit(cursor.fetchall())
            threading.Event().wait(0.1)
            first.close()
        finally:
            gate.release.set()

        self.assert_worker_survived()
        # The queued fetch executed even though nobody received its result:
        # the rows are consumed instead of the operation being silently dropped.
        self.assertEqual([], run_on_new_loop(cursor.fetchall()))
        self.assertEqual(
            [("one",)],
            run_on_new_loop(
                self.db.execute_fetchall("select name from t where id = 1")
            ),
        )

    def test_cursor_use_after_connection_close(self) -> None:
        cursor = run_on_new_loop(self.db.execute("select 1"))
        run_on_new_loop(self.db.close())
        self.worker.join(5)
        self.assertFalse(self.worker.is_alive())
        self.assertIsNone(self.db._connection)

        fresh = _LoopThread()
        fresh.start()
        try:
            future = fresh.submit(cursor.fetchall())
            with self.assertRaisesRegex(ValueError, "Connection closed"):
                future.result(5)
        finally:
            fresh.close()
