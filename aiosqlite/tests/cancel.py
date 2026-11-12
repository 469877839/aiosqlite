# Copyright Amethyst Reese
# Licensed under the MIT license

import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event
from unittest import IsolatedAsyncioTestCase

import aiosqlite
from aiosqlite import CancellationPolicy, OperationCancelledError
from aiosqlite.core import _Operation, _OperationQueue
from .helpers import setup_logger


class CancellationQueueTest(IsolatedAsyncioTestCase):
    """State-machine tests that do not need a running worker thread."""

    def setUp(self):
        self.queue = _OperationQueue()
        self.loop = asyncio.new_event_loop()
        self.addCleanup(self.loop.close)

    def operation(self, interrupt: bool = True) -> _Operation:
        future = self.loop.create_future()
        return _Operation(
            self.next_id(),
            future,
            lambda: None,
            Event() if interrupt else None,
            interrupt,
        )

    def next_id(self) -> int:
        self._ids = getattr(self, "_ids", 0) + 1
        return self._ids

    def test_cancel_queued_operation_never_runs(self):
        first = self.operation()
        second = self.operation()
        self.queue.enqueue(first)
        self.queue.enqueue(second)

        info = self.queue.cancel(second)
        self.assertEqual(info.phase, "queued")
        self.assertFalse(info.interrupted)
        self.assertTrue(second.cancelled)

        self.assertIs(self.queue.dequeue(), first)
        self.queue.finish(first)

        # The cancelled operation is skipped and its future finalized.
        self.assertIsNone(self.queue._take_locked())
        self.loop.run_until_complete(asyncio.sleep(0))
        exc = second.future.exception()
        self.assertIsInstance(exc, OperationCancelledError)
        self.assertEqual(exc.info.phase, "queued")

    def test_cancel_running_interrupt_operation(self):
        operation = self.operation(interrupt=True)
        self.queue.enqueue(operation)
        self.assertIs(self.queue.dequeue(), operation)
        self.assertTrue(operation.committed)

        info = self.queue.cancel(operation)
        self.assertEqual(info.phase, "interrupted")
        self.assertTrue(info.interrupted)
        self.assertTrue(operation.interrupt_requested)
        self.assertTrue(self.queue.request_interrupt(operation))

    def test_cancel_running_never_policy_runs_to_completion(self):
        operation = self.operation(interrupt=False)
        self.queue.enqueue(operation)
        self.queue.dequeue()

        info = self.queue.cancel(operation)
        self.assertEqual(info.phase, "running")
        self.assertFalse(info.interrupted)
        self.assertFalse(operation.interrupt_requested)

        self.queue.finish(operation)
        self.assertFalse(self.queue.request_interrupt(operation))

    def test_cancel_after_finish_loses_the_race(self):
        operation = self.operation(interrupt=True)
        self.queue.enqueue(operation)
        self.queue.dequeue()
        self.queue.finish(operation)

        self.assertIsNone(self.queue.cancel(operation))
        self.assertFalse(self.queue.request_interrupt(operation))

    def test_double_cancel_is_idempotent(self):
        operation = self.operation(interrupt=True)
        self.queue.enqueue(operation)
        self.queue.dequeue()
        first = self.queue.cancel(operation)
        second = self.queue.cancel(operation)
        self.assertEqual(first.phase, "interrupted")
        self.assertIsNone(second)


class CancellationIntegrationTest(IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        setup_logger()

    def setUp(self):
        td = TemporaryDirectory()
        self.addCleanup(td.cleanup)
        self.db = Path(td.name).resolve() / "test.db"

    async def asyncSetUp(self):
        self.conn = await aiosqlite.connect(self.db)
        await self.conn.execute("create table items (id integer primary key, tag text)")
        await self.conn.commit()

    async def asyncTearDown(self):
        await self.conn.close()

    async def _parked_barrier(self, policy):
        operation = await self.conn._cancellable_barrier(policy)
        for _ in range(100):
            await asyncio.sleep(0.005)
            if operation.committed:
                break
        self.assertTrue(operation.committed)
        return operation

    async def _wait_until(self, predicate, timeout=5.0):
        for _ in range(int(timeout / 0.005)):
            if predicate():
                return True
            await asyncio.sleep(0.005)
        return False

    async def test_queued_cancel_never_executes(self):
        blocker = await self._parked_barrier(CancellationPolicy.INTERRUPT)

        task = asyncio.ensure_future(
            self.conn.execute_cancellable(
                "insert into items (tag) values ('queued')",
                policy=CancellationPolicy.NEVER,
            )
        )
        await asyncio.sleep(0.05)
        # The blocker still owns the worker, so the insert is queued behind it.
        self.assertFalse(self.conn._tx.empty())

        task.cancel()
        with self.assertRaises(OperationCancelledError) as caught:
            await task
        self.assertEqual(caught.exception.info.phase, "queued")

        self.conn._release_barrier(blocker)
        await blocker.task

        rows = await self.conn.execute_fetchall("select count(*) from items")
        self.assertEqual(rows, [(0,)])
        self.assertTrue(self.conn._tx.empty())

    async def test_interrupt_does_not_hit_next_operation(self):
        # A is parked inside SQLite execution and gets interrupted; B, queued
        # behind A, must execute completely and see a healthy connection.
        op_a = await self._parked_barrier(CancellationPolicy.INTERRUPT)

        task_b = asyncio.ensure_future(
            self.conn.execute_cancellable(
                "insert into items (tag) values ('b')",
                policy=CancellationPolicy.INTERRUPT,
            )
        )
        await asyncio.sleep(0.05)

        op_a.task.cancel()
        with self.assertRaises(OperationCancelledError) as caught:
            await op_a.task
        info = caught.exception.info
        self.assertEqual(info.phase, "interrupted")
        self.assertTrue(info.interrupted)
        self.assertEqual(op_a.operation_id, info.operation_id)

        cursor_b = await task_b
        self.assertIsInstance(cursor_b, aiosqlite.Cursor)
        await cursor_b.close()

        rows = await self.conn.execute_fetchall("select tag from items")
        self.assertEqual(rows, [("b",)])
        self.assertEqual(await self.conn.execute_fetchall("select 1 + 1"), [(2,)])

    async def test_interrupt_aborts_recursive_query_but_keeps_connection(self):
        entered = Event()
        release = Event()

        def barrier(value):
            entered.set()
            release.wait(1)
            return value

        await self.conn.create_function("barrier", 1, barrier)

        recursive_sql = (
            "with recursive cte(x) as ("
            "select barrier(1) "
            "union all "
            "select x + 1 from cte where x < 1000000"
            ") select count(*) from cte"
        )
        task = asyncio.ensure_future(
            self.conn.execute_fetchall_cancellable(
                recursive_sql, policy=CancellationPolicy.INTERRUPT
            )
        )
        self.assertTrue(await self._wait_until(entered.is_set))

        task.cancel()
        with self.assertRaises(OperationCancelledError) as caught:
            await task
        self.assertEqual(caught.exception.info.phase, "interrupted")
        # The UDF wait bounded the abort latency; the connection recovered.
        self.assertTrue(entered.is_set())

        # The interrupted statement left the connection usable.
        self.assertEqual(
            await self.conn.execute_fetchall("select 42"), [(42,)]
        )

    async def test_never_policy_completes_executing_statement(self):
        op_a = await self._parked_barrier(CancellationPolicy.NEVER)

        op_a.task.cancel()
        with self.assertRaises(OperationCancelledError) as caught:
            await op_a.task
        self.assertEqual(caught.exception.info.phase, "running")
        self.assertFalse(caught.exception.info.interrupted)

        # The worker is still parked; releasing lets the statement finish.
        self.conn._release_barrier(op_a)
        for _ in range(100):
            await asyncio.sleep(0.005)
            if self.conn._tx.empty():
                break
        self.assertTrue(self.conn._tx.empty())
        self.assertEqual(await self.conn.execute_fetchall("select 1"), [(1,)])

    async def test_cancel_does_not_roll_back_connection_transaction(self):
        # One statement already wrote inside the open transaction...
        await self.conn.execute("insert into items (tag) values ('kept')")
        self.assertTrue(self.conn.in_transaction)

        op_a = await self._parked_barrier(CancellationPolicy.INTERRUPT)
        op_a.task.cancel()
        with self.assertRaises(OperationCancelledError):
            await op_a.task

        # ...the interrupt must not roll back that surrounding transaction.
        self.assertTrue(self.conn.in_transaction)
        await self.conn.commit()
        self.assertFalse(self.conn.in_transaction)
        rows = await self.conn.execute_fetchall("select tag from items")
        self.assertEqual(rows, [("kept",)])

    async def test_interrupted_write_is_statement_rolled_back(self):
        entered = Event()
        release = Event()

        def barrier(value):
            entered.set()
            release.wait(1)
            return value

        await self.conn.create_function("barrier", 1, barrier)
        await self.conn.execute("insert into items (tag) values ('before')")

        task = asyncio.ensure_future(
            self.conn.execute_cancellable(
                "insert into items (tag) values (barrier('interrupted'))",
                policy=CancellationPolicy.INTERRUPT,
            )
        )
        self.assertTrue(await self._wait_until(entered.is_set))
        task.cancel()
        with self.assertRaises(OperationCancelledError):
            await task

        self.assertTrue(self.conn.in_transaction)
        await self.conn.rollback()
        rows = await self.conn.execute_fetchall("select tag from items")
        self.assertEqual(rows, [])

    async def test_cursor_from_cancelled_operation_is_not_returned(self):
        blocker = await self._parked_barrier(CancellationPolicy.INTERRUPT)
        task = asyncio.ensure_future(
            self.conn.execute_cancellable(
                "insert into items (tag) values ('x')",
                policy=CancellationPolicy.INTERRUPT,
            )
        )
        await asyncio.sleep(0.05)
        task.cancel()
        with self.assertRaises(OperationCancelledError):
            await task
        self.conn._release_barrier(blocker)
        await blocker.task

        # No cursor escaped to the caller; a fresh cursor still works and
        # closes cleanly.
        async with self.conn.cursor() as cursor:
            await cursor.execute("select 1")
            self.assertEqual(await cursor.fetchone(), (1,))

    async def test_legacy_execute_keeps_legacy_cancellation_semantics(self):
        blocker = await self._parked_barrier(CancellationPolicy.INTERRUPT)
        task = asyncio.ensure_future(self.conn.execute_fetchall("select 1"))
        await asyncio.sleep(0.05)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError) as caught:
            await task
        # Legacy path keeps the plain CancelledError, not the new type.
        self.assertIs(type(caught.exception), asyncio.CancelledError)
        self.conn._release_barrier(blocker)
        await blocker.task
        self.assertEqual(await self.conn.execute_fetchall("select 1"), [(1,)])

    async def test_close_waits_for_interrupted_operations(self):
        op_a = await self._parked_barrier(CancellationPolicy.INTERRUPT)
        op_a.task.cancel()
        with self.assertRaises(OperationCancelledError):
            await op_a.task
        await self.conn.close()
        self.assertTrue(self.conn._tx.empty())
        self.assertFalse(self.conn._thread.is_alive())
