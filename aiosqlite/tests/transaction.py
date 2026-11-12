# Copyright Amethyst Reese
# Licensed under the MIT license

import asyncio
import sqlite3
from unittest import IsolatedAsyncioTestCase

import aiosqlite
from aiosqlite import TransactionError
from .helpers import setup_logger


class FailingCommit(sqlite3.Connection):
    fail_next_commit = False

    def commit(self):
        if FailingCommit.fail_next_commit:
            FailingCommit.fail_next_commit = False
            raise sqlite3.OperationalError("simulated commit failure")
        return super().commit()


class TransactionTest(IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        setup_logger()

    async def asyncSetUp(self):
        self.db = await aiosqlite.connect(":memory:")
        await self.db.execute("create table t (name text)")
        await self.db.commit()

    async def asyncTearDown(self):
        await self.db.close()

    async def test_rollback_does_not_take_other_task_commit(self):
        # A fails and rolls back; B waits, then commits. B's row must survive.
        barrier = asyncio.Barrier(2)
        a_rolled_back = asyncio.Event()

        async def task_a():
            with self.assertRaises(RuntimeError):
                async with self.db.transaction():
                    await self.db.execute("insert into t values ('A')")
                    await barrier.wait()
                    raise RuntimeError("A fails on purpose")
            a_rolled_back.set()

        async def task_b():
            await barrier.wait()
            async with self.db.transaction():
                await self.db.execute("insert into t values ('B')")

        await asyncio.gather(task_a(), task_b())
        await a_rolled_back.wait()

        rows = await self.db.execute_fetchall("select name from t order by name")
        self.assertEqual(rows, [("B",)])
        self.assertIsNone(self.db._transaction)
        self.assertEqual(self.db._transaction_waiters, [])

    async def test_waiter_and_owner_cancellation_leaves_no_ownership(self):
        a_entered = asyncio.Event()
        waiter_got_cancel = asyncio.Event()

        async def owner_a():
            async with self.db.transaction():
                await self.db.execute("insert into t values ('A')")
                a_entered.set()
                await asyncio.sleep(60)

        async def waiter_w():
            await a_entered.wait()
            try:
                await self.db.execute("insert into t values ('W')")
            except asyncio.CancelledError:
                waiter_got_cancel.set()
                raise

        owner_task = asyncio.ensure_future(owner_a())
        await a_entered.wait()
        waiter_task = asyncio.ensure_future(waiter_w())
        await asyncio.sleep(0.05)
        self.assertEqual(len(self.db._transaction_waiters), 1)

        waiter_task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter_task
        self.assertTrue(waiter_got_cancel.is_set())
        self.assertEqual(self.db._transaction_waiters, [])

        owner_task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await owner_task

        self.assertIsNone(self.db._transaction)
        self.assertEqual(self.db._transaction_waiters, [])

        # A later, unrelated task can take ownership normally.
        async with self.db.transaction():
            await self.db.execute("insert into t values ('B')")
        rows = await self.db.execute_fetchall("select name from t order by name")
        self.assertEqual(rows, [("B",)])

    async def test_child_task_inheriting_contextvar_denied(self):
        async with self.db.transaction():
            async def child():
                with self.assertRaises(TransactionError):
                    await self.db.execute("select 1")
                with self.assertRaises(TransactionError):
                    async with self.db.transaction():
                        pass

            # A real child task copies the current context (including the
            # ownership ContextVar) but remains a distinct task.
            await asyncio.create_task(child())

    async def test_cancel_waiter_blocked_on_transaction_entry(self):
        started = asyncio.Event()

        async def owner():
            async with self.db.transaction():
                await self.db.execute("insert into t values ('A')")
                started.set()
                await asyncio.sleep(0.3)

        async def waiter():
            await started.wait()
            async with self.db.transaction():
                await self.db.execute("insert into t values ('W')")

        owner_task = asyncio.ensure_future(owner())
        await started.wait()
        waiter_task = asyncio.ensure_future(waiter())
        await asyncio.sleep(0.05)
        self.assertEqual(len(self.db._transaction_waiters), 1)

        waiter_task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter_task
        self.assertEqual(self.db._transaction_waiters, [])

        await owner_task
        self.assertIsNone(self.db._transaction)
        self.assertEqual(
            await self.db.execute_fetchall("select name from t"), [("A",)]
        )

    async def test_owner_runs_connection_and_cursor_methods(self):
        async with self.db.transaction() as transaction:
            self.assertTrue(transaction.active)
            await self.db.execute("insert into t values ('C1')")
            cursor = await self.db.cursor()
            await cursor.execute("insert into t values ('C2')")
            await cursor.close()
        self.assertFalse(transaction.active)
        rows = await self.db.execute_fetchall("select name from t order by name")
        self.assertEqual(rows, [("C1",), ("C2",)])

    async def test_nested_transaction_rejected_without_side_effects(self):
        async with self.db.transaction():
            with self.assertRaisesRegex(TransactionError, "nested"):
                async with self.db.transaction():
                    pass

            # Owner context is still usable after the rejected attempt.
            await self.db.execute("insert into t values ('N')")
        rows = await self.db.execute_fetchall("select name from t")
        self.assertEqual(rows, [("N",)])

    async def test_manual_transaction_control_rejected(self):
        async with self.db.transaction():
            for statement in (
                "BEGIN",
                " begin immediate",
                "COMMIT",
                "rollback",
                "savepoint s",
                "release s",
            ):
                with self.assertRaises(TransactionError):
                    await self.db.execute(statement)
                with self.assertRaises(TransactionError):
                    cursor = await self.db.cursor()
                    await cursor.execute(statement)
            with self.assertRaises(TransactionError):
                await self.db.executescript("insert into t values ('X');")

    async def test_failed_commit_rolls_back_and_releases_ownership(self):
        db = await aiosqlite.connect(":memory:", factory=FailingCommit)
        try:
            await db.execute("create table t (name text)")
            await db.commit()

            FailingCommit.fail_next_commit = True
            with self.assertRaises(sqlite3.OperationalError):
                async with db.transaction():
                    await db.execute("insert into t values ('D')")

            self.assertIsNone(db._transaction)
            self.assertEqual(db._transaction_waiters, [])
            self.assertEqual(await db.execute_fetchall("select * from t"), [])

            async with db.transaction():
                await db.execute("insert into t values ('E')")
            self.assertEqual(
                await db.execute_fetchall("select name from t"), [("E",)]
            )
        finally:
            await db.close()

    async def test_stale_cursor_after_close(self):
        cursor = await self.db.execute("select 1")
        await self.db.close()
        with self.assertRaisesRegex(ValueError, "Connection closed"):
            await cursor.fetchall()

    async def test_legacy_connection_transaction_semantics_unchanged(self):
        await self.db.execute("insert into t values ('legacy')")
        await self.db.rollback()
        self.assertEqual(await self.db.execute_fetchall("select * from t"), [])

        await self.db.execute("insert into t values ('legacy2')")
        await self.db.commit()
        self.assertEqual(
            await self.db.execute_fetchall("select name from t"), [("legacy2",)]
        )

    async def test_close_while_transaction_active_cancels_waiters(self):
        started = asyncio.Event()

        async def owner():
            try:
                async with self.db.transaction():
                    await self.db.execute("insert into t values ('A')")
                    started.set()
                    await asyncio.sleep(60)
            except asyncio.CancelledError:
                pass

        owner_task = asyncio.ensure_future(owner())
        await started.wait()
        waiter_task = asyncio.ensure_future(self.db.execute("select 1"))
        await asyncio.sleep(0.05)

        await self.db.close()
        owner_task.cancel()
        waiter_task.cancel()
        await asyncio.gather(owner_task, waiter_task, return_exceptions=True)

        self.assertIsNone(self.db._transaction)
        self.assertEqual(self.db._transaction_waiters, [])
        self.assertFalse(self.db._running)
