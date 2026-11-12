# Copyright Amethyst Reese
# Licensed under the MIT license

"""
Core implementation of aiosqlite proxies
"""

import asyncio
import enum
import logging
import sqlite3
from collections import deque
from collections.abc import AsyncIterator, Generator, Iterable
from functools import partial
from pathlib import Path
from queue import Empty, Queue
from threading import Event, Lock, Thread
from typing import Any, Callable, Literal, Optional, Union
from warnings import warn

from .context import contextmanager
from .cursor import Cursor

__all__ = [
    "connect",
    "Connection",
    "Cursor",
    "CancellationPolicy",
    "CancellationInfo",
    "OperationCancelledError",
]

AuthorizerCallback = Callable[[int, str, str, str, str], int]

LOG = logging.getLogger("aiosqlite")


IsolationLevel = Optional[Literal["DEFERRED", "IMMEDIATE", "EXCLUSIVE"]]


def set_result(fut: asyncio.Future, result: Any) -> None:
    """Set the result of a future if it hasn't been set already."""
    if not fut.done():
        fut.set_result(result)


def set_exception(fut: asyncio.Future, e: BaseException) -> None:
    """Set the exception of a future if it hasn't been set already."""
    if not fut.done():
        fut.set_exception(e)


_STOP_RUNNING_SENTINEL = object()


class CancellationPolicy(enum.Enum):
    """
    Declared behavior when a cancellable operation is cancelled.

    ``NEVER``
        Cancellation never reaches the worker thread.  Operations that were
        still queued are skipped, operations already executing always run to
        completion; the caller receives ``OperationCancelledError`` only when
        the operation never executed.

    ``INTERRUPT``
        Same queued-operation handling as ``NEVER``, but an operation that has
        already started executing in the worker thread is aborted through
        ``sqlite3.Connection.interrupt()``.  The interrupt is scoped to that
        operation only and is never issued once the operation has finished its
        SQLite work, so a later operation cannot be affected.
    """

    NEVER = "never"
    INTERRUPT = "interrupt"


class OperationCancelledError(asyncio.CancelledError):
    """
    Raised on a cancellable operation whose result is not delivered.

    The :attr:`CancellationInfo.phase` attribute describes how far the
    operation got, which determines the database side effects:

    ``"queued"``
        The operation was removed from the queue before the worker thread ever
        executed it, so it has no database side effects at all.

    ``"running"``
        The operation was already executing when cancelled, but its policy did
        not permit interrupting it, so it ran to completion.  Its full effects
        are in place; only the result is discarded.

    ``"completed"``
        The operation had already finished its SQLite work when cancellation
        won the race against completion.  Its full effects are in place; the
        result is discarded and this error is raised instead.

    ``"interrupted"``
        The operation was executing in the worker thread and was aborted by an
        interrupt.  An interrupted statement is rolled back by SQLite at the
        statement level, but any surrounding connection transaction and any
        earlier completed statements are left untouched.

    Cancellation never rolls back the whole connection transaction on its own.
    """

    def __init__(self, info: "CancellationInfo") -> None:
        self.info = info
        super().__init__(f"operation cancelled during {info.phase}")


class CancellationInfo:
    """
    Describes the outcome of cancelling one cancellable operation.

    ``operation_id`` is the identity of the cancelled operation as assigned
    by the owning connection; ``phase`` is one of ``"queued"``, ``"running"``,
    ``"interrupted"``, or ``"completed"``; ``interrupted`` is True only when
    ``sqlite3.Connection.interrupt()`` was actually delivered for it.
    """

    __slots__ = ("operation_id", "phase", "interrupted")

    def __init__(self, operation_id: int, phase: str, interrupted: bool) -> None:
        self.operation_id = operation_id
        self.phase = phase
        self.interrupted = interrupted

    def __repr__(self) -> str:
        return (
            "CancellationInfo("
            f"operation_id={self.operation_id}, phase={self.phase!r}, "
            f"interrupted={self.interrupted})"
        )


class _Operation:
    """
    One unit of work for the connection's worker thread.

    Each operation carries its own identity and lifecycle state so that
    cancellation can be correlated with its exact position in the queue and
    with the SQLite execution phase, instead of interrupting the connection
    unconditionally.

    :meta private:
    """

    __slots__ = (
        "operation_id",
        "future",
        "function",
        "cancel_event",
        "committed",
        "cancelled",
        "cancelled_outcome",
        "finished",
        "interrupt_requested",
        "interrupt_on_cancel",
        "task",
    )

    def __init__(
        self,
        operation_id: int,
        future: Optional[asyncio.Future],
        function: Callable[[], Any],
        cancel_event: Optional[Event],
        interrupt_on_cancel: bool,
    ) -> None:
        self.operation_id = operation_id
        self.future = future
        self.function = function
        self.cancel_event = cancel_event
        #: Set under the queue lock once the worker thread owns this operation.
        self.committed = False
        #: Set under the queue lock by the canceller.
        self.cancelled = False
        #: Phase recorded by the canceller, or finally justified in ``finished``.
        self.cancelled_outcome: Optional[str] = None
        #: Set under the queue lock after the SQLite work returned (or failed).
        self.finished = False
        #: Set under the queue lock when an interrupt should be delivered.
        self.interrupt_requested = False
        self.interrupt_on_cancel = interrupt_on_cancel
        self.task: Optional[asyncio.Task] = None


class _OperationQueue:
    """
    FIFO queue with per-operation cancellation support.

    Cancellation is decided under a lock shared with the worker thread so the
    queued/executing boundary is observed exactly once per operation.

    :meta private:
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._pending: deque[_Operation] = deque()
        self._wakeup = Event()
        self._running: Optional[_Operation] = None

    def enqueue(self, operation: _Operation) -> None:
        with self._lock:
            self._pending.append(operation)
        self._wakeup.set()

    def empty(self) -> bool:
        """True when nothing is queued and no operation is executing."""
        with self._lock:
            return not self._pending and self._running is None

    def dequeue(self) -> Optional[_Operation]:
        """Hand the next non-cancelled operation to the worker thread."""
        while True:
            self._wakeup.wait()
            with self._lock:
                operation = self._take_locked()
            if operation is not None:
                return operation

    def _take_locked(self) -> Optional[_Operation]:
        while self._pending:
            operation = self._pending.popleft()
            if operation.cancelled:
                self._finalize_queued_cancel(operation)
                continue
            operation.committed = True
            self._running = operation
            if not self._pending:
                self._wakeup.clear()
            return operation
        self._wakeup.clear()
        return None

    def finish(self, operation: _Operation) -> None:
        """
        Record that the SQLite work for ``operation`` has returned.

        If a cancellation raced the completion, this is where the winner is
        decided; the outcome is stashed on the operation for the connection to
        translate into either an interrupt or an ordinary completion.
        """
        with self._lock:
            if operation is self._running:
                self._running = None
            operation.finished = True
            if operation.cancelled and operation.cancelled_outcome is None:
                operation.cancelled_outcome = "completed"

    def request_interrupt(self, operation: _Operation) -> bool:
        """
        Decide under the lock whether ``operation`` still owns the connection.

        Returns True only when the operation is the active, not-yet-finished
        one, guaranteeing an interrupt can never land on a later caller.
        """
        with self._lock:
            if (
                operation is self._running
                and not operation.finished
                and operation.interrupt_requested
            ):
                return True
            return False

    def cancel(self, operation: _Operation) -> Optional[CancellationInfo]:
        """
        Try to cancel one operation.

        Returns a ``CancellationInfo`` describing the result, or ``None`` when
        the operation already completed and its result stands.
        """
        with self._lock:
            if operation is self._running:
                if operation.cancelled:
                    return None
                operation.cancelled = True
                if operation.interrupt_on_cancel:
                    if operation.finished:
                        operation.cancelled_outcome = "completed"
                        return CancellationInfo(
                            operation.operation_id, "completed", False
                        )
                    operation.interrupt_requested = True
                    operation.cancelled_outcome = "interrupted"
                    return CancellationInfo(
                        operation.operation_id, "interrupted", True
                    )
                operation.cancelled_outcome = "running"
                return CancellationInfo(operation.operation_id, "running", False)

            if operation.cancelled:
                return None

            if operation.finished:
                return None

            for queued in self._pending:
                if queued is operation:
                    operation.cancelled = True
                    operation.cancelled_outcome = "queued"
                    return CancellationInfo(
                        operation.operation_id, "queued", False
                    )
            return None

    def _finalize_queued_cancel(self, operation: _Operation) -> None:
        info = CancellationInfo(operation.operation_id, "queued", False)
        future = operation.future
        if future is None:
            return
        future.get_loop().call_soon_threadsafe(
            set_exception, future, OperationCancelledError(info)
        )


def _connection_worker_thread(tx: "_OperationQueue"):
    """
    Execute function calls on a separate thread.

    Continues running until all queue items are processed, even after the
    connection is closed, so every queued future is finalized.

    :meta private:
    """
    while True:
        operation = tx.dequeue()
        if operation is None:
            continue

        try:
            LOG.debug("executing %s", operation.function)
            result = operation.function()

            if result is _STOP_RUNNING_SENTINEL:
                # Finish before waking the closer so that `empty()` is a
                # reliable post-condition once `close()` returns.
                tx.finish(operation)

            future = operation.future
            if future is not None and not future.done():
                future.get_loop().call_soon_threadsafe(set_result, future, result)
            LOG.debug("operation %s completed", operation.function)

            if result is _STOP_RUNNING_SENTINEL:
                break

        except BaseException as e:  # noqa B036
            LOG.debug("returning exception %s", e)
            future = operation.future
            if future is not None and not future.done():
                future.get_loop().call_soon_threadsafe(set_exception, future, e)

        finally:
            tx.finish(operation)


class Connection:
    def __init__(
        self,
        connector: Callable[[], sqlite3.Connection],
        iter_chunk_size: int,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        self._running = True
        self._connection: Optional[sqlite3.Connection] = None
        self._connector = connector
        self._tx = _OperationQueue()
        self._iter_chunk_size = iter_chunk_size
        self._thread = Thread(target=_connection_worker_thread, args=(self._tx,))
        self._operation_ids = 0
        self._operation_id_lock = Lock()

        if loop is not None:
            warn(
                "aiosqlite.Connection no longer uses the `loop` parameter",
                DeprecationWarning,
            )

    def __del__(self):
        if self._connection is None:
            return

        warn(
            (
                f"{self!r} was deleted before being closed. "
                "Please use 'async with' or '.close()' to close the connection properly."
            ),
            ResourceWarning,
            stacklevel=1,
        )

        # Don't try to be creative here, the event loop may have already been closed.
        # Simply stop the worker thread, and let the underlying sqlite3 connection
        # be finalized by its own __del__.
        self.stop()

    def stop(self) -> Optional[asyncio.Future]:
        """Stop the background thread. Prefer `async with` or `await close()`"""
        self._running = False

        def close_and_stop():
            if self._connection is not None:
                self._connection.close()
                self._connection = None
            return _STOP_RUNNING_SENTINEL

        try:
            future = asyncio.get_event_loop().create_future()
        except Exception:
            future = None

        operation = _Operation(
            self._next_operation_id(), future, close_and_stop, None, False
        )
        self._tx.enqueue(operation)
        return future

    def _next_operation_id(self) -> int:
        with self._operation_id_lock:
            self._operation_ids += 1
            return self._operation_ids

    @property
    def _conn(self) -> sqlite3.Connection:
        if self._connection is None:
            raise ValueError("no active connection")

        return self._connection

    def _execute_insert(self, sql: str, parameters: Any) -> Optional[sqlite3.Row]:
        cursor = self._conn.execute(sql, parameters)
        cursor.execute("SELECT last_insert_rowid()")
        return cursor.fetchone()

    def _execute_fetchall(self, sql: str, parameters: Any) -> Iterable[sqlite3.Row]:
        cursor = self._conn.execute(sql, parameters)
        return cursor.fetchall()

    async def _execute(self, fn, *args, **kwargs):
        """Queue a function with the given arguments for execution."""
        if not self._running or not self._connection:
            raise ValueError("Connection closed")

        function = partial(fn, *args, **kwargs)
        future = asyncio.get_event_loop().create_future()
        operation = _Operation(
            self._next_operation_id(), future, function, None, False
        )
        self._tx.enqueue(operation)

        return await future

    async def _execute_cancellable(
        self,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        policy: CancellationPolicy,
    ) -> Any:
        """
        Queue a function whose cancellation is correlated with its execution.

        Unlike plain ``_execute``, cancelling the returned awaitable only
        affects this single operation: queued work is skipped before it ever
        touches SQLite, and executing work is interrupted only when the caller
        explicitly opted into ``CancellationPolicy.INTERRUPT``.
        """
        if not self._running or not self._connection:
            raise ValueError("Connection closed")

        future = asyncio.get_event_loop().create_future()
        operation = self._new_operation(
            partial(fn, *args, **kwargs), future, policy
        )
        return await self._await_operation(operation, future)

    def _new_operation(
        self,
        function: Callable[[], Any],
        future: asyncio.Future,
        policy: CancellationPolicy,
    ) -> _Operation:
        cancel_event = Event() if policy is CancellationPolicy.INTERRUPT else None
        return _Operation(
            self._next_operation_id(),
            future,
            function,
            cancel_event,
            policy is CancellationPolicy.INTERRUPT,
        )

    async def _await_operation(
        self, operation: _Operation, future: asyncio.Future
    ) -> Any:
        self._tx.enqueue(operation)
        try:
            return await future
        except asyncio.CancelledError:
            info = self._cancel_operation(operation)
            if info is None:
                # The operation completed before cancellation was observed.
                # Its result (and side effects) stand; the await was already
                # cancelled, so surface that instead of the discarded result.
                raise
            raise OperationCancelledError(info) from None

    def _cancel_operation(self, operation: _Operation) -> Optional[CancellationInfo]:
        info = self._tx.cancel(operation)
        if info is not None and info.interrupted:
            # Only interrupt while this exact operation still owns the
            # connection and has not finished.  sqlite3.interrupt() with no
            # statement in flight is a no-op, and the queue lock guarantees
            # the next operation cannot have started yet, so the interrupt
            # cannot land on a later caller's statement.
            if self._tx.request_interrupt(operation):
                self._conn.interrupt()
                if operation.cancel_event is not None:
                    # Release any progress/UDF hook parked for this operation.
                    operation.cancel_event.set()
            else:
                # Completion won between the cancel() and interrupt() calls.
                outcome = operation.cancelled_outcome or "completed"
                info = CancellationInfo(operation.operation_id, outcome, False)
        return info

    async def _connect(self) -> "Connection":
        """Connect to the actual sqlite database."""
        if self._connection is None:
            try:
                future = asyncio.get_event_loop().create_future()
                operation = _Operation(
                    self._next_operation_id(), future, self._connector, None, False
                )
                self._tx.enqueue(operation)
                self._connection = await future
            except BaseException:
                self.stop()
                self._connection = None
                raise

        return self

    def __await__(self) -> Generator[Any, None, "Connection"]:
        self._thread.start()
        return self._connect().__await__()

    async def __aenter__(self) -> "Connection":
        return await self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    @contextmanager
    async def cursor(self) -> Cursor:
        """Create an aiosqlite cursor wrapping a sqlite3 cursor object."""
        return Cursor(self, await self._execute(self._conn.cursor))

    async def commit(self) -> None:
        """Commit the current transaction."""
        await self._execute(self._conn.commit)

    async def rollback(self) -> None:
        """Roll back the current transaction."""
        await self._execute(self._conn.rollback)

    async def close(self) -> None:
        """Complete queued queries/cursors and close the connection."""

        if self._connection is None:
            return

        try:
            await self._execute(self._conn.close)
        except Exception:
            LOG.info("exception occurred while closing connection")
            raise
        finally:
            self._connection = None
            future = self.stop()
            if future:
                await future

    @contextmanager
    async def execute(
        self, sql: str, parameters: Optional[Iterable[Any]] = None
    ) -> Cursor:
        """Helper to create a cursor and execute the given query."""
        if parameters is None:
            parameters = []
        cursor = await self._execute(self._conn.execute, sql, parameters)
        return Cursor(self, cursor)

    @contextmanager
    async def execute_insert(
        self, sql: str, parameters: Optional[Iterable[Any]] = None
    ) -> Optional[sqlite3.Row]:
        """Helper to insert and get the last_insert_rowid."""
        if parameters is None:
            parameters = []
        return await self._execute(self._execute_insert, sql, parameters)

    @contextmanager
    async def execute_fetchall(
        self, sql: str, parameters: Optional[Iterable[Any]] = None
    ) -> Iterable[sqlite3.Row]:
        """Helper to execute a query and return all the data."""
        if parameters is None:
            parameters = []
        return await self._execute(self._execute_fetchall, sql, parameters)

    @contextmanager
    async def execute_cancellable(
        self,
        sql: str,
        parameters: Optional[Iterable[Any]] = None,
        *,
        policy: CancellationPolicy = CancellationPolicy.NEVER,
    ) -> Cursor:
        """
        Execute a query with explicit, per-operation cancellation semantics.

        Cancelling the returned awaitable (e.g. via ``asyncio.Task.cancel`` or
        ``asyncio.wait_for``) affects only this operation, never the whole
        connection:

        - if the operation is still queued, it is removed and never executed,
          so it has no database side effects;
        - if it is already executing, it is interrupted only when
          ``policy=CancellationPolicy.INTERRUPT`` was declared, otherwise it
          runs to completion and its result is discarded;
        - if it already completed, its effects stand and the caller observes
          the cancellation instead of the result.

        Cancellation raises :class:`OperationCancelledError` whose
        ``info.phase`` attribute reports which of the above happened.  An
        interrupted statement is rolled back by SQLite at the statement level
        only; the surrounding connection transaction is never rolled back
        automatically, and earlier committed work is unaffected.
        """
        if parameters is None:
            parameters = []
        cursor = await self._execute_cancellable(
            self._conn.execute, (sql, parameters), {}, policy
        )
        return Cursor(self, cursor)

    @contextmanager
    async def execute_fetchall_cancellable(
        self,
        sql: str,
        parameters: Optional[Iterable[Any]] = None,
        *,
        policy: CancellationPolicy = CancellationPolicy.NEVER,
    ) -> Iterable[sqlite3.Row]:
        """
        Execute a query and return all rows, with the same explicit
        cancellation semantics as :meth:`execute_cancellable`.
        """
        if parameters is None:
            parameters = []
        return await self._execute_cancellable(
            self._execute_fetchall, (sql, parameters), {}, policy
        )

    async def _cancellable_barrier(self, policy: CancellationPolicy) -> _Operation:
        """
        Run :meth:`_run_barrier` under the cancellation machinery and return
        the in-flight operation so tests can interleave cancellation with the
        exact SQLite execution phase.

        :meta private:
        """
        future = asyncio.get_event_loop().create_future()
        cancel_event = Event()
        operation = _Operation(
            self._next_operation_id(),
            future,
            lambda: None,
            cancel_event,
            policy is CancellationPolicy.INTERRUPT,
        )
        operation.function = partial(self._run_barrier, operation)

        async def drive() -> None:
            await self._await_operation(operation, future)

        operation.task = asyncio.ensure_future(drive())
        return operation

    def _release_barrier(self, operation: _Operation) -> None:
        """
        Release a parked barrier operation so its statement can finish.

        :meta private:
        """
        if operation.cancel_event is None:
            return
        operation.cancel_event.set()

    def _run_barrier(self, operation: _Operation) -> None:
        """
        Park the worker thread inside a SQLite progress hook until the
        operation is either cancelled or explicitly released.

        The hook runs on the worker thread in the middle of statement
        execution, so blocking it there lets a canceller observe the exact
        "executing" phase and exercise the interrupt path deterministically.
        When the operation was cancelled under ``INTERRUPT``, the hook returns
        ``1`` itself as a backstop; otherwise it returns ``0`` and the queued
        statement completes normally.

        :meta private:
        """
        sqlite_connection = self._conn
        assert operation.cancel_event is not None
        cancel_event = operation.cancel_event

        def hook() -> int:
            cancel_event.wait()
            return 1 if operation.interrupt_requested else 0

        sqlite_connection.set_progress_handler(hook, 1)
        try:
            sqlite_connection.execute("select 1").fetchall()
        finally:
            sqlite_connection.set_progress_handler(None, 0)

    @contextmanager
    async def executemany(
        self, sql: str, parameters: Iterable[Iterable[Any]]
    ) -> Cursor:
        """Helper to create a cursor and execute the given multiquery."""
        cursor = await self._execute(self._conn.executemany, sql, parameters)
        return Cursor(self, cursor)

    @contextmanager
    async def executescript(self, sql_script: str) -> Cursor:
        """Helper to create a cursor and execute a user script."""
        cursor = await self._execute(self._conn.executescript, sql_script)
        return Cursor(self, cursor)

    async def interrupt(self) -> None:
        """Interrupt pending queries."""
        return self._conn.interrupt()

    async def create_function(
        self, name: str, num_params: int, func: Callable, deterministic: bool = False
    ) -> None:
        """
        Create user-defined function that can be later used
        within SQL statements. Must be run within the same thread
        that query executions take place so instead of executing directly
        against the connection, we defer this to `run` function.

        If ``deterministic`` is true, the created function is marked as deterministic,
        which allows SQLite to perform additional optimizations. This flag is supported
        by SQLite 3.8.3 or higher, ``NotSupportedError`` will be raised if used with
        older versions.
        """
        await self._execute(
            self._conn.create_function,
            name,
            num_params,
            func,
            deterministic=deterministic,
        )

    @property
    def in_transaction(self) -> bool:
        return self._conn.in_transaction

    @property
    def isolation_level(self) -> Optional[str]:
        return self._conn.isolation_level

    @isolation_level.setter
    def isolation_level(self, value: IsolationLevel) -> None:
        self._conn.isolation_level = value

    @property
    def row_factory(self) -> Optional[type]:
        return self._conn.row_factory

    @row_factory.setter
    def row_factory(self, factory: Optional[type]) -> None:
        self._conn.row_factory = factory

    @property
    def text_factory(self) -> Callable[[bytes], Any]:
        return self._conn.text_factory

    @text_factory.setter
    def text_factory(self, factory: Callable[[bytes], Any]) -> None:
        self._conn.text_factory = factory

    @property
    def total_changes(self) -> int:
        return self._conn.total_changes

    async def enable_load_extension(self, value: bool) -> None:
        await self._execute(self._conn.enable_load_extension, value)  # type: ignore

    async def load_extension(self, path: str):
        await self._execute(self._conn.load_extension, path)  # type: ignore

    async def set_progress_handler(
        self, handler: Callable[[], Optional[int]], n: int
    ) -> None:
        await self._execute(self._conn.set_progress_handler, handler, n)

    async def set_trace_callback(self, handler: Callable) -> None:
        await self._execute(self._conn.set_trace_callback, handler)

    async def set_authorizer(
        self, authorizer_callback: Optional[AuthorizerCallback]
    ) -> None:
        """
        Set an authorizer callback to control database access.

        The authorizer callback is invoked for each SQL statement that is prepared,
        and controls whether specific operations are permitted.

        Example::

            import sqlite3

            def restrict_drops(action_code, arg1, arg2, db_name, trigger_name):
                # Deny all DROP operations
                if action_code == sqlite3.SQLITE_DROP_TABLE:
                    return sqlite3.SQLITE_DENY
                # Allow everything else
                return sqlite3.SQLITE_OK

            await conn.set_authorizer(restrict_drops)

        See ``sqlite3`` documentation for details:
        https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.set_authorizer

        :param authorizer_callback: An optional callable that receives five arguments:

            - ``action_code`` (int): The action to be authorized (e.g., ``SQLITE_READ``)
            - ``arg1`` (str): First argument, meaning depends on ``action_code``
            - ``arg2`` (str): Second argument, meaning depends on ``action_code``
            - ``db_name`` (str): Database name (e.g., ``"main"``, ``"temp"``)
            - ``trigger_name`` (str): Name of trigger or view that is doing the access,
              or ``None``

            The callback should return:

            - ``SQLITE_OK`` (0): Allow the operation
            - ``SQLITE_DENY`` (1): Deny the operation, raise ``sqlite3.DatabaseError``
            - ``SQLITE_IGNORE`` (2): Treat operation as no-op

            Pass ``None`` to remove the authorizer.
        """
        await self._execute(self._conn.set_authorizer, authorizer_callback)

    async def iterdump(self) -> AsyncIterator[str]:
        """
        Return an async iterator to dump the database in SQL text format.

        Example::

            async for line in db.iterdump():
                ...

        """
        dump_queue: Queue = Queue()

        def dumper():
            try:
                for line in self._conn.iterdump():
                    dump_queue.put_nowait(line)
                dump_queue.put_nowait(None)

            except Exception:
                LOG.exception("exception while dumping db")
                dump_queue.put_nowait(None)
                raise

        fut = self._execute(dumper)
        task = asyncio.ensure_future(fut)

        while True:
            try:
                line: Optional[str] = dump_queue.get_nowait()
                if line is None:
                    break
                yield line

            except Empty:
                if task.done():
                    LOG.warning("iterdump completed unexpectedly")
                    break

                await asyncio.sleep(0.01)

        await task

    async def backup(
        self,
        target: Union["Connection", sqlite3.Connection],
        *,
        pages: int = 0,
        progress: Optional[Callable[[int, int, int], None]] = None,
        name: str = "main",
        sleep: float = 0.250,
    ) -> None:
        """
        Make a backup of the current database to the target database.

        Takes either a standard sqlite3 or aiosqlite Connection object as the target.
        """
        if isinstance(target, Connection):
            target = target._conn

        await self._execute(
            self._conn.backup,
            target,
            pages=pages,
            progress=progress,
            name=name,
            sleep=sleep,
        )


def connect(
    database: Union[str, Path],
    *,
    iter_chunk_size=64,
    loop: Optional[asyncio.AbstractEventLoop] = None,
    **kwargs: Any,
) -> Connection:
    """Create and return a connection proxy to the sqlite database."""

    if loop is not None:
        warn(
            "aiosqlite.connect() no longer uses the `loop` parameter",
            DeprecationWarning,
        )

    def connector() -> sqlite3.Connection:
        if isinstance(database, str):
            loc = database
        elif isinstance(database, bytes):
            loc = database.decode("utf-8")
        else:
            loc = str(database)

        return sqlite3.connect(loc, **kwargs)

    return Connection(connector, iter_chunk_size)
