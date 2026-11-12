# Copyright Amethyst Reese
# Licensed under the MIT license

"""
Explicit, task-local transaction ownership for a shared connection.
"""

import asyncio
import contextvars
from types import TracebackType
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .core import Connection

__all__ = ["Transaction", "TransactionError"]

#: Task-local ownership of a connection's explicit transaction. Inheriting
#: this value in a child task does not grant ownership; only the task that
#: entered the transaction context may use the connection while it is active.
_TRANSACTION_OWNER: contextvars.ContextVar[Optional["Transaction"]] = (
    contextvars.ContextVar("aiosqlite_transaction_owner", default=None)
)


class TransactionError(RuntimeError):
    """Raised when explicit transaction ownership rules are violated."""


class Transaction:
    """
    Async context manager granting one task exclusive ownership of the
    connection's transaction.

    Other tasks that use the connection while a transaction is active wait
    until the transaction is committed or rolled back instead of mixing their
    work into the owner's connection-level transaction.

    Inheriting the ownership :class:`~contextvars.ContextVar` (for example via
    ``asyncio.gather``) does not grant ownership: only the task that entered
    the context may run connection or cursor operations while it is active.
    """

    __slots__ = ("_conn", "_entered", "_finished", "_owner_task")

    def __init__(self, conn: "Connection") -> None:
        self._conn = conn
        self._entered = False
        self._finished = False
        self._owner_task: Optional[asyncio.Task] = None

    async def __aenter__(self) -> "Transaction":
        if self._entered:
            raise TransactionError("transaction context cannot be reused")

        current_task = asyncio.current_task()
        while True:
            current = self._conn._current_transaction()
            if current is None:
                break
            if current._owner_task is current_task:
                raise TransactionError("nested transactions are not supported")
            await self._conn._wait_for_transaction()

        await self._conn._execute(self._begin, _bypass_transaction=True)
        self._owner_task = current_task
        self._conn._begin_transaction(self)
        self._entered = True
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if not self._entered:
            return

        try:
            if exc_type is None:
                try:
                    await self._conn._execute(
                        self._conn._conn.commit, _bypass_transaction=True
                    )
                except BaseException:  # noqa B036
                    await self._rollback()
                    raise
            else:
                await self._rollback()
        finally:
            self._finished = True
            self._conn._end_transaction(self)

    def _begin(self) -> None:
        conn = self._conn._conn
        if conn.in_transaction:
            raise TransactionError(
                "cannot start an explicit transaction while another transaction "
                "is already active on this connection"
            )
        conn.execute("BEGIN")

    async def _rollback(self) -> None:
        try:
            await self._conn._execute(
                self._conn._conn.rollback, _bypass_transaction=True
            )
        except BaseException:  # noqa B036
            pass

    @property
    def active(self) -> bool:
        """Whether this context currently owns the connection transaction."""
        return self._entered and not self._finished
