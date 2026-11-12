# Copyright Amethyst Reese
# Licensed under the MIT license

"""
Standalone reproduction for the closed-event-loop worker crash.

Before the fix in aiosqlite/core.py, closing the event loop that submitted a
queued operation made the worker's ``call_soon_threadsafe`` raise
``RuntimeError``; the exception handler repeated the same call and the worker
thread died, stranding every later operation and leaking the SQLite
connection.  Run directly::

    python scripts/reproduce_closed_loop_worker.py

Exit code 0 means the worker survives the dead loop, later operations
complete, and the connection is released cleanly.
"""

import asyncio
import threading

import aiosqlite


class LoopThread(threading.Thread):
    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.loop = asyncio.new_event_loop()
        self.ready = threading.Event()

    def run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.ready.set()
        self.loop.run_forever()
        asyncio.set_event_loop(None)

    def submit(self, coro):
        self.ready.wait(5)
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def close(self) -> None:
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.join(5)
        self.loop.close()


def main() -> None:
    setup_loop = asyncio.new_event_loop()

    async def open_connection() -> aiosqlite.Connection:
        return await aiosqlite.connect(":memory:")

    db = setup_loop.run_until_complete(open_connection())
    worker = db._thread

    gate_entered = threading.Barrier(2)
    gate_release = threading.Event()

    def blocking_operation() -> str:
        gate_entered.wait(5)
        gate_release.wait(5)
        return "gate"

    first = LoopThread()
    first.start()
    first.submit(db._execute(blocking_operation))
    gate_entered.wait(5)

    second = LoopThread()
    second.start()
    second_future = second.submit(db.execute_fetchall("select 42"))
    threading.Event().wait(0.1)

    # Kill the first loop while its operation is still queued in the worker.
    first.close()
    gate_release.set()

    assert worker.is_alive(), "worker thread died with the closed submitting loop"
    assert second_future.result(5) == [(42,)], "later operation never completed"

    async def probe():
        return await db.execute_fetchall("select 7")

    assert second.submit(probe()).result(5) == [(7,)]
    second.submit(db.close()).result(5)
    second.close()
    worker.join(5)

    assert not worker.is_alive(), "worker thread was not terminated by close()"
    assert db._connection is None, "sqlite connection was not released"
    setup_loop.close()
    print("ok: worker survived the closed loop and the connection was released")


if __name__ == "__main__":
    main()
