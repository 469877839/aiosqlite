import asyncio, threading, time
import aiosqlite

async def cancellation_case():
    db = await aiosqlite.connect(":memory:")
    def slow(x):
        time.sleep(0.4)
        return x
    await db.create_function("slow", 1, slow)
    task = asyncio.create_task(db.execute("SELECT slow(42)"))
    await asyncio.sleep(0.1)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        print("outer task: CancelledError")
    await asyncio.sleep(0.6)  # let the worker finish the in-flight call
    cur = await db.execute("SELECT 1")
    print("connection still usable:", await cur.fetchall())
    await db.close()

def closed_loop_case():
    loop = asyncio.new_event_loop()
    async def setup():
        db = await aiosqlite.connect(":memory:")
        def slow(x):
            time.sleep(0.5)
            return x
        await db.create_function("slow", 1, slow)
        return db
    db = loop.run_until_complete(setup())

    async def q():
        return await db.execute("SELECT slow(1)")
    task = loop.create_task(q())
    loop.run_until_complete(asyncio.sleep(0.1))
    print("query in flight, closing loop while worker still busy")
    loop.close()
    t0 = time.monotonic()
    while db._thread.is_alive() and time.monotonic() - t0 < 2:
        time.sleep(0.05)
    print("worker alive after loop closed:", db._thread.is_alive(),
          "task done:", task.done())
    try:
        db.stop()
    except Exception as e:
        print("stop ->", type(e).__name__, str(e))

asyncio.run(cancellation_case())
print()
closed_loop_case()
time.sleep(0.2)
