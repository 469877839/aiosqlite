import asyncio, threading, time
import aiosqlite

async def main():
    db = await aiosqlite.connect(":memory:")
    await db.execute("CREATE TABLE t(x)")
    await db.execute("INSERT INTO t VALUES (1),(2),(3)")

    calls = []
    def prog_a():
        calls.append("a")
        return 0
    def prog_b():
        calls.append("b")
        return 0

    def slow(x):
        time.sleep(0.3)  # blocks the worker; queue items behind it cannot apply mid-query
        return x
    await db.create_function("slow", 1, slow)

    await db.set_progress_handler(prog_a, 1)
    # queue: slow query first, then the handler swap; swap only applies after query ends
    t0 = time.monotonic()
    q = asyncio.create_task(db.execute("SELECT slow(1)"))
    await asyncio.sleep(0.05)
    swap = asyncio.create_task(db.set_progress_handler(prog_b, 1))
    await q
    await swap
    print(f"swap applied after {time.monotonic()-t0:.2f}s; calls during slow query: {calls}")
    await (await db.execute("SELECT count(*) FROM t")).fetchall()
    print("calls after follow-up query:", calls)
    await db.close()

asyncio.run(main())
