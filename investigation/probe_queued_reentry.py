import asyncio, concurrent.futures, time, threading
import aiosqlite

async def main():
    db = await aiosqlite.connect(":memory:")
    loop = asyncio.get_running_loop()
    await db.execute("CREATE TABLE t(x)")
    await db.execute("INSERT INTO t VALUES (10),(20),(30)")

    inner_done = threading.Event()
    async def inner_query():
        # proves the inner tx was actually processed once the worker freed up
        cur = await db.execute("SELECT count(*) FROM t")
        rows = await cur.fetchall()
        inner_done.set()
        return rows

    def udf(x):
        t0 = time.monotonic()
        fut = asyncio.run_coroutine_threadsafe(inner_query(), loop)
        try:
            fut.result(timeout=1.0)
        except concurrent.futures.TimeoutError:
            print(f"  [udf] inner query timed out after {time.monotonic()-t0:.2f}s "
                  f"(queue size {db._tx.qsize()}); returning anyway", flush=True)
            return -1
        return 0
    await db.create_function("reenter", 1, udf)

    cur = await db.execute("SELECT reenter(1)")
    print("outer result:", await cur.fetchall())
    # after the callback returns, the queued inner tx must get processed
    await asyncio.sleep(0.2)
    print("inner eventually ran:", inner_done.is_set(), flush=True)
    await db.close()

t0 = time.monotonic()
asyncio.run(main())
print(f"elapsed {time.monotonic()-t0:.2f}s")
