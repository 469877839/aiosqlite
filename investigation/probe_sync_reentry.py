import asyncio, threading
import aiosqlite

async def main():
    db = await aiosqlite.connect(":memory:")
    await db.execute("CREATE TABLE t(x)")
    await db.execute("INSERT INTO t VALUES (1),(2),(3)")
    conn = db._conn  # raw sqlite3 connection; only usable on the worker thread

    def udf_sync(x):
        # same-thread synchronous reentrancy on the raw connection
        rows = conn.execute("SELECT sum(x) FROM t WHERE x <= ?", (x,)).fetchall()
        return rows[0][0]
    await db.create_function("reenter", 1, udf_sync)
    cur = await db.execute("SELECT reenter(2)")
    print("sync reentrant UDF ->", await cur.fetchall())

    # what happens if the main/coroutine thread touches the raw connection directly?
    try:
        conn.execute("SELECT 1").fetchall()
    except Exception as e:
        print("main-thread raw access ->", type(e).__name__, str(e))
    await db.close()

asyncio.run(main())
