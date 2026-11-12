import asyncio, faulthandler, os, threading, time
import aiosqlite

# watchdog: dump all thread stacks at 2s, hard-exit at 4s so the script always terminates
faulthandler.dump_traceback_later(2.0, repeat=False)
threading.Timer(4.0, lambda: (print("WATCHDOG: still blocked, os._exit(3)", flush=True), os._exit(3))).start()

async def main():
    db = await aiosqlite.connect(":memory:")
    def udf(x):
        print("  [udf] entered on worker thread; calling asyncio.run(db.execute)", flush=True)
        return asyncio.run(db.execute("SELECT 1")).result()
    await db.create_function("reenter", 1, udf)
    print("outer query starting", flush=True)
    cur = await db.execute("SELECT reenter(1)")
    print("outer query done:", await cur.fetchall(), flush=True)
    await db.close()

asyncio.run(main())
print("NO DEADLOCK", flush=True)
