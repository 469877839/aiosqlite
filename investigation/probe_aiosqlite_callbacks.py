import asyncio, sqlite3, threading, os

import aiosqlite

sqlite3.enable_callback_tracebacks(True)
MAIN = threading.get_ident()

def in_cb(tag, ret=None):
    def cb(*args):
        try:
            asyncio.get_running_loop()
            loop = "RUNNING"
        except RuntimeError:
            loop = "NO_LOOP"
        print(f"  [{tag}] tid={threading.get_ident()} worker={THREAD_ID[0]} main={MAIN} loop={loop} args={args!r}", flush=True)
        return ret
    return cb

THREAD_ID = [None]

async def main():
    async with aiosqlite.connect(":memory:") as db:
        THREAD_ID[0] = db._thread.ident
        print("main tid", MAIN, "worker tid", THREAD_ID[0], "thread name", db._thread.name)
        await db.execute("CREATE TABLE t(x)")
        await db.execute("INSERT INTO t VALUES (1),(2),(3)")

        print("\n== UDF ==")
        await db.create_function("f_ok", 1, in_cb("udf-ok", 7))
        def bad(x):
            raise ValueError("boom-udf")
        await db.create_function("f_raise", 1, bad)
        await db.create_function("f_obj", 1, lambda x: object())

        for sql in ["SELECT f_ok(1)", "SELECT f_raise(1)", "SELECT f_obj(1)"]:
            try:
                cur = await db.execute(sql)
                print(sql, "->", await cur.fetchall())
            except Exception as e:
                print(sql, "-> EXC", type(e).__name__, repr(str(e)), "cause", repr(e.__cause__), "ctx", repr(e.__context__))

        print("\n== progress ==")
        count = [0]
        def prog():
            count[0] += 1
            if count[0] == 1:
                print(f"  [prog] tid={threading.get_ident()} worker={THREAD_ID[0]}")
            return 0
        await db.set_progress_handler(prog, 1)
        cur = await db.execute("SELECT count(*) FROM t")
        print("progress query ->", await cur.fetchall(), "calls", count[0])
        await db.set_progress_handler(None, 0)
        print("after clear ->", (await (await db.execute("SELECT 1")).fetchall()))

        print("\n== progress raises ==")
        def prog_raise():
            print(f"  [prog-raise] tid={threading.get_ident()}")
            raise RuntimeError("boom-progress")
        await db.set_progress_handler(prog_raise, 1)
        try:
            cur = await db.execute("SELECT count(*) FROM t")
            print("query ->", await cur.fetchall())
        except Exception as e:
            print("query -> EXC", type(e).__name__, repr(str(e)))
        await db.set_progress_handler(None, 0)
        cur = await db.execute("SELECT count(*) FROM t")
        print("after clear ->", await cur.fetchall())

        print("\n== trace ==")
        traces = []
        def trace(sql):
            traces.append(sql)
            print(f"  [trace] tid={threading.get_ident()} worker={THREAD_ID[0]} sql={sql!r}")
        await db.set_trace_callback(trace)
        await (await db.execute("SELECT 1")).fetchall()
        await db.set_trace_callback(None)
        print("traces", traces)

        print("\n== trace raises ==")
        def trace_raise(sql):
            print(f"  [trace-raise] tid={threading.get_ident()} sql={sql!r}")
            raise RuntimeError("boom-trace")
        await db.set_trace_callback(trace_raise)
        try:
            cur = await db.execute("SELECT 1")
            print("query ->", await cur.fetchall())
        except Exception as e:
            print("query -> EXC", type(e).__name__, repr(str(e)))
        await db.set_trace_callback(None)
        print("after clear ->", await (await db.execute("SELECT 1")).fetchall())

asyncio.run(main())
