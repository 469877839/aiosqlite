import sqlite3, asyncio
import aiosqlite

# --- sync type edge cases ---
conn = sqlite3.connect(":memory:")
conn.execute("CREATE TABLE t(x)")
conn.execute("INSERT INTO t VALUES (1),(2)")
sqlite3.enable_callback_tracebacks(True)

print("== progress returning non-integer ==")
def prog_str():
    return "not-an-int"
conn.set_progress_handler(prog_str, 1)
try:
    print("rows:", conn.execute("SELECT count(*) FROM t").fetchall())
except Exception as e:
    print("return str ->", type(e).__name__, str(e))
conn.set_progress_handler(None, 0)

def prog_two():
    return 2
conn.set_progress_handler(prog_two, 1)
try:
    conn.execute("SELECT count(*) FROM t").fetchall()
except Exception as e:
    print("return 2 ->", type(e).__name__, str(e))
conn.set_progress_handler(None, 0)

print("== authorizer (different protocol) ==")
def auth_deny(action, arg1, arg2, db, trig):
    return sqlite3.SQLITE_DENY
conn.set_authorizer(auth_deny)
try:
    conn.execute("SELECT * FROM t").fetchall()
except Exception as e:
    print("DENY ->", type(e).__name__, str(e))

def auth_raise(*a):
    raise RuntimeError("boom-auth")
conn.set_authorizer(auth_raise)
try:
    conn.execute("SELECT * FROM t").fetchall()
except Exception as e:
    print("raise ->", type(e).__name__, str(e))

def auth_bad(*a):
    return "weird"
conn.set_authorizer(auth_bad)
try:
    conn.execute("SELECT * FROM t").fetchall()
except Exception as e:
    print("bad return ->", type(e).__name__, str(e))
conn.set_authorizer(None)
print("after clear ->", conn.execute("SELECT count(*) FROM t").fetchall())

# --- aiosqlite replacement/clearing via public API ---
async def main():
    db = await aiosqlite.connect(":memory:")
    await db.execute("CREATE TABLE t(x)")
    await db.execute("INSERT INTO t VALUES (1),(2),(3)")

    log = []
    await db.create_function("f", 1, lambda x: x + 1)
    print("\naiosqlite UDF v1:", await (await db.execute("SELECT f(10)")).fetchall())
    await db.create_function("f", 1, lambda x: x + 100)
    print("aiosqlite UDF v2:", await (await db.execute("SELECT f(10)")).fetchall())
    await db.create_function("f", 1, None)
    try:
        await (await db.execute("SELECT f(10)")).fetchall()
    except Exception as e:
        print("after UDF clear ->", type(e).__name__, str(e))

    await db.set_trace_callback(lambda sql: log.append(("a", sql)))
    await (await db.execute("SELECT 1")).fetchall()
    await db.set_trace_callback(lambda sql: log.append(("b", sql)))
    await (await db.execute("SELECT 2")).fetchall()
    await db.set_trace_callback(None)
    await (await db.execute("SELECT 3")).fetchall()
    print("trace log:", log)

    calls = {"a": 0, "b": 0}
    await db.set_progress_handler(lambda: (calls.__setitem__("a", calls["a"]+1), 0)[1], 1)
    await (await db.execute("SELECT count(*) FROM t")).fetchall()
    await db.set_progress_handler(lambda: (calls.__setitem__("b", calls["b"]+1), 0)[1], 1)
    await (await db.execute("SELECT count(*) FROM t")).fetchall()
    await db.set_progress_handler(None, 0)
    await (await db.execute("SELECT count(*) FROM t")).fetchall()
    print("progress calls:", calls)
    await db.close()

asyncio.run(main())
