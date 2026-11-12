import asyncio, threading, sqlite3
import aiosqlite

async def main():
    db = await aiosqlite.connect(":memory:")
    await db.execute("CREATE TABLE t(x)")
    await db.execute("INSERT INTO t VALUES (1)")

    def auth_deny(*a):
        print(f"  [auth] tid={threading.get_ident()} worker={db._thread.ident}", flush=True)
        return sqlite3.SQLITE_DENY
    await db.set_authorizer(auth_deny)
    try:
        await (await db.execute("SELECT * FROM t")).fetchall()
    except Exception as e:
        print("DENY ->", type(e).__name__, repr(str(e)))

    def auth_raise(*a):
        raise ValueError("boom-auth")
    await db.set_authorizer(auth_raise)
    try:
        await (await db.execute("SELECT * FROM t")).fetchall()
    except Exception as e:
        print("raise ->", type(e).__name__, repr(str(e)))

    def auth_bad(*a):
        return "not-an-int"
    await db.set_authorizer(auth_bad)
    try:
        await (await db.execute("SELECT * FROM t")).fetchall()
    except Exception as e:
        print("bad return ->", type(e).__name__, repr(str(e)))

    await db.set_authorizer(None)
    print("after clear ->", await (await db.execute("SELECT count(*) FROM t")).fetchall())
    await db.close()

asyncio.run(main())
