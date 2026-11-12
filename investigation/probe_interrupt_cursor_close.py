import asyncio, time
import aiosqlite

async def main():
    db = await aiosqlite.connect(":memory:")
    await db.create_function("slow", 1, lambda x: (time.sleep(0.6), x)[1])
    cur = asyncio.create_task(db.execute("SELECT slow(1)"))
    await asyncio.sleep(0.15)
    await db.interrupt()
    outer = await cur
    await outer.close()   # finalize the interrupted statement without fetching
    try:
        print("after outer.close(), next query ->", await (await db.execute("SELECT 1")).fetchall(), flush=True)
    except Exception as e:
        print("next query ->", type(e).__name__, e, flush=True)
    await db.close()

asyncio.run(main())
