import asyncio, time
import aiosqlite

async def main():
    db = await aiosqlite.connect(":memory:")
    progress = [0]
    def prog():
        progress[0] += 1
        return 0
    await db.set_progress_handler(prog, 1)
    def slow(x):
        time.sleep(0.3)
        return x
    await db.create_function("slow", 1, slow)
    task = asyncio.create_task(db.execute("SELECT slow(42)"))
    await asyncio.sleep(0.1)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        print("CancelledError in coroutine; worker still completes the call")
    await asyncio.sleep(0.5)
    print("next query works:", await (await db.execute("SELECT 1")).fetchall())
    await db.close()

asyncio.run(main())
