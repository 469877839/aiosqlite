import sqlite3, threading, sys
sqlite3.enable_callback_tracebacks(False)
conn = sqlite3.connect(":memory:")
local = threading.local()
seen = []

def trace(sql):
    seen.append(sql)
    if getattr(local, "inside", False):
        return  # guard: do not re-enter, just observe the nested trace
    local.inside = True
    try:
        rows = conn.execute("SELECT 1").fetchall()
        print(f"  guarded nested SQL ok, trace text for nested stmt: {seen[-1]!r}, rows={rows}")
    finally:
        local.inside = False

conn.set_trace_callback(trace)
print("outer ->", conn.execute("SELECT 1").fetchall())
print("trace texts:", seen)

# unguarded: nested statement re-traces itself; let it hit a low recursion limit
seen.clear()
sys.setrecursionlimit(120)
def trace_unguarded(sql):
    seen.append(sql)
    conn.execute("SELECT 2").fetchall()
conn.set_trace_callback(trace_unguarded)
try:
    print("unguarded outer ->", conn.execute("SELECT 1").fetchall())
except Exception as e:
    print("unguarded EXC", type(e).__name__, str(e))
print("trace invocations before stop:", len(seen), "sample:", seen[:2])
conn.set_trace_callback(None)
print("after clear ->", conn.execute("SELECT 3").fetchall())
