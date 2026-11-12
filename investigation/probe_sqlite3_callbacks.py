import sqlite3, threading, sys, traceback

print("sqlite", sqlite3.sqlite_version, "threadsafety", sqlite3.threadsafety)
main = threading.get_ident()
sqlite3.enable_callback_tracebacks(True)

conn = sqlite3.connect(":memory:")

def record(tag, ret=None):
    def cb(*args):
        print(f"  [{tag}] thread={threading.get_ident()} main={main} args={args!r}")
        return ret
    return cb

conn.execute("CREATE TABLE t(x)")
conn.execute("INSERT INTO t VALUES (1),(2),(3)")

print("\n== UDF thread/return ==")
conn.create_function("f_id", 1, record("udf-id", 7))
conn.create_function("f_raise", 1, lambda x: (_ for _ in ()).throw(ValueError("boom-udf")))
conn.create_function("f_obj", 1, lambda x: object())

for sql in ["SELECT f_id(1)", "SELECT f_raise(1)", "SELECT f_obj(1)"]:
    try:
        print(sql, "->", conn.execute(sql).fetchall())
    except Exception as e:
        print(sql, "-> EXC", type(e).__name__, repr(str(e)))

print("\n== progress handler ==")
events = []
def prog_ok():
    events.append(threading.get_ident())
    return 0
def prog_raise():
    print(f"  [prog-raise] thread={threading.get_ident()}")
    raise RuntimeError("boom-progress")
def prog_abort():
    print(f"  [prog-abort] thread={threading.get_ident()}")
    return 1

conn.set_progress_handler(prog_ok, 10)
print("ok handler rows:", conn.execute("SELECT count(*) FROM t").fetchall(), "events", len(events))

conn.set_progress_handler(prog_raise, 10)
for sql in ["SELECT count(*) FROM t", "INSERT INTO t VALUES (99)"]:
    try:
        print(sql, "->", conn.execute(sql).fetchall())
    except Exception as e:
        print(sql, "-> EXC", type(e).__name__, repr(str(e)), "in_transaction", conn.in_transaction)

conn.set_progress_handler(prog_abort, 10)
try:
    print("abort ->", conn.execute("SELECT count(*) FROM t").fetchall())
except Exception as e:
    print("abort -> EXC", type(e).__name__, repr(str(e)))
conn.set_progress_handler(None, 0)
try:
    print("after clear ->", conn.execute("SELECT count(*) FROM t").fetchall())
except Exception as e:
    print("after clear -> EXC", type(e).__name__, repr(str(e)))

print("\n== trace callback ==")
traces = []
def trace_raise(sql):
    traces.append(sql)
    print(f"  [trace-raise] thread={threading.get_ident()} sql={sql!r}")
    raise RuntimeError("boom-trace")
conn.set_trace_callback(trace_raise)
for sql in ["SELECT 1", "INSERT INTO t VALUES (100)"]:
    try:
        print(sql, "->", conn.execute(sql).fetchall())
    except Exception as e:
        print(sql, "-> EXC", type(e).__name__, repr(str(e)))
conn.set_trace_callback(None)
print("after clear SELECT ->", conn.execute("SELECT 1").fetchall(), "traces", len(traces))
