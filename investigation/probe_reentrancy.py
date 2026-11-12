import sqlite3, threading, sys

sqlite3.enable_callback_tracebacks(True)
conn = sqlite3.connect(":memory:")
conn.execute("CREATE TABLE t(x)")
conn.execute("INSERT INTO t VALUES (1),(2),(3)")
conn.commit()

print("== reentrant SQL inside UDF (read) ==")
def udf_nested_read(x):
    cur = conn.execute("SELECT sum(x) FROM t WHERE x <= ?", (x,))
    return cur.fetchone()[0]
conn.create_function("nested_read", 1, udf_nested_read)
print("SELECT nested_read(2) ->", conn.execute("SELECT nested_read(2)").fetchall())

print("== reentrant SQL inside UDF calling another UDF ==")
conn.create_function("add1", 1, lambda x: x + 1)
conn.create_function("nested_udf", 1, lambda x: conn.execute("SELECT add1(?)", (x,)).fetchone()[0])
print("SELECT nested_udf(41) ->", conn.execute("SELECT nested_udf(41)").fetchall())

print("== reentrant write inside UDF ==")
def udf_nested_write(x):
    conn.execute("INSERT INTO t VALUES (?)", (x,))
    return x * 10
conn.create_function("nested_write", 1, udf_nested_write)
try:
    print("SELECT nested_write(9) ->", conn.execute("SELECT nested_write(9)").fetchall())
except Exception as e:
    print("EXC", type(e).__name__, repr(str(e)))
conn.rollback()

print("== reentrant SQL inside progress handler ==")
calls = {"n": 0}
def prog_nested():
    calls["n"] += 1
    if calls["n"] == 3:
        try:
            print("  progress nested select ->", conn.execute("SELECT x FROM t LIMIT 1").fetchall())
        except Exception as e:
            print("  EXC", type(e).__name__, repr(str(e)))
    return 0
conn.set_progress_handler(prog_nested, 1)
print("query result ->", conn.execute("SELECT count(*) FROM t").fetchall(), "calls", calls["n"])
conn.set_progress_handler(None, 0)

print("== reentrant SQL inside trace callback ==")
def trace_nested(sql):
    try:
        conn.execute("SELECT 1").fetchall()
        print("  trace nested select ok for", sql)
    except Exception as e:
        print("  EXC for", sql, type(e).__name__, repr(str(e)))
conn.set_trace_callback(trace_nested)
try:
    conn.execute("SELECT count(*) FROM t").fetchall()
except Exception as e:
    print("outer EXC", type(e).__name__, repr(str(e)))
conn.set_trace_callback(None)

print("== replace/clear from inside callbacks ==")
def prog_replacing():
    conn.set_progress_handler(None, 0)
    print("  progress handler cleared itself")
    return 0
conn.set_progress_handler(prog_replacing, 1)
print("query ->", conn.execute("SELECT count(*) FROM t").fetchall())
print("followup ->", conn.execute("SELECT 1").fetchall())

def udf_replacing(x):
    conn.set_trace_callback(lambda sql: print("  new trace saw", sql))
    return x
conn.create_function("rep_trace", 1, udf_replacing)
conn.set_trace_callback(lambda sql: print("  old trace saw", sql))
print("SELECT rep_trace(5) ->", conn.execute("SELECT rep_trace(5)").fetchall())
print("next statement:")
conn.execute("SELECT 123").fetchall()
conn.set_trace_callback(None)
