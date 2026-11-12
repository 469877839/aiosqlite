import sqlite3, sys, io, contextlib
sqlite3.enable_callback_tracebacks(False)
conn = sqlite3.connect(":memory:")
conn.execute("CREATE TABLE t(x)")
conn.create_function("f_raise", 1, lambda x: 1/0)
conn.create_function("f_obj", 1, lambda x: object())
conn.create_function("f_asyncish", 1, lambda x: (lambda: None))

print("UDF raise, default tracebacks:")
try:
    conn.execute("SELECT f_raise(1)").fetchall()
except Exception as e:
    print("  surfaced:", type(e).__name__, str(e))

print("UDF bad return, default tracebacks:")
try:
    conn.execute("SELECT f_obj(1)").fetchall()
except Exception as e:
    print("  surfaced:", type(e).__name__, str(e))
print("done")
