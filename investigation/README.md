# aiosqlite 回调边界复现脚本

环境：aiosqlite 0.22.1（本仓库源码），CPython 3.13.15，SQLite 3.53.1。
运行：`timeout 10 .venv/bin/python investigation/<script>.py`，除死锁脚本外均秒级结束。

## 脚本与结论映射

| 脚本 | 证明的结论 |
| --- | --- |
| `probe_sqlite3_callbacks.py` | 裸 sqlite3 三类回调的协议差异：UDF 异常/非法返回→OperationalError；progress 异常被忽略并按 interrupted 终止；trace 异常完全被忽略、查询照常 |
| `probe_aiosqlite_callbacks.py` | 三类回调全部在 worker 线程执行、无运行中的事件循环；future 回传后异常在 `await` 处抛出（无 `__cause__`）；async 函数不能当 UDF |
| `probe_visibility.py` | 默认不开 `enable_callback_tracebacks` 时，回调原始异常在 stderr 完全不可见，只剩通用 OperationalError |
| `probe_reentrancy.py` | UDF/progress 内可同线程同步重入 SQL（读、写、嵌套 UDF 均成功）；trace 内重入 SQL 形成递归反馈环 |
| `probe_trace_recursion.py` | 受控证明 trace 重入：嵌套语句的 SQL 文本带 `'-- '` 前缀；无防护重入打到递归上限但查询仍完成 |
| `probe_sync_reentry.py` | worker 线程内原始连接可同步重入；协程线程直碰 `db._conn` 触发 ProgrammingError |
| `probe_queued_reentry.py` | 回调内 `run_coroutine_threadsafe(...).result(timeout=1)` 1 秒超时（队列里正卡着内层 tx），回调返回后内层查询立刻被执行，全程不永久挂死 |
| `deadlock_asyncio_run.py` | 回调内 `asyncio.run(db.execute(...))` 必然死锁：2 秒栈转储 + 4 秒看门狗 `os._exit(3)`，退出码 3 |
| `probe_replace_and_types.py` | 回调更换/清除 API 行为；progress 任意真值返回都终止查询；authorizer 是第四种协议（DatabaseError not authorized）；3.13 上 `create_function(name,n,None)` 不注销函数 |
| `probe_registration_order.py` | 注册/更换/清除是排队操作，不能影响在途查询，只对后续语句生效 |
| `probe_cancel_mid_callback.py` | 协程取消只放弃 future，worker 上的 SQL/回调照常跑完，连接继续可用 |
| `probe_future_lifecycle.py` | future 回传两端：任务取消安全（done 检查吞掉结果）；事件循环提前关闭则 worker 线程因二次 call_soon_threadsafe 失败而死亡 |
| `probe_interrupt_sticky.py` | `interrupt()` 不能抢占 Python 回调；`execute()` 只做首次 step，中断标志要等原语句再次 step/finalize 才清除，期间污染所有后续语句 |
| `probe_interrupt_cursor_close.py` | 关闭（finalize）被中断的 cursor 即可清除粘性中断标志 |
| `probe_authorizer.py` | authorizer 在 prepare 期于 worker 线程触发，异常/非法返回统一变 DatabaseError |

## 源码定位（aiosqlite 0.22.1）

- 注册全部入队到 worker：`aiosqlite/core.py:262`（create_function）、`:322`（progress）、`:327`（trace）、`:330`（authorizer）
- worker 执行与 future 回传：`aiosqlite/core.py:48-75`（异常经 `call_soon_threadsafe(set_exception)` 回传）
- 入队与等待：`aiosqlite/core.py:150-160`
- 唯一不经队列的旁路：`aiosqlite/core.py:258-260`（`interrupt()` 直接调原始连接）
