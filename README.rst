aiosqlite\: Sqlite for AsyncIO
==============================

.. image:: https://readthedocs.org/projects/aiosqlite/badge/?version=latest
   :target: https://aiosqlite.omnilib.dev/en/latest/?badge=latest
   :alt: Documentation Status
.. image:: https://img.shields.io/pypi/v/aiosqlite.svg
   :target: https://pypi.org/project/aiosqlite
   :alt: PyPI Release
.. image:: https://img.shields.io/badge/change-log-blue
   :target: https://github.com/omnilib/aiosqlite/blob/master/CHANGELOG.md
   :alt: Changelog
.. image:: https://img.shields.io/pypi/l/aiosqlite.svg
   :target: https://github.com/omnilib/aiosqlite/blob/master/LICENSE
   :alt: MIT Licensed

aiosqlite provides a friendly, async interface to sqlite databases.

It replicates the standard ``sqlite3`` module, but with async versions
of all the standard connection and cursor methods, plus context managers for
automatically closing connections and cursors:

.. code-block:: python

    async with aiosqlite.connect(...) as db:
        await db.execute("INSERT INTO some_table ...")
        await db.commit()

        async with db.execute("SELECT * FROM some_table") as cursor:
            async for row in cursor:
                ...

It can also be used in the traditional, procedural manner:

.. code-block:: python

    db = await aiosqlite.connect(...)
    cursor = await db.execute('SELECT * FROM some_table')
    row = await cursor.fetchone()
    rows = await cursor.fetchall()
    await cursor.close()
    await db.close()

aiosqlite also replicates most of the advanced features of ``sqlite3``:

.. code-block:: python

    async with aiosqlite.connect(...) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM some_table') as cursor:
            async for row in cursor:
                value = row['column']

        await db.execute('INSERT INTO foo some_table')
        assert db.total_changes > 0


Install
-------

aiosqlite is compatible with Python 3.8 and newer.
You can install it from PyPI:

.. code-block:: console

    $ pip install aiosqlite


Details
-------

aiosqlite allows interaction with SQLite databases on the main AsyncIO event
loop without blocking execution of other coroutines while waiting for queries
or data fetches.  It does this by using a single, shared thread per connection.
This thread executes all actions within a shared request queue to prevent
overlapping actions.

Connection objects are proxies to the real connections, contain the shared
execution thread, and provide context managers to handle automatically closing
connections.  Cursors are similarly proxies to the real cursors, and provide
async iterators to query results.


Cancellation
------------

Plain ``await db.execute(...)`` keeps the traditional AsyncIO semantics: cancelling
the await raises ``CancelledError`` but does not tell the worker thread anything,
so the SQL may still be queued or still running.  Use the explicit cancellable
helpers when the caller needs to know what happened to the database:

.. code-block:: python

    from aiosqlite import CancellationPolicy, OperationCancelledError

    try:
        async with db.execute_cancellable(
            "INSERT INTO some_table ...",
            policy=CancellationPolicy.INTERRUPT,
        ):
            ...
    except OperationCancelledError as exc:
        print(exc.info.phase)  # queued, running, interrupted, or completed

``policy=CancellationPolicy.NEVER`` (the default) never interrupts the worker:
queued operations are dropped before they run, and an operation that already
started runs to completion.  ``CancellationPolicy.INTERRUPT`` additionally
interrupts the *one* operation the caller owns when it is already executing.
The interrupt is correlated with that operation's queue state and execution
phase, and is withheld once that operation has finished, so it cannot spill
over into the next caller's statement.

``exc.info.phase`` describes the database side effects:

- ``queued``: the operation never ran; there are no side effects.
- ``running``: cancellation was requested under ``NEVER`` while the statement
  was executing; it ran to completion and its effects stand.
- ``interrupted``: the statement was aborted.  SQLite rolls back the current
  *statement* only; the surrounding connection transaction, earlier statements
  in that transaction, and previously committed writes are untouched.
- ``completed``: the operation finished before cancellation won the race; its
  effects stand and only the result is discarded.

Cancellation never automatically rolls back the whole connection transaction.
If a cancelled write must be undone, decide that explicitly with
``await db.rollback()`` (or ``commit`` the work that should be kept).


License
-------

aiosqlite is copyright `Amethyst Reese <https://noswap.com>`_, and licensed under the
MIT license.  I am providing code in this repository to you under an open source
license.  This is my personal repository; the license you receive to my code
is from me and not from my employer. See the `LICENSE`_ file for details.

.. _LICENSE: https://github.com/omnilib/aiosqlite/blob/master/LICENSE
