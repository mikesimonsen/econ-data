"""Postgres connection helper.

Single import point for reader modules so they don't each duplicate the
load_dotenv + os.environ + psycopg.connect boilerplate. After Step 4 cutover
this becomes the canonical connect path used by store.py and all readers.

Returns a lazy module-level singleton so that hot paths (e.g. analyze_series
called per-series in a loop) don't pay the ~50-200ms TLS handshake to Neon
on every call. The connection is opened on first use and reused for the
process lifetime; psycopg cleans it up at interpreter shutdown.

Callers must NOT call con.close() — that would close the singleton for
everyone. Writers must call con.commit() explicitly (autocommit is off by
default, which is the right choice for batched inserts in save()).
"""
import os
from typing import Optional

import psycopg
from dotenv import load_dotenv

load_dotenv()

_conn: Optional[psycopg.Connection] = None


def connect() -> psycopg.Connection:
    """Return the process-wide Postgres connection (lazy singleton).

    autocommit=True so SELECTs don't keep an idle transaction open between
    calls — Neon kills idle-in-transaction connections after ~5 min, which
    bites when run.py launches the replicator as a subprocess in between
    other DB calls. For batch writes that need atomicity, wrap them in
    `with conn.transaction(): ...`.
    """
    global _conn
    if _conn is None or _conn.closed:
        _conn = psycopg.connect(os.environ["DATABASE_URL"], autocommit=True)
    return _conn


_last_checked = 0.0


def connect_checked(max_idle: float = 60.0) -> psycopg.Connection:
    """connect() for callers that spend minutes on network I/O between
    queries (scrapers). Neon drops idle connections, and a dropped socket
    still reports closed=False — even a probe query would hang until the OS
    TCP timeout. So after `max_idle` seconds without a checked call, replace
    the connection outright instead of probing it."""
    global _conn, _last_checked
    import time
    now = time.monotonic()
    if _conn is not None and now - _last_checked > max_idle:
        try:
            _conn.close()
        except Exception:
            pass
        _conn = None
    _last_checked = now
    return connect()
