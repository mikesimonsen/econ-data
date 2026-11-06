"""One pipeline run at a time, via a lease row in Postgres.

GitHub's crons fire hours late at unpredictable times, so without this a cron
run and a page-button refresh can land together and collide when rendering and
pushing docs/index.html. A crashed run's lease simply expires.
"""
from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Callable, Optional

from econ_data.db import connect, connect_checked

LEASE_NAME = "pipeline"
LEASE_SECONDS = 20 * 60  # comfortably longer than a full run (about 7 minutes)
POLL_SECONDS = 10


def try_acquire(holder: str, lease_seconds: int = LEASE_SECONDS) -> bool:
    cur = connect().execute(
        """
        INSERT INTO pipeline_lease (name, holder, expires_at)
        VALUES (%s, %s, NOW() + %s * INTERVAL '1 second')
        ON CONFLICT (name) DO UPDATE SET
            holder = EXCLUDED.holder, expires_at = EXCLUDED.expires_at
        WHERE pipeline_lease.expires_at < NOW()
        RETURNING holder
        """,
        (LEASE_NAME, holder, lease_seconds),
    )
    return cur.fetchone() is not None


def acquire(holder: str, wait_seconds: int = 12 * 60,
            on_wait: Optional[Callable[[], None]] = None) -> bool:
    """Wait for the lease. Returns False if it's still held after wait_seconds."""
    deadline = time.monotonic() + wait_seconds
    while True:
        if try_acquire(holder):
            return True
        if time.monotonic() >= deadline:
            return False
        if on_wait:
            on_wait()
        time.sleep(POLL_SECONDS)


def release(holder: str) -> None:
    """Only the holder can release. After a long run the connection may have
    gone idle, hence connect_checked."""
    connect_checked().execute(
        "DELETE FROM pipeline_lease WHERE name = %s AND holder = %s",
        (LEASE_NAME, holder),
    )


@contextmanager
def scheduled_run_lock(holder: str, log: Callable[[str], None] = print):
    """For cron entrypoints. Waits its turn, but a lease problem must never stop
    scheduled data collection, so it runs anyway if the wait or the lease fails.
    """
    held = False
    try:
        held = acquire(holder)
        if not held:
            log(f"Lease still held after waiting; {holder} continuing anyway.")
    except Exception as e:
        log(f"Lease unavailable ({e}); {holder} continuing anyway.")
    try:
        yield
    finally:
        if held:
            try:
                release(holder)
            except Exception as e:
                log(f"Could not release lease ({e}); it will expire.")
