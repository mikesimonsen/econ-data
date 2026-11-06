"""Econ data pipeline.

Runs in US Eastern time wherever it's hosted (Fly machines default to UTC), so
"today", release due-dates and page date stamps follow the ET release calendar
rather than rolling over at 7 PM ET. db.connect() sets the Postgres session to
match. The POSIX form needs no tzdata files in the slim Docker image.
"""
import os
import time

os.environ["TZ"] = "EST5EDT,M3.2.0,M11.1.0"
time.tzset()
