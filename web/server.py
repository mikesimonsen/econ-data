"""Always-on service behind the page's refresh button (plans/manual-refresh.md).

The published page is static and public, so this holds what it can't: the key
check, the Fly token, and database access. It records a request and starts a
short-lived machine in econ-data-cron to do the work (run_refresh.py), then
reports progress. All data logic stays in that job.
"""
from __future__ import annotations

import hmac
import json
import os
import re
import time
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg

ALLOWED_ORIGIN = "https://mikesimonsen.github.io"
BUILD_URL = f"{ALLOWED_ORIGIN}/econ-data/build.json"
FLY_API = "https://api.machines.dev/v1/apps/econ-data-cron/machines"

STALL_SECONDS = 180        # no heartbeat for this long means the job is stuck
PUBLISH_SLOW_SECONDS = 900  # pushed this long ago, and Pages still isn't serving it
MAX_REQUESTS_PER_HOUR = 60
MAX_BODY_BYTES = 4096
SERIES_RE = re.compile(r"^[A-Za-z0-9_.\-]{1,64}$")
PATH_RE = re.compile(r"^/refresh/([0-9a-f-]{36})$")


def authorized(header: str | None, key: str | None) -> bool:
    if not header or not key:
        return False
    return hmac.compare_digest(header.encode(), f"Bearer {key}".encode())


def public_stage(stage: str, heartbeat_age: float, finished: bool) -> str:
    """What the panel should show, given how long the job has been silent."""
    if finished:
        return stage
    if stage == "pushed":
        return "publish_slow" if heartbeat_age > PUBLISH_SLOW_SECONDS else stage
    return "stalled" if heartbeat_age > STALL_SECONDS else stage


def machine_config(template: dict, request_id: str) -> dict:
    """A one-off copy of the cron machine. It drops the process-group metadata
    so cron.yml never picks it, and destroys itself on exit, so it can't touch
    (or be restarted by) a scheduled run."""
    config = json.loads(json.dumps(template))
    config["env"] = {**config.get("env", {}), "RUN_SCRIPT": "run_refresh.py",
                     "REFRESH_REQUEST_ID": request_id}
    config["auto_destroy"] = True
    config["restart"] = {"policy": "no"}
    config["metadata"] = {"refresh_request": request_id}
    return config


def _fly(method: str, url: str, body: dict | None = None) -> dict:
    token = os.environ["FLY_API_TOKEN"]
    auth = token if token.startswith("FlyV1 ") else f"Bearer {token}"
    request = urllib.request.Request(
        url, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": auth, "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def start_machine(request_id: str, fly=_fly) -> str:
    machines = fly("GET", FLY_API)
    templates = [m for m in machines
                 if m.get("config", {}).get("metadata", {}).get("fly_process_group") == "app"]
    if not templates:
        raise RuntimeError("no econ-data-cron machine to copy")
    template = sorted(templates, key=lambda m: m["id"])[0]
    created = fly("POST", FLY_API, {
        "region": template.get("region", "iad"),
        "config": machine_config(template["config"], request_id),
    })
    return created["id"]


def is_live(build_epoch: int | None) -> bool:
    """Is Pages serving this build, or a later one, yet?"""
    if build_epoch is None:
        return False
    try:
        url = f"{BUILD_URL}?t={int(time.time())}"
        with urllib.request.urlopen(url, timeout=10) as response:
            return int(json.load(response)["epoch"]) >= build_epoch
    except Exception:
        return False


def connect() -> psycopg.Connection:
    return psycopg.connect(os.environ["DATABASE_URL"], autocommit=True, connect_timeout=10)


def create_request(con: psycopg.Connection, series_id: str, start=start_machine) -> tuple[int, dict]:
    known = con.execute(
        "SELECT 1 FROM observations WHERE series_id = %s LIMIT 1", (series_id,)
    ).fetchone()
    if not known:
        return 404, {"error": f"Unknown series: {series_id}"}

    recent = con.execute(
        "SELECT COUNT(*) FROM refresh_requests WHERE created_at > NOW() - INTERVAL '1 hour'"
    ).fetchone()[0]
    if recent >= MAX_REQUESTS_PER_HOUR:
        return 429, {"error": "Too many refreshes in the last hour."}

    # Clear dead attempts for this series, so a stalled one can't block a retry.
    con.execute(
        """
        UPDATE refresh_requests SET finished_at = NOW(),
            stage = CASE WHEN stage = 'pushed' THEN 'done' ELSE 'failed' END,
            error = CASE WHEN stage = 'pushed' THEN error
                         ELSE COALESCE(error, 'The job stopped responding.') END
        WHERE series_id = %s AND finished_at IS NULL
          AND heartbeat_at < NOW() - make_interval(secs =>
              CASE WHEN stage = 'pushed' THEN %s ELSE %s END)
        """,
        (series_id, PUBLISH_SLOW_SECONDS, STALL_SECONDS),
    )

    request_id = str(uuid.uuid4())
    inserted = con.execute(
        "INSERT INTO refresh_requests (id, series_id, stage) VALUES (%s, %s, 'starting') "
        "ON CONFLICT (series_id) WHERE finished_at IS NULL DO NOTHING RETURNING id",
        (request_id, series_id),
    ).fetchone()
    if not inserted:  # a double-click, or already running: join that one
        active = con.execute(
            "SELECT id FROM refresh_requests WHERE series_id = %s AND finished_at IS NULL",
            (series_id,),
        ).fetchone()
        return 200, {"id": str(active[0]), "joined": True}

    try:
        machine_id = start(request_id)
        con.execute("UPDATE refresh_requests SET machine_id = %s, heartbeat_at = NOW() "
                    "WHERE id = %s", (machine_id, request_id))
    except Exception as e:
        detail = e.read().decode(errors="replace")[:200] if isinstance(e, urllib.error.HTTPError) else str(e)[:200]
        con.execute(
            "UPDATE refresh_requests SET stage = 'failed', finished_at = NOW(), error = %s "
            "WHERE id = %s",
            (f"Fly couldn't start the machine: {detail}", request_id),
        )
    return 202, {"id": request_id}


def get_status(con: psycopg.Connection, request_id: str, live=is_live) -> tuple[int, dict]:
    row = con.execute(
        """
        SELECT series_id, stage, verdict, error, build_epoch, finished_at IS NOT NULL,
               EXTRACT(EPOCH FROM NOW() - heartbeat_at)
        FROM refresh_requests WHERE id = %s
        """,
        (request_id,),
    ).fetchone()
    if not row:
        return 404, {"error": "Unknown request."}
    series_id, stage, verdict, error, build_epoch, finished, age = row

    if stage == "pushed" and not finished and live(build_epoch):
        con.execute("UPDATE refresh_requests SET stage = 'live', finished_at = NOW() "
                    "WHERE id = %s", (request_id,))
        stage, finished = "live", True

    return 200, {"id": request_id, "series_id": series_id, "verdict": verdict,
                 "stage": public_stage(stage, float(age), finished),
                 "error": error, "finished": finished}


class Handler(BaseHTTPRequestHandler):
    server_version = "econ-refresh"
    sys_version = ""

    def _send(self, status: int, body: dict | None = None) -> None:
        payload = json.dumps(body, default=str).encode() if body is not None else b""
        self.send_response(status)
        if self.headers.get("Origin") == ALLOWED_ORIGIN:
            self.send_header("Access-Control-Allow-Origin", ALLOWED_ORIGIN)
            self.send_header("Vary", "Origin")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _authorized(self) -> bool:
        if authorized(self.headers.get("Authorization"), os.environ.get("REFRESH_KEY")):
            return True
        self._send(401, {"error": "Invalid key."})
        return False

    def _guard(self, work) -> None:
        try:
            with connect() as con:
                self._send(*work(con))
        except Exception as e:
            self.log_error("request failed: %s", e)
            self._send(503, {"error": "The refresh service can't reach its database."})

    def do_OPTIONS(self):
        self.send_response(204)
        if self.headers.get("Origin") == ALLOWED_ORIGIN:
            self.send_header("Access-Control-Allow-Origin", ALLOWED_ORIGIN)
            self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Max-Age", "600")
            self.send_header("Vary", "Origin")
        self.end_headers()

    def do_GET(self):
        if self.path == "/health":
            return self._send(200, {"ok": True})
        if not self._authorized():
            return
        if self.path == "/auth":
            return self._send(200, {"ok": True})
        match = PATH_RE.match(self.path)
        if not match:
            return self._send(404, {"error": "Not found."})
        self._guard(lambda con: get_status(con, match.group(1)))

    def do_POST(self):
        if not self._authorized():
            return
        if self.path != "/refresh":
            return self._send(404, {"error": "Not found."})
        try:
            length = int(self.headers.get("Content-Length", 0))
            if length > MAX_BODY_BYTES:
                raise ValueError
            series_id = json.loads(self.rfile.read(length))["series_id"]
            if not isinstance(series_id, str) or not SERIES_RE.match(series_id):
                raise ValueError
        except (ValueError, KeyError, TypeError):
            return self._send(400, {"error": "Expected a valid series_id."})
        self._guard(lambda con: create_request(con, series_id))


def main() -> None:
    ThreadingHTTPServer(("0.0.0.0", int(os.environ.get("PORT", 8080))), Handler).serve_forever()


if __name__ == "__main__":
    main()
