"""Original loopback demo portal -- a *protocol fixture*, not a service.

This module is an in-process, in-memory HTTP portal that speaks exactly the
export protocol exercised by the capture client.  It exists solely so the
capture workflow and its tests can run against a real HTTP server on
loopback without any account, external host or customer system:

* it only binds ``127.0.0.1`` and serves synthetic bytes supplied by the
  caller (the repository's own v2 fixtures by default);
* every job lives in memory; nothing here is production storage;
* fault injection (dropped 201 responses, HTTP 500s, delays, short reads,
  digest mismatches, shuffled or malformed manifests) is intentionally
  available so recovery paths are testable deterministically.

Run it standalone for a manual demo::

    python -m contract_warehouse.capture.portal --port 8080 \
        --source ../../fixtures_inherited

The job starts ``pending``; a loopback-only control request
``POST /__demo__/ready?request_id=...`` flips it to ``ready`` so the same
capture command can be used to demonstrate resumption.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit, parse_qs, unquote

from .protocol import (
    MAX_CSV_BYTES,
    MAX_JSON_BYTES,
    MODE_INHERITED,
    ROLES,
    SUPPORTED_SCHEMA_VERSION,
)

CSV_FILES = {
    "customers": "customers.csv",
    "hierarchy": "hierarchy.csv",
    "contracts": "contracts.csv",
    "tickets": "tickets.csv",
}


def _strict_json(raw: bytes):
    def reject(pairs):
        seen = set()
        result = {}
        for key, value in pairs:
            if key in seen:
                raise ValueError(f"duplicate JSON key {key!r}")
            seen.add(key)
            result[key] = value
        return result

    return json.loads(raw.decode("utf-8"), object_pairs_hook=reject)


class DemoPortal:
    """Configurable in-memory portal state, shared with its HTTP handler."""

    def __init__(self, files: dict[str, bytes] | None = None):
        self._lock = threading.Lock()
        self.jobs: dict[str, dict] = {}          # request_id -> job record
        self.request_log: list[dict] = []
        self.create_count = 0
        # Fault injection knobs (all default to well-behaved).
        self.drop_create_response: set[str] = set()
        self.before_create_response = None       # callable(request_id) or None
        self.by_request_status: dict[str, int] = {}
        self.by_request_delay: dict[str, float] = {}
        self.by_request_identity_mismatch: set[str] = set()
        # Optional response-body rewriter for by-request lookups, keyed by
        # request_id: callable(request_id, job) -> replacement dict.
        self.by_request_rewrite = None
        self.job_status_override: dict[str, int] = {}
        self.file_faults: dict[str, dict] = {}
        self.manifest_shuffle: set[str] = set()
        self.manifest_mutator = None             # callable(request_id, files)
        self.auto_ready_after: float | None = None
        self._files = files or {}

    # ---------------------------------------------------------------
    # Portal-side job management
    # ---------------------------------------------------------------

    def load_files_from(self, source_dir) -> None:
        root = Path(source_dir)
        self._files = {
            role: (root / name).read_bytes() for role, name in CSV_FILES.items()
        }

    def _manifest(self, request_id: str, job_id: str) -> list[dict]:
        files = []
        for role in ROLES:
            raw = self._files[role]
            files.append(
                {
                    "role": role,
                    "url": f"/exports/files/{job_id}/{role}.csv",
                    "sha256": hashlib.sha256(raw).hexdigest(),
                    "byte_length": len(raw),
                }
            )
        if request_id in self.manifest_shuffle:
            files = list(reversed(files))
        if self.manifest_mutator is not None:
            files = self.manifest_mutator(request_id, files)
        return files

    def create(self, request_id: str) -> dict:
        """Idempotent creation: one request_id maps to exactly one job."""
        with self._lock:
            existing = self.jobs.get(request_id)
            if existing is not None:
                return existing
            self.create_count += 1
            # The internal job_id stays ASCII and path-safe; the caller's
            # request_id (which may contain slashes or non-ASCII characters)
            # is kept separately and verbatim in every identity response.
            digest = hashlib.sha256(request_id.encode("utf-8")).hexdigest()[:16]
            job_id = f"job-{self.create_count:04d}-{digest}"
            job = {
                "request_id": request_id,
                "job_id": job_id,
                "state": "pending",
                "snapshot_id": None,
                "error": None,
                "files": [],
            }
            self.jobs[request_id] = job
            if self.auto_ready_after is not None:
                delay = self.auto_ready_after

                def flip():
                    time.sleep(delay)
                    self.mark_ready(request_id)

                threading.Thread(target=flip, daemon=True).start()
            return job

    def mark_ready(self, request_id: str) -> None:
        with self._lock:
            job = self.jobs[request_id]
            job["state"] = "ready"
            job["snapshot_id"] = f"snap-{job['job_id']}"
            job["files"] = self._manifest(request_id, job["job_id"])

    def mark_failed(self, request_id: str, error: str) -> None:
        with self._lock:
            job = self.jobs[request_id]
            job["state"] = "failed"
            job["error"] = error

    def job_by_request(self, request_id: str):
        with self._lock:
            return self.jobs.get(request_id)

    def job_by_id(self, job_id: str):
        with self._lock:
            for job in self.jobs.values():
                if job["job_id"] == job_id:
                    return job
        return None

    def log(self, method: str, target: str, status: int) -> None:
        with self._lock:
            self.request_log.append(
                {"method": method, "target": target, "status": status}
            )

    def requests_to(self, substring: str) -> list[dict]:
        with self._lock:
            return [
                entry
                for entry in self.request_log
                if substring in entry["target"]
            ]

    def file_bytes_for(self, role: str) -> bytes:
        fault = self.file_faults.get(role, {})
        raw = self._files[role]
        if fault.get("truncate"):
            raw = raw[: max(0, len(raw) - fault["truncate"])]
        if fault.get("corrupt_tail"):
            raw = raw[:-1] + (b"X" if raw else b"X")
        if fault.get("replace") is not None:
            raw = fault["replace"]
        return raw

    def declared_length_for(self, role: str, actual: bytes) -> int:
        fault = self.file_faults.get(role, {})
        if fault.get("declare_full_length"):
            return len(self._files[role])
        return len(actual)


def make_server(portal: DemoPortal, host: str = "127.0.0.1", port: int = 0):
    handler = _build_handler(portal)

    class _ReusableServer(ThreadingHTTPServer):
        # Ephemeral ports are reused frequently in the test suite; avoid
        # transient "address already in use" races between server instances.
        allow_reuse_address = True
        daemon_threads = True

    server = _ReusableServer((host, port), handler)
    return server


def _build_handler(portal: DemoPortal):
    class PortalHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):  # quiet fixture server
            pass

        def handle(self):
            # Fault-injection tests deliberately drop connections and let
            # clients time out mid-response; the resulting reset/broken pipe
            # on the fixture side is expected and must not print tracebacks.
            try:
                super().handle()
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                pass

        # -----------------------------------------------------------
        # Helpers
        # -----------------------------------------------------------

        def _send_json(self, status: int, payload) -> None:
            body = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Encoding", "identity")
            self.end_headers()
            self.wfile.write(body)

        def _drop_connection(self) -> None:
            # Simulate a response lost after the server accepted the request.
            self.close_connection = True
            sock = self.connection
            try:
                sock.shutdown(1)  # SHUT_WR: no response will ever arrive
            except OSError:
                pass
            sock.close()

        def _read_json_body(self):
            length = int(self.headers.get("Content-Length", "0"))
            if length > MAX_JSON_BYTES:
                self._send_json(413, {"error": "body too large"})
                return None
            encoding = self.headers.get("Content-Encoding")
            if encoding is not None and encoding.lower() != "identity":
                self._send_json(415, {"error": "only identity encoding"})
                return None
            raw = self.rfile.read(length)
            if len(raw) != length:
                self._send_json(400, {"error": "truncated request body"})
                return None
            try:
                return _strict_json(raw)
            except (ValueError, UnicodeDecodeError):
                self._send_json(400, {"error": "malformed strict JSON"})
                return None

        # -----------------------------------------------------------
        # Routing
        # -----------------------------------------------------------

        def do_GET(self):
            parts = urlsplit(self.path)
            target = parts.path
            if target.startswith("/exports/by-request/"):
                return self._handle_by_request(target, parts.query)
            if target.startswith("/exports/files/"):
                return self._handle_file(target)
            if target.startswith("/exports/"):
                return self._handle_job(target)
            portal.log("GET", self.path, 404)
            self._send_json(404, {"error": "unknown path"})

        def do_POST(self):
            parts = urlsplit(self.path)
            if parts.path == "/exports":
                return self._handle_create()
            if parts.path == "/__demo__/ready":
                query = parse_qs(parts.query)
                request_id = query.get("request_id", [""])[0]
                if portal.job_by_request(request_id) is None:
                    portal.log("POST", self.path, 404)
                    self._send_json(404, {"error": "unknown request_id"})
                    return
                portal.mark_ready(request_id)
                portal.log("POST", self.path, 200)
                self._send_json(200, {"ok": True})
                return
            portal.log("POST", self.path, 404)
            self._send_json(404, {"error": "unknown path"})

        # -----------------------------------------------------------
        # Protocol endpoints
        # -----------------------------------------------------------

        def _handle_by_request(self, target, query):
            request_id = unquote(target.removeprefix("/exports/by-request/"))
            # Fault overrides are sticky for the whole test scenario: a
            # resuming client must observe the same injected condition until
            # the test explicitly clears it.
            forced = portal.by_request_status.get(request_id)
            delay = portal.by_request_delay.get(request_id)
            if delay is not None:
                time.sleep(delay)
            if forced is not None:
                portal.log("GET", self.path, forced)
                if forced == 0:  # 0 means "drop the connection"
                    return self._drop_connection()
                self._send_json(forced, {"error": f"injected status {forced}"})
                return
            job = portal.job_by_request(request_id)
            if job is None:
                portal.log("GET", self.path, 404)
                self._send_json(404, {"error": "no export for request_id"})
                return
            payload = {
                "schema_version": SUPPORTED_SCHEMA_VERSION,
                "request_id": (
                    "WRONG-ID"
                    if request_id in portal.by_request_identity_mismatch
                    else job["request_id"]
                ),
                "job_id": job["job_id"],
            }
            if portal.by_request_rewrite is not None:
                payload = portal.by_request_rewrite(request_id, job, payload)
            portal.log("GET", self.path, 200)
            self._send_json(200, payload)

        def _handle_create(self):
            payload = self._read_json_body()
            if payload is None:
                portal.log("POST", self.path, 400)
                return
            if set(payload) != {"schema_version", "request_id", "mode"}:
                self._send_json(400, {"error": "unexpected creation fields"})
                return
            if type(payload.get("schema_version")) is not int \
                    or payload["schema_version"] != SUPPORTED_SCHEMA_VERSION \
                    or type(payload["schema_version"]) is bool:
                self._send_json(400, {"error": "unsupported schema_version"})
                return
            if payload.get("mode") != MODE_INHERITED:
                self._send_json(400, {"error": "unsupported mode"})
                return
            request_id = payload.get("request_id")
            if not isinstance(request_id, str) or not request_id \
                    or request_id != request_id.strip():
                self._send_json(400, {"error": "bad request_id"})
                return
            job = portal.create(request_id)
            if portal.before_create_response is not None:
                portal.before_create_response(request_id)
            if request_id in portal.drop_create_response:
                portal.drop_create_response.discard(request_id)
                portal.log("POST", self.path, None)
                return self._drop_connection()
            body = {
                "schema_version": SUPPORTED_SCHEMA_VERSION,
                "request_id": job["request_id"],
                "job_id": job["job_id"],
            }
            portal.log("POST", self.path, 201)
            self._send_json(201, body)

        def _handle_job(self, target):
            job_id = unquote(target.removeprefix("/exports/"))
            forced = portal.job_status_override.pop(job_id, None)
            if forced is not None:
                portal.log("GET", self.path, forced)
                self._send_json(forced, {"error": f"injected status {forced}"})
                return
            job = portal.job_by_id(job_id)
            if job is None:
                portal.log("GET", self.path, 404)
                self._send_json(404, {"error": "unknown job_id"})
                return
            portal.log("GET", self.path, 200)
            self._send_json(200, {"schema_version": SUPPORTED_SCHEMA_VERSION, **job})

        def _handle_file(self, target):
            # /exports/files/<job_id>/<role>.csv
            remainder = target.removeprefix("/exports/files/")
            try:
                _job_id, file_name = remainder.split("/", 1)
            except ValueError:
                portal.log("GET", self.path, 404)
                self._send_json(404, {"error": "bad file path"})
                return
            role = next(
                (candidate for candidate, name in CSV_FILES.items() if name == file_name),
                None,
            )
            if role is None:
                portal.log("GET", self.path, 404)
                self._send_json(404, {"error": "unknown file role"})
                return
            raw = portal.file_bytes_for(role)
            if len(raw) > MAX_CSV_BYTES:
                portal.log("GET", self.path, 413)
                self._send_json(413, {"error": "file too large"})
                return
            declared = portal.declared_length_for(role, raw)
            portal.log("GET", self.path, 200)
            self.send_response(200)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Length", str(declared))
            self.send_header("Content-Encoding", "identity")
            self.end_headers()
            self.wfile.write(raw)

    return PortalHandler


# ---------------------------------------------------------------------------
# Standalone manual demo
# ---------------------------------------------------------------------------


def serve(source_dir, host: str = "127.0.0.1", port: int = 8080) -> None:
    portal = DemoPortal()
    portal.load_files_from(source_dir)
    server = make_server(portal, host, port)
    actual_host, actual_port = server.server_address[:2]
    print(
        "Loopback demo portal (protocol fixture, synthetic data only) listening on "
        f"http://{actual_host}:{actual_port}\n"
        "New export jobs start pending; mark one ready with:\n"
        "  curl -X POST 'http://127.0.0.1:"
        f"{actual_port}/__demo__/ready?request_id=<ID>'\n"
        "Press Ctrl+C to stop."
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Loopback export portal demo (protocol fixture, not a service)"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument(
        "--source",
        default=str(Path(__file__).resolve().parents[3] / "fixtures_inherited"),
        help="directory with the four synthetic v2 CSV files",
    )
    args = parser.parse_args()
    if args.host not in ("127.0.0.1", "localhost", "::1"):
        raise SystemExit("the demo portal only binds loopback hosts")
    serve(args.source, args.host, args.port)


if __name__ == "__main__":
    main()
