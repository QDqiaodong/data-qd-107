"""SQLite-backed state for exactly one export-task capture.

One state file manages one export.  Before the first network request the
three binding parameters (endpoint, request_id, output) are persisted
atomically together with the initial phase; later runs must present the same
bindings or fail before any network access.  An existing database with an
unknown structure is never overwritten -- a missing file may be initialized,
nothing else may.

Phases distinguish every local outcome the workflow can reach:

``unbound``            bindings saved, no creation result observed yet
``create_submitted``   POST was sent; creation receipt is unknown locally
``bound``              job_id accepted from a 201 or a by-request lookup
``waiting``            last portal answer was pending
``remote_failed``      portal reported an explicit job failure
``frozen``             a ready manifest (one snapshot) was accepted
``validation_failed``  all four files captured, business validation rejected
``published``          the output directory was delivered and re-verified

The stored download checkpoints (raw bytes plus completion flag in one
transaction) survive crashes and are independent of the private staging
directory, which is always disposable.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import sqlite3
import tempfile

from .errors import StateError

PHASE_PREPARED = "prepared"
PHASE_CREATE_SUBMITTED = "create_submitted"
PHASE_BOUND = "bound"
PHASE_WAITING = "waiting"
PHASE_REMOTE_FAILED = "remote_failed"
PHASE_FROZEN = "frozen"
PHASE_VALIDATION_FAILED = "validation_failed"
PHASE_PUBLISHED = "published"

PHASES = (
    PHASE_PREPARED,
    PHASE_CREATE_SUBMITTED,
    PHASE_BOUND,
    PHASE_WAITING,
    PHASE_REMOTE_FAILED,
    PHASE_FROZEN,
    PHASE_VALIDATION_FAILED,
    PHASE_PUBLISHED,
)

#: Allowed forward/backward transitions.  Identity never changes once bound;
#: a frozen snapshot can never go back to a network-facing phase.
_TRANSITIONS = {
    PHASE_PREPARED: {PHASE_CREATE_SUBMITTED, PHASE_BOUND},
    PHASE_CREATE_SUBMITTED: {PHASE_BOUND},
    PHASE_BOUND: {PHASE_WAITING, PHASE_REMOTE_FAILED, PHASE_FROZEN},
    PHASE_WAITING: {PHASE_WAITING, PHASE_REMOTE_FAILED, PHASE_FROZEN},
    PHASE_REMOTE_FAILED: set(),
    PHASE_FROZEN: {PHASE_VALIDATION_FAILED, PHASE_PUBLISHED},
    PHASE_VALIDATION_FAILED: {PHASE_PUBLISHED},
    PHASE_PUBLISHED: set(),
}

_BINDING_KEYS = ("endpoint", "request_id", "output")
_META_KEYS = _BINDING_KEYS + (
    "phase", "job_id", "snapshot_id", "ready_response_sha256",
    "remote_error", "validation_error",
)

_SCHEMA = """
CREATE TABLE capture_meta(
 meta_key TEXT PRIMARY KEY CHECK(meta_key IN (
  'endpoint','request_id','output','phase','job_id','snapshot_id',
  'ready_response_sha256','remote_error','validation_error')),
 meta_value TEXT NOT NULL);

CREATE TABLE capture_http(
 exchange_id INTEGER PRIMARY KEY AUTOINCREMENT,
 request_method TEXT NOT NULL,
 request_url TEXT NOT NULL,
 request_body BLOB,
 request_source TEXT NOT NULL,
 response_status INTEGER,
 response_body BLOB,
 response_sha256 TEXT,
 note TEXT);

CREATE TABLE capture_job(
 job_id TEXT PRIMARY KEY,
 request_id TEXT NOT NULL,
 snapshot_id TEXT NOT NULL,
 ready_raw BLOB NOT NULL,
 ready_sha256 TEXT NOT NULL);

CREATE TABLE capture_file(
 role TEXT PRIMARY KEY,
 url TEXT NOT NULL,
 declared_sha256 TEXT NOT NULL,
 declared_length INTEGER NOT NULL,
 completed INTEGER NOT NULL CHECK(completed IN (0,1)),
 actual_sha256 TEXT,
 actual_length INTEGER,
 raw_bytes BLOB);
"""

_EXPECTED_TABLES = {"capture_meta", "capture_http", "capture_job", "capture_file"}

_EXPECTED_COLUMNS = {
    "capture_meta": {"meta_key", "meta_value"},
    "capture_http": {
        "exchange_id", "request_method", "request_url", "request_body",
        "request_source", "response_status", "response_body",
        "response_sha256", "note",
    },
    "capture_job": {
        "job_id", "request_id", "snapshot_id", "ready_raw", "ready_sha256",
    },
    "capture_file": {
        "role", "url", "declared_sha256", "declared_length", "completed",
        "actual_sha256", "actual_length", "raw_bytes",
    },
}


def reject_path_symlinks(path: Path, what: str) -> None:
    """Reject a path that is itself a symlink or traverses one."""
    path = Path(path)
    if path.is_symlink():
        raise StateError(f"{what} must not be a symlink: {path}")
    current = Path(path.anchor)
    for component in path.parts[1:]:
        current = current / component
        if current.is_symlink():
            raise StateError(
                f"{what} must not traverse a symlink component: {current}"
            )


def _check_structure(db: sqlite3.Connection, path: Path) -> None:
    tables = {
        row[0]
        for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    # sqlite_sequence is created automatically by AUTOINCREMENT; anything
    # beyond the expected set plus that internal table is a foreign database.
    allowed = _EXPECTED_TABLES | {"sqlite_sequence"}
    if not _EXPECTED_TABLES <= tables or tables - allowed:
        raise StateError(
            f"state database {path} has an unknown structure; refusing to "
            f"reuse or overwrite it (tables={sorted(tables)!r})"
        )
    for table in _EXPECTED_TABLES:
        columns = {row[1] for row in db.execute(f"PRAGMA table_info({table})")}
        if columns != _EXPECTED_COLUMNS[table]:
            raise StateError(
                f"state database {path}: table {table!r} has columns "
                f"{sorted(columns)!r}, expected {sorted(_EXPECTED_COLUMNS[table])!r}"
            )


def _connect(path: Path) -> sqlite3.Connection:
    db = sqlite3.connect(str(path))
    db.execute("PRAGMA foreign_keys=ON")
    return db


def initialize(path: Path, endpoint: str, request_id: str, output: str) -> None:
    """Create a brand-new state database atomically; never overwrite one."""
    path = Path(path)
    reject_path_symlinks(path, "state file")
    if path.exists():
        raise StateError(
            f"state database already exists: {path}; initialize only handles a "
            "missing file"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    reject_path_symlinks(path.parent, "state directory")
    fd, tmp_name = tempfile.mkstemp(
        prefix=".capture-state-", suffix=".tmp", dir=path.parent
    )
    os.close(fd)
    tmp = Path(tmp_name)
    try:
        with _connect(tmp) as db:
            db.executescript(_SCHEMA)
            db.executemany(
                "INSERT INTO capture_meta(meta_key, meta_value) VALUES (?,?)",
                [
                    ("endpoint", endpoint),
                    ("request_id", request_id),
                    ("output", output),
                    ("phase", PHASE_PREPARED),
                ],
            )
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def open_state(path: Path, for_status: bool = False):
    """Open an existing, structurally recognized state database."""
    path = Path(path)
    if not path.exists():
        raise StateError(f"state database not found: {path}")
    reject_path_symlinks(path, "state file")
    if for_status:
        # mode=ro opens strictly read-only: a missing file errors instead of
        # being created, and no write transaction can ever be started.
        uri = path.absolute().as_uri() + "?mode=ro"
        db = sqlite3.connect(uri, uri=True)
    else:
        db = _connect(path)
    db.execute("PRAGMA foreign_keys=ON")
    _check_structure(db, path)
    return db


def bindings(db: sqlite3.Connection) -> dict:
    rows = dict(db.execute("SELECT meta_key, meta_value FROM capture_meta"))
    return {key: rows.get(key) for key in _BINDING_KEYS}


def phase(db: sqlite3.Connection) -> str:
    value = db.execute(
        "SELECT meta_value FROM capture_meta WHERE meta_key='phase'"
    ).fetchone()
    return value[0] if value else PHASE_PREPARED


def require_bindings_match(
    db: sqlite3.Connection, endpoint: str, request_id: str, output: str
) -> None:
    """Binding changes are rejected before any network access."""
    current = bindings(db)
    expected = {"endpoint": endpoint, "request_id": request_id, "output": output}
    for key in _BINDING_KEYS:
        if current.get(key) != expected[key]:
            raise StateError(
                f"binding {key!r} changed since the state file was created: "
                f"stored={current.get(key)!r} requested={expected[key]!r}; "
                "a state file manages exactly one export"
            )


def set_phase(db: sqlite3.Connection, new_phase: str) -> None:
    current = phase(db)
    if new_phase not in PHASES:
        raise StateError(f"unknown phase {new_phase!r}")
    if new_phase != current and new_phase not in _TRANSITIONS[current]:
        raise StateError(
            f"illegal phase transition {current!r} -> {new_phase!r}"
        )
    db.execute(
        "UPDATE capture_meta SET meta_value=? WHERE meta_key='phase'",
        (new_phase,),
    )


def set_meta(db: sqlite3.Connection, key: str, value: str | None) -> None:
    if key not in _META_KEYS or key in _BINDING_KEYS or key == "phase":
        raise StateError(f"meta key {key!r} is not writable via set_meta")
    db.execute("DELETE FROM capture_meta WHERE meta_key=?", (key,))
    if value is not None:
        db.execute(
            "INSERT INTO capture_meta(meta_key, meta_value) VALUES (?,?)",
            (key, value),
        )


def get_meta(db: sqlite3.Connection, key: str) -> str | None:
    row = db.execute(
        "SELECT meta_value FROM capture_meta WHERE meta_key=?", (key,)
    ).fetchone()
    return row[0] if row else None


def record_http(
    db: sqlite3.Connection,
    method: str,
    url: str,
    request_body: bytes | None,
    request_source: str,
    status: int | None,
    response_body: bytes | None,
    note: str | None = None,
) -> int:
    """Persist the actual wire exchange (never a fabricated receipt)."""
    digest = (
        hashlib.sha256(response_body).hexdigest()
        if response_body is not None
        else None
    )
    cur = db.execute(
        "INSERT INTO capture_http(request_method, request_url, request_body, "
        "request_source, response_status, response_body, response_sha256, note) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (
            method, url,
            sqlite3.Binary(request_body) if request_body is not None else None,
            request_source, status,
            sqlite3.Binary(response_body) if response_body is not None else None,
            digest, note,
        ),
    )
    return cur.lastrowid


def http_exchanges(db: sqlite3.Connection) -> list[dict]:
    rows = db.execute(
        "SELECT exchange_id, request_method, request_url, request_source, "
        "response_status, response_sha256, note FROM capture_http "
        "ORDER BY exchange_id"
    ).fetchall()
    return [
        {
            "exchange_id": exchange_id,
            "method": method,
            "url": url,
            "request_source": source,
            "status": status,
            "response_sha256": digest,
            "note": note,
        }
        for exchange_id, method, url, source, status, digest, note in rows
    ]


def freeze_job(db: sqlite3.Connection, job: dict, ready_raw: bytes) -> None:
    """Bind the one accepted frozen manifest and its four role rows."""
    digest = hashlib.sha256(ready_raw).hexdigest()
    db.execute(
        "INSERT INTO capture_job(job_id, request_id, snapshot_id, ready_raw, "
        "ready_sha256) VALUES (?,?,?,?,?)",
        (
            job["job_id"], job["request_id"], job["snapshot_id"],
            sqlite3.Binary(ready_raw), digest,
        ),
    )
    db.executemany(
        "INSERT INTO capture_file(role, url, declared_sha256, declared_length, "
        "completed, actual_sha256, actual_length, raw_bytes) "
        "VALUES (?,?,?,?,0,NULL,NULL,NULL)",
        [
            (
                item["role"], item["url"], item["sha256"], item["byte_length"],
            )
            for item in job["files"]
        ],
    )
    set_meta(db, "snapshot_id", job["snapshot_id"])
    set_meta(db, "ready_response_sha256", digest)


def load_frozen(db: sqlite3.Connection) -> dict:
    """Reload the accepted frozen manifest purely from local state."""
    job_row = db.execute(
        "SELECT job_id, request_id, snapshot_id, ready_raw, ready_sha256 "
        "FROM capture_job"
    ).fetchone()
    if job_row is None:
        raise StateError("no frozen job manifest is bound in this state file")
    job_id, request_id, snapshot_id, ready_raw, ready_sha256 = job_row
    files = [
        {
            "role": role, "url": url, "sha256": declared_sha256,
            "byte_length": declared_length, "completed": bool(completed),
            "actual_sha256": actual_sha256, "actual_length": actual_length,
        }
        for role, url, declared_sha256, declared_length, completed,
        actual_sha256, actual_length in db.execute(
            "SELECT role, url, declared_sha256, declared_length, completed, "
            "actual_sha256, actual_length FROM capture_file ORDER BY role"
        )
    ]
    return {
        "job_id": job_id,
        "request_id": request_id,
        "snapshot_id": snapshot_id,
        "ready_raw": bytes(ready_raw),
        "ready_sha256": ready_sha256,
        "files": files,
    }


def role_row(db: sqlite3.Connection, role: str):
    return db.execute(
        "SELECT role, url, declared_sha256, declared_length, completed, "
        "actual_sha256, actual_length, raw_bytes FROM capture_file WHERE role=?",
        (role,),
    ).fetchone()


def save_role_blob(
    db: sqlite3.Connection, role: str, raw: bytes, actual_sha256: str,
    actual_length: int,
) -> None:
    """Mark a role complete; blob and flag land in the same transaction."""
    db.execute(
        "UPDATE capture_file SET completed=1, actual_sha256=?, actual_length=?, "
        "raw_bytes=? WHERE role=?",
        (actual_sha256, actual_length, sqlite3.Binary(raw), role),
    )
    if db.total_changes == 0:
        raise StateError(f"role {role!r} is not part of the frozen manifest")


def drop_role_checkpoint(db: sqlite3.Connection, role: str) -> None:
    """Remove a checkpoint whose stored bytes no longer verify."""
    db.execute(
        "UPDATE capture_file SET completed=0, actual_sha256=NULL, "
        "actual_length=NULL, raw_bytes=NULL WHERE role=?",
        (role,),
    )


def completed_roles(db: sqlite3.Connection) -> list[str]:
    return [
        role
        for (role,) in db.execute(
            "SELECT role FROM capture_file WHERE completed=1 ORDER BY role"
        )
    ]


def status_snapshot(db: sqlite3.Connection) -> dict:
    """Read-only projection used by the ``status`` command and tests."""
    current_phase = phase(db)
    result = {
        "phase": current_phase,
        "endpoint": get_meta(db, "endpoint"),
        "request_id": get_meta(db, "request_id"),
        "output": get_meta(db, "output"),
        "job_id": get_meta(db, "job_id"),
        "snapshot_id": get_meta(db, "snapshot_id"),
        "remote_error": get_meta(db, "remote_error"),
        "validation_error": get_meta(db, "validation_error"),
        "ready_response_sha256": get_meta(db, "ready_response_sha256"),
        "files": [],
        "http_exchanges": http_exchanges(db),
    }
    if current_phase in (
        PHASE_FROZEN, PHASE_VALIDATION_FAILED, PHASE_PUBLISHED
    ):
        result["files"] = [
            {
                "role": role,
                "url": url,
                "sha256": declared,
                "byte_length": length,
                "completed": bool(completed),
                "actual_sha256": actual,
                "actual_length": actual_length,
            }
            for role, url, declared, length, completed, actual, actual_length in db.execute(
                "SELECT role, url, declared_sha256, declared_length, completed, "
                "actual_sha256, actual_length FROM capture_file ORDER BY role"
            )
        ]
    return result
