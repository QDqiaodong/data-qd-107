"""Resumable export-task capture workflow.

The engine turns the strict wire protocol (:mod:`protocol`) and the local
SQLite state (:mod:`state`) into one operator-driven, serially resumable
capture.  It never chooses a second request id, never mixes two snapshots,
never treats a half-finished staging directory as delivered, and never
re-opens a network source after bytes have been accepted -- digests are
computed on the received bytes only.

Crash windows are reachable through named, synchronous barrier hooks
(:func:`barrier`) instead of timing-based sleeps.  The barriers can be armed
with the ``CAPTURE_BARRIER`` environment variable for real subprocess tests
or monkeypatched for in-process tests.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile

from .errors import (
    BusinessValidationError,
    CaptureError,
    ProtocolError,
    StateError,
)
from . import protocol
from . import state as state_mod
from .state import (
    PHASE_BOUND,
    PHASE_CREATE_SUBMITTED,
    PHASE_FROZEN,
    PHASE_PUBLISHED,
    PHASE_REMOTE_FAILED,
    PHASE_PREPARED,
    PHASE_VALIDATION_FAILED,
    PHASE_WAITING,
)
from ..hierarchy import ValidationError, load_inherited, build_bridge

#: Fixed names of a delivered input directory consumed by inherited.build.
CSV_NAMES = {
    "customers": "customers.csv",
    "hierarchy": "hierarchy.csv",
    "contracts": "contracts.csv",
    "tickets": "tickets.csv",
}
CAPTURE_JSON = "capture.json"
DELIVERED_FILES = set(CSV_NAMES.values()) | {CAPTURE_JSON}

#: Barrier names (synchronous, deterministic test hooks).
BARRIER_CREATE_SENT = "create_sent"
BARRIER_ROLE_BEFORE_COMMIT = "role_before_commit"
BARRIER_ROLE_SAVED = "role_saved"
BARRIER_AFTER_RENAME = "after_rename"
BARRIER_BEFORE_PUBLISH_COMMIT = "before_publish_commit"


class BarrierExit(CaptureError):
    """Raised by an armed in-process barrier to simulate a hard termination."""


def barrier(name: str) -> None:
    """Synchronous test hook.

    Set ``CAPTURE_BARRIER`` to a comma-separated list of barrier names; when
    ``name`` matches, a subprocess terminates immediately with
    ``os._exit(3)`` (no ``finally`` cleanup, like SIGKILL at that exact
    instruction).  In-process tests may monkeypatch this function instead.
    """
    armed = {item.strip() for item in os.environ.get("CAPTURE_BARRIER", "").split(",") if item.strip()}
    if name in armed:
        os._exit(3)


# ---------------------------------------------------------------------------
# Local safety checks
# ---------------------------------------------------------------------------


def _check_state_outside_outputs(state_path: Path, output: Path) -> None:
    state_resolved = state_path.resolve(strict=False)
    output_resolved = output.resolve(strict=False)
    if state_resolved == output_resolved:
        raise StateError("state file must not be located inside the output directory")
    try:
        state_resolved.relative_to(output_resolved)
    except ValueError:
        pass
    else:
        raise StateError("state file must not be located inside the output directory")


def _check_output_path(output: Path) -> None:
    state_mod.reject_path_symlinks(output, "output directory")
    if output.exists() and not output.is_dir():
        raise StateError(f"output path exists but is not a directory: {output}")


# ---------------------------------------------------------------------------
# capture.json and directory verification
# ---------------------------------------------------------------------------


def _digest(raw: bytes) -> tuple[str, int]:
    return hashlib.sha256(raw).hexdigest(), len(raw)


def build_capture_payload(request_id: str, job_id: str, snapshot_id: str,
                          frozen: dict) -> dict:
    """Assemble the deterministic capture.json payload.

    The payload keeps the frozen manifest's original ready-response text and
    its SHA256, the declared per-role manifest, and each role's local source
    path with both the declared and the actually-stored digest/length.  It
    contains no wall clock, duration or machine path.
    """
    manifest = sorted(
        [
            {
                "role": item["role"],
                "url": item["url"],
                "sha256": item["sha256"],
                "byte_length": item["byte_length"],
            }
            for item in frozen["files"]
        ],
        key=lambda item: item["role"],
    )
    files = {}
    for item in manifest:
        role = item["role"]
        files[role] = {
            "source_path": CSV_NAMES[role],
            "declared_sha256": item["sha256"],
            "declared_byte_length": item["byte_length"],
            "actual_sha256": item.get("actual_sha256") or item["sha256"],
            "actual_byte_length": (
                item["actual_length"]
                if item.get("actual_length") is not None
                else item["byte_length"]
            ),
        }
    ready_raw = frozen.get("ready_raw")
    return {
        "schema_version": protocol.SUPPORTED_SCHEMA_VERSION,
        "mode": protocol.MODE_INHERITED,
        "request_id": request_id,
        "job_id": job_id,
        "snapshot_id": snapshot_id,
        "ready_response": {
            "sha256": frozen["ready_sha256"],
            "raw": bytes(ready_raw).decode("utf-8") if ready_raw is not None else None,
        },
        "manifest": manifest,
        "files": files,
        "interpretation": (
            "这些摘要只证明获取内容与门户冻结清单声明一致，"
            "不证明其中的客户关系或工单是真实客户事实。"
        ),
    }


def serialize_capture_json(payload: dict) -> bytes:
    return (
        json.dumps(payload, sort_keys=True, ensure_ascii=False, indent=2) + "\n"
    ).encode("utf-8")


def _verify_published_dir(output: Path, request_id: str, job_id: str,
                          snapshot_id: str, frozen: dict) -> None:
    """Full offline verification of a directory claiming to be this delivery.

    Any mismatch (missing file, extra file, changed bytes, foreign identity,
    symlink, non-deterministic capture.json) raises and leaves the directory
    untouched.
    """
    state_mod.reject_path_symlinks(output, "output directory")
    if not output.is_dir():
        raise StateError(f"published output directory is missing: {output}")
    entries = sorted(entry.name for entry in output.iterdir())
    if entries != sorted(DELIVERED_FILES):
        raise StateError(
            f"output directory file set {entries!r} does not match the "
            f"delivered set {sorted(DELIVERED_FILES)!r}; refusing to mark published"
        )
    expected_payload = build_capture_payload(
        request_id, job_id, snapshot_id, frozen
    )
    expected_capture = serialize_capture_json(expected_payload)
    actual_capture = (output / CAPTURE_JSON).read_bytes()
    if actual_capture != expected_capture:
        raise StateError(
            "capture.json does not match the deterministic content for this "
            "request; refusing to mark published"
        )
    # Re-parse the on-disk document with strict rules as an independent check.
    try:
        parsed = json.loads(actual_capture.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise StateError(f"capture.json is unreadable: {exc}") from exc
    if (
        parsed.get("request_id") != request_id
        or parsed.get("job_id") != job_id
        or parsed.get("snapshot_id") != snapshot_id
    ):
        raise StateError("capture.json identity does not belong to this request")
    by_role = {item["role"]: item for item in frozen["files"]}
    for role, name in sorted(CSV_NAMES.items()):
        path = output / name
        state_mod.reject_path_symlinks(path, f"output {name}")
        raw = path.read_bytes()
        digest, length = _digest(raw)
        item = by_role[role]
        if digest != item["sha256"] or length != item["byte_length"]:
            raise StateError(
                f"delivered {name} no longer matches the frozen manifest; "
                "refusing to mark published"
            )


# ---------------------------------------------------------------------------
# Network phases
# ---------------------------------------------------------------------------


def _bind_identity(db, response_raw: bytes, request_id: str, source: str) -> str:
    identity = protocol.parse_identity(response_raw, source)
    if identity["request_id"] != request_id:
        raise ProtocolError(
            f"{source}: portal request_id {identity['request_id']!r} does not "
            f"match local {request_id!r}"
        )
    existing = state_mod.get_meta(db, "job_id")
    if existing is not None and existing != identity["job_id"]:
        raise ProtocolError(
            f"portal job_id {identity['job_id']!r} differs from the already "
            f"accepted job_id {existing!r}; identity is immutable"
        )
    return identity["job_id"]


def _ensure_job_bound(db, endpoint: str, request_id: str) -> None:
    """by-request lookup, explicit 404 -> one creation, then bind job_id."""
    current = state_mod.phase(db)
    lookup_url = protocol.by_request_url(endpoint, request_id)
    try:
        status, body = protocol.request(
            endpoint, "GET", protocol.path_of(endpoint, lookup_url)
        )
    except ProtocolError:
        with db:
            state_mod.record_http(
                db, "GET", lookup_url, None, "by-request lookup", None, None,
                note="network error before any response",
            )
        raise
    if status == 404:
        with db:
            state_mod.record_http(
                db, "GET", lookup_url, None, "by-request lookup", status, body
            )
        if current != PHASE_PREPARED:
            # Only an unbound export may create; a lost receipt stays a lookup.
            raise ProtocolError(
                "by-request returned 404 although a creation was already "
                "submitted locally; not creating a second task"
            )
        post_url = protocol.create_url(endpoint)
        post_body = protocol.create_body(request_id)
        post_path = protocol.path_of(endpoint, post_url)
        with db:
            state_mod.set_phase(db, PHASE_CREATE_SUBMITTED)
            state_mod.record_http(
                db, "POST", post_url, post_body, "create (after explicit 404)",
                None, None, note="request prepared",
            )

        def _sent():
            barrier(BARRIER_CREATE_SENT)

        try:
            created_status, created_body = protocol.request(
                endpoint, "POST", post_path, body=post_body, on_sent=_sent
            )
        except ProtocolError as exc:
            # The creation may have happened server-side; the receipt is
            # unknown, so record the fact and let a later process rebind via
            # by-request. Never treat this as a 404 and never repost here.
            with db:
                state_mod.record_http(
                    db, "POST", post_url, post_body,
                    "create (after explicit 404)", None, None,
                    note=f"response lost: {exc}",
                )
            raise
        if created_status != 201:
            with db:
                state_mod.record_http(
                    db, "POST", post_url, post_body,
                    "create (after explicit 404)", created_status, created_body,
                    note="unexpected creation status",
                )
            raise ProtocolError(
                f"export creation expected 201, got {created_status}"
            )
        with db:
            job_id = _bind_identity(db, created_body, request_id, "create response")
            state_mod.record_http(
                db, "POST", post_url, post_body,
                "create (after explicit 404)", created_status, created_body
            )
            state_mod.set_meta(db, "job_id", job_id)
            state_mod.set_phase(db, PHASE_BOUND)
        return
    if status != 200:
        with db:
            state_mod.record_http(
                db, "GET", lookup_url, None, "by-request lookup", status, body,
                note="non-200/non-404 status",
            )
        raise ProtocolError(
            f"by-request lookup expected 200 or explicit 404, got {status}"
        )
    try:
        job_id = _bind_identity(db, body, request_id, "by-request lookup")
    except ProtocolError:
        with db:
            state_mod.record_http(
                db, "GET", lookup_url, None, "by-request lookup", status, body,
                note="identity mismatch",
            )
        raise
    with db:
        state_mod.record_http(
            db, "GET", lookup_url, None, "by-request lookup", status, body
        )
        if state_mod.get_meta(db, "job_id") is None:
            state_mod.set_meta(db, "job_id", job_id)
        state_mod.set_phase(db, PHASE_BOUND)


def _poll_job_once(db, endpoint: str, request_id: str) -> str:
    """At most one status query per run; returns the resulting phase."""
    job_id = state_mod.get_meta(db, "job_id")
    if not job_id:
        raise StateError("job_id is not bound but a status poll was requested")
    poll_url = protocol.job_url(endpoint, job_id)
    status_code, body = protocol.request(
        endpoint, "GET", protocol.path_of(endpoint, poll_url)
    )
    if status_code != 200:
        with db:
            state_mod.record_http(
                db, "GET", poll_url, None, "job status", status_code, body,
                note="unexpected job status code",
            )
        raise ProtocolError(f"job status expected 200, got {status_code}")
    job = protocol.parse_job(body)
    if job["request_id"] != request_id:
        raise ProtocolError(
            f"job status request_id {job['request_id']!r} does not match local "
            f"{request_id!r}"
        )
    if job["job_id"] != job_id:
        raise ProtocolError("job status returned a different, immutable job_id")
    with db:
        state_mod.record_http(db, "GET", poll_url, None, "job status", 200, body)
        if job["state"] == "pending":
            state_mod.set_phase(db, PHASE_WAITING)
            return PHASE_WAITING
        if job["state"] == "failed":
            state_mod.set_meta(db, "remote_error", job["error"])
            state_mod.set_phase(db, PHASE_REMOTE_FAILED)
            return PHASE_REMOTE_FAILED
        # ready: accept the one frozen manifest; later runs never poll again.
        state_mod.freeze_job(db, job, body)
        state_mod.set_phase(db, PHASE_FROZEN)
        return PHASE_FROZEN


# ---------------------------------------------------------------------------
# Download phase (resumable, one frozen manifest only)
# ---------------------------------------------------------------------------


def _download_role(db, endpoint: str, role: str, item: dict) -> None:
    url_path = item["url"]
    full_url = protocol.compose_same_origin_url(endpoint, url_path)
    status_code, raw = protocol.request(
        endpoint, "GET", url_path, limit=protocol.MAX_CSV_BYTES
    )
    if status_code != 200:
        with db:
            state_mod.record_http(
                db, "GET", full_url, None, f"download {role}", status_code, raw,
                note="unexpected download status",
            )
        raise ProtocolError(f"download of {role} expected 200, got {status_code}")
    digest, length = _digest(raw)
    if length != item["byte_length"]:
        with db:
            state_mod.record_http(
                db, "GET", full_url, None, f"download {role}", status_code, raw,
                note="short read / byte_length mismatch",
            )
        raise ProtocolError(
            f"download of {role}: received {length} bytes but frozen manifest "
            f"declares {item['byte_length']}"
        )
    if digest != item["sha256"]:
        with db:
            state_mod.record_http(
                db, "GET", full_url, None, f"download {role}", status_code, raw,
                note="sha256 mismatch",
            )
        raise ProtocolError(
            f"download of {role}: sha256 {digest} does not match frozen manifest"
        )

    # The HTTP exchange record, BLOB and completion flag commit as one
    # transaction.  The pre-commit barrier fires inside that transaction so a
    # hard termination rolls BLOB and flag back together; the post-commit
    # barrier fires only after the commit has returned so already-accepted
    # role checkpoints survive a later termination.
    with db:
        state_mod.record_http(
            db, "GET", full_url, None, f"download {role}", status_code, raw
        )
        state_mod.save_role_blob(db, role, raw, digest, length)
        barrier(BARRIER_ROLE_BEFORE_COMMIT)
    barrier(BARRIER_ROLE_SAVED)


def _complete_downloads(db, endpoint: str) -> dict:
    frozen = state_mod.load_frozen(db)
    for item in frozen["files"]:
        role = item["role"]
        row = state_mod.role_row(db, role)
        (
            _role, _url, declared_sha, declared_len, completed,
            actual_sha, actual_len, blob,
        ) = row
        if completed:
            # Never trust the flag alone: re-verify the stored bytes.
            stored = bytes(blob) if blob is not None else b""
            digest, length = _digest(stored)
            if (
                digest == declared_sha and length == declared_len
                and digest == actual_sha and length == actual_len
            ):
                continue  # genuine checkpoint hit: no HTTP request at all
            # Corrupted checkpoint: clear it and re-fetch the same frozen role.
            with db:
                state_mod.drop_role_checkpoint(db, role)
        _download_role(db, endpoint, role, item)
    return state_mod.load_frozen(db)


# ---------------------------------------------------------------------------
# Business validation and atomic delivery
# ---------------------------------------------------------------------------


def _business_validate(blobs: dict[str, bytes], scratch_dir: Path) -> None:
    """Run the existing full v2 rules; no second, looser parser exists here."""
    validation_dir = Path(
        tempfile.mkdtemp(prefix=".capture-validate-", dir=scratch_dir)
    )
    try:
        for role, raw in blobs.items():
            (validation_dir / CSV_NAMES[role]).write_bytes(raw)
        try:
            bundle = load_inherited(validation_dir)
            build_bridge(bundle.edges)
        except ValidationError as exc:
            raise BusinessValidationError(str(exc)) from exc
    finally:
        shutil.rmtree(validation_dir, ignore_errors=True)


def _publish(db, output: Path, frozen: dict, request_id: str, job_id: str,
             snapshot_id: str, scratch_dir: Path) -> None:
    payload = build_capture_payload(request_id, job_id, snapshot_id, frozen)
    capture_bytes = serialize_capture_json(payload)
    staging = Path(
        tempfile.mkdtemp(prefix=".capture-stage-", dir=scratch_dir)
    )
    try:
        blobs = {}
        for item in frozen["files"]:
            row = state_mod.role_row(db, item["role"])
            blobs[item["role"]] = bytes(row[7])
            with (staging / CSV_NAMES[item["role"]]).open("wb") as handle:
                handle.write(blobs[item["role"]])
        with (staging / CAPTURE_JSON).open("wb") as handle:
            handle.write(capture_bytes)
        # Re-verify every staged file after closing it.
        for item in frozen["files"]:
            raw = (staging / CSV_NAMES[item["role"]]).read_bytes()
            digest, length = _digest(raw)
            if digest != item["sha256"] or length != item["byte_length"]:
                raise StateError(
                    f"staged {item['role']} failed post-write verification"
                )
        if (staging / CAPTURE_JSON).read_bytes() != capture_bytes:
            raise StateError("staged capture.json failed post-write verification")
        if output.exists():
            raise StateError(
                f"output {output} appeared before publication; refusing to overwrite"
            )
        os.rename(staging, output)
        barrier(BARRIER_AFTER_RENAME)
        with db:
            barrier(BARRIER_BEFORE_PUBLISH_COMMIT)
            state_mod.set_phase(db, PHASE_PUBLISHED)
            state_mod.set_meta(db, "validation_error", None)
    except BaseException:
        # Only the private, unpublished staging is disposable. The SQLite
        # download checkpoints and any already-renamed directory stay intact.
        shutil.rmtree(staging, ignore_errors=True)
        raise


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def run(endpoint: str, request_id: str, state_path, output) -> dict:
    """Execute one resumable capture run and return a local receipt dict."""
    if not isinstance(request_id, str) or not request_id or request_id != request_id.strip():
        raise CaptureError(
            "request_id must be a non-empty string without leading/trailing whitespace"
        )
    endpoint = protocol.validate_endpoint(endpoint)
    state_path = Path(state_path)
    output = Path(output)
    _check_output_path(output)
    state_mod.reject_path_symlinks(state_path, "state file")
    _check_state_outside_outputs(state_path, output)

    if not state_path.exists():
        state_mod.initialize(state_path, endpoint, request_id, str(output))

    db = state_mod.open_state(state_path)
    scratch_dir = output.parent
    scratch_dir.mkdir(parents=True, exist_ok=True)
    stale_staging = [
        path
        for path in scratch_dir.glob(".capture-stage-*")
        if path.is_dir()
    ] + [
        path
        for path in scratch_dir.glob(".capture-validate-*")
        if path.is_dir()
    ]
    state_resolved = state_path.resolve(strict=False)
    for path in stale_staging:
        try:
            state_resolved.relative_to(path.resolve(strict=False))
        except ValueError:
            pass
        else:
            raise StateError(
                "state file must not be located inside a private staging directory"
            )
    for path in stale_staging:
        shutil.rmtree(path, ignore_errors=True)
    try:
        state_mod.require_bindings_match(db, endpoint, request_id, str(output))
        current = state_mod.phase(db)

        # A terminal remote failure is a recorded fact: reruns end explicitly
        # without creating a replacement task or issuing any HTTP request.
        if current == PHASE_REMOTE_FAILED:
            raise CaptureError(
                "portal reported export failure: "
                f"{state_mod.get_meta(db, 'remote_error')}"
            )

        # Offline-only phase: a directory previously renamed but whose
        # published marker was never committed gets reconciled with no HTTP.
        if current == PHASE_PUBLISHED:
            frozen = state_mod.load_frozen(db)
            _verify_published_dir(
                output, request_id, frozen["job_id"], frozen["snapshot_id"], frozen
            )
            return _receipt(db, current, note="offline verification only")
        if output.exists():
            # Any pre-existing directory outside the recognized published
            # recovery path belongs to someone else: never overwrite it.
            frozen = state_mod.load_frozen(db) if current in (
                PHASE_FROZEN, PHASE_VALIDATION_FAILED
            ) else None
            if frozen is not None:
                try:
                    _verify_published_dir(
                        output, request_id, frozen["job_id"],
                        frozen["snapshot_id"], frozen
                    )
                except StateError:
                    raise StateError(
                        f"output directory {output} already exists and is not a "
                        "complete delivery of this request; refusing to overwrite"
                    )
                with db:
                    state_mod.set_phase(db, PHASE_PUBLISHED)
                return _receipt(db, PHASE_PUBLISHED, note="recovered publication")
            raise StateError(
                f"output directory {output} already exists and does not belong "
                "to this request; refusing to overwrite"
            )

        if current in (PHASE_PREPARED, PHASE_CREATE_SUBMITTED):
            _ensure_job_bound(db, endpoint, request_id)
            current = state_mod.phase(db)

        if current in (PHASE_BOUND, PHASE_WAITING):
            current = _poll_job_once(db, endpoint, request_id)
            if current == PHASE_WAITING:
                return _receipt(db, current, note="portal job still pending")
            if current == PHASE_REMOTE_FAILED:
                raise CaptureError(
                    "portal reported export failure: "
                    f"{state_mod.get_meta(db, 'remote_error')}"
                )

        if current in (PHASE_FROZEN, PHASE_VALIDATION_FAILED):
            frozen = _complete_downloads(db, endpoint)
            blobs = {
                item["role"]: bytes(state_mod.role_row(db, item["role"])[7])
                for item in frozen["files"]
            }
            try:
                _business_validate(blobs, scratch_dir)
            except BusinessValidationError as exc:
                with db:
                    state_mod.set_meta(db, "validation_error", str(exc))
                    if state_mod.phase(db) == PHASE_FROZEN:
                        state_mod.set_phase(db, PHASE_VALIDATION_FAILED)
                raise
            _publish(
                db, output, frozen, request_id, frozen["job_id"],
                frozen["snapshot_id"], scratch_dir,
            )
            current = PHASE_PUBLISHED
        return _receipt(db, current)
    finally:
        db.close()


def _receipt(db, current_phase: str, note: str | None = None) -> dict:
    snapshot = state_mod.status_snapshot(db)
    receipt = {
        "phase": current_phase,
        "endpoint": snapshot["endpoint"],
        "request_id": snapshot["request_id"],
        "output": snapshot["output"],
        "job_id": snapshot["job_id"],
        "snapshot_id": snapshot["snapshot_id"],
        "completed_roles": [
            item["role"] for item in snapshot["files"] if item["completed"]
        ],
    }
    if note:
        receipt["note"] = note
    return receipt


def status(state_path) -> dict:
    """Read-only, offline projection; never creates the database or networked."""
    db = state_mod.open_state(Path(state_path), for_status=True)
    try:
        return state_mod.status_snapshot(db)
    finally:
        db.close()
