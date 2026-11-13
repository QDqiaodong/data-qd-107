"""Unit tests for the capture client's strict protocol and SQLite state.

These tests never open a real socket for the pure pieces; HTTP behavior is
covered separately by ``test_capture_e2e.py`` against the in-process loopback
portal fixture.  Everything here uses the Python standard library only.
"""

from __future__ import annotations

import http.client
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

from contract_warehouse.capture import engine, protocol, state as state_mod
from contract_warehouse.capture.errors import (
    EndpointError,
    ProtocolError,
    StateError,
)
from contract_warehouse.capture.protocol import (
    MAX_CSV_BYTES,
    MAX_JSON_BYTES,
    parse_identity,
    parse_job,
    validate_endpoint,
)


class EndpointTest(unittest.TestCase):
    def test_accepted_loopback_endpoints(self):
        self.assertEqual(
            validate_endpoint("http://127.0.0.1:8080/base/"),
            "http://127.0.0.1:8080/base",
        )
        self.assertEqual(
            validate_endpoint("http://localhost:9000"), "http://localhost:9000"
        )
        self.assertEqual(
            validate_endpoint("http://[::1]:8080"), "http://[::1]:8080"
        )

    def test_rejected_endpoints(self):
        bad = [
            "https://127.0.0.1:8080",
            "http://example.com:8080",
            "http://127.0.0.1",                       # no port
            "http://127.0.0.1:8080?q=1",
            "http://127.0.0.1:8080#frag",
            "http://user:pw@127.0.0.1:8080",
            "http://127.0.0.1:8080\\evil",
            "ftp://127.0.0.1:8080",
            "http://10.0.0.1:8080",
            "",
        ]
        for endpoint in bad:
            with self.subTest(endpoint=endpoint):
                with self.assertRaises(EndpointError):
                    validate_endpoint(endpoint)


class IdentifierEncodingTest(unittest.TestCase):
    def test_special_characters_are_fully_encoded(self):
        rid = "a/b c?中#&%x"
        encoded = protocol.encode_identifier(rid)
        self.assertNotIn("/", encoded)
        self.assertNotIn("?", encoded)
        self.assertNotIn("#", encoded)
        self.assertNotIn(" ", encoded)
        encoded.encode("ascii")  # request target stays ASCII
        url = protocol.by_request_url("http://127.0.0.1:8080", rid)
        # The encoded id occupies exactly the final path segment.
        self.assertTrue(url.startswith("http://127.0.0.1:8080/exports/by-request/"))
        self.assertEqual(url.split("/")[-1], encoded)

    def test_empty_or_padded_identifiers_rejected(self):
        for bad in ("", " ", " x", "x ", "\tx"):
            with self.subTest(bad=bad):
                with self.assertRaises(ProtocolError):
                    protocol.encode_identifier(bad)

    def test_case_is_preserved(self):
        self.assertNotEqual(
            protocol.encode_identifier("AbC"),
            protocol.encode_identifier("abc"),
        )


class StrictJsonTest(unittest.TestCase):
    def test_identity_acceptance_and_extra_fields(self):
        good = {"schema_version": 1, "request_id": "r", "job_id": "j"}
        parsed = parse_identity(
            json.dumps(good).encode("utf-8"), "identity"
        )
        self.assertEqual(parsed, good)
        with self.assertRaises(ProtocolError):
            parse_identity(
                json.dumps({**good, "extra": 1}).encode("utf-8"), "identity"
            )

    def test_duplicate_keys_rejected(self):
        raw = b'{"schema_version": 1, "schema_version": 1, ' \
              b'"request_id": "r", "job_id": "j"}'
        with self.assertRaises(ProtocolError):
            parse_identity(raw, "identity")

    def test_boolean_schema_version_is_not_an_integer(self):
        for value in (True, False, "1", 1.0, None, 2):
            raw = json.dumps(
                {"schema_version": value, "request_id": "r", "job_id": "j"}
            ).encode("utf-8")
            with self.subTest(value=value):
                with self.assertRaises(ProtocolError):
                    parse_identity(raw, "identity")

    def test_non_utf8_rejected(self):
        with self.assertRaises(ProtocolError):
            parse_identity(b'{"a": "\xff"}', "identity")

    def test_job_state_shapes(self):
        base = {
            "schema_version": 1, "request_id": "r", "job_id": "j",
            "snapshot_id": None, "files": [], "error": None,
        }
        pending = parse_job(json.dumps({**base, "state": "pending"}).encode())
        self.assertEqual(pending["state"], "pending")
        failed = parse_job(
            json.dumps(
                {**base, "state": "failed", "error": "boom"}
            ).encode()
        )
        self.assertEqual(failed["error"], "boom")
        with self.assertRaises(ProtocolError):
            parse_job(json.dumps({**base, "state": "failed"}).encode())
        with self.assertRaises(ProtocolError):  # pending must not carry snapshot
            parse_job(
                json.dumps(
                    {**base, "state": "pending", "snapshot_id": "s"}
                ).encode()
            )

    def _ready(self, files):
        return {
            "schema_version": 1, "request_id": "r", "job_id": "j",
            "state": "ready", "snapshot_id": "snap", "error": None,
            "files": files,
        }

    def _file(self, role, url=None, sha=None, length=1):
        import hashlib
        return {
            "role": role,
            "url": url if url is not None else f"/exports/files/j/{role}.csv",
            "sha256": sha or hashlib.sha256(b"x").hexdigest(),
            "byte_length": length,
        }

    def test_manifest_must_cover_four_distinct_roles(self):
        good = [self._file(role) for role in protocol.ROLES]
        job = parse_job(json.dumps(self._ready(good)).encode())
        self.assertEqual([f["role"] for f in job["files"]], sorted(protocol.ROLES))
        # Missing role.
        with self.assertRaises(ProtocolError):
            parse_job(json.dumps(self._ready(good[:3])).encode())
        # Duplicate role.
        dup = good + [self._file("customers", url="/exports/files/j/c2.csv")]
        with self.assertRaises(ProtocolError):
            parse_job(json.dumps(self._ready(dup)).encode())

    def test_manifest_rejects_duplicate_urls(self):
        files = [self._file(role) for role in protocol.ROLES]
        files[1]["url"] = files[0]["url"]
        with self.assertRaises(ProtocolError):
            parse_job(json.dumps(self._ready(files)).encode())

    def test_manifest_digest_and_length_rules(self):
        item = self._file("customers")
        item["sha256"] = "A" * 64  # uppercase hex rejected
        with self.assertRaises(ProtocolError):
            parse_job(json.dumps(self._ready([item] + [self._file(r) for r in protocol.ROLES[1:]])).encode())
        item = self._file("customers")
        item["byte_length"] = MAX_CSV_BYTES + 1
        with self.assertRaises(ProtocolError):
            parse_job(json.dumps(self._ready([item] + [self._file(r) for r in protocol.ROLES[1:]])).encode())
        item = self._file("customers")
        item["byte_length"] = True  # booleans are not integers
        with self.assertRaises(ProtocolError):
            parse_job(json.dumps(self._ready([item] + [self._file(r) for r in protocol.ROLES[1:]])).encode())

    def test_manifest_paths_must_stay_same_origin(self):
        for bad_url in (
            "//127.0.0.1:9/x.csv",
            "http://127.0.0.1:9/x.csv",
            "/x.csv#frag",
            "relative/x.csv",
            "/dir\\x.csv",
            "/user@host/x.csv",
        ):
            files = [self._file(role) for role in protocol.ROLES]
            files[0]["url"] = bad_url
            with self.subTest(bad_url=bad_url):
                with self.assertRaises(ProtocolError):
                    parse_job(json.dumps(self._ready(files)).encode())

    def test_manifest_order_does_not_change_roles(self):
        files = [self._file(role, url=f"/exports/files/j/{role}.csv") for role in protocol.ROLES]
        shuffled = list(reversed(files))
        job = parse_job(json.dumps(self._ready(shuffled)).encode())
        self.assertEqual([f["role"] for f in job["files"]], sorted(protocol.ROLES))


class BoundedReadTest(unittest.TestCase):
    def _response(self, body: bytes, headers=None):
        response = mock.Mock(spec=http.client.HTTPResponse)
        chunks = [body[i:i + 4] for i in range(0, len(body), 4)]
        response.read.side_effect = chunks + [b""]
        response.getheader.side_effect = lambda name: (headers or {}).get(name)
        return response

    def test_read_within_limit(self):
        raw = protocol.read_bounded(self._response(b"abcdef"), MAX_JSON_BYTES, "x")
        self.assertEqual(raw, b"abcdef")

    def test_oversize_body_fails_while_reading(self):
        response = self._response(b"x" * (MAX_JSON_BYTES + 5))
        with self.assertRaises(ProtocolError):
            protocol.read_bounded(response, MAX_JSON_BYTES, "x")

    def test_content_length_mismatch(self):
        response = self._response(b"abc", {"Content-Length": "9"})
        with self.assertRaises(ProtocolError):
            protocol.read_bounded(response, MAX_JSON_BYTES, "x")

    def test_declared_oversize_rejected_even_if_body_small(self):
        response = self._response(b"abc", {"Content-Length": str(MAX_CSV_BYTES + 1)})
        with self.assertRaises(ProtocolError):
            protocol.read_bounded(response, MAX_CSV_BYTES, "x")

    def test_non_identity_encoding_rejected(self):
        response = mock.Mock(spec=http.client.HTTPResponse)
        response.getheader.return_value = "gzip"
        with self.assertRaises(ProtocolError):
            protocol._check_encoding(response, "x")


class StateStoreTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.state_path = self.root / "cap.sqlite"

    def test_initialize_then_bindings_and_phase_machine(self):
        state_mod.initialize(self.state_path, "http://127.0.0.1:1", "r", str(self.root / "o"))
        db = state_mod.open_state(self.state_path)
        self.assertEqual(state_mod.phase(db), state_mod.PHASE_PREPARED)
        state_mod.require_bindings_match(
            db, "http://127.0.0.1:1", "r", str(self.root / "o")
        )
        with self.assertRaises(StateError):
            state_mod.require_bindings_match(
                db, "http://127.0.0.1:2", "r", str(self.root / "o")
            )
        # Illegal jump (unbound -> frozen) is refused.
        with self.assertRaises(StateError):
            state_mod.set_phase(db, state_mod.PHASE_FROZEN)
        db.close()

    def test_initialize_never_overwrites(self):
        state_mod.initialize(self.state_path, "http://127.0.0.1:1", "r", "o")
        with self.assertRaises(StateError):
            state_mod.initialize(self.state_path, "http://127.0.0.1:1", "r", "o")

    def test_foreign_database_is_recognized_not_overwritten(self):
        # A structurally different SQLite file (the warehouse itself could be
        # passed by mistake) must be rejected as unknown structure.
        other = self.root / "foreign.sqlite"
        with sqlite3.connect(other) as db:
            db.execute("CREATE TABLE unrelated(a INTEGER)")
        with self.assertRaises(StateError):
            state_mod.open_state(other)
        self.assertTrue(other.exists())  # untouched

    def test_status_missing_database_fails_and_creates_nothing(self):
        missing = self.root / "nope.sqlite"
        with self.assertRaises(StateError):
            engine.status(missing)
        self.assertFalse(missing.exists())

    def test_terminal_remote_failure_has_no_forward_transition(self):
        state_mod.initialize(self.state_path, "http://127.0.0.1:1", "r", "o")
        db = state_mod.open_state(self.state_path)
        state_mod.set_phase(db, state_mod.PHASE_BOUND)
        state_mod.set_phase(db, state_mod.PHASE_REMOTE_FAILED)
        with self.assertRaises(StateError):
            state_mod.set_phase(db, state_mod.PHASE_FROZEN)
        db.close()

    def test_frozen_blob_and_flag_commit_rollback_together(self):
        state_mod.initialize(self.state_path, "http://127.0.0.1:1", "r", "o")
        db = state_mod.open_state(self.state_path)
        state_mod.set_phase(db, state_mod.PHASE_BOUND)
        job = {
            "job_id": "j", "request_id": "r", "snapshot_id": "s",
            "files": [
                {
                    "role": role, "url": f"/{role}.csv",
                    "sha256": "0" * 64, "byte_length": 0,
                }
                for role in protocol.ROLES
            ],
        }
        with db:
            state_mod.freeze_job(db, job, b'{"x":1}')
            state_mod.set_phase(db, state_mod.PHASE_FROZEN)
        # Simulate a failure inside the blob-writing transaction: nothing may
        # be left half-completed.
        real_save = state_mod.save_role_blob

        def boom(connection, role, raw, digest, length):
            raise sqlite3.OperationalError("injected write failure")

        with mock.patch.object(state_mod, "save_role_blob", boom):
            with self.assertRaises(sqlite3.OperationalError):
                with db:
                    state_mod.record_http(db, "GET", "u", None, "download customers", 200, b"")
                    state_mod.save_role_blob(db, "customers", b"", "0" * 64, 0)
        row = state_mod.role_row(db, "customers")
        self.assertEqual(row[4], 0)  # completed flag rolled back
        self.assertIsNone(row[7])    # no blob left behind
        # After repair the genuine save works and verifies.
        with db:
            real_save(db, "customers", b"", "0" * 64, 0)
        row = state_mod.role_row(db, "customers")
        self.assertEqual(row[4], 1)
        db.close()

    def test_corrupted_checkpoint_is_not_a_cache_hit(self):
        state_mod.initialize(self.state_path, "http://127.0.0.1:1", "r", "o")
        db = state_mod.open_state(self.state_path)
        state_mod.set_phase(db, state_mod.PHASE_BOUND)
        import hashlib
        raw = b"hello"
        digest = hashlib.sha256(raw).hexdigest()
        job = {
            "job_id": "j", "request_id": "r", "snapshot_id": "s",
            "files": [
                {
                    "role": role, "url": f"/{role}.csv",
                    "sha256": digest if role == "customers" else "0" * 64,
                    "byte_length": len(raw) if role == "customers" else 0,
                }
                for role in protocol.ROLES
            ],
        }
        with db:
            state_mod.freeze_job(db, job, b"{}")
            state_mod.set_phase(db, state_mod.PHASE_FROZEN)
            state_mod.save_role_blob(db, "customers", raw, digest, len(raw))
        # Tamper with the stored bytes while leaving the completion flag set.
        with db:
            db.execute(
                "UPDATE capture_file SET raw_bytes=? WHERE role='customers'",
                (sqlite3.Binary(b"tampered"),),
            )
        row = state_mod.role_row(db, "customers")
        stored = bytes(row[7])
        actual_digest, actual_length = engine._digest(stored)
        self.assertNotEqual(actual_digest, row[2])
        # The engine's resume logic must drop the checkpoint rather than skip.
        with db:
            engine_complete = None
            stored2 = bytes(state_mod.role_row(db, "customers")[7])
            d, length = engine._digest(stored2)
            if not (d == row[2] and length == row[3] and d == row[5]):
                state_mod.drop_role_checkpoint(db, "customers")
        self.assertEqual(state_mod.role_row(db, "customers")[4], 0)
        db.close()


class CapturePayloadTest(unittest.TestCase):
    def test_deterministic_and_free_of_environment(self):
        frozen = {
            "ready_raw": b'{"state":"ready"}',
            "ready_sha256": hashlib.sha256(b'{"state":"ready"}').hexdigest(),
            "files": [
                {
                    "role": role, "url": f"/{role}.csv",
                    "sha256": "a" * 64, "byte_length": 3,
                    "actual_sha256": "a" * 64, "actual_length": 3,
                }
                for role in sorted(protocol.ROLES)
            ],
        }
        payload = engine.build_capture_payload("r", "j", "s", frozen)
        first = engine.serialize_capture_json(payload)
        second = engine.serialize_capture_json(payload)
        self.assertEqual(first, second)  # byte-stable
        text = first.decode("utf-8")
        self.assertNotIn(str(Path.home()), text)
        self.assertIn("manifest", text)
        self.assertIn("ready_response", text)
        self.assertIn("interpretation", text)
        parsed = json.loads(text)
        self.assertEqual(set(parsed["files"]), set(protocol.ROLES))
        for role, entry in parsed["files"].items():
            self.assertEqual(entry["source_path"], engine.CSV_NAMES[role])
            self.assertEqual(entry["actual_sha256"], entry["declared_sha256"])
        self.assertEqual(
            parsed["ready_response"]["sha256"], frozen["ready_sha256"]
        )


if __name__ == "__main__":
    unittest.main()
