"""End-to-end tests for the export-task capture client.

Every HTTP interaction here goes through a real loopback TCP server
(:class:`DemoPortal`, an explicit protocol fixture -- never a production
service).  The tests assert externally observable behavior: actual request
counts and targets on the portal log, files on disk, SQLite checkpoints and
real CLI subprocesses -- not just internal phase strings.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

from contract_warehouse.capture import engine, state as state_mod
from contract_warehouse.capture import portal as portal_mod
from contract_warehouse.capture import protocol as protocol_mod
from contract_warehouse.capture.errors import (
    BusinessValidationError,
    CaptureError,
    ProtocolError,
    StateError,
)
from contract_warehouse.capture.portal import DemoPortal, make_server
from contract_warehouse.capture.protocol import ROLES
from contract_warehouse import inherited

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
V2_FIX = ROOT / "fixtures_inherited"

CUSTOMERS_HEADER = "customer\n"
HIERARCHY_HEADER = "edge_id,customer,parent,valid_from,valid_to\n"
CONTRACTS_HEADER = "version_id,customer,valid_from,valid_to,tier\n"
TICKETS_HEADER = "ticket_id,customer,opened,cost_cents\n"


class PortalCase(unittest.TestCase):
    """Spin up a real loopback portal around the synthetic v2 fixtures."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        files = {
            role: (V2_FIX / name).read_bytes()
            for role, name in portal_mod.CSV_FILES.items()
        }
        self.original_files = files
        self.portal = DemoPortal({role: raw for role, raw in files.items()})
        self.server = make_server(self.portal)
        host, port = self.server.server_address[:2]
        self.endpoint = f"http://{host}:{port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self._shutdown)

    def _shutdown(self):
        self.server.shutdown()
        self.server.server_close()

    def paths(self, name: str):
        return (
            self.root / f"{name}.sqlite",
            self.root / f"out-{name}",
        )

    def run_capture(self, request_id="req", state_name="cap", output_name=None):
        state_f = self.root / f"{state_name}.sqlite"
        out = self.root / (output_name or f"out-{state_name}")
        return engine.run(self.endpoint, request_id, state_f, out), state_f, out

    # ---------------------------------------------------------------
    # Happy path, resumption and provenance
    # ---------------------------------------------------------------

    def test_pending_then_ready_full_delivery_and_determinism(self):
        (receipt, state_f, out) = self.run_capture("req-1")
        self.assertEqual(receipt["phase"], "waiting")
        self.assertFalse(out.exists())  # no half-published directory
        self.portal.mark_ready("req-1")
        (receipt2, _, _) = self.run_capture("req-1")
        self.assertEqual(receipt2["phase"], "published")
        self.assertEqual(
            sorted(p.name for p in out.iterdir()),
            sorted(engine.DELIVERED_FILES),
        )
        # Captured bytes are the fixture bytes, unchanged.
        for role, name in portal_mod.CSV_FILES.items():
            self.assertEqual(
                (out / name).read_bytes(), (V2_FIX / name).read_bytes()
            )
        capture_doc = json.loads((out / "capture.json").read_text("utf-8"))
        self.assertEqual(capture_doc["request_id"], "req-1")
        self.assertTrue(capture_doc["job_id"])
        self.assertTrue(capture_doc["snapshot_id"])
        # Repeating the exact run is offline-only and byte-deterministic.
        before = {p.name: p.read_bytes() for p in out.iterdir()}
        receipt3 = engine.run(self.endpoint, "req-1", state_f, out)
        self.assertEqual(receipt3.get("note"), "offline verification only")
        after = {p.name: p.read_bytes() for p in out.iterdir()}
        self.assertEqual(before, after)
        # Exactly one portal creation happened.
        self.assertEqual(self.portal.create_count, 1)
        gets = [e for e in self.portal.request_log if e["method"] == "GET"]
        posts = [e for e in self.portal.request_log if e["method"] == "POST"]
        self.assertEqual(len(posts), 1)
        # by-request once, job status once, four file downloads.
        self.assertEqual(sum("/exports/by-request/" in e["target"] for e in gets), 1)
        self.assertEqual(sum("/exports/files/" in e["target"] for e in gets), 4)
        # Rerun after publication performs no HTTP at all.
        logged_before = len(self.portal.request_log)
        engine.run(self.endpoint, "req-1", state_f, out)
        self.assertEqual(len(self.portal.request_log), logged_before)

    def test_delivered_directory_feeds_inherited_build_with_expected_numbers(self):
        self.run_capture("req-build")
        self.portal.mark_ready("req-build")
        _, state_f, out = self.run_capture("req-build")
        db = self.root / "warehouse.sqlite"
        result = inherited.build(out, db)
        self.assertEqual(result["tickets"], 6)
        self.assertEqual(
            result["tiers"],
            [
                {"tier": "basic", "tickets": 3, "cost_cents": 800},
                {"tier": "premium", "tickets": 2, "cost_cents": 700},
                {"tier": "uncovered", "tickets": 1, "cost_cents": 600},
            ],
        )
        # Per-ticket contract/depth matches the original v2 fixture behavior.
        with sqlite3.connect(db) as conn:
            facts = {
                ticket_id: (version_id, depth)
                for ticket_id, version_id, depth in conn.execute(
                    "SELECT t.ticket_id, c.version_id, t.match_depth "
                    "FROM dwd_ticket t LEFT JOIN dim_contract c USING(contract_sk)"
                )
            }
        self.assertEqual(
            facts,
            {
                "T1": ("K1", 1), "T2": ("K3", 0), "T3": ("K2", 2),
                "T4": ("K2", 2), "T5": ("K4", 1), "T6": (None, None),
            },
        )

    def test_state_and_output_recover_ready_raw_and_independent_digests(self):
        self.run_capture("req-prov")
        self.portal.mark_ready("req-prov")
        _, state_f, out = self.run_capture("req-prov")
        with sqlite3.connect(state_f) as db:
            job_id, request_id, snapshot_id, ready_raw, ready_sha = db.execute(
                "SELECT job_id, request_id, snapshot_id, ready_raw, ready_sha256 "
                "FROM capture_job"
            ).fetchone()
            self.assertEqual(request_id, "req-prov")
            self.assertEqual(hashlib.sha256(bytes(ready_raw)).hexdigest(), ready_sha)
            for role, name, blob, digest, length in db.execute(
                "SELECT role, url, raw_bytes, actual_sha256, actual_length "
                "FROM capture_file ORDER BY role"
            ):
                raw = bytes(blob)
                # Independent recomputation from stored bytes.
                self.assertEqual(hashlib.sha256(raw).hexdigest(), digest)
                self.assertEqual(len(raw), length)
                self.assertEqual(raw, (out / portal_mod.CSV_FILES[role]).read_bytes())
        # The ready document itself parses back to the four frozen roles.
        ready_doc = json.loads(bytes(ready_raw).decode("utf-8"))
        self.assertEqual(sorted(f["role"] for f in ready_doc["files"]), sorted(ROLES))
        self.assertEqual(ready_doc["snapshot_id"], snapshot_id)

    # ---------------------------------------------------------------
    # Creation/receipt recovery and idempotency
    # ---------------------------------------------------------------

    def test_dropped_201_response_is_recovered_by_lookup_with_one_creation(self):
        request_id = "req-drop"
        state_f, out = self.paths("drop")
        self.portal.drop_create_response.add(request_id)
        with self.assertRaises(ProtocolError):
            engine.run(self.endpoint, request_id, state_f, out)
        self.assertEqual(state_mod.open_state(state_f).execute(
            "SELECT meta_value FROM capture_meta WHERE meta_key='phase'"
        ).fetchone()[0], state_mod.PHASE_CREATE_SUBMITTED)
        # New process: by-request finds the existing job; no second creation.
        receipt = engine.run(self.endpoint, request_id, state_f, out)
        self.assertIn(receipt["phase"], ("bound", "waiting"))
        self.assertEqual(self.portal.create_count, 1)
        # Completing the capture still uses the single task.
        self.portal.mark_ready(request_id)
        receipt = engine.run(self.endpoint, request_id, state_f, out)
        self.assertEqual(receipt["phase"], "published")
        self.assertEqual(self.portal.create_count, 1)

    def test_synchronous_hook_exit_before_accepting_creation_recovers(self):
        request_id = "req-hook"
        state_f, out = self.paths("hook")

        # Client-side synchronous barrier: the POST bytes are already on the
        # wire (the portal accepts and creates exactly one job) but the client
        # process is terminated before it reads/accepts the 201 receipt.
        def hook(name):
            if name == engine.BARRIER_CREATE_SENT:
                raise engine.BarrierExit("client exits before accepting create receipt")

        with mock.patch.object(engine, "barrier", hook):
            with self.assertRaises(engine.BarrierExit):
                engine.run(self.endpoint, request_id, state_f, out)
        # The portal created exactly one job; the receipt is locally unknown.
        self.assertEqual(self.portal.create_count, 1)
        with sqlite3.connect(state_f) as db:
            self.assertEqual(db.execute(
                "SELECT meta_value FROM capture_meta WHERE meta_key='phase'"
            ).fetchone()[0], state_mod.PHASE_CREATE_SUBMITTED)
        # A new process rebinds through by-request without a second creation.
        receipt = engine.run(self.endpoint, request_id, state_f, out)
        self.assertIn(receipt["phase"], ("bound", "waiting"))
        self.assertEqual(self.portal.create_count, 1)
        # And the capture can still be completed via the same single task.
        self.portal.mark_ready(request_id)
        receipt = engine.run(self.endpoint, request_id, state_f, out)
        self.assertEqual(receipt["phase"], "published")
        self.assertEqual(self.portal.create_count, 1)

    def test_lookup_500_timeout_and_identity_mismatch_never_create(self):
        request_id = "req-noise"
        state_f, out = self.paths("noise")
        self.portal.by_request_status[request_id] = 500
        with self.assertRaises(ProtocolError):
            engine.run(self.endpoint, request_id, state_f, out)
        self.assertEqual(self.portal.create_count, 0)
        # Timeout-like dropped connection (injected status 0).
        self.portal.by_request_status[request_id] = 0
        with self.assertRaises(ProtocolError):
            engine.run(self.endpoint, request_id, state_f, out)
        self.assertEqual(self.portal.create_count, 0)
        # Once the noise clears, the normal flow creates exactly one job.
        del self.portal.by_request_status[request_id]
        receipt = engine.run(self.endpoint, request_id, state_f, out)
        self.assertEqual(receipt["phase"], "waiting")
        self.assertEqual(self.portal.create_count, 1)
        # A 200 lookup carrying a foreign request_id must be rejected as an
        # identity mismatch without creating.  Pre-create the portal job so
        # the lookup answers 200, then rewrite only its identity field.
        other_id = "req-foreign-identity"
        other_state = self.root / "foreign-id.sqlite"
        other_out = self.root / "out-foreign-id"
        self.portal.create(other_id)

        def rewrite(rid, job, payload):
            if rid == other_id:
                payload["request_id"] = "WRONG-ID"
            return payload

        self.portal.by_request_rewrite = rewrite
        with self.assertRaises(ProtocolError):
            engine.run(self.endpoint, other_id, other_state, other_out)
        # Only the two pre-existing jobs (req-noise above + this one); the
        # mismatch lookup must not have created another task.
        self.assertEqual(sorted(self.portal.jobs), [other_id, request_id])
        self.assertEqual(self.portal.create_count, 2)
        self.portal.by_request_rewrite = None
        # And the logged failures are real persisted exchanges, never 404s.
        with sqlite3.connect(state_f) as db:
            statuses = [
                row[0] for row in db.execute(
                    "SELECT response_status FROM capture_http "
                    "WHERE request_source='by-request lookup' ORDER BY exchange_id"
                )
            ]
        self.assertIn(500, statuses)
        self.assertIn(None, statuses)  # the dropped connection

    def test_special_character_request_ids_encode_and_keep_identity(self):
        for request_id in ("a/b/c", "with space", "中文请求", "q?x=1&y=2", "MiXeD-Case"):
            with self.subTest(request_id=request_id):
                state_f = self.root / f"state-{hashlib.sha256(request_id.encode()).hexdigest()[:8]}.sqlite"
                out = self.root / f"out-{state_f.stem}"
                receipt = engine.run(self.endpoint, request_id, state_f, out)
                self.assertEqual(receipt["request_id"], request_id)
                # The wire target contains no raw slash/space/question marks
                # inside the encoded segment.
                lookup = next(
                    e for e in self.portal.request_log
                    if "/exports/by-request/" in e["target"]
                )
                segment = lookup["target"].rsplit("/", 1)[1].split("?", 1)[0]
                self.assertNotIn("/", segment)
                self.assertNotIn(" ", segment)
                self.assertNotIn("?", segment)
                self.portal.request_log.clear()
                self.portal.mark_ready(request_id)
                receipt = engine.run(self.endpoint, request_id, state_f, out)
                self.assertEqual(receipt["phase"], "published")
                self.assertEqual(
                    json.loads((out / "capture.json").read_text())["request_id"],
                    request_id,
                )

    def test_binding_change_rejected_before_network(self):
        request_id = "req-bind"
        state_f, out = self.paths("bind")
        engine.run(self.endpoint, request_id, state_f, out)
        logged = len(self.portal.request_log)
        with self.assertRaises(StateError):
            engine.run(self.endpoint, request_id, state_f, self.root / "other-out")
        with self.assertRaises(StateError):
            engine.run(self.endpoint, "different-id", state_f, out)
        with self.assertRaises(StateError):
            engine.run("http://127.0.0.1:9999", request_id, state_f, out)
        self.assertEqual(len(self.portal.request_log), logged)

    # ---------------------------------------------------------------
    # Frozen manifest: partial downloads, faults, no re-query
    # ---------------------------------------------------------------

    def _freeze(self, request_id="req-frozen"):
        state_f, out = self.paths("frozen")
        engine.run(self.endpoint, request_id, state_f, out)
        self.portal.mark_ready(request_id)
        return state_f, out

    def test_short_read_on_third_role_resumes_without_requery_or_redownload(self):
        request_id = "req-short"
        state_f, out = self.paths("short")
        engine.run(self.endpoint, request_id, state_f, out)
        self.portal.mark_ready(request_id)
        # Roles are processed in fixed alphabetical order (contracts,
        # customers, hierarchy, tickets).  Fail the third role: the server
        # sends the original Content-Length but fewer bytes, so the client
        # observes an actual short read; no checkpoint is committed for it.
        self.portal.file_faults["hierarchy"] = {
            "truncate": 5, "declare_full_length": True,
        }
        with mock.patch.object(protocol_mod, "REQUEST_TIMEOUT_SECONDS", 0.4):
            with self.assertRaises(ProtocolError):
                engine.run(self.endpoint, request_id, state_f, out)
        self.assertFalse(out.exists())
        with sqlite3.connect(state_f) as db:
            done = {r[0] for r in db.execute(
                "SELECT role FROM capture_file WHERE completed=1")}
        self.assertEqual(done, {"contracts", "customers"})
        # Fix the portal; resume must not query job status again and must not
        # re-request the two completed roles.
        self.portal.file_faults.clear()
        logged = list(self.portal.request_log)
        receipt = engine.run(self.endpoint, request_id, state_f, out)
        self.assertEqual(receipt["phase"], "published")
        new_entries = self.portal.request_log[len(logged):]
        self.assertFalse(
            any(e["method"] == "GET" and "/exports/by-request/" in e["target"]
                for e in new_entries)
        )
        job_targets = [
            e["target"] for e in new_entries
            if e["method"] == "GET" and "/exports/files/" not in e["target"]
        ]
        self.assertEqual(job_targets, [])  # no status re-query at all
        file_roles = [
            e["target"].rsplit("/", 1)[1] for e in new_entries
            if "/exports/files/" in e["target"]
        ]
        self.assertEqual(sorted(file_roles), ["hierarchy.csv", "tickets.csv"])

    def test_digest_mismatch_is_rejected_and_same_snapshot_resumed(self):
        request_id = "req-bad-digest"
        state_f, out = self.paths("bad-digest")
        engine.run(self.endpoint, request_id, state_f, out)
        self.portal.mark_ready(request_id)
        self.portal.file_faults["tickets"] = {"corrupt_tail": True}
        with self.assertRaises(ProtocolError):
            engine.run(self.endpoint, request_id, state_f, out)
        # The frozen manifest and the other checkpoints are retained.
        with sqlite3.connect(state_f) as db:
            snapshot = db.execute("SELECT snapshot_id FROM capture_job").fetchone()[0]
            done = {r[0] for r in db.execute(
                "SELECT role FROM capture_file WHERE completed=1")}
        self.assertEqual(done, {"customers", "hierarchy", "contracts"})
        self.portal.file_faults.clear()
        receipt = engine.run(self.endpoint, request_id, state_f, out)
        self.assertEqual(receipt["phase"], "published")
        with sqlite3.connect(state_f) as db:
            self.assertEqual(db.execute("SELECT snapshot_id FROM capture_job").fetchone()[0], snapshot)

    def test_corrupted_local_blob_is_detected_and_refetched(self):
        request_id = "req-tamper"
        state_f, out = self.paths("tamper")
        engine.run(self.endpoint, request_id, state_f, out)
        self.portal.mark_ready(request_id)
        # Let two roles complete and stop on the third, then tamper with a
        # saved blob locally: the completion flag alone must not be trusted.
        self.portal.file_faults["contracts"] = {"truncate": 2}
        with self.assertRaises(ProtocolError):
            engine.run(self.endpoint, request_id, state_f, out)
        with sqlite3.connect(state_f) as db:
            db.execute(
                "UPDATE capture_file SET raw_bytes=? WHERE role='customers'",
                (sqlite3.Binary(b"tampered bytes"),),
            )
        self.portal.file_faults.clear()
        logged = len(self.portal.request_log)
        receipt = engine.run(self.endpoint, request_id, state_f, out)
        self.assertEqual(receipt["phase"], "published")
        # The tampered role was re-fetched rather than treated as a hit.
        new_targets = [e["target"] for e in self.portal.request_log[logged:]]
        self.assertTrue(any("customers.csv" in t for t in new_targets))

    def test_manifest_shuffle_keeps_fixed_role_binding(self):
        request_id = "req-shuffle"
        self.portal.manifest_shuffle.add(request_id)
        state_f, out = self.paths("shuffle")
        engine.run(self.endpoint, request_id, state_f, out)
        self.portal.mark_ready(request_id)
        receipt = engine.run(self.endpoint, request_id, state_f, out)
        self.assertEqual(receipt["phase"], "published")
        # Each delivered file matches its own role's fixture bytes regardless
        # of the array order in the ready response.
        for role, name in portal_mod.CSV_FILES.items():
            self.assertEqual((out / name).read_bytes(), (V2_FIX / name).read_bytes())

    def test_missing_duplicate_role_and_cross_origin_links_fail(self):
        request_id = "req-bad-manifest"
        state_f, out = self.paths("bad-manifest")
        engine.run(self.endpoint, request_id, state_f, out)

        def drop_role(rid, files):
            return files[:3]  # one role missing

        self.portal.manifest_mutator = drop_role
        self.portal.mark_ready(request_id)
        with self.assertRaises(ProtocolError):
            engine.run(self.endpoint, request_id, state_f, out)
        self.assertFalse(out.exists())

        # A cross-origin manifest link must be refused before download.
        fresh_state = self.root / "cross.sqlite"
        fresh_out = self.root / "out-cross"
        rid2 = "req-cross"
        engine.run(self.endpoint, rid2, fresh_state, fresh_out)

        def cross_origin(rid, files):
            files[0]["url"] = "//evil.example/x.csv"
            return files

        self.portal.manifest_mutator = cross_origin
        self.portal.mark_ready(rid2)
        with self.assertRaises(ProtocolError):
            engine.run(self.endpoint, rid2, fresh_state, fresh_out)

    def test_remote_failure_is_terminal_and_creates_no_replacement(self):
        request_id = "req-fail"
        state_f, out = self.paths("fail")
        engine.run(self.endpoint, request_id, state_f, out)
        self.portal.mark_failed(request_id, "upstream exploded")
        with self.assertRaises(CaptureError):
            engine.run(self.endpoint, request_id, state_f, out)
        with sqlite3.connect(state_f) as db:
            phase = db.execute(
                "SELECT meta_value FROM capture_meta WHERE meta_key='phase'"
            ).fetchone()[0]
            error = db.execute(
                "SELECT meta_value FROM capture_meta WHERE meta_key='remote_error'"
            ).fetchone()[0]
        self.assertEqual(phase, state_mod.PHASE_REMOTE_FAILED)
        self.assertIn("upstream exploded", error)
        logged = len(self.portal.request_log)
        # Reruns end offline from the recorded fact; no HTTP, no new task.
        with self.assertRaises(CaptureError):
            engine.run(self.endpoint, request_id, state_f, out)
        self.assertEqual(len(self.portal.request_log), logged)
        self.assertEqual(self.portal.create_count, 1)

    # ---------------------------------------------------------------
    # Business validation reuses the full v2 rules
    # ---------------------------------------------------------------

    def _capture_bad_snapshot(self, request_id, mutated_files):
        state_f, out = self.paths(f"biz-{request_id}")
        self.portal._files = {**self.original_files, **mutated_files}
        engine.run(self.endpoint, request_id, state_f, out)
        self.portal.mark_ready(request_id)
        return state_f, out

    def test_business_failures_keep_bytes_but_never_publish(self):
        scenarios = {
            "cycle": {
                "hierarchy.csv": (
                    HIERARCHY_HEADER
                    + "e1,S,D1,2026-01-01,\n"
                    + "e2,D1,S,2026-01-01,\n"
                )
            },
            "unknown_customer": {
                "tickets.csv": TICKETS_HEADER + "T9,ZZZ,2026-01-10,1\n"
            },
            "bad_date": {
                "tickets.csv": TICKETS_HEADER + "T9,S,2026-02-30,1\n"
            },
            "bad_amount": {
                "tickets.csv": TICKETS_HEADER + "T9,S,2026-01-10,-5\n"
            },
            "overlap": {
                "contracts.csv": (
                    (V2_FIX / "contracts.csv").read_text()
                    + "K9,H,2026-01-05,2026-01-10,premium\n"
                )
            },
        }
        for label, mutated in scenarios.items():
            with self.subTest(label=label):
                # Isolated portal content per scenario.
                self.portal._files = {
                    role: (V2_FIX / name).read_bytes()
                    for role, name in portal_mod.CSV_FILES.items()
                }
                rid = f"req-{label}"
                state_f = self.root / f"{rid}.sqlite"
                out = self.root / f"out-{rid}"
                for name, content in mutated.items():
                    role = next(r for r, n in portal_mod.CSV_FILES.items() if n == name)
                    raw = content.encode("utf-8") if isinstance(content, str) else content
                    self.portal._files[role] = raw
                engine.run(self.endpoint, rid, state_f, out)
                self.portal.mark_ready(rid)
                with self.assertRaises(BusinessValidationError):
                    engine.run(self.endpoint, rid, state_f, out)
                self.assertFalse(out.exists())
                with sqlite3.connect(state_f) as db:
                    phase = db.execute(
                        "SELECT meta_value FROM capture_meta WHERE meta_key='phase'"
                    ).fetchone()[0]
                    message = db.execute(
                        "SELECT meta_value FROM capture_meta "
                        "WHERE meta_key='validation_error'"
                    ).fetchone()[0]
                self.assertEqual(phase, state_mod.PHASE_VALIDATION_FAILED)
                self.assertTrue(message)
                # All four raw blobs remain inspectable in state.
                with sqlite3.connect(state_f) as db:
                    self.assertEqual(db.execute(
                        "SELECT count(*) FROM capture_file WHERE completed=1"
                    ).fetchone()[0], 4)

    def test_empty_hierarchy_is_a_valid_delivery(self):
        self.portal._files = {
            role: (V2_FIX / name).read_bytes()
            for role, name in portal_mod.CSV_FILES.items()
        }
        self.portal._files["hierarchy"] = HIERARCHY_HEADER.encode("utf-8")
        rid = "req-empty-hierarchy"
        state_f = self.root / f"{rid}.sqlite"
        out = self.root / f"out-{rid}"
        engine.run(self.endpoint, rid, state_f, out)
        self.portal.mark_ready(rid)
        receipt = engine.run(self.endpoint, rid, state_f, out)
        self.assertEqual(receipt["phase"], "published")
        # Non-empty customers/contracts/tickets rules still hold downstream.
        db = self.root / "empty-h.sqlite"
        result = inherited.build(out, db)
        self.assertEqual(result["edges"], 0)

    def test_empty_customers_still_rejected_by_v2_rules(self):
        self.portal._files = {
            role: (V2_FIX / name).read_bytes()
            for role, name in portal_mod.CSV_FILES.items()
        }
        self.portal._files["customers"] = CUSTOMERS_HEADER.encode("utf-8")
        rid = "req-empty-customers"
        state_f = self.root / f"{rid}.sqlite"
        out = self.root / f"out-{rid}"
        engine.run(self.endpoint, rid, state_f, out)
        self.portal.mark_ready(rid)
        with self.assertRaises(BusinessValidationError):
            engine.run(self.endpoint, rid, state_f, out)
        self.assertFalse(out.exists())

    # ---------------------------------------------------------------
    # Publication atomicity and offline recovery
    # ---------------------------------------------------------------

    def test_barrier_between_rename_and_marker_is_reconciled_offline(self):
        request_id = "req-barrier"
        state_f, out = self.paths("barrier")
        engine.run(self.endpoint, request_id, state_f, out)
        self.portal.mark_ready(request_id)
        # Arm an in-process barrier that raises at the exact instruction
        # between os.rename() and the published-marker transaction.
        with mock.patch.object(
            engine, "barrier",
            side_effect=lambda name: (_ for _ in ()).throw(
                engine.BarrierExit(f"barrier {name}")
            ) if name == engine.BARRIER_AFTER_RENAME else None,
        ):
            with self.assertRaises(engine.BarrierExit):
                engine.run(self.endpoint, request_id, state_f, out)
        # The directory exists but the local marker is still pre-publish.
        self.assertTrue(out.is_dir())
        with sqlite3.connect(state_f) as db:
            phase = db.execute(
                "SELECT meta_value FROM capture_meta WHERE meta_key='phase'"
            ).fetchone()[0]
        self.assertEqual(phase, state_mod.PHASE_FROZEN)
        logged = len(self.portal.request_log)
        # Recovery performs the full offline verification and commits the
        # marker without any HTTP.
        receipt = engine.run(self.endpoint, request_id, state_f, out)
        self.assertEqual(receipt["phase"], "published")
        self.assertEqual(receipt.get("note"), "recovered publication")
        self.assertEqual(len(self.portal.request_log), logged)

    def test_published_damage_is_not_silently_ignored(self):
        request_id = "req-damage"
        state_f, out = self.paths("damage")
        engine.run(self.endpoint, request_id, state_f, out)
        self.portal.mark_ready(request_id)
        engine.run(self.endpoint, request_id, state_f, out)
        # Extra file, missing file, modified byte, modified manifest.
        (out / "extra.txt").write_text("nope")
        with self.assertRaises(StateError):
            engine.run(self.endpoint, request_id, state_f, out)
        (out / "extra.txt").unlink()
        shutil.move(str(out / "tickets.csv"), self.root / "tickets-held.csv")
        with self.assertRaises(StateError):
            engine.run(self.endpoint, request_id, state_f, out)
        shutil.move(str(self.root / "tickets-held.csv"), out / "tickets.csv")
        with (out / "customers.csv").open("ab") as handle:
            handle.write(b"tamper\n")
        with self.assertRaises(StateError):
            engine.run(self.endpoint, request_id, state_f, out)
        # The damaged directory is preserved for inspection, never overwritten.
        self.assertTrue(out.is_dir())

    def test_foreign_output_directory_is_refused(self):
        request_id = "req-foreign"
        state_f, out = self.paths("foreign")
        out.mkdir()
        (out / "someone-else.txt").write_text("x")
        engine.run(self.endpoint, request_id, state_f, self.root / "unused")
        self.portal.mark_ready(request_id)
        with self.assertRaises(StateError):
            engine.run(self.endpoint, request_id, state_f, out)
        self.assertEqual((out / "someone-else.txt").read_text(), "x")

    def test_symlink_output_and_state_paths_refused(self):
        request_id = "req-symlink"
        real_out = self.root / "real-out"
        link_out = self.root / "link-out"
        link_out.symlink_to(real_out, target_is_directory=True)
        state_f = self.root / "sym.sqlite"
        with self.assertRaises(StateError):
            engine.run(self.endpoint, request_id, state_f, link_out)

    def test_state_inside_output_rejected(self):
        request_id = "req-nested"
        out = self.root / "nested-out"
        state_f = out / "cap.sqlite"
        with self.assertRaises(StateError):
            engine.run(self.endpoint, request_id, state_f, out)

    def test_rename_failure_keeps_checkpoint_and_resumes(self):
        request_id = "req-rename-fail"
        state_f, out = self.paths("rename-fail")
        engine.run(self.endpoint, request_id, state_f, out)
        self.portal.mark_ready(request_id)
        real_rename = os.rename

        def boom(src, dst):
            if Path(dst) == out:
                raise OSError("injected rename failure")
            return real_rename(src, dst)

        with mock.patch("contract_warehouse.capture.engine.os.rename", boom):
            with self.assertRaises(OSError):
                engine.run(self.endpoint, request_id, state_f, out)
        self.assertFalse(out.exists())
        # Checkpoints persist; no staging directory is left behind.
        self.assertEqual(
            list(self.root.glob(".capture-stage-*")), []
        )
        receipt = engine.run(self.endpoint, request_id, state_f, out)
        self.assertEqual(receipt["phase"], "published")

    def test_sql_failure_before_role_commit_rolls_back_blob_and_flag(self):
        request_id = "req-sql-boom"
        state_f, out = self.paths("sql-boom")
        engine.run(self.endpoint, request_id, state_f, out)
        self.portal.mark_ready(request_id)
        real_save = state_mod.save_role_blob

        def boom(db, role, raw, digest, length):
            if role == "customers":
                raise sqlite3.OperationalError("injected sql failure")
            return real_save(db, role, raw, digest, length)

        with mock.patch.object(state_mod, "save_role_blob", boom):
            with self.assertRaises(sqlite3.OperationalError):
                engine.run(self.endpoint, request_id, state_f, out)
        with sqlite3.connect(state_f) as db:
            row = db.execute(
                "SELECT completed, raw_bytes FROM capture_file WHERE role='customers'"
            ).fetchone()
        self.assertEqual(row[0], 0)
        self.assertIsNone(row[1])
        # Resume after the environment is repaired.
        receipt = engine.run(self.endpoint, request_id, state_f, out)
        self.assertEqual(receipt["phase"], "published")

    def test_full_acceptance_chain_creation_continuation_recovery_build(self):
        """One test spans creation loss, cross-process continuation, the
        rename/publish-marker crash window, offline recovery and the original
        warehouse build with the exact expected business numbers."""
        request_id = "req-acceptance"
        state_f, out = self.paths("acceptance")
        # 1) Creation request is sent but its 201 receipt is lost.
        self.portal.drop_create_response.add(request_id)
        with self.assertRaises(ProtocolError):
            engine.run(self.endpoint, request_id, state_f, out)
        self.assertEqual(self.portal.create_count, 1)
        # 2) A new process rebinds the existing task via by-request (no new task).
        receipt = engine.run(self.endpoint, request_id, state_f, out)
        self.assertEqual(receipt["phase"], "waiting")
        self.assertEqual(self.portal.create_count, 1)
        # 3) Portal freezes the snapshot; capture reaches publication, then a
        #    hard landing between rename and the published marker is simulated.
        self.portal.mark_ready(request_id)
        with mock.patch.object(
            engine, "barrier",
            side_effect=lambda name: (_ for _ in ()).throw(
                engine.BarrierExit(name)
            ) if name == engine.BARRIER_AFTER_RENAME else None,
        ):
            with self.assertRaises(engine.BarrierExit):
                engine.run(self.endpoint, request_id, state_f, out)
        # 4) Recovery performs full offline verification and reconciles the
        #    marker without issuing any HTTP request.
        logged = len(self.portal.request_log)
        receipt = engine.run(self.endpoint, request_id, state_f, out)
        self.assertEqual(receipt["phase"], "published")
        self.assertEqual(receipt.get("note"), "recovered publication")
        self.assertEqual(len(self.portal.request_log), logged)
        # 5) The delivered directory feeds the original v2 warehouse with the
        #    exact fixture-derived numbers and per-ticket contract/depth.
        db = self.root / "acceptance-warehouse.sqlite"
        result = inherited.build(out, db)
        self.assertEqual(result["tickets"], 6)
        self.assertEqual(
            result["tiers"],
            [
                {"tier": "basic", "tickets": 3, "cost_cents": 800},
                {"tier": "premium", "tickets": 2, "cost_cents": 700},
                {"tier": "uncovered", "tickets": 1, "cost_cents": 600},
            ],
        )
        with sqlite3.connect(db) as conn:
            facts = {
                ticket_id: (version_id, depth)
                for ticket_id, version_id, depth in conn.execute(
                    "SELECT t.ticket_id, c.version_id, t.match_depth "
                    "FROM dwd_ticket t LEFT JOIN dim_contract c USING(contract_sk)"
                )
            }
        self.assertEqual(
            facts,
            {
                "T1": ("K1", 1), "T2": ("K3", 0), "T3": ("K2", 2),
                "T4": ("K2", 2), "T5": ("K4", 1), "T6": (None, None),
            },
        )
        # The portal-fully-shut variant of the same recovery is covered
        # separately by CliSubprocessTest.test_cli_hard_kill_between_rename...


class CliSubprocessTest(PortalCase):
    """Real CLI subprocesses, including hard termination at a barrier."""

    def _cli(self, *args, env_extra=None):
        env = {k: v for k, v in os.environ.items() if k != "CAPTURE_BARRIER"}
        env["PYTHONPATH"] = str(SRC_DIR)
        if env_extra:
            env.update(env_extra)
        return subprocess.run(
            [sys.executable, "-m", "contract_warehouse.capture", *args],
            cwd=SRC_DIR, capture_output=True, text=True, env=env,
        )

    def test_cli_pending_ready_then_inherited_cli_and_compare_unchanged(self):
        request_id = "req-cli-e2e"
        state_f = self.root / "cli.sqlite"
        out = self.root / "cli-out"
        proc = self._cli(
            "run", "--endpoint", self.endpoint, "--request-id", request_id,
            "--state", str(state_f), "--output", str(out),
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["phase"], "waiting")
        self.portal.mark_ready(request_id)
        proc = self._cli(
            "run", "--endpoint", self.endpoint, "--request-id", request_id,
            "--state", str(state_f), "--output", str(out),
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["phase"], "published")
        # The original inherited CLI consumes the delivered directory.
        db = self.root / "cli-warehouse.sqlite"
        build = subprocess.run(
            [sys.executable, "-m", "contract_warehouse.inherited",
             "--source", str(out), "--database", str(db)],
            cwd=SRC_DIR, capture_output=True, text=True,
        )
        self.assertEqual(build.returncode, 0, build.stderr)
        payload = json.loads(build.stdout)
        self.assertEqual(payload["tickets"], 6)
        self.assertEqual(
            payload["tiers"],
            [
                {"tier": "basic", "tickets": 3, "cost_cents": 800},
                {"tier": "premium", "tickets": 2, "cost_cents": 700},
                {"tier": "uncovered", "tickets": 1, "cost_cents": 600},
            ],
        )

    def test_cli_status_is_offline_and_missing_state_fails_without_creating(self):
        request_id = "req-cli-status"
        state_f = self.root / "status.sqlite"
        out = self.root / "status-out"
        self._cli(
            "run", "--endpoint", self.endpoint, "--request-id", request_id,
            "--state", str(state_f), "--output", str(out),
        )
        logged = len(self.portal.request_log)
        proc = self._cli("status", "--state", str(state_f))
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(json.loads(proc.stdout)["phase"], "waiting")
        self.assertEqual(len(self.portal.request_log), logged)
        missing = self.root / "missing.sqlite"
        proc = self._cli("status", "--state", str(missing))
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("not found", proc.stderr)
        self.assertFalse(missing.exists())

    def test_cli_hard_kill_between_rename_and_marker_then_offline_recovery(self):
        request_id = "req-cli-barrier"
        state_f = self.root / "barrier-cli.sqlite"
        out = self.root / "barrier-cli-out"
        self._cli(
            "run", "--endpoint", self.endpoint, "--request-id", request_id,
            "--state", str(state_f), "--output", str(out),
        )
        self.portal.mark_ready(request_id)
        # os._exit inside the child: no finally blocks, exactly like a hard
        # termination landing after the directory rename.
        proc = self._cli(
            "run", "--endpoint", self.endpoint, "--request-id", request_id,
            "--state", str(state_f), "--output", str(out),
            env_extra={"CAPTURE_BARRIER": engine.BARRIER_AFTER_RENAME},
        )
        self.assertEqual(proc.returncode, 3)
        self.assertTrue(out.is_dir())
        # Shut the portal down before recovery: it must not need the network.
        self.server.shutdown()
        proc = self._cli(
            "run", "--endpoint", self.endpoint, "--request-id", request_id,
            "--state", str(state_f), "--output", str(out),
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        receipt = json.loads(proc.stdout)
        self.assertEqual(receipt["phase"], "published")
        self.assertEqual(receipt.get("note"), "recovered publication")
        # Restart not needed; subsequent status stays consistent offline.
        proc = self._cli("status", "--state", str(state_f))
        self.assertEqual(json.loads(proc.stdout)["phase"], "published")

    def test_cli_role_pre_commit_barrier_rolls_back_and_resumes(self):
        request_id = "req-cli-role-barrier"
        state_f = self.root / "role-barrier.sqlite"
        out = self.root / "role-barrier-out"
        self._cli(
            "run", "--endpoint", self.endpoint, "--request-id", request_id,
            "--state", str(state_f), "--output", str(out),
        )
        self.portal.mark_ready(request_id)
        # Hard termination inside the first role's write transaction: BLOB
        # and completion flag must roll back together, leaving no half mark.
        proc = self._cli(
            "run", "--endpoint", self.endpoint, "--request-id", request_id,
            "--state", str(state_f), "--output", str(out),
            env_extra={"CAPTURE_BARRIER": engine.BARRIER_ROLE_BEFORE_COMMIT},
        )
        self.assertEqual(proc.returncode, 3)
        with sqlite3.connect(state_f) as db:
            self.assertEqual(db.execute(
                "SELECT count(*) FROM capture_file WHERE completed=1"
            ).fetchone()[0], 0)
            for (blob,) in db.execute("SELECT raw_bytes FROM capture_file"):
                self.assertIsNone(blob)
        proc = self._cli(
            "run", "--endpoint", self.endpoint, "--request-id", request_id,
            "--state", str(state_f), "--output", str(out),
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["phase"], "published")

    def test_cli_role_post_commit_barrier_keeps_checkpoint_and_skips_http(self):
        request_id = "req-cli-role-saved"
        state_f = self.root / "role-saved.sqlite"
        out = self.root / "role-saved-out"
        self._cli(
            "run", "--endpoint", self.endpoint, "--request-id", request_id,
            "--state", str(state_f), "--output", str(out),
        )
        self.portal.mark_ready(request_id)
        # Hard termination just after the first role's commit returned.
        proc = self._cli(
            "run", "--endpoint", self.endpoint, "--request-id", request_id,
            "--state", str(state_f), "--output", str(out),
            env_extra={"CAPTURE_BARRIER": engine.BARRIER_ROLE_SAVED},
        )
        self.assertEqual(proc.returncode, 3)
        with sqlite3.connect(state_f) as db:
            completed = {
                row[0] for row in db.execute(
                    "SELECT role FROM capture_file WHERE completed=1"
                )
            }
        # Exactly the first role (alphabetical order) survived.
        self.assertEqual(completed, {"contracts"})
        # Resume re-verifies the stored BLOB and skips its HTTP request.
        logged = len(self.portal.request_log)
        proc = self._cli(
            "run", "--endpoint", self.endpoint, "--request-id", request_id,
            "--state", str(state_f), "--output", str(out),
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        targets = [e["target"] for e in self.portal.request_log[logged:]]
        self.assertFalse(any("contracts.csv" in t for t in targets))
        self.assertTrue(any("customers.csv" in t for t in targets))


if __name__ == "__main__":
    unittest.main()
