"""Tests for the inherited-contract (v2) warehouse.

Covers the headline inheritance scenario, temporal hierarchy validation,
bridge-table granularity, an independent day-by-day reference oracle, source
provenance, the real CLI subprocess and single-writer failure semantics.
The day-by-day expansion below is a small-test oracle only; the production
algorithm (contract_warehouse.hierarchy) is interval/endpoint based.
"""

from contextlib import closing
from datetime import date, timedelta
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from contract_warehouse import inherited
from contract_warehouse.hierarchy import (
    ValidationError,
    active_parent_edge,
    build_bridge,
    load_inherited,
    select_contract,
)
from contract_warehouse.build import build as build_v1

ROOT = Path(__file__).resolve().parents[1]
V1_FIX = ROOT / "fixtures"
V2_FIX = ROOT / "fixtures_inherited"
SRC_DIR = ROOT / "src"

CUSTOMERS_HEADER = "customer\n"
HIERARCHY_HEADER = "edge_id,customer,parent,valid_from,valid_to\n"
CONTRACTS_HEADER = "version_id,customer,valid_from,valid_to,tier\n"
TICKETS_HEADER = "ticket_id,customer,opened,cost_cents\n"


def write_sources(directory: Path, mapping: dict[str, str]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for name, content in mapping.items():
        (directory / name).write_text(content, encoding="utf-8")


def inherited_sources(directory: Path, **overrides) -> None:
    mapping = {
        "customers.csv": (V2_FIX / "customers.csv").read_text(encoding="utf-8"),
        "hierarchy.csv": (V2_FIX / "hierarchy.csv").read_text(encoding="utf-8"),
        "contracts.csv": (V2_FIX / "contracts.csv").read_text(encoding="utf-8"),
        "tickets.csv": (V2_FIX / "tickets.csv").read_text(encoding="utf-8"),
    }
    mapping.update(overrides)
    write_sources(directory, mapping)


def daterange(start: date, end: date):
    current = start
    while current < end:
        yield current
        current += timedelta(days=1)


def ordered_dump(db: sqlite3.Connection, table: str):
    columns = db.execute(f"PRAGMA table_info({table})").fetchall()
    ordinals = ",".join(str(i) for i in range(1, len(columns) + 1))
    return db.execute(f"SELECT * FROM {table} ORDER BY {ordinals}").fetchall()


class InheritedScenarioTest(unittest.TestCase):
    """Requirement 2/3: the six-ticket group hierarchy scenario."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.source = self.root / "input"
        self.db = self.root / "warehouse.sqlite"
        inherited_sources(self.source)

    def build(self):
        return inherited.build(self.source, self.db)

    def test_summary_and_ticket_facts(self):
        result = self.build()
        self.assertEqual(result["schema"], "inherited_contracts_v2")
        self.assertEqual(result["schema_version"], "2")
        self.assertEqual(
            result["tiers"],
            [
                {"tier": "basic", "tickets": 3, "cost_cents": 800},
                {"tier": "premium", "tickets": 2, "cost_cents": 700},
                {"tier": "uncovered", "tickets": 1, "cost_cents": 600},
            ],
        )
        expected = {
            "T1": ("K1", 1, "D1", 100),
            "T2": ("K3", 0, "S", 200),
            "T3": ("K2", 2, "H", 300),
            "T4": ("K2", 2, "H", 400),
            "T5": ("K4", 1, "D2", 500),
            "T6": (None, None, None, 600),
        }
        with closing(sqlite3.connect(self.db)) as db:
            rows = {
                ticket_id: (version_id, depth, holder, cost)
                for ticket_id, version_id, depth, holder, cost in db.execute(
                    "SELECT t.ticket_id,c.version_id,t.match_depth,t.contract_holder,t.cost_cents "
                    "FROM dwd_ticket t LEFT JOIN dim_contract c USING(contract_sk)"
                )
            }
            self.assertEqual(rows, expected)
            # The served customer is never rewritten into the holder.
            served = dict(db.execute("SELECT ticket_id,customer FROM dwd_ticket"))
            self.assertEqual(
                served,
                {
                    "T1": "S", "T2": "S", "T3": "S", "T4": "S", "T5": "S", "T6": "U",
                },
            )
            # Holder must match the selected dimension row exactly.
            mismatch = db.execute(
                "SELECT count(*) FROM dwd_ticket t JOIN dim_contract c USING(contract_sk) "
                "WHERE t.contract_holder <> c.customer"
            ).fetchone()[0]
            self.assertEqual(mismatch, 0)
            self.assertEqual(db.execute("PRAGMA foreign_key_check").fetchall(), [])

    def test_owner_ads_and_grand_totals(self):
        self.build()
        with closing(sqlite3.connect(self.db)) as db:
            owners = db.execute(
                "SELECT contract_holder,tier,tickets,cost_cents "
                "FROM ads_contract_owner_cost ORDER BY contract_holder,tier"
            ).fetchall()
            self.assertEqual(
                owners,
                [
                    (None, "uncovered", 1, 600),
                    ("D1", "basic", 1, 100),
                    ("D2", "basic", 1, 500),
                    ("H", "premium", 2, 700),
                    ("S", "basic", 1, 200),
                ],
            )
            totals = db.execute(
                "SELECT sum(tickets),sum(cost_cents) FROM ads_contract_owner_cost"
            ).fetchone()
            self.assertEqual(totals, (6, 2100))
            tier_totals = db.execute(
                "SELECT sum(tickets),sum(cost_cents) FROM ads_tier_cost"
            ).fetchone()
            self.assertEqual(tier_totals, (6, 2100))
            # ADS must be derivable once from the final facts, never by
            # expanding bridge/audit rows.
            fact_count = db.execute("SELECT count(*) FROM dwd_ticket").fetchone()[0]
            audit_count = db.execute("SELECT count(*) FROM audit_ticket_match").fetchone()[0]
            self.assertEqual(fact_count, 6)
            self.assertGreater(audit_count, fact_count)

    def test_bridge_keeps_both_s_to_h_paths_separately(self):
        self.build()
        with closing(sqlite3.connect(self.db)) as db:
            paths = db.execute(
                "SELECT depth,edge_path_json,valid_from,valid_to "
                "FROM bridge_hierarchy_ancestor WHERE descendant='S' AND ancestor='H' "
                "ORDER BY valid_from"
            ).fetchall()
            self.assertEqual(
                paths,
                [
                    (2, '["e1","e3"]', "2026-01-01", "2026-02-01"),
                    (2, '["e2","e4"]', "2026-02-01", None),
                ],
            )
            # Ordered edge detail connects every bridge step back to facts.
            detail = db.execute(
                "SELECT b.valid_from,p.step,p.edge_id,e.customer,e.parent "
                "FROM bridge_hierarchy_ancestor b "
                "JOIN bridge_hierarchy_path p USING(bridge_sk) "
                "JOIN dim_hierarchy_edge e USING(edge_id) "
                "WHERE b.descendant='S' AND b.ancestor='H' ORDER BY b.valid_from,p.step"
            ).fetchall()
            self.assertEqual(
                detail,
                [
                    ("2026-01-01", 1, "e1", "S", "D1"),
                    ("2026-01-01", 2, "e3", "D1", "H"),
                    ("2026-02-01", 1, "e2", "S", "D2"),
                    ("2026-02-01", 2, "e4", "D2", "H"),
                ],
            )
            # No self rows in the bridge.
            self.assertEqual(
                db.execute(
                    "SELECT count(*) FROM bridge_hierarchy_ancestor WHERE descendant=ancestor"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                db.execute("SELECT min(depth) FROM bridge_hierarchy_ancestor").fetchone()[0],
                1,
            )

    def test_audit_explains_every_skip_and_selection(self):
        self.build()
        with closing(sqlite3.connect(self.db)) as db:
            # T3 (2026-01-20): self S empty, D1's K1 has just expired and is
            # skipped rather than blocking the climb, H's K2 is selected.
            t3 = db.execute(
                "SELECT depth,customer,entry_edge_id,status,version_id "
                "FROM audit_ticket_match WHERE ticket_id='T3' ORDER BY depth"
            ).fetchall()
            self.assertEqual(
                t3,
                [
                    (0, "S", None, "no_active_contract", None),
                    (1, "D1", "e1", "no_active_contract", None),
                    (2, "H", "e3", "selected", "K2"),
                ],
            )
            # The skipped layer is verifiable against the contract dimension:
            # D1's only contract ended before the opened date (half-open end).
            self.assertEqual(
                db.execute(
                    "SELECT count(*) FROM dim_contract "
                    "WHERE customer='D1' AND valid_from<='2026-01-20' "
                    "AND (valid_to IS NULL OR '2026-01-20'<valid_to)"
                ).fetchone()[0],
                0,
            )
            # T4 (2026-02-01): migration happened; the chain goes via D2.
            t4 = db.execute(
                "SELECT depth,customer,entry_edge_id,status,version_id "
                "FROM audit_ticket_match WHERE ticket_id='T4' ORDER BY depth"
            ).fetchall()
            self.assertEqual(
                t4,
                [
                    (0, "S", None, "no_active_contract", None),
                    (1, "D2", "e2", "no_active_contract", None),
                    (2, "H", "e4", "selected", "K2"),
                ],
            )
            # The old e1 edge is no longer usable on 2026-02-01; e2 replaces it.
            bundle = load_inherited(self.source)
            self.assertEqual(
                active_parent_edge(bundle.edges, "S", date(2026, 1, 31)).edge_id, "e1"
            )
            feb1 = active_parent_edge(bundle.edges, "S", date(2026, 2, 1))
            self.assertEqual(feb1.edge_id, "e2")
            # Uncovered ticket audits through to the day's root.
            t6 = db.execute(
                "SELECT depth,customer,entry_edge_id,status "
                "FROM audit_ticket_match WHERE ticket_id='T6' ORDER BY depth"
            ).fetchall()
            self.assertEqual(t6, [(0, "U", None, "no_active_contract")])
            # Self-selection carries no entry edge.
            t2 = db.execute(
                "SELECT depth,entry_edge_id,status,version_id "
                "FROM audit_ticket_match WHERE ticket_id='T2'"
            ).fetchall()
            self.assertEqual(t2, [(0, None, "selected", "K3")])

    def test_near_layer_other_day_contract_does_not_block_and_no_premium_borrow(self):
        # 2026-01-10: S has no active contract (K3 starts later), so the climb
        # reaches D1's basic K1 even though H holds a premium K2.
        bundle = load_inherited(self.source)
        ticket = next(t for t in bundle.tickets if t.ticket_id == "T1")
        match = select_contract(bundle, ticket)
        self.assertEqual(match.contract.version_id, "K1")
        self.assertEqual(match.depth, 1)
        # Self basic wins over a farther premium: T2 keeps K3.
        ticket2 = next(t for t in bundle.tickets if t.ticket_id == "T2")
        self.assertEqual(select_contract(bundle, ticket2).contract.version_id, "K3")

    def test_old_parent_cannot_be_used_after_migration(self):
        # Remove the open-ended e2 so that after 2026-02-01 S has no parent at
        # all: the expired e1 chain must not be followed to H.
        hierarchy = HIERARCHY_HEADER + "e1,S,D1,2026-01-01,2026-02-01\n"
        write_sources(self.source, {"hierarchy.csv": hierarchy})
        inherited.build(self.source, self.db)
        with closing(sqlite3.connect(self.db)) as db:
            row = db.execute(
                "SELECT match_depth,contract_sk FROM dwd_ticket WHERE ticket_id='T4'"
            ).fetchone()
            self.assertEqual(row, (None, None))
            audit = db.execute(
                "SELECT depth,customer,status FROM audit_ticket_match "
                "WHERE ticket_id='T4' ORDER BY depth"
            ).fetchall()
            self.assertEqual(audit, [(0, "S", "no_active_contract")])


class TemporalValidationTest(unittest.TestCase):
    """Requirement 4: interval-based forest validation over full history."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.source = self.root / "input"
        self.db = self.root / "out.sqlite"

    # contracts.csv and tickets.csv must stay non-empty; validation-only
    # scenarios use a neutral registered root customer.
    def base(self, hierarchy: str, customers="A\nB\nC\nR\n",
             contracts="K0,R,2026-01-01,,basic\n",
             tickets="T0,R,2026-01-01,1\n"):
        write_sources(
            self.source,
            {
                "customers.csv": CUSTOMERS_HEADER + customers,
                "hierarchy.csv": HIERARCHY_HEADER + hierarchy,
                "contracts.csv": CONTRACTS_HEADER + contracts,
                "tickets.csv": TICKETS_HEADER + tickets,
            },
        )

    def test_opposite_edges_at_different_times_are_legal(self):
        self.base(
            "e1,A,B,2026-01-01,2026-02-01\n"
            "e2,B,A,2026-02-01,2026-03-01\n"
        )
        bundle = load_inherited(self.source)
        bridge = build_bridge(bundle.edges)
        intervals = {(r.descendant, r.ancestor): (r.valid_from, r.valid_to) for r in bridge}
        self.assertEqual(
            intervals[("A", "B")], (date(2026, 1, 1), date(2026, 2, 1))
        )
        self.assertEqual(
            intervals[("B", "A")], (date(2026, 2, 1), date(2026, 3, 1))
        )

    def test_simultaneously_valid_two_node_cycle_is_rejected_with_overlap_window(self):
        self.base(
            "e1,A,B,2026-01-01,2026-03-01\n"
            "e2,B,A,2026-02-01,2026-04-01\n"
        )
        with self.assertRaises(ValidationError) as ctx:
            load_inherited(self.source)
        message = str(ctx.exception)
        # Locate the exact non-empty intersection window, the node loop and
        # both conflicting edge facts with their source row numbers.
        self.assertRegex(message, r"2026-02-01.*2026-03-01")
        self.assertIn("A -> B -> A", message)
        self.assertIn("'e1'", message)
        self.assertIn("'e2'", message)
        self.assertIn("row 1", message)
        self.assertIn("row 2", message)

    def test_cycle_in_component_without_tickets_or_contracts_is_rejected(self):
        # The cycle lives in an isolated component (X/Y/Z) with no ticket and
        # no contract of its own; validation must still cover it.
        self.base(
            "e1,X,Y,2026-01-01,\n"
            "e2,Y,Z,2026-01-01,\n"
            "e3,Z,X,2026-01-01,\n",
            customers="A\nB\nC\nR\nX\nY\nZ\n",
        )
        with self.assertRaisesRegex(ValidationError, "cycle"):
            load_inherited(self.source)

    def test_open_ended_cycle_reports_open_overlap_window(self):
        self.base(
            "e1,A,B,2026-01-01,\n"
            "e2,B,A,2026-02-01,\n"
        )
        with self.assertRaisesRegex(ValidationError, r"2026-02-01.*open"):
            load_inherited(self.source)

    def test_self_reference_rejected(self):
        self.base("e1,A,A,2026-01-01,\n")
        with self.assertRaisesRegex(ValidationError, "self reference"):
            load_inherited(self.source)

    def test_overlapping_parents_same_parent_still_rejected(self):
        self.base(
            "e1,A,B,2026-01-01,2026-03-01\n"
            "e2,A,B,2026-02-01,\n"
        )
        with self.assertRaisesRegex(ValidationError, "overlapping parent edges"):
            load_inherited(self.source)

    def test_adjacent_parent_switch_and_gap_are_legal(self):
        self.base(
            "e1,A,B,2026-01-01,2026-02-01\n"
            "e2,A,C,2026-02-01,\n"
        )
        bundle = load_inherited(self.source)
        self.assertIsNotNone(active_parent_edge(bundle.edges, "A", date(2026, 1, 31)))
        self.assertEqual(
            active_parent_edge(bundle.edges, "A", date(2026, 2, 1)).edge_id, "e2"
        )

    def test_gap_means_root_during_gap(self):
        self.base("e1,A,B,2026-01-01,2026-01-10\ne2,A,B,2026-01-20,\n")
        bundle = load_inherited(self.source)
        self.assertIsNone(active_parent_edge(bundle.edges, "A", date(2026, 1, 15)))
        self.assertEqual(
            active_parent_edge(bundle.edges, "A", date(2026, 1, 20)).edge_id, "e2"
        )

    def test_empty_hierarchy_file_is_legal(self):
        self.base("")
        bundle = load_inherited(self.source)
        self.assertEqual(bundle.edges, [])
        self.assertEqual(build_bridge(bundle.edges), [])
        result = inherited.build(self.source, self.db)
        self.assertEqual(result["edges"], 0)
        self.assertEqual(result["bridge_paths"], 0)

    def test_open_ended_relationship_not_truncated(self):
        self.base("e1,A,B,2026-01-01,\n")
        bridge = build_bridge(load_inherited(self.source).edges)
        self.assertEqual(
            (bridge[0].valid_from, bridge[0].valid_to), (date(2026, 1, 1), None)
        )

    def test_leap_day_relationship_and_ticket(self):
        self.base(
            "e1,A,B,2028-02-28,\n",
            contracts="K1,B,2028-02-28,,basic\n",
            tickets="T1,A,2028-02-29,40\n",
        )
        inherited.build(self.source, self.db)
        with closing(sqlite3.connect(self.db)) as db:
            self.assertEqual(
                db.execute(
                    "SELECT c.version_id,t.match_depth FROM dwd_ticket t "
                    "JOIN dim_contract c USING(contract_sk)"
                ).fetchone(),
                ("K1", 1),
            )

    def test_unknown_endpoint_rejected(self):
        self.base("e1,A,Z,2026-01-01,\n")
        with self.assertRaisesRegex(ValidationError, "unknown customer"):
            load_inherited(self.source)

    def test_duplicate_edge_id_rejected(self):
        self.base(
            "e1,A,B,2026-01-01,2026-02-01\n"
            "e1,A,C,2026-03-01,\n"
        )
        with self.assertRaisesRegex(ValidationError, "duplicate edge_id"):
            load_inherited(self.source)

    def test_duplicate_customer_rejected(self):
        write_sources(
            self.source,
            {
                "customers.csv": CUSTOMERS_HEADER + "A\nA\n",
                "hierarchy.csv": HIERARCHY_HEADER,
                "contracts.csv": CONTRACTS_HEADER + "K1,A,2026-01-01,,basic\n",
                "tickets.csv": TICKETS_HEADER + "T1,A,2026-01-01,1\n",
            },
        )
        with self.assertRaisesRegex(ValidationError, "duplicate customer"):
            load_inherited(self.source)

    def test_unknown_contract_and_ticket_customers_rejected(self):
        write_sources(
            self.source,
            {
                "customers.csv": CUSTOMERS_HEADER + "A\n",
                "hierarchy.csv": HIERARCHY_HEADER,
                "contracts.csv": CONTRACTS_HEADER + "K1,Z,2026-01-01,,basic\n",
                "tickets.csv": TICKETS_HEADER + "T1,A,2026-01-01,1\n",
            },
        )
        with self.assertRaisesRegex(ValidationError, "unknown customer"):
            load_inherited(self.source)

    def test_bad_dates_and_intervals(self):
        cases = [
            "e1,A,B,2026-1-01,\n",
            "e1,A,B,2026-02-30,\n",
            "e1,A,B,2026-03-01,2026-03-01\n",
            "e1,A,B,2026-03-02,2026-03-01\n",
        ]
        for hierarchy in cases:
            with self.subTest(hierarchy=hierarchy):
                self.base(hierarchy)
                with self.assertRaises(ValidationError):
                    load_inherited(self.source)

    def test_whitespace_and_case_sensitive_identifiers(self):
        self.base("e1,A,B,2026-01-01,\n")
        # Case-sensitive: lowercase 'a' is a different, unregistered customer.
        write_sources(
            self.source,
            {"tickets.csv": TICKETS_HEADER + "T1,a,2026-01-01,1\n"},
        )
        with self.assertRaisesRegex(ValidationError, "unknown customer"):
            load_inherited(self.source)
        # Surrounding whitespace is rejected, never auto-trimmed.
        write_sources(
            self.source,
            {"tickets.csv": TICKETS_HEADER + "T1,A ,2026-01-01,1\n"},
        )
        with self.assertRaisesRegex(ValidationError, "whitespace"):
            load_inherited(self.source)

    def test_required_headers_and_non_empty_files(self):
        write_sources(
            self.source,
            {
                "customers.csv": "wrong\nA\n",
                "hierarchy.csv": HIERARCHY_HEADER,
                "contracts.csv": CONTRACTS_HEADER,
                "tickets.csv": TICKETS_HEADER,
            },
        )
        with self.assertRaisesRegex(ValidationError, "unexpected columns"):
            load_inherited(self.source)
        # contracts.csv must contain at least one data row.
        write_sources(
            self.source,
            {
                "customers.csv": CUSTOMERS_HEADER + "A\nB\n",
                "hierarchy.csv": HIERARCHY_HEADER,
                "contracts.csv": CONTRACTS_HEADER,
                "tickets.csv": TICKETS_HEADER + "T1,A,2026-01-01,1\n",
            },
        )
        with self.assertRaisesRegex(ValidationError, "at least one data row"):
            load_inherited(self.source)
        # tickets.csv must contain at least one data row as well.
        write_sources(
            self.source,
            {
                "customers.csv": CUSTOMERS_HEADER + "A\nB\n",
                "hierarchy.csv": HIERARCHY_HEADER,
                "contracts.csv": CONTRACTS_HEADER + "K1,A,2026-01-01,,basic\n",
                "tickets.csv": TICKETS_HEADER,
            },
        )
        with self.assertRaisesRegex(ValidationError, "at least one data row"):
            load_inherited(self.source)
        # hierarchy.csv is the only file allowed to contain only a header.
        write_sources(
            self.source,
            {
                "customers.csv": CUSTOMERS_HEADER + "A\nB\n",
                "hierarchy.csv": HIERARCHY_HEADER,
                "contracts.csv": CONTRACTS_HEADER + "K1,A,2026-01-01,,basic\n",
                "tickets.csv": TICKETS_HEADER + "T1,A,2026-01-01,1\n",
            },
        )
        self.assertEqual(load_inherited(self.source).edges, [])


class BridgeReferenceOracleTest(unittest.TestCase):
    """Requirement 5: independent day-by-day forest oracle on a small range."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.source = self.root / "input"
        # A small synthetic forest with migrations, gaps and open ends.
        write_sources(
            self.source,
            {
                "customers.csv": CUSTOMERS_HEADER + "A\nB\nC\nD\nR\n",
                "hierarchy.csv": (
                    HIERARCHY_HEADER
                    + "e1,A,B,2026-01-01,2026-01-10\n"
                    + "e2,A,C,2026-01-12,\n"
                    + "e3,B,R,2026-01-01,\n"
                    + "e4,C,D,2026-01-05,2026-01-20\n"
                    + "e5,D,R,2026-01-05,\n"
                ),
                "contracts.csv": CONTRACTS_HEADER + "K0,R,2026-01-01,,basic\n",
                "tickets.csv": TICKETS_HEADER + "T0,R,2026-01-01,1\n",
            },
        )
        self.bundle = load_inherited(self.source)
        self.start = date(2026, 1, 1)
        self.end = date(2026, 1, 25)

    def daily_forest(self, day):
        parent = {}
        edge_of = {}
        for edge in self.bundle.edges:
            if edge.valid_from <= day and (edge.valid_to is None or day < edge.valid_to):
                parent[edge.customer] = edge.parent
                edge_of[edge.customer] = edge.edge_id
        return parent, edge_of

    def oracle_paths(self, day):
        """Walk each node's unique parent chain for one day (test only)."""
        parent, edge_of = self.daily_forest(day)
        found = set()
        for node in parent:
            current = node
            depth = 0
            path = []
            seen = set()
            while current in parent:
                self.assertNotIn(current, seen)  # oracle assumes a forest
                seen.add(current)
                depth += 1
                path.append(edge_of[current])
                current = parent[current]
                found.add((node, current, depth, tuple(path)))
        return found

    def test_bridge_matches_daily_oracle_every_day(self):
        bridge = build_bridge(self.bundle.edges)
        for day in daterange(self.start, self.end):
            expected = self.oracle_paths(day)
            actual = {
                (row.descendant, row.ancestor, row.depth, tuple(row.edge_path))
                for row in bridge
                if row.valid_from <= day and (row.valid_to is None or day < row.valid_to)
            }
            self.assertEqual(actual, expected, f"mismatch on {day}")

    def test_bridge_intervals_are_maximal_and_path_distinguishing(self):
        bridge = build_bridge(self.bundle.edges)
        # A->R exists through two different complete paths; they stay split.
        a_r = [row for row in bridge if row.descendant == "A" and row.ancestor == "R"]
        self.assertEqual(
            sorted((tuple(r.edge_path), r.valid_from, r.valid_to, r.depth) for r in a_r),
            [
                (("e1", "e3"), date(2026, 1, 1), date(2026, 1, 10), 2),
                (("e2", "e4", "e5"), date(2026, 1, 12), date(2026, 1, 20), 3),
            ],
        )
        # After e4 ends, A no longer reaches D or R on the new chain;
        # only the still-open edge to C remains.
        active_on_21 = [
            row for row in bridge
            if row.descendant == "A" and row.valid_from <= date(2026, 1, 21)
            and (row.valid_to is None or date(2026, 1, 21) < row.valid_to)
        ]
        self.assertEqual(
            sorted((r.ancestor, r.depth) for r in active_on_21),
            [("C", 1)],
        )

    def test_same_complete_path_is_kept_with_disjoint_maximal_intervals(self):
        # The same ordered edge path produced via disjoint timeline segments
        # must not be duplicated: one row per distinct (descendant, ancestor,
        # depth, edge path), and its stored intervals must be disjoint and
        # half-open with a strictly positive length.
        bridge = build_bridge(self.bundle.edges)
        grouped: dict = {}
        for row in bridge:
            key = (row.descendant, row.ancestor, row.depth, row.edge_path)
            grouped.setdefault(key, []).append((row.valid_from, row.valid_to))
        for key, intervals in grouped.items():
            self.assertEqual(len(intervals), len(set(intervals)), key)
            for start, end in intervals:
                if end is not None:
                    self.assertLess(start, end, key)
            for (_, prev_end), (next_start, _) in zip(intervals, intervals[1:]):
                self.assertIsNotNone(prev_end, key)
                self.assertLessEqual(prev_end, next_start, key)

    def test_nearest_contract_selection_oracle(self):
        # Independent re-implementation of the nearest-contract rule.
        contracts = [
            ("A", date(2026, 1, 12), None, "KA"),
            ("R", date(2026, 1, 1), None, "KR"),
        ]
        write_sources(
            self.source,
            {
                "contracts.csv": (
                    CONTRACTS_HEADER
                    + "KA,A,2026-01-12,,basic\n"
                    + "KR,R,2026-01-01,,premium\n"
                ),
                "tickets.csv": (
                    TICKETS_HEADER
                    + "T1,A,2026-01-08,10\n"
                    + "T2,A,2026-01-11,10\n"
                    + "T3,A,2026-01-12,10\n"
                ),
            },
        )
        bundle = load_inherited(self.source)

        def oracle(customer, day):
            chain = [customer]
            parent, edge_of = self.daily_forest(day)
            current = customer
            while current in parent:
                current = parent[current]
                chain.append(current)
            for depth, holder in enumerate(chain):
                for owner, start, end, version in contracts:
                    if owner == holder and start <= day and (end is None or day < end):
                        return version, depth, holder
            return None, None, None

        for ticket in bundle.tickets:
            match = select_contract(bundle, ticket)
            expected = oracle(ticket.customer, ticket.opened)
            got = (
                match.contract.version_id if match.contract else None,
                match.depth,
                match.holder,
            )
            self.assertEqual(got, expected, ticket.ticket_id)
        # 2026-01-11 is inside the parent-gap: A is its own root and uncovered.
        gap_ticket = next(t for t in bundle.tickets if t.ticket_id == "T2")
        self.assertIsNone(select_contract(bundle, gap_ticket).contract)


class ShuffleTest(unittest.TestCase):
    """Requirement 5: row order must not change business outputs."""

    def test_shuffled_csv_rows_keep_logic_but_move_source_positions(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        src_a = root / "a"
        src_b = root / "b"
        inherited_sources(src_a)

        def split(text):
            lines = text.splitlines(keepends=True)
            return lines[0], lines[1:]

        shuffled = {}
        for name in ("customers.csv", "hierarchy.csv", "contracts.csv", "tickets.csv"):
            header, body = split((V2_FIX / name).read_text(encoding="utf-8"))
            shuffled[name] = header + "".join(reversed(body))
        write_sources(src_b, shuffled)

        db_a, db_b = root / "a.sqlite", root / "b.sqlite"
        inherited.build(src_a, db_a)
        inherited.build(src_b, db_b)

        # Business content excluding the 1-based source locators, which must
        # truthfully move when the input rows move.
        projections = {
            "dim_customer": "SELECT customer FROM dim_customer ORDER BY 1",
            "dim_hierarchy_edge": (
                "SELECT edge_id,customer,parent,valid_from,valid_to "
                "FROM dim_hierarchy_edge ORDER BY 1"
            ),
            "dim_contract": (
                "SELECT version_id,customer,valid_from,valid_to,tier "
                "FROM dim_contract ORDER BY 1"
            ),
            "bridge_hierarchy_ancestor": (
                "SELECT descendant,ancestor,depth,edge_path_json,valid_from,valid_to "
                "FROM bridge_hierarchy_ancestor ORDER BY 1,2,3,4,5,6"
            ),
            "bridge_hierarchy_path": (
                "SELECT bridge_sk,step,edge_id FROM bridge_hierarchy_path ORDER BY 1,2"
            ),
            "dwd_ticket": (
                "SELECT ticket_id,customer,opened,cost_cents,match_depth,"
                "contract_sk,contract_holder FROM dwd_ticket ORDER BY 1"
            ),
            "audit_ticket_match": (
                "SELECT ticket_id,depth,customer,entry_edge_id,status,contract_sk,"
                "version_id,tier FROM audit_ticket_match ORDER BY 1,2"
            ),
            "ads_tier_cost": "SELECT tier,tickets,cost_cents FROM ads_tier_cost ORDER BY 1",
            "ads_contract_owner_cost": (
                "SELECT contract_holder,tier,tickets,cost_cents "
                "FROM ads_contract_owner_cost ORDER BY 1,2"
            ),
        }
        with closing(sqlite3.connect(db_a)) as da, closing(sqlite3.connect(db_b)) as db:
            for label, query in projections.items():
                self.assertEqual(da.execute(query).fetchall(), db.execute(query).fetchall(), label)
            # Source positions truthfully reflect the new order.
            pos_e4 = db.execute(
                "SELECT source_row_number FROM dim_hierarchy_edge WHERE edge_id='e4'"
            ).fetchone()[0]
            pos_e1 = db.execute(
                "SELECT source_row_number FROM dim_hierarchy_edge WHERE edge_id='e1'"
            ).fetchone()[0]
            self.assertLess(pos_e4, pos_e1)
            # ODS payload order follows the shuffled file as well.
            first_edge = json.loads(
                db.execute("SELECT payload FROM ods_hierarchy WHERE row_number=1").fetchone()[0]
            )
            self.assertEqual(first_edge["edge_id"], "e4")


class ProvenanceTest(unittest.TestCase):
    """Requirement: one captured byte stream, stored raw bytes, re-hashable."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.source = self.root / "input"
        self.db = self.root / "warehouse.sqlite"
        inherited_sources(self.source)

    def test_raw_bytes_hashes_and_row_locators(self):
        inherited.build(self.source, self.db)
        with closing(sqlite3.connect(self.db)) as db:
            stored = {
                role: (name, digest, size, raw)
                for role, name, digest, size, raw in db.execute(
                    "SELECT source_role,file_name,sha256,byte_length,raw_bytes FROM ods_source_file"
                )
            }
            self.assertEqual(
                sorted(stored), ["contracts", "customers", "hierarchy", "tickets"]
            )
            for role, (name, digest, size, raw) in stored.items():
                original = (self.source / name).read_bytes()
                self.assertEqual(raw, original)
                self.assertEqual(hashlib.sha256(original).hexdigest(), digest)
                self.assertEqual(size, len(original))
            # Dimension/fact rows carry 1-based source record numbers.
            self.assertEqual(
                db.execute(
                    "SELECT source_row_number FROM dim_contract WHERE version_id='K2'"
                ).fetchone()[0],
                2,
            )
            self.assertEqual(
                db.execute(
                    "SELECT source_row_number FROM dim_hierarchy_edge WHERE edge_id='e3'"
                ).fetchone()[0],
                3,
            )
            # Audit selection evidence points back at the contract source row.
            self.assertEqual(
                db.execute(
                    "SELECT source_contract_row FROM audit_ticket_match "
                    "WHERE ticket_id='T3' AND status='selected'"
                ).fetchone()[0],
                2,
            )

    def test_provenance_survives_input_removal(self):
        inherited.build(self.source, self.db)
        removed = self.root / "moved_input"
        shutil.move(str(self.source), removed)
        self.assertFalse(self.source.exists())
        with closing(sqlite3.connect(self.db)) as db:
            for role, name, digest, size, raw in db.execute(
                "SELECT source_role,file_name,sha256,byte_length,raw_bytes FROM ods_source_file"
            ):
                self.assertEqual(hashlib.sha256(bytes(raw)).hexdigest(), digest)
                self.assertEqual(len(bytes(raw)), size)
            # Path evidence still resolves to the stored relationship facts.
            broken = db.execute(
                "SELECT count(*) FROM bridge_hierarchy_path p "
                "LEFT JOIN dim_hierarchy_edge e USING(edge_id) WHERE e.edge_id IS NULL"
            ).fetchone()[0]
            self.assertEqual(broken, 0)
            rows = db.execute(
                "SELECT b.edge_path_json,group_concat(p.edge_id,',') "
                "FROM bridge_hierarchy_ancestor b JOIN bridge_hierarchy_path p USING(bridge_sk) "
                "GROUP BY b.bridge_sk ORDER BY b.bridge_sk"
            ).fetchall()
            for path_json, detail in rows:
                self.assertEqual(",".join(json.loads(path_json)), detail)


class CliSubprocessTest(unittest.TestCase):
    """Requirement 6: real CLI subprocess end-to-end on four CSVs."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.source = self.root / "input"
        self.db = self.root / "warehouse.sqlite"
        inherited_sources(self.source)

    def run_cli(self, source, database):
        return subprocess.run(
            [sys.executable, "-m", "contract_warehouse.inherited",
             "--source", str(source), "--database", str(database)],
            cwd=SRC_DIR,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_cli_builds_and_queries_work(self):
        proc = self.run_cli(self.source, self.db)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["schema"], "inherited_contracts_v2")
        with closing(sqlite3.connect(self.db)) as db:
            self.assertEqual(
                db.execute("SELECT count(*) FROM bridge_hierarchy_ancestor").fetchone()[0], 6
            )
            self.assertEqual(
                db.execute("SELECT count(*) FROM audit_ticket_match").fetchone()[0], 12
            )
            self.assertEqual(
                db.execute("SELECT count(*) FROM dwd_ticket").fetchone()[0], 6
            )
            self.assertEqual(
                db.execute(
                    "SELECT tickets,cost_cents FROM ads_tier_cost WHERE tier='premium'"
                ).fetchone(),
                (2, 700),
            )
            self.assertEqual(
                db.execute(
                    "SELECT tickets,cost_cents FROM ads_contract_owner_cost "
                    "WHERE contract_holder='H'"
                ).fetchone(),
                (2, 700),
            )

    def test_cli_failure_is_nonzero_with_message(self):
        write_sources(
            self.source,
            {"hierarchy.csv": HIERARCHY_HEADER + "e1,S,S,2026-01-01,\n"},
        )
        proc = self.run_cli(self.source, self.db)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("self reference", proc.stderr)
        self.assertFalse(self.db.exists())

    def test_output_cannot_target_input_file(self):
        proc = self.run_cli(self.source, self.source / "tickets.csv")
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("must not point at an input file", proc.stderr)
        # Input file untouched.
        self.assertTrue((self.source / "tickets.csv").read_bytes())


class FailureAndRerunTest(unittest.TestCase):
    """Single-writer boundary: failures keep the old database byte-identical."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.source = self.root / "input"
        self.db = self.root / "warehouse.sqlite"
        inherited_sources(self.source)
        inherited.build(self.source, self.db)
        self.before = self.db.read_bytes()

    def temp_files(self):
        return list(self.db.parent.glob(".inherited-warehouse-*.tmp"))

    def test_bridge_write_failure_preserves_old_database(self):
        def boom(db, bridge):
            raise sqlite3.OperationalError("injected bridge failure")

        with mock.patch.object(inherited, "_insert_bridge", boom):
            with self.assertRaises(sqlite3.OperationalError):
                inherited.build(self.source, self.db)
        self.assertEqual(self.db.read_bytes(), self.before)
        self.assertEqual(self.temp_files(), [])
        # Rerun after fixing the injection succeeds.
        inherited.build(self.source, self.db)
        self.assertTrue(self.db.read_bytes())

    def test_ticket_write_failure_preserves_old_database(self):
        def boom(db, ticket, match, contract_sk):
            raise sqlite3.OperationalError("injected ticket failure")

        with mock.patch.object(inherited, "_write_ticket", boom):
            with self.assertRaises(sqlite3.OperationalError):
                inherited.build(self.source, self.db)
        self.assertEqual(self.db.read_bytes(), self.before)
        self.assertEqual(self.temp_files(), [])
        inherited.build(self.source, self.db)

    def test_foreign_key_check_failure_preserves_old_database(self):
        def boom(db):
            raise sqlite3.IntegrityError("injected fk failure")

        with mock.patch.object(inherited, "_check_foreign_keys", boom):
            with self.assertRaises(sqlite3.IntegrityError):
                inherited.build(self.source, self.db)
        self.assertEqual(self.db.read_bytes(), self.before)
        self.assertEqual(self.temp_files(), [])
        inherited.build(self.source, self.db)

    def test_replace_failure_preserves_old_database(self):
        real_replace = os.replace

        def boom(src, dst):
            if Path(dst) == self.db:
                raise OSError("injected replace failure")
            return real_replace(src, dst)

        with mock.patch("contract_warehouse.inherited.os.replace", boom):
            with self.assertRaises(OSError):
                inherited.build(self.source, self.db)
        self.assertEqual(self.db.read_bytes(), self.before)
        self.assertEqual(self.temp_files(), [])
        inherited.build(self.source, self.db)

    def test_validation_failure_leaves_no_artifact(self):
        write_sources(
            self.source,
            {"tickets.csv": TICKETS_HEADER + "T9,A,2026-02-30,1\n"},
        )
        with self.assertRaises(ValidationError):
            inherited.build(self.source, self.db)
        self.assertEqual(self.db.read_bytes(), self.before)
        self.assertEqual(self.temp_files(), [])

    def test_deterministic_rerun(self):
        first = inherited.build(self.source, self.db)
        with closing(sqlite3.connect(self.db)) as db:
            snapshot = {
                table: ordered_dump(db, table)
                for table in (
                    "dim_customer", "dim_hierarchy_edge", "dim_contract",
                    "bridge_hierarchy_ancestor", "bridge_hierarchy_path",
                    "dwd_ticket", "audit_ticket_match",
                    "ads_tier_cost", "ads_contract_owner_cost",
                    "ods_source_file", "ods_customers", "ods_hierarchy",
                    "ods_contracts", "ods_tickets",
                )
            }
        second = inherited.build(self.source, self.db)
        self.assertEqual(first, second)
        with closing(sqlite3.connect(self.db)) as db:
            for table, rows in snapshot.items():
                self.assertEqual(ordered_dump(db, table), rows, table)

    def test_integer_overflow_fails_without_float_coercion(self):
        huge = "9" * 19  # 10^19 - 1 > signed 64-bit max
        write_sources(
            self.source,
            {"tickets.csv": TICKETS_HEADER + f"T9,U,2026-01-10,{huge}\n"},
        )
        with self.assertRaises(ValidationError):
            inherited.build(self.source, self.db)
        self.assertEqual(self.db.read_bytes(), self.before)
        # Aggregation overflow path: individual values fit but their sum does not.
        write_sources(
            self.source,
            {
                "tickets.csv": (
                    TICKETS_HEADER
                    + "T1,S,2026-01-15,9223372036854775000\n"
                    + "T2,S,2026-01-15,9223372036854775000\n"
                ),
            },
        )
        with self.assertRaises(OverflowError):
            inherited.build(self.source, self.db)
        self.assertEqual(self.db.read_bytes(), self.before)
        self.assertEqual(self.temp_files(), [])


class BaselineCompatibilityTest(unittest.TestCase):
    """Requirement 1: the original two-file mode is untouched."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.source = self.root / "input"
        shutil.copytree(V1_FIX, self.source)
        self.db = self.root / "legacy.sqlite"

    def test_legacy_baseline_numbers_and_rerun(self):
        result = build_v1(self.source, self.db)
        self.assertEqual(
            result["tiers"],
            [
                {"tier": "basic", "tickets": 2, "cost_cents": 13000},
                {"tier": "premium", "tickets": 1, "cost_cents": 20000},
                {"tier": "uncovered", "tickets": 2, "cost_cents": 12000},
            ],
        )
        self.assertEqual(build_v1(self.source, self.db), result)
        with closing(sqlite3.connect(self.db)) as db:
            # Legacy database has no v2 marker and no v2 tables.
            self.assertEqual(
                db.execute("SELECT count(*) FROM sqlite_master WHERE name='wh_meta'").fetchone()[0],
                0,
            )
            self.assertEqual(
                db.execute(
                    "SELECT count(*) FROM sqlite_master WHERE name='bridge_hierarchy_ancestor'"
                ).fetchone()[0],
                0,
            )

    def test_legacy_failure_preserves_database(self):
        build_v1(self.source, self.db)
        before = self.db.read_bytes()
        with (self.source / "tickets.csv").open("a") as handle:
            handle.write("T9,C1,2026-02-30,10\n")
        with self.assertRaises(ValueError):
            build_v1(self.source, self.db)
        self.assertEqual(self.db.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
