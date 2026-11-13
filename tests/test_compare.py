"""Tests for the two-period customer-mix cost attribution report.

Covers the headline acceptance decomposition, exclusive customers and tier
splitting, the v2 shared-contract mode, edge cases (zero cost, empty
periods, disjoint supports, single common customer, uncovered), period
validation and A/B swap symmetry, determinism, 64-bit-overflow-safe exact
arithmetic, corrupted/unsupported inputs, single-snapshot reads under a
concurrent commit, an independent Fraction oracle, output-atomicity
failure injection and real CLI subprocess chains.
"""

from contextlib import closing
from datetime import date
from fractions import Fraction
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from contract_warehouse import compare as compare_mod
from contract_warehouse.compare import CompareError, compare
from contract_warehouse.build import build as build_v1
from contract_warehouse import inherited

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
V1_FIX = ROOT / "fixtures"
COMPARE_FIX = ROOT / "fixtures_compare"

CONTRACTS_HEADER = "version_id,customer,valid_from,valid_to,tier\n"
TICKETS_HEADER = "ticket_id,customer,opened,cost_cents\n"
CUSTOMERS_HEADER = "customer\n"
HIERARCHY_HEADER = "edge_id,customer,parent,valid_from,valid_to\n"

A_START, A_END = "2026-01-01", "2026-02-01"
B_START, B_END = "2026-03-01", "2026-04-01"

HEADLINE_CONTRACTS = (
    "CX,X,2026-01-01,2026-04-01,basic\n"
    "CY,Y,2026-01-01,2026-04-01,basic\n"
)
HEADLINE_TICKETS = (
    "A1,X,2026-01-10,100\n"
    "A2,X,2026-01-20,100\n"
    "A3,Y,2026-01-15,300\n"
    "B1,X,2026-03-10,200\n"
    "B2,Y,2026-03-05,400\n"
    "B3,Y,2026-03-20,400\n"
)

# Hand-computed expectation for the headline scenario (acceptance item 1):
# common-support means 500/3 and 1000/3, difference 500/3 = 100/1 + 200/3.
EXPECTED_HEADLINE_ALL = {
    "periods": {
        "a": {"tickets": 3, "cost_cents": 500, "mean_cost_cents": "500/3"},
        "b": {"tickets": 3, "cost_cents": 1000, "mean_cost_cents": "1000/3"},
    },
    "common_support": {
        "customers": 2,
        "coverage": {
            "a": {
                "tickets": 3, "cost_cents": 500,
                "ticket_share": "1/1", "cost_share": "1/1",
            },
            "b": {
                "tickets": 3, "cost_cents": 1000,
                "ticket_share": "1/1", "cost_share": "1/1",
            },
        },
        "attribution": {
            "status": "comparable",
            "mean_cost_cents": {"a": "500/3", "b": "1000/3"},
            "difference_cents": "500/3",
            "cost_change_cents": "100/1",
            "composition_cents": "200/3",
        },
        "customers": [
            {
                "customer": "X",
                "a": {"tickets": 2, "cost_cents": 200,
                      "mean_cost_cents": "100/1", "ticket_share": "2/3"},
                "b": {"tickets": 1, "cost_cents": 200,
                      "mean_cost_cents": "200/1", "ticket_share": "1/3"},
                "cost_change_contribution_cents": "50/1",
                "composition_contribution_cents": "-50/1",
            },
            {
                "customer": "Y",
                "a": {"tickets": 1, "cost_cents": 300,
                      "mean_cost_cents": "300/1", "ticket_share": "1/3"},
                "b": {"tickets": 2, "cost_cents": 800,
                      "mean_cost_cents": "400/1", "ticket_share": "2/3"},
                "cost_change_contribution_cents": "50/1",
                "composition_contribution_cents": "350/3",
            },
        ],
    },
    "exclusive": {
        "a_only": {"customers": 0, "tickets": 0, "cost_cents": 0, "detail": []},
        "b_only": {"customers": 0, "tickets": 0, "cost_cents": 0, "detail": []},
    },
}

EMPTY_GROUP = {
    "periods": {
        "a": {"tickets": 0, "cost_cents": 0, "mean_cost_cents": None},
        "b": {"tickets": 0, "cost_cents": 0, "mean_cost_cents": None},
    },
    "common_support": {
        "customers": 0,
        "coverage": {
            "a": {"tickets": 0, "cost_cents": 0,
                  "ticket_share": None, "cost_share": None},
            "b": {"tickets": 0, "cost_cents": 0,
                  "ticket_share": None, "cost_share": None},
        },
        "attribution": {
            "status": "insufficient_common_support",
            "mean_cost_cents": {"a": None, "b": None},
            "difference_cents": None,
            "cost_change_cents": None,
            "composition_cents": None,
        },
        "customers": [],
    },
    "exclusive": {
        "a_only": {"customers": 0, "tickets": 0, "cost_cents": 0, "detail": []},
        "b_only": {"customers": 0, "tickets": 0, "cost_cents": 0, "detail": []},
    },
}


def rat(value):
    return None if value is None else f"{value.numerator}/{value.denominator}"


def frac(text):
    return None if text is None else Fraction(text)


def write_texts(directory: Path, mapping: dict[str, str]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for name, content in mapping.items():
        (directory / name).write_text(content, encoding="utf-8")


def v1_sources(directory: Path, contracts: str, tickets: str) -> None:
    write_texts(
        directory,
        {
            "contracts.csv": CONTRACTS_HEADER + contracts,
            "tickets.csv": TICKETS_HEADER + tickets,
        },
    )


def v2_sources(directory: Path, customers: str, hierarchy: str,
               contracts: str, tickets: str) -> None:
    write_texts(
        directory,
        {
            "customers.csv": CUSTOMERS_HEADER + customers,
            "hierarchy.csv": HIERARCHY_HEADER + hierarchy,
            "contracts.csv": CONTRACTS_HEADER + contracts,
            "tickets.csv": TICKETS_HEADER + tickets,
        },
    )


def digest_of(evidence_tickets) -> str:
    canonical = json.dumps(
        evidence_tickets, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")
    return hashlib.sha256(canonical).hexdigest()


def oracle_group(evidence_tickets, tier):
    """Independent spec re-implementation over the report's own evidence."""
    selected = [
        t for t in evidence_tickets if tier == "all" or t["tier"] == tier
    ]
    rolls = {"a": {}, "b": {}}
    for ticket in selected:
        roll = rolls[ticket["period"]].setdefault(ticket["customer"], [0, 0])
        roll[0] += 1
        roll[1] += ticket["cost_cents"]
    periods = {}
    for label in ("a", "b"):
        count = sum(v[0] for v in rolls[label].values())
        cost = sum(v[1] for v in rolls[label].values())
        periods[label] = {
            "tickets": count,
            "cost_cents": cost,
            "mean_cost_cents": rat(Fraction(cost, count)) if count else None,
        }
    common = sorted(set(rolls["a"]) & set(rolls["b"]))
    only_a = sorted(set(rolls["a"]) - set(rolls["b"]))
    only_b = sorted(set(rolls["b"]) - set(rolls["a"]))
    coverage = {}
    commons = {}
    for label in ("a", "b"):
        count = sum(rolls[label][c][0] for c in common)
        cost = sum(rolls[label][c][1] for c in common)
        commons[label] = (count, cost)
        coverage[label] = {
            "tickets": count,
            "cost_cents": cost,
            "ticket_share": rat(Fraction(count, periods[label]["tickets"]))
            if periods[label]["tickets"] else None,
            "cost_share": rat(Fraction(cost, periods[label]["cost_cents"]))
            if periods[label]["cost_cents"] else None,
        }
    if common:
        (n_a, c_a), (n_b, c_b) = commons["a"], commons["b"]
        mean_a, mean_b = Fraction(c_a, n_a), Fraction(c_b, n_b)
        cost_change, composition = Fraction(0), Fraction(0)
        rows = []
        for customer in common:
            na, ca = rolls["a"][customer]
            nb, cb = rolls["b"][customer]
            ma, mb = Fraction(ca, na), Fraction(cb, nb)
            pa, pb = Fraction(na, n_a), Fraction(nb, n_b)
            change = (pa + pb) * (mb - ma) / 2
            mix = (pb - pa) * (ma + mb) / 2
            cost_change += change
            composition += mix
            rows.append({
                "customer": customer,
                "a": {"tickets": na, "cost_cents": ca,
                      "mean_cost_cents": rat(ma), "ticket_share": rat(pa)},
                "b": {"tickets": nb, "cost_cents": cb,
                      "mean_cost_cents": rat(mb), "ticket_share": rat(pb)},
                "cost_change_contribution_cents": rat(change),
                "composition_contribution_cents": rat(mix),
            })
        attribution = {
            "status": "comparable",
            "mean_cost_cents": {"a": rat(mean_a), "b": rat(mean_b)},
            "difference_cents": rat(mean_b - mean_a),
            "cost_change_cents": rat(cost_change),
            "composition_cents": rat(composition),
        }
    else:
        rows = []
        attribution = {
            "status": "insufficient_common_support",
            "mean_cost_cents": {"a": None, "b": None},
            "difference_cents": None,
            "cost_change_cents": None,
            "composition_cents": None,
        }

    def exclusive(customers, label):
        return {
            "customers": len(customers),
            "tickets": sum(rolls[label][c][0] for c in customers),
            "cost_cents": sum(rolls[label][c][1] for c in customers),
            "detail": [
                {"customer": c, "tickets": rolls[label][c][0],
                 "cost_cents": rolls[label][c][1]}
                for c in customers
            ],
        }

    return {
        "periods": periods,
        "common_support": {
            "customers": len(common),
            "coverage": coverage,
            "attribution": attribution,
            "customers": rows,
        },
        "exclusive": {
            "a_only": exclusive(only_a, "a"),
            "b_only": exclusive(only_b, "b"),
        },
    }


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.source = self.root / "input"
        self.db = self.root / "warehouse.sqlite"
        self.output = self.root / "report.json"

    def build_headline(self):
        v1_sources(self.source, HEADLINE_CONTRACTS, HEADLINE_TICKETS)
        build_v1(self.source, self.db)

    def run_compare(self, a_start=A_START, a_end=A_END,
                    b_start=B_START, b_end=B_END, output=None):
        return compare(
            self.db, a_start, a_end, b_start, b_end,
            output if output is not None else self.output,
        )

    def temp_files(self):
        return list(self.output.parent.glob(".compare-report-*.tmp"))


class HeadlineScenarioTest(Base):
    """Acceptance 1: X/Y common support, exact Bennet decomposition."""

    def setUp(self):
        super().setUp()
        self.build_headline()

    def test_exact_decomposition_all_and_basic(self):
        report = self.run_compare()
        self.assertEqual(report["schema_version"], "compare_report_v1")
        self.assertEqual(report["input"]["database_schema"], "baseline_v1")
        self.assertEqual(
            report["input"]["periods"],
            {"a": {"start": A_START, "end": A_END},
             "b": {"start": B_START, "end": B_END}},
        )
        groups = report["groups"]
        self.assertEqual(sorted(groups), ["all", "basic", "premium", "uncovered"])
        # The decomposition is over means, never over total cost differences.
        self.assertEqual(groups["all"], EXPECTED_HEADLINE_ALL)
        self.assertEqual(groups["basic"], EXPECTED_HEADLINE_ALL)
        self.assertEqual(groups["premium"], EMPTY_GROUP)
        self.assertEqual(groups["uncovered"], EMPTY_GROUP)

    def test_closure_and_evidence_digest(self):
        report = self.run_compare()
        attribution = report["groups"]["all"]["common_support"]["attribution"]
        self.assertEqual(
            frac(attribution["difference_cents"]),
            frac(attribution["cost_change_cents"])
            + frac(attribution["composition_cents"]),
        )
        rows = report["groups"]["all"]["common_support"]["customers"]
        self.assertEqual(
            sum(frac(r["cost_change_contribution_cents"]) for r in rows),
            frac(attribution["cost_change_cents"]),
        )
        self.assertEqual(
            sum(frac(r["composition_contribution_cents"]) for r in rows),
            frac(attribution["composition_cents"]),
        )
        # Non-integer contributions stay exact rationals, never floats.
        self.assertEqual(rows[1]["composition_contribution_cents"], "350/3")
        evidence = report["evidence"]["tickets"]
        self.assertEqual([t["ticket_id"] for t in evidence],
                         ["A1", "A2", "A3", "B1", "B2", "B3"])
        self.assertEqual(report["evidence"]["sha256"], digest_of(evidence))
        # v1 saves no inheritance depth; the contract version and holder
        # (the contract's own customer) are still reported from dim_contract.
        a1 = evidence[0]
        self.assertEqual(
            a1,
            {"ticket_id": "A1", "customer": "X", "opened": "2026-01-10",
             "cost_cents": 100, "period": "a", "tier": "basic",
             "version_id": "CX", "contract_holder": "X", "match_depth": None},
        )

    def test_report_file_is_deterministic_json(self):
        self.run_compare()
        first = self.output.read_bytes()
        self.run_compare()
        self.assertEqual(first, self.output.read_bytes())
        payload = json.loads(first.decode("utf-8"))
        self.assertEqual(payload["schema_version"], "compare_report_v1")
        self.assertNotIn(str(self.root), first.decode("utf-8"))


class ExclusiveAndTierSplitTest(Base):
    """Acceptance 2: exclusive customers and tier-specific supports."""

    def setUp(self):
        super().setUp()
        contracts = HEADLINE_CONTRACTS + (
            "CL,L,2026-01-01,2026-02-01,basic\n"
            "CN,N,2026-03-01,2026-04-01,basic\n"
            "CP,P,2026-01-01,2026-04-01,premium\n"
        )
        tickets = HEADLINE_TICKETS + (
            "A4,L,2026-01-05,900\n"
            "B4,N,2026-03-02,50\n"
            "B5,N,2026-03-25,50\n"
            "P1,P,2026-01-12,1000\n"
            "P2,P,2026-03-12,1200\n"
        )
        v1_sources(self.source, contracts, tickets)
        build_v1(self.source, self.db)
        self.report = self.run_compare()

    def test_common_support_decomposition_unchanged(self):
        basic = self.report["groups"]["basic"]
        # L and N join neither side of the common support: the attribution
        # block is exactly the headline one, and so is the whole basic group
        # except the period totals, coverage and exclusive diagnostics.
        self.assertEqual(
            basic["common_support"]["attribution"],
            EXPECTED_HEADLINE_ALL["common_support"]["attribution"],
        )
        self.assertEqual(
            basic["common_support"]["customers"],
            EXPECTED_HEADLINE_ALL["common_support"]["customers"],
        )
        self.assertEqual(
            basic["periods"],
            {
                "a": {"tickets": 4, "cost_cents": 1400, "mean_cost_cents": "350/1"},
                "b": {"tickets": 5, "cost_cents": 1100, "mean_cost_cents": "220/1"},
            },
        )
        self.assertEqual(
            basic["common_support"]["coverage"],
            {
                "a": {"tickets": 3, "cost_cents": 500,
                      "ticket_share": "3/4", "cost_share": "5/14"},
                "b": {"tickets": 3, "cost_cents": 1000,
                      "ticket_share": "3/5", "cost_share": "10/11"},
            },
        )
        self.assertEqual(
            basic["exclusive"],
            {
                "a_only": {"customers": 1, "tickets": 1, "cost_cents": 900,
                           "detail": [{"customer": "L", "tickets": 1,
                                       "cost_cents": 900}]},
                "b_only": {"customers": 1, "tickets": 2, "cost_cents": 100,
                           "detail": [{"customer": "N", "tickets": 2,
                                       "cost_cents": 100}]},
            },
        )

    def test_overall_mean_difference_is_not_the_common_support_one(self):
        all_group = self.report["groups"]["all"]
        # Overall means move with L/N/P in the population...
        self.assertEqual(all_group["periods"]["a"]["mean_cost_cents"], "480/1")
        self.assertEqual(all_group["periods"]["b"]["mean_cost_cents"], "1150/3")
        overall_diff = (
            frac(all_group["periods"]["b"]["mean_cost_cents"])
            - frac(all_group["periods"]["a"]["mean_cost_cents"])
        )
        self.assertEqual(overall_diff, Fraction(-290, 3))
        # ...while the common-support decomposition explains 175/1 only;
        # the report never equates the two.
        attribution = all_group["common_support"]["attribution"]
        self.assertEqual(attribution["difference_cents"], "175/1")
        self.assertNotEqual(frac(attribution["difference_cents"]), overall_diff)
        self.assertEqual(
            frac(attribution["cost_change_cents"])
            + frac(attribution["composition_cents"]),
            Fraction(175, 1),
        )

    def test_tier_supports_need_not_match_all_support(self):
        groups = self.report["groups"]
        common = {
            name: [
                row["customer"]
                for row in groups[name]["common_support"]["customers"]
            ]
            for name in ("all", "basic", "premium", "uncovered")
        }
        # P appears only in premium: it joins the premium and all supports
        # but never the basic one.
        self.assertEqual(common["basic"], ["X", "Y"])
        self.assertEqual(common["premium"], ["P"])
        self.assertEqual(common["all"], ["P", "X", "Y"])
        self.assertEqual(common["uncovered"], [])
        premium = groups["premium"]["common_support"]["attribution"]
        self.assertEqual(premium["status"], "comparable")
        self.assertEqual(premium["difference_cents"], "200/1")
        self.assertEqual(premium["cost_change_cents"], "200/1")
        self.assertEqual(premium["composition_cents"], "0/1")


class V2SharedContractTest(Base):
    """Acceptance 3: v2 shared parent contract, served-customer grouping."""

    def setUp(self):
        super().setUp()
        v2_sources(
            self.source,
            customers="X\nY\nD\nH\n",
            hierarchy=(
                "e1,X,D,2026-01-01,\n"
                "e2,D,H,2026-01-01,\n"
                "e3,Y,H,2026-01-01,\n"
            ),
            contracts="KH,H,2026-01-01,2026-04-01,basic\n",
            tickets=HEADLINE_TICKETS,
        )
        inherited.build(self.source, self.db)
        self.report = self.run_compare()

    def test_grouping_by_served_customer_without_amplification(self):
        self.assertEqual(self.report["input"]["database_schema"],
                         "inherited_contracts_v2")
        # Same headline decomposition even though every contract sits on H.
        self.assertEqual(self.report["groups"]["all"], EXPECTED_HEADLINE_ALL)
        self.assertEqual(self.report["groups"]["basic"], EXPECTED_HEADLINE_ALL)
        evidence = self.report["evidence"]["tickets"]
        # One evidence row per ticket: bridge paths and audit layers never
        # amplify the fact count.
        self.assertEqual(len(evidence), 6)
        self.assertEqual(
            sorted(t["ticket_id"] for t in evidence),
            ["A1", "A2", "A3", "B1", "B2", "B3"],
        )
        # Analysis customers are the served customers, never holder H or D.
        self.assertEqual({t["customer"] for t in evidence}, {"X", "Y"})
        depths = {t["ticket_id"]: t["match_depth"] for t in evidence}
        self.assertEqual(
            depths,
            {"A1": 2, "A2": 2, "A3": 1, "B1": 2, "B2": 1, "B3": 1},
        )
        for ticket in evidence:
            self.assertEqual(ticket["version_id"], "KH")
            self.assertEqual(ticket["contract_holder"], "H")
        # The multi-layer structures really exist in this database.
        with closing(sqlite3.connect(self.db)) as db:
            self.assertGreater(
                db.execute(
                    "SELECT count(*) FROM bridge_hierarchy_ancestor"
                ).fetchone()[0],
                0,
            )
            self.assertGreater(
                db.execute("SELECT count(*) FROM audit_ticket_match").fetchone()[0],
                db.execute("SELECT count(*) FROM dwd_ticket").fetchone()[0],
            )


class V1FixtureRegressionTest(Base):
    """Acceptance 3: the shipped v1 fixture still builds and compares."""

    def setUp(self):
        super().setUp()
        shutil.copytree(V1_FIX, self.source)

    def test_legacy_entry_unchanged_and_compare_works(self):
        result = build_v1(self.source, self.db)
        self.assertEqual(
            result["tiers"],
            [
                {"tier": "basic", "tickets": 2, "cost_cents": 13000},
                {"tier": "premium", "tickets": 1, "cost_cents": 20000},
                {"tier": "uncovered", "tickets": 2, "cost_cents": 12000},
            ],
        )
        report = compare(
            self.db, "2026-01-01", "2026-02-01", "2026-02-01", "2026-03-01",
            self.output,
        )
        groups = report["groups"]
        self.assertEqual(
            groups["all"]["periods"],
            {
                "a": {"tickets": 4, "cost_cents": 25000,
                      "mean_cost_cents": "6250/1"},
                "b": {"tickets": 1, "cost_cents": 20000,
                      "mean_cost_cents": "20000/1"},
            },
        )
        # Only C1 has tickets on both sides of 2026-02-01.
        attribution = groups["all"]["common_support"]["attribution"]
        self.assertEqual(attribution["status"], "comparable")
        self.assertEqual(attribution["difference_cents"], "10000/1")
        self.assertEqual(attribution["cost_change_cents"], "10000/1")
        self.assertEqual(attribution["composition_cents"], "0/1")
        self.assertEqual(
            groups["all"]["exclusive"]["a_only"]["detail"],
            [
                {"customer": "C2", "tickets": 2, "cost_cents": 8000},
                {"customer": "C3", "tickets": 1, "cost_cents": 7000},
            ],
        )
        # Tier groups split the facts; B has no basic/uncovered tickets.
        self.assertEqual(groups["basic"]["periods"]["a"]["tickets"], 2)
        self.assertEqual(
            groups["basic"]["common_support"]["attribution"]["status"],
            "insufficient_common_support",
        )
        self.assertEqual(groups["premium"]["periods"]["b"]["tickets"], 1)
        self.assertEqual(groups["uncovered"]["periods"]["a"]["cost_cents"], 12000)
        # v1 evidence carries no inheritance depth but keeps version/holder.
        evidence = {t["ticket_id"]: t for t in report["evidence"]["tickets"]}
        self.assertEqual(evidence["T1"]["version_id"], "V1")
        self.assertEqual(evidence["T1"]["contract_holder"], "C1")
        self.assertIsNone(evidence["T1"]["match_depth"])
        self.assertIsNone(evidence["T3"]["version_id"])
        self.assertEqual(evidence["T3"]["tier"], "uncovered")


class EdgeCasesTest(Base):
    """Acceptance 3: zero cost, empty periods, disjoint supports, etc."""

    def test_zero_cost_tickets_are_real_observations(self):
        v1_sources(
            self.source,
            "CW,W,2026-01-01,2026-04-01,basic\n",
            "Z1,Z,2026-01-10,0\nZ2,Z,2026-03-10,0\nW1,W,2026-01-10,100\n",
        )
        build_v1(self.source, self.db)
        report = self.run_compare()
        uncovered = report["groups"]["uncovered"]
        # Z's zero-cost tickets keep it inside the common support.
        attribution = uncovered["common_support"]["attribution"]
        self.assertEqual(attribution["status"], "comparable")
        self.assertEqual(attribution["mean_cost_cents"],
                         {"a": "0/1", "b": "0/1"})
        self.assertEqual(attribution["difference_cents"], "0/1")
        self.assertEqual(attribution["cost_change_cents"], "0/1")
        self.assertEqual(attribution["composition_cents"], "0/1")
        # Zero total cost makes the cost share undefined, not zero.
        coverage = uncovered["common_support"]["coverage"]
        self.assertEqual(coverage["a"]["ticket_share"], "1/1")
        self.assertIsNone(coverage["a"]["cost_share"])
        self.assertIsNone(coverage["b"]["cost_share"])
        # W is A-only in basic; the zero-cost Z still counts in 'all'.
        all_group = report["groups"]["all"]
        self.assertEqual(all_group["periods"]["a"]["mean_cost_cents"], "50/1")
        self.assertEqual(all_group["periods"]["b"]["mean_cost_cents"], "0/1")
        self.assertEqual(
            all_group["common_support"]["coverage"]["a"]["cost_share"], "0/1"
        )

    def test_both_periods_empty(self):
        self.build_headline()
        report = self.run_compare("2027-01-01", "2027-02-01",
                                  "2027-03-01", "2027-04-01")
        for group in ("all", "basic", "premium", "uncovered"):
            self.assertEqual(report["groups"][group], EMPTY_GROUP, group)
        self.assertEqual(report["evidence"]["tickets"], [])
        self.assertEqual(
            report["evidence"]["sha256"],
            hashlib.sha256(b"[]").hexdigest(),
        )

    def test_completely_disjoint_customer_sets(self):
        v1_sources(
            self.source,
            "CP,P,2026-01-01,2026-04-01,basic\nCQ,Q,2026-01-01,2026-04-01,basic\n",
            "P1,P,2026-01-10,100\nQ1,Q,2026-03-10,200\n",
        )
        build_v1(self.source, self.db)
        report = self.run_compare()
        all_group = report["groups"]["all"]
        attribution = all_group["common_support"]["attribution"]
        self.assertEqual(attribution["status"], "insufficient_common_support")
        self.assertIsNone(attribution["difference_cents"])
        # Overall and exclusive diagnostics are still fully reported.
        self.assertEqual(all_group["periods"]["a"]["mean_cost_cents"], "100/1")
        self.assertEqual(all_group["periods"]["b"]["mean_cost_cents"], "200/1")
        self.assertEqual(
            all_group["exclusive"]["a_only"]["detail"],
            [{"customer": "P", "tickets": 1, "cost_cents": 100}],
        )
        self.assertEqual(
            all_group["exclusive"]["b_only"]["detail"],
            [{"customer": "Q", "tickets": 1, "cost_cents": 200}],
        )
        coverage = all_group["common_support"]["coverage"]
        self.assertEqual(coverage["a"]["ticket_share"], "0/1")
        self.assertEqual(coverage["b"]["cost_share"], "0/1")

    def test_single_common_customer_is_comparable(self):
        v1_sources(
            self.source,
            HEADLINE_CONTRACTS + "CZ,Z,2026-01-01,2026-04-01,basic\n",
            "A1,X,2026-01-10,100\nA2,Y,2026-01-11,50\n"
            "B1,X,2026-03-10,300\nB2,Z,2026-03-11,70\n",
        )
        build_v1(self.source, self.db)
        report = self.run_compare()
        common = report["groups"]["all"]["common_support"]
        self.assertEqual(common["attribution"]["status"], "comparable")
        self.assertEqual([r["customer"] for r in common["customers"]], ["X"])
        # One customer: shares are 1/1, composition is exactly zero.
        self.assertEqual(common["attribution"]["difference_cents"], "200/1")
        self.assertEqual(common["attribution"]["cost_change_cents"], "200/1")
        self.assertEqual(common["attribution"]["composition_cents"], "0/1")

    def test_customer_changing_tier_joins_different_groups(self):
        v1_sources(
            self.source,
            "CB,C,2026-01-01,2026-02-01,basic\nCM,C,2026-03-01,2026-04-01,premium\n",
            "C1,C,2026-01-10,100\nC2,C,2026-03-10,400\n",
        )
        build_v1(self.source, self.db)
        report = self.run_compare()
        groups = report["groups"]
        all_common = groups["all"]["common_support"]
        self.assertEqual([r["customer"] for r in all_common["customers"]], ["C"])
        self.assertEqual(all_common["attribution"]["difference_cents"], "300/1")
        # In the tier groups C is exclusive to one side each.
        self.assertEqual(
            groups["basic"]["exclusive"]["a_only"]["detail"],
            [{"customer": "C", "tickets": 1, "cost_cents": 100}],
        )
        self.assertEqual(
            groups["premium"]["exclusive"]["b_only"]["detail"],
            [{"customer": "C", "tickets": 1, "cost_cents": 400}],
        )
        self.assertEqual(
            groups["basic"]["common_support"]["attribution"]["status"],
            "insufficient_common_support",
        )
        # The evidence tier follows the final matched contract per period.
        tiers = {t["ticket_id"]: t["tier"]
                 for t in report["evidence"]["tickets"]}
        self.assertEqual(tiers, {"C1": "basic", "C2": "premium"})


class PeriodValidationTest(Base):
    """Acceptance 4: boundaries, leap days, invalid and swapped periods."""

    def setUp(self):
        super().setUp()
        v1_sources(
            self.source,
            "CX,X,2026-01-01,2026-04-01,basic\n",
            "S1,X,2026-01-01,10\n"
            "E1,X,2026-02-01,20\n"
            "G1,X,2026-02-15,50\n"
            "S2,X,2026-03-01,30\n"
            "E2,X,2026-04-01,40\n",
        )
        build_v1(self.source, self.db)

    def test_half_open_boundaries_and_gap(self):
        report = self.run_compare()
        evidence = {t["ticket_id"]: t["period"]
                    for t in report["evidence"]["tickets"]}
        # Starts are inclusive, ends exclusive, the gap month is out.
        self.assertEqual(evidence, {"S1": "a", "S2": "b"})

    def test_adjacent_periods_assign_each_ticket_once(self):
        report = self.run_compare(A_START, A_END, A_END, B_START)
        evidence = {t["ticket_id"]: t["period"]
                    for t in report["evidence"]["tickets"]}
        self.assertEqual(evidence, {"S1": "a", "E1": "b", "G1": "b"})

    def test_leap_day_ticket_and_boundary(self):
        v1_sources(
            self.source,
            "CX,X,2024-02-01,2024-05-01,basic\n",
            "L1,X,2024-02-29,10\nM1,X,2024-04-15,20\n",
        )
        build_v1(self.source, self.db)
        report = self.run_compare("2024-02-29", "2024-03-29",
                                  "2024-04-01", "2024-05-01")
        evidence = {t["ticket_id"]: t["period"]
                    for t in report["evidence"]["tickets"]}
        self.assertEqual(evidence, {"L1": "a", "M1": "b"})

    def test_invalid_dates_rejected(self):
        for bad in ("2026-02-29", "2026-13-01", "2026-1-01", "20260101",
                    "2026-02-01 ", "", "2026/02/01", "abcd"):
            with self.subTest(bad=bad):
                with self.assertRaises(CompareError):
                    self.run_compare(bad, A_END)
                with self.assertRaises(CompareError):
                    self.run_compare(A_START, bad)

    def test_reversed_and_overlapping_periods_rejected(self):
        with self.assertRaisesRegex(CompareError, "earlier than end"):
            self.run_compare(A_START, A_START)
        with self.assertRaisesRegex(CompareError, "earlier than end"):
            self.run_compare(A_END, A_START)
        with self.assertRaisesRegex(CompareError, "must not overlap"):
            self.run_compare(A_START, "2026-03-15", "2026-02-15", B_END)
        with self.assertRaisesRegex(CompareError, "must not overlap"):
            self.run_compare("2026-02-15", B_END, A_START, "2026-03-15")

    def test_b_may_precede_a_and_swap_negates_contributions(self):
        self.build_headline()
        forward = self.run_compare()
        swapped = self.run_compare(B_START, B_END, A_START, A_END)
        for name in ("all", "basic"):
            g_f = forward["groups"][name]
            g_s = swapped["groups"][name]
            self.assertEqual(g_f["periods"]["a"], g_s["periods"]["b"])
            self.assertEqual(g_f["periods"]["b"], g_s["periods"]["a"])
            att_f = g_f["common_support"]["attribution"]
            att_s = g_s["common_support"]["attribution"]
            self.assertEqual(att_f["status"], att_s["status"])
            self.assertEqual(att_f["mean_cost_cents"]["a"],
                             att_s["mean_cost_cents"]["b"])
            for key in ("difference_cents", "cost_change_cents",
                        "composition_cents"):
                self.assertEqual(frac(att_f[key]), -frac(att_s[key]), key)
            # Common support itself is unchanged; per-customer rows swap
            # their bases and negate their signed contributions.
            rows_f = {r["customer"]: r for r in g_f["common_support"]["customers"]}
            rows_s = {r["customer"]: r for r in g_s["common_support"]["customers"]}
            self.assertEqual(set(rows_f), set(rows_s))
            for customer, row_f in rows_f.items():
                row_s = rows_s[customer]
                self.assertEqual(row_f["a"], row_s["b"])
                self.assertEqual(row_f["b"], row_s["a"])
                self.assertEqual(
                    frac(row_f["cost_change_contribution_cents"]),
                    -frac(row_s["cost_change_contribution_cents"]),
                )
                self.assertEqual(
                    frac(row_f["composition_contribution_cents"]),
                    -frac(row_s["composition_contribution_cents"]),
                )
            self.assertEqual(g_f["exclusive"]["a_only"], g_s["exclusive"]["b_only"])
            self.assertEqual(g_f["exclusive"]["b_only"], g_s["exclusive"]["a_only"])
        # Evidence keeps the same tickets with period labels swapped.
        periods_f = {t["ticket_id"]: t["period"]
                     for t in forward["evidence"]["tickets"]}
        periods_s = {t["ticket_id"]: t["period"]
                     for t in swapped["evidence"]["tickets"]}
        self.assertEqual(
            periods_s, {k: ("b" if v == "a" else "a") for k, v in periods_f.items()}
        )


class DeterminismTest(Base):
    """Acceptance 4: insert order, unrelated tables and reruns."""

    def test_row_order_and_unrelated_tables_do_not_change_output(self):
        v1_sources(self.source, HEADLINE_CONTRACTS, HEADLINE_TICKETS)
        build_v1(self.source, self.db)
        shuffled = self.root / "shuffled"
        v1_sources(
            shuffled,
            "".join(reversed(HEADLINE_CONTRACTS.splitlines(keepends=True))),
            "".join(reversed(HEADLINE_TICKETS.splitlines(keepends=True))),
        )
        db2 = self.root / "shuffled.sqlite"
        build_v1(shuffled, db2)
        out1, out2 = self.root / "r1.json", self.root / "r2.json"
        compare(self.db, A_START, A_END, B_START, B_END, out1)
        compare(db2, A_START, A_END, B_START, B_END, out2)
        self.assertEqual(out1.read_bytes(), out2.read_bytes())
        # An unrelated extra table changes nothing either.
        with sqlite3.connect(self.db) as db:
            db.execute("CREATE TABLE unrelated(note TEXT)")
            db.execute("INSERT INTO unrelated VALUES ('noise')")
        compare(self.db, A_START, A_END, B_START, B_END, out2)
        self.assertEqual(out1.read_bytes(), out2.read_bytes())

    def test_rerun_is_byte_identical(self):
        self.build_headline()
        self.run_compare()
        first = self.output.read_bytes()
        self.run_compare()
        self.assertEqual(first, self.output.read_bytes())


class OverflowPrecisionTest(Base):
    """Acceptance 4: sums beyond int64 and non-integer contributions."""

    def test_sum_beyond_sqlite_int64_stays_exact(self):
        huge = 9223372036854775000  # fits signed int64; two of them do not
        v1_sources(
            self.source,
            "CX,X,2026-01-01,2026-04-01,basic\n",
            "B1,X,2026-01-10,1\nB2,X,2026-01-11,1\nB3,X,2026-03-10,7\n",
        )
        build_v1(self.source, self.db)
        with sqlite3.connect(self.db) as db:
            db.execute(
                "UPDATE dwd_ticket SET cost_cents=? WHERE ticket_id IN ('B1','B2')",
                (huge,),
            )
        report = self.run_compare()
        all_group = report["groups"]["all"]
        total = 2 * huge
        self.assertGreater(total, 2**63 - 1)
        self.assertEqual(all_group["periods"]["a"]["cost_cents"], total)
        self.assertEqual(all_group["periods"]["a"]["mean_cost_cents"],
                         f"{huge}/1")
        attribution = all_group["common_support"]["attribution"]
        expected_diff = 7 - huge
        self.assertEqual(attribution["difference_cents"], f"{expected_diff}/1")
        self.assertEqual(attribution["cost_change_cents"], f"{expected_diff}/1")
        self.assertEqual(attribution["composition_cents"], "0/1")
        # JSON carries the exact integer and no float anywhere.
        text = self.output.read_text(encoding="utf-8")
        self.assertIn(f'"cost_cents": {total}', text)
        self.assertNotIn("e+", text.lower())
        loaded = json.loads(text)
        self.assertEqual(
            loaded["groups"]["all"]["periods"]["a"]["cost_cents"], total
        )
        rationals = re.findall(r'"(-?\d+/\d+)"', text)
        self.assertTrue(rationals)
        for value in rationals:
            numerator, denominator = value.split("/")
            self.assertGreater(int(denominator), 0)
            self.assertEqual(Fraction(value), Fraction(int(numerator), int(denominator)))


class CorruptionFormatTest(Base):
    """Acceptance 5: missing, incompatible, unsupported or corrupted input."""

    def craft_v1(self, contracts, tickets):
        with sqlite3.connect(self.db) as db:
            db.execute(
                "CREATE TABLE dim_contract(contract_sk, version_id, customer,"
                " valid_from, valid_to, tier)"
            )
            db.execute(
                "CREATE TABLE dwd_ticket(ticket_id, customer, opened,"
                " cost_cents, contract_sk)"
            )
            db.executemany("INSERT INTO dim_contract VALUES (?,?,?,?,?,?)", contracts)
            db.executemany("INSERT INTO dwd_ticket VALUES (?,?,?,?,?)", tickets)

    def craft_v2(self, contracts, tickets,
                 meta=(("schema_family", "inherited_contracts_v2"),
                       ("schema_version", "2"))):
        with sqlite3.connect(self.db) as db:
            db.execute("CREATE TABLE wh_meta(meta_key, meta_value)")
            db.executemany("INSERT INTO wh_meta VALUES (?,?)", meta)
            db.execute(
                "CREATE TABLE dim_contract(contract_sk, version_id, customer,"
                " valid_from, valid_to, tier, source_row_number)"
            )
            db.execute(
                "CREATE TABLE dwd_ticket(ticket_id, customer, opened, cost_cents,"
                " match_depth, contract_sk, contract_holder)"
            )
            db.executemany(
                "INSERT INTO dim_contract VALUES (?,?,?,?,?,?,?)", contracts
            )
            db.executemany("INSERT INTO dwd_ticket VALUES (?,?,?,?,?,?,?)", tickets)

    def assert_rejected(self, pattern):
        with self.assertRaisesRegex(CompareError, pattern):
            self.run_compare()
        self.assertFalse(self.output.exists())

    def test_missing_database_never_created(self):
        missing = self.root / "nope.sqlite"
        with self.assertRaisesRegex(CompareError, "read-only"):
            compare(missing, A_START, A_END, B_START, B_END, self.output)
        self.assertFalse(missing.exists())
        self.assertFalse(self.output.exists())

    def test_not_a_database(self):
        self.db.write_bytes(b"this is not sqlite")
        self.assert_rejected("not a readable SQLite database")

    def test_missing_fact_table_is_incompatible(self):
        with sqlite3.connect(self.db) as db:
            db.execute(
                "CREATE TABLE dim_contract(contract_sk, version_id, customer,"
                " valid_from, valid_to, tier)"
            )
        self.assert_rejected("incompatible structure.*dwd_ticket")

    def test_wrong_columns_are_incompatible(self):
        with sqlite3.connect(self.db) as db:
            db.execute(
                "CREATE TABLE dim_contract(contract_sk, version_id, customer,"
                " valid_from, valid_to, tier)"
            )
            db.execute(
                "CREATE TABLE dwd_ticket(ticket_id, customer, opened, cost_cents)"
            )
        self.assert_rejected("incompatible structure")

    def test_unsupported_format_marker(self):
        self.craft_v2(
            [(1, "K", "X", "2026-01-01", None, "basic", 1)],
            [("T1", "X", "2026-01-10", 1, 0, 1, "X")],
            meta=(("schema_family", "future_format"), ("schema_version", "9")),
        )
        self.assert_rejected("unsupported format marker")
        self.db.unlink()
        self.craft_v2(
            [(1, "K", "X", "2026-01-01", None, "basic", 1)],
            [("T1", "X", "2026-01-10", 1, 0, 1, "X")],
            meta=(("schema_family", "inherited_contracts_v2"),
                  ("schema_version", "3")),
        )
        self.assert_rejected("unsupported format marker")

    def test_dangling_contract_sk_is_data_error_not_uncovered(self):
        self.craft_v1(
            [(1, "K", "X", "2026-01-01", None, "basic")],
            [("T1", "X", "2026-01-10", 100, 99)],
        )
        self.assert_rejected(r"T1.*dangling contract_sk 99")

    def test_ambiguous_contract_join_rejected(self):
        self.craft_v1(
            [(1, "K1", "X", "2026-01-01", None, "basic"),
             (1, "K2", "X", "2026-01-01", None, "basic")],
            [("T1", "X", "2026-01-10", 100, 1)],
        )
        self.assert_rejected("ambiguous join.*contract_sk 1")

    def test_illegal_tier_rejected(self):
        self.craft_v1(
            [(1, "K", "X", "2026-01-01", None, "gold")],
            [("T1", "X", "2026-01-10", 100, 1)],
        )
        self.assert_rejected("tier must be basic/premium")

    def test_corrupt_fact_values_locate_business_key(self):
        cases = [
            ([("T1", "X", "2026-01-10", "abc", 1)], r"T1.*cost_cents"),
            ([("T1", "X", "2026-01-10", 1.5, 1)], r"T1.*cost_cents"),
            ([("T1", "X", "2026-01-10", -5, 1)], r"T1.*non-negative"),
            ([("T1", "X", "2026-01-10", None, 1)], r"T1.*cost_cents"),
            ([("T1", "X", "2026-02-30", 1, 1)], r"T1.*opened"),
            ([("T1", "X", 20260110, 1, 1)], r"T1.*opened must be text"),
            ([("T1", None, "2026-01-10", 1, 1)], r"T1.*customer"),
            ([("T1", "X", "2026-01-10", 1, 1),
              ("T1", "X", "2026-01-11", 2, 1)], r"T1.*duplicate ticket_id"),
            ([(None, "X", "2026-01-10", 1, 1)], r"ticket_id must be non-empty"),
            ([("T1", "X", "2026-01-10", 1, "1")], r"T1.*contract_sk"),
        ]
        for tickets, pattern in cases:
            with self.subTest(tickets=tickets):
                self.craft_v1(
                    [(1, "K", "X", "2026-01-01", None, "basic")], tickets
                )
                self.assert_rejected(pattern)
                self.db.unlink()

    def test_v2_fact_invariants_checked(self):
        contracts = [(1, "K", "X", "2026-01-01", None, "basic", 1)]
        # Uncovered ticket must not carry inheritance details.
        self.craft_v2(contracts, [("T1", "X", "2026-01-10", 1, 0, None, None)])
        self.assert_rejected(r"T1.*uncovered")
        self.db.unlink()
        # Stored holder contradicting the contract dimension is corruption.
        self.craft_v2(contracts, [("T1", "X", "2026-01-10", 1, 0, 1, "ZZ")])
        self.assert_rejected(r"T1.*contract_holder")
        self.db.unlink()
        # Covered ticket without a depth is corruption in v2.
        self.craft_v2(contracts, [("T1", "X", "2026-01-10", 1, None, 1, "X")])
        self.assert_rejected(r"T1.*match_depth")


class SnapshotIsolationTest(Base):
    """Acceptance 5: facts and contracts come from one read snapshot."""

    def setUp(self):
        super().setUp()
        self.build_headline()
        with sqlite3.connect(self.db) as db:
            db.execute("PRAGMA journal_mode=WAL")

    def test_concurrent_commit_between_reads_stays_invisible(self):
        clean = self.run_compare()
        clean_bytes = self.output.read_bytes()
        committed = []

        original = compare_mod._read_tickets

        def interleaved(db, mode, contracts):
            # Another connection commits after contracts were read but
            # before the fact read: no sleeps, exact interleaving.
            with sqlite3.connect(self.db) as other:
                other.execute(
                    "INSERT INTO dwd_ticket VALUES ('ZZ','X','2026-01-12',500,NULL)"
                )
                other.commit()
            committed.append(True)
            return original(db, mode, contracts)

        with mock.patch.object(compare_mod, "_read_tickets", interleaved):
            report = self.run_compare()
        self.assertEqual(committed, [True])
        # The open read transaction still sees the pre-commit snapshot.
        self.assertEqual(report, clean)
        self.assertEqual(self.output.read_bytes(), clean_bytes)
        self.assertEqual(
            report["groups"]["all"]["periods"]["a"]["tickets"], 3
        )
        # The commit was real: a fresh run observes the extra ticket.
        after = self.run_compare()
        self.assertEqual(after["groups"]["all"]["periods"]["a"]["tickets"], 4)
        self.assertNotEqual(after["evidence"]["sha256"],
                            clean["evidence"]["sha256"])


class IndependentOracleTest(Base):
    """Acceptance 6: independent Fraction oracle over the report evidence."""

    def test_all_groups_match_oracle_and_digest_after_db_removed(self):
        contracts = HEADLINE_CONTRACTS + (
            "CL,L,2026-01-01,2026-02-01,basic\n"
            "CN,N,2026-03-01,2026-04-01,basic\n"
            "CP,P,2026-01-01,2026-04-01,premium\n"
            "CU,U,2026-01-01,2026-04-01,basic\n"
            "CV,V,2026-01-01,2026-04-01,basic\n"
        )
        tickets = HEADLINE_TICKETS + (
            "A4,L,2026-01-05,900\n"
            "B4,N,2026-03-02,50\n"
            "B5,N,2026-03-25,50\n"
            "P1,P,2026-01-12,1000\n"
            "P2,P,2026-03-12,1200\n"
            "U1,U,2026-01-15,0\n"
            "U2,U,2026-03-16,0\n"
            "Q1,Q,2026-03-15,60\n"  # Q has no contract: uncovered
        )
        # V holds a contract but never opens a ticket: a contract alone is
        # not an observation; Q is uncovered; U has zero-cost observations.
        v1_sources(self.source, contracts, tickets)
        build_v1(self.source, self.db)
        report = self.run_compare()
        moved = self.root / "moved.sqlite"
        shutil.move(self.db, moved)
        self.assertFalse(self.db.exists())

        evidence = report["evidence"]["tickets"]
        # Every group, every number: recomputed independently from evidence.
        for group in ("all", "basic", "premium", "uncovered"):
            self.assertEqual(
                report["groups"][group], oracle_group(evidence, group), group
            )
        # Digest recomputed from the report alone, input database gone.
        self.assertEqual(report["evidence"]["sha256"], digest_of(evidence))
        # Exact closure per group and per contribution sum.
        for group in ("all", "basic", "premium", "uncovered"):
            common = report["groups"][group]["common_support"]
            attribution = common["attribution"]
            if attribution["status"] != "comparable":
                self.assertIsNone(attribution["difference_cents"])
                continue
            self.assertEqual(
                frac(attribution["difference_cents"]),
                frac(attribution["cost_change_cents"])
                + frac(attribution["composition_cents"]),
                group,
            )
            rows = common["customers"]
            self.assertEqual(
                sum(frac(r["cost_change_contribution_cents"]) for r in rows),
                frac(attribution["cost_change_cents"]),
                group,
            )
            self.assertEqual(
                sum(frac(r["composition_contribution_cents"]) for r in rows),
                frac(attribution["composition_cents"]),
                group,
            )
        # U's zero-cost ticket keeps it in the basic support; Q is uncovered.
        basic_customers = [
            r["customer"]
            for r in report["groups"]["basic"]["common_support"]["customers"]
        ]
        self.assertIn("U", basic_customers)
        self.assertEqual(
            report["groups"]["uncovered"]["exclusive"]["b_only"]["detail"],
            [{"customer": "Q", "tickets": 1, "cost_cents": 60}],
        )
        # V has a contract in both periods but no ticket: not an observation.
        mentioned = set()
        for group in report["groups"].values():
            mentioned.update(
                r["customer"] for r in group["common_support"]["customers"]
            )
            for side in ("a_only", "b_only"):
                mentioned.update(
                    r["customer"] for r in group["exclusive"][side]["detail"]
                )
        self.assertNotIn("V", mentioned)


class OutputSafetyTest(Base):
    """Acceptance 6: atomic replace, aliases, cleanup and rerun."""

    def setUp(self):
        super().setUp()
        self.build_headline()
        self.run_compare()
        self.old_report = self.output.read_bytes()
        self.db_bytes = self.db.read_bytes()

    def test_replace_failure_preserves_old_report_and_cleans_up(self):
        def boom(src, dst):
            if Path(dst) == self.output:
                raise OSError("injected replace failure")
            return os.replace(src, dst)

        with mock.patch("contract_warehouse.compare.os.replace", boom):
            with self.assertRaises(OSError):
                self.run_compare()
        self.assertEqual(self.output.read_bytes(), self.old_report)
        self.assertEqual(self.db.read_bytes(), self.db_bytes)
        self.assertEqual(self.temp_files(), [])
        # Fixed: rerun succeeds in place.
        self.run_compare()
        self.assertTrue(self.output.read_bytes())

    def test_write_failure_preserves_old_report_and_cleans_up(self):
        with mock.patch(
            "contract_warehouse.compare.os.fdopen",
            side_effect=OSError("injected write failure"),
        ):
            with self.assertRaises(OSError):
                self.run_compare()
        self.assertEqual(self.output.read_bytes(), self.old_report)
        self.assertEqual(self.temp_files(), [])
        self.run_compare()

    def test_output_must_not_alias_input_database(self):
        with self.assertRaisesRegex(CompareError, "must not overwrite"):
            self.run_compare(output=self.db)
        link = self.root / "linked.sqlite"
        os.symlink(self.db, link)
        with self.assertRaisesRegex(CompareError, "must not overwrite"):
            self.run_compare(output=link)
        hard = self.root / "hard.sqlite"
        os.link(self.db, hard)
        with self.assertRaisesRegex(CompareError, "hard link|alias|overwrite"):
            self.run_compare(output=hard)
        self.assertEqual(self.db.read_bytes(), self.db_bytes)
        self.assertEqual(self.output.read_bytes(), self.old_report)
        self.assertEqual(self.temp_files(), [])


class CliSubprocessTest(Base):
    """Acceptance 1/6: real CLI chains for both warehouse modes."""

    def run_cli(self, *args):
        return subprocess.run(
            [sys.executable, "-m", *args],
            cwd=SRC_DIR,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_v2_build_then_compare_cli_end_to_end(self):
        v2_sources(
            self.source,
            customers="X\nY\nD\nH\n",
            hierarchy=(
                "e1,X,D,2026-01-01,\n"
                "e2,D,H,2026-01-01,\n"
                "e3,Y,H,2026-01-01,\n"
            ),
            contracts="KH,H,2026-01-01,2026-04-01,basic\n",
            tickets=HEADLINE_TICKETS,
        )
        build = self.run_cli(
            "contract_warehouse.inherited",
            "--source", str(self.source),
            "--database", str(self.db),
        )
        self.assertEqual(build.returncode, 0, build.stderr)
        proc = self.run_cli(
            "contract_warehouse.compare",
            "--database", str(self.db),
            "--a-start", A_START, "--a-end", A_END,
            "--b-start", B_START, "--b-end", B_END,
            "--output", str(self.output),
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        summary = json.loads(proc.stdout)
        self.assertEqual(summary["schema_version"], "compare_report_v1")
        self.assertEqual(summary["database_schema"], "inherited_contracts_v2")
        self.assertEqual(
            summary["groups"]["all"]["difference_cents"], "500/3"
        )
        report = json.loads(self.output.read_text(encoding="utf-8"))
        self.assertEqual(report["groups"]["all"], EXPECTED_HEADLINE_ALL)
        self.assertEqual(
            report["evidence"]["sha256"],
            digest_of(report["evidence"]["tickets"]),
        )

    def test_v1_build_then_compare_cli_end_to_end(self):
        shutil.copytree(COMPARE_FIX, self.source)
        build = self.run_cli(
            "contract_warehouse.cli",
            "--source", str(self.source),
            "--database", str(self.db),
        )
        self.assertEqual(build.returncode, 0, build.stderr)
        proc = self.run_cli(
            "contract_warehouse.compare",
            "--database", str(self.db),
            "--a-start", A_START, "--a-end", A_END,
            "--b-start", B_START, "--b-end", B_END,
            "--output", str(self.output),
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        report = json.loads(self.output.read_text(encoding="utf-8"))
        basic = report["groups"]["basic"]
        self.assertEqual(
            basic["common_support"]["attribution"],
            EXPECTED_HEADLINE_ALL["common_support"]["attribution"],
        )
        self.assertEqual(
            basic["exclusive"]["a_only"]["detail"],
            [{"customer": "L", "tickets": 1, "cost_cents": 900}],
        )

    def test_cli_failures_are_nonzero_without_artifacts(self):
        self.build_headline()
        proc = self.run_cli(
            "contract_warehouse.compare",
            "--database", str(self.db),
            "--a-start", A_START, "--a-end", "2026-03-15",
            "--b-start", "2026-02-15", "--b-end", B_END,
            "--output", str(self.output),
        )
        self.assertEqual(proc.returncode, 2)
        self.assertIn("must not overlap", proc.stderr)
        self.assertFalse(self.output.exists())
        # Missing database: nonzero and no file is created.
        missing = self.root / "missing.sqlite"
        proc = self.run_cli(
            "contract_warehouse.compare",
            "--database", str(missing),
            "--a-start", A_START, "--a-end", A_END,
            "--b-start", B_START, "--b-end", B_END,
            "--output", str(self.output),
        )
        self.assertEqual(proc.returncode, 2)
        self.assertFalse(missing.exists())
        self.assertFalse(self.output.exists())
        # Output aliasing the input database is refused.
        proc = self.run_cli(
            "contract_warehouse.compare",
            "--database", str(self.db),
            "--a-start", A_START, "--a-end", A_END,
            "--b-start", B_START, "--b-end", B_END,
            "--output", str(self.db),
        )
        self.assertEqual(proc.returncode, 2)
        self.assertIn("must not overwrite", proc.stderr)


if __name__ == "__main__":
    unittest.main()
