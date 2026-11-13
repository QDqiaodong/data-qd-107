"""Two-period customer-mix cost attribution report (independent analysis).

Consumes the existing one-row-per-ticket fact layer (``dwd_ticket``) plus the
final matched contract dimension (``dim_contract``) of either warehouse mode
-- the two-file baseline (v1) or a database explicitly marked
``inherited_contracts_v2`` -- and explains a mean ticket cost increase between
two half-open ``[start, end)`` periods as the sum of

* a **cost-change** term (same customers' own mean cost moved), and
* a **composition** term (the ticket share of customers moved),

computed only over the *common support*: customers with at least one ticket
in both periods inside one group.  The decomposition is descriptive, never a
causal claim about contract tiers or organization changes, and it is never
presented as the cost difference of the whole population.

Everything is read inside one SQLite read transaction opened ``mode=ro``:
the analysis never creates, upgrades or modifies the input database, never
re-matches contracts and never rebuilds ancestor bridges.  Amounts and
counts stay arbitrary-precision integers; every mean, share, difference and
contribution is an exact :class:`fractions.Fraction`, serialized as the
reduced string ``"numerator/positive_denominator"`` (zero is ``"0/1"``).
No wall clock, no durations and no machine absolute paths enter the report.

See ``docs/contract.md`` for the full report contract and analysis rules.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from contextlib import closing
from dataclasses import dataclass
from datetime import date
from fractions import Fraction
from pathlib import Path
import sqlite3
import tempfile

#: Independent schema version of this report (unrelated to warehouse marks).
REPORT_SCHEMA_VERSION = "compare_report_v1"

V2_SCHEMA_FAMILY = "inherited_contracts_v2"
V2_SCHEMA_VERSION = "2"

#: Input mode labels stored in the report.
MODE_V1 = "baseline_v1"
MODE_V2 = V2_SCHEMA_FAMILY

GROUPS = ("all", "basic", "premium", "uncovered")

INTERPRETATION = (
    "描述性分解：仅把共同支持客户（两期都至少有一张工单）上 B 期相对 A 期的"
    "平均工单成本差额拆分为成本变化项与组合变化项；不代表合同等级或组织调整"
    "对成本的因果效应，也不解释共同支持之外的总体均值变化。"
)

# Exact column sets of the consumed tables, per warehouse mode.  Anything
# else is an incompatible structure and fails the run before any data read.
_V1_TABLES = {
    "dwd_ticket": {"ticket_id", "customer", "opened", "cost_cents", "contract_sk"},
    "dim_contract": {
        "contract_sk", "version_id", "customer", "valid_from", "valid_to", "tier",
    },
}
_V2_TABLES = {
    "dwd_ticket": {
        "ticket_id", "customer", "opened", "cost_cents",
        "match_depth", "contract_sk", "contract_holder",
    },
    "dim_contract": {
        "contract_sk", "version_id", "customer", "valid_from", "valid_to",
        "tier", "source_row_number",
    },
    "wh_meta": {"meta_key", "meta_value"},
}


class CompareError(ValueError):
    """Any invalid parameter, incompatible database or corrupted fact."""


@dataclass(frozen=True)
class FactTicket:
    """One consumed fact row with its final stored attribution."""

    ticket_id: str
    customer: str
    opened: date
    cost_cents: int
    tier: str  # basic / premium / uncovered (from the final stored contract)
    version_id: str | None
    contract_holder: str | None
    match_depth: int | None  # v2 only; None means "not saved" (v1) or uncovered


# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------


def _parse_day(text, label: str) -> date:
    if not isinstance(text, str) or not text:
        raise CompareError(f"{label}: date must be strict YYYY-MM-DD, got {text!r}")
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise CompareError(
            f"{label}: date must be strict YYYY-MM-DD, got {text!r}"
        ) from exc
    if parsed.isoformat() != text:
        raise CompareError(f"{label}: date must be strict YYYY-MM-DD, got {text!r}")
    return parsed


def _validate_periods(a_start: str, a_end: str, b_start: str, b_end: str):
    a_s = _parse_day(a_start, "a-start")
    a_e = _parse_day(a_end, "a-end")
    b_s = _parse_day(b_start, "b-start")
    b_e = _parse_day(b_end, "b-end")
    if not a_s < a_e:
        raise CompareError(f"period A: start {a_start!r} must be earlier than end {a_end!r}")
    if not b_s < b_e:
        raise CompareError(f"period B: start {b_start!r} must be earlier than end {b_end!r}")
    # Half-open intervals [start, end) overlap iff each start < other end.
    if a_s < b_e and b_s < a_e:
        raise CompareError(
            "periods A and B must not overlap: "
            f"A=[{a_start}, {a_end}), B=[{b_start}, {b_end})"
        )
    return a_s, a_e, b_s, b_e


# ---------------------------------------------------------------------------
# Snapshot read (one SQLite read transaction; test seams are the two readers)
# ---------------------------------------------------------------------------


def _open_readonly(database: Path) -> sqlite3.Connection:
    uri = database.absolute().as_uri() + "?mode=ro"
    try:
        # autocommit: the explicit BEGIN below owns the read transaction.
        return sqlite3.connect(uri, uri=True, isolation_level=None)
    except sqlite3.Error as exc:
        raise CompareError(
            f"cannot open database read-only (missing file is never created): "
            f"{database}: {exc}"
        ) from exc


def _table_columns(db: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in db.execute(f"PRAGMA table_info({table})")}


def _detect_mode(db: sqlite3.Connection) -> str:
    try:
        tables = {
            row[0]
            for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
    except sqlite3.DatabaseError as exc:
        raise CompareError(f"input is not a readable SQLite database: {exc}") from exc
    if "wh_meta" in tables:
        required = _V2_TABLES
        mode = MODE_V2
    else:
        required = _V1_TABLES
        mode = MODE_V1
    for table, columns in sorted(required.items()):
        if table not in tables:
            raise CompareError(
                f"incompatible structure for mode {mode}: missing table {table!r}"
            )
        actual = _table_columns(db, table)
        if actual != columns:
            raise CompareError(
                f"incompatible structure for mode {mode}: table {table!r} has "
                f"columns {sorted(actual)!r}, expected {sorted(columns)!r}"
            )
    if mode == MODE_V2:
        meta = dict(db.execute("SELECT meta_key, meta_value FROM wh_meta"))
        family = meta.get("schema_family")
        version = meta.get("schema_version")
        if (family, version) != (V2_SCHEMA_FAMILY, V2_SCHEMA_VERSION):
            raise CompareError(
                "unsupported format marker: schema_family="
                f"{family!r} schema_version={version!r}; expected "
                f"{V2_SCHEMA_FAMILY!r} version {V2_SCHEMA_VERSION!r}"
            )
    return mode


def _read_contracts(db: sqlite3.Connection, mode: str) -> dict[int, tuple[str, str, str]]:
    """contract_sk -> (version_id, holder, tier); unique and legal tiers only."""
    contracts: dict[int, tuple[str, str, str]] = {}
    for sk, version_id, customer, tier in db.execute(
        "SELECT contract_sk, version_id, customer, tier FROM dim_contract"
    ):
        if type(sk) is not int:
            raise CompareError(f"dim_contract: contract_sk must be an integer, got {sk!r}")
        if sk in contracts:
            raise CompareError(
                f"dim_contract: ambiguous join, duplicate contract_sk {sk}"
            )
        if type(version_id) is not str or not version_id:
            raise CompareError(
                f"dim_contract contract_sk {sk}: version_id must be non-empty text, "
                f"got {version_id!r}"
            )
        if type(customer) is not str or not customer:
            raise CompareError(
                f"dim_contract {version_id!r}: customer must be non-empty text, "
                f"got {customer!r}"
            )
        if tier not in ("basic", "premium"):
            raise CompareError(
                f"dim_contract {version_id!r}: tier must be basic/premium, got {tier!r}"
            )
        contracts[sk] = (version_id, customer, tier)
    return contracts


def _read_tickets(
    db: sqlite3.Connection, mode: str, contracts: dict[int, tuple[str, str, str]]
) -> list[FactTicket]:
    """Validate every consumed fact row; errors carry the ticket business key."""
    if mode == MODE_V2:
        rows = db.execute(
            "SELECT ticket_id, customer, opened, cost_cents, contract_sk, "
            "match_depth, contract_holder FROM dwd_ticket"
        ).fetchall()
    else:
        rows = [
            (*row, None, None)
            for row in db.execute(
                "SELECT ticket_id, customer, opened, cost_cents, contract_sk "
                "FROM dwd_ticket"
            )
        ]
    tickets: list[FactTicket] = []
    seen: set[str] = set()
    for row_number, (ticket_id, customer, opened, cost, sk, depth, holder) in enumerate(
        rows, 1
    ):
        where = f"dwd_ticket row {row_number} ticket_id {ticket_id!r}"
        if type(ticket_id) is not str or not ticket_id:
            raise CompareError(
                f"dwd_ticket row {row_number}: ticket_id must be non-empty text, "
                f"got {ticket_id!r}"
            )
        if ticket_id in seen:
            raise CompareError(f"{where}: duplicate ticket_id")
        seen.add(ticket_id)
        if type(customer) is not str or not customer:
            raise CompareError(
                f"{where}: customer must be non-empty text, got {customer!r}"
            )
        if type(opened) is not str:
            raise CompareError(f"{where}: opened must be text YYYY-MM-DD, got {opened!r}")
        opened_day = _parse_day(opened, where + " opened")
        if type(cost) is not int:
            raise CompareError(
                f"{where}: cost_cents must be a non-negative integer, got {cost!r}"
            )
        if cost < 0:
            raise CompareError(f"{where}: cost_cents must be non-negative, got {cost}")
        if sk is None:
            if depth is not None or holder is not None:
                raise CompareError(
                    f"{where}: uncovered ticket must not carry match_depth or "
                    f"contract_holder"
                )
            tickets.append(FactTicket(ticket_id, customer, opened_day, cost,
                                      "uncovered", None, None, None))
            continue
        if type(sk) is not int:
            raise CompareError(f"{where}: contract_sk must be an integer, got {sk!r}")
        contract = contracts.get(sk)
        if contract is None:
            raise CompareError(
                f"{where}: dangling contract_sk {sk}; a non-null contract_sk must "
                "uniquely join dim_contract (it is never re-read as uncovered)"
            )
        version_id, contract_holder, tier = contract
        if mode == MODE_V2:
            if type(depth) is not int or depth < 0:
                raise CompareError(
                    f"{where}: match_depth must be a non-negative integer, got {depth!r}"
                )
            if holder != contract_holder:
                raise CompareError(
                    f"{where}: stored contract_holder {holder!r} contradicts "
                    f"dim_contract customer {contract_holder!r}"
                )
        tickets.append(
            FactTicket(ticket_id, customer, opened_day, cost,
                       tier, version_id, contract_holder, depth)
        )
    return tickets


def _read_snapshot(db: sqlite3.Connection):
    """All consumed facts and contracts come from this one read transaction."""
    db.execute("BEGIN")
    try:
        mode = _detect_mode(db)
        contracts = _read_contracts(db, mode)
        tickets = _read_tickets(db, mode, contracts)
    finally:
        db.execute("ROLLBACK")
    return mode, tickets


# ---------------------------------------------------------------------------
# Exact rational analysis
# ---------------------------------------------------------------------------


def _rat(value: Fraction | None) -> str | None:
    """Reduced 'numerator/positive_denominator'; zero is '0/1'; None stays null."""
    if value is None:
        return None
    return f"{value.numerator}/{value.denominator}"


def _customer_rolls(tickets: list[FactTicket]) -> dict[str, list[int]]:
    rolls: dict[str, list[int]] = {}
    for ticket in tickets:
        roll = rolls.setdefault(ticket.customer, [0, 0])
        roll[0] += 1
        roll[1] += ticket.cost_cents
    return rolls


def _exclusive(rolls: dict[str, list[int]], customers: list[str]) -> dict:
    return {
        "customers": len(customers),
        "tickets": sum(rolls[c][0] for c in customers),
        "cost_cents": sum(rolls[c][1] for c in customers),
        "detail": [
            {"customer": c, "tickets": rolls[c][0], "cost_cents": rolls[c][1]}
            for c in customers
        ],
    }


def _analyze_group(a_tickets: list[FactTicket], b_tickets: list[FactTicket]) -> dict:
    rolls = {"a": _customer_rolls(a_tickets), "b": _customer_rolls(b_tickets)}
    periods: dict[str, dict] = {}
    for label in ("a", "b"):
        count = sum(roll[0] for roll in rolls[label].values())
        cost = sum(roll[1] for roll in rolls[label].values())
        mean = Fraction(cost, count) if count else None
        periods[label] = {
            "tickets": count,
            "cost_cents": cost,
            "mean_cost_cents": _rat(mean),
        }

    common = sorted(set(rolls["a"]) & set(rolls["b"]))
    only_a = sorted(set(rolls["a"]) - set(rolls["b"]))
    only_b = sorted(set(rolls["b"]) - set(rolls["a"]))

    coverage: dict[str, dict] = {}
    common_counts: dict[str, int] = {}
    common_costs: dict[str, int] = {}
    for label in ("a", "b"):
        count = sum(rolls[label][c][0] for c in common)
        cost = sum(rolls[label][c][1] for c in common)
        common_counts[label] = count
        common_costs[label] = cost
        total = periods[label]
        coverage[label] = {
            "tickets": count,
            "cost_cents": cost,
            "ticket_share": _rat(Fraction(count, total["tickets"]))
            if total["tickets"]
            else None,
            "cost_share": _rat(Fraction(cost, total["cost_cents"]))
            if total["cost_cents"]
            else None,
        }

    if common:
        total_a = common_counts["a"]
        total_b = common_counts["b"]
        mean_a = Fraction(common_costs["a"], total_a)
        mean_b = Fraction(common_costs["b"], total_b)
        cost_change = Fraction(0)
        composition = Fraction(0)
        customer_rows = []
        for customer in common:
            n_a, cost_a = rolls["a"][customer]
            n_b, cost_b = rolls["b"][customer]
            m_a = Fraction(cost_a, n_a)
            m_b = Fraction(cost_b, n_b)
            p_a = Fraction(n_a, total_a)
            p_b = Fraction(n_b, total_b)
            change_c = (p_a + p_b) * (m_b - m_a) / 2
            mix_c = (p_b - p_a) * (m_a + m_b) / 2
            cost_change += change_c
            composition += mix_c
            customer_rows.append(
                {
                    "customer": customer,
                    "a": {
                        "tickets": n_a,
                        "cost_cents": cost_a,
                        "mean_cost_cents": _rat(m_a),
                        "ticket_share": _rat(p_a),
                    },
                    "b": {
                        "tickets": n_b,
                        "cost_cents": cost_b,
                        "mean_cost_cents": _rat(m_b),
                        "ticket_share": _rat(p_b),
                    },
                    "cost_change_contribution_cents": _rat(change_c),
                    "composition_contribution_cents": _rat(mix_c),
                }
            )
        attribution = {
            "status": "comparable",
            "mean_cost_cents": {"a": _rat(mean_a), "b": _rat(mean_b)},
            "difference_cents": _rat(mean_b - mean_a),
            "cost_change_cents": _rat(cost_change),
            "composition_cents": _rat(composition),
        }
    else:
        customer_rows = []
        attribution = {
            "status": "insufficient_common_support",
            "mean_cost_cents": {"a": None, "b": None},
            "difference_cents": None,
            "cost_change_cents": None,
            "composition_cents": None,
        }

    return {
        "periods": periods,
        "common_support": {
            "customers": len(common),
            "coverage": coverage,
            "attribution": attribution,
            "customers": customer_rows,
        },
        "exclusive": {
            "a_only": _exclusive(rolls["a"], only_a),
            "b_only": _exclusive(rolls["b"], only_b),
        },
    }


# ---------------------------------------------------------------------------
# Evidence digest: documented canonical form, recomputable from the report
# ---------------------------------------------------------------------------


def _evidence_digest(evidence_tickets: list[dict]) -> str:
    """SHA256 over the canonical serialization of the per-ticket evidence.

    Canonical form (documented in docs/contract.md): the evidence ticket
    array -- already sorted by exact ticket_id -- serialized as
    ``json.dumps(tickets, sort_keys=True, separators=(",", ":"),
    ensure_ascii=True)`` and hashed as ASCII bytes.  This is a logical
    digest of the consumed facts, never the SQLite file bytes.
    """
    canonical = json.dumps(
        evidence_tickets, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")
    return hashlib.sha256(canonical).hexdigest()


# ---------------------------------------------------------------------------
# Output safety
# ---------------------------------------------------------------------------


def _check_output_not_input(database: Path, output: Path) -> None:
    """Reject any recognizable alias of the input database as output."""
    if os.path.realpath(output) == os.path.realpath(database):
        raise CompareError(
            f"output {output} must not overwrite the input database {database}"
        )
    try:
        if os.path.samefile(output, database):
            raise CompareError(
                f"output {output} is a hard link or alias of the input "
                f"database {database}"
            )
    except FileNotFoundError:
        pass  # output (or a path component) does not exist yet: no alias.


def _write_report(output: Path, payload: bytes) -> None:
    """Write to a same-directory temp file, close it, then replace the target."""
    output.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(
        prefix=".compare-report-", suffix=".tmp", dir=output.parent
    )
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
        os.replace(name, output)
    finally:
        Path(name).unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def compare(database, a_start, a_end, b_start, b_end, output) -> dict:
    """Build the two-period attribution report and atomically write it.

    ``database`` is an existing v1 or ``inherited_contracts_v2`` warehouse
    opened read-only; the four dates are strict ``YYYY-MM-DD`` strings
    delimiting the half-open periods A and B (labels, not inferred order);
    ``output`` is the JSON report path.  Returns the report dict.
    """
    a_s, a_e, b_s, b_e = _validate_periods(a_start, a_end, b_start, b_end)
    database = Path(database)
    output = Path(output)
    _check_output_not_input(database, output)

    with closing(_open_readonly(database)) as db:
        mode, tickets = _read_snapshot(db)

    # A ticket enters at most one period (periods are disjoint by validation).
    grouped: dict[str, dict[str, list[FactTicket]]] = {
        group: {"a": [], "b": []} for group in GROUPS
    }
    evidence: list[dict] = []
    for ticket in tickets:
        if a_s <= ticket.opened < a_e:
            period = "a"
        elif b_s <= ticket.opened < b_e:
            period = "b"
        else:
            continue
        grouped["all"][period].append(ticket)
        grouped[ticket.tier][period].append(ticket)
        evidence.append(
            {
                "ticket_id": ticket.ticket_id,
                "customer": ticket.customer,
                "opened": ticket.opened.isoformat(),
                "cost_cents": ticket.cost_cents,
                "period": period,
                "tier": ticket.tier,
                "version_id": ticket.version_id,
                "contract_holder": ticket.contract_holder,
                "match_depth": ticket.match_depth,
            }
        )
    evidence.sort(key=lambda row: row["ticket_id"])

    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "interpretation": INTERPRETATION,
        "input": {
            "database_schema": mode,
            "periods": {
                "a": {"start": a_start, "end": a_end},
                "b": {"start": b_start, "end": b_end},
            },
        },
        "groups": {
            group: _analyze_group(grouped[group]["a"], grouped[group]["b"])
            for group in GROUPS
        },
        "evidence": {
            "sha256": _evidence_digest(evidence),
            "tickets": evidence,
        },
    }

    # Fully serialize before touching the output location.
    payload = (json.dumps(report, sort_keys=True, ensure_ascii=False, indent=2) + "\n").encode(
        "utf-8"
    )
    _write_report(output, payload)
    return report


def _summary(report: dict, output: Path) -> dict:
    return {
        "schema_version": report["schema_version"],
        "database_schema": report["input"]["database_schema"],
        "output": str(output),
        "groups": {
            group: {
                "a_tickets": block["periods"]["a"]["tickets"],
                "b_tickets": block["periods"]["b"]["tickets"],
                "attribution_status": block["common_support"]["attribution"]["status"],
                "difference_cents": block["common_support"]["attribution"][
                    "difference_cents"
                ],
            }
            for group, block in report["groups"].items()
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Two-period customer-mix cost attribution report"
    )
    parser.add_argument("--database", required=True)
    parser.add_argument("--a-start", required=True)
    parser.add_argument("--a-end", required=True)
    parser.add_argument("--b-start", required=True)
    parser.add_argument("--b-end", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    try:
        report = compare(
            args.database,
            args.a_start,
            args.a_end,
            args.b_start,
            args.b_end,
            args.output,
        )
    except (CompareError, OSError, sqlite3.Error, OverflowError) as exc:
        parser.exit(2, f"{exc}\n")
    try:
        # The report is already safely replaced; a summary print failure
        # never rolls it back.
        print(json.dumps(_summary(report, Path(args.output)), sort_keys=True))
    except OSError as exc:
        parser.exit(1, f"report written but failed to print summary: {exc}\n")


if __name__ == "__main__":
    main()
