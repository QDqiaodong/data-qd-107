"""Historical customer hierarchy: parsing, temporal validation, bridge paths.

This module is the pure, deterministic, standard-library-only transformation
layer used by the inherited (v2) warehouse entry point.  It never opens a
database and never depends on the current wall clock.

Interval convention
-------------------
All validity intervals are half open ``[valid_from, valid_to)``.  ``None``
represents an open (unbounded) ``valid_to``.  Every algorithm below works on
interval endpoints and interval intersections only; dates are never expanded
day by day and open-ended relationships are never truncated at an artificial
cutoff.
"""

from __future__ import annotations

import csv
import hashlib
import io
from dataclasses import dataclass
from datetime import date
from pathlib import Path

CONTRACT_FIELDS = ["version_id", "customer", "valid_from", "valid_to", "tier"]
TICKET_FIELDS = ["ticket_id", "customer", "opened", "cost_cents"]
CUSTOMER_FIELDS = ["customer"]
HIERARCHY_FIELDS = ["edge_id", "customer", "parent", "valid_from", "valid_to"]

# SQLite stores signed 64-bit integers; anything larger must fail loudly
# instead of being silently coerced to a floating point value.
SQLITE_INT_MAX = 2**63 - 1


class ValidationError(ValueError):
    """Raised for any malformed input or violated business invariant."""


# ---------------------------------------------------------------------------
# Small value objects
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Edge:
    """One stored hierarchy relationship fact (one hierarchy.csv row)."""

    edge_id: str
    customer: str
    parent: str
    valid_from: date
    valid_to: date | None
    row_number: int


@dataclass(frozen=True)
class Contract:
    version_id: str
    customer: str
    valid_from: date
    valid_to: date | None
    tier: str
    row_number: int


@dataclass(frozen=True)
class Ticket:
    ticket_id: str
    customer: str
    opened: date
    cost_cents: int
    row_number: int


@dataclass(frozen=True)
class BridgeRow:
    """One materialized ancestor path, valid over one maximal interval."""

    descendant: str
    ancestor: str
    depth: int
    edge_path: tuple[str, ...]
    valid_from: date
    valid_to: date | None


@dataclass(frozen=True)
class SourceFile:
    role: str
    path: Path
    raw: bytes
    sha256: str


@dataclass(frozen=True)
class InputBundle:
    files: dict[str, SourceFile]
    customers: list[tuple[str, int]]  # (customer, source row number)
    edges: list[Edge]
    contracts: list[Contract]
    tickets: list[Ticket]
    # Raw ordered CSV row payloads, keyed by logical file role.  Row numbers
    # are 1-based data record numbers (the header line is not counted).
    rows: dict[str, list[dict[str, str]]]


# ---------------------------------------------------------------------------
# Primitive validation helpers
# ---------------------------------------------------------------------------


def parse_day(text: str, what: str, row_number: int) -> date:
    if text is None or text == "":
        raise ValidationError(f"{what} row {row_number}: empty date")
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise ValidationError(
            f"{what} row {row_number}: date must be strict YYYY-MM-DD, got {text!r}"
        ) from exc
    if parsed.isoformat() != text:
        raise ValidationError(
            f"{what} row {row_number}: date must be strict YYYY-MM-DD, got {text!r}"
        )
    return parsed


def check_identifier(value: str, what: str, row_number: int) -> str:
    """Business keys are non-empty, case sensitive, with no surrounding space."""
    if value is None or value == "":
        raise ValidationError(f"{what} row {row_number}: empty identifier")
    if value != value.strip():
        raise ValidationError(
            f"{what} row {row_number}: identifier has leading or trailing whitespace: {value!r}"
        )
    return value


def intersect(a: tuple[date, date | None], b: tuple[date, date | None]):
    """Intersection of two half-open intervals; ``None`` when empty."""
    lo = max(a[0], b[0])
    if a[1] is None:
        hi = b[1]
    elif b[1] is None:
        hi = a[1]
    else:
        hi = min(a[1], b[1])
    if hi is not None and lo >= hi:
        return None
    return lo, hi


def _end_key(end: date | None) -> date:
    return date.max if end is None else end


# ---------------------------------------------------------------------------
# CSV capture (raw bytes are read exactly once per file)
# ---------------------------------------------------------------------------


def read_source(path: Path, role: str, fields: list[str], allow_empty: bool):
    raw = path.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValidationError(f"{role}: file is not valid UTF-8") from exc
    reader = csv.DictReader(io.StringIO(text, newline=""))
    if reader.fieldnames != fields:
        raise ValidationError(
            f"{role}: unexpected columns {reader.fieldnames!r}, expected {fields!r}"
        )
    rows = list(reader)
    if not allow_empty and not rows:
        raise ValidationError(f"{role}: input must contain at least one data row")
    for index, row in enumerate(rows, 1):
        if None in row or any(value is None for value in row.values()):
            raise ValidationError(f"{role} row {index}: ragged CSV row")
    return SourceFile(role=role, path=path, raw=raw, sha256=digest), rows


# ---------------------------------------------------------------------------
# Full input validation and parsing
# ---------------------------------------------------------------------------


def load_inherited(source_dir: str | Path) -> InputBundle:
    root = Path(source_dir)
    spec = [
        ("customers", root / "customers.csv", CUSTOMER_FIELDS, False),
        ("hierarchy", root / "hierarchy.csv", HIERARCHY_FIELDS, True),
        ("contracts", root / "contracts.csv", CONTRACT_FIELDS, False),
        ("tickets", root / "tickets.csv", TICKET_FIELDS, False),
    ]
    files: dict[str, SourceFile] = {}
    all_rows: dict[str, list[dict[str, str]]] = {}
    for role, path, fields, allow_empty in spec:
        if not path.is_file():
            raise ValidationError(f"{role}: required input file not found: {path.name}")
        source_file, rows = read_source(path, role, fields, allow_empty)
        files[role] = source_file
        all_rows[role] = rows

    # --- customers -------------------------------------------------------
    customers: list[tuple[str, int]] = []
    known_customers: set[str] = set()
    for row_number, row in enumerate(all_rows["customers"], 1):
        name = check_identifier(row["customer"], "customers", row_number)
        if name in known_customers:
            raise ValidationError(f"customers row {row_number}: duplicate customer {name!r}")
        known_customers.add(name)
        customers.append((name, row_number))
    if not customers:
        raise ValidationError("customers: input must contain at least one data row")

    def require_customer(value: str, what: str, row_number: int) -> str:
        check_identifier(value, what, row_number)
        if value not in known_customers:
            raise ValidationError(
                f"{what} row {row_number}: unknown customer {value!r}; "
                "every endpoint must be registered in customers.csv"
            )
        return value

    # --- hierarchy edges -------------------------------------------------
    edges: list[Edge] = []
    seen_edge_ids: set[str] = set()
    for row_number, row in enumerate(all_rows["hierarchy"], 1):
        edge_id = check_identifier(row["edge_id"], "hierarchy", row_number)
        if edge_id in seen_edge_ids:
            raise ValidationError(f"hierarchy row {row_number}: duplicate edge_id {edge_id!r}")
        seen_edge_ids.add(edge_id)
        child = require_customer(row["customer"], "hierarchy", row_number)
        parent = require_customer(row["parent"], "hierarchy", row_number)
        start = parse_day(row["valid_from"], "hierarchy", row_number)
        end = (
            parse_day(row["valid_to"], "hierarchy", row_number)
            if row["valid_to"] != ""
            else None
        )
        if end is not None and start >= end:
            raise ValidationError(
                f"hierarchy row {row_number} edge {edge_id!r}: "
                "finite valid_to must be later than valid_from"
            )
        if child == parent:
            raise ValidationError(
                f"hierarchy row {row_number} edge {edge_id!r}: self reference is not allowed"
            )
        edges.append(Edge(edge_id, child, parent, start, end, row_number))

    _validate_parent_intervals(edges)
    _validate_temporal_forest(edges)

    # --- contracts -------------------------------------------------------
    contracts: list[Contract] = []
    seen_versions: set[str] = set()
    contract_intervals: dict[str, list[tuple[date, date | None]]] = {}
    for row_number, row in enumerate(all_rows["contracts"], 1):
        version_id = check_identifier(row["version_id"], "contracts", row_number)
        if version_id in seen_versions:
            raise ValidationError(
                f"contracts row {row_number}: duplicate version_id {version_id!r}"
            )
        seen_versions.add(version_id)
        customer = require_customer(row["customer"], "contracts", row_number)
        if row["tier"] not in ("basic", "premium"):
            raise ValidationError(
                f"contracts row {row_number}: unknown tier {row['tier']!r}"
            )
        start = parse_day(row["valid_from"], "contracts", row_number)
        end = (
            parse_day(row["valid_to"], "contracts", row_number)
            if row["valid_to"] != ""
            else None
        )
        if end is not None and start >= end:
            raise ValidationError(
                f"contracts row {row_number} version {version_id!r}: "
                "finite valid_to must be later than valid_from"
            )
        contract_intervals.setdefault(customer, []).append((start, end))
        contracts.append(
            Contract(version_id, customer, start, end, row["tier"], row_number)
        )
    for customer, spans in contract_intervals.items():
        spans.sort(key=lambda span: (span[0], _end_key(span[1])))
        for (_, prev_end), (next_start, _) in zip(spans, spans[1:]):
            if prev_end is None or prev_end > next_start:
                raise ValidationError(
                    f"contracts: overlapping validity intervals for customer {customer!r}"
                )

    # --- tickets ---------------------------------------------------------
    tickets: list[Ticket] = []
    seen_tickets: set[str] = set()
    for row_number, row in enumerate(all_rows["tickets"], 1):
        ticket_id = check_identifier(row["ticket_id"], "tickets", row_number)
        if ticket_id in seen_tickets:
            raise ValidationError(
                f"tickets row {row_number}: duplicate ticket_id {ticket_id!r}"
            )
        seen_tickets.add(ticket_id)
        customer = require_customer(row["customer"], "tickets", row_number)
        opened = parse_day(row["opened"], "tickets", row_number)
        cost_text = row["cost_cents"]
        if not cost_text.isascii() or not cost_text.isdigit():
            raise ValidationError(
                f"tickets row {row_number}: cost_cents must be a non-negative integer"
            )
        cost = int(cost_text)
        if cost > SQLITE_INT_MAX:
            raise ValidationError(
                f"tickets row {row_number}: cost_cents exceeds the storable integer range"
            )
        tickets.append(Ticket(ticket_id, customer, opened, cost, row_number))

    return InputBundle(
        files=files,
        customers=customers,
        edges=edges,
        contracts=contracts,
        tickets=tickets,
        rows=all_rows,
    )


def _validate_parent_intervals(edges: list[Edge]) -> None:
    """One customer cannot have two simultaneously valid parents.

    Applies to the whole relationship history, including edges sharing the
    same parent.  Adjacent intervals and gaps are legal.
    """
    by_child: dict[str, list[Edge]] = {}
    for edge in edges:
        by_child.setdefault(edge.customer, []).append(edge)
    for child, child_edges in by_child.items():
        child_edges.sort(key=lambda edge: (edge.valid_from, _end_key(edge.valid_to)))
        for earlier, later in zip(child_edges, child_edges[1:]):
            if earlier.valid_to is None or earlier.valid_to > later.valid_from:
                raise ValidationError(
                    f"hierarchy: customer {child!r} has overlapping parent edges "
                    f"{earlier.edge_id!r} (row {earlier.row_number}) and "
                    f"{later.edge_id!r} (row {later.row_number})"
                )


def _validate_temporal_forest(edges: list[Edge]) -> None:
    """Reject self references/cycles whenever they are simultaneously valid.

    The union of historical edges is not checked statically: edges that never
    overlap in time may point in opposite directions legally.  Instead the
    timeline is cut at every relationship endpoint; within each resulting
    segment the active edge set is constant and must form a forest.
    """
    if not edges:
        return
    points = {edge.valid_from for edge in edges}
    points.update(edge.valid_to for edge in edges if edge.valid_to is not None)
    timeline = sorted(points)
    # Finite segments between consecutive endpoints, plus one final open
    # segment beginning at the last endpoint so open-ended relationships are
    # validated without any artificial cutoff.
    segments = list(zip(timeline, timeline[1:])) + [(timeline[-1], None)]

    def active_throughout(edge, segment_start, segment_end):
        if edge.valid_from > segment_start:
            return False
        if edge.valid_to is None:
            return True
        if segment_end is None:
            return edge.valid_to > segment_start
        return edge.valid_to >= segment_end

    for segment_start, segment_end in segments:
        active = [
            edge
            for edge in edges
            if active_throughout(edge, segment_start, segment_end)
        ]
        parent_of: dict[str, Edge] = {}
        for edge in active:
            if edge.customer in parent_of:
                # Defensive: the per-customer overlap check should have caught
                # this already, but keep the forest invariant explicit.
                other = parent_of[edge.customer]
                raise ValidationError(
                    f"hierarchy: customer {edge.customer!r} has two active parents on "
                    f"[{segment_start}, {segment_end}): edges {other.edge_id!r} and "
                    f"{edge.edge_id!r}"
                )
            parent_of[edge.customer] = edge
        cycle = _find_cycle(parent_of)
        if cycle is not None:
            cycle_edges, cycle_nodes = cycle
            overlap_start = max(edge.valid_from for edge in cycle_edges)
            finite_ends = [edge.valid_to for edge in cycle_edges if edge.valid_to is not None]
            overlap_end = min(finite_ends) if finite_ends else None
            raise ValidationError(
                "hierarchy: cycle simultaneously valid on "
                f"[{overlap_start}, {overlap_end or 'open'}): "
                + " -> ".join(cycle_nodes)
                + " via edges ["
                + ", ".join(
                    f"{edge.edge_id!r} row {edge.row_number}" for edge in cycle_edges
                )
                + "]"
            )


def _find_cycle(parent_of: dict[str, Edge]):
    """Follow each node's unique active parent chain and report the first loop."""
    for start in parent_of:
        seen: dict[str, int] = {}
        ordered_nodes: list[str] = []
        current = start
        while current in parent_of:
            if current in seen:
                loop_nodes = ordered_nodes[seen[current]:]
                loop_edges = [parent_of[node] for node in loop_nodes]
                return loop_edges, loop_nodes + [current]
            seen[current] = len(ordered_nodes)
            ordered_nodes.append(current)
            current = parent_of[current].parent
    return None


# ---------------------------------------------------------------------------
# Ancestor bridge construction
# ---------------------------------------------------------------------------


def build_bridge(edges: list[Edge]) -> list[BridgeRow]:
    """Materialize every non-trivial ancestor path with its maximal intervals.

    Depth-1 rows are the relationship facts themselves.  A depth-N path is
    extended one edge at a time; its interval is the intersection of the
    intervals of all edges on the path, and empty intersections are discarded.
    Equal complete paths (same ordered edge ids) are merged into the largest
    continuous interval; paths that differ in any edge stay separate even when
    descendant, ancestor and depth happen to coincide.
    """
    # (descendant, ancestor, depth, edge ids, start, end)
    current: list[tuple[str, str, int, tuple[str, ...], date, date | None]] = [
        (edge.customer, edge.parent, 1, (edge.edge_id,), edge.valid_from, edge.valid_to)
        for edge in edges
    ]
    produced = list(current)
    edge_by_child: dict[str, list[Edge]] = {}
    for edge in edges:
        edge_by_child.setdefault(edge.customer, []).append(edge)

    guard = len(edges)
    while current:
        nxt: list[tuple[str, str, int, tuple[str, ...], date, date | None]] = []
        for descendant, ancestor, depth, path, start, end in current:
            for edge in edge_by_child.get(ancestor, ()):
                overlap = intersect((start, end), (edge.valid_from, edge.valid_to))
                if overlap is None:
                    continue
                nxt.append(
                    (
                        descendant,
                        edge.parent,
                        depth + 1,
                        path + (edge.edge_id,),
                        overlap[0],
                        overlap[1],
                    )
                )
        if not nxt:
            break
        if any(item[2] > guard for item in nxt):
            # A temporal forest has max depth < number of edges; reaching here
            # means validation let a cycle through, which must never happen.
            raise ValidationError("hierarchy: ancestor path exceeded edge count; possible cycle")
        produced.extend(nxt)
        current = nxt

    grouped: dict[tuple[str, str, int, tuple[str, ...]], list[tuple[date, date | None]]] = {}
    for descendant, ancestor, depth, path, start, end in produced:
        grouped.setdefault((descendant, ancestor, depth, path), []).append((start, end))

    bridge: list[BridgeRow] = []
    for (descendant, ancestor, depth, path), intervals in grouped.items():
        intervals.sort(key=lambda interval: (interval[0], _end_key(interval[1])))
        merged: list[tuple[date, date | None]] = []
        for start, end in intervals:
            if merged and start <= _end_key(merged[-1][1]):
                prev_start, prev_end = merged[-1]
                if prev_end is None:
                    new_end = None
                elif end is None:
                    new_end = None
                else:
                    new_end = max(prev_end, end)
                merged[-1] = (prev_start, new_end)
            else:
                merged.append((start, end))
        for start, end in merged:
            bridge.append(BridgeRow(descendant, ancestor, depth, path, start, end))

    bridge.sort(
        key=lambda row: (
            row.descendant,
            row.ancestor,
            row.depth,
            row.edge_path,
            row.valid_from,
            _end_key(row.valid_to),
        )
    )
    return bridge


# ---------------------------------------------------------------------------
# Nearest-contract inheritance matching
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MatchLayer:
    depth: int
    customer: str
    entry_edge_id: str | None


@dataclass(frozen=True)
class MatchResult:
    layers: list[MatchLayer]
    contract: Contract | None
    depth: int | None
    holder: str | None


def active_parent_edge(edges: list[Edge], customer: str, on_day: date) -> Edge | None:
    matches = [
        edge
        for edge in edges
        if edge.customer == customer
        and edge.valid_from <= on_day
        and (edge.valid_to is None or on_day < edge.valid_to)
    ]
    if not matches:
        return None
    # Overlapping parent intervals are rejected up front, so at most one fits.
    return matches[0]


def active_contract(contracts: list[Contract], customer: str, on_day: date) -> Contract | None:
    matches = [
        contract
        for contract in contracts
        if contract.customer == customer
        and contract.valid_from <= on_day
        and (contract.valid_to is None or on_day < contract.valid_to)
    ]
    if not matches:
        return None
    # Overlapping holder intervals are rejected up front, so no tie rule exists.
    return matches[0]


def select_contract(bundle: InputBundle, ticket: Ticket) -> MatchResult:
    """Self first (depth 0), then the active parent chain, first holder wins."""
    edges_by_child: dict[str, list[Edge]] = {}
    for edge in bundle.edges:
        edges_by_child.setdefault(edge.customer, []).append(edge)
    contracts_by_customer: dict[str, list[Contract]] = {}
    for contract in bundle.contracts:
        contracts_by_customer.setdefault(contract.customer, []).append(contract)

    layers: list[MatchLayer] = []
    current = ticket.customer
    depth = 0
    entry_edge_id: str | None = None
    visited: set[str] = set()
    while True:
        layers.append(MatchLayer(depth, current, entry_edge_id))
        contract = active_contract(contracts_by_customer.get(current, []), current, ticket.opened)
        if contract is not None:
            return MatchResult(layers, contract, depth, current)
        if current in visited:
            raise ValidationError(
                f"tickets row {ticket.row_number}: parent chain cycles at {current!r}"
            )
        visited.add(current)
        edge = active_parent_edge(edges_by_child.get(current, []), current, ticket.opened)
        if edge is None:
            return MatchResult(layers, None, None, None)
        current = edge.parent
        depth += 1
        entry_edge_id = edge.edge_id
