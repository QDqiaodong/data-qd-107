"""Inherited (v2) warehouse entry point: historical hierarchy + nearest contract.

Independent of the two-file baseline module ``build.py``.  Reads four UTF-8
CSVs (customers, hierarchy, contracts, tickets), validates the whole input
*before* writing anything, materializes the historical ancestor bridge into
SQLite, and resolves each ticket's nearest valid contract by its ``opened``
date while preserving the distinction between the served customer and the
contract holder.

Single-writer, full-rebuild semantics: all dimensions, bridge tables, facts,
audit and aggregates are written into a temporary SQLite file in the target
directory; foreign keys are enabled and explicitly checked; only then is the
target atomically replaced.  Any failure aborts before replacement, the prior
target stays byte-identical and the temporary file is removed.
"""

from __future__ import annotations

import argparse
import json
import os
from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile

from .hierarchy import (
    SQLITE_INT_MAX,
    BridgeRow,
    InputBundle,
    ValidationError,
    build_bridge,
    load_inherited,
    select_contract,
)

SCHEMA_FAMILY = "inherited_contracts_v2"
SCHEMA_VERSION = "2"

SCHEMA = """
CREATE TABLE wh_meta(meta_key TEXT PRIMARY KEY, meta_value TEXT NOT NULL);

CREATE TABLE ods_source_file(
 source_role TEXT PRIMARY KEY, file_name TEXT NOT NULL, sha256 TEXT NOT NULL,
 byte_length INTEGER NOT NULL, raw_bytes BLOB NOT NULL);
CREATE TABLE ods_customers(row_number INTEGER PRIMARY KEY, payload TEXT NOT NULL);
CREATE TABLE ods_hierarchy(row_number INTEGER PRIMARY KEY, payload TEXT NOT NULL);
CREATE TABLE ods_contracts(row_number INTEGER PRIMARY KEY, payload TEXT NOT NULL);
CREATE TABLE ods_tickets(row_number INTEGER PRIMARY KEY, payload TEXT NOT NULL);

CREATE TABLE dim_customer(
 customer TEXT PRIMARY KEY, source_row_number INTEGER NOT NULL);
CREATE TABLE dim_hierarchy_edge(
 edge_sk INTEGER PRIMARY KEY, edge_id TEXT UNIQUE NOT NULL,
 customer TEXT NOT NULL REFERENCES dim_customer(customer),
 parent TEXT NOT NULL REFERENCES dim_customer(customer),
 valid_from TEXT NOT NULL, valid_to TEXT,
 source_row_number INTEGER NOT NULL,
 CHECK(customer <> parent),
 CHECK(valid_to IS NULL OR valid_from < valid_to));
CREATE TABLE dim_contract(
 contract_sk INTEGER PRIMARY KEY, version_id TEXT UNIQUE NOT NULL,
 customer TEXT NOT NULL REFERENCES dim_customer(customer),
 valid_from TEXT NOT NULL, valid_to TEXT, tier TEXT NOT NULL CHECK(tier IN ('basic','premium')),
 source_row_number INTEGER NOT NULL,
 UNIQUE(contract_sk, customer),
 CHECK(valid_to IS NULL OR valid_from < valid_to));

CREATE TABLE bridge_hierarchy_ancestor(
 bridge_sk INTEGER PRIMARY KEY,
 descendant TEXT NOT NULL REFERENCES dim_customer(customer),
 ancestor TEXT NOT NULL REFERENCES dim_customer(customer),
 depth INTEGER NOT NULL CHECK(depth >= 1),
 edge_path_json TEXT NOT NULL,
 valid_from TEXT NOT NULL, valid_to TEXT,
 CHECK(valid_to IS NULL OR valid_from < valid_to));
CREATE TABLE bridge_hierarchy_path(
 bridge_sk INTEGER NOT NULL REFERENCES bridge_hierarchy_ancestor(bridge_sk),
 step INTEGER NOT NULL CHECK(step >= 1),
 edge_id TEXT NOT NULL REFERENCES dim_hierarchy_edge(edge_id),
 PRIMARY KEY(bridge_sk, step));

CREATE TABLE dwd_ticket(
 ticket_id TEXT PRIMARY KEY,
 customer TEXT NOT NULL REFERENCES dim_customer(customer),
 opened TEXT NOT NULL, cost_cents INTEGER NOT NULL CHECK(cost_cents >= 0),
 match_depth INTEGER,
 contract_sk INTEGER,
 contract_holder TEXT,
 FOREIGN KEY(contract_sk, contract_holder)
  REFERENCES dim_contract(contract_sk, customer),
 CHECK((contract_sk IS NULL AND contract_holder IS NULL AND match_depth IS NULL)
    OR (contract_sk IS NOT NULL AND contract_holder IS NOT NULL AND match_depth IS NOT NULL)),
 CHECK(match_depth IS NULL OR match_depth >= 0));

CREATE TABLE audit_ticket_match(
 ticket_id TEXT NOT NULL REFERENCES dwd_ticket(ticket_id),
 depth INTEGER NOT NULL CHECK(depth >= 0),
 customer TEXT NOT NULL REFERENCES dim_customer(customer),
 entry_edge_id TEXT REFERENCES dim_hierarchy_edge(edge_id),
 status TEXT NOT NULL CHECK(status IN ('selected','no_active_contract')),
 contract_sk INTEGER REFERENCES dim_contract(contract_sk),
 version_id TEXT REFERENCES dim_contract(version_id),
 tier TEXT,
 source_contract_row INTEGER,
 PRIMARY KEY(ticket_id, depth),
 CHECK((depth = 0 AND entry_edge_id IS NULL) OR depth > 0),
 CHECK(depth = 0 OR entry_edge_id IS NOT NULL),
 CHECK((status = 'selected' AND contract_sk IS NOT NULL)
    OR (status = 'no_active_contract' AND contract_sk IS NULL
        AND version_id IS NULL AND tier IS NULL AND source_contract_row IS NULL)));

CREATE TABLE ads_tier_cost(
 tier TEXT PRIMARY KEY, tickets INTEGER NOT NULL, cost_cents INTEGER NOT NULL);
CREATE TABLE ads_contract_owner_cost(
 owner_sk INTEGER PRIMARY KEY,
 contract_holder TEXT REFERENCES dim_customer(customer),
 tier TEXT NOT NULL, tickets INTEGER NOT NULL, cost_cents INTEGER NOT NULL,
 UNIQUE(contract_holder, tier));
"""


def _iso(value) -> str | None:
    return None if value is None else value.isoformat()


def _check_foreign_keys(db: sqlite3.Connection) -> None:
    """Explicit constraint pass; isolated as a seam for failure-injection tests."""
    failures = db.execute("PRAGMA foreign_key_check").fetchall()
    if failures:
        raise sqlite3.IntegrityError(f"foreign key check failed: {failures!r}")


def _insert_bridge(db: sqlite3.Connection, bridge: list[BridgeRow]) -> int:
    """Persist bridge rows plus the ordered edge path detail (test seam)."""
    for bridge_sk, row in enumerate(bridge, 1):
        db.execute(
            "INSERT INTO bridge_hierarchy_ancestor VALUES (?,?,?,?,?,?,?)",
            (
                bridge_sk,
                row.descendant,
                row.ancestor,
                row.depth,
                json.dumps(list(row.edge_path), separators=(",", ":")),
                row.valid_from.isoformat(),
                _iso(row.valid_to),
            ),
        )
        db.executemany(
            "INSERT INTO bridge_hierarchy_path(bridge_sk, step, edge_id) VALUES (?,?,?)",
            [(bridge_sk, step, edge_id) for step, edge_id in enumerate(row.edge_path, 1)],
        )
    return len(bridge)


def _write_ticket(db: sqlite3.Connection, ticket, match, contract_sk: dict) -> None:
    """Persist one ticket fact plus its layer audit (test-injection seam)."""
    if match.contract is None:
        db.execute(
            "INSERT INTO dwd_ticket VALUES (?,?,?,?,?,?,?)",
            (
                ticket.ticket_id,
                ticket.customer,
                ticket.opened.isoformat(),
                ticket.cost_cents,
                None,
                None,
                None,
            ),
        )
    else:
        sk = contract_sk[match.contract.version_id]
        db.execute(
            "INSERT INTO dwd_ticket VALUES (?,?,?,?,?,?,?)",
            (
                ticket.ticket_id,
                ticket.customer,
                ticket.opened.isoformat(),
                ticket.cost_cents,
                match.depth,
                sk,
                match.holder,
            ),
        )
    # Layer-by-layer audit, depth 0 (self) first.
    for layer in match.layers:
        if match.contract is not None and layer.customer == match.holder and (
            layer.depth == match.depth
        ):
            contract = match.contract
            db.execute(
                "INSERT INTO audit_ticket_match VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    ticket.ticket_id,
                    layer.depth,
                    layer.customer,
                    layer.entry_edge_id,
                    "selected",
                    contract_sk[contract.version_id],
                    contract.version_id,
                    contract.tier,
                    contract.row_number,
                ),
            )
        else:
            db.execute(
                "INSERT INTO audit_ticket_match VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    ticket.ticket_id,
                    layer.depth,
                    layer.customer,
                    layer.entry_edge_id,
                    "no_active_contract",
                    None,
                    None,
                    None,
                    None,
                ),
            )


def _checked_add(running: int, amount: int, what: str) -> int:
    total = running + amount
    if total > SQLITE_INT_MAX or total < -(2**63):
        raise OverflowError(f"{what} exceeds the exact integer range storable in SQLite")
    return total


def build(source, target):
    # Phase 1: capture raw bytes once each, parse and validate everything,
    # including the full relationship history and components without tickets.
    bundle: InputBundle = load_inherited(source)
    bridge = build_bridge(bundle.edges)

    target = Path(target)
    target_resolved = target.resolve(strict=False)
    for source_file in bundle.files.values():
        if target_resolved == source_file.path.resolve():
            raise ValidationError(
                "output database must not point at an input file: "
                f"{source_file.role} ({source_file.path.name})"
            )
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=".inherited-warehouse-", suffix=".tmp", dir=target.parent)
    os.close(fd)
    try:
        with closing(sqlite3.connect(name)) as db, db:
            db.execute("PRAGMA foreign_keys=ON")
            db.executescript(SCHEMA)
            db.executemany(
                "INSERT INTO wh_meta VALUES (?,?)",
                [
                    ("schema_family", SCHEMA_FAMILY),
                    ("schema_version", SCHEMA_VERSION),
                ],
            )

            # ODS: the exact captured bytes plus ordered JSON row payloads.
            db.executemany(
                "INSERT INTO ods_source_file VALUES (?,?,?,?,?)",
                [
                    (
                        role,
                        source_file.path.name,
                        source_file.sha256,
                        len(source_file.raw),
                        sqlite3.Binary(source_file.raw),
                    )
                    for role, source_file in sorted(bundle.files.items())
                ],
            )
            for role in ("customers", "hierarchy", "contracts", "tickets"):
                db.executemany(
                    f"INSERT INTO ods_{role} VALUES (?,?)",
                    [
                        (row_number, json.dumps(row, sort_keys=True))
                        for row_number, row in enumerate(bundle.rows[role], 1)
                    ],
                )

            # Dimensions with stable surrogate keys.
            for customer, row_number in sorted(bundle.customers):
                db.execute("INSERT INTO dim_customer VALUES (?,?)", (customer, row_number))
            edge_sk = {
                edge.edge_id: sk
                for sk, edge in enumerate(sorted(bundle.edges, key=lambda edge: edge.edge_id), 1)
            }
            for edge in sorted(bundle.edges, key=lambda edge: edge.edge_id):
                db.execute(
                    "INSERT INTO dim_hierarchy_edge VALUES (?,?,?,?,?,?,?)",
                    (
                        edge_sk[edge.edge_id],
                        edge.edge_id,
                        edge.customer,
                        edge.parent,
                        edge.valid_from.isoformat(),
                        _iso(edge.valid_to),
                        edge.row_number,
                    ),
                )
            contract_sk: dict[str, int] = {}
            contracts_sorted = sorted(bundle.contracts, key=lambda contract: contract.version_id)
            for sk, contract in enumerate(contracts_sorted, 1):
                contract_sk[contract.version_id] = sk
                db.execute(
                    "INSERT INTO dim_contract VALUES (?,?,?,?,?,?,?)",
                    (
                        sk,
                        contract.version_id,
                        contract.customer,
                        contract.valid_from.isoformat(),
                        _iso(contract.valid_to),
                        contract.tier,
                        contract.row_number,
                    ),
                )

            # Historical ancestor bridge, reusable by plain SQL.
            _insert_bridge(db, bridge)

            # Facts: one row per ticket, served customer never rewritten.
            covered = 0
            for ticket in bundle.tickets:
                match = select_contract(bundle, ticket)
                if match.contract is not None:
                    covered += 1
                _write_ticket(db, ticket, match, contract_sk)

            # ADS computed solely from the one-row-per-ticket final facts.
            tier_groups: dict[str, list[int]] = {}
            owner_groups: dict[tuple[str | None, str], list[int]] = {}
            fact_rows = db.execute(
                "SELECT t.cost_cents, c.tier, c.customer "
                "FROM dwd_ticket t LEFT JOIN dim_contract c USING(contract_sk) "
                "ORDER BY t.ticket_id"
            ).fetchall()
            for cost, tier, holder in fact_rows:
                tier_key = tier if tier is not None else "uncovered"
                bucket = tier_groups.setdefault(tier_key, [0, 0])
                bucket[0] += 1
                bucket[1] = _checked_add(bucket[1], cost, "ads_tier_cost.cost_cents")
                owner_bucket = owner_groups.setdefault((holder, tier_key), [0, 0])
                owner_bucket[0] += 1
                owner_bucket[1] = _checked_add(
                    owner_bucket[1], cost, "ads_contract_owner_cost.cost_cents"
                )
            for tier, (count, cost) in sorted(tier_groups.items()):
                db.execute(
                    "INSERT INTO ads_tier_cost VALUES (?,?,?)", (tier, count, cost)
                )
            for owner_sk, ((holder, tier), (count, cost)) in enumerate(
                sorted(owner_groups.items(), key=lambda item: ((item[0][0] is None), item[0])),
                1,
            ):
                db.execute(
                    "INSERT INTO ads_contract_owner_cost VALUES (?,?,?,?,?)",
                    (owner_sk, holder, tier, count, cost),
                )

            _check_foreign_keys(db)

        # Phase 2: only a fully written, checked, closed database is swapped in.
        os.replace(name, target)
    finally:
        Path(name).unlink(missing_ok=True)

    with closing(sqlite3.connect(target)) as db:
        tiers = [
            {"tier": tier, "tickets": count, "cost_cents": cost}
            for tier, count, cost in db.execute(
                "SELECT tier, tickets, cost_cents FROM ads_tier_cost ORDER BY tier"
            )
        ]
        owners = [
            {
                "contract_holder": holder,
                "tier": tier,
                "tickets": count,
                "cost_cents": cost,
            }
            for holder, tier, count, cost in db.execute(
                "SELECT contract_holder, tier, tickets, cost_cents "
                "FROM ads_contract_owner_cost ORDER BY owner_sk"
            )
        ]
    return {
        "schema": SCHEMA_FAMILY,
        "schema_version": SCHEMA_VERSION,
        "customers": len(bundle.customers),
        "edges": len(bundle.edges),
        "contracts": len(bundle.contracts),
        "tickets": len(bundle.tickets),
        "bridge_paths": len(bridge),
        "covered": covered,
        "uncovered": len(bundle.tickets) - covered,
        "tiers": tiers,
        "owners": owners,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build the inherited-contract (v2) hierarchy warehouse"
    )
    parser.add_argument("--source", required=True)
    parser.add_argument("--database", required=True)
    args = parser.parse_args()
    try:
        result = build(args.source, args.database)
    except (ValidationError, OSError, sqlite3.Error, OverflowError) as exc:
        parser.exit(2, f"{exc}\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
