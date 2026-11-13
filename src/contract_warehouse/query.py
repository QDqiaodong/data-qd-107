"""Restricted per-principal SQL query gateway for the v2 warehouse.

Outsourced maintenance staff run self-service SQL over the work-order facts,
but must never read other contract parties' rows.  This module adds a
restricted query API and CLI on top of an existing ``inherited_contracts_v2``
database without changing any existing interface.

Trust model
-----------

* The **caller is trusted**: it names the ``principal``, supplies the
  *strict JSON policy* (a principal -> ``contract_holder`` whitelist) and
  positive integer budgets (VM instruction steps, result rows).  The caller
  is expected to have authenticated the human operator; **authentication is
  out of scope** for this module.
* The **SQL text is untrusted**.  It is never executed against the real
  warehouse and never filtered by keyword matching.

Exposure and row-level rule
---------------------------

Exactly one relation is exposed: ``tickets(ticket_id, customer, opened,
cost_cents, contract_holder)``.  Its rows are the persisted ``dwd_ticket``
facts whose stored ``contract_holder`` is in the principal's whitelist --
nothing else.  A NULL holder never authorizes a row, and authorization is
never widened through the served customer or the hierarchy bridge.  An empty
whitelist yields an empty ``tickets`` relation.

Enforcement (structural, not keyword filtering)
-----------------------------------------------

1. The warehouse is opened ``mode=ro``; format markers and the exact
   ``wh_meta``/``dwd_ticket`` structure are verified inside one read
   transaction, then the authorized rows -- and only those rows -- are
   copied out.  The source file is never written.
2. The untrusted statement runs against a throwaway in-memory sandbox that
   contains *only* the ``tickets`` table with the authorized rows.  Base
   tables, ODS raw bytes and audit data are physically absent.
3. A default-deny SQLite authorizer (driven by the SQLite parser, not by
   string matching) permits only ``SELECT``/recursive-CTE evaluation, reads
   of the five exposed columns, and a whitelist of safe functions.  Schema
   tables, ATTACH/DETACH, PRAGMA, writes, DDL, transactions, savepoints and
   extension loading are denied; ``PRAGMA query_only=ON`` and
   ``SQLITE_LIMIT_ATTACHED=0`` are defence in depth, and SQLite length /
   column / depth limits bound resource use.
4. A progress handler enforces the VM instruction budget; the result row
   budget is enforced while fetching.  On *any* rejection or budget breach
   nothing is emitted -- results are materialized and validated (JSON-safe
   values only) before they are returned, so partial output is impossible.
   Every call builds a fresh sandbox, so a rejected or aborted query never
   affects later ones.

Only the Python standard library and SQLite are used.  See
``docs/contract.md`` for the full gateway contract.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import math
import os
from pathlib import Path
import sqlite3
import sys
from types import MappingProxyType

from .inherited import SCHEMA_FAMILY, SCHEMA_VERSION

# ---------------------------------------------------------------------------
# Constants of the gateway contract
# ---------------------------------------------------------------------------

#: Required ``schema`` marker of the strict JSON policy.
POLICY_SCHEMA = "contract_query_policy_v1"

#: Policy text limits (defence in depth for the trusted channel).
MAX_POLICY_CHARS = 1_048_576  # 1 MiB
MAX_PRINCIPALS = 1024
MAX_HOLDERS_PER_PRINCIPAL = 1024
MAX_NAME_CHARS = 128

#: Maximum length of the untrusted SQL text (mirrors SQLITE_LIMIT_SQL_LENGTH).
MAX_SQL_CHARS = 1_000_000

#: The single exposed relation and its five exposed columns.
TICKETS_TABLE = "tickets"
TICKETS_COLUMNS = ("ticket_id", "customer", "opened", "cost_cents", "contract_holder")

#: Functions the untrusted SQL may call (reported by SQLite in canonical
#: lower case).  Aggregates, window functions and pure scalar/date helpers
#: only: no randomness, no blobs, no printf-style formatting, no extension
#: or schema introspection functions.  Availability of a whitelisted name
#: still depends on the SQLite build (e.g. math functions, string_agg).
ALLOWED_FUNCTIONS = frozenset(
    {
        # aggregates
        "avg", "count", "group_concat", "string_agg", "max", "min", "sum", "total",
        # window functions
        "row_number", "rank", "dense_rank", "ntile", "lag", "lead",
        "first_value", "last_value", "nth_value",
        # operators implemented as functions
        "like", "glob",
        # safe scalar helpers
        "abs", "coalesce", "ifnull", "iif", "nullif", "length", "lower", "upper",
        "substr", "trim", "ltrim", "rtrim", "replace", "instr", "hex", "quote",
        "round", "typeof", "unicode", "char", "sign", "concat", "concat_ws",
        "likelihood", "likely", "unlikely",
        # date/time helpers (pure functions of their arguments)
        "date", "time", "datetime", "julianday", "strftime", "unixepoch", "timediff",
        # math functions (present when the SQLite build enables them)
        "floor", "ceil", "ceiling", "power", "pow", "sqrt", "exp", "ln", "log",
        "log10", "log2", "pi", "sin", "cos", "tan", "asin", "acos", "atan",
        "atan2", "sinh", "cosh", "tanh", "asinh", "acosh", "atanh", "degrees",
        "radians", "mod", "trunc",
    }
)

#: SQLite limits applied to the sandbox connection before any untrusted input.
_SANDBOX_LIMITS = (
    (sqlite3.SQLITE_LIMIT_ATTACHED, 0),          # no ATTACH at all
    (sqlite3.SQLITE_LIMIT_LENGTH, 67_108_864),   # 64 MiB per string/blob
    (sqlite3.SQLITE_LIMIT_SQL_LENGTH, MAX_SQL_CHARS),
    (sqlite3.SQLITE_LIMIT_COLUMN, 256),
    (sqlite3.SQLITE_LIMIT_EXPR_DEPTH, 1000),
    (sqlite3.SQLITE_LIMIT_COMPOUND_SELECT, 500),
)

#: The progress handler checkpoints at most this many VM instructions apart.
_PROGRESS_MAX_INTERVAL = 1000
#: Holders per IN(...) chunk when extracting authorized rows.
_EXTRACT_CHUNK = 500
#: Rows fetched per fetchmany() call while enforcing the row budget.
_FETCH_CHUNK = 512

#: Exact structure the gateway consumes (anything else is not a v2 database).
_REQUIRED_STRUCTURE = {
    "wh_meta": {"meta_key", "meta_value"},
    "dwd_ticket": {
        "ticket_id", "customer", "opened", "cost_cents",
        "match_depth", "contract_sk", "contract_holder",
    },
}


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class QueryError(ValueError):
    """Base class for every rejected restricted-query request."""


class PolicyError(QueryError):
    """The trusted JSON policy is malformed or violates the strict schema."""


class UnknownPrincipalError(QueryError):
    """The principal is not present in the trusted policy."""


class WarehouseError(QueryError):
    """The database is missing, unreadable or not a marked v2 warehouse."""


class UnsafeSqlError(QueryError):
    """The untrusted SQL was rejected (structure, authorization, shape)."""


class BudgetExceededError(QueryError):
    """The instruction-step or result-row budget was exceeded."""


class ResultError(QueryError):
    """The result contains a value that is not JSON-representable."""


# ---------------------------------------------------------------------------
# Trusted policy
# ---------------------------------------------------------------------------


def _strict_object(pairs):
    """object_pairs_hook rejecting duplicate JSON object keys."""
    obj = {}
    for key, value in pairs:
        if key in obj:
            raise PolicyError(f"duplicate JSON object key {key!r}")
        obj[key] = value
    return obj


def _reject_constant(value):
    raise PolicyError(f"non-strict JSON constant {value!r} is not allowed")


class Policy:
    """Trusted principal -> contract_holder whitelist (strict JSON source).

    The mapping is immutable: principals are exact, case-sensitive strings
    and each whitelist is a frozenset of exact ``contract_holder`` strings.
    """

    __slots__ = ("_holders",)

    def __init__(self, holders):
        self._holders = MappingProxyType(
            {principal: frozenset(whitelist) for principal, whitelist in holders.items()}
        )

    @classmethod
    def from_json(cls, text) -> "Policy":
        """Parse the strict JSON policy; every violation raises PolicyError."""
        if not isinstance(text, str):
            raise PolicyError(f"policy must be JSON text, got {type(text).__name__}")
        if len(text) > MAX_POLICY_CHARS:
            raise PolicyError(
                f"policy has {len(text)} characters, over the {MAX_POLICY_CHARS} limit"
            )
        try:
            data = json.loads(
                text, object_pairs_hook=_strict_object, parse_constant=_reject_constant
            )
        except PolicyError:
            raise
        except RecursionError as exc:
            raise PolicyError(f"policy JSON is nested too deeply: {exc}") from exc
        except ValueError as exc:
            raise PolicyError(f"policy is not well-formed JSON: {exc}") from exc

        if not isinstance(data, dict) or set(data) != {"schema", "principals"}:
            raise PolicyError(
                "policy top level must be an object with exactly the keys "
                "'schema' and 'principals'"
            )
        if data["schema"] != POLICY_SCHEMA:
            raise PolicyError(
                f"policy schema must be {POLICY_SCHEMA!r}, got {data['schema']!r}"
            )
        principals = data["principals"]
        if not isinstance(principals, dict):
            raise PolicyError("'principals' must be an object mapping names to whitelists")
        if len(principals) > MAX_PRINCIPALS:
            raise PolicyError(
                f"policy has {len(principals)} principals, over the {MAX_PRINCIPALS} limit"
            )
        holders = {}
        for principal, entry in principals.items():
            if not principal or len(principal) > MAX_NAME_CHARS:
                raise PolicyError(
                    f"principal name must be 1..{MAX_NAME_CHARS} characters, got {principal!r}"
                )
            if not isinstance(entry, dict) or set(entry) != {"contract_holders"}:
                raise PolicyError(
                    f"principal {principal!r}: entry must be an object with exactly "
                    "the key 'contract_holders'"
                )
            whitelist = entry["contract_holders"]
            if not isinstance(whitelist, list):
                raise PolicyError(f"principal {principal!r}: 'contract_holders' must be an array")
            if len(whitelist) > MAX_HOLDERS_PER_PRINCIPAL:
                raise PolicyError(
                    f"principal {principal!r}: {len(whitelist)} holders, over the "
                    f"{MAX_HOLDERS_PER_PRINCIPAL} limit"
                )
            seen = set()
            for holder in whitelist:
                if not isinstance(holder, str) or not holder or len(holder) > MAX_NAME_CHARS:
                    raise PolicyError(
                        f"principal {principal!r}: contract_holder entries must be "
                        f"strings of 1..{MAX_NAME_CHARS} characters, got {holder!r}"
                    )
                if holder in seen:
                    raise PolicyError(
                        f"principal {principal!r}: duplicate contract_holder {holder!r}"
                    )
                seen.add(holder)
            holders[principal] = seen
        return cls(holders)

    def holders_for(self, principal: str):
        """Return the whitelist frozenset, or None when the principal is unknown."""
        return self._holders.get(principal)

    @property
    def principals(self) -> tuple:
        """Known principal names, sorted by code point for determinism."""
        return tuple(sorted(self._holders))


# ---------------------------------------------------------------------------
# Query result
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class QueryResult:
    """One fully materialized authorized result set (JSON-safe values only)."""

    columns: tuple
    rows: tuple

    @property
    def row_count(self) -> int:
        return len(self.rows)


# ---------------------------------------------------------------------------
# Warehouse read (trusted): validate v2 markers, copy authorized rows out
# ---------------------------------------------------------------------------


def _connect_readonly(database) -> sqlite3.Connection:
    if not isinstance(database, (str, os.PathLike)):
        raise WarehouseError(f"database must be a path, got {type(database).__name__}")
    path = Path(database)
    uri = path.absolute().as_uri() + "?mode=ro"
    try:
        # autocommit: the explicit BEGIN below owns the single read transaction.
        return sqlite3.connect(uri, uri=True, isolation_level=None)
    except sqlite3.Error as exc:
        raise WarehouseError(
            f"cannot open warehouse read-only (missing file is never created): "
            f"{path}: {exc}"
        ) from exc


def _validate_v2(db: sqlite3.Connection) -> None:
    try:
        tables = {
            row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
    except sqlite3.DatabaseError as exc:
        raise WarehouseError(f"input is not a readable SQLite database: {exc}") from exc
    missing = sorted(set(_REQUIRED_STRUCTURE) - tables)
    if missing:
        raise WarehouseError(
            f"not a v2 {SCHEMA_FAMILY} warehouse: missing table(s) {missing}"
        )
    for table, expected in sorted(_REQUIRED_STRUCTURE.items()):
        actual = {row[1] for row in db.execute(f"PRAGMA table_info({table})")}
        if actual != expected:
            raise WarehouseError(
                f"incompatible v2 structure: table {table!r} has columns "
                f"{sorted(actual)!r}, expected {sorted(expected)!r}"
            )
    meta = dict(db.execute("SELECT meta_key, meta_value FROM wh_meta"))
    family = meta.get("schema_family")
    version = meta.get("schema_version")
    if (family, version) != (SCHEMA_FAMILY, SCHEMA_VERSION):
        raise WarehouseError(
            "unsupported format marker: schema_family="
            f"{family!r} schema_version={version!r}; expected "
            f"{SCHEMA_FAMILY!r} version {SCHEMA_VERSION!r}"
        )


def _select_authorized(db: sqlite3.Connection, whitelist) -> list:
    """Rows whose persisted holder is whitelisted; NULL never matches IN."""
    rows = []
    holders = sorted(whitelist)
    for start in range(0, len(holders), _EXTRACT_CHUNK):
        chunk = holders[start:start + _EXTRACT_CHUNK]
        marks = ",".join("?" * len(chunk))
        rows.extend(
            db.execute(
                "SELECT ticket_id, customer, opened, cost_cents, contract_holder "
                f"FROM dwd_ticket WHERE contract_holder IN ({marks}) "
                "ORDER BY ticket_id",
                chunk,
            ).fetchall()
        )
    rows.sort(key=lambda row: row[0])  # ticket_id is unique: total order
    for row in rows:
        ticket_id, customer, opened, cost_cents, contract_holder = row
        if not (
            isinstance(ticket_id, str)
            and isinstance(customer, str)
            and isinstance(opened, str)
            and isinstance(contract_holder, str)
            and not isinstance(cost_cents, bool)
            and isinstance(cost_cents, int)
        ):
            raise WarehouseError(
                f"dwd_ticket row for ticket_id {ticket_id!r} has unexpected value "
                "types; refusing to expose it"
            )
    return rows


def _extract_authorized_rows(database, whitelist) -> list:
    """Validate the v2 database and copy authorized rows out, all inside one
    read transaction on a read-only connection; the source is never written."""
    db = _connect_readonly(database)
    try:
        db.execute("BEGIN")
        _validate_v2(db)
        rows = _select_authorized(db, whitelist)
        db.execute("COMMIT")
        return rows
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Sandboxed execution of the untrusted SQL
# ---------------------------------------------------------------------------


def _authorizer(action, arg1, arg2, dbname, source):
    """Default-deny gate driven by the SQLite parser (never keyword matching).

    Allows: SELECT/recursive-CTE evaluation, reads of the five exposed
    ``tickets`` columns (the empty column name is SQLite's pseudo-read for
    ``count(*)``/``EXISTS`` and reads no data), and whitelisted functions.
    Everything else -- schema tables, ATTACH/DETACH, PRAGMA, writes, DDL,
    transactions, savepoints, extension loading -- is denied.
    """
    if action in (sqlite3.SQLITE_SELECT, sqlite3.SQLITE_RECURSIVE):
        return sqlite3.SQLITE_OK
    if action == sqlite3.SQLITE_READ:
        if arg1 == TICKETS_TABLE and (not arg2 or arg2 in TICKETS_COLUMNS):
            return sqlite3.SQLITE_OK
        return sqlite3.SQLITE_DENY
    if action == sqlite3.SQLITE_FUNCTION:
        if arg2 is not None and arg2.lower() in ALLOWED_FUNCTIONS:
            return sqlite3.SQLITE_OK
        return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_DENY


class _StepBudget:
    """Counts VM instructions via the progress handler; aborts past budget."""

    def __init__(self, max_steps: int):
        self.max_steps = max_steps
        self.interval = max(1, min(_PROGRESS_MAX_INTERVAL, max_steps))
        self.used = 0

    def __call__(self) -> int:
        self.used += self.interval
        return 1 if self.used > self.max_steps else 0


def _map_sqlite_error(exc: sqlite3.Error) -> QueryError:
    code = getattr(exc, "sqlite_errorcode", None)
    if code == sqlite3.SQLITE_INTERRUPT:
        return BudgetExceededError("instruction step budget exceeded; statement aborted")
    if code == sqlite3.SQLITE_AUTH:
        return UnsafeSqlError("statement rejected by the gateway authorizer")
    if code == sqlite3.SQLITE_TOOBIG:
        return BudgetExceededError("statement or value exceeds the gateway size limit")
    return UnsafeSqlError(f"statement rejected: {exc}")


def _execute(sandbox: sqlite3.Connection, sql: str):
    try:
        return sandbox.execute(sql)
    except sqlite3.ProgrammingError as exc:
        # More than one statement, or unbound parameters.
        raise UnsafeSqlError(f"statement rejected: {exc}") from exc
    except sqlite3.Error as exc:
        raise _map_sqlite_error(exc) from exc
    except ValueError as exc:
        # e.g. an embedded NUL character in the SQL text.
        raise UnsafeSqlError(f"SQL text rejected: {exc}") from exc


def _fetch_bounded(cursor, max_rows: int) -> list:
    rows = []
    try:
        while True:
            batch = cursor.fetchmany(_FETCH_CHUNK)
            if not batch:
                return rows
            rows.extend(batch)
            if len(rows) > max_rows:
                raise BudgetExceededError(
                    f"result row budget exceeded: more than {max_rows} rows"
                )
    except sqlite3.Error as exc:
        raise _map_sqlite_error(exc) from exc


def _check_result_values(rows) -> None:
    for row in rows:
        for value in row:
            if value is None or isinstance(value, (str, int)):
                continue
            if isinstance(value, float) and math.isfinite(value):
                continue
            raise ResultError(
                "result contains a value that is not JSON-representable "
                f"({type(value).__name__}); cast BLOBs to text and avoid "
                "non-finite numbers"
            )


def _run_sandboxed(sql: str, authorized_rows: list, max_steps: int, max_rows: int) -> QueryResult:
    """Run the untrusted statement in a fresh in-memory sandbox that contains
    only the authorized rows; nothing partial is ever returned."""
    sandbox = sqlite3.connect(":memory:", isolation_level=None)
    try:
        for category, limit in _SANDBOX_LIMITS:
            sandbox.setlimit(category, limit)
        sandbox.execute(
            "CREATE TABLE tickets("
            "ticket_id TEXT, customer TEXT, opened TEXT, "
            "cost_cents INTEGER, contract_holder TEXT)"
        )
        sandbox.executemany("INSERT INTO tickets VALUES (?,?,?,?,?)", authorized_rows)
        # Defence in depth below the authorizer: no writes, no attached dbs.
        sandbox.execute("PRAGMA query_only = ON")
        sandbox.set_authorizer(_authorizer)
        budget = _StepBudget(max_steps)
        sandbox.set_progress_handler(budget, budget.interval)

        cursor = _execute(sandbox, sql)
        if cursor.description is None:
            raise UnsafeSqlError(
                "statement produced no result set; only SELECT queries are accepted"
            )
        columns = tuple(description[0] for description in cursor.description)
        rows = _fetch_bounded(cursor, max_rows)
    finally:
        sandbox.close()
    _check_result_values(rows)
    return QueryResult(columns=columns, rows=tuple(rows))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _check_positive_int(value, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise QueryError(f"{label} must be a positive integer, got {value!r}")


def run_query(database, policy: Policy, principal: str, sql: str,
              max_steps: int, max_rows: int) -> QueryResult:
    """Run one restricted query and return its full authorized result.

    Trusted inputs: ``policy`` (a parsed :class:`Policy`), ``principal`` and
    the positive integer budgets ``max_steps``/``max_rows``.  Untrusted input:
    the ``sql`` text.  Any rejection raises a :class:`QueryError` subclass
    and returns nothing; the source database is only ever read.
    """
    if not isinstance(policy, Policy):
        raise QueryError(f"policy must be a query.Policy, got {type(policy).__name__}")
    _check_positive_int(max_steps, "max_steps")
    _check_positive_int(max_rows, "max_rows")
    if not isinstance(principal, str) or not principal:
        raise QueryError(f"principal must be a non-empty string, got {principal!r}")
    whitelist = policy.holders_for(principal)
    if whitelist is None:
        raise UnknownPrincipalError(
            f"unknown principal {principal!r}: not present in the policy"
        )
    if not isinstance(sql, str) or not sql.strip():
        raise UnsafeSqlError("sql must be a non-empty string")
    if len(sql) > MAX_SQL_CHARS:
        raise UnsafeSqlError(f"sql has {len(sql)} characters, over the {MAX_SQL_CHARS} limit")

    authorized_rows = _extract_authorized_rows(database, whitelist)
    return _run_sandboxed(sql, authorized_rows, max_steps, max_rows)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _positive_int_arg(text: str) -> int:
    try:
        value = int(text, 10)
    except ValueError:
        raise argparse.ArgumentTypeError(f"must be a positive integer, got {text!r}")
    if value <= 0:
        raise argparse.ArgumentTypeError(f"must be a positive integer, got {text!r}")
    return value


def _read_capped(path: str, cap: int, label: str) -> str:
    """Read a UTF-8 text file ('-' means stdin) with a hard size cap."""
    if path == "-":
        text = sys.stdin.read()
    else:
        size = os.path.getsize(path)  # OSError is handled by the caller
        if size > cap:
            raise QueryError(f"{label} file {path} is {size} bytes, over the {cap} limit")
        try:
            text = Path(path).read_text(encoding="utf-8")
        except UnicodeDecodeError as exc:
            raise QueryError(f"{label} file {path} is not valid UTF-8: {exc}") from exc
    if len(text) > cap:
        raise QueryError(f"{label} is over the {cap} character limit")
    return text


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run one restricted SQL query against a v2 "
        "inherited-contracts warehouse (untrusted SQL, trusted principal/"
        "policy/budgets; only whitelisted contract_holder rows are exposed)"
    )
    parser.add_argument("--database", required=True,
                        help="v2 warehouse SQLite file (opened read-only)")
    parser.add_argument("--policy", required=True,
                        help="strict JSON policy file (principal -> contract_holder whitelist)")
    parser.add_argument("--principal", required=True,
                        help="principal to query as; must be present in the policy")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--sql", help="a single SELECT statement")
    source.add_argument("--sql-file", help="file with the SQL text; '-' reads standard input")
    parser.add_argument("--max-steps", required=True, type=_positive_int_arg,
                        help="positive VM instruction step budget")
    parser.add_argument("--max-rows", required=True, type=_positive_int_arg,
                        help="positive result row budget")
    args = parser.parse_args()
    try:
        policy = Policy.from_json(_read_capped(args.policy, MAX_POLICY_CHARS, "policy"))
        sql = args.sql if args.sql is not None else _read_capped(
            args.sql_file, MAX_SQL_CHARS, "SQL"
        )
        result = run_query(
            args.database, policy, args.principal, sql, args.max_steps, args.max_rows
        )
    except (QueryError, OSError, sqlite3.Error) as exc:
        parser.exit(2, f"{exc}\n")
    payload = {
        "columns": list(result.columns),
        "principal": args.principal,
        "row_count": len(result.rows),
        "rows": [list(row) for row in result.rows],
    }
    print(json.dumps(payload, sort_keys=True))


if __name__ == "__main__":
    main()
