"""Tests for the restricted per-principal SQL query gateway.

Covers the strict JSON policy parser, request validation, v2 database
detection, the row-level authorization rule on the original fixture
(principal authorized for holder H sees only T3/T4 = 700 cents; a principal
authorized for S is not widened to all tickets served to customer S), the
allowed SQL surface (CTE/subquery/set operations/aggregation), the hostile
counterexamples (nested bypass, UNION exfiltration, schema tables, ATTACH,
PRAGMA, writes, VACUUM INTO, extensions, recursion exhaustion), budget
enforcement, no-partial-output and recovery, and the real CLI subprocess.
"""

from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from contract_warehouse import inherited, query
from contract_warehouse.build import build as build_v1

ROOT = Path(__file__).resolve().parents[1]
V1_FIX = ROOT / "fixtures"
V2_FIX = ROOT / "fixtures_inherited"
POLICY_FIX = ROOT / "fixtures_query" / "policy.json"
SRC_DIR = ROOT / "src"

MAX_STEPS = 1_000_000
MAX_ROWS = 1_000


def load_policy():
    return query.Policy.from_json(POLICY_FIX.read_text(encoding="utf-8"))


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class PolicyParseTest(unittest.TestCase):
    """Strict JSON policy: schema, types, duplicates, caps."""

    def parse(self, text):
        return query.Policy.from_json(text)

    def policy_text(self, principals):
        return json.dumps({"schema": "contract_query_policy_v1", "principals": principals})

    def test_fixture_policy_parses(self):
        policy = load_policy()
        self.assertEqual(policy.holders_for("tech-h"), frozenset({"H"}))
        self.assertEqual(policy.holders_for("tech-d1d2"), frozenset({"D1", "D2"}))
        self.assertEqual(policy.holders_for("tech-none"), frozenset())
        self.assertIsNone(policy.holders_for("nobody"))
        self.assertEqual(
            policy.principals, ("tech-d1d2", "tech-h", "tech-none", "tech-s", "tech-u")
        )

    def test_valid_minimal(self):
        policy = self.parse(self.policy_text({"p": {"contract_holders": ["H"]}}))
        self.assertEqual(policy.holders_for("p"), frozenset({"H"}))

    def test_empty_principals_object_is_legal(self):
        policy = self.parse(self.policy_text({}))
        self.assertIsNone(policy.holders_for("p"))

    def test_whitelists_are_exact_and_case_sensitive(self):
        policy = self.parse(self.policy_text({"p": {"contract_holders": ["H"]}}))
        self.assertIsNone(policy.holders_for("P"))
        self.assertIsNone(policy.holders_for(" p"))
        self.assertIsNone(policy.holders_for("p "))

    def test_policy_is_immutable(self):
        policy = self.parse(self.policy_text({"p": {"contract_holders": ["H"]}}))
        with self.assertRaises(TypeError):
            policy._holders["p"] = frozenset()
        with self.assertRaises(AttributeError):
            policy.holders_for("p").add("X")

    def test_rejects_malformed_json(self):
        for text in ["", "not json", "{", "[1,2]", '"s"', "42", "null", "{'a':1}"]:
            with self.assertRaises(query.PolicyError, msg=text):
                self.parse(text)

    def test_rejects_non_strict_json(self):
        for text in [
            '{"schema": "contract_query_policy_v1", "principals": {}, "x": NaN}',
            '{"schema": "contract_query_policy_v1", "principals": {"p": Infinity}}',
        ]:
            with self.assertRaises(query.PolicyError, msg=text):
                self.parse(text)

    def test_rejects_duplicate_object_keys(self):
        text = (
            '{"schema": "contract_query_policy_v1", "principals": {}, '
            '"principals": {}}'
        )
        with self.assertRaisesRegex(query.PolicyError, "duplicate"):
            self.parse(text)
        text = (
            '{"schema": "contract_query_policy_v1", "principals": '
            '{"p": {"contract_holders": [], "contract_holders": []}}}'
        )
        with self.assertRaisesRegex(query.PolicyError, "duplicate"):
            self.parse(text)

    def test_rejects_wrong_shape(self):
        bad = [
            {},  # missing keys
            {"schema": "contract_query_policy_v1"},  # missing principals
            {"principals": {}},  # missing schema
            {"schema": "contract_query_policy_v1", "principals": {}, "extra": 1},
            {"schema": "wrong", "principals": {}},
            {"schema": 1, "principals": {}},
            {"schema": "contract_query_policy_v1", "principals": []},
            {"schema": "contract_query_policy_v1", "principals": {"p": ["H"]}},
            {"schema": "contract_query_policy_v1",
             "principals": {"p": {"contract_holders": ["H"], "extra": 1}}},
            {"schema": "contract_query_policy_v1",
             "principals": {"p": {"contract_holders": "H"}}},
            {"schema": "contract_query_policy_v1",
             "principals": {"p": {"contract_holders": [1]}}},
            {"schema": "contract_query_policy_v1",
             "principals": {"p": {"contract_holders": [None]}}},
            {"schema": "contract_query_policy_v1",
             "principals": {"p": {"contract_holders": [""]}}},
            {"schema": "contract_query_policy_v1",
             "principals": {"": {"contract_holders": ["H"]}}},
            {"schema": "contract_query_policy_v1",
             "principals": {"p": {"contract_holders": ["H", "H"]}}},
        ]
        for doc in bad:
            with self.assertRaises(query.PolicyError, msg=repr(doc)):
                self.parse(json.dumps(doc))

    def test_rejects_deep_nesting_and_oversize(self):
        with self.assertRaises(query.PolicyError):
            self.parse('{"schema": "contract_query_policy_v1", "principals": ' + "[" * 2000 + "]" * 2000 + "}")
        with self.assertRaisesRegex(query.PolicyError, "limit"):
            self.parse(" " * (query.MAX_POLICY_CHARS + 1))

    def test_rejects_caps(self):
        too_many_principals = {
            f"p{i}": {"contract_holders": []} for i in range(query.MAX_PRINCIPALS + 1)
        }
        with self.assertRaisesRegex(query.PolicyError, "principals"):
            self.parse(self.policy_text(too_many_principals))
        too_many_holders = {"p": {"contract_holders": [f"h{i}" for i in range(query.MAX_HOLDERS_PER_PRINCIPAL + 1)]}}
        with self.assertRaisesRegex(query.PolicyError, "holders"):
            self.parse(self.policy_text(too_many_holders))
        long_name = "x" * (query.MAX_NAME_CHARS + 1)
        with self.assertRaisesRegex(query.PolicyError, "principal name"):
            self.parse(self.policy_text({long_name: {"contract_holders": []}}))
        with self.assertRaisesRegex(query.PolicyError, "contract_holder"):
            self.parse(self.policy_text({"p": {"contract_holders": [long_name]}}))

    def test_rejects_non_string_text(self):
        with self.assertRaises(query.PolicyError):
            query.Policy.from_json(b"{}")


class RequestValidationTest(unittest.TestCase):
    """Trusted-caller inputs are validated before any database access."""

    def setUp(self):
        self.policy = load_policy()
        self.missing_db = "/nonexistent/definitely-not-here.sqlite"

    def test_rejects_non_policy(self):
        for bad in [{"tech-h": ["H"]}, "policy", None, 42]:
            with self.assertRaisesRegex(query.QueryError, "policy"):
                query.run_query(self.missing_db, bad, "tech-h", "SELECT 1", 10, 10)

    def test_rejects_bad_limits(self):
        for bad in [0, -1, -100, "10", 1.5, True, False, None]:
            with self.assertRaisesRegex(query.QueryError, "max_steps"):
                query.run_query(self.missing_db, self.policy, "tech-h", "SELECT 1", bad, 10)
            with self.assertRaisesRegex(query.QueryError, "max_rows"):
                query.run_query(self.missing_db, self.policy, "tech-h", "SELECT 1", 10, bad)

    def test_rejects_bad_principal(self):
        for bad in ["", None, 42, b"tech-h"]:
            with self.assertRaisesRegex(query.QueryError, "principal"):
                query.run_query(self.missing_db, self.policy, bad, "SELECT 1", 10, 10)

    def test_unknown_principal_before_db_access(self):
        with self.assertRaises(query.UnknownPrincipalError):
            query.run_query(self.missing_db, self.policy, "nobody", "SELECT 1", 10, 10)

    def test_rejects_bad_sql_before_db_access(self):
        for bad in ["", "   ", "\n\t ", None, 42, b"SELECT 1"]:
            with self.assertRaises(query.UnsafeSqlError, msg=repr(bad)):
                query.run_query(self.missing_db, self.policy, "tech-h", bad, 10, 10)
        with self.assertRaisesRegex(query.UnsafeSqlError, "limit"):
            query.run_query(
                self.missing_db, self.policy, "tech-h",
                "SELECT " + "1" * query.MAX_SQL_CHARS, 10, 10,
            )


class WarehouseValidationTest(unittest.TestCase):
    """Only a marked inherited_contracts_v2 database is accepted."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.policy = load_policy()

    def run_q(self, db):
        return query.run_query(db, self.policy, "tech-h", "SELECT count(*) FROM tickets", MAX_STEPS, MAX_ROWS)

    def test_missing_file_rejected_and_never_created(self):
        target = self.root / "nope.sqlite"
        with self.assertRaises(query.WarehouseError):
            self.run_q(target)
        self.assertFalse(target.exists())

    def test_non_sqlite_file_rejected(self):
        target = self.root / "junk.sqlite"
        target.write_bytes(b"this is not a sqlite database" * 10)
        with self.assertRaises(query.WarehouseError):
            self.run_q(target)

    def test_v1_baseline_rejected(self):
        target = self.root / "v1.sqlite"
        build_v1(V1_FIX, target)
        with self.assertRaisesRegex(query.WarehouseError, "wh_meta"):
            self.run_q(target)

    def test_tampered_meta_rejected(self):
        target = self.root / "v2.sqlite"
        inherited.build(V2_FIX, target)
        with closing(sqlite3.connect(target)) as db, db:
            db.execute("UPDATE wh_meta SET meta_value='forged' WHERE meta_key='schema_family'")
        with self.assertRaisesRegex(query.WarehouseError, "format marker"):
            self.run_q(target)

    def test_wrong_structure_rejected(self):
        target = self.root / "fake.sqlite"
        with closing(sqlite3.connect(target)) as db, db:
            db.execute("CREATE TABLE wh_meta(meta_key TEXT, meta_value TEXT)")
            db.executemany(
                "INSERT INTO wh_meta VALUES (?,?)",
                [("schema_family", "inherited_contracts_v2"), ("schema_version", "2")],
            )
            db.execute("CREATE TABLE dwd_ticket(ticket_id TEXT)")  # wrong columns
        with self.assertRaisesRegex(query.WarehouseError, "structure"):
            self.run_q(target)

    def test_non_path_database_rejected(self):
        with self.assertRaises(query.WarehouseError):
            self.run_q(42)


class GatewayApiTest(unittest.TestCase):
    """Row-level authorization, allowed SQL surface, rejections, budgets."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.tmp.cleanup)
        cls.root = Path(cls.tmp.name)
        cls.db = cls.root / "warehouse.sqlite"
        inherited.build(V2_FIX, cls.db)
        cls.db_digest = sha256(cls.db)
        cls.policy = load_policy()

    def run_q(self, sql, principal="tech-h", max_steps=MAX_STEPS, max_rows=MAX_ROWS, db=None):
        return query.run_query(
            db if db is not None else self.db, self.policy, principal, sql, max_steps, max_rows
        )

    # -- the authorization rule on the original fixture --------------------

    def test_h_principal_sees_only_t3_t4_total_700(self):
        result = self.run_q(
            "SELECT ticket_id, customer, opened, cost_cents, contract_holder "
            "FROM tickets ORDER BY ticket_id"
        )
        self.assertEqual(
            result.columns,
            ("ticket_id", "customer", "opened", "cost_cents", "contract_holder"),
        )
        self.assertEqual(
            result.rows,
            (
                ("T3", "S", "2026-01-20", 300, "H"),
                ("T4", "S", "2026-02-01", 400, "H"),
            ),
        )
        self.assertEqual(result.row_count, 2)
        aggregate = self.run_q("SELECT count(*), sum(cost_cents) FROM tickets")
        self.assertEqual(aggregate.rows, ((2, 700),))

    def test_s_principal_not_expanded_by_served_customer(self):
        # S holds contract K3 (ticket T2 only); the five tickets *served to*
        # customer S (T1..T5, 1500 cents) must not leak through.
        result = self.run_q("SELECT ticket_id, cost_cents FROM tickets", principal="tech-s")
        self.assertEqual(result.rows, (("T2", 200),))
        self.assertEqual(
            self.run_q("SELECT count(*) FROM tickets WHERE customer = 'S'", principal="tech-s").rows,
            ((1,),),
        )
        self.assertEqual(
            self.run_q("SELECT coalesce(sum(cost_cents), 0) FROM tickets", principal="tech-s").rows,
            ((200,),),
        )

    def test_d1d2_principal_multi_holder_whitelist(self):
        result = self.run_q(
            "SELECT ticket_id, cost_cents FROM tickets ORDER BY ticket_id",
            principal="tech-d1d2",
        )
        self.assertEqual(result.rows, (("T1", 100), ("T5", 500)))

    def test_null_holder_never_authorized(self):
        # T6 is uncovered (persisted holder NULL): invisible to everyone,
        # including a principal whitelisted for its served customer U.
        self.assertEqual(
            self.run_q("SELECT count(*) FROM tickets", principal="tech-u").rows, ((0,),)
        )
        for principal in self.policy.principals:
            self.assertEqual(
                self.run_q(
                    "SELECT count(*) FROM tickets "
                    "WHERE contract_holder IS NULL OR ticket_id = 'T6'",
                    principal=principal,
                ).rows,
                ((0,),),
                msg=principal,
            )

    def test_empty_whitelist_returns_empty_set(self):
        result = self.run_q("SELECT * FROM tickets", principal="tech-none")
        self.assertEqual(result.rows, ())
        self.assertEqual(
            result.columns,
            ("ticket_id", "customer", "opened", "cost_cents", "contract_holder"),
        )
        self.assertEqual(
            self.run_q("SELECT count(*) FROM tickets", principal="tech-none").rows, ((0,),)
        )

    def test_deterministic_across_calls(self):
        first = self.run_q("SELECT * FROM tickets ORDER BY ticket_id")
        second = self.run_q("SELECT * FROM tickets ORDER BY ticket_id")
        self.assertEqual(first, second)

    # -- the allowed SQL surface -------------------------------------------

    def test_cte_subquery_setops_aggregation_allowed(self):
        cases = [
            ("WITH big AS (SELECT * FROM tickets WHERE cost_cents >= 400) "
             "SELECT ticket_id FROM big", (("T4",),)),
            ("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 5) "
             "SELECT max(x) FROM c", ((5,),)),
            ("SELECT ticket_id FROM tickets WHERE cost_cents = "
             "(SELECT max(cost_cents) FROM tickets)", (("T4",),)),
            ("SELECT ticket_id FROM tickets t WHERE cost_cents > "
             "(SELECT avg(cost_cents) FROM tickets)", (("T4",),)),
            ("SELECT ticket_id FROM tickets UNION SELECT ticket_id FROM tickets "
             "ORDER BY 1", (("T3",), ("T4",))),
            ("SELECT ticket_id FROM tickets UNION ALL SELECT ticket_id FROM tickets "
             "ORDER BY 1", (("T3",), ("T3",), ("T4",), ("T4",))),
            ("SELECT ticket_id FROM tickets INTERSECT SELECT 'T3'", (("T3",),)),
            ("SELECT ticket_id FROM tickets EXCEPT SELECT 'T3'", (("T4",),)),
            ("SELECT contract_holder, count(*), sum(cost_cents) FROM tickets "
             "GROUP BY 1 HAVING count(*) > 1", (("H", 2, 700),)),
            ("SELECT ticket_id, rank() OVER (ORDER BY cost_cents) r FROM tickets "
             "ORDER BY r", (("T3", 1), ("T4", 2))),
            ("SELECT COUNT(*), MAX(cost_cents), SUM(cost_cents) FROM tickets",
             ((2, 400, 700),)),
            ("SELECT EXISTS(SELECT 1 FROM tickets WHERE ticket_id = 'T3')", ((1,),)),
            ("SELECT ticket_id FROM tickets WHERE cost_cents BETWEEN 250 AND 350",
             (("T3",),)),
            ("SELECT CASE WHEN cost_cents >= 400 THEN 'big' ELSE 'small' END "
             "FROM tickets ORDER BY 1", (("big",), ("small",))),
            ("SELECT CAST(cost_cents AS TEXT) FROM tickets ORDER BY 1",
             (("300",), ("400",))),
            ("SELECT strftime('%Y-%m', opened) FROM tickets ORDER BY 1",
             (("2026-01",), ("2026-02",))),
            ("SELECT DISTINCT customer FROM tickets ORDER BY 1 LIMIT 1 OFFSET 0",
             (("S",),)),
            ("SELECT a.ticket_id FROM tickets a JOIN tickets b "
             "ON a.ticket_id = b.ticket_id ORDER BY 1", (("T3",), ("T4",))),
            ("SELECT * FROM (VALUES (1, 'x'), (2, 'y')) ORDER BY 1",
             ((1, "x"), (2, "y"))),
            ("SELECT ticket_id FROM tickets WHERE customer LIKE 'S%' "
             "AND ticket_id GLOB 'T*' ORDER BY 1", (("T3",), ("T4",))),
            ("SELECT group_concat(ticket_id, '|') FROM tickets", (("T3|T4",),)),
            ("SELECT 1", ((1,),)),
            ("SELECT NULL, 1, 1.5, 'x'", ((None, 1, 1.5, "x"),)),
        ]
        for sql, expected in cases:
            with self.subTest(sql=sql[:60]):
                self.assertEqual(self.run_q(sql).rows, expected)

    def test_explain_is_readonly_and_allowed(self):
        result = self.run_q("EXPLAIN SELECT ticket_id FROM tickets")
        self.assertIn("opcode", result.columns)
        self.assertTrue(result.rows)

    def test_keyword_lookalikes_in_literals_are_not_filtered(self):
        # The gate is structural: keywords inside string literals or quoted
        # identifiers are data, not statements.
        result = self.run_q(
            "SELECT 'PRAGMA database_list; DROP TABLE tickets; ATTACH DATABASE ''x''; "
            "SELECT load_extension('"
        )
        self.assertEqual(result.row_count, 1)
        result = self.run_q("SELECT ticket_id AS \"attach\" FROM tickets WHERE customer = 'VACUUM INTO'")
        self.assertEqual(result.columns, ("attach",))
        self.assertEqual(result.rows, ())

    # -- rejections: nothing outside the authorized relation ----------------

    def assert_rejected(self, sql, kind=query.UnsafeSqlError):
        with self.assertRaises(kind, msg=sql):
            self.run_q(sql)

    def test_base_tables_unreachable_even_nested(self):
        self.assert_rejected("SELECT * FROM dwd_ticket")
        self.assert_rejected(
            "SELECT * FROM tickets WHERE ticket_id IN (SELECT ticket_id FROM dwd_ticket)"
        )
        self.assert_rejected(
            "WITH leak AS (SELECT raw_bytes FROM ods_source_file) "
            "SELECT * FROM tickets WHERE ticket_id IN (SELECT ticket_id FROM leak)"
        )
        self.assert_rejected(
            "SELECT (SELECT version_id FROM dim_contract LIMIT 1) FROM tickets"
        )
        self.assert_rejected(
            "SELECT * FROM tickets t JOIN dim_customer c ON t.customer = c.customer"
        )
        self.assert_rejected("SELECT * FROM audit_ticket_match")
        self.assert_rejected(
            "SELECT group_concat(raw_bytes) FROM ods_source_file"
        )

    def test_union_exfiltration_rejected(self):
        self.assert_rejected(
            "SELECT ticket_id FROM tickets UNION SELECT raw_bytes FROM ods_source_file"
        )
        self.assert_rejected(
            "SELECT ticket_id FROM tickets UNION ALL SELECT version_id FROM dim_contract"
        )

    def test_schema_tables_rejected(self):
        self.assert_rejected("SELECT * FROM sqlite_master")
        self.assert_rejected("SELECT * FROM sqlite_schema")
        self.assert_rejected("SELECT sql FROM sqlite_master")
        self.assert_rejected("SELECT * FROM pragma_table_info('tickets')")
        self.assert_rejected("SELECT * FROM dbstat")

    def test_attach_detach_rejected(self):
        self.assert_rejected("ATTACH DATABASE ':memory:' AS x")
        self.assert_rejected(f"ATTACH DATABASE '{self.root}/evil.sqlite' AS x")
        self.assert_rejected("DETACH DATABASE main")

    def test_pragma_rejected(self):
        self.assert_rejected("PRAGMA database_list")
        self.assert_rejected("PRAGMA table_info(tickets)")
        self.assert_rejected("PRAGMA query_only = OFF")

    def test_writes_and_ddl_rejected(self):
        self.assert_rejected("INSERT INTO tickets VALUES ('X','Y','2026-01-01',1,'Z')")
        self.assert_rejected("UPDATE tickets SET cost_cents = 0")
        self.assert_rejected("DELETE FROM tickets")
        self.assert_rejected("REPLACE INTO tickets VALUES ('X','Y','2026-01-01',1,'Z')")
        self.assert_rejected("CREATE TABLE evil(x)")
        self.assert_rejected("CREATE VIEW evil AS SELECT * FROM tickets")
        self.assert_rejected("CREATE INDEX evil ON tickets(ticket_id)")
        self.assert_rejected("CREATE TRIGGER evil AFTER INSERT ON tickets BEGIN SELECT 1; END")
        self.assert_rejected("DROP TABLE tickets")
        self.assert_rejected("ALTER TABLE tickets ADD COLUMN x")
        self.assert_rejected("REINDEX")
        self.assert_rejected("ANALYZE")

    def test_vacuum_rejected_and_creates_no_file(self):
        target = self.root / "vacuum-stolen.sqlite"
        self.assert_rejected("VACUUM")
        self.assert_rejected(f"VACUUM INTO '{target}'")
        self.assertFalse(target.exists())

    def test_extensions_and_dangerous_functions_rejected(self):
        self.assert_rejected("SELECT load_extension('/tmp/nope')")
        self.assert_rejected("SELECT readfile('/etc/passwd')")
        self.assert_rejected("SELECT writefile('/tmp/x', 'y')")
        self.assert_rejected("SELECT printf('%d', 1)")
        self.assert_rejected("SELECT format('%d', 1)")
        self.assert_rejected("SELECT zeroblob(8)")
        self.assert_rejected("SELECT randomblob(8)")
        self.assert_rejected("SELECT random()")
        self.assert_rejected("SELECT sqlite_version()")
        self.assert_rejected("SELECT last_insert_rowid()")

    def test_transactions_savepoints_rejected(self):
        self.assert_rejected("BEGIN")
        self.assert_rejected("COMMIT")
        self.assert_rejected("SAVEPOINT s")
        self.assert_rejected("RELEASE s")

    def test_multiple_statements_rejected(self):
        self.assert_rejected("SELECT 1; SELECT 2")
        self.assert_rejected("SELECT 1; DROP TABLE tickets")
        self.assert_rejected("SELECT 1; ATTACH DATABASE ':memory:' AS x")

    def test_rowid_hidden(self):
        self.assert_rejected("SELECT rowid FROM tickets")
        self.assert_rejected("SELECT tickets.rowid FROM tickets")

    def test_explain_of_write_rejected(self):
        self.assert_rejected("EXPLAIN INSERT INTO tickets VALUES ('X','Y','2026-01-01',1,'Z')")

    def test_table_valued_functions_rejected(self):
        self.assert_rejected("SELECT * FROM json_each('[1,2]')")

    def test_no_result_set_rejected(self):
        # A bare REINDEX is a harmless no-op but is not a SELECT: no output.
        self.assert_rejected("REINDEX")
        self.assert_rejected("-- only a comment")
        self.assert_rejected("/* only a comment */")

    def test_unbound_parameters_rejected(self):
        self.assert_rejected("SELECT ?")
        self.assert_rejected("SELECT :holder")
        self.assert_rejected("SELECT * FROM tickets WHERE contract_holder = ?")

    # -- budgets ------------------------------------------------------------

    def test_step_budget_stops_recursion_exhaustion(self):
        sql = ("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c) "
               "SELECT max(x) FROM c")
        with self.assertRaisesRegex(query.BudgetExceededError, "instruction step budget"):
            self.run_q(sql, max_steps=10_000)
        with self.assertRaises(query.BudgetExceededError):
            self.run_q(sql, max_steps=10)
        # Recovery: the next legal query works in the same process.
        self.assertEqual(self.run_q("SELECT count(*) FROM tickets").rows, ((2,),))

    def test_step_budget_allows_legit_queries(self):
        result = self.run_q("SELECT count(*) FROM tickets", max_steps=1_000_000)
        self.assertEqual(result.rows, ((2,),))

    def test_row_budget_enforced_without_partial_output(self):
        sql = "SELECT ticket_id FROM tickets UNION ALL SELECT ticket_id FROM tickets"
        self.assertEqual(self.run_q(sql, max_rows=4).row_count, 4)  # exact boundary
        with self.assertRaisesRegex(query.BudgetExceededError, "row budget"):
            self.run_q(sql, max_rows=3)
        with self.assertRaises(query.BudgetExceededError):
            self.run_q(sql, max_rows=1)
        # Recovery after a budget breach.
        self.assertEqual(self.run_q("SELECT count(*) FROM tickets").rows, ((2,),))

    def test_size_limit_stops_string_bomb(self):
        sql = ("WITH RECURSIVE c(s) AS (SELECT 'xxxxxxxxxx' UNION ALL SELECT s||s FROM c) "
               "SELECT max(length(s)) FROM c")
        with self.assertRaises(query.BudgetExceededError):
            self.run_q(sql)

    # -- result contract -----------------------------------------------------

    def test_blob_result_rejected(self):
        with self.assertRaises(query.ResultError):
            self.run_q("SELECT CAST(customer AS BLOB) FROM tickets")

    def test_non_finite_result_rejected(self):
        with self.assertRaises(query.ResultError):
            self.run_q("SELECT 9e999")
        with self.assertRaises(query.ResultError):
            self.run_q("SELECT -9e999")

    def test_empty_result_keeps_column_shape(self):
        result = self.run_q("SELECT ticket_id, cost_cents FROM tickets WHERE 0")
        self.assertEqual(result.columns, ("ticket_id", "cost_cents"))
        self.assertEqual(result.rows, ())

    # -- rejection does not poison the session; source db is byte-identical --

    def test_rejection_battery_leaves_session_and_source_untouched(self):
        hostile = [
            "DELETE FROM tickets",
            "DROP TABLE tickets",
            f"VACUUM INTO '{self.root}/stolen.sqlite'",
            f"ATTACH DATABASE '{self.root}/stolen.sqlite' AS x",
            "PRAGMA query_only = OFF",
            "SELECT * FROM sqlite_master",
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c) SELECT max(x) FROM c",
        ]
        for sql in hostile:
            with self.assertRaises(query.QueryError, msg=sql):
                self.run_q(sql, max_steps=10_000)
            legal = self.run_q("SELECT count(*), sum(cost_cents) FROM tickets")
            self.assertEqual(legal.rows, ((2, 700),), msg=f"recovery after {sql!r}")
        self.assertEqual(sha256(self.db), self.db_digest)
        self.assertFalse((self.root / "stolen.sqlite").exists())
        # No SQLite sidecar files were ever created next to the source.
        self.assertEqual({p.name for p in self.root.iterdir()}, {"warehouse.sqlite"})


class CliTest(unittest.TestCase):
    """Real CLI subprocess end-to-end, including the hostile counterexamples."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.tmp.cleanup)
        cls.root = Path(cls.tmp.name)
        cls.db = cls.root / "warehouse.sqlite"
        inherited.build(V2_FIX, cls.db)
        cls.db_digest = sha256(cls.db)
        cls.v1_db = cls.root / "v1.sqlite"
        build_v1(V1_FIX, cls.v1_db)
        cls.bad_policy = cls.root / "bad-policy.json"
        cls.bad_policy.write_text('{"schema": "contract_query_policy_v1", "principals": [}',
                                  encoding="utf-8")

    def run_cli(self, *args, input_text=None):
        return subprocess.run(
            [sys.executable, "-m", "contract_warehouse.query", *args],
            cwd=SRC_DIR, capture_output=True, text=True, check=False, input=input_text,
        )

    def base_args(self, principal="tech-h", max_steps=MAX_STEPS, max_rows=MAX_ROWS):
        return [
            "--database", str(self.db),
            "--policy", str(POLICY_FIX),
            "--principal", principal,
            "--max-steps", str(max_steps),
            "--max-rows", str(max_rows),
        ]

    def assert_ok(self, proc):
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return json.loads(proc.stdout)

    def assert_rejected(self, proc, stderr_hint=None):
        self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
        self.assertEqual(proc.stdout, "", "no partial result may be printed")
        self.assertTrue(proc.stderr.strip())
        if stderr_hint is not None:
            self.assertIn(stderr_hint, proc.stderr)

    def test_cli_aggregate_h_principal(self):
        proc = self.run_cli(
            *self.base_args(),
            "--sql", "SELECT count(*) AS n, sum(cost_cents) AS total FROM tickets",
        )
        payload = self.assert_ok(proc)
        self.assertEqual(
            payload,
            {"columns": ["n", "total"], "principal": "tech-h", "row_count": 1,
             "rows": [[2, 700]]},
        )

    def test_cli_rows_h_principal(self):
        proc = self.run_cli(
            *self.base_args(),
            "--sql", "SELECT ticket_id, customer, opened, cost_cents, contract_holder "
                     "FROM tickets ORDER BY ticket_id",
        )
        payload = self.assert_ok(proc)
        self.assertEqual(
            payload["rows"],
            [["T3", "S", "2026-01-20", 300, "H"], ["T4", "S", "2026-02-01", 400, "H"]],
        )
        self.assertEqual(
            payload["columns"],
            ["ticket_id", "customer", "opened", "cost_cents", "contract_holder"],
        )

    def test_cli_s_principal_gets_only_own_holder_rows(self):
        proc = self.run_cli(
            *self.base_args(principal="tech-s"),
            "--sql", "SELECT ticket_id, cost_cents FROM tickets",
        )
        self.assertEqual(self.assert_ok(proc)["rows"], [["T2", 200]])
        proc = self.run_cli(
            *self.base_args(principal="tech-s"),
            "--sql", "SELECT count(*) FROM tickets WHERE customer = 'S'",
        )
        self.assertEqual(self.assert_ok(proc)["rows"], [[1]])

    def test_cli_empty_whitelist(self):
        proc = self.run_cli(
            *self.base_args(principal="tech-none"),
            "--sql", "SELECT count(*) FROM tickets",
        )
        self.assertEqual(self.assert_ok(proc)["rows"], [[0]])

    def test_cli_sql_file_and_stdin(self):
        sql_file = self.root / "query.sql"
        sql_file.write_text("SELECT sum(cost_cents) FROM tickets", encoding="utf-8")
        proc = self.run_cli(*self.base_args(), "--sql-file", str(sql_file))
        self.assertEqual(self.assert_ok(proc)["rows"], [[700]])
        proc = self.run_cli(
            *self.base_args(), "--sql-file", "-", input_text="SELECT sum(cost_cents) FROM tickets"
        )
        self.assertEqual(self.assert_ok(proc)["rows"], [[700]])

    def test_cli_nested_bypass_rejected_then_recovers(self):
        proc = self.run_cli(
            *self.base_args(),
            "--sql", "WITH leak AS (SELECT raw_bytes FROM ods_source_file) "
                     "SELECT * FROM tickets WHERE ticket_id IN (SELECT ticket_id FROM leak)",
        )
        self.assert_rejected(proc, "no such table")
        recovery = self.run_cli(*self.base_args(), "--sql", "SELECT count(*) FROM tickets")
        self.assertEqual(self.assert_ok(recovery)["rows"], [[2]])

    def test_cli_union_exfiltration_rejected(self):
        proc = self.run_cli(
            *self.base_args(),
            "--sql", "SELECT ticket_id FROM tickets UNION SELECT raw_bytes FROM ods_source_file",
        )
        self.assert_rejected(proc, "no such table")

    def test_cli_schema_table_rejected(self):
        proc = self.run_cli(*self.base_args(), "--sql", "SELECT * FROM sqlite_master")
        self.assert_rejected(proc, "authorizer")

    def test_cli_attach_rejected(self):
        proc = self.run_cli(
            *self.base_args(), "--sql", f"ATTACH DATABASE '{self.root}/evil.sqlite' AS x"
        )
        self.assert_rejected(proc, "authorizer")
        self.assertFalse((self.root / "evil.sqlite").exists())

    def test_cli_pragma_rejected(self):
        proc = self.run_cli(*self.base_args(), "--sql", "PRAGMA database_list")
        self.assert_rejected(proc, "authorizer")

    def test_cli_write_rejected_source_unchanged_then_recovers(self):
        proc = self.run_cli(*self.base_args(), "--sql", "DELETE FROM tickets")
        self.assert_rejected(proc, "authorizer")
        self.assertEqual(sha256(self.db), self.db_digest)
        recovery = self.run_cli(*self.base_args(), "--sql", "SELECT count(*) FROM tickets")
        self.assertEqual(self.assert_ok(recovery)["rows"], [[2]])

    def test_cli_vacuum_into_rejected_no_file(self):
        target = self.root / "vacuum-stolen.sqlite"
        proc = self.run_cli(*self.base_args(), "--sql", f"VACUUM INTO '{target}'")
        self.assert_rejected(proc, "authorizer")
        self.assertFalse(target.exists())

    def test_cli_recursion_exhaustion_rejected_then_recovers(self):
        proc = self.run_cli(
            *self.base_args(max_steps=5_000),
            "--sql", "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c) "
                     "SELECT max(x) FROM c",
        )
        self.assert_rejected(proc, "instruction step budget")
        recovery = self.run_cli(*self.base_args(), "--sql", "SELECT count(*) FROM tickets")
        self.assertEqual(self.assert_ok(recovery)["rows"], [[2]])

    def test_cli_row_budget_rejected_without_partial_output(self):
        proc = self.run_cli(
            *self.base_args(max_rows=1),
            "--sql", "SELECT ticket_id FROM tickets ORDER BY ticket_id",
        )
        self.assert_rejected(proc, "row budget")

    def test_cli_unknown_principal_rejected(self):
        proc = self.run_cli(
            *self.base_args(principal="mallory"), "--sql", "SELECT count(*) FROM tickets"
        )
        self.assert_rejected(proc, "unknown principal")

    def test_cli_bad_policy_rejected(self):
        proc = self.run_cli(
            "--database", str(self.db), "--policy", str(self.bad_policy),
            "--principal", "tech-h", "--max-steps", "100", "--max-rows", "100",
            "--sql", "SELECT 1",
        )
        self.assert_rejected(proc, "policy")

    def test_cli_non_v2_database_rejected(self):
        proc = self.run_cli(
            "--database", str(self.v1_db), "--policy", str(POLICY_FIX),
            "--principal", "tech-h", "--max-steps", "100", "--max-rows", "100",
            "--sql", "SELECT 1",
        )
        self.assert_rejected(proc, "wh_meta")

    def test_cli_bad_limits_rejected(self):
        for steps, rows in [("0", "10"), ("-3", "10"), ("abc", "10"), ("10", "0"), ("10", "1.5")]:
            proc = self.run_cli(
                "--database", str(self.db), "--policy", str(POLICY_FIX),
                "--principal", "tech-h", "--max-steps", steps, "--max-rows", rows,
                "--sql", "SELECT 1",
            )
            self.assert_rejected(proc)

    def test_cli_argument_errors(self):
        # Both --sql and --sql-file, or neither: argparse exits non-zero.
        proc = self.run_cli(*self.base_args(), "--sql", "SELECT 1", "--sql-file", "x.sql")
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proc.stdout, "")
        proc = self.run_cli(*self.base_args())
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proc.stdout, "")

    def test_cli_missing_policy_file_rejected(self):
        proc = self.run_cli(
            "--database", str(self.db), "--policy", str(self.root / "nope.json"),
            "--principal", "tech-h", "--max-steps", "100", "--max-rows", "100",
            "--sql", "SELECT 1",
        )
        self.assert_rejected(proc)


if __name__ == "__main__":
    unittest.main()
