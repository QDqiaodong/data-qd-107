from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest
from contract_warehouse.build import build
FIX=Path(__file__).resolve().parents[1]/'fixtures'
class WarehouseTest(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup)
        self.root=Path(t.name);self.source=self.root/'input';shutil.copytree(FIX,self.source);self.db=self.root/'out.sqlite'
    def append(self,file,line):
        with (self.source/file).open('a') as f:f.write(line+'\n')
    def test_layers_and_boundary(self):
        r=build(self.source,self.db)
        self.assertEqual(r['tiers'],[{'tier':'basic','tickets':2,'cost_cents':13000},{'tier':'premium','tickets':1,'cost_cents':20000},{'tier':'uncovered','tickets':2,'cost_cents':12000}])
        with sqlite3.connect(self.db) as d:
            self.assertEqual(d.execute('SELECT count(*) FROM ods_tickets').fetchone()[0],5)
            self.assertEqual(d.execute("SELECT version_id FROM dwd_ticket JOIN dim_contract USING(contract_sk) WHERE ticket_id='T2'").fetchone()[0],'V2')
            self.assertEqual(d.execute('PRAGMA foreign_key_check').fetchall(),[])
    def test_repeat(self):self.assertEqual(build(self.source,self.db),build(self.source,self.db))
    def test_overlap(self):
        self.append('contracts.csv','V4,C1,2026-01-31,2026-02-02,basic')
        with self.assertRaisesRegex(ValueError,'overlap'):build(self.source,self.db)
    def test_open_ended_overlap(self):
        self.append('contracts.csv','V4,C1,2027-01-01,,basic')
        with self.assertRaises(ValueError):build(self.source,self.db)
    def test_failure_preserves_output(self):
        build(self.source,self.db);before=self.db.read_bytes()
        self.append('tickets.csv','T9,C1,2026-02-30,10')
        with self.assertRaises(ValueError):build(self.source,self.db)
        self.assertEqual(self.db.read_bytes(),before)
    def test_duplicate_ticket(self):
        self.append('tickets.csv','T1,C9,2026-01-01,0')
        with self.assertRaisesRegex(ValueError,'duplicate ticket'):build(self.source,self.db)
    def test_negative_money(self):
        self.append('tickets.csv','T9,C1,2026-01-01,-1')
        with self.assertRaises(ValueError):build(self.source,self.db)
    def test_unknown_tier(self):
        self.append('contracts.csv','V9,C9,2026-01-01,,gold')
        with self.assertRaises(ValueError):build(self.source,self.db)

    def test_same_start_overlap_with_open_end(self):
        self.append('contracts.csv','V4,C1,2026-02-01,2026-02-02,basic')
        with self.assertRaisesRegex(ValueError,'overlap'):build(self.source,self.db)
