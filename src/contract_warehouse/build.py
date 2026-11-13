from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from .ingest import load

SCHEMA="""
CREATE TABLE ods_contracts(row_number INTEGER PRIMARY KEY,payload TEXT NOT NULL);
CREATE TABLE ods_tickets(row_number INTEGER PRIMARY KEY,payload TEXT NOT NULL);
CREATE TABLE dim_contract(contract_sk INTEGER PRIMARY KEY,version_id TEXT UNIQUE NOT NULL,
 customer TEXT NOT NULL,valid_from TEXT NOT NULL,valid_to TEXT,tier TEXT NOT NULL);
CREATE TABLE dwd_ticket(ticket_id TEXT PRIMARY KEY,customer TEXT NOT NULL,opened TEXT NOT NULL,
 cost_cents INTEGER NOT NULL CHECK(cost_cents>=0),contract_sk INTEGER REFERENCES dim_contract(contract_sk));
CREATE VIEW ads_tier_cost AS
 SELECT coalesce(c.tier,'uncovered') AS tier,count(*) AS tickets,sum(t.cost_cents) AS cost_cents
 FROM dwd_ticket t LEFT JOIN dim_contract c USING(contract_sk) GROUP BY coalesce(c.tier,'uncovered');
"""

def build(source, target):
    contracts,tickets=load(source)
    target=Path(target);target.parent.mkdir(parents=True,exist_ok=True)
    fd,name=tempfile.mkstemp(prefix='.warehouse-',suffix='.tmp',dir=target.parent);os.close(fd)
    try:
        with closing(sqlite3.connect(name)) as db, db:
            db.execute('PRAGMA foreign_keys=ON');db.executescript(SCHEMA)
            for table,data in [('contracts',contracts),('tickets',tickets)]:
                db.executemany('INSERT INTO ods_'+table+' VALUES (?,?)',[(i,json.dumps(r,sort_keys=True)) for i,r in enumerate(data,1)])
            for sk,row in enumerate(sorted(contracts,key=lambda x:x['version_id']),1):
                db.execute('INSERT INTO dim_contract VALUES (?,?,?,?,?,?)',(sk,row['version_id'],row['customer'],row['valid_from'],row['valid_to'] or None,row['tier']))
            for row in tickets:
                match=db.execute('SELECT contract_sk FROM dim_contract WHERE customer=? AND valid_from<=? AND (valid_to IS NULL OR ?<valid_to)',(row['customer'],row['opened'],row['opened'])).fetchall()
                db.execute('INSERT INTO dwd_ticket VALUES (?,?,?,?,?)',(row['ticket_id'],row['customer'],row['opened'],int(row['cost_cents']),match[0][0] if match else None))
            result={'contracts':len(contracts),'tickets':len(tickets),'tiers':[dict(zip(['tier','tickets','cost_cents'],r)) for r in db.execute('SELECT * FROM ads_tier_cost ORDER BY tier')]}
        os.replace(name,target)
        return result
    finally:
        Path(name).unlink(missing_ok=True)
