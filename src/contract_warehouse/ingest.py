import csv
from datetime import date
from pathlib import Path

def day(s):
    d=date.fromisoformat(s)
    if d.isoformat()!=s: raise ValueError('date must be YYYY-MM-DD')
    return d

def rows(path, fields):
    with Path(path).open(newline='',encoding='utf-8') as f:
        reader=csv.DictReader(f)
        if reader.fieldnames!=fields: raise ValueError('unexpected CSV columns')
        result=list(reader)
    if not result: raise ValueError('empty input')
    for row in result:
        if None in row or any(v is None for v in row.values()):raise ValueError('ragged CSV')
    return result

def load(directory):
    root=Path(directory)
    contracts=rows(root/'contracts.csv',['version_id','customer','valid_from','valid_to','tier'])
    tickets=rows(root/'tickets.csv',['ticket_id','customer','opened','cost_cents'])
    ids=set(); intervals={}
    for row in contracts:
        if any(not row[k].strip() for k in ('version_id','customer','valid_from','tier')):
            raise ValueError('empty contract field')
        if row['tier'] not in ('basic','premium'): raise ValueError('unknown tier')
        if row['version_id'] in ids: raise ValueError('duplicate contract version')
        ids.add(row['version_id']); start=day(row['valid_from']); end=day(row['valid_to']) if row['valid_to'] else None
        if end is not None and start>=end: raise ValueError('non-positive validity interval')
        intervals.setdefault(row['customer'],[]).append((start,end))
    for spans in intervals.values():
        spans.sort(key=lambda span:span[0])
        for (_,end),(start2,_) in zip(spans,spans[1:]):
            if end is None or end>start2: raise ValueError('overlapping contract validity')
    ids=set()
    for row in tickets:
        if any(not row[k].strip() for k in row): raise ValueError('empty ticket field')
        if row['ticket_id'] in ids: raise ValueError('duplicate ticket')
        ids.add(row['ticket_id']);day(row['opened'])
        if not row['cost_cents'].isascii() or not row['cost_cents'].isdigit():raise ValueError('cost must be non-negative integer cents')
    return contracts,tickets
