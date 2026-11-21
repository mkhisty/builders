#!/usr/bin/env python3
"""Strict input parsing, DuckDB sorting, and streaming session aggregation."""
import csv
import json
import os
import re
import tempfile
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

DATA = Path(os.environ.get('DATA_DIR', '/data'))
OUT = Path(os.environ.get('OUTPUT_DIR', '/output'))
SCRATCH = Path(os.environ.get('SCRATCH_DIR', '/scratch'))
SCRATCH.mkdir(parents=True, exist_ok=True)
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
DIGITS = re.compile(r'[0-9]+\Z')
ISO = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:Z|[+-][0-9]{2}:[0-9]{2})\Z')


def timestamp(value):
    if DIGITS.fullmatch(value):
        # Values beyond this length cannot be in any supported window.
        value = value.lstrip('0') or '0'
        return int(value) if len(value) < 19 else None
    if not ISO.fullmatch(value):
        return None
    if value[-1] != 'Z' and (int(value[-5:-3]) > 23 or int(value[-2:]) > 59):
        return None
    try:
        delta = datetime.fromisoformat(value.replace('Z', '+00:00')) - EPOCH
        return delta.days * 86400000 + delta.seconds * 1000
    except ValueError:
        return None


def rows(paths, count):
    for path in sorted(paths):
        with path.open(encoding='utf-8', newline='\n') as source:
            next(source, None)
            for line in source:
                # Only LF terminates a record; CR is field content.
                line = line.removesuffix('\n')
                fields = line.split(',')
                if len(fields) != count:
                    continue
                valid = True
                for i, value in enumerate(fields):
                    if value.startswith('"') and value.endswith('"') and len(value) >= 2:
                        value = value[1:-1]
                    if '"' in value:
                        valid = False
                        break
                    fields[i] = value
                if valid:
                    yield fields


parent = {}

def find(x):
    root = x
    while root in parent and parent[root] != root:
        root = parent[root]
    while x in parent and parent[x] != x:
        old = parent[x]
        parent[x] = root
        x = old
    return root


for a, b in rows((DATA / 'links').glob('*.csv'), 2):
    if a and b:
        a, b = find(a), find(b)
        if a != b:
            parent[max(a, b)] = min(a, b)
for key in parent:
    parent[key] = find(key)
products = dict(rows([DATA / 'products.csv'], 2))
campaigns = dict(rows([DATA / 'campaigns.csv'], 2))
manifest = json.loads((DATA / 'manifest.json').read_text())
start, end = (timestamp(manifest[k]) for k in ('window_start', 'window_end'))

with tempfile.TemporaryDirectory(dir=SCRATCH) as temp:
    temp = Path(temp)
    con = duckdb.connect(str(temp / 'work.db'))
    con.execute(f"SET threads={int(os.environ.get('NCPU', '8'))}")
    con.execute(f"SET memory_limit='{max(1, int(int(os.environ.get('MEM_GB', '8')) * .65))}GB'")
    con.execute('SET temp_directory=?', [str(temp / 'spill')])
    con.execute('SET preserve_insertion_order=false')
    with (temp / 'events.csv').open('w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'ingest', 'person', 'ts', 'category', 'revenue', 'valid'])
        for eid, uid, ts, ingest, kind, product, price in rows((DATA / 'events').glob('*.csv'), 7):
            if not eid:
                continue
            t, ing = timestamp(ts), timestamp(ingest)
            revenue = 0
            valid = bool(uid and product and kind in ('view', 'cart', 'purchase') and t is not None and start <= t < end)
            if kind == 'purchase':
                clean = price.lstrip('0')
                if not DIGITS.fullmatch(price) or len(clean) > 8 or not clean or not 1 <= int(clean) <= 10000000:
                    valid = False
                else:
                    revenue = int(clean)
            valid = valid and ing - (t or 0) <= 86400000
            writer.writerow([eid, ing, find(uid), t or 0, products.get(product, 'unknown'), revenue, valid])
    con.execute("CREATE TABLE events AS SELECT * FROM read_csv(?, header=true, columns={'id':'VARCHAR','ingest':'BIGINT','person':'VARCHAR','ts':'BIGINT','category':'VARCHAR','revenue':'BIGINT','valid':'BOOLEAN'}) QUALIFY row_number() OVER(PARTITION BY id ORDER BY ingest)=1", [str(temp / 'events.csv')])
    with (temp / 'clicks.csv').open('w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'person', 'ts', 'campaign'])
        for cid, uid, campaign, ts in rows((DATA / 'clicks').glob('*.csv'), 4):
            t = timestamp(ts)
            if cid and uid and campaign in campaigns and t is not None and start - 604800000 <= t < end:
                writer.writerow([cid, find(uid), t, campaign])
    con.execute("CREATE TABLE clicks AS SELECT DISTINCT * FROM read_csv(?, header=true, columns={'id':'VARCHAR','person':'VARCHAR','ts':'BIGINT','campaign':'VARCHAR'})", [str(temp / 'clicks.csv')])
    result = con.execute("SELECT person, ts, 1 AS kind, id, campaign AS value, 0 AS revenue FROM clicks UNION ALL SELECT person, ts, 2, id, category, revenue FROM events WHERE valid ORDER BY person, ts, kind, id")
    users = {}
    credited = defaultdict(lambda: [0, 0, 0, 0])
    current = None
    clicks = deque()
    session = None

    def finish():
        if session is None:
            return
        person, first, count, revenue, categories, winner, assists = session
        day = (EPOCH + timedelta(milliseconds=first)).date()
        entry = users.setdefault((person, day), [0, 0, 0, defaultdict(int)])
        entry[0] += 1
        entry[1] += count
        entry[2] += revenue
        for category, amount in categories.items():
            entry[3][category] += amount
        entry = credited[winner, day]
        entry[0] += 1
        entry[1] += int(revenue > 0)
        entry[2] += revenue
        for campaign in assists:
            credited[campaign, day][3] += 1

    previous = None
    while batch := result.fetchmany(65536):
        for person, ts, kind, eid, value, revenue in batch:
            if person != current:
                finish()
                session = None
                clicks.clear()
                current = person
                previous = None
            if kind == 1:
                clicks.append((ts, eid, value))
                continue
            if previous is None or ts - previous > 1800000:
                finish()
                while clicks and clicks[0][0] < ts - 604800000:
                    clicks.popleft()
                winner = min(clicks, key=lambda c: (-c[0], c[1]))[2] if clicks else 'organic'
                session = [person, ts, 0, 0, defaultdict(int), winner, {c[2] for c in clicks}]
            previous = ts
            session[2] += 1
            session[3] += revenue
            if revenue:
                session[4][value] += revenue
    finish()
    con.execute('CREATE TABLE user_daily(user_id VARCHAR, day DATE, sessions BIGINT, events BIGINT, revenue_cents BIGINT, top_category VARCHAR)')
    con.execute('CREATE TABLE campaign_daily(campaign_id VARCHAR, channel VARCHAR, day DATE, sessions BIGINT, purchasing_sessions BIGINT, revenue_cents BIGINT, assisted_sessions BIGINT)')
    def insert(table, records):
        path = temp / (table + '.csv')
        with path.open('w', newline='') as target:
            csv.writer(target).writerows(records)
        if path.stat().st_size:
            con.execute(f"INSERT INTO {table} SELECT * FROM read_csv(?, header=false, auto_detect=false, columns=" + ("{'user_id':'VARCHAR','day':'DATE','sessions':'BIGINT','events':'BIGINT','revenue_cents':'BIGINT','top_category':'VARCHAR'}" if table == 'user_daily' else "{'campaign_id':'VARCHAR','channel':'VARCHAR','day':'DATE','sessions':'BIGINT','purchasing_sessions':'BIGINT','revenue_cents':'BIGINT','assisted_sessions':'BIGINT'}") + ")", [str(path)])
    con.execute('BEGIN')
    insert('user_daily', ((person, day, n, events, revenue, min(cats, key=lambda c: (-cats[c], c)) if cats else None) for (person, day), (n, events, revenue, cats) in users.items()))
    insert('campaign_daily', ((cid, campaigns.get(cid, 'organic'), day, *values) for (cid, day), values in credited.items()))
    con.execute('COMMIT')
    for table in ('user_daily', 'campaign_daily'):
        folder = OUT / table
        folder.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY {table} TO ? (FORMAT PARQUET)", [str(folder / 'part-0.parquet')])
    con.close()
